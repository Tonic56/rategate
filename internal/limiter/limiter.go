package limiter

import (
	"context"
	"math"
	"time"
)

const (
	// defaultSweepInterval is how often Run removes idle buckets unless
	// WithSweepInterval says otherwise.
	defaultSweepInterval = time.Minute

	// minIdleTTL is the lower bound for idleTTL. With a fast rate a bucket
	// refills in milliseconds, and removing it that early would only make
	// the limiter delete and recreate buckets for nothing. Keeping a bucket
	// longer than needed is always safe, it only costs memory.
	minIdleTTL = time.Minute
)

// Limiter is the contract for rate limiting implementations.
// A Limiter must be safe for concurrent use by multiple goroutines.
type Limiter interface {
	Check(ctx context.Context, req Request) (Result, error)
}

// clock abstracts the source of time so that tests can control the passage
// of time instead of relying on the real wall clock.
type clock interface {
	Now() time.Time
}

// realClock returns the current wall-clock time.
type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

// TokenBucketLimiter is an in-memory rate limiter based on the token bucket
// algorithm. Each key gets its own bucket that is refilled at a fixed rate.
//
// Buckets of keys that stop sending requests are only freed while Run is
// running; without it the limiter still works, but memory keeps growing.
type TokenBucketLimiter struct {
	limit         float64
	rate          float64
	store         *shardedStore
	clock         clock
	sweepInterval time.Duration
}

// bucket holds the state for a single key.
type bucket struct {
	tokens     float64
	lastRefill time.Time
}

// Request describes a single rate limit check: how much a client identified
// by Key is asking for.
type Request struct {
	Key  string
	Cost float64
}

// Result describes the outcome of a rate limit check.
type Result struct {
	Allowed    bool
	Remaining  float64
	RetryAfter time.Duration
}

// validate returns ErrInvalidRequest if the request cannot be satisfied by
// any limiter (empty key or non-positive/non-finite cost). Whether the cost
// fits within a particular bucket limit is checked separately in Check.
func (r Request) validate() error {
	if r.Key == "" ||
		r.Cost <= 0 ||
		math.IsInf(r.Cost, 0) ||
		math.IsNaN(r.Cost) {
		return ErrInvalidRequest
	}
	return nil
}

// NewTokenBucket creates a limiter with the given maximum number of tokens
// (limit) and refill speed in tokens per second (rate). It does not start any
// background work: call Run to remove idle buckets.
func NewTokenBucket(limit float64, rate float64, opts ...Option) (*TokenBucketLimiter, error) {
	if limit <= 0 || math.IsInf(limit, 0) || math.IsNaN(limit) {
		return nil, ErrInvalidLimit
	}
	if rate <= 0 || math.IsInf(rate, 0) || math.IsNaN(rate) {
		return nil, ErrInvalidRate
	}

	maxWait := limit / rate
	if maxWait >= float64(math.MaxInt64)/1e9 {
		return nil, ErrRefillTooSlow
	}

	cfg := config{
		clock:         realClock{},
		sweepInterval: defaultSweepInterval,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.sweepInterval <= 0 {
		return nil, ErrInvalidSweepInterval
	}

	l := &TokenBucketLimiter{
		limit:         limit,
		rate:          rate,
		store:         newShardedStore(),
		clock:         cfg.clock,
		sweepInterval: cfg.sweepInterval,
	}

	return l, nil
}

// NewRequest creates a validated request.
func NewRequest(key string, cost float64) (Request, error) {
	r := Request{Key: key, Cost: cost}
	if err := r.validate(); err != nil {
		return Request{}, err
	}
	return r, nil
}

// Check tries to consume req.Cost tokens from the bucket for req.Key. It
// reports whether the request is allowed, how many tokens remain, and when
// the client may retry a denied request. The bucket is created lazily the
// first time a key is seen.
func (l *TokenBucketLimiter) Check(ctx context.Context, req Request) (Result, error) {
	if err := req.validate(); err != nil {
		return Result{}, err
	}
	if req.Cost > l.limit {
		return Result{}, ErrCostExceedsLimit
	}

	// Cancel early rather than blocking on a mutex when the caller no longer
	// needs an answer.
	select {
	case <-ctx.Done():
		return Result{}, ctx.Err()
	default:
	}

	var result Result

	// The time passed here is only used as lastRefill for a brand-new bucket;
	// tryConsume reads the clock again once the lock is held.
	l.store.withBucket(req.Key, l.clock.Now(), l.limit, func(b *bucket) {
		result = l.tryConsume(b, req)
	})

	return result, nil
}

// refill returns the token count after elapsed seconds at rate tokens per
// second, capped at limit. A negative elapsed (clock went backwards) adds
// nothing.
func refill(tokens, elapsed, rate, limit float64) float64 {
	if elapsed < 0 {
		elapsed = 0
	}

	tokens += elapsed * rate

	if tokens > limit {
		tokens = limit
	}
	return tokens
}

// tryConsume refills b up to the current time and then either takes req.Cost
// tokens or computes how long the client has to wait.
//
// It must be called with the lock protecting b held (see store.withBucket).
// The current time is read here, under that lock, on purpose: a time read
// before waiting for the lock may be older than b.lastRefill written by
// another goroutine, and storing it would move lastRefill backwards so the
// same interval gets refilled twice.
func (l *TokenBucketLimiter) tryConsume(b *bucket, req Request) Result {
	now := l.clock.Now()
	elapsed := now.Sub(b.lastRefill).Seconds()

	b.tokens = refill(b.tokens, elapsed, l.rate, l.limit)

	b.lastRefill = now

	if b.tokens >= req.Cost {
		b.tokens -= req.Cost
		return Result{
			Allowed:    true,
			Remaining:  b.tokens,
			RetryAfter: 0,
		}
	}

	waitSeconds := (req.Cost - b.tokens) / l.rate
	return Result{
		Allowed:    false,
		Remaining:  b.tokens,
		RetryAfter: time.Duration(math.Ceil(waitSeconds * float64(time.Second))),
	}
}

// idleTTL returns how long a bucket may stay untouched before it can be
// removed: the time a full refill takes (limit/rate seconds), but at least
// minIdleTTL. A bucket idle that long is full again, so dropping it is
// invisible to the client. The conversion cannot overflow because
// NewTokenBucket rejects limit/rate values that do not fit in a Duration.
func idleTTL(limit float64, rate float64) time.Duration {
	return max(time.Duration((limit/rate)*float64(time.Second)), minIdleTTL)
}

// Run removes idle buckets every sweep interval until ctx is cancelled.
// It blocks, so the caller decides where it runs and for how long, usually
// with go l.Run(ctx) and a ctx that is cancelled on shutdown.
func (l *TokenBucketLimiter) Run(ctx context.Context) {
	ticker := time.NewTicker(l.sweepInterval)
	defer ticker.Stop()

	ttl := idleTTL(l.limit, l.rate)

	for {
		select {
		case <-ticker.C:
			l.store.sweep(l.clock.Now(), ttl)
		case <-ctx.Done():
			return
		}
	}
}
