package limiter

import (
	"context"
	"math"
	"sync"
	"time"
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
type TokenBucketLimiter struct {
	limit   float64
	rate    float64
	buckets map[string]*bucket
	mu      sync.Mutex
	clock   clock
}

// bucket holds the state for a single key.
type bucket struct {
	tokens     float64
	lastRefill time.Time
	mu         sync.Mutex
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
// (limit) and refill speed in tokens per second (rate).
func NewTokenBucket(limit float64, rate float64) (*TokenBucketLimiter, error) {
	if limit <= 0 || math.IsInf(limit, 0) || math.IsNaN(limit) {
		return nil, ErrInvalidLimit
	}
	if rate <= 0 || math.IsInf(rate, 0) || math.IsNaN(rate) {
		return nil, ErrInvalidRate
	}

	return &TokenBucketLimiter{
		limit:   limit,
		rate:    rate,
		buckets: make(map[string]*bucket),
		clock:   realClock{},
	}, nil
}

// NewRequest creates a validated request.
func NewRequest(key string, cost float64) (Request, error) {
	r := Request{Key: key, Cost: cost}
	if err := r.validate(); err != nil {
		return Request{}, err
	}
	return r, nil
}

// Check consumes one token for the given key. It reports whether the request
// is allowed, how many tokens remain, and when the client may retry a denied
// request. The bucket is created lazily the first time a key is seen.
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

	l.mu.Lock()

	currentBucket, exists := l.buckets[req.Key]

	if !exists {
		currentBucket = &bucket{
			tokens:     l.limit,
			lastRefill: l.clock.Now(),
		}
		l.buckets[req.Key] = currentBucket
	}

	l.mu.Unlock()

	currentBucket.mu.Lock()
	defer currentBucket.mu.Unlock()

	currentTime := l.clock.Now()
	elapsed := currentTime.Sub(currentBucket.lastRefill).Seconds()

	currentBucket.tokens = refill(currentBucket.tokens, elapsed, l.rate, l.limit)

	currentBucket.lastRefill = currentTime

	if currentBucket.tokens >= req.Cost {
		currentBucket.tokens -= req.Cost
		return Result{
			Allowed:    true,
			Remaining:  currentBucket.tokens,
			RetryAfter: 0,
		}, nil
	}

	waitSeconds := (req.Cost - currentBucket.tokens) / l.rate
	return Result{
		Allowed:    false,
		Remaining:  currentBucket.tokens,
		RetryAfter: time.Duration(math.Ceil(waitSeconds * float64(time.Second))),
	}, nil
}

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
