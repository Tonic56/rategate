package limiter

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClock is a clock that stands still until the test moves it with Add.
// It is safe for concurrent use.
type fakeClock struct {
	current time.Time
	mu      sync.Mutex
}

func newFakeClock(t time.Time) *fakeClock {
	return &fakeClock{
		current: t,
	}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.current
}

func (f *fakeClock) Add(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.current = f.current.Add(d)
}

// newTestLimiter returns a limiter whose clock is a fakeClock set to
// 2024-01-01 00:00:00 UTC. Move time with advance.
// The test fails right away if the limiter cannot be created.
func newTestLimiter(tb testing.TB, limit, rate float64) *TokenBucketLimiter {
	tb.Helper()
	fc := newFakeClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	l, err := NewTokenBucket(limit, rate, withClock(fc))
	require.NoError(tb, err)
	return l
}

func TestInvalidLimit(t *testing.T) {
	expectErr(t, 0, 1, ErrInvalidLimit)
}

func TestInvalidRate(t *testing.T) {
	expectErr(t, 1, 0, ErrInvalidRate)
}

func TestNaNAndInf(t *testing.T) {
	expectErr(t, math.Inf(1), 1, ErrInvalidLimit)

	expectErr(t, 1, math.NaN(), ErrInvalidRate)
}

func TestInvalidSweepInterval(t *testing.T) {
	_, err := NewTokenBucket(1, 1, WithSweepInterval(0))
	require.ErrorIs(t, err, ErrInvalidSweepInterval)
}

func TestInvalidRequest(t *testing.T) {
	cases := []struct {
		name string
		key  string
		cost float64
	}{
		{"empty key", "", 1},
		{"zero cost", "ip:1", 0},
		{"negative cost", "ip:1", -1},
		{"cost +Inf", "ip:1", math.Inf(1)},
		{"cost -Inf", "ip:1", math.Inf(-1)},
		{"cost NaN", "ip:1", math.NaN()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewRequest(tc.key, tc.cost)
			require.ErrorIs(t, err, ErrInvalidRequest)
		})
	}
}

func TestRefillTooSlow(t *testing.T) {
	expectErr(t, 1_000_000, 1.0/86400, ErrRefillTooSlow)
}

func TestHappyPath(t *testing.T) {
	l := newTestLimiter(t, 3, 1)

	req := mustNewRequest(t, "ip:1", 1)

	var allowed int
	for range 4 {
		res := mustCheck(t, l, req)
		if res.Allowed {
			allowed++
		}
	}
	require.Equal(t, 3, allowed, "4 requests to a full bucket of 3")

	allowed = 0
	advance(l, time.Second)
	for range 2 {
		res := mustCheck(t, l, req)
		if res.Allowed {
			allowed++
		}
	}
	assert.Equal(t, 1, allowed, "2 requests after a second of refill")
}

func TestDifferentKeys(t *testing.T) {
	l := newTestLimiter(t, 1, 1)

	reqA := mustNewRequest(t, "ip:a", 1)
	reqB := mustNewRequest(t, "ip:b", 1)

	res := mustCheck(t, l, reqA)
	assert.True(t, res.Allowed, "ip:a, first request")

	res = mustCheck(t, l, reqA)
	assert.False(t, res.Allowed, "ip:a, second request")

	res = mustCheck(t, l, reqB)
	assert.True(t, res.Allowed, "ip:b, first request")
}

func TestCostExceedsLimit(t *testing.T) {
	l := newTestLimiter(t, 10, 1)
	req := mustNewRequest(t, "ip:1", 11)

	_, err := l.Check(t.Context(), req)
	require.ErrorIs(t, err, ErrCostExceedsLimit)
}

func TestCheckRejectsInvalidRequest(t *testing.T) {
	l := newTestLimiter(t, 10, 1)

	_, err := l.Check(t.Context(), Request{Key: "", Cost: 1})
	require.ErrorIs(t, err, ErrInvalidRequest)
}

func TestRefillCapsAtLimit(t *testing.T) {
	l := newTestLimiter(t, 10, 100)

	req := mustNewRequest(t, "ip:1", 1)

	mustCheck(t, l, req)

	advance(l, 10*time.Minute)

	res := mustCheck(t, l, req)
	assert.InDelta(t, 9, res.Remaining, 1e-9, "refilled to limit 10, minus 1")
}

func TestContextCancelled(t *testing.T) {
	l := newTestLimiter(t, 10, 1)

	req := mustNewRequest(t, "ip:1", 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := l.Check(ctx, req)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRefillCases(t *testing.T) {
	cases := []struct {
		name    string
		tokens  float64
		elapsed float64
		rate    float64
		limit   float64
		want    float64
	}{
		{"tokens > limit", 50, 1, 1, 10, 10},
		{"tokens < limit", 5, 1, 1, 10, 6},
		{"elapsed < 0", 5, -10, 1, 10, 5},
		{"elapsed = 0", 5, 0, 1, 10, 5},
		{"elapsed * rate = limit", 5, 1, 5, 10, 10},
		{"elapsed * rate > limit", 5, 2, 5, 10, 10},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.InDelta(t, tc.want, refill(tc.tokens, tc.elapsed, tc.rate, tc.limit), 1e-9)
		})
	}
}

// TestDeniedResult checks what a client is told when a request is denied. Each
// case can expose mistakes the others cannot: with no tokens Cost-tokens equals
// Cost, with rate 1 dividing equals multiplying, and only a wait that is not a
// whole number of nanoseconds shows whether it is rounded up.
func TestDeniedResult(t *testing.T) {
	cases := []struct {
		name           string
		limit          float64
		rate           float64
		spend          float64
		wait           time.Duration
		cost           float64
		wantRetryAfter time.Duration
		wantRemaining  float64
	}{
		{"empty bucket", 10, 2, 10, 0, 1, 500 * time.Millisecond, 0},
		{"partly refilled", 10, 2, 10, 300 * time.Millisecond, 1, 200 * time.Millisecond, 0.6},
		{"some tokens left", 10, 1, 7, 0, 5, 2 * time.Second, 3},
		{"wait rounds up", 10, 3, 10, 0, 1, 333333334 * time.Nanosecond, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := newTestLimiter(t, tc.limit, tc.rate)
			mustCheck(t, l, mustNewRequest(t, "ip:1", tc.spend))
			advance(l, tc.wait)
			res := mustCheck(t, l, mustNewRequest(t, "ip:1", tc.cost))

			want := Result{Allowed: false, RetryAfter: tc.wantRetryAfter, Remaining: tc.wantRemaining}
			assert.Equal(t, want, res)
		})
	}
}

func TestIdleTTL(t *testing.T) {
	cases := []struct {
		name  string
		limit float64
		rate  float64
		want  time.Duration
	}{
		{"short refill uses minimum", 10, 1, time.Minute},
		{"long refill is kept", 1000, 1, 16*time.Minute + 40*time.Second},
		{"fractional rate", 10, 0.1, 100 * time.Second},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, idleTTL(tc.limit, tc.rate))
		})
	}
}

func TestConcurrentCheck(t *testing.T) {
	l := newTestLimiter(t, 100, 1)

	req := mustNewRequest(t, "ip:1", 1)

	var wg sync.WaitGroup
	var allowed atomic.Int64
	for range 200 {
		wg.Go(func() {
			res, err := l.Check(context.Background(), req)
			if err != nil {
				t.Errorf("Check: %v", err)
				return
			}
			if res.Allowed {
				allowed.Add(1)
			}
		})
	}

	wg.Wait()

	assert.Equal(t, int64(100), allowed.Load())
}

func TestConcurrentStore(t *testing.T) {
	l := newTestLimiter(t, 1, 1)

	var wg sync.WaitGroup
	var allowed atomic.Int64

	for i := range 200 {
		wg.Go(func() {
			key := fmt.Sprintf("192.168.1.%d", i)
			req, err := NewRequest(key, 1)
			if err != nil {
				t.Errorf("NewRequest: %v", err)
				return
			}
			res, err := l.Check(context.Background(), req)
			if err != nil {
				t.Errorf("Check: %v", err)
				return
			}
			if res.Allowed {
				allowed.Add(1)
			}
		})
	}
	wg.Wait()

	assert.Equal(t, int64(200), allowed.Load())
}

func TestSweepKeepsBucketUntilFull(t *testing.T) {
	l := newTestLimiter(t, 10, 1)

	req := mustNewRequest(t, "ip:a", 1)

	for range 10 {
		mustCheck(t, l, req)
	}

	advance(l, 5*time.Second)
	idleTTL := 10 * time.Second

	removed := l.store.sweep(l.clock.Now(), idleTTL)
	assert.Zero(t, removed)

	res := mustCheck(t, l, req)
	assert.InDelta(t, 4, res.Remaining, 1e-9, "refilled 5, minus 1")
}

// TestSweepIsInvisibleToClients removes a bucket at the moment it becomes full
// and checks that the next request gets the answer the old bucket would give.
// That answer is the same with or without the removal, so the test also has to
// check that the bucket is really gone.
func TestSweepIsInvisibleToClients(t *testing.T) {
	l := newTestLimiter(t, 10, 1)

	req := mustNewRequest(t, "ip:1", 1)

	for range 10 {
		mustCheck(t, l, req)
	}

	advance(l, 10*time.Second)

	idleTTL := 10 * time.Second

	removed := l.store.sweep(l.clock.Now(), idleTTL)

	assert.Equal(t, 1, removed, "buckets removed")
	assert.Zero(t, countBuckets(l.store), "buckets left after the sweep")

	res := mustCheck(t, l, req)
	assert.Equal(t, Result{Allowed: true, Remaining: 9}, res, "a new bucket of 10, minus 1")
	assert.Equal(t, 1, countBuckets(l.store), "buckets after Check")
}

func TestRunRemovesIdleBuckets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l, err := NewTokenBucket(10, 1, WithSweepInterval(time.Minute))
		require.NoError(t, err)
		go l.Run(t.Context())
		req := mustNewRequest(t, "ip:1", 5)

		mustCheck(t, l, req)

		require.Equal(t, 1, countBuckets(l.store), "buckets before the janitor ran")

		time.Sleep(3 * time.Minute)

		synctest.Wait()

		assert.Zero(t, countBuckets(l.store), "buckets after the janitor ran")
	})
}

func TestRunStopsWhenContextIsCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l, err := NewTokenBucket(10, 1, WithSweepInterval(time.Minute))
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		go l.Run(ctx)

		req := mustNewRequest(t, "ip:1", 5)

		mustCheck(t, l, req)

		require.Equal(t, 1, countBuckets(l.store), "buckets before the janitor ran")

		cancel()

		synctest.Wait()

		time.Sleep(3 * time.Minute)

		synctest.Wait()

		assert.Equal(t, 1, countBuckets(l.store), "Run was stopped, so nothing may be removed")
	})
}

// advance moves the fakeClock of a limiter from newTestLimiter forward by d.
func advance(l *TokenBucketLimiter, d time.Duration) {
	l.clock.(*fakeClock).Add(d)
}

// expectErr checks that NewTokenBucket(limit, rate) fails with want.
func expectErr(tb testing.TB, limit, rate float64, want error) {
	tb.Helper()
	_, err := NewTokenBucket(limit, rate)
	require.ErrorIs(tb, err, want)
}

// mustNewRequest returns a valid request or fails the test.
func mustNewRequest(tb testing.TB, key string, cost float64) Request {
	tb.Helper()

	req, err := NewRequest(key, cost)
	require.NoError(tb, err)

	return req
}

// mustCheck runs l.Check and fails the test on error. Like any helper that
// stops the test (require calls t.FailNow), it must only be called from the
// test's own goroutine.
func mustCheck(tb testing.TB, l *TokenBucketLimiter, req Request) Result {
	tb.Helper()

	res, err := l.Check(tb.Context(), req)
	require.NoError(tb, err)

	return res
}
