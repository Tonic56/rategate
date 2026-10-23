package limiter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"testing/synctest"
	"time"
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
// 2024-01-01 00:00:00 UTC. Move time with l.clock.(*fakeClock).Add.
// The test fails right away if the limiter cannot be created.
func newTestLimiter(t testing.TB, limit, rate float64) *TokenBucketLimiter {
	t.Helper()
	fc := newFakeClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	l, err := NewTokenBucket(limit, rate, withClock(fc))
	if err != nil {
		t.Fatalf("newTestLimiter: %v", err)
	}
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
	if _, err := NewTokenBucket(1, 1, WithSweepInterval(0)); !errors.Is(err, ErrInvalidSweepInterval) {
		t.Fatalf("got %v, want %v", err, ErrInvalidSweepInterval)
	}
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
			if _, err := NewRequest(tc.key, tc.cost); !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("got %v, want %v", err, ErrInvalidRequest)
			}
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
	if allowed != 3 {
		t.Fatalf("expected 3 allowed, got %d", allowed)
	}
	allowed = 0
	l.clock.(*fakeClock).Add(time.Second)
	for range 2 {
		res := mustCheck(t, l, req)
		if res.Allowed {
			allowed++
		}
	}
	if allowed != 1 {
		t.Fatalf("expected 1 allowed, got %d", allowed)
	}
}

func TestDifferentKeys(t *testing.T) {
	l := newTestLimiter(t, 1, 1)

	reqA := mustNewRequest(t, "ip:a", 1)
	reqB := mustNewRequest(t, "ip:b", 1)

	res := mustCheck(t, l, reqA)
	if !res.Allowed {
		t.Error("ip:a: expected Allowed=true")
	}

	res = mustCheck(t, l, reqA)
	if res.Allowed {
		t.Error("ip:a again: expected Allowed=false")
	}

	res = mustCheck(t, l, reqB)
	if !res.Allowed {
		t.Error("ip:b expected Allowed=true")
	}
}

func TestCostExceedsLimit(t *testing.T) {
	l := newTestLimiter(t, 10, 1)
	req := mustNewRequest(t, "ip:1", 11)

	if _, err := l.Check(t.Context(), req); !errors.Is(err, ErrCostExceedsLimit) {
		t.Fatalf("got %v, want %v", err, ErrCostExceedsLimit)
	}
}

func TestCheckRejectsInvalidRequest(t *testing.T) {
	l := newTestLimiter(t, 10, 1)

	if _, err := l.Check(t.Context(), Request{Key: "", Cost: 1}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("got %v, want %v", err, ErrInvalidRequest)
	}
}

func TestRefillCapsAtLimit(t *testing.T) {
	l := newTestLimiter(t, 10, 100)

	req := mustNewRequest(t, "ip:1", 1)

	mustCheck(t, l, req)

	l.clock.(*fakeClock).Add(10 * time.Minute)

	res := mustCheck(t, l, req)
	if res.Remaining != 9 {
		t.Errorf("expected Remaining=9 (refilled to limit 10, minus 1), got %f", res.Remaining)
	}
}

func TestContextCancelled(t *testing.T) {
	l := newTestLimiter(t, 10, 1)

	req := mustNewRequest(t, "ip:1", 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := l.Check(ctx, req); !errors.Is(err, context.Canceled) {
		t.Errorf("expected error from cancelled context, got %v", err)
	}
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
			tokens := refill(tc.tokens, tc.elapsed, tc.rate, tc.limit)
			if tokens != tc.want {
				t.Errorf("refill(%v, %v, %v, %v) = %v, want %v", tc.tokens, tc.elapsed, tc.rate, tc.limit, tokens, tc.want)
			}
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
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := idleTTL(tc.limit, tc.rate); got != tc.want {
				t.Errorf("idleTTL(%v, %v) = %v, want %v", tc.limit, tc.rate, got, tc.want)
			}
		})
	}
}

func TestConcurrentCheck(t *testing.T) {
	l := newTestLimiter(t, 100, 1)

	req := mustNewRequest(t, "ip:1", 1)

	var wg sync.WaitGroup
	var allowed int
	var mu sync.Mutex
	for range 200 {
		wg.Go(func() {
			res, err := l.Check(context.Background(), req)
			if err != nil {
				t.Errorf("Check: %v", err)
				return
			}
			if res.Allowed {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		})
	}

	wg.Wait()

	if allowed != 100 {
		t.Errorf("expected exactly 100 allowed, got %d", allowed)
	}
}

func TestConcurrentStore(t *testing.T) {
	l := newTestLimiter(t, 1, 1)

	var wg sync.WaitGroup
	var allowed int
	var mu sync.Mutex

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
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if allowed != 200 {
		t.Fatalf("expected 200 allowed, got %d", allowed)
	}
}

func TestSweepIsInvisibleToClients(t *testing.T) {
	l := newTestLimiter(t, 10, 1)

	req := mustNewRequest(t, "ip:a", 1)

	for range 10 {
		mustCheck(t, l, req)
	}

	l.clock.(*fakeClock).Add(5 * time.Second)
	idleTTL := 10 * time.Second

	removed := l.store.sweep(l.clock.Now(), idleTTL)

	if removed != 0 {
		t.Errorf("sweep removed %d buckets, want 0", removed)
	}

	res := mustCheck(t, l, req)
	if res.Remaining != 4 {
		t.Errorf("expected Remaining=4 (refilled 5, minus 1), got %v", res.Remaining)
	}
}

func TestRunRemovesIdleBuckets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l, err := NewTokenBucket(10, 1, WithSweepInterval(time.Minute))
		if err != nil {
			t.Fatalf("NewTokenBucket: %v", err)
		}
		go l.Run(t.Context())
		req := mustNewRequest(t, "ip:1", 5)

		mustCheck(t, l, req)

		if n := countBuckets(l.store); n != 1 {
			t.Fatalf("before the janitor ran: %d buckets, want 1", n)
		}

		time.Sleep(3 * time.Minute)

		synctest.Wait()

		if n := countBuckets(l.store); n != 0 {
			t.Errorf("%d buckets left after the janitor ran, want 0", n)
		}
	})
}

func TestRunStopsWhenContextIsCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l, err := NewTokenBucket(10, 1, WithSweepInterval(time.Minute))
		if err != nil {
			t.Fatalf("NewTokenBucket: %v", err)
		}

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		go l.Run(ctx)

		req := mustNewRequest(t, "ip:1", 5)

		mustCheck(t, l, req)

		if n := countBuckets(l.store); n != 1 {
			t.Fatalf("before the janitor ran: %d buckets, want 1", n)
		}

		cancel()

		synctest.Wait()

		time.Sleep(3 * time.Minute)

		synctest.Wait()

		if n := countBuckets(l.store); n != 1 {
			t.Errorf("%d buckets left after Run was stopped, want 1 (nothing may be removed)", n)
		}
	})
}

// expectErr checks that NewTokenBucket(limit, rate) fails with want.
func expectErr(t testing.TB, limit, rate float64, want error) {
	t.Helper()
	if _, err := NewTokenBucket(limit, rate); !errors.Is(err, want) {
		t.Errorf("NewTokenBucket(%v, %v): got %v, want %v", limit, rate, err, want)
	}
}

// mustNewRequest returns a valid request or fails the test.
func mustNewRequest(t testing.TB, key string, cost float64) Request {
	t.Helper()

	req, err := NewRequest(key, cost)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	return req
}

// mustCheck runs l.Check and fails the test on error. Like any helper that
// calls t.Fatal, it must only be called from the test's own goroutine.
func mustCheck(t testing.TB, l *TokenBucketLimiter, req Request) Result {
	t.Helper()

	res, err := l.Check(t.Context(), req)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}

	return res
}
