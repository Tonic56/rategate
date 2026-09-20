package limiter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
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
func newTestLimiter(limit, rate float64) (*TokenBucketLimiter, error) {
	fc := newFakeClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	l, err := NewTokenBucket(limit, rate, withClock(fc))
	if err != nil {
		return nil, err
	}
	return l, nil
}

func TestInvalidLimit(t *testing.T) {
	if _, err := NewTokenBucket(0, 1); !errors.Is(err, ErrInvalidLimit) {
		t.Errorf("expected ErrInvalidLimit, got %v", err)
	}
}

func TestInvalidRate(t *testing.T) {
	if _, err := NewTokenBucket(1, 0); !errors.Is(err, ErrInvalidRate) {
		t.Errorf("expected ErrInvalidRate, got %v", err)
	}
}

func TestNanAndInf(t *testing.T) {
	if _, err := NewTokenBucket(math.Inf(1), 1); !errors.Is(err, ErrInvalidLimit) {
		t.Errorf("expected ErrInvalidLimit for Inf limit, got %v", err)
	}
	if _, err := NewTokenBucket(1, math.NaN()); !errors.Is(err, ErrInvalidRate) {
		t.Errorf("expected ErrInvalidRate for NaN rate, got %v", err)
	}
}

func TestRefillTooSlow(t *testing.T) {
	_, err := NewTokenBucket(1_000_000, 1.0/86400)
	if !errors.Is(err, ErrRefillTooSlow) {
		t.Errorf("want ErrRefillTooSlow, got %v", err)
	}
}

func TestHappyPath(t *testing.T) {
	l, err := newTestLimiter(3, 1)
	if err != nil {
		t.Fatalf("newTestLimiter: %v", err)
	}

	req, err := NewRequest("ip:1", 1)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	var allowed int
	for range 4 {
		res, err := l.Check(context.Background(), req)
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
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
		res, err := l.Check(context.Background(), req)
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if res.Allowed {
			allowed++
		}
	}
	if allowed != 1 {
		t.Fatalf("expected 1 allowed, got %d", allowed)
	}
}

func TestDifferentKeys(t *testing.T) {
	l, err := newTestLimiter(1, 1)
	if err != nil {
		t.Fatalf("newTestLimiter: %v", err)
	}

	reqA, err := NewRequest("ip:a", 1)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	reqB, err := NewRequest("ip:b", 1)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	res, err := l.Check(context.Background(), reqA)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !res.Allowed {
		t.Error("ip:a: expected Allowed=true")
	}

	res, err = l.Check(context.Background(), reqA)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if res.Allowed {
		t.Error("ip:a again: expected Allowed=false")
	}

	res, err = l.Check(context.Background(), reqB)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !res.Allowed {
		t.Errorf("ip:b expected Allowed=true")
	}
}

func TestRefillCapsAtLimit(t *testing.T) {
	l, err := newTestLimiter(10, 100)
	if err != nil {
		t.Fatalf("newTestLimiter: %v", err)
	}

	req, err := NewRequest("ip:1", 1)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	if _, err := l.Check(context.Background(), req); err != nil {
		t.Fatalf("Check: %v", err)
	}

	l.clock.(*fakeClock).Add(10 * time.Minute)

	res, err := l.Check(context.Background(), req)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}

	if res.Remaining != 9 {
		t.Errorf("expected Remaining=9 (refilled to limit 10, minus 1), got %f", res.Remaining)
	}
}

func TestContextCancelled(t *testing.T) {
	l, err := newTestLimiter(10, 1)
	if err != nil {
		t.Fatalf("newTestLimiter: %v", err)
	}

	req, err := NewRequest("ip:1", 1)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

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
				t.Errorf("refill(%s) = %v, want = %v", tc.name, tokens, tc.want)
			}
		})
	}
}

func TestConcurrentCheck(t *testing.T) {
	l, err := newTestLimiter(100, 1)
	if err != nil {
		t.Fatalf("newTestLimiter: %v", err)
	}
	req, err := NewRequest("ip:1", 1)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

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
	l, err := newTestLimiter(1, 1)
	if err != nil {
		t.Fatalf("newTestLimiter: %v", err)
	}

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
		t.Fatalf("expected allowed = 200, got: %d", allowed)
	}
}

func TestSweepIsInvisibleToClients(t *testing.T) {
	l, err := newTestLimiter(10, 1)
	if err != nil {
		t.Fatalf("newTestLimiter: %v", err)
	}

	req, err := NewRequest("ip:a", 1)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	for range 10 {
		_, err = l.Check(context.Background(), req)
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
	}

	l.clock.(*fakeClock).Add(5 * time.Second)
	idleTTL := 10 * time.Second

	removed := l.store.sweep(l.clock.Now(), idleTTL)

	if removed != 0 {
		t.Errorf("sweep removed %d buckets, want 0", removed)
	}

	res, err := l.Check(context.Background(), req)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}

	if res.Remaining != 4 {
		t.Errorf("expected Remaining = 4, got: %v", res.Remaining)
	}
}
