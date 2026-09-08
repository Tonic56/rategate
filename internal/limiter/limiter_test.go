package limiter

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

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

func newTestLimiter(limit, rate float64) (*TokenBucketLimiter, error) {
	l, err := NewTokenBucket(limit, rate)
	if err != nil {
		return nil, err
	}
	l.clock = newFakeClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
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

func TestHappyPath(t *testing.T) {
	l, err := newTestLimiter(3, 1)
	if err != nil {
		t.Fatalf("TestHappyPath: %v", err)
	}

	req, err := NewRequest("ip:1", 1)
	if err != nil {
		t.Fatalf("TestHappyPath: %v", err)
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
		t.Fatalf("Expected allowed=1: %d", allowed)
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
		t.Fatalf("Expected allowed=1: %d", allowed)
	}
}

func TestDifferentKeys(t *testing.T) {
	l, err := newTestLimiter(1, 1)
	if err != nil {
		t.Fatalf("NewTokenBucket: %v", err)
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

func TestNoOverflow(t *testing.T) {
	l, err := newTestLimiter(10, 100)
	if err != nil {
		t.Fatalf("TestTokenBucket: %v", err)
	}

	req, err := NewRequest("ip:1", 1)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	res, err := l.Check(context.Background(), req)
	l.clock.(*fakeClock).Add(10 * time.Minute)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}

	if res.Remaining != 9 {
		t.Errorf("expected clamp at limit, got %f", res.Remaining)
	}
}

func TestContextCancelled(t *testing.T) {
	l, err := newTestLimiter(10, 1)
	if err != nil {
		t.Fatalf("NewTokenBucket: %v", err)
	}

	req, err := NewRequest("ip:1", 1)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := l.Check(ctx, req); err == nil {
		t.Error("expected error from cancelled context")
	}
}

func TestConcurrent(t *testing.T) {
	l, err := newTestLimiter(100, 1)
	if err != nil {
		t.Fatalf("NewTokenBucket: %v", err)
	}
	req, err := NewRequest("ip:1", 1)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	var wg sync.WaitGroup
	var allowed int
	var mu sync.Mutex
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := l.Check(context.Background(), req)
			if err != nil {
				return
			}
			if res.Allowed {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	if allowed != 100 {
		t.Errorf("Expected exactly 100 allowed, got %d", allowed)
	}
}
