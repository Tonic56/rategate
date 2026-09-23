package limiter

import (
	"testing"
	"time"
)

// countBuckets returns how many buckets the store holds across all shards.
func countBuckets(s *shardedStore) int {
	total := 0
	for _, sh := range s.shards {
		sh.mu.Lock()
		total += len(sh.buckets)
		sh.mu.Unlock()
	}
	return total
}

func TestSweepRemovesIdleBuckets(t *testing.T) {

	s := newShardedStore()
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	s.withBucket("ip:a", t0, 5, func(*bucket) {})
	s.withBucket("ip:b", t0, 5, func(*bucket) {})

	idleTTL := 10 * time.Second
	removed := s.sweep(t0.Add(idleTTL), idleTTL)

	if removed != 2 {
		t.Errorf("sweep removed %d buckets, want 2", removed)
	}

	if left := countBuckets(s); left != 0 {
		t.Errorf("%d buckets left in the store want 0", left)
	}
}

func TestSweepBoundary(t *testing.T) {
	s := newShardedStore()
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	s.withBucket("ip:a", t0, 5, func(*bucket) {})
	s.withBucket("ip:b", t0, 5, func(*bucket) {})

	idleTTL := 10 * time.Second
	removed := s.sweep(t0.Add(idleTTL-time.Nanosecond), idleTTL)

	if removed != 0 {
		t.Errorf("sweep removed %d buckets, want 0", removed)
	}

	if left := countBuckets(s); left != 2 {
		t.Errorf("%d buckets left in the store want 2", left)
	}
}

func TestSweepKeepsRecentBuckets(t *testing.T) {
	s := newShardedStore()
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	s.withBucket("ip:a", t0, 5, func(*bucket) {})
	s.withBucket("ip:b", t0.Add(5*time.Second), 5, func(*bucket) {})

	idleTTL := 10 * time.Second
	removed := s.sweep(t0.Add(idleTTL), idleTTL)

	if removed != 1 {
		t.Errorf("sweep removed %d buckets, want 1", removed)
	}

	if left := countBuckets(s); left != 1 {
		t.Errorf("%d buckets left in the store want 1", left)
	}
}
