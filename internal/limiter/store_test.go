package limiter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

	s.withBucket("ip:a", func(b *bucket, _ bool) { b.lastRefill = t0 })
	s.withBucket("ip:b", func(b *bucket, _ bool) { b.lastRefill = t0 })

	idleTTL := 10 * time.Second
	removed := s.sweep(t0.Add(idleTTL), idleTTL)

	assert.Equal(t, 2, removed)

	assert.Zero(t, countBuckets(s), "buckets left")
}

func TestSweepBoundary(t *testing.T) {
	s := newShardedStore()
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	s.withBucket("ip:a", func(b *bucket, _ bool) { b.lastRefill = t0 })
	s.withBucket("ip:b", func(b *bucket, _ bool) { b.lastRefill = t0 })

	idleTTL := 10 * time.Second
	removed := s.sweep(t0.Add(idleTTL-time.Nanosecond), idleTTL)

	assert.Zero(t, removed)

	assert.Equal(t, 2, countBuckets(s), "buckets left")
}

func TestSweepKeepsRecentBuckets(t *testing.T) {
	s := newShardedStore()
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	s.withBucket("ip:a", func(b *bucket, _ bool) { b.lastRefill = t0 })
	s.withBucket("ip:b", func(b *bucket, _ bool) { b.lastRefill = t0.Add(5 * time.Second) })

	idleTTL := 10 * time.Second
	removed := s.sweep(t0.Add(idleTTL), idleTTL)

	assert.Equal(t, 1, removed)

	assert.Equal(t, 1, countBuckets(s), "buckets left")
}
