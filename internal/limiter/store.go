package limiter

import (
	"hash/fnv"
	"sync"
	"time"
)

// shardCount is the number of independent shards. Keys are spread across
// shards by hash, so requests for different keys rarely wait on the same
// mutex.
const shardCount = 32

// shard is one independent part of shardedStore: a map of buckets guarded by
// its own mutex.
type shard struct {
	mu      sync.Mutex
	buckets map[string]*bucket // keyed by the raw key, so hash collisions between keys are harmless
}

// shardedStore keeps one bucket per key in memory and serialises access to
// it. Keys are split across shardCount shards to reduce lock contention.
type shardedStore struct {
	shards [shardCount]*shard
}

// newShardedStore returns a store with all shards initialised and empty.
func newShardedStore() *shardedStore {
	s := &shardedStore{}
	for i := range s.shards {
		s.shards[i] = &shard{buckets: make(map[string]*bucket)}
	}
	return s
}

// shardFor picks the shard for key using FNV-1a: fast, allocation-free and
// spreads similar keys (e.g. neighbouring IPs) evenly. hash.Hash.Write never
// returns an error, so its result is ignored.
func (s *shardedStore) shardFor(key string) *shard {
	h := fnv.New32a()
	h.Write([]byte(key))

	return s.shards[h.Sum32()%shardCount]
}

// withBucket calls fn with the bucket for key, creating it with initial
// tokens and lastRefill = now if the key is new.
//
// The shard lock is held for the whole lookup-or-create and for fn, so
// nothing can observe or delete the bucket halfway through an update. For
// the same reason fn must be quick, must not block and must not keep b
// after it returns.
func (s *shardedStore) withBucket(key string, now time.Time, initial float64, fn func(b *bucket)) {
	sh := s.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	b, exists := sh.buckets[key]

	if !exists {
		b = &bucket{
			tokens:     initial,
			lastRefill: now,
		}
		sh.buckets[key] = b
	}
	fn(b)
}

// sweep removes idle buckets from every shard. Shards are locked one at a
// time, so a sweep never blocks the whole store.
func (s *shardedStore) sweep(now time.Time, idleTTL time.Duration) int {
	var counter int
	for _, sh := range s.shards {
		counter += sh.sweep(now, idleTTL)
	}

	return counter
}

// sweep removes the buckets of this shard that have been idle for at least
// idleTTL and returns how many were removed. A bucket idle that long is
// already refilled to the limit, so a client cannot tell a removed bucket
// from a fresh one.
func (sh *shard) sweep(now time.Time, idleTTL time.Duration) int {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	var counter int

	for key, b := range sh.buckets {
		if now.Sub(b.lastRefill) >= idleTTL {
			delete(sh.buckets, key)
			counter++
		}
	}

	return counter
}
