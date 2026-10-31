package limiter

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkCheck(b *testing.B) {
	l, err := NewTokenBucket(1e9, 1e9)
	if err != nil {
		b.Fatal(err)
	}

	req := mustNewRequest(b, "ip:1", 1)

	ctx := context.Background()

	for b.Loop() {
		if _, err := l.Check(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCheckParallel(b *testing.B) {
	b.Run("same key", func(b *testing.B) {
		l, err := NewTokenBucket(1e9, 1e9)
		if err != nil {
			b.Fatal(err)
		}

		req := mustNewRequest(b, "ip:1", 1)
		ctx := context.Background()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, err := l.Check(ctx, req)
				if err != nil {
					b.Error(err)
					return
				}
			}
		})
	})

	b.Run("key per goroutine", func(b *testing.B) {
		l, err := NewTokenBucket(1e9, 1e9)
		if err != nil {
			b.Fatal(err)
		}

		var id atomic.Int64

		ctx := context.Background()

		b.RunParallel(func(pb *testing.PB) {
			req, err := NewRequest(fmt.Sprintf("ip:%d", id.Add(1)), 1)
			if err != nil {
				b.Error(err)
				return
			}

			for pb.Next() {
				_, err := l.Check(ctx, req)
				if err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}

func BenchmarkMemoryPerKey(b *testing.B) {
	const keys = 1_000_000
	var perKey, perKeySwept float64

	for b.Loop() {
		before := liveHeap()

		l, err := NewTokenBucket(10, 1)
		if err != nil {
			b.Fatal(err)
		}

		for i := range keys {
			req := mustNewRequest(b, fmt.Sprintf("ip:%d", i), 1)
			mustCheck(b, l, req)
		}

		filled := liveHeap()

		l.store.sweep(time.Now().Add(time.Hour), idleTTL(l.limit, l.rate))

		swept := liveHeap()
		// l is not used after the sweep, so without this the GC inside
		// liveHeap frees the whole limiter and the last reading drops to ~0.
		runtime.KeepAlive(l)

		perKey += (filled - before) / keys
		perKeySwept += (swept - before) / keys
	}
	b.ReportMetric(perKey/float64(b.N), "B/key")
	b.ReportMetric(perKeySwept/float64(b.N), "B/key-after-sweep")
}

// liveHeap runs a garbage collection and returns the bytes held by live heap
// objects, as a float64 so that differences between readings cannot wrap
// around the way uint64 subtraction does.
func liveHeap() float64 {
	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc)
}
