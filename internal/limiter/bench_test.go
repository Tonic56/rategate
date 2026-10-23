package limiter

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
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
