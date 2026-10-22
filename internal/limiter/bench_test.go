package limiter

import (
	"context"
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
