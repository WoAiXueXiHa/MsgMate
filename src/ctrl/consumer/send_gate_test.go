package consumer

import (
	"context"
	"testing"
	"time"
)

func TestChannelGateBoundAndCancellation(t *testing.T) {
	g := newSendGate(1, 0)
	release, err := g.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if r, err := g.acquire(ctx); err == nil {
		r()
		t.Fatal("cancelled waiter acquired occupied gate")
	}
	release()
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := g.acquire(ctx)
	if err != nil {
		t.Fatal("cancelled waiter leaked capacity", err)
	}
	r()
}
func TestChannelPacingAndCancellation(t *testing.T) {
	g := newSendGate(4, 10)
	r, err := g.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r()
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	if r, err := g.acquire(ctx); err == nil {
		r()
		t.Fatal("paced wait ignored deadline")
	}
	cancel()
	r, err = g.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r()
	if time.Since(started) < 80*time.Millisecond {
		t.Fatal("channel start rate was exceeded")
	}
}
