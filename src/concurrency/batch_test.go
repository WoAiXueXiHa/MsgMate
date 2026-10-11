package concurrency

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestBoundedBatchAndStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var active, max, started int64
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, 100, 2, func(int) error {
			n := atomic.AddInt64(&active, 1)
			atomic.AddInt64(&started, 1)
			for {
				m := atomic.LoadInt64(&max)
				if n <= m || atomic.CompareAndSwapInt64(&max, m, n) {
					break
				}
			}
			entered <- struct{}{}
			<-release
			atomic.AddInt64(&active, -1)
			return nil
		})
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("workers did not start")
		}
	}
	cancel()
	close(release)
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("stop did not join workers")
	}
	if max != 2 || started != 2 || active != 0 {
		t.Fatalf("max=%d started=%d active=%d", max, started, active)
	}
}
