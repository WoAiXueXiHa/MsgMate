package data

import (
	"context"
	"errors"
	"github.com/segmentio/kafka-go"
	"sync/atomic"
	"testing"
	"time"
)

func TestHandlerFailureDoesNotCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commits := 0
	ok := handleAndCommit(ctx, nil, func([]byte) error { cancel(); return errors.New("retry unavailable") }, func() error { commits++; return nil })
	if ok || commits != 0 {
		t.Fatalf("committed failed handler: %d", commits)
	}
}
func TestCommitRetryDoesNotRepeatHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	calls, commits := 0, 0
	ok := handleAndCommit(ctx, nil, func([]byte) error { calls++; return nil }, func() error {
		commits++
		if commits == 1 {
			return errors.New("commit unavailable")
		}
		return nil
	})
	if !ok || calls != 1 || commits != 2 {
		t.Fatalf("calls=%d commits=%d", calls, commits)
	}
}

func TestBatchWaitsForEarlierOffset(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slow := make(chan struct{})
	fast := make(chan struct{})
	committed := make(chan struct{}, 1)
	done := make(chan bool, 1)
	batch := []kafka.Message{{Offset: 1, Value: []byte("slow")}, {Offset: 2, Value: []byte("fast")}}
	go func() {
		done <- handleBatchAndCommit(ctx, batch, 2, func(body []byte) error {
			if string(body) == "slow" {
				<-slow
			} else {
				close(fast)
			}
			return nil
		}, func() error { committed <- struct{}{}; return nil })
	}()
	select {
	case <-fast:
	case <-time.After(time.Second):
		t.Fatal("later message did not run concurrently")
	}
	select {
	case <-committed:
		t.Fatal("committed past unfinished earlier offset")
	default:
	}
	close(slow)
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("batch failed")
		}
	case <-time.After(time.Second):
		t.Fatal("batch did not complete")
	}
	if len(committed) != 1 {
		t.Fatal("batch not committed exactly once")
	}
}
func TestBatchCommitRetryDoesNotResend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var calls int64
	commits := 0
	ok := handleBatchAndCommit(ctx, []kafka.Message{{Value: []byte("a")}, {Value: []byte("b")}}, 2, func([]byte) error { atomic.AddInt64(&calls, 1); return nil }, func() error {
		commits++
		if commits == 1 {
			return errors.New("broker unavailable")
		}
		return nil
	})
	if !ok || calls != 2 || commits != 2 {
		t.Fatalf("ok=%v calls=%d commits=%d", ok, calls, commits)
	}
}
func TestBatchFailureAndCancellationDoNotCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commits := 0
	ok := handleBatchAndCommit(ctx, []kafka.Message{{Value: []byte("bad")}, {Value: []byte("good")}}, 2, func(body []byte) error {
		if string(body) == "bad" {
			cancel()
			return errors.New("retry handoff failed")
		}
		return nil
	}, func() error { commits++; return nil })
	if ok || commits != 0 {
		t.Fatal("committed incomplete batch")
	}
}
