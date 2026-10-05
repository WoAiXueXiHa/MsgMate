package data

import (
	"context"
	"errors"
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
