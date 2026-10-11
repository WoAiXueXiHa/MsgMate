// Package concurrency executes bounded batches without an unbounded task queue.
package concurrency

import (
	"context"
	"sync"
)

// Run waits for all started callbacks. Unstarted work is left to the caller on cancellation.
func Run(ctx context.Context, size, workers int, run func(int) error) error {
	if size == 0 {
		return ctx.Err()
	}
	if workers < 1 {
		workers = 1
	}
	if workers > size {
		workers = size
	}
	jobs := make(chan int)
	var wg sync.WaitGroup
	var once sync.Once
	var first error
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if ctx.Err() != nil {
					continue
				}
				if err := run(i); err != nil {
					once.Do(func() { first = err })
				}
			}
		}()
	}
loop:
	for i := 0; i < size; i++ {
		select {
		case <-ctx.Done():
			break loop
		case jobs <- i:
		}
	}
	close(jobs)
	wg.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return first
}
