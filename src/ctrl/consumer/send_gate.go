package consumer

import (
	"context"
	"sync"
	"time"
)

// sendGate is shared across priorities for one channel in this process.
// Rate 0 disables pacing; otherwise starts are spaced, with no token burst.
type sendGate struct {
	slots    chan struct{}
	interval time.Duration
	mu       sync.Mutex
	next     time.Time
}

func newSendGate(concurrency, rate int) *sendGate {
	g := &sendGate{slots: make(chan struct{}, concurrency)}
	if rate > 0 {
		g.interval = time.Second / time.Duration(rate)
	}
	return g
}
func (g *sendGate) acquire(ctx context.Context) (func(), error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case g.slots <- struct{}{}:
	}
	release := func() { <-g.slots }
	if g.interval > 0 {
		// Recheck admission after every wait so competing waiters cannot
		// consume the same start interval. Waiting remains cancellable.
		for {
			g.mu.Lock()
			now := time.Now()
			delay := g.next.Sub(now)
			if delay <= 0 {
				g.next = now.Add(g.interval)
				g.mu.Unlock()
				break
			}
			g.mu.Unlock()
			if !wait(ctx, delay) {
				release()
				return nil, ctx.Err()
			}
		}
	}
	if ctx.Err() != nil {
		release()
		return nil, ctx.Err()
	}
	return release, nil
}
func (s *MsgConsume) gate(channel int) *sendGate {
	s.gateMu.Lock()
	defer s.gateMu.Unlock()
	if g := s.gates[channel]; g != nil {
		return g
	}
	g := newSendGate(s.settings.ChannelConcurrency, s.settings.ChannelRatePerSecond)
	s.gates[channel] = g
	return g
}
func (s *MsgConsume) lockMessage(ctx context.Context, id string) (func(), error) {
	var hash uint32 = 2166136261
	for i := 0; i < len(id); i++ {
		hash = (hash ^ uint32(id[i])) * 16777619
	}
	slot := s.messageLocks[hash%uint32(len(s.messageLocks))]
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case slot <- struct{}{}:
		return func() { <-slot }, nil
	}
}
