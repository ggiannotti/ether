package usenet

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/braedonsaunders/ether/internal/config"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/usenet/nntptest"
)

func TestBurstBorrowsIdleSlotButYieldsToLive(t *testing.T) {
	s := newPrioSem(2, 1)
	s.reserveLive = true
	if !s.TryAcquire(core.PriorityReadahead) {
		t.Fatal("first background slot")
	}
	if s.TryAcquire(core.PriorityReadahead) {
		t.Fatal("ordinary read stole live reserve")
	}
	ctx := context.WithValue(context.Background(), burstKey{}, time.Now().Add(time.Second))
	if err := s.Acquire(ctx, core.PriorityReadahead); err != nil {
		t.Fatal(err)
	}
	if s.stats().InUse != 2 {
		t.Fatal("burst left idle capacity")
	}
	live := acquireAsync(s, context.Background(), core.PriorityLive)
	waitQueued(t, s, core.PriorityLive, 1)
	burst := acquireAsync(s, ctx, core.PriorityReadahead)
	waitQueued(t, s, core.PriorityReadahead, 1)
	s.Release(core.PriorityReadahead)
	if err := <-live; err != nil {
		t.Fatal(err)
	}
	select {
	case <-burst:
		t.Fatal("burst displaced active live read")
	default:
	}
	s.Release(core.PriorityLive)
	if err := <-burst; err != nil {
		t.Fatal(err)
	}
	s.Release(core.PriorityReadahead)
	s.Release(core.PriorityReadahead)
}

func TestExpiredBurstCannotBorrowReserve(t *testing.T) {
	s := newPrioSem(2, 1)
	s.reserveLive = true
	if !s.TryAcquire(core.PriorityReadahead) {
		t.Fatal("first")
	}
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), burstKey{}, time.Now().Add(-time.Second)), 10*time.Millisecond)
	defer cancel()
	if err := s.Acquire(ctx, core.PriorityReadahead); err == nil {
		t.Fatal("expired burst borrowed reserve")
	}
	s.Release(core.PriorityReadahead)
}

func TestBodyBurstUsesEveryIdleConnection(t *testing.T) {
	s := nntptest.Start(t, nntptest.Options{})
	for i := range 4 {
		addSeg(t, s, fmt.Sprintf("burst%d@test", i), []byte("payload"))
	}
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	defer release()
	entered := make(chan struct{}, 4)
	s.SetFault(func(cmd, id string) nntptest.Fault {
		if cmd == "BODY" {
			entered <- struct{}{}
			<-gate
		}
		return nntptest.Fault{}
	})
	f := newFetcher(t, []config.UsenetProvider{s.Provider("burst", 4)}, Options{MinIdle: -1})
	done := make(chan error, 4)
	// Generous bounds: this asserts all four sockets fill, not timing, and
	// loaded CI machines can be slow to dial and schedule the workers.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i := range 4 {
		go func(i int) {
			_, _, err := f.BodyPartBurst(ctx, fmt.Sprintf("burst%d@test", i), nil, core.PriorityReadahead, time.Now().Add(10*time.Second))
			done <- err
		}(i)
	}
	for range 4 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("burst failed to occupy all four idle sockets")
		}
	}
	release()
	for range 4 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
