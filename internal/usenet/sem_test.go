package usenet

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
)

// acquireAsync starts an Acquire and returns a channel that receives its
// result.
func acquireAsync(s *prioSem, ctx context.Context, p core.Priority) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- s.Acquire(ctx, p) }()
	return ch
}

func waitQueued(t *testing.T, s *prioSem, p core.Priority, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for s.stats().Waiting[p] != n {
		if time.Now().After(deadline) {
			t.Fatalf("waiting[%d] = %d, want %d", p, s.stats().Waiting[p], n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSemPriorityOrder(t *testing.T) {
	s := newPrioSem(1, 1)
	ctx := context.Background()
	if err := s.Acquire(ctx, core.PriorityLive); err != nil {
		t.Fatal(err)
	}
	pre := acquireAsync(s, ctx, core.PriorityPrefetch)
	waitQueued(t, s, core.PriorityPrefetch, 1)
	ra := acquireAsync(s, ctx, core.PriorityReadahead)
	waitQueued(t, s, core.PriorityReadahead, 1)
	live := acquireAsync(s, ctx, core.PriorityLive)
	waitQueued(t, s, core.PriorityLive, 1)

	s.Release(core.PriorityLive)
	if err := <-live; err != nil {
		t.Fatal(err)
	}
	select {
	case <-ra:
		t.Fatal("readahead granted while slot held")
	case <-pre:
		t.Fatal("prefetch granted while slot held")
	default:
	}
	s.Release(core.PriorityLive)
	if err := <-ra; err != nil {
		t.Fatal(err)
	}
	s.Release(core.PriorityReadahead)
	if err := <-pre; err != nil {
		t.Fatal(err)
	}
	s.Release(core.PriorityPrefetch)
	if st := s.stats(); st.InUse != 0 {
		t.Fatalf("in use %d", st.InUse)
	}
}

func TestSemPrefetchCap(t *testing.T) {
	s := newPrioSem(4, 1)
	ctx := context.Background()
	if !s.TryAcquire(core.PriorityPrefetch) {
		t.Fatal("first prefetch should succeed")
	}
	if s.TryAcquire(core.PriorityPrefetch) {
		t.Fatal("prefetch above its cap")
	}
	// Live and readahead can still use the rest.
	for i := 0; i < 3; i++ {
		if !s.TryAcquire(core.PriorityLive) {
			t.Fatal("live should fit")
		}
	}
	if s.TryAcquire(core.PriorityLive) {
		t.Fatal("over capacity")
	}
	pre := acquireAsync(s, ctx, core.PriorityPrefetch)
	waitQueued(t, s, core.PriorityPrefetch, 1)
	// Freeing a live slot must not admit a second prefetch.
	s.Release(core.PriorityLive)
	select {
	case <-pre:
		t.Fatal("second prefetch admitted")
	case <-time.After(20 * time.Millisecond):
	}
	s.Release(core.PriorityPrefetch)
	if err := <-pre; err != nil {
		t.Fatal(err)
	}
}

func TestSemPrefetchYieldsToWaitingLive(t *testing.T) {
	s := newPrioSem(2, 2)
	ctx := context.Background()
	if !s.TryAcquire(core.PriorityPrefetch) || !s.TryAcquire(core.PriorityPrefetch) {
		t.Fatal("prefetch should take both slots")
	}
	live := acquireAsync(s, ctx, core.PriorityLive)
	waitQueued(t, s, core.PriorityLive, 1)
	pre := acquireAsync(s, ctx, core.PriorityPrefetch)
	waitQueued(t, s, core.PriorityPrefetch, 1)
	if s.TryAcquire(core.PriorityPrefetch) {
		t.Fatal("TryAcquire jumped the queue")
	}
	s.Release(core.PriorityPrefetch)
	if err := <-live; err != nil {
		t.Fatal(err)
	}
	select {
	case <-pre:
		t.Fatal("prefetch should still wait")
	default:
	}
	s.Release(core.PriorityPrefetch)
	if err := <-pre; err != nil {
		t.Fatal(err)
	}
}

func TestSemCancelReleases(t *testing.T) {
	s := newPrioSem(1, 1)
	if err := s.Acquire(context.Background(), core.PriorityLive); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ch := acquireAsync(s, ctx, core.PriorityLive)
	waitQueued(t, s, core.PriorityLive, 1)
	cancel()
	if err := <-ch; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if st := s.stats(); st.Waiting[core.PriorityLive] != 0 || st.InUse != 1 {
		t.Fatalf("stats %+v", st)
	}
	s.Release(core.PriorityLive)
	if !s.TryAcquire(core.PriorityLive) {
		t.Fatal("slot leaked")
	}
	// Already-cancelled context with a free slot still gets the slot
	// (the fast path), but with no free slot fails immediately.
	if err := s.Acquire(ctx, core.PriorityLive); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestSemCancelUnblocksPrefetchBehind(t *testing.T) {
	// A cancelled live waiter must not leave a prefetch waiter stranded.
	s := newPrioSem(2, 1)
	if !s.TryAcquire(core.PriorityLive) || !s.TryAcquire(core.PriorityLive) {
		t.Fatal("setup")
	}
	ctx, cancel := context.WithCancel(context.Background())
	live := acquireAsync(s, ctx, core.PriorityLive)
	waitQueued(t, s, core.PriorityLive, 1)
	pre := acquireAsync(s, context.Background(), core.PriorityPrefetch)
	waitQueued(t, s, core.PriorityPrefetch, 1)
	cancel()
	<-live
	s.Release(core.PriorityLive)
	select {
	case err := <-pre:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("prefetch stranded")
	}
}

func TestSemClose(t *testing.T) {
	s := newPrioSem(1, 1)
	_ = s.Acquire(context.Background(), core.PriorityLive)
	ch := acquireAsync(s, context.Background(), core.PriorityReadahead)
	waitQueued(t, s, core.PriorityReadahead, 1)
	s.Close()
	if err := <-ch; !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("err = %v", err)
	}
	if err := s.Acquire(context.Background(), core.PriorityLive); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("err = %v", err)
	}
	s.Release(core.PriorityLive) // holders may still release
}

func TestSemStress(t *testing.T) {
	s := newPrioSem(3, 1)
	var mu sync.Mutex
	inUse, prefetching, maxIn, maxPre := 0, 0, 0, 0
	var wg sync.WaitGroup
	for i := 0; i < 60; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := core.Priority(i % 3)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(i%7)*time.Millisecond+time.Millisecond)
			defer cancel()
			if i%5 == 0 {
				ctx = context.Background()
			}
			if err := s.Acquire(ctx, p); err != nil {
				return
			}
			mu.Lock()
			inUse++
			if p == core.PriorityPrefetch {
				prefetching++
			}
			maxIn, maxPre = max(maxIn, inUse), max(maxPre, prefetching)
			mu.Unlock()
			time.Sleep(time.Millisecond)
			mu.Lock()
			inUse--
			if p == core.PriorityPrefetch {
				prefetching--
			}
			mu.Unlock()
			s.Release(p)
		}(i)
	}
	wg.Wait()
	if maxIn > 3 || maxPre > 1 {
		t.Fatalf("max in use %d, max prefetch %d", maxIn, maxPre)
	}
	if st := s.stats(); st.InUse != 0 || st.Waiting != [3]int{} {
		t.Fatalf("leak: %+v", st)
	}
}

func TestSemReservedLiveCombinedBackgroundAndResize(t *testing.T) {
	s := newPrioSem(4, 4)
	s.reserveLive = true
	if !s.TryAcquire(core.PriorityPrefetch) || !s.TryAcquire(core.PriorityReadahead) || !s.TryAcquire(core.PriorityReadahead) {
		t.Fatal("background capacity unavailable")
	}
	if s.TryAcquire(core.PriorityReadahead) || s.TryAcquire(core.PriorityPrefetch) {
		t.Fatal("background consumed live reservation")
	}
	if !s.TryAcquire(core.PriorityLive) {
		t.Fatal("live reservation unavailable")
	}
	s.setSize(2, 1)
	s.Release(core.PriorityReadahead)
	s.Release(core.PriorityReadahead)
	if s.TryAcquire(core.PriorityReadahead) {
		t.Fatal("shrink ignored combined background limit")
	}
	s.Release(core.PriorityLive)
	if !s.TryAcquire(core.PriorityLive) {
		t.Fatal("shrink lost reserved slot")
	}
	s.Release(core.PriorityLive)
	s.Release(core.PriorityPrefetch)
	s.setSize(1, 1)
	if !s.TryAcquire(core.PriorityReadahead) {
		t.Fatal("single connection cannot make progress")
	}
	queued := acquireAsync(s, context.Background(), core.PriorityReadahead)
	waitQueued(t, s, core.PriorityReadahead, 1)
	s.setSize(3, 1)
	if err := <-queued; err != nil {
		t.Fatal(err)
	}
	if !s.TryAcquire(core.PriorityLive) {
		t.Fatal("growth failed to retain reservation")
	}
	s.Release(core.PriorityLive)
	s.Release(core.PriorityReadahead)
	s.Release(core.PriorityReadahead)
}

func TestPrioSemAvailableDoesNotMutateState(t *testing.T) {
	type semSnapshot struct {
		inUse, nonLive, prefetching int
		waiting                     [numPriorities]int
		closed                      bool
	}
	snapshot := func(s *prioSem) semSnapshot {
		s.mu.Lock()
		defer s.mu.Unlock()
		snap := semSnapshot{inUse: s.inUse, nonLive: s.nonLive, prefetching: s.prefetching, closed: s.closed}
		for p := range s.waiters {
			snap.waiting[p] = s.waiters[p].Len()
		}
		return snap
	}
	check := func(t *testing.T, s *prioSem, prio core.Priority, want int) {
		t.Helper()
		before := snapshot(s)
		if got := s.available(prio, time.Time{}); got != want {
			t.Fatalf("available(%d) = %d, want %d", prio, got, want)
		}
		if after := snapshot(s); after != before {
			t.Fatalf("available(%d) mutated state: before %+v, after %+v", prio, before, after)
		}
	}

	s := newPrioSem(4, 1)
	s.reserveLive = true
	if !s.TryAcquire(core.PriorityLive) || !s.TryAcquire(core.PriorityPrefetch) {
		t.Fatal("setup acquires should succeed")
	}
	// inUse=2, nonLive=1, prefetching=1 (at its cap).
	check(t, s, core.PriorityLive, 2)
	check(t, s, core.PriorityReadahead, 2)
	check(t, s, core.PriorityPrefetch, 0)
	s.Release(core.PriorityPrefetch)
	s.Release(core.PriorityLive)

	// mustQueue: a queued waiter makes every priority report 0, and the
	// probe must not disturb the waiter or the counters.
	s = newPrioSem(1, 1)
	if err := s.Acquire(context.Background(), core.PriorityLive); err != nil {
		t.Fatal(err)
	}
	queued := acquireAsync(s, context.Background(), core.PriorityReadahead)
	waitQueued(t, s, core.PriorityReadahead, 1)
	for _, prio := range []core.Priority{core.PriorityLive, core.PriorityReadahead, core.PriorityPrefetch} {
		check(t, s, prio, 0)
	}

	// Closed: still 0 and still no mutation.
	s.Close()
	if err := <-queued; !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("queued waiter err = %v, want ErrPoolClosed", err)
	}
	for _, prio := range []core.Priority{core.PriorityLive, core.PriorityReadahead, core.PriorityPrefetch} {
		check(t, s, prio, 0)
	}
	s.Release(core.PriorityLive) // holders may still release
}
