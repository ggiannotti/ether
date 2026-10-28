package usenet

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
)

const numPriorities = 3

func clampPriority(p core.Priority) core.Priority {
	if p < core.PriorityLive {
		return core.PriorityLive
	}
	if p > core.PriorityPrefetch {
		return core.PriorityPrefetch
	}
	return p
}

// prioSem is a counting semaphore with three strict priority classes.
// Freed slots go to the oldest waiter of the most urgent class. Prefetch
// holders are additionally capped at prefetchMax, and a prefetch request is
// never granted while a live or readahead request is waiting, so background
// work yields to viewers as soon as a slot frees up.
type prioSem struct {
	mu          sync.Mutex
	size        int
	prefetchMax int
	inUse       int
	prefetching int
	nonLive     int
	reserveLive bool                     // connection pools reserve one slot against all speculative work
	waiters     [numPriorities]list.List // of *semWaiter
	closed      bool
}

type semWaiter struct {
	burstUntil time.Time
	prio       core.Priority
	ready      chan struct{} // closed when granted (or the semaphore closed)
	ok         bool          // granted (false + ready closed = semaphore closed)
}

func newPrioSem(size, prefetchMax int) *prioSem {
	if size < 1 {
		size = 1
	}
	if prefetchMax < 1 {
		prefetchMax = 1
	}
	if prefetchMax > size {
		prefetchMax = size
	}
	return &prioSem{size: size, prefetchMax: prefetchMax}
}

// grantableLocked reports whether a request of prio may take a slot now,
// ignoring queue order.
func (s *prioSem) grantableLocked(prio core.Priority) bool {
	return s.grantableBurstLocked(prio, time.Time{})
}

func (s *prioSem) grantableBurstLocked(prio core.Priority, until time.Time) bool {
	if s.inUse >= s.size {
		return false
	}
	if s.reserveLive && s.size >= 2 && prio != core.PriorityLive && s.nonLive >= s.size-1 {
		// Borrow the idle reserved slot only during an explicit short burst,
		// with no live owner or live waiter. Existing transfers are not aborted.
		if prio != core.PriorityReadahead || !time.Now().Before(until) || s.inUse != s.nonLive || s.waiters[core.PriorityLive].Len() != 0 {
			return false
		}
	}
	if prio == core.PriorityPrefetch && s.prefetching >= s.prefetchMax {
		return false
	}
	return true
}

// mustQueueLocked reports whether a new request of prio has to wait behind
// already-queued requests of the same or more urgent priority.
func (s *prioSem) mustQueueLocked(prio core.Priority) bool {
	for p := core.PriorityLive; p <= prio; p++ {
		if s.waiters[p].Len() > 0 {
			return true
		}
	}
	return false
}

func (s *prioSem) takeLocked(prio core.Priority) {
	s.inUse++
	if prio != core.PriorityLive {
		s.nonLive++
	}
	if prio == core.PriorityPrefetch {
		s.prefetching++
	}
}

func (s *prioSem) untakeLocked(prio core.Priority) {
	s.inUse--
	if prio != core.PriorityLive {
		s.nonLive--
	}
	if prio == core.PriorityPrefetch {
		s.prefetching--
	}
}

// available reports how many slots a request of prio could take right now,
// without queuing. The probe takes and returns each counted slot under the
// same lock hold, so the semaphore state is unchanged on return.
func (s *prioSem) available(prio core.Priority, until time.Time) int {
	prio = clampPriority(prio)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.mustQueueLocked(prio) {
		return 0
	}
	n := 0
	for s.grantableBurstLocked(prio, until) {
		s.takeLocked(prio)
		n++
	}
	for range n {
		s.untakeLocked(prio)
	}
	return n
}

// prefetchSlots is how many of size slots prefetch may hold.
func prefetchSlots(size int, fraction float64) int {
	return max(1, int(float64(size)*fraction))
}

// setSize changes the capacity (holders above a reduced size keep their
// slots until they release them). It reports whether the size changed.
func (s *prioSem) setSize(size int, prefetchFraction float64) bool {
	size = max(size, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.size == size {
		return false
	}
	s.size = size
	s.prefetchMax = min(prefetchSlots(size, prefetchFraction), size)
	s.dispatchLocked()
	return true
}

// TryAcquire takes a slot without waiting.
func (s *prioSem) TryAcquire(prio core.Priority) bool {
	prio = clampPriority(prio)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.mustQueueLocked(prio) || !s.grantableLocked(prio) {
		return false
	}
	s.takeLocked(prio)
	return true
}

// Acquire waits for a slot. It returns ctx.Err() if ctx ends first (no slot
// is held in that case) and ErrPoolClosed if the semaphore is closed.
func (s *prioSem) Acquire(ctx context.Context, prio core.Priority) error {
	prio = clampPriority(prio)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrPoolClosed
	}
	if !s.mustQueueLocked(prio) && s.grantableBurstLocked(prio, burstUntil(ctx)) {
		s.takeLocked(prio)
		s.mu.Unlock()
		return nil
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	w := &semWaiter{prio: prio, burstUntil: burstUntil(ctx), ready: make(chan struct{})}
	elem := s.waiters[prio].PushBack(w)
	s.mu.Unlock()

	select {
	case <-w.ready:
		if !w.ok {
			return ErrPoolClosed
		}
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		select {
		case <-w.ready:
			// Granted (or closed) concurrently with cancellation.
			ok := w.ok
			s.mu.Unlock()
			if ok {
				s.Release(prio)
			}
		default:
			s.waiters[prio].Remove(elem)
			// Removing a waiter can unblock others (e.g. a prefetch request
			// that was queued behind this one).
			s.dispatchLocked()
			s.mu.Unlock()
		}
		return ctx.Err()
	}
}

// Release returns a slot taken at prio.
func (s *prioSem) Release(prio core.Priority) {
	prio = clampPriority(prio)
	s.mu.Lock()
	s.untakeLocked(prio)
	if s.inUse < 0 || s.prefetching < 0 || s.nonLive < 0 {
		s.mu.Unlock()
		panic("usenet: semaphore released more than acquired")
	}
	s.dispatchLocked()
	s.mu.Unlock()
}

// dispatchLocked hands free slots to waiters, most urgent class first.
// Prefetch waiters are only served when no live or readahead request waits.
func (s *prioSem) dispatchLocked() {
	if s.closed {
		return
	}
	for p := core.PriorityLive; p <= core.PriorityPrefetch; p++ {
		q := &s.waiters[p]
		for q.Len() > 0 {
			if !s.grantableBurstLocked(p, q.Front().Value.(*semWaiter).burstUntil) {
				break
			}
			w := q.Remove(q.Front()).(*semWaiter)
			s.takeLocked(p)
			w.ok = true
			close(w.ready)
		}
		if q.Len() > 0 {
			// Higher-priority work is still waiting: nothing below it may
			// jump the queue.
			return
		}
	}
}

// Close fails every current and future waiter with ErrPoolClosed. Holders
// may still Release.
func (s *prioSem) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for p := range s.waiters {
		q := &s.waiters[p]
		for q.Len() > 0 {
			w := q.Remove(q.Front()).(*semWaiter)
			close(w.ready)
		}
	}
}

// semStats is a snapshot for statistics.
type semStats struct {
	InUse, Prefetching, Size int
	Waiting                  [numPriorities]int
}

func (s *prioSem) stats() semStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := semStats{InUse: s.inUse, Prefetching: s.prefetching, Size: s.size}
	for p := range s.waiters {
		st.Waiting[p] = s.waiters[p].Len()
	}
	return st
}
