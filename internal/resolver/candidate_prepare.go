package resolver

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
)

type observationTitleKey struct{}

// launchValidation runs one candidate check in its own goroutine: a
// per-candidate timeout over workCtx, reusing a prepared check when present,
// tagging the result with its candidate index and sending it on results.
// The caller owns workCtx cancellation and joining wg; results buffering is
// the caller's choice and is unchanged by this extraction.
func (s *Service) launchValidation(wg *sync.WaitGroup, workCtx context.Context, cand candidate, i int, results chan<- validation) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		checkCtx, cancel := context.WithTimeout(workCtx, candidateBudget)
		defer cancel()
		var v validation
		if cand.prepared != nil {
			v = cand.prepared.wait(checkCtx)
		} else {
			v = s.validateOne(checkCtx, cand.rel)
		}
		v.index = i
		results <- v
	}()
}

func (s *Service) validateOne(ctx context.Context, rel core.Release) validation {
	v := validation{}
	at := time.Now()
	v.nzb, v.err = s.indexer.FetchNZB(ctx, rel)
	v.fetchDuration = time.Since(at)
	if v.err == nil {
		v.healthy = true
		if s.health != nil {
			v.healthChecked = true
			at = time.Now()
			v.healthy, v.healthCached, v.err = s.checkHealthCached(ctx, rel.ID, v.nzb)
			if v.healthy && v.err == nil && !v.healthCached && s.observed != nil {
				if title, ok := ctx.Value(observationTitleKey{}).(*core.Title); ok {
					s.observed(ctx, title, rel)
				}
			}
			v.healthDuration = time.Since(at)
		}
	}
	return v
}

type candidateCheck struct {
	started          time.Time // includes time spent checking during source discovery
	done             chan struct{}
	cancel           context.CancelFunc
	result           validation                       // immutable after done closes
	retrySpeculative func(context.Context) validation // foreground may admit work speculation refused
}

func (c *candidateCheck) wait(ctx context.Context) validation {
	select {
	case <-c.done:
		if c.retrySpeculative != nil && (errors.Is(c.result.err, core.ErrNotCached) || errors.Is(c.result.err, core.ErrBudgetExhausted)) {
			return c.retrySpeculative(ctx)
		}
		return c.result
	case <-ctx.Done():
		return validation{err: ctx.Err()}
	}
}

// pendingPrep is one descriptor-preparation call running alongside the
// speculative checks. done is buffered so the goroutine can always finish;
// consumed marks that the result was already taken (or drained by close).
type pendingPrep struct {
	cancel   context.CancelFunc
	done     chan []core.Release
	consumed bool
}

// Owned by one Resolve call. Its deferred close cancels and joins every
// speculative check even when another source wins or the client leaves.
type candidateChecks struct {
	all   []*candidateCheck
	preps []*pendingPrep
}

func (c *candidateChecks) start(ctx context.Context, s *Service, rel core.Release) *candidateCheck {
	ctx, cancel := context.WithTimeout(ctx, candidateBudget)
	f := &candidateCheck{started: time.Now(), done: make(chan struct{}), cancel: cancel}
	c.all = append(c.all, f)
	go func() { defer close(f.done); defer cancel(); f.result = s.validateOne(ctx, rel) }()
	return f
}

func (c *candidateChecks) close() {
	for _, f := range c.all {
		f.cancel()
	}
	for _, p := range c.preps {
		p.cancel()
	}
	for _, f := range c.all {
		<-f.done
	}
	for _, p := range c.preps {
		if !p.consumed {
			<-p.done
			p.consumed = true
		}
	}
}
