package resolver

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

// validateRanked validates candidates in rank order with a sliding window:
// two checks stay in flight, and when one fails the next candidate starts at
// once instead of after the rest of its batch. The best-ranked candidate
// (the "lead") wins when healthy; a later healthy candidate may win while
// the lead is still being checked, after rankedSourceGrace when the two are
// equivalent under the owner's existing quality and score policy.
// A failed lead hands leadership to the next candidate, whose own grace
// starts when its check started. A lead still unanswered after its grace
// stops holding a window slot for one equivalent backup: a slow-failing
// lead must not delay the only checks that could overtake it.
// After two confirmed failures (missing articles or an unhealthy sample;
// never a timeout or other transient error) the window widens to four for
// candidates equivalent to the lead, still launched strictly in rank order:
// a run of dead posts must not hold the release that plays behind them.
func (s *Service) validateRanked(ctx context.Context, cands []candidate, tr *Trace) (int, []byte, error) {
	ctx, cancelBudget := context.WithTimeout(ctx, validationBudget)
	defer cancelBudget()
	workCtx, cancelWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		cancelWork()
		wg.Wait()
	}()
	margin := s.resolverCfg().SourceScoreMargin
	if margin <= 0 {
		margin = defaultSourceScoreMargin
	}
	const width, wideWidth = 2, 4
	limit := min(len(cands), maxHealthAttempts)
	results := make(chan validation, limit)
	startedAt := make([]time.Time, limit)
	done := make([]*validation, limit)
	next, inflight, confirmed := 0, 0, 0
	launch := func() {
		i := next
		next++
		inflight++
		startedAt[i] = time.Now()
		if prepared := cands[i].prepared; prepared != nil && !prepared.started.IsZero() {
			// Discovery already spent part (or all) of this source's grace.
			// Reusing the check must not restart that grace period.
			startedAt[i] = prepared.started
		}
		if gs := cands[i].graceStart; !gs.IsZero() && gs.Before(startedAt[i]) {
			// Descriptor discovery for this source overran the grace while
			// an equivalent was already healthy; that time counts too.
			startedAt[i] = gs
		}
		s.launchValidation(&wg, workCtx, cands[i], i, results)
	}
	// stalled reports whether the lead is still checking past its grace,
	// so that a healthy equivalent backup would already be allowed to win.
	stalled := func(lead int) bool {
		return lead < next && done[lead] == nil && time.Since(startedAt[lead]) >= rankedSourceGrace
	}
	// capacity is how many checks may run before cands[next] starts. Extra
	// slots only ever go to a candidate that could overtake the lead
	// (equivalentHedge): one while the lead is stalled, up to wideWidth in
	// all once confirmed failures show the leading releases are dying.
	capacity := func(lead int) int {
		switch {
		case !equivalentHedge(cands[lead], cands[next], margin):
			return width
		case confirmed >= 2:
			return wideWidth
		case stalled(lead):
			return width + 1
		}
		return width
	}
	// fill keeps width checks running, but a verified lead runs alone:
	// reusing a known working release must not start speculative downloads.
	fill := func(lead int) {
		// Once a healthy backup is ready, wait for the preferred source or
		// its grace period instead of spending more indexer/provider calls.
		for i := lead; i < next; i++ {
			if v := done[i]; v != nil && v.healthy && v.err == nil {
				return
			}
		}
		for next < limit {
			if next > lead && lead < limit && cands[lead].verified && done[lead] == nil {
				return
			}
			// A candidate that cannot overtake the lead waits for a free
			// slot, and nothing ranked below it starts first.
			if inflight >= capacity(lead) {
				return
			}
			launch()
		}
	}
	var transient error
	record := func(v validation) {
		inflight--
		done[v.index] = &v
		ct := &tr.Candidates[cands[v.index].trace]
		ct.Attempted = true
		ct.FetchDuration = v.fetchDuration
		ct.HealthDuration = v.healthDuration
		ct.HealthChecked = v.healthChecked
		ct.Healthy = v.healthy && v.err == nil
		if ct.Healthy {
			return
		}
		ct.Rejected = true
		if v.err != nil {
			ct.Reason = v.err.Error()
		} else {
			ct.Reason = "health: unhealthy"
		}
		if v.err != nil && !permanentFailure(v.err) {
			transient = errors.Join(transient, v.err)
		} else {
			confirmed++
			s.noteFailedHealth(cands[v.index].rel.ID)
			s.recordReputation(ctx, cands[v.index].rel.Indexer, false)
		}
	}
	win := func(v validation, lead int) (int, []byte, error) {
		if v.index != lead {
			ct := &tr.Candidates[cands[v.index].trace]
			ct.Route = strings.TrimPrefix(strings.TrimSpace(ct.Route+"; healthy equivalent after preferred source exceeded grace"), "; ")
			tr.Candidates[cands[lead].trace].Attempted = true
		}
		if v.healthChecked && !v.healthCached {
			s.markVerified(ctx, cands[v.index].rel.ID)
			s.noteStartup(cands[v.index].rel, max(v.fetchDuration+v.healthDuration, time.Microsecond))
		}
		s.recordReputation(ctx, cands[v.index].rel.Indexer, true)
		return v.index, v.nzb, nil
	}
	lead := 0
	fill(lead)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		// Consume completed checks before choosing an alternative, so a
		// buffered preferred result wins even when the grace timer also fires.
	drain:
		for {
			select {
			case v := <-results:
				if ctx.Err() != nil {
					return -1, nil, ctx.Err()
				}
				record(v)
			default:
				break drain
			}
		}
		if ctx.Err() != nil {
			return -1, nil, ctx.Err()
		}
		// Settle everything already known, in rank order.
		for lead < limit && done[lead] != nil {
			if done[lead].healthy && done[lead].err == nil {
				return win(*done[lead], lead)
			}
			lead++
		}
		if lead >= limit {
			break
		}
		fill(lead)
		// A healthy later candidate may overtake a lead still checking.
		age := time.Since(startedAt[lead])
		var wait time.Duration = -1
		for k := lead + 1; k < limit; k++ {
			v := done[k]
			if v == nil || !v.healthy || v.err != nil {
				continue
			}
			switch {
			case equivalentHedge(cands[lead], cands[k], margin):
				if age >= rankedSourceGrace {
					return win(*v, lead)
				}
				if left := rankedSourceGrace - age; wait < 0 || left < wait {
					wait = left
				}
			}
		}
		// Wake when the lead's grace ends if an equivalent backup could then
		// take its slot.
		if lead < next && done[lead] == nil && next < limit && inflight == width && equivalentHedge(cands[lead], cands[next], margin) {
			if left := rankedSourceGrace - age; left > 0 && (wait < 0 || left < wait) {
				wait = left
			}
		}
		var tick <-chan time.Time
		if wait >= 0 {
			timer.Reset(wait)
			tick = timer.C
		}
		select {
		case v := <-results:
			if ctx.Err() != nil {
				return -1, nil, ctx.Err()
			}
			record(v)
		case <-tick:
		case <-ctx.Done():
			return -1, nil, ctx.Err()
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
	if transient == nil && len(cands) > limit {
		return -1, nil, errHealthBudget
	}
	return -1, nil, transient
}
