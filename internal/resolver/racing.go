package resolver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
)

type candidate struct {
	rel        core.Release
	trace      int
	verified   bool
	bandwidth  int             // fits client cap (+1), unknown (0), or exceeds it (-1)
	prepared   *candidateCheck // validation overlapped with descriptor discovery
	graceStart time.Time       // discovery start, set only when preparation overran the grace
}
type validation struct {
	index                         int
	nzb                           []byte
	err                           error
	healthy, healthChecked        bool
	healthCached                  bool // answered by the verified cache, not a check
	fetchDuration, healthDuration time.Duration
}

const (
	validationBudget = 25 * time.Second
	candidateBudget  = 8 * time.Second
)

var errHealthBudget = errors.New("source validation limit reached; more releases remain to try")

// Close cancels and joins optimistic checks. Owners should close before the DB.
func (s *Service) Close() error {
	s.bgMu.Lock()
	s.bgClosed = true
	s.bgCancel()
	s.bgMu.Unlock()
	s.bgWG.Wait()
	s.SetBus(nil)
	return nil
}
func (s *Service) optimisticCheck(r core.Release, nzb []byte) {
	if s.health == nil {
		return
	}
	s.bgMu.Lock()
	if s.bgClosed {
		s.bgMu.Unlock()
		return
	}
	s.bgWG.Add(1)
	s.bgMu.Unlock()
	go func() {
		defer s.bgWG.Done()
		ctx, cancel := context.WithTimeout(s.bgCtx, 30*time.Second)
		defer cancel()
		ok, err := s.checkHealth(ctx, r.ID, nzb)
		if ctx.Err() != nil {
			return
		}
		if (!ok && err == nil) || permanentFailure(err) {
			_ = s.MarkBad(ctx, r.ID, "optimistic background health check failed")
		}
	}()
}

func (s *Service) resolution(key core.RatingKey, cands []candidate, winner int, nzb []byte, tr *Trace) *core.Resolution {
	res := &core.Resolution{Title: key, Release: cands[winner].rel, NZB: nzb, ResolvedAt: s.now()}
	// Failover stays on the same cut first: alternates of the winner's
	// edition come before other cuts (ranked order within each group).
	edition := cands[winner].rel.Info.Edition
	for _, same := range []bool{true, false} {
		for i, c := range cands {
			if i == winner || tr.Candidates[c.trace].Rejected || len(res.Alternates) >= s.maxCandidates() {
				continue
			}
			if (c.rel.Info.Edition == edition) == same {
				res.Alternates = append(res.Alternates, c.rel)
			}
		}
	}
	return res
}

// validateCandidates races bounded groups when enabled. Ranked mode still starts
// two checks together, preferring rank order during a short grace. A healthy
// equivalent can then bypass a stalled preferred source without losing quality.
// Every worker is cancelled and joined before this function returns.
func (s *Service) validateCandidates(ctx context.Context, cands []candidate, tr *Trace, race int) (int, []byte, error) {
	if race <= 0 {
		return s.validateRanked(ctx, cands, tr)
	}
	ctx, cancelBudget := context.WithTimeout(ctx, validationBudget)
	defer cancelBudget()
	width := min(race, 4)
	limit := min(len(cands), maxHealthAttempts)
	var transient error
	for base := 0; base < limit; {
		count := min(width, limit-base)
		if cands[base].verified {
			// Reusing a known working release must not start speculative
			// downloads and health checks for other releases alongside it.
			count = 1
		}
		workCtx, cancel := context.WithCancel(ctx)
		results := make(chan validation, count)
		var wg sync.WaitGroup
		for j := 0; j < count; j++ {
			i := base + j
			s.launchValidation(&wg, workCtx, cands[i], i, results)
		}
		winner := -1
		var nzb []byte
		for j := 0; j < count; j++ {
			v := <-results
			ct := &tr.Candidates[cands[v.index].trace]
			ct.Attempted = true
			ct.FetchDuration = v.fetchDuration
			ct.HealthDuration = v.healthDuration
			ct.HealthChecked = v.healthChecked
			ct.Healthy = v.healthy && v.err == nil
			if ctx.Err() != nil {
				cancel()
				wg.Wait()
				return -1, nil, ctx.Err()
			}
			if ct.Healthy {
				if v.healthChecked && !v.healthCached {
					// A real check always counts, even when a coarse clock
					// (Windows) measured it as taking no time.
					s.noteStartup(cands[v.index].rel, max(v.fetchDuration+v.healthDuration, time.Microsecond))
				}
				s.recordReputation(ctx, cands[v.index].rel.Indexer, true)
				winner = v.index
				nzb = v.nzb
				break
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
				s.noteFailedHealth(cands[v.index].rel.ID)
				s.recordReputation(ctx, cands[v.index].rel.Indexer, false)
			}
		}
		cancel()
		wg.Wait()
		if winner >= 0 {
			return winner, nzb, nil
		}
		base += count
	}
	if transient == nil && len(cands) > limit {
		return -1, nil, errHealthBudget
	}
	return -1, nil, transient
}

// compatibilityScore puts bandwidth-fit direct play ahead of a large source
// that would require a video transcode, while leaving both as failover choices.
func compatibilityScore(r core.Release, c core.ClientCaps, t *core.Title) (float64, string) {
	score := 0.0
	var notes []string
	if r.Info.VideoCodec != "" && len(c.VideoCodecs) > 0 && !c.SupportsVideo(r.Info.VideoCodec) {
		score -= 20
		notes = append(notes, "video transcode required")
	}
	if r.Info.AudioCodec != "" && len(c.AudioCodecs) > 0 {
		if c.SupportsAudio(r.Info.AudioCodec) {
			score += 10
			notes = append(notes, "audio copies")
		} else {
			score -= 15
			notes = append(notes, "audio transcode required")
		}
	}
	if c.MaxBitrateKbps > 0 && r.Size > 0 && t.Runtime > 0 {
		kbps := float64(r.Size) * 8 / t.Runtime.Seconds() / 1000
		// NZB sizes include archive overhead; reserve 10% for instantaneous peaks.
		if kbps*1.1 <= float64(c.MaxBitrateKbps) {
			score += 200
			notes = append(notes, fmt.Sprintf("estimated %.0f kbps fits client cap", kbps))
		} else {
			score -= 200
			notes = append(notes, fmt.Sprintf("estimated %.0f kbps exceeds client cap", kbps))
		}
	}
	if r.Info.Atmos && c.SupportsAudio(r.Info.AudioCodec) {
		score += 5
	}
	return score, strings.Join(notes, "; ")
}

func permanentFailure(err error) bool {
	return errors.Is(err, core.ErrArticleMissing) || errors.Is(err, core.ErrArticleCorrupt) || errors.Is(err, core.ErrReleaseRejected)
}
