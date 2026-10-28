package resolver

import (
	"context"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/store"
)

const failedHealthTTL = 2 * time.Minute

// Failed sources are skipped briefly, so retries reach lower-ranked releases
// instead of probing the same DMCA'd articles again. This is not MarkBad: a new
// provider, a repair or a recovered feed can make the source usable later.
func (s *Service) noteFailedHealth(releaseID string) {
	if releaseID == "" {
		return
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failedHealth == nil {
		s.failedHealth = make(map[string]time.Time)
	}
	if len(s.failedHealth) >= 10_000 {
		for id, at := range s.failedHealth {
			if now.Sub(at) >= failedHealthTTL {
				delete(s.failedHealth, id)
			}
		}
		if len(s.failedHealth) >= 10_000 {
			clear(s.failedHealth)
		}
	}
	s.failedHealth[releaseID] = now
}

func (s *Service) recentlyFailedHealth(releaseID string) bool {
	s.mu.RLock()
	at, ok := s.failedHealth[releaseID]
	s.mu.RUnlock()
	return ok && s.now().Sub(at) < failedHealthTTL
}

func init() {
	store.Register("resolver", 2, `
CREATE TABLE resolver_verified (
  release_id TEXT PRIMARY KEY,
  checked_at INTEGER NOT NULL
);`)
}

// checkHealth runs the health checker unless the release passed a check
// within healthTTL. Health checks dominate time-to-playable, and a release
// that was fine an hour ago almost always still is; if it isn't, playback
// fails over mid-stream and MarkBad drops it from the cache.
func (s *Service) checkHealth(ctx context.Context, releaseID string, nzb []byte) (bool, error) {
	ok, _, err := s.checkHealthCached(ctx, releaseID, nzb)
	return ok, err
}

// checkHealthCached is checkHealth that also reports whether the answer
// came from the verified cache rather than a check that just ran.
func (s *Service) checkHealthCached(ctx context.Context, releaseID string, nzb []byte) (ok, cached bool, err error) {
	if s.health == nil {
		return true, true, nil
	}
	if s.recentlyVerified(ctx, releaseID) {
		return true, true, nil
	}
	ok, err = core.CheckReleaseHealth(ctx, s.health, releaseID, nzb)
	if ok && err == nil && ctx.Value(speculativeHealthKey{}) == nil {
		s.markVerified(ctx, releaseID)
	}
	return ok, false, err
}

// markVerified records a passed health check for the verified cache.
func (s *Service) markVerified(ctx context.Context, releaseID string) {
	if s.healthTTL <= 0 || releaseID == "" {
		return
	}
	now := s.now()
	s.mu.Lock()
	s.boundVerifiedLocked(now)
	s.verified[releaseID] = now
	s.mu.Unlock()
	if s.db != nil {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO resolver_verified(release_id, checked_at) VALUES (?, ?)
			ON CONFLICT(release_id) DO UPDATE SET checked_at=excluded.checked_at`, releaseID, now.Unix()); err != nil {
			s.log.Debug("resolver verified check not saved", "release", releaseID, "err", err)
		}
	}
}

func (s *Service) recentlyVerified(ctx context.Context, releaseID string) bool {
	if s.healthTTL <= 0 || releaseID == "" {
		return false
	}
	s.mu.RLock()
	at, ok := s.verified[releaseID]
	s.mu.RUnlock()
	if !ok && s.db != nil {
		var unix int64
		if err := s.db.QueryRowContext(ctx, `SELECT checked_at FROM resolver_verified WHERE release_id=?`, releaseID).Scan(&unix); err == nil {
			at, ok = time.Unix(unix, 0), true
			s.mu.Lock()
			s.boundVerifiedLocked(s.now())
			s.verified[releaseID] = at
			s.mu.Unlock()
		}
	}
	return ok && s.now().Sub(at) < s.healthTTL
}

func (s *Service) forgetVerified(ctx context.Context, releaseID string) {
	if s.invalidated != nil {
		s.invalidated(ctx, releaseID)
	}
	s.mu.Lock()
	delete(s.verified, releaseID)
	s.mu.Unlock()
	if s.db != nil {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM resolver_verified WHERE release_id=?`, releaseID); err != nil {
			s.log.Debug("resolver verified check not forgotten", "release", releaseID, "err", err)
		}
	}
}

// boundVerifiedLocked keeps the in-memory verified map below its cap; the
// table remains the source of truth, so evicted entries are re-read on
// demand. Callers hold s.mu.
func (s *Service) boundVerifiedLocked(now time.Time) {
	ttl := s.healthTTL
	boundMap(s.verified, maxVerifiedEntries, func(at time.Time) bool { return ttl <= 0 || now.Sub(at) >= ttl })
}
