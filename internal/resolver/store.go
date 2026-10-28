package resolver

import (
	"context"
	"database/sql"
	"strconv"
	"time"

	"github.com/braedonsaunders/ether/internal/store"
)

func init() {
	store.Register("resolver", 1, `
CREATE TABLE resolver_bad (
  release_id TEXT PRIMARY KEY,
  reason TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL
);
CREATE TABLE resolver_availability (
  rating_key INTEGER PRIMARY KEY,
  available INTEGER NOT NULL,
  checked_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL
);
CREATE TABLE resolver_reputation (
  indexer TEXT PRIMARY KEY,
  successes INTEGER NOT NULL DEFAULT 0,
  failures INTEGER NOT NULL DEFAULT 0
);`)
}

type reputation struct{ successes, failures int }

func (r reputation) score() float64 {
	total := r.successes + r.failures
	if total < 3 {
		return 0
	}
	rate := float64(r.successes) / float64(total)
	return (rate - 0.5) * 10
}

func (s *Service) loadPersisted(ctx context.Context) {
	if s.db == nil {
		return
	}
	// Owner blocks (manual) never expire; automatic marks are loaded only
	// while inside badRetention of when they were last marked.
	rows, err := s.db.QueryContext(ctx, `SELECT release_id, reason, updated_at, manual FROM resolver_bad WHERE manual=1 OR updated_at>=?`, s.now().Add(-badRetention).Unix())
	if err == nil {
		s.mu.Lock()
		for rows.Next() {
			var id, reason string
			var at int64
			var manual int
			if rows.Scan(&id, &reason, &at, &manual) == nil {
				s.bad[id] = badMark{reason: reason, at: time.Unix(at, 0), manual: manual == 1}
			}
		}
		s.mu.Unlock()
		if err := rows.Err(); err != nil {
			s.log.Debug("resolver blocks not fully loaded", "err", err)
		}
		rows.Close()
	}
	s.loadPins(ctx)
	rrows, err := s.db.QueryContext(ctx, `SELECT indexer, successes, failures FROM resolver_reputation`)
	if err == nil {
		s.mu.Lock()
		for rrows.Next() {
			var name string
			var rep reputation
			if rrows.Scan(&name, &rep.successes, &rep.failures) == nil {
				s.reps[name] = rep
			}
		}
		s.mu.Unlock()
		if err := rrows.Err(); err != nil {
			s.log.Debug("resolver reputations not fully loaded", "err", err)
		}
		rrows.Close()
	}
}

// persistBad upserts a blocklist row. An automatic mark never downgrades
// an owner block: the owner's reason, source and manual flag survive it.
func (s *Service) persistBad(ctx context.Context, e BlockEntry) error {
	if s.db == nil {
		return nil
	}
	now := s.now().Unix()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO resolver_bad(release_id, reason, created_at, name, indexer, rating_key, title_name, source, manual, updated_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(release_id) DO UPDATE SET
		   reason=CASE WHEN resolver_bad.manual=1 AND excluded.manual=0 THEN resolver_bad.reason ELSE excluded.reason END,
		   name=CASE WHEN excluded.name<>'' THEN excluded.name ELSE resolver_bad.name END,
		   indexer=CASE WHEN excluded.indexer<>'' THEN excluded.indexer ELSE resolver_bad.indexer END,
		   rating_key=CASE WHEN excluded.rating_key<>0 THEN excluded.rating_key ELSE resolver_bad.rating_key END,
		   title_name=CASE WHEN excluded.title_name<>'' THEN excluded.title_name ELSE resolver_bad.title_name END,
		   source=CASE WHEN excluded.manual=1 THEN excluded.source ELSE resolver_bad.source END,
		   manual=CASE WHEN excluded.manual=1 THEN 1 ELSE resolver_bad.manual END,
		   updated_at=excluded.updated_at`,
		e.ReleaseID, e.Reason, now, e.Name, e.Indexer, int64(e.TitleKey), e.TitleName, e.Source, boolInt(e.Manual), now)
	if err == nil {
		s.notify("bad:" + e.ReleaseID)
	}
	return err
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (s *Service) recordReputation(ctx context.Context, indexer string, success bool) {
	if indexer == "" {
		return
	}
	s.mu.Lock()
	rep := s.reps[indexer]
	if success {
		rep.successes++
	} else {
		rep.failures++
	}
	s.reps[indexer] = rep
	s.mu.Unlock()
	if s.db != nil {
		col := "failures"
		if success {
			col = "successes"
		}
		if _, err := s.db.ExecContext(ctx,
			`INSERT INTO resolver_reputation(indexer, `+col+`) VALUES (?,1)
			 ON CONFLICT(indexer) DO UPDATE SET `+col+`=resolver_reputation.`+col+`+1`, indexer); err == nil {
			s.notify("reps")
		}
	}
}

type availEntry struct {
	checked   time.Time
	available bool
	expires   time.Time
}

func (s *Service) setAvailability(ctx context.Context, key int64, available bool, ttl time.Duration) {
	now := s.now()
	exp := now.Add(ttl)
	s.mu.Lock()
	boundMap(s.avail, maxAvailEntries, func(e availEntry) bool { return now.After(e.expires) })
	s.avail[key] = availEntry{available: available, expires: exp, checked: now}
	s.mu.Unlock()
	if s.db != nil {
		v := 0
		if available {
			v = 1
		}
		if _, err := s.db.ExecContext(ctx,
			`INSERT INTO resolver_availability(rating_key, available, checked_at, expires_at) VALUES (?,?,?,?)
			 ON CONFLICT(rating_key) DO UPDATE SET available=excluded.available, checked_at=excluded.checked_at, expires_at=excluded.expires_at`,
			key, v, s.now().Unix(), exp.Unix()); err == nil {
			s.notify("avail:" + strconv.FormatInt(key, 10))
		}
	}
}

func (s *Service) getAvailability(ctx context.Context, key int64) (availEntry, bool) {
	s.mu.RLock()
	e, ok := s.avail[key]
	s.mu.RUnlock()
	if ok {
		if s.now().After(e.expires) {
			return availEntry{}, false
		}
		return e, true
	}
	if s.db != nil {
		var v int
		var exp, checked int64
		err := s.db.QueryRowContext(ctx,
			`SELECT available, expires_at,checked_at FROM resolver_availability WHERE rating_key=?`, key).Scan(&v, &exp, &checked)
		if err == nil {
			e := availEntry{available: v == 1, expires: time.Unix(exp, 0), checked: time.Unix(checked, 0)}
			if s.now().After(e.expires) {
				return availEntry{}, false
			}
			now := s.now()
			s.mu.Lock()
			boundMap(s.avail, maxAvailEntries, func(e availEntry) bool { return now.After(e.expires) })
			s.avail[key] = e
			s.mu.Unlock()
			return e, true
		} else if err != sql.ErrNoRows {
			s.log.Debug("resolver availability lookup failed", "err", err)
		}
	}
	return availEntry{}, false
}
