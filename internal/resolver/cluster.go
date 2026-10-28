package resolver

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/braedonsaunders/ether/internal/clusterbus"
	"github.com/braedonsaunders/ether/internal/core"
)

// Cluster cache coherence. The resolver mirrors resolver_bad and
// resolver_reputation in memory and reads resolver_availability and
// resolver_verified through a cache; when another node writes them it
// publishes a hint and this node drops or reloads the affected entries:
//
//	bad:<release id>   reload that release's bad mark, forget its verification
//	avail:<rating key> forget the cached availability (re-read on demand)
//	reps               reload indexer reputations
//	pin:<rating key>   reload that title's owner pin (pinned, unpinned or
//	                   its last status changed)

// SetBus connects the cluster bus (nil disconnects). A standalone server
// never calls it.
func (s *Service) SetBus(bus clusterbus.Bus) {
	iv := clusterbus.NewInvalidator(bus, "resolver", s.log, s.applyRemote)
	s.invalMu.Lock()
	old := s.inval
	s.inval = iv
	s.invalMu.Unlock()
	old.Close()
}

func (s *Service) notify(hint string) {
	s.invalMu.Lock()
	iv := s.inval
	s.invalMu.Unlock()
	iv.Notify(hint)
}

// notifyPin tells the other nodes a title's pin changed.
func (s *Service) notifyPin(key core.RatingKey) { s.notify("pin:" + strconv.FormatInt(int64(key), 10)) }

func (s *Service) applyRemote(ctx context.Context, hints []string, all bool) {
	if all {
		s.mu.Lock()
		s.bad = map[string]badMark{}
		s.reps = map[string]reputation{}
		s.avail = map[int64]availEntry{}
		s.verified = map[string]time.Time{}
		s.mu.Unlock()
		s.loadPersisted(ctx)
		return
	}
	reps := false
	for _, h := range hints {
		kind, arg, _ := strings.Cut(h, ":")
		switch kind {
		case "bad":
			s.reloadBad(ctx, arg)
		case "avail":
			if k, err := strconv.ParseInt(arg, 10, 64); err == nil {
				s.mu.Lock()
				delete(s.avail, k)
				s.mu.Unlock()
			}
		case "reps":
			reps = true
		case "pin":
			if k, err := strconv.ParseInt(arg, 10, 64); err == nil {
				s.reloadPin(ctx, core.RatingKey(k))
			}
		}
	}
	if reps {
		s.reloadReps(ctx)
	}
}

func (s *Service) reloadBad(ctx context.Context, id string) {
	if s.db == nil || id == "" {
		return
	}
	var reason string
	var at int64
	var manual int
	err := s.db.QueryRowContext(ctx, `SELECT reason, updated_at, manual FROM resolver_bad WHERE release_id=?`, id).Scan(&reason, &at, &manual)
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.verified, id)
	switch {
	case err == nil:
		s.bad[id] = badMark{reason: reason, at: time.Unix(at, 0), manual: manual == 1}
	case errors.Is(err, sql.ErrNoRows):
		delete(s.bad, id) // unblocked on another node
	}
}

func (s *Service) reloadReps(ctx context.Context) {
	if s.db == nil {
		return
	}
	rows, err := s.db.QueryContext(ctx, `SELECT indexer, successes, failures FROM resolver_reputation`)
	if err != nil {
		return
	}
	defer rows.Close()
	reps := map[string]reputation{}
	for rows.Next() {
		var name string
		var rep reputation
		if rows.Scan(&name, &rep.successes, &rep.failures) == nil {
			reps[name] = rep
		}
	}
	if err := rows.Err(); err != nil {
		s.log.Debug("resolver reputations not fully loaded", "err", err)
		return
	}
	s.mu.Lock()
	s.reps = reps
	s.mu.Unlock()
}
