package resolver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/releaseparse"
)

// Owner pins
//
// A pin tells Resolve to try one specific release first for a movie or
// episode, the way a viewer's Play Version pick does (ResolveRequest.Prefer):
// it is fetched and health-checked like any candidate and, when it fails,
// resolution falls back to normal ranking. A failing pin is never deleted;
// its last status is recorded and surfaced to the owner.
//
// Gates. A pin overrides the owner's own policy: quality profile limits
// (height, HDR/DV policy, imported quality gates, size range, minimum
// custom format score) and identity heuristics (title/year matching,
// blocked terms and groups, CAM, sample detection) do not apply, because
// the owner matched the release to the title by hand. It still respects
// what would make playback fail or misbehave regardless of policy:
//   - a passworded torrent release, a blocklisted release, or one already tried in
//     this playback (failover Exclude);
//   - a season pack for an episode (playback cannot pick the episode);
//   - a video codec the client cannot decode (direct play is impossible and
//     offline downloads never transcode);
//   - Dolby Vision without an HDR10 base layer on a client without Dolby
//     Vision (it would play with wrong colours).
//
// Scope. A viewer's explicit Play Version pick wins over a pin, and pins do
// not apply to offline downloads, which use their own compact profile. For
// the version picker the pin is Auto, the release normal resolution tries
// first.

// PinStatus is the outcome of the last time a pin was tried or checked.
type PinStatus string

const (
	PinHealthy     PinStatus = "healthy"     // passed a health check and played
	PinUnhealthy   PinStatus = "unhealthy"   // failed a health check or the NZB is gone
	PinRejected    PinStatus = "rejected"    // a hard gate excluded it for a client
	PinUnavailable PinStatus = "unavailable" // a transient failure (provider outage)
)

// Pin is an owner's release pin for a title.
type Pin struct {
	TitleKey      core.RatingKey `json:"titleKey,string"`
	TitleName     string         `json:"titleName"`
	Release       core.Release   `json:"release"`
	Note          string         `json:"note,omitempty"`
	PinnedAt      time.Time      `json:"pinnedAt"`
	LastStatus    PinStatus      `json:"lastStatus,omitempty"`
	LastReason    string         `json:"lastReason,omitempty"`
	LastCheckedAt time.Time      `json:"lastCheckedAt"`
}

func pinnableKey(k core.RatingKey) bool {
	return k.Valid() && (k.Kind() == core.KindMovie || k.Kind() == core.KindEpisode)
}

// Pin pins rel to a movie or episode, replacing any earlier pin. The full
// release is stored, so the pin works after the release drops out of
// cached search results.
func (s *Service) Pin(ctx context.Context, key core.RatingKey, titleName string, rel core.Release, note string) (Pin, error) {
	if !pinnableKey(key) {
		return Pin{}, ErrNotPinnable
	}
	if rel.ID == "" {
		return Pin{}, fmt.Errorf("resolver: empty release id")
	}
	if s.isBad(rel.ID) {
		return Pin{}, ErrPinBlocked
	}
	if titleName == "" {
		s.con.mu.Lock()
		titleName = s.con.recent[rel.ID].titleName
		s.con.mu.Unlock()
	}
	note = strings.TrimSpace(note)
	p := Pin{TitleKey: key, TitleName: titleName, Release: rel, Note: note, PinnedAt: s.now()}
	if s.db != nil {
		raw, err := json.Marshal(rel)
		if err != nil {
			return Pin{}, err
		}
		_, err = s.db.ExecContext(ctx,
			`INSERT INTO resolver_pins(rating_key, release_id, release_json, title_name, note, pinned_at, last_status, last_reason, last_checked_at)
			 VALUES (?,?,?,?,?,?,'','',0)
			 ON CONFLICT(rating_key) DO UPDATE SET release_id=excluded.release_id, release_json=excluded.release_json,
			   title_name=excluded.title_name, note=excluded.note, pinned_at=excluded.pinned_at,
			   last_status='', last_reason='', last_checked_at=0`,
			int64(key), rel.ID, string(raw), titleName, note, p.PinnedAt.Unix())
		if err != nil {
			return Pin{}, err
		}
	}
	s.con.mu.Lock()
	s.con.pins[key] = p
	s.con.mu.Unlock()
	s.notifyPin(key)
	return p, nil
}

// Unpin removes a title's pin; it reports whether there was one.
func (s *Service) Unpin(ctx context.Context, key core.RatingKey) (bool, error) {
	s.con.mu.Lock()
	_, had := s.con.pins[key]
	delete(s.con.pins, key)
	s.con.mu.Unlock()
	if s.db == nil {
		return had, nil
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM resolver_pins WHERE rating_key=?`, int64(key))
	if err != nil {
		return had, err
	}
	s.notifyPin(key)
	n, _ := res.RowsAffected()
	return had || n > 0, nil
}

// Pins lists every pin, newest first.
func (s *Service) Pins() []Pin {
	s.con.mu.Lock()
	out := make([]Pin, 0, len(s.con.pins))
	for _, p := range s.con.pins {
		out = append(out, p)
	}
	s.con.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if !out[i].PinnedAt.Equal(out[j].PinnedAt) {
			return out[i].PinnedAt.After(out[j].PinnedAt)
		}
		return out[i].TitleKey < out[j].TitleKey
	})
	return out
}

// PinFor returns a title's pin.
func (s *Service) PinFor(key core.RatingKey) (Pin, bool) {
	s.con.mu.Lock()
	defer s.con.mu.Unlock()
	p, ok := s.con.pins[key]
	return p, ok
}

// pinColumns are the resolver_pins columns scanPin reads.
const pinColumns = `rating_key, release_json, title_name, note, pinned_at, last_status, last_reason, last_checked_at`

// scanPin reads one resolver_pins row (pinColumns).
func scanPin(row interface{ Scan(...any) error }) (Pin, error) {
	var key, pinnedAt, checkedAt int64
	var raw, status string
	var p Pin
	if err := row.Scan(&key, &raw, &p.TitleName, &p.Note, &pinnedAt, &status, &p.LastReason, &checkedAt); err != nil {
		return p, err
	}
	p.TitleKey = core.RatingKey(key)
	if err := json.Unmarshal([]byte(raw), &p.Release); err != nil || p.Release.ID == "" {
		return p, fmt.Errorf("resolver pin %d unreadable: %v", key, err)
	}
	p.PinnedAt = time.Unix(pinnedAt, 0)
	p.LastStatus = PinStatus(status)
	if checkedAt > 0 {
		p.LastCheckedAt = time.Unix(checkedAt, 0)
	}
	return p, nil
}

// loadPins replaces the in-memory pins with the stored ones.
func (s *Service) loadPins(ctx context.Context) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+pinColumns+` FROM resolver_pins`)
	if err != nil {
		s.log.Warn("resolver pins unavailable", "err", err)
		return
	}
	defer rows.Close()
	pins := map[core.RatingKey]Pin{}
	for rows.Next() {
		p, err := scanPin(rows)
		if err != nil {
			s.log.Warn("resolver pin unreadable", "err", err)
			continue
		}
		pins[p.TitleKey] = p
	}
	if err := rows.Err(); err != nil {
		s.log.Warn("resolver pins not fully loaded", "err", err)
		return
	}
	s.con.mu.Lock()
	s.con.pins = pins
	s.con.mu.Unlock()
}

// reloadPin re-reads one title's pin after another node changed it.
func (s *Service) reloadPin(ctx context.Context, key core.RatingKey) {
	if s.db == nil || key == 0 {
		return
	}
	p, err := scanPin(s.db.QueryRowContext(ctx, `SELECT `+pinColumns+` FROM resolver_pins WHERE rating_key=?`, int64(key)))
	s.con.mu.Lock()
	defer s.con.mu.Unlock()
	switch {
	case err == nil:
		s.con.pins[key] = p
	case errors.Is(err, sql.ErrNoRows):
		delete(s.con.pins, key) // unpinned on another node
	}
}

// notePin records the outcome of trying a title's pin, when releaseID is
// still the pinned release.
func (s *Service) notePin(ctx context.Context, key core.RatingKey, releaseID string, status PinStatus, reason string) {
	now := s.now()
	s.con.mu.Lock()
	p, ok := s.con.pins[key]
	if ok && p.Release.ID == releaseID {
		p.LastStatus, p.LastReason, p.LastCheckedAt = status, reason, now
		s.con.pins[key] = p
	}
	s.con.mu.Unlock()
	if !ok || s.db == nil {
		return
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE resolver_pins SET last_status=?, last_reason=?, last_checked_at=? WHERE rating_key=? AND release_id=?`,
		string(status), reason, now.Unix(), int64(key), releaseID)
	if err != nil {
		s.log.Debug("resolver pin status not saved", "title", key, "err", err)
		return
	}
	s.notifyPin(key)
}

// notePinsBlocked marks pins of a newly blocklisted release as failing, so
// the Pinned list shows why the pin is being skipped.
func (s *Service) notePinsBlocked(ctx context.Context, releaseID, reason string) {
	s.con.mu.Lock()
	var keys []core.RatingKey
	for k, p := range s.con.pins {
		if p.Release.ID == releaseID {
			keys = append(keys, k)
		}
	}
	s.con.mu.Unlock()
	for _, k := range keys {
		s.notePin(ctx, k, releaseID, PinUnhealthy, "blocklisted: "+reason)
	}
}

// pinForResolve returns the title's pin when it applies to req, else nil
// and, when there is a pin, why it does not apply.
func (s *Service) pinForResolve(req core.ResolveRequest) (*Pin, string) {
	if req.Title == nil {
		return nil, ""
	}
	p, ok := s.PinFor(req.Title.Key)
	if !ok {
		return nil, ""
	}
	switch {
	case req.Prefer != "" && req.Prefer != p.Release.ID:
		return nil, "skipped: viewer picked another version"
	case req.Client.Download:
		return nil, "skipped: downloads use their own profile"
	}
	for _, id := range req.Exclude {
		if id == p.Release.ID {
			return nil, "skipped: already tried in this playback"
		}
	}
	if s.isBad(p.Release.ID) {
		return nil, "skipped: pinned release is blocklisted"
	}
	return &p, ""
}

// pinRejectReason is the gate set for an owner pin (see the package
// comment above): "" to accept.
func (s *Service) pinRejectReason(rel core.Release, sreq core.SearchRequest, caps core.ClientCaps, exclude map[string]bool) string {
	switch {
	case rel.Password && rel.Protocol == core.ProtocolTorrent:
		return "passworded"
	case exclude[rel.ID]:
		return "excluded (already tried)"
	case s.isBad(rel.ID):
		return "marked bad"
	case sreq.Type == core.TypeEpisode && rel.Info.FullSeason && rel.Protocol != core.ProtocolTorrent:
		return "season pack (skipped in v1)"
	case sreq.Type == core.TypeEpisode && rel.Info.FullSeason && !packCoversSeason(rel.Info, sreq.Season):
		return "season/episode mismatch"
	case rel.Info.VideoCodec != "" && len(caps.VideoCodecs) > 0 && !caps.SupportsVideo(rel.Info.VideoCodec):
		return "video codec unsupported by client"
	case rel.Info.DolbyVision && !rel.Info.HDR10 && !caps.DolbyVision && len(caps.VideoCodecs) > 0:
		return "dolby vision without hdr10 fallback on a client without dolby vision"
	}
	return ""
}

// withRelease returns results with rel appended when it is not already
// among them.
func withRelease(results []core.Release, rel core.Release) []core.Release {
	for _, r := range results {
		if r.ID == rel.ID {
			return results
		}
	}
	if rel.Info.Title == "" && rel.Info.Height == 0 && rel.Info.Source == core.SourceUnknown {
		rel.Info = releaseparse.Parse(rel.Name)
	}
	return append(results, rel)
}

// pinFirst moves the pinned candidate to the front: Resolve tries it
// before the ranking.
func pinFirst(cands []candidate, pinID string) []candidate {
	if pinID == "" {
		return cands
	}
	for i, c := range cands {
		if c.rel.ID == pinID {
			out := make([]candidate, 0, len(cands))
			out = append(out, c)
			out = append(out, cands[:i]...)
			return append(out, cands[i+1:]...)
		}
	}
	return cands
}

// candidateReason is a candidate's trace reason, for pin status notes.
func candidateReason(tr *Trace, id string) string {
	for _, c := range tr.Candidates {
		if c.ReleaseID == id {
			if c.Reason != "" {
				return c.Reason
			}
			if c.HealthChecked && !c.Healthy {
				return "health: unhealthy"
			}
		}
	}
	return "not playable for this client"
}
