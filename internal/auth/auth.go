// Package auth implements core.Auth: it maps the X-Plex-Token a Plex app
// presents to an Ether user, and keeps the users table with per-user
// settings (stream limit, content rating cap, languages, sections).
//
// A token is recognised, in order, as
//
//  1. the server's own token (the owner),
//  2. a server access token synced from plex.tv for the owner or a friend,
//  3. any other plex.tv token, validated against plex.tv (GET /api/v2/user)
//     and checked for access to this server (owner id, synced friend, or
//     the server in that token's resources list).
//
// Results of (3) are cached (positive and negative) so plex.tv is asked at
// most once per token per TTL; during a plex.tv outage a previously valid
// token keeps working for StaleTTL.
package auth

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/braedonsaunders/ether/internal/clusterbus"
	"github.com/braedonsaunders/ether/internal/config"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/flight"
	"github.com/braedonsaunders/ether/internal/plextv"
	"github.com/braedonsaunders/ether/internal/store"
)

func init() {
	store.Register("auth", 1, `
CREATE TABLE auth_users (
	id                 INTEGER PRIMARY KEY AUTOINCREMENT,
	plex_id            INTEGER NOT NULL UNIQUE,
	uuid               TEXT    NOT NULL DEFAULT '',
	username           TEXT    NOT NULL DEFAULT '',
	title              TEXT    NOT NULL DEFAULT '',
	email              TEXT    NOT NULL DEFAULT '',
	thumb              TEXT    NOT NULL DEFAULT '',
	is_owner           INTEGER NOT NULL DEFAULT 0,
	is_home            INTEGER NOT NULL DEFAULT 0,
	restricted         INTEGER NOT NULL DEFAULT 0,
	active             INTEGER NOT NULL DEFAULT 1,
	origin             TEXT    NOT NULL DEFAULT 'friend',
	plex_sections      TEXT    NOT NULL DEFAULT '[]',
	stream_limit       INTEGER NOT NULL DEFAULT 0,
	max_content_rating TEXT    NOT NULL DEFAULT '',
	subtitle_languages TEXT    NOT NULL DEFAULT '[]',
	audio_languages    TEXT    NOT NULL DEFAULT '[]',
	allowed_sections   TEXT    NOT NULL DEFAULT '[]',
	created_at         INTEGER NOT NULL,
	updated_at         INTEGER NOT NULL,
	last_seen_at       INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE auth_tokens (
	token_hash TEXT    PRIMARY KEY,
	user_id    INTEGER NOT NULL REFERENCES auth_users(id) ON DELETE CASCADE,
	source     TEXT    NOT NULL,
	created_at INTEGER NOT NULL
);
CREATE INDEX auth_tokens_user ON auth_tokens(user_id);
`)
	// At most one owner, enforced by the database so concurrent SetOwner
	// calls on different nodes cannot both win. Older databases could hold
	// several owner rows; keep the most recently updated one.
	store.Register("auth", 2, `
UPDATE auth_users SET is_owner=0 WHERE is_owner=1 AND id <> (
	SELECT id FROM auth_users WHERE is_owner=1 ORDER BY updated_at DESC, id DESC LIMIT 1);
CREATE UNIQUE INDEX auth_users_one_owner ON auth_users(is_owner) WHERE is_owner=1;
`)
}

// User origins.
const (
	originFriend    = "friend"    // from the plex.tv friend sync
	originValidated = "validated" // first seen through plex.tv validation
)

// Token sources in auth_tokens.
const (
	sourceServer = "server" // the server token (owner)
	sourceFriend = "friend" // synced access tokens (owner's or a friend's)
)

// Validator checks tokens Ether does not already know against plex.tv.
type Validator interface {
	// Account returns the account behind token. An error matching
	// core.ErrUnauthorized means the token is invalid; other errors mean
	// plex.tv could not answer.
	Account(ctx context.Context, token string) (*plextv.Account, error)
	// HasServer reports whether token's resources list includes this
	// server (i.e. the account was granted access).
	HasServer(ctx context.Context, token string) (bool, error)
}

// PlexTVValidator is the production Validator.
type PlexTVValidator struct {
	Client    *plextv.Client
	MachineID string
}

// Account implements Validator.
func (v PlexTVValidator) Account(ctx context.Context, token string) (*plextv.Account, error) {
	return v.Client.User(ctx, token)
}

// HasServer implements Validator.
func (v PlexTVValidator) HasServer(ctx context.Context, token string) (bool, error) {
	_, err := v.Client.FindServer(ctx, token, v.MachineID)
	if errors.Is(err, core.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// Options configures the Service.
type Options struct {
	// Validator checks unknown tokens; nil means only server and synced
	// tokens are accepted.
	Validator Validator
	// CacheTTL is how long a plex.tv-validated token is trusted (15m).
	CacheTTL time.Duration
	// NegativeTTL is how long a rejected token is remembered (2m).
	NegativeTTL time.Duration
	// StaleTTL lets a validated token keep working this long past CacheTTL
	// while plex.tv is unreachable (24h).
	StaleTTL time.Duration
	// MaxCacheEntries bounds the token caches (10000).
	MaxCacheEntries int
	// MaxConcurrentValidations bounds distinct unknown tokens being checked
	// with plex.tv (16). Excess requests fail fast; cached tokens still work.
	MaxConcurrentValidations int
	Logger                   *slog.Logger
	Now                      func() time.Time
}

type cacheEntry struct {
	userID  int64
	expires time.Time
}

// Service implements core.Auth and plextv.UserSync.
type Service struct {
	db  *sql.DB
	o   Options
	log *slog.Logger

	mu          sync.RWMutex
	users       map[int64]*row  // by local id
	byPlex      map[int64]int64 // plex id -> local id
	tokens      map[string]int64
	pos         map[string]cacheEntry
	neg         map[string]time.Time
	inflight    flight.Group[string, *core.User]
	ownerPlexID int64

	membershipMu sync.Mutex // serializes validation writes with authoritative revocation
	reloadMu     sync.Mutex // orders database snapshots through publication; acquired before mu
	seenMu       sync.Mutex
	seen         map[int64]time.Time // pending last_seen_at writes

	invalMu sync.Mutex
	inval   *clusterbus.Invalidator // nil on a standalone server
}

type row struct {
	core.User
	Active       bool
	Origin       string
	Home         bool
	Restricted   bool
	PlexSections []int
	LastSeen     time.Time
}

var (
	_ core.Auth       = (*Service)(nil)
	_ plextv.UserSync = (*Service)(nil)
)

// New loads users and known tokens from db (migrated with store.Migrate).
func New(ctx context.Context, db *sql.DB, o Options) (*Service, error) {
	if db == nil {
		return nil, errors.New("auth: db required")
	}
	if o.CacheTTL <= 0 {
		o.CacheTTL = 15 * time.Minute
	}
	if o.NegativeTTL <= 0 {
		o.NegativeTTL = 2 * time.Minute
	}
	if o.StaleTTL <= 0 {
		o.StaleTTL = 24 * time.Hour
	}
	if o.MaxCacheEntries <= 0 {
		o.MaxCacheEntries = 10000
	}
	if o.MaxConcurrentValidations <= 0 {
		o.MaxConcurrentValidations = 16
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	s := &Service{db: db, o: o, log: o.Logger.With("component", "auth"),
		pos: map[string]cacheEntry{}, neg: map[string]time.Time{},
		seen: map[int64]time.Time{}}
	if err := s.reload(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// HashToken is how tokens are stored: never in plain text.
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

const userCols = `id, plex_id, uuid, username, title, email, thumb, is_owner, is_home, restricted, active, origin,
	plex_sections, stream_limit, max_content_rating, subtitle_languages, audio_languages, allowed_sections, last_seen_at`

func scanRow(sc interface{ Scan(...any) error }) (*row, error) {
	var r row
	var isOwner, home, restricted, active int
	var plexSecs, subs, auds, secs string
	var lastSeen int64
	if err := sc.Scan(&r.ID, &r.PlexID, &r.UUID, &r.Username, &r.Title, &r.Email, &r.Thumb, &isOwner, &home,
		&restricted, &active, &r.Origin, &plexSecs, &r.StreamLimit, &r.MaxContentRating, &subs, &auds, &secs, &lastSeen); err != nil {
		return nil, err
	}
	r.IsOwner, r.Home, r.Restricted, r.Active = isOwner == 1, home == 1, restricted == 1, active == 1
	_ = json.Unmarshal([]byte(plexSecs), &r.PlexSections)
	_ = json.Unmarshal([]byte(subs), &r.SubtitleLanguages)
	_ = json.Unmarshal([]byte(auds), &r.AudioLanguages)
	_ = json.Unmarshal([]byte(secs), &r.AllowedSections)
	if lastSeen > 0 {
		r.LastSeen = time.Unix(lastSeen, 0)
	}
	return &r, nil
}

// SetBus connects the cluster bus (nil disconnects): after every write this
// service tells the other nodes to reload users and tokens, and it reloads
// when they write. A standalone server never calls it.
func (s *Service) SetBus(bus clusterbus.Bus) {
	iv := clusterbus.NewInvalidator(bus, "auth", s.log, s.applyRemote)
	s.invalMu.Lock()
	old := s.inval
	s.inval = iv
	s.invalMu.Unlock()
	old.Close()
}

// Close disconnects the cluster bus.
func (s *Service) Close() { s.SetBus(nil) }

// notify tells other nodes this one changed users or tokens. hint is
// "" (users/tokens changed), "caches" (drop every cached validation) or
// "token:<hash>" (forget one token's cached validation).
func (s *Service) notify(hint string) {
	s.invalMu.Lock()
	iv := s.inval
	s.invalMu.Unlock()
	iv.Notify(hint)
}

// applyRemote follows another node's write: reload users and tokens (which
// also drops cached validations of users who lost access) and forget
// negative results, which may predate access granted elsewhere.
func (s *Service) applyRemote(ctx context.Context, hints []string, all bool) {
	if err := s.reload(ctx); err != nil {
		s.log.Warn("auth: reload after another node's change", "err", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.neg = map[string]time.Time{}
	if all {
		s.pos = map[string]cacheEntry{}
		return
	}
	for _, h := range hints {
		switch {
		case h == "caches":
			s.pos = map[string]cacheEntry{}
		case strings.HasPrefix(h, "token:"):
			delete(s.pos, strings.TrimPrefix(h, "token:"))
		}
	}
}

// reload rebuilds the in-memory users and token maps from the database.
func (s *Service) reload(ctx context.Context) error {
	// An older reload must publish before a newer one reads the database.
	// Otherwise it can restore revoked users and tokens after SyncFriends
	// has already published their removal and returned successfully.
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()
	rows, err := s.db.QueryContext(ctx, `SELECT `+userCols+` FROM auth_users`)
	if err != nil {
		return fmt.Errorf("auth: load users: %w", err)
	}
	users := map[int64]*row{}
	byPlex := map[int64]int64{}
	var owner int64
	for rows.Next() {
		r, err := scanRow(rows)
		if err != nil {
			rows.Close()
			return err
		}
		users[r.ID] = r
		byPlex[r.PlexID] = r.ID
		if r.IsOwner {
			owner = r.PlexID
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	trows, err := s.db.QueryContext(ctx, `SELECT token_hash, user_id FROM auth_tokens`)
	if err != nil {
		return fmt.Errorf("auth: load tokens: %w", err)
	}
	tokens := map[string]int64{}
	for trows.Next() {
		var h string
		var id int64
		if err := trows.Scan(&h, &id); err != nil {
			trows.Close()
			return err
		}
		tokens[h] = id
	}
	trows.Close()
	if err := trows.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.users, s.byPlex, s.tokens, s.ownerPlexID = users, byPlex, tokens, owner
	// Cached validations may point at users whose state changed.
	for h, e := range s.pos {
		if u := users[e.userID]; u == nil || !u.Active {
			delete(s.pos, h)
		}
	}
	s.mu.Unlock()
	return nil
}

// effective returns the core.User view of a row.
func (r *row) effective() core.User {
	u := r.User
	u.SubtitleLanguages = append([]string{}, r.SubtitleLanguages...)
	u.AudioLanguages = append([]string{}, r.AudioLanguages...)
	u.AllowedSections = append([]int{}, r.AllowedSections...)
	if len(u.AllowedSections) == 0 && len(r.PlexSections) > 0 && !r.IsOwner {
		// No Ether-side restriction: honour the sections the owner shared
		// on plex.tv.
		u.AllowedSections = append([]int{}, r.PlexSections...)
	}
	return u
}

// Authenticate implements core.Auth.
func (s *Service) Authenticate(ctx context.Context, token string) (*core.User, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, core.ErrUnauthorized
	}
	if strings.HasPrefix(token, delegationPrefix) {
		return s.authenticateDelegation(ctx, token)
	}
	h := HashToken(token)
	now := s.o.Now()

	s.mu.RLock()
	if id, ok := s.tokens[h]; ok {
		u := s.activeUser(id)
		s.mu.RUnlock()
		if u != nil {
			s.touch(u.ID, now)
			return u, nil
		}
		return nil, core.ErrUnauthorized
	}
	if e, ok := s.pos[h]; ok && now.Before(e.expires) {
		u := s.activeUser(e.userID)
		s.mu.RUnlock()
		if u != nil {
			s.touch(u.ID, now)
			return u, nil
		}
		return nil, core.ErrUnauthorized
	}
	if exp, ok := s.neg[h]; ok && now.Before(exp) {
		s.mu.RUnlock()
		return nil, core.ErrUnauthorized
	}
	s.mu.RUnlock()

	if s.o.Validator == nil {
		return nil, core.ErrUnauthorized
	}
	return s.validate(ctx, token, h)
}

// activeUser returns a copy of an active user; callers hold s.mu.
func (s *Service) activeUser(id int64) *core.User {
	r := s.users[id]
	if r == nil || !r.Active {
		return nil
	}
	u := r.effective()
	return &u
}

// validate asks plex.tv, collapsing concurrent requests for one token.
func (s *Service) validate(ctx context.Context, token, h string) (*core.User, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !s.inflight.Has(h) && s.inflight.Len() >= s.o.MaxConcurrentValidations {
		return s.stale(h, errors.New("auth: token validation capacity reached"))
	}
	// Detached, so one impatient client does not fail the others waiting
	// on the same token.
	return s.inflight.DoDetached(ctx, h, func(ctx context.Context) (*core.User, error) {
		ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		return s.validateOnce(ctx, token, h)
	})
}

func (s *Service) validateOnce(ctx context.Context, token, h string) (*core.User, error) {
	acct, err := s.o.Validator.Account(ctx, token)
	if err != nil {
		if errors.Is(err, core.ErrUnauthorized) {
			s.reject(h)
			return nil, core.ErrUnauthorized
		}
		return s.stale(h, err)
	}
	// Membership changes must remain available while plex.tv is slow. Only
	// local membership decisions and writes hold this lock, never network I/O.
	s.membershipMu.Lock()
	defer s.membershipMu.Unlock()
	s.mu.RLock()
	owner := s.ownerPlexID
	localID, known := s.byPlex[acct.ID]
	var knownFriend, revoked bool
	if known {
		revoked = s.users[localID] != nil && !s.users[localID].Active
		knownFriend = s.users[localID] != nil && s.users[localID].Active && s.users[localID].Origin == originFriend
	}
	s.mu.RUnlock()

	if revoked {
		s.reject(h)
		return nil, core.ErrUnauthorized
	}
	switch {
	case owner != 0 && acct.ID == owner:
		// The owner signed in on another device.
	case knownFriend:
		// A friend from the last sync, using a personal token.
	default:
		s.membershipMu.Unlock()
		ok, err := s.o.Validator.HasServer(ctx, token)
		s.membershipMu.Lock()
		if err != nil {
			if errors.Is(err, core.ErrUnauthorized) {
				s.reject(h)
				return nil, core.ErrUnauthorized
			}
			return s.stale(h, err)
		}
		if !ok {
			if known {
				if _, err := s.db.ExecContext(ctx, `UPDATE auth_users SET active=0 WHERE id=? AND is_owner=0`, localID); err != nil {
					return nil, err
				}
				if err := s.reload(ctx); err != nil {
					return nil, err
				}
				s.notify("")
			}
			s.log.Info("token without access to this server", "plexId", acct.ID, "username", acct.Username)
			s.reject(h)
			return nil, core.ErrUnauthorized
		}
		// A sync may have revoked this account during HasServer. The upsert
		// preserves active=0 on existing rows; activeUser below then refuses it.
		id, err := s.upsertValidated(ctx, acct)
		if err != nil {
			return nil, err
		}
		localID = id
	}
	if owner != 0 && acct.ID == owner {
		s.mu.RLock()
		localID = s.byPlex[owner]
		s.mu.RUnlock()
	}
	s.mu.Lock()
	s.evictLocked()
	s.pos[h] = cacheEntry{userID: localID, expires: s.o.Now().Add(s.o.CacheTTL)}
	delete(s.neg, h)
	u := s.activeUser(localID)
	s.mu.Unlock()
	if u == nil {
		return nil, core.ErrUnauthorized
	}
	s.touch(u.ID, s.o.Now())
	return u, nil
}

func (s *Service) reject(h string) {
	s.mu.Lock()
	s.evictLocked()
	s.neg[h] = s.o.Now().Add(s.o.NegativeTTL)
	delete(s.pos, h)
	s.mu.Unlock()
}

// stale serves an expired positive entry while plex.tv is unreachable.
func (s *Service) stale(h string, cause error) (*core.User, error) {
	now := s.o.Now()
	s.mu.RLock()
	e, ok := s.pos[h]
	var u *core.User
	if ok && now.Before(e.expires.Add(s.o.StaleTTL)) {
		u = s.activeUser(e.userID)
	}
	s.mu.RUnlock()
	if u != nil {
		s.log.Warn("plex.tv unreachable; using cached token validation", "err", cause)
		return u, nil
	}
	return nil, fmt.Errorf("auth: validate token with plex.tv: %w", cause)
}

// evictLocked bounds the caches; callers hold s.mu for writing.
func (s *Service) evictLocked() {
	if len(s.pos)+len(s.neg) < s.o.MaxCacheEntries {
		return
	}
	now := s.o.Now()
	for h, e := range s.pos {
		if now.After(e.expires.Add(s.o.StaleTTL)) {
			delete(s.pos, h)
		}
	}
	for h, exp := range s.neg {
		if now.After(exp) {
			delete(s.neg, h)
		}
	}
	// Still full: drop arbitrary entries (map order is random).
	for h := range s.neg {
		if len(s.pos)+len(s.neg) < s.o.MaxCacheEntries*3/4 {
			break
		}
		delete(s.neg, h)
	}
	for h := range s.pos {
		if len(s.pos)+len(s.neg) < s.o.MaxCacheEntries*3/4 {
			break
		}
		delete(s.pos, h)
	}
}

// Invalidate forgets cached validations of token.
func (s *Service) Invalidate(token string) {
	h := HashToken(token)
	s.mu.Lock()
	delete(s.pos, h)
	delete(s.neg, h)
	s.mu.Unlock()
	s.notify("token:" + h)
}

// touch records activity; written to the database by FlushSeen.
func (s *Service) touch(id int64, at time.Time) {
	s.seenMu.Lock()
	if at.After(s.seen[id]) {
		s.seen[id] = at
	}
	s.seenMu.Unlock()
}

// FlushSeen persists last-seen times (call periodically and at shutdown).
// Every node flushes its own observations; the update only ever moves
// last_seen_at forward, so nodes flushing in any order keep the latest.
func (s *Service) FlushSeen(ctx context.Context) error {
	s.seenMu.Lock()
	pending := s.seen
	s.seen = map[int64]time.Time{}
	s.seenMu.Unlock()
	for id, at := range pending {
		if _, err := s.db.ExecContext(ctx, `UPDATE auth_users SET last_seen_at=? WHERE id=? AND last_seen_at<?`, at.Unix(), id, at.Unix()); err != nil {
			// Keep this and the remaining observations for the next flush.
			// Authentication may have added newer values in the meantime.
			s.seenMu.Lock()
			for userID, observed := range pending {
				if observed.After(s.seen[userID]) {
					s.seen[userID] = observed
				}
			}
			s.seenMu.Unlock()
			return err
		}
		delete(pending, id)
		s.mu.Lock()
		if r := s.users[id]; r != nil && at.After(r.LastSeen) {
			r.LastSeen = at
		}
		s.mu.Unlock()
	}
	return nil
}

func (s *Service) upsertValidated(ctx context.Context, a *plextv.Account) (int64, error) {
	now := s.o.Now().Unix()
	_, err := s.db.ExecContext(ctx, `
INSERT INTO auth_users (plex_id, uuid, username, title, email, thumb, is_home, restricted, active, origin, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
ON CONFLICT(plex_id) DO UPDATE SET uuid=excluded.uuid, username=excluded.username, title=excluded.title,
	email=excluded.email, thumb=excluded.thumb, updated_at=excluded.updated_at`,
		a.ID, a.UUID, a.Username, a.DisplayName(), a.Email, a.Thumb, b2i(a.Home), b2i(a.Restricted), originValidated, now, now)
	if err != nil {
		return 0, fmt.Errorf("auth: save user: %w", err)
	}
	if err := s.reload(ctx); err != nil {
		return 0, err
	}
	s.notify("")
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.byPlex[a.ID], nil
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// BootstrapOwner creates the owner from config without network access (on
// startup, before plex.tv is reachable) and registers the server token.
func (s *Service) BootstrapOwner(ctx context.Context, p config.Plex) error {
	if p.OwnerPlexID == 0 {
		return nil
	}
	s.mu.RLock()
	id, ok := s.byPlex[p.OwnerPlexID]
	var r *row
	if ok {
		r = s.users[id]
	}
	s.mu.RUnlock()
	acct := plextv.Account{ID: p.OwnerPlexID, Username: p.OwnerUsername, Title: p.OwnerUsername}
	if r != nil {
		// Keep the richer profile already stored.
		acct.UUID, acct.Email, acct.Thumb, acct.Title = r.UUID, r.Email, r.Thumb, r.Title
		if acct.Username == "" {
			acct.Username = r.Username
		}
	}
	return s.SetOwner(ctx, acct, p.Token)
}

// SetOwner implements plextv.UserSync: it makes a the owner and registers
// serverToken as the owner's token. A unique index admits one owner row, so
// when another node changes the owner concurrently the losing transaction is
// retried against the winner's committed state.
func (s *Service) SetOwner(ctx context.Context, a plextv.Account, serverToken string) error {
	if a.ID == 0 {
		return errors.New("auth: owner without plex id")
	}
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if err = s.setOwnerTx(ctx, a, serverToken); err == nil || !isUniqueViolation(err) {
			break
		}
	}
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.pos = map[string]cacheEntry{}
	s.neg = map[string]time.Time{}
	s.mu.Unlock()
	err = s.reload(ctx)
	s.notify("caches")
	return err
}

// isUniqueViolation reports a unique-constraint failure from SQLite or
// PostgreSQL (SQLSTATE 23505).
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	m := err.Error()
	return strings.Contains(m, "UNIQUE constraint failed") || strings.Contains(m, "23505") ||
		strings.Contains(m, "duplicate key value")
}

func (s *Service) setOwnerTx(ctx context.Context, a plextv.Account, serverToken string) error {
	now := s.o.Now().Unix()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE auth_users SET is_owner=0, origin=CASE WHEN origin='owner' THEN 'friend' ELSE origin END,
		active=CASE WHEN is_owner=1 THEN 0 ELSE active END WHERE is_owner=1 AND plex_id<>?`, a.ID); err != nil {
		return err
	}
	title := a.DisplayName()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO auth_users (plex_id, uuid, username, title, email, thumb, is_owner, is_home, active, origin, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, 1, ?, 1, 'owner', ?, ?)
ON CONFLICT(plex_id) DO UPDATE SET
	uuid=CASE WHEN excluded.uuid<>'' THEN excluded.uuid ELSE auth_users.uuid END,
	username=CASE WHEN excluded.username<>'' THEN excluded.username ELSE auth_users.username END,
	title=CASE WHEN excluded.title<>'' THEN excluded.title ELSE auth_users.title END,
	email=CASE WHEN excluded.email<>'' THEN excluded.email ELSE auth_users.email END,
	thumb=CASE WHEN excluded.thumb<>'' THEN excluded.thumb ELSE auth_users.thumb END,
	is_owner=1, active=1, origin='owner', updated_at=excluded.updated_at`,
		a.ID, a.UUID, a.Username, title, a.Email, a.Thumb, b2i(a.Home), now, now); err != nil {
		return fmt.Errorf("auth: save owner: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM auth_tokens WHERE source=?`, sourceServer); err != nil {
		return err
	}
	if serverToken != "" {
		if _, err := tx.ExecContext(ctx, `INSERT INTO auth_tokens (token_hash, user_id, source, created_at)
			SELECT CAST(? AS TEXT), id, CAST(? AS TEXT), CAST(? AS BIGINT) FROM auth_users WHERE plex_id=?
			ON CONFLICT(token_hash) DO UPDATE SET user_id=excluded.user_id, source=excluded.source, created_at=excluded.created_at`,
			HashToken(serverToken), sourceServer, now, a.ID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// SyncFriends implements plextv.UserSync: it replaces the synced access
// tokens, upserts friends' profiles (never their Ether settings) and
// deactivates every non-owner absent from the authoritative membership list,
// including viewers first discovered through personal-token validation.
func (s *Service) SyncFriends(ctx context.Context, friends []plextv.Friend) error {
	s.membershipMu.Lock()
	defer s.membershipMu.Unlock()
	now := s.o.Now().Unix()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var ownerLocal int64
	if err := tx.QueryRowContext(ctx, `SELECT id FROM auth_users WHERE is_owner=1`).Scan(&ownerLocal); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM auth_tokens WHERE source=?`, sourceFriend); err != nil {
		return err
	}
	// Upsert in plex id order so concurrent syncs on different nodes take
	// row locks in the same order.
	friends = append([]plextv.Friend(nil), friends...)
	sort.SliceStable(friends, func(i, j int) bool { return friends[i].PlexID < friends[j].PlexID })
	keep := map[int64]bool{}
	for _, f := range friends {
		var uid int64
		switch {
		case f.Owned:
			if ownerLocal == 0 {
				continue
			}
			uid = ownerLocal
		case f.PlexID == 0:
			continue
		default:
			secs, _ := json.Marshal(sectionIDs(f.Sections, f.AllLibs))
			if err := tx.QueryRowContext(ctx, `
INSERT INTO auth_users (plex_id, uuid, username, title, email, thumb, is_home, restricted, active, origin, plex_sections, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'friend', ?, ?, ?)
ON CONFLICT(plex_id) DO UPDATE SET
	uuid=CASE WHEN excluded.uuid<>'' THEN excluded.uuid ELSE auth_users.uuid END,
	username=CASE WHEN excluded.username<>'' THEN excluded.username ELSE auth_users.username END,
	title=CASE WHEN CAST(? AS TEXT)<>'' THEN CAST(? AS TEXT) ELSE auth_users.title END,
	email=CASE WHEN excluded.email<>'' THEN excluded.email ELSE auth_users.email END,
	thumb=CASE WHEN excluded.thumb<>'' THEN excluded.thumb ELSE auth_users.thumb END,
	is_home=excluded.is_home, restricted=excluded.restricted,
	active=CASE WHEN auth_users.is_owner=1 THEN 1 ELSE excluded.active END,
	origin=CASE WHEN auth_users.origin='owner' THEN auth_users.origin ELSE 'friend' END,
	plex_sections=excluded.plex_sections, updated_at=excluded.updated_at
RETURNING id`,
				f.PlexID, f.UUID, f.Username, f.DisplayName(), f.Email, f.Thumb, b2i(f.Home), b2i(f.Restricted),
				b2i(!f.Pending), string(secs), now, now, f.Title, f.Title).Scan(&uid); err != nil {
				return fmt.Errorf("auth: save friend %d: %w", f.PlexID, err)
			}
			if f.Pending {
				continue // invited but not accepted: no access yet
			}
			keep[uid] = true
		}
		if f.Token == "" {
			continue // Membership is authoritative even without a shared token.
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO auth_tokens (token_hash, user_id, source, created_at) VALUES (?, ?, ?, ?)
			ON CONFLICT(token_hash) DO UPDATE SET user_id=excluded.user_id, source=excluded.source, created_at=excluded.created_at`,
			HashToken(f.Token), uid, sourceFriend, now); err != nil {
			return err
		}
	}
	// Friends no longer in the list lost access.
	rows, err := tx.QueryContext(ctx, `SELECT id FROM auth_users WHERE is_owner=0 AND active=1`)
	if err != nil {
		return err
	}
	var gone []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		if !keep[id] {
			gone = append(gone, id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range gone {
		if _, err := tx.ExecContext(ctx, `UPDATE auth_users SET active=0, updated_at=? WHERE id=?`, now, id); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if len(gone) > 0 {
		s.log.Info("friends lost access", "count", len(gone))
	}
	err = s.reload(ctx)
	s.notify("")
	return err
}

// sectionIDs converts plex.tv section keys ("1", "/library/sections/1") to
// ints; nil when everything is shared.
func sectionIDs(keys []string, all bool) []int {
	if all {
		return []int{}
	}
	out := []int{}
	for _, k := range keys {
		k = strings.TrimRight(k, "/")
		if i := strings.LastIndex(k, "/"); i >= 0 {
			k = k[i+1:]
		}
		if n, err := strconv.Atoi(k); err == nil {
			out = append(out, n)
		}
	}
	return out
}

// Users implements core.Auth: active users, owner first, then by name.
func (s *Service) Users(ctx context.Context) ([]core.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]core.User, 0, len(s.users))
	for _, r := range s.users {
		if r.Active {
			out = append(out, r.effective())
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].IsOwner != out[j].IsOwner {
			return out[i].IsOwner
		}
		ti, tj := strings.ToLower(out[i].Title), strings.ToLower(out[j].Title)
		if ti != tj {
			return ti < tj
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

// ActiveUser returns a current access snapshot, including for stream grants.
func (s *Service) ActiveUser(ctx context.Context, id int64) (*core.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if u := s.activeUser(id); u != nil {
		return u, nil
	}
	return nil, core.ErrUnauthorized
}

// UserInfo is a user plus bookkeeping for the admin UI.
type UserInfo struct {
	core.User
	Active       bool      `json:"active"`
	Origin       string    `json:"origin"`
	Home         bool      `json:"home"`
	Restricted   bool      `json:"restricted"`
	PlexSections []int     `json:"plexSections"`
	LastSeen     time.Time `json:"lastSeen,omitempty"`
}

// AllUsers lists every known user including those who lost access.
func (s *Service) AllUsers(ctx context.Context) ([]UserInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]UserInfo, 0, len(s.users))
	for _, r := range s.users {
		out = append(out, UserInfo{User: r.effective(), Active: r.Active, Origin: r.Origin, Home: r.Home,
			Restricted: r.Restricted, PlexSections: append([]int{}, r.PlexSections...), LastSeen: r.LastSeen})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// User returns one user by local id (active or not).
func (s *Service) User(ctx context.Context, id int64) (*core.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r := s.users[id]
	if r == nil {
		return nil, core.ErrNotFound
	}
	u := r.effective()
	return &u, nil
}

// Owner implements core.Auth.
func (s *Service) Owner(ctx context.Context) (*core.User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.ownerPlexID == 0 {
		return nil, core.ErrNotFound
	}
	r := s.users[s.byPlex[s.ownerPlexID]]
	if r == nil {
		return nil, core.ErrNotFound
	}
	u := r.effective()
	return &u, nil
}

// UpdateUser implements core.Auth. It changes only per-user settings:
// stream limit, content rating cap, languages and allowed sections.
// Identity fields come from plex.tv and are ignored.
func (s *Service) UpdateUser(ctx context.Context, u core.User) error {
	if u.StreamLimit < 0 {
		return fmt.Errorf("auth: stream limit must be >= 0")
	}
	norm := func(xs []string) []string {
		out := []string{}
		seen := map[string]bool{}
		for _, x := range xs {
			x = strings.ToLower(strings.TrimSpace(x))
			if x != "" && !seen[x] {
				seen[x] = true
				out = append(out, x)
			}
		}
		return out
	}
	subs, _ := json.Marshal(norm(u.SubtitleLanguages))
	auds, _ := json.Marshal(norm(u.AudioLanguages))
	secs := u.AllowedSections
	if secs == nil {
		secs = []int{}
	}
	secJSON, _ := json.Marshal(secs)
	res, err := s.db.ExecContext(ctx, `UPDATE auth_users SET stream_limit=?, max_content_rating=?, subtitle_languages=?,
		audio_languages=?, allowed_sections=?, updated_at=? WHERE id=?`,
		u.StreamLimit, strings.TrimSpace(u.MaxContentRating), string(subs), string(auds), string(secJSON), s.o.Now().Unix(), u.ID)
	if err != nil {
		return fmt.Errorf("auth: update user: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return core.ErrNotFound
	}
	err = s.reload(ctx)
	s.notify("")
	return err
}
