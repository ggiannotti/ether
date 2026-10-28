package collections

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/store"
)

func init() {
	store.Register("collections", 2, `
CREATE TABLE collections_specs (
  id INTEGER PRIMARY KEY,
  slug TEXT NOT NULL DEFAULT '',
  builtin INTEGER NOT NULL DEFAULT 0,
  owner_user_id INTEGER NOT NULL DEFAULT 0,
  spec_json TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL DEFAULT 0,
  last_built_at INTEGER NOT NULL DEFAULT 0,
  item_count INTEGER NOT NULL DEFAULT 0,
  warnings_json TEXT NOT NULL DEFAULT '[]',
  build_hash TEXT NOT NULL DEFAULT '',
  changed_at INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX collections_specs_slug ON collections_specs(slug);
CREATE TABLE collections_members (
  spec_id INTEGER NOT NULL,
  pos INTEGER NOT NULL,
  key INTEGER NOT NULL,
  PRIMARY KEY (spec_id, pos)
);
CREATE TABLE collections_names (
  kind TEXT NOT NULL,
  query TEXT NOT NULL,
  id INTEGER NOT NULL DEFAULT 0,
  name TEXT NOT NULL DEFAULT '',
  at INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (kind, query)
);
CREATE TABLE collections_external (
  source TEXT NOT NULL,
  ext TEXT NOT NULL,
  key INTEGER NOT NULL DEFAULT 0,
  at INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (source, ext)
);
CREATE TABLE collections_meta (k TEXT PRIMARY KEY, v TEXT NOT NULL DEFAULT '');
`)
	// shared=0 marks a member of a match-"any" collection that met none of
	// the shared rules: it is shown only to viewers a per-user rule admits.
	store.Register("collections", 3, `ALTER TABLE collections_members ADD COLUMN shared INTEGER NOT NULL DEFAULT 1;`)
	// Builds stored before v3 mark every member shared; rebuild them all so
	// match-"any" collections with per-viewer rules serve correctly.
	store.Register("collections", 4, `UPDATE collections_specs SET build_hash='';`)
}

// builtinBase keeps shipped collections' ids far above owner ids.
const builtinBase int64 = 900_000

// specRow is one persisted collection.
type specRow struct {
	id        int64
	slug      string
	builtin   bool
	owner     int64
	spec      core.CollectionSpec
	enabled   bool
	createdAt time.Time
	updatedAt time.Time
	lastBuilt time.Time
	itemCount int
	warnings  []string
	buildHash string
	changedAt time.Time // last build whose members differed from the one before
}

func (r *specRow) key() core.RatingKey { return core.SmartKey(r.id) }

// stamp is when what Plex shows last changed: an edit, or a build with
// different members. Rebuilding an unchanged collection keeps the stamp,
// so clients keep their cached artwork.
func (r *specRow) stamp() time.Time {
	if r.updatedAt.After(r.changedAt) {
		return r.updatedAt
	}
	return r.changedAt
}

// publicID is the id clients use: the slug for built-ins, the number otherwise.
func (r *specRow) publicID() string {
	if r.slug != "" {
		return r.slug
	}
	return strconv.FormatInt(r.id, 10)
}

// view is the spec as the API returns it, with server-maintained fields.
func (r *specRow) view() core.CollectionSpec {
	s := cloneSpec(r.spec)
	s.ID = r.publicID()
	s.BuiltIn = r.builtin
	s.OwnerUserID = r.owner
	s.Enabled = r.enabled
	s.RatingKey = r.key().String()
	s.ItemCount = r.itemCount
	s.Warnings = append([]string(nil), r.warnings...)
	if !r.updatedAt.IsZero() {
		t := r.updatedAt.UTC()
		s.UpdatedAt = &t
	}
	if !r.lastBuilt.IsZero() {
		t := r.lastBuilt.UTC()
		s.LastBuiltAt = &t
	} else {
		s.LastBuiltAt = nil
	}
	return s
}

const specCols = `id,slug,builtin,owner_user_id,spec_json,enabled,created_at,updated_at,last_built_at,item_count,warnings_json,build_hash,changed_at`

func scanSpecRow(sc interface{ Scan(...any) error }) (*specRow, error) {
	var r specRow
	var builtin, enabled int
	var specJSON, warnJSON string
	var created, updated, built, changed int64
	if err := sc.Scan(&r.id, &r.slug, &builtin, &r.owner, &specJSON, &enabled, &created, &updated, &built,
		&r.itemCount, &warnJSON, &r.buildHash, &changed); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(specJSON), &r.spec); err != nil {
		return nil, fmt.Errorf("collections: spec %d: %w", r.id, err)
	}
	Normalize(&r.spec)
	r.builtin = builtin != 0
	r.enabled = enabled != 0
	if created > 0 {
		r.createdAt = time.Unix(created, 0).UTC()
	}
	if updated > 0 {
		r.updatedAt = time.Unix(updated, 0).UTC()
	}
	if built > 0 {
		r.lastBuilt = time.Unix(built, 0).UTC()
	}
	if changed > 0 {
		r.changedAt = time.Unix(changed, 0).UTC()
	}
	_ = json.Unmarshal([]byte(warnJSON), &r.warnings)
	return &r, nil
}

func (s *Service) loadRows(ctx context.Context) ([]*specRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+specCols+` FROM collections_specs ORDER BY builtin ASC, id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*specRow
	for rows.Next() {
		r, err := scanSpecRow(rows)
		if err != nil {
			s.log.Warn("collections: skipping unreadable collection", "err", err)
			continue
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// rowByID finds a row by public id (number or built-in slug).
func (s *Service) rowByID(ctx context.Context, id string) (*specRow, error) {
	id = strings.TrimSpace(id)
	var row *sql.Row
	if n, err := strconv.ParseInt(id, 10, 64); err == nil {
		row = s.db.QueryRowContext(ctx, `SELECT `+specCols+` FROM collections_specs WHERE id=?`, n)
	} else if id != "" {
		row = s.db.QueryRowContext(ctx, `SELECT `+specCols+` FROM collections_specs WHERE slug=? AND builtin=1`, id)
	} else {
		return nil, core.ErrNotFound
	}
	r, err := scanSpecRow(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, core.ErrNotFound
	}
	return r, err
}

func (s *Service) rowByKey(ctx context.Context, key core.RatingKey) (*specRow, error) {
	if key.Kind() != core.KindSmart {
		return nil, core.ErrNotFound
	}
	return s.rowByID(ctx, strconv.FormatInt(key.TMDB(), 10))
}

func (s *Service) nextID(ctx context.Context, tx *sql.Tx) (int64, error) { return nextSpecID(ctx, tx) }

// nextSpecID allocates an owner collection id. Touching the counter row
// before reading it takes its row lock, so creators on different nodes
// allocate one at a time and never hand out the same id.
func nextSpecID(ctx context.Context, tx *sql.Tx) (int64, error) {
	if _, err := tx.ExecContext(ctx, `INSERT INTO collections_meta(k,v) VALUES('next_id','0') ON CONFLICT(k) DO NOTHING`); err != nil {
		return 0, err
	}
	var v string
	if err := tx.QueryRowContext(ctx, `UPDATE collections_meta SET v=v WHERE k='next_id' RETURNING v`).Scan(&v); err != nil {
		return 0, err
	}
	next, _ := strconv.ParseInt(v, 10, 64)
	var maxID int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(id),0) FROM collections_specs WHERE id<?`, builtinBase).Scan(&maxID); err != nil {
		return 0, err
	}
	if next <= maxID {
		next = maxID + 1
	}
	if next >= builtinBase {
		return 0, fmt.Errorf("collections: id space exhausted")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE collections_meta SET v=? WHERE k='next_id'`, strconv.FormatInt(next+1, 10)); err != nil {
		return 0, err
	}
	return next, nil
}

func specJSON(sp core.CollectionSpec) (string, error) {
	// Server-maintained fields are columns, not part of the document.
	sp.ID, sp.UpdatedAt, sp.LastBuiltAt, sp.ItemCount = "", nil, nil, 0
	sp.BuiltIn, sp.RatingKey, sp.Warnings, sp.OwnerUserID = false, "", nil, 0
	b, err := json.Marshal(sp)
	return string(b), err
}

func (s *Service) insertRow(ctx context.Context, sp core.CollectionSpec, owner int64) (*specRow, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	id, err := s.insertSpecTx(ctx, tx, sp, owner)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return s.rowByID(ctx, strconv.FormatInt(id, 10))
}

// insertSpecTx stores a new owner collection inside tx and returns its id.
func (s *Service) insertSpecTx(ctx context.Context, tx *sql.Tx, sp core.CollectionSpec, owner int64) (int64, error) {
	js, err := specJSON(sp)
	if err != nil {
		return 0, err
	}
	id, err := s.nextID(ctx, tx)
	if err != nil {
		return 0, err
	}
	now := s.now().Unix()
	if _, err := tx.ExecContext(ctx, `INSERT INTO collections_specs(id,slug,builtin,owner_user_id,spec_json,enabled,created_at,updated_at)
		VALUES(?, '', 0, ?, ?, ?, ?, ?)`, id, owner, js, boolInt(sp.Enabled), now, now); err != nil {
		return 0, err
	}
	return id, nil
}

func (s *Service) updateRow(ctx context.Context, id int64, sp core.CollectionSpec) error {
	js, err := specJSON(sp)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `UPDATE collections_specs SET spec_json=?, enabled=?, updated_at=?, build_hash='' WHERE id=?`,
		js, boolInt(sp.Enabled), s.now().Unix(), id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return core.ErrNotFound
	}
	return nil
}

func (s *Service) deleteRow(ctx context.Context, id int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `DELETE FROM collections_specs WHERE id=? AND builtin=0`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return core.ErrNotFound
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM collections_members WHERE spec_id=?`, id); err != nil {
		return err
	}
	return tx.Commit()
}

// saveBuild persists a build's members and report.
func (s *Service) saveBuild(ctx context.Context, id int64, keys []core.RatingKey, unshared map[core.RatingKey]bool, itemCount int, warnings []string, hash string, at time.Time) error {
	wj, _ := json.Marshal(warnings)
	if warnings == nil {
		wj = []byte("[]")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM collections_members WHERE spec_id=?`, id); err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO collections_members(spec_id,pos,key,shared) VALUES(?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for i, k := range keys {
		if _, err := stmt.ExecContext(ctx, id, i, int64(k), boolInt(!unshared[k])); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE collections_specs SET changed_at=CASE WHEN build_hash<>? THEN ? ELSE changed_at END,
		last_built_at=?, item_count=?, warnings_json=?, build_hash=? WHERE id=?`,
		hash, at.Unix(), at.Unix(), itemCount, string(wj), hash, id); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) loadMembers(ctx context.Context, id int64) ([]core.RatingKey, map[core.RatingKey]bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT key,shared FROM collections_members WHERE spec_id=? ORDER BY pos`, id)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var out []core.RatingKey
	var unshared map[core.RatingKey]bool
	for rows.Next() {
		var k int64
		var shared int
		if err := rows.Scan(&k, &shared); err != nil {
			return nil, nil, err
		}
		out = append(out, core.RatingKey(k))
		if shared == 0 {
			if unshared == nil {
				unshared = map[core.RatingKey]bool{}
			}
			unshared[core.RatingKey(k)] = true
		}
	}
	return out, unshared, rows.Err()
}

func (s *Service) meta(ctx context.Context, k string) string {
	var v string
	_ = s.db.QueryRowContext(ctx, `SELECT v FROM collections_meta WHERE k=?`, k).Scan(&v)
	return v
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// specFromFilter converts a filter-based smart collection (core.Filter) to a spec.
func specFromFilter(c core.Collection) core.CollectionSpec {
	f := core.Filter{}
	if c.Filter != nil {
		f = *c.Filter
	}
	typ := c.Type
	if typ == "" {
		typ = f.Type
	}
	sp := core.CollectionSpec{
		Name: c.Title, Description: c.Summary, MediaType: MediaMixed,
		Sources: []core.CollectionSource{{Kind: "catalog"}}, Match: MatchAll,
		Refresh: RefreshLive, Enabled: true, Sort: string(c.Sort), SortDesc: c.Sort != core.SortTitle,
		Artwork: core.CollectionArtwork{Mode: "auto"},
		Targets: core.CollectionTargets{CollectionsTab: true, Users: core.AllUsers()},
	}
	switch typ {
	case core.TypeMovie:
		sp.MediaType = MediaMovie
	case core.TypeShow:
		sp.MediaType = MediaShow
	}
	if sp.Sort == "" {
		sp.Sort = "popularity"
		sp.SortDesc = true
	}
	if c.OwnerUserID != 0 {
		sp.Targets.Users = core.UserTargets{IDs: []int64{c.OwnerUserID}}
	}
	add := func(field, op string, v any) {
		b, _ := json.Marshal(v)
		sp.Rules = append(sp.Rules, core.CollectionRule{Field: field, Op: op, Value: b})
	}
	if len(f.Genres) > 0 {
		add("genre", "in", f.Genres)
	}
	if f.YearFrom > 0 {
		add("year", "gte", f.YearFrom)
	}
	if f.YearTo > 0 {
		add("year", "lte", f.YearTo)
	}
	if f.MinRating > 0 {
		add("rating", "gte", f.MinRating)
	}
	if f.MaxRuntime > 0 {
		add("runtime", "lte", int(f.MaxRuntime/time.Minute))
	}
	if f.MinRuntime > 0 {
		add("runtime", "gte", int(f.MinRuntime/time.Minute))
	}
	if len(f.Languages) > 0 {
		add("language", "in", f.Languages)
	}
	if len(f.Countries) > 0 {
		add("country", "in", f.Countries)
	}
	if len(f.ContentRatings) > 0 {
		add("contentRating", "in", f.ContentRatings)
	}
	if f.MaxContentRating != "" {
		add("contentRating", "lte", f.MaxContentRating)
	}
	if f.Collection > 0 {
		add("collection", "is", f.Collection)
	}
	if f.Person > 0 {
		add("person", "is", f.Person)
	}
	if f.Studio != "" {
		add("studio", "is", f.Studio)
	}
	Normalize(&sp)
	return sp
}
