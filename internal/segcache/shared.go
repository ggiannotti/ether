package segcache

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/braedonsaunders/ether/internal/store"
)

func init() {
	store.Register("segcache", 1, `
CREATE TABLE segcache_pool (id INTEGER PRIMARY KEY CHECK(id=1), token TEXT NOT NULL, used INTEGER NOT NULL DEFAULT 0);
CREATE TABLE segcache_segments (release_id TEXT NOT NULL, segment INTEGER NOT NULL, file TEXT NOT NULL, size INTEGER NOT NULL, touched INTEGER NOT NULL, PRIMARY KEY(release_id,segment));
CREATE INDEX segcache_segments_age ON segcache_segments(touched);
CREATE TABLE segcache_releases (release_id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT '', size INTEGER NOT NULL DEFAULT 0, plays INTEGER NOT NULL DEFAULT 0, played INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0, segments INTEGER NOT NULL DEFAULT 0, touched INTEGER NOT NULL DEFAULT 0);
CREATE TABLE segcache_pins (release_id TEXT NOT NULL, reason TEXT NOT NULL, owner TEXT NOT NULL, expires INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(release_id,reason,owner));
CREATE TABLE segcache_files (file TEXT PRIMARY KEY, created INTEGER NOT NULL);
`)
}

type sharedCache struct {
	db    *sql.DB
	node  string
	token string
	c     *Cache
	// Guarded by c.mu. Only this process's active transient pins are renewed.
	active map[string]map[string]bool
}

// OpenShared opens a NAS pool. The directory must already exist and be mounted.
// The server database owns the index, pins and accounting: local SQLite for a
// standalone server, shared PostgreSQL for a cluster. Media files have immutable,
// random names. No SQLite database is placed on the NAS.
// Each mount must expose the same marker created by the first pool member.
func OpenShared(dir string, budget int64, db *sql.DB, node string, logger *slog.Logger) (*Cache, error) {
	if db == nil || node == "" {
		return nil, errors.New("shared cache requires a database and node identity")
	}
	if !filepath.IsAbs(dir) {
		return nil, errors.New("shared cache requires an absolute mounted directory")
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("shared cache mount is unavailable: %s", dir)
	}
	if logger == nil {
		logger = slog.Default()
	}
	c := &Cache{dir: dir, logger: logger.With("component", "shared-cache"), budget: budget, now: time.Now}
	s := &sharedCache{db: db, node: node, c: c, active: map[string]map[string]bool{}}
	c.shared = s
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = s.tx(ctx, func(tx *sql.Tx) error {
		var token string
		err := tx.QueryRowContext(ctx, `SELECT token FROM segcache_pool WHERE id=1`).Scan(&token)
		if errors.Is(err, sql.ErrNoRows) {
			token = randomName()
			// Never adopt a directory belonging to another cluster or a local cache.
			if _, e := os.Stat(filepath.Join(dir, "data")); e == nil {
				return errors.New("choose a dedicated empty directory for the shared cache; local caches are not adopted")
			}
			marker := filepath.Join(dir, ".ether-shared-cache")
			f, e := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if e != nil {
				return fmt.Errorf("initialize shared cache marker: %w", e)
			}
			_, e = f.WriteString(token)
			if e == nil {
				e = f.Sync()
			}
			ce := f.Close()
			if e == nil {
				e = ce
			}
			if e != nil {
				return e
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO segcache_pool(id,token) VALUES(1,?)`, token); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		s.token = token
		return s.checkMount()
	})
	if err != nil {
		return nil, err
	}
	c.dataDir = filepath.Join(dir, "shared-data")
	if err = os.MkdirAll(c.dataDir, 0755); err != nil {
		return nil, err
	}
	return c, nil
}
func randomName() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func (s *sharedCache) checkMount() error {
	b, err := os.ReadFile(filepath.Join(s.c.dir, ".ether-shared-cache"))
	if err != nil || string(b) != s.token {
		return errors.New("shared cache mount is unavailable or belongs to a different pool")
	}
	return nil
}
func (s *sharedCache) tx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = store.LockKey(ctx, store.DialectOf(s.db), tx, "segcache:pool"); err != nil {
		return err
	}
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *sharedCache) context() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}
func (s *sharedCache) log(err error) {
	if err != nil {
		s.c.logger.Warn("shared cache operation", "err", err)
	}
}

// logRows logs a rows iteration error with the operation that produced
// the cursor, so a partial read is attributable in the log.
func (s *sharedCache) logRows(op string, err error) {
	if err != nil {
		s.c.logger.Warn("shared cache "+op+" not fully loaded", "err", err)
	}
}
func (s *sharedCache) path(file string) string {
	return filepath.Join(s.c.dataDir, file[:2], file+".seg")
}
func validFile(file string) bool {
	if len(file) != 32 {
		return false
	}
	_, err := hex.DecodeString(file)
	return err == nil
}
func (s *sharedCache) get(id string, seg int) ([]byte, bool) {
	if s.closed() {
		return nil, false
	}
	ctx, cancel := s.context()
	defer cancel()
	var file string
	var size, touched int64
	err := s.db.QueryRowContext(ctx, `SELECT file,size,touched FROM segcache_segments WHERE release_id=? AND segment=?`, id, seg).Scan(&file, &size, &touched)
	if err != nil || !validFile(file) || s.checkMount() != nil {
		return s.miss()
	}
	b, err := readSegment(s.path(file), size)
	if err != nil {
		return s.miss()
	}
	if time.Now().Unix()-touched >= 60 {
		_, _ = s.db.ExecContext(ctx, `UPDATE segcache_segments SET touched=? WHERE release_id=? AND segment=? AND file=?`, time.Now().Unix(), id, seg, file)
		_, _ = s.db.ExecContext(ctx, `UPDATE segcache_releases SET touched=? WHERE release_id=? AND touched<?`, time.Now().Unix(), id, time.Now().Unix()-60)
	}
	s.c.mu.Lock()
	s.c.metrics.Hits++
	s.c.metrics.ServedBytes += int64(len(b))
	s.c.mu.Unlock()
	return b, true
}
func (s *sharedCache) miss() ([]byte, bool) {
	s.c.mu.Lock()
	s.c.metrics.Misses++
	s.c.mu.Unlock()
	return nil, false
}
func (s *sharedCache) has(id string, seg int) bool {
	if s.closed() {
		return false
	}
	ctx, cancel := s.context()
	defer cancel()
	var file string
	if s.db.QueryRowContext(ctx, `SELECT file FROM segcache_segments WHERE release_id=? AND segment=?`, id, seg).Scan(&file) != nil || !validFile(file) || s.checkMount() != nil {
		return false
	}
	_, err := os.Stat(s.path(file))
	return err == nil
}
func (s *sharedCache) put(id string, seg int, b []byte) error {
	if err := validKey(id, seg); err != nil {
		return err
	}
	s.c.mu.Lock()
	closed, off, budget := s.c.closed, s.c.policy.Mode == "off", s.c.budget
	s.c.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if off {
		return nil
	}
	if budget > 0 && int64(len(b)) > budget {
		return ErrTooLarge
	}
	if err := s.checkMount(); err != nil {
		return err
	}
	ctx, cancel := s.context()
	defer cancel()
	file := randomName()
	path := s.path(file)
	// Journal allocations before disk IO so a crash leaves discoverable garbage.
	if _, err := s.db.ExecContext(ctx, `INSERT INTO segcache_files(file,created) VALUES(?,?)`, file, time.Now().Unix()); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	if err := writeSharedSegment(path, b); err != nil {
		return err
	}
	var err error
	var old string
	err = s.tx(ctx, func(tx *sql.Tx) error {
		var size int64
		err := tx.QueryRowContext(ctx, `SELECT file,size FROM segcache_segments WHERE release_id=? AND segment=?`, id, seg).Scan(&old, &size)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO segcache_segments(release_id,segment,file,size,touched) VALUES(?,?,?,?,?) ON CONFLICT(release_id,segment) DO UPDATE SET file=excluded.file,size=excluded.size,touched=excluded.touched`, id, seg, file, len(b), time.Now().Unix()); err != nil {
			return err
		}
		count := 0
		if old == "" {
			count = 1
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO segcache_releases(release_id,bytes,segments,touched) VALUES(?,?,?,?) ON CONFLICT(release_id) DO UPDATE SET bytes=segcache_releases.bytes+excluded.bytes,segments=segcache_releases.segments+excluded.segments,touched=excluded.touched`, id, int64(len(b))-size, count, time.Now().Unix()); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE segcache_pool SET used=used+? WHERE id=1`, int64(len(b))-size)
		return err
	})
	if err != nil {
		return err
	} // Never unlink on an ambiguous commit; GC resolves it.
	if old != "" {
		s.unlink(old)
	}
	s.sweep(false)
	return nil
}
func (s *sharedCache) unlink(file string) {
	if !validFile(file) || s.checkMount() != nil {
		return
	}
	// Caller only supplies immutable versions that are no longer referenced.
	if err := os.Remove(s.path(file)); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.log(err)
		return
	}
	ctx, cancel := s.context()
	defer cancel()
	_, err := s.db.ExecContext(ctx, `DELETE FROM segcache_files WHERE file=?`, file)
	s.log(err)
}
func transient(reason string) bool {
	return reason == "playback:live" || reason == "playback:inprogress"
}
func (s *sharedCache) pin(id, reason string, add bool) error {
	if s.closed() {
		return ErrClosed
	}
	if id == "" || reason == "" {
		return errors.New("empty release or pin reason")
	}
	owner := ""
	expires := int64(0)
	if transient(reason) {
		owner = s.node
		expires = 1 // replaced with the database clock inside the transaction
	}
	ctx, cancel := s.context()
	defer cancel()
	err := s.tx(ctx, func(tx *sql.Tx) error {
		var err error
		if owner != "" {
			var now int64
			err = tx.QueryRowContext(ctx, "SELECT "+s.nowSQL()).Scan(&now)
			if err != nil {
				return err
			}
			expires = now + 180
		}
		if add {
			_, err = tx.ExecContext(ctx, `INSERT INTO segcache_pins(release_id,reason,owner,expires) VALUES(?,?,?,?) ON CONFLICT(release_id,reason,owner) DO UPDATE SET expires=excluded.expires`, id, reason, owner, expires)
		} else {
			_, err = tx.ExecContext(ctx, `DELETE FROM segcache_pins WHERE release_id=? AND reason=? AND owner=?`, id, reason, owner)
		}
		return err
	})
	if err == nil && owner != "" {
		s.c.mu.Lock()
		if add {
			if s.active[id] == nil {
				s.active[id] = map[string]bool{}
			}
			s.active[id][reason] = true
		} else {
			delete(s.active[id], reason)
		}
		s.c.mu.Unlock()
	}
	return err
}
func (s *sharedCache) pins() map[string][]string {
	ctx, cancel := s.context()
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT release_id,reason FROM segcache_pins WHERE expires=0 OR expires>`+s.nowSQL()+` ORDER BY reason`)
	if err != nil {
		s.log(err)
		return nil
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var id, reason string
		if rows.Scan(&id, &reason) == nil {
			out[id] = append(out[id], reason)
		}
	}
	s.logRows("pins", rows.Err())
	return out
}
func (s *sharedCache) usage() (int64, int64) {
	ctx, cancel := s.context()
	defer cancel()
	var n int64
	s.log(s.db.QueryRowContext(ctx, `SELECT used FROM segcache_pool WHERE id=1`).Scan(&n))
	s.c.mu.Lock()
	b := s.c.budget
	s.c.mu.Unlock()
	return n, b
}
func (s *sharedCache) releaseBytes(id string) int64 {
	ctx, cancel := s.context()
	defer cancel()
	var n int64
	s.log(s.db.QueryRowContext(ctx, `SELECT COALESCE((SELECT bytes FROM segcache_releases WHERE release_id=?),0)`, id).Scan(&n))
	return n
}
func (s *sharedCache) describe(id, title string, size int64, play bool) {
	s.c.mu.Lock()
	off := s.c.closed || (play && s.c.policy.Mode == "off")
	s.c.mu.Unlock()
	if off {
		return
	}
	ctx, cancel := s.context()
	defer cancel()
	n, at := 0, int64(0)
	if play {
		n = 1
		at = time.Now().Unix()
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO segcache_releases(release_id,title,size,plays,played) VALUES(?,?,?,?,?) ON CONFLICT(release_id) DO UPDATE SET title=excluded.title,size=excluded.size,plays=segcache_releases.plays+excluded.plays,played=CASE WHEN excluded.played>0 THEN excluded.played ELSE segcache_releases.played END`, id, title, size, n, at)
	s.log(err)
}
func (s *sharedCache) releases() []ReleaseUsage {
	ctx, cancel := s.context()
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT release_id,bytes,segments,touched,title,size,plays,played FROM segcache_releases WHERE segments>0 ORDER BY touched DESC`)
	if err != nil {
		s.log(err)
		return nil
	}
	var out []ReleaseUsage
	for rows.Next() {
		var r ReleaseUsage
		var touched, played int64
		if err = rows.Scan(&r.ReleaseID, &r.Bytes, &r.Segments, &touched, &r.Title, &r.Size, &r.Plays, &played); err != nil {
			break
		}
		r.LastUsed = time.Unix(touched, 0)
		if played > 0 {
			r.LastPlayed = time.Unix(played, 0)
		}
		out = append(out, r)
	}
	s.logRows("releases", rows.Err())
	rows.Close()
	s.log(err)
	pins := s.pins()
	for i := range out {
		out[i].Pins = pins[out[i].ReleaseID]
		delete(pins, out[i].ReleaseID)
	}
	for id, p := range pins {
		out = append(out, ReleaseUsage{ReleaseID: id, Pins: p})
	}
	return out
}
func (s *sharedCache) remove(id string) error { return s.evictWhere(id, false) }
func (s *sharedCache) sweep(expire bool)      { s.log(s.evictWhere("", expire)) }
func (s *sharedCache) evictWhere(id string, expire bool) error {
	if err := s.checkMount(); err != nil {
		return err
	}
	s.c.mu.Lock()
	budget, p, closed := s.c.budget, s.c.policy, s.c.closed
	s.c.mu.Unlock()
	if closed {
		return ErrClosed
	}
	prot, _ := s.c.protected()
	ctx, cancel := s.context()
	defer cancel()
	var files []string
	var reclaimed int64
	err := s.tx(ctx, func(tx *sql.Tx) error {
		var used int64
		if err := tx.QueryRowContext(ctx, `SELECT used FROM segcache_pool WHERE id=1`).Scan(&used); err != nil {
			return err
		}
		if id == "" && !expire && (budget <= 0 || used <= budget) {
			return nil
		}
		// Segment-granular eviction, with a bounded batch per sweep. Shared pins
		// are checked in the same transaction as removal and accounting.
		q := `SELECT s.release_id,s.segment,s.file,s.size,s.touched,COALESCE(r.plays,0),COALESCE(r.played,0) FROM segcache_segments s LEFT JOIN segcache_releases r ON r.release_id=s.release_id WHERE NOT EXISTS(SELECT 1 FROM segcache_pins p WHERE p.release_id=s.release_id AND (p.expires=0 OR p.expires>` + s.nowSQL() + `))`
		args := []any{}
		if id != "" {
			q += ` AND s.release_id=?`
			args = append(args, id)
		}
		q += ` ORDER BY s.touched LIMIT 256`
		rows, err := tx.QueryContext(ctx, q, args...)
		if err != nil {
			return err
		}
		type candidate struct {
			id, file                            string
			seg                                 int
			size, touched, plays, played, score int64
		}
		var cs []candidate
		for rows.Next() {
			var v candidate
			if err = rows.Scan(&v.id, &v.seg, &v.file, &v.size, &v.touched, &v.plays, &v.played); err != nil {
				break
			}
			v.score = v.touched
			if p.Mode == "adaptive" && time.Now().Unix()-v.played < 30*86400 {
				v.score += min(max(v.plays-1, 0), 8) * int64(p.ReplayBoost/time.Second)
			}
			cs = append(cs, v)
		}
		re := rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if re != nil {
			return re
		}
		sort.Slice(cs, func(i, j int) bool { return cs[i].score < cs[j].score })
		deltas := map[string][2]int64{}
		for _, v := range cs {
			expired := expire && (p.Mode == "off" || (p.Retention > 0 && time.Since(time.Unix(v.touched, 0)) >= p.Retention))
			if prot[v.id] || (id == "" && !expired && (budget <= 0 || used <= budget)) {
				continue
			}
			if _, err = tx.ExecContext(ctx, `DELETE FROM segcache_segments WHERE release_id=? AND segment=? AND file=?`, v.id, v.seg, v.file); err != nil {
				return err
			}
			d := deltas[v.id]
			d[0] += v.size
			d[1]++
			deltas[v.id] = d
			used -= v.size
			reclaimed += v.size
			files = append(files, v.file)
		}
		for id, d := range deltas {
			if _, err = tx.ExecContext(ctx, `UPDATE segcache_releases SET bytes=bytes-?,segments=segments-? WHERE release_id=?`, d[0], d[1], id); err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(ctx, `UPDATE segcache_pool SET used=? WHERE id=1`, used)
		return err
	})
	if err != nil {
		return err
	}
	for _, f := range files {
		s.unlink(f)
	}
	s.c.mu.Lock()
	s.c.metrics.EvictedBytes += reclaimed
	s.c.mu.Unlock()
	return nil
}
func (s *sharedCache) heartbeat() {
	ctx, cancel := s.context()
	defer cancel()
	s.c.mu.Lock()
	var pins [][2]string
	for id, rs := range s.active {
		for r := range rs {
			pins = append(pins, [2]string{id, r})
		}
	}
	s.c.mu.Unlock()
	for _, p := range pins {
		_, err := s.db.ExecContext(ctx, `UPDATE segcache_pins SET expires=`+s.nowSQL()+`+180 WHERE release_id=? AND reason=? AND owner=?`, p[0], p[1], s.node)
		s.log(err)
	}
	// Immutable abandoned allocations are safe to reclaim. Never scan or delete
	// another node's live temp files, and never recreate an unmounted NAS path.
	if s.checkMount() != nil {
		return
	}
	rows, err := s.db.QueryContext(ctx, `SELECT file FROM segcache_files f WHERE created<? AND NOT EXISTS(SELECT 1 FROM segcache_segments s WHERE s.file=f.file) LIMIT 256`, time.Now().Add(-24*time.Hour).Unix())
	if err != nil {
		return
	}
	var files []string
	for rows.Next() {
		var f string
		if rows.Scan(&f) == nil {
			files = append(files, f)
		}
	}
	s.logRows("heartbeat", rows.Err())
	rows.Close()
	for _, f := range files {
		s.unlink(f)
	}
}

// Shared reports whether this cache uses the cluster's NAS pool.
func (c *Cache) Shared() bool { return c.shared != nil }

// StorageError is intentionally suitable for the owner UI and contains no credentials.
func (c *Cache) StorageError() string {
	if c.shared != nil {
		if err := c.shared.checkMount(); err != nil {
			return err.Error()
		}
		ctx, cancel := c.shared.context()
		defer cancel()
		if err := c.shared.db.PingContext(ctx); err != nil {
			return "Shared cache database is unavailable"
		}
	}
	return ""
}

// SharedPinTx joins a fenced preloader transaction. This package owns the pin
// SQL; the caller owns its lease check. Both locks live until the same commit.
func (c *Cache) SharedPinTx(ctx context.Context, tx *sql.Tx, id, reason string, add bool) error {
	if c.shared == nil {
		return errors.New("not a shared cache")
	}
	if err := store.LockKey(ctx, store.DialectOf(c.shared.db), tx, "segcache:pool"); err != nil {
		return err
	}
	var err error
	if add {
		_, err = tx.ExecContext(ctx, `INSERT INTO segcache_pins(release_id,reason,owner,expires) VALUES(?,?,'',0) ON CONFLICT(release_id,reason,owner) DO NOTHING`, id, reason)
	} else {
		_, err = tx.ExecContext(ctx, `DELETE FROM segcache_pins WHERE release_id=? AND reason=? AND owner=''`, id, reason)
	}
	return err
}

// ValidateSharedMount checks a proposed mount without creating a pool or moving
// bytes. A missing marker on an existing pool is a different/unmounted share.
func ValidateSharedMount(dir string, db *sql.DB) error {
	if db == nil {
		return errors.New("shared cache database is unavailable")
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return errors.New("mount the NAS directory on this node before saving")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var token string
	err = db.QueryRowContext(ctx, `SELECT token FROM segcache_pool WHERE id=1`).Scan(&token)
	if errors.Is(err, sql.ErrNoRows) {
		entries, e := os.ReadDir(dir)
		if e != nil {
			return e
		}
		if len(entries) > 0 {
			return errors.New("choose an empty dedicated directory for the new shared pool")
		}
		return nil
	}
	if err != nil {
		return errors.New("cannot verify the shared cache database")
	}
	b, err := os.ReadFile(filepath.Join(dir, ".ether-shared-cache"))
	if err != nil || string(b) != token {
		return errors.New("this directory does not expose this server's cache pool marker; mount the original NAS share")
	}
	return nil
}

func writeSharedSegment(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	var hdr [headerSize]byte
	copy(hdr[:4], headerMagic[:])
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.Checksum(b, castagnoli))
	binary.LittleEndian.PutUint64(hdr[8:16], uint64(len(b)))
	_, err = f.Write(hdr[:])
	if err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	ce := f.Close()
	if err == nil {
		err = ce
	}
	return err
}
func (s *sharedCache) closed() bool { s.c.mu.Lock(); defer s.c.mu.Unlock(); return s.c.closed }

func (s *sharedCache) nowSQL() string {
	if store.DialectOf(s.db) == store.Postgres {
		return "CAST(EXTRACT(EPOCH FROM clock_timestamp()) AS BIGINT)"
	}
	return "CAST(strftime('%s','now') AS INTEGER)"
}

// ReleaseSizes is a single metadata query regardless of shelf or segment count.
func (c *Cache) ReleaseSizes() map[string]int64 {
	out := map[string]int64{}
	if c.shared == nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		for id, r := range c.rels {
			out[id] = r.bytes
		}
		return out
	}
	ctx, cancel := c.shared.context()
	defer cancel()
	rows, err := c.shared.db.QueryContext(ctx, `SELECT release_id,bytes FROM segcache_releases WHERE bytes>0`)
	if err != nil {
		c.shared.log(err)
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var n int64
		if rows.Scan(&id, &n) == nil {
			out[id] = n
		}
	}
	c.shared.logRows("ReleaseSizes", rows.Err())
	return out
}
