// Package segcache is Ether's on-disk cache of decoded Usenet segments. It
// is a disk budget with segment-granular LRU eviction, pinning and a
// protection callback, not a library: the stream engine stores segments here
// so rewatches, seeks and the next episode are instant.
//
// Concurrency model: a single mutex guards the in-memory index (O(1) map
// lookups plus an intrusive LRU list); segment file IO happens outside it.
// Per-key striped RW locks order the file-level operations on one key: Get
// reads under the read lock, while the rename of a Put and the unlink of an
// eviction/Remove run under the write lock and re-check the index, so an
// eviction never deletes a newer file put concurrently and a reader never
// observes a file being replaced underneath it.
//
// A segment larger than the whole budget is rejected by Put with ErrTooLarge.
// If everything that could be evicted is pinned or protected the cache is
// allowed to exceed its budget (logged at most once a minute).
package segcache

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/maphash"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
)

const (
	numStripes         = 256
	defaultTouchEvery  = time.Minute
	defaultProtectTTL  = 5 * time.Second
	overBudgetLogEvery = time.Minute
)

var (
	// ErrTooLarge is returned by Put for a segment bigger than the whole budget.
	ErrTooLarge = errors.New("segcache: segment larger than cache budget")
	// ErrClosed is returned by mutating calls after Close.
	ErrClosed = errors.New("segcache: cache closed")
)

// Cache implements core.SegmentCache.
type Cache struct {
	shared  *sharedCache
	dir     string
	dataDir string
	logger  *slog.Logger
	seed    maphash.Seed
	stripes [numStripes]sync.RWMutex

	mu          sync.Mutex // guards everything below up to saveMu
	closed      bool
	budget      int64
	used        int64
	segCount    int
	rels        map[string]*release
	pins        map[string]map[string]struct{}
	lru         lruList // evictable candidates, head = most recent
	parked      lruList // entries found pinned/protected during eviction
	parkDirty   bool    // protection may have shrunk; merge parked back into lru
	lastOverLog time.Time
	policy      Policy
	metrics     Metrics

	saveMu sync.Mutex // serialises pins.json writes

	protMu    sync.Mutex
	protFn    func() map[string]bool
	protSet   map[string]bool
	protAt    time.Time
	protValid bool

	// Tunables, overridden by tests.
	now        func() time.Time
	touchEvery time.Duration
	protectTTL time.Duration
}

var _ core.SegmentCache = (*Cache)(nil)

// release is the index record of one release directory. id and dir are
// immutable; the rest is guarded by Cache.mu. Records are never deleted while
// the cache is open (an emptied release keeps its directory and metadata).
type release struct {
	id         string
	dir        string
	segs       map[int]*entry
	head, tail *entry
	bytes      int64
	lastUsed   int64
	metaOK     bool // directory and release.json exist on disk
	// metaMu serializes writing release.json: concurrent Puts for a new
	// release would otherwise race their atomic renames onto the same
	// file, which Windows refuses ("Access is denied").
	metaMu     sync.Mutex
	experience experience
}

// ReleaseUsage describes one release in the cache, for the admin UI.
type ReleaseUsage struct {
	ReleaseID  string
	Bytes      int64
	Segments   int
	LastUsed   time.Time
	Pins       []string
	Title      string
	Size       int64
	Plays      int
	LastPlayed time.Time
}

// Open opens (creating) a cache rooted at dir with a byte budget (<=0 means
// unlimited), rebuilding the in-memory index from disk (and cleaning stale
// temp files from interrupted writes). logger may be nil (slog.Default()).
func Open(dir string, budget int64, logger *slog.Logger) (*Cache, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if dir == "" {
		return nil, errors.New("segcache: open: empty directory")
	}
	c := &Cache{
		dir:        dir,
		dataDir:    filepath.Join(dir, dataDirName),
		logger:     logger.With("component", "segcache"),
		seed:       maphash.MakeSeed(),
		budget:     max(budget, 0),
		rels:       make(map[string]*release),
		pins:       make(map[string]map[string]struct{}),
		now:        time.Now,
		touchEvery: defaultTouchEvery,
		protectTTL: defaultProtectTTL,
	}
	if err := os.MkdirAll(c.dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("segcache: open %s: %w", dir, err)
	}
	if err := c.loadPins(); err != nil {
		return nil, err
	}
	if err := c.rebuild(); err != nil {
		return nil, err
	}
	c.evict()
	used, b := c.Usage()
	c.logger.Info("segment cache opened", "dir", dir, "segments", c.segCountSafe(),
		"used", used, "budget", b)
	return c, nil
}

func (c *Cache) segCountSafe() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.segCount
}

// Get returns the cached payload (a fresh buffer owned by the caller) and marks it as
// recently used. A missing, vanished or corrupt file is a miss.
func (c *Cache) Get(releaseID string, segment int) ([]byte, bool) {
	if c.shared != nil {
		return c.shared.get(releaseID, segment)
	}
	st := c.stripe(releaseID, segment)
	st.RLock()
	defer st.RUnlock()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, false
	}
	r := c.rels[releaseID]
	if r == nil {
		c.metrics.Misses++
		c.mu.Unlock()
		return nil, false
	}
	e := r.segs[segment]
	if e == nil {
		c.metrics.Misses++
		c.mu.Unlock()
		return nil, false
	}
	size := e.size
	c.mu.Unlock()

	path := filepath.Join(r.dir, segName(segment))
	data, err := readSegment(path, size)
	if err != nil {
		// Holding the stripe read lock excludes our own renames/unlinks of this
		// key, so the file genuinely vanished or was damaged externally.
		c.mu.Lock()
		c.metrics.Misses++
		if r.segs[segment] == e {
			c.dropLocked(e)
		}
		c.mu.Unlock()
		if errors.Is(err, fs.ErrNotExist) {
			c.logger.Warn("cached segment vanished from disk", "release", releaseID, "segment", segment)
		} else {
			c.logger.Warn("dropping unreadable cached segment", "release", releaseID,
				"segment", segment, "err", err)
			if rerr := os.Remove(path); rerr != nil && !errors.Is(rerr, fs.ErrNotExist) {
				c.logger.Warn("remove corrupt segment", "path", path, "err", rerr)
			}
		}
		return nil, false
	}

	now := c.now()
	nowNs := now.UnixNano()
	touch := false
	c.mu.Lock()
	c.metrics.Hits++
	c.metrics.ServedBytes += int64(len(data))
	if r.segs[segment] == e { // may have been evicted while we read; data is still valid
		c.listOf(e).remove(e)
		e.parked = false
		c.lru.pushFront(e)
		r.removeEntry(e)
		r.pushEntry(e)
		e.lastUsed = nowNs
		r.lastUsed = max(r.lastUsed, nowNs)
		if time.Duration(nowNs-e.touched) >= c.touchEvery {
			e.touched = nowNs
			touch = true
		}
	}
	c.mu.Unlock()
	if touch {
		if err := os.Chtimes(path, now, now); err != nil && !errors.Is(err, fs.ErrNotExist) {
			c.logger.Debug("touch segment", "path", path, "err", err)
		}
	}
	return data, true
}

// Has reports whether the segment is indexed. No disk IO, no recency update.
func (c *Cache) Has(releaseID string, segment int) bool {
	if c.shared != nil {
		return c.shared.has(releaseID, segment)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	r := c.rels[releaseID]
	return r != nil && r.segs[segment] != nil
}

// Put stores (or replaces) a segment crash-safely (temp file + rename) and
// then evicts down to the budget. A payload larger than the whole budget is
// rejected with ErrTooLarge and nothing is stored. Note that a stored segment
// may be evicted again immediately if everything else is pinned/protected.
func (c *Cache) Put(releaseID string, segment int, data []byte) error {
	if c.shared != nil {
		return c.shared.put(releaseID, segment, data)
	}
	if err := validKey(releaseID, segment); err != nil {
		return err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	if c.policy.Mode == "off" {
		c.mu.Unlock()
		return nil
	}
	if c.budget > 0 && int64(len(data)) > c.budget {
		b := c.budget
		c.mu.Unlock()
		return fmt.Errorf("segcache: put %q segment %d (%d bytes, budget %d): %w",
			releaseID, segment, len(data), b, ErrTooLarge)
	}
	r := c.releaseLocked(releaseID)
	metaOK := r.metaOK
	c.mu.Unlock()

	if !metaOK {
		if err := c.ensureReleaseDir(r, false); err != nil {
			return fmt.Errorf("segcache: put %q segment %d: %w", releaseID, segment, err)
		}
	}
	tmp, err := writeSegmentTemp(r.dir, segment, data)
	if errors.Is(err, fs.ErrNotExist) {
		// The release directory was removed behind our back; recreate it once.
		if err = c.ensureReleaseDir(r, true); err == nil {
			tmp, err = writeSegmentTemp(r.dir, segment, data)
		}
	}
	if err != nil {
		return fmt.Errorf("segcache: put %q segment %d: write: %w", releaseID, segment, err)
	}

	final := filepath.Join(r.dir, segName(segment))
	st := c.stripe(releaseID, segment)
	st.Lock()
	if err := os.Rename(tmp, final); err != nil {
		st.Unlock()
		_ = os.Remove(tmp)
		return fmt.Errorf("segcache: put %q segment %d: rename: %w", releaseID, segment, err)
	}
	nowNs := c.now().UnixNano()
	c.mu.Lock()
	c.insertLocked(r, segment, int64(len(data)), nowNs)
	c.mu.Unlock()
	st.Unlock()

	c.evict()
	return nil
}

// Pin protects every segment of a release from eviction while it has at
// least one pin reason. Pins are persisted to pins.json. If persisting fails
// the pin still applies in memory and the error is returned.
func (c *Cache) Pin(releaseID string, reason string) error {
	if c.shared != nil {
		return c.shared.pin(releaseID, reason, true)
	}
	if releaseID == "" || reason == "" {
		return errors.New("segcache: pin: empty release id or reason")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	set := c.pins[releaseID]
	if set == nil {
		set = make(map[string]struct{})
		c.pins[releaseID] = set
	}
	if _, ok := set[reason]; ok {
		c.mu.Unlock()
		return nil
	}
	set[reason] = struct{}{}
	c.mu.Unlock()
	return c.savePins()
}

// Unpin removes one pin reason. When the last reason goes the release becomes
// evictable again and the cache is evicted down to budget immediately.
func (c *Cache) Unpin(releaseID string, reason string) error {
	if c.shared != nil {
		return c.shared.pin(releaseID, reason, false)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	set := c.pins[releaseID]
	if _, ok := set[reason]; !ok {
		c.mu.Unlock()
		return nil
	}
	delete(set, reason)
	if len(set) == 0 {
		delete(c.pins, releaseID)
		c.parkDirty = true
	}
	c.mu.Unlock()
	err := c.savePins()
	c.evict()
	return err
}

// Pins returns a copy: releaseID -> sorted pin reasons.
func (c *Cache) Pins() map[string][]string {
	if c.shared != nil {
		return c.shared.pins()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pinsSnapshotLocked()
}

// Usage returns the payload bytes stored and the budget (0 = unlimited).
func (c *Cache) Usage() (used, budget int64) {
	if c.shared != nil {
		return c.shared.usage()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used, c.budget
}

// ReleaseBytes reports how many bytes of a release are currently cached.
func (c *Cache) ReleaseBytes(releaseID string) int64 {
	if c.shared != nil {
		return c.shared.releaseBytes(releaseID)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if r := c.rels[releaseID]; r != nil {
		return r.bytes
	}
	return 0
}

// SetBudget changes the budget (<=0 = unlimited) and evicts immediately if
// the cache is now over it.
func (c *Cache) SetBudget(budget int64) {
	c.mu.Lock()
	c.budget = max(budget, 0)
	c.mu.Unlock()
	c.evict()
}

// Protect registers a callback returning release IDs that must not be
// evicted right now. It is only called when an eviction is needed and its
// result is cached for a few seconds. A nil fn clears it. fn must not call
// Protect.
func (c *Cache) Protect(fn func() map[string]bool) {
	c.protMu.Lock()
	c.protFn = fn
	c.protSet = nil
	c.protValid = false
	c.protMu.Unlock()
	c.mu.Lock()
	c.parkDirty = true
	c.mu.Unlock()
	c.evict()
}

// Remove drops all segments of a release from the index and disk. Pins are
// kept.
func (c *Cache) Remove(releaseID string) error {
	if c.shared != nil {
		return c.shared.remove(releaseID)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	r := c.rels[releaseID]
	if r == nil {
		c.mu.Unlock()
		return nil
	}
	victims := make([]*entry, 0, len(r.segs))
	for _, e := range r.segs {
		victims = append(victims, e)
	}
	for _, e := range victims {
		c.dropLocked(e)
	}
	c.mu.Unlock()
	if err := c.deleteFiles(victims); err != nil {
		return fmt.Errorf("segcache: remove %q: %w", releaseID, err)
	}
	return nil
}

// Releases lists every release that has cached segments or pins, most
// recently used first.
func (c *Cache) Releases() []ReleaseUsage {
	if c.shared != nil {
		return c.shared.releases()
	}
	c.mu.Lock()
	out := make([]ReleaseUsage, 0, len(c.rels))
	seen := make(map[string]bool, len(c.rels))
	for id, r := range c.rels {
		if len(r.segs) == 0 && len(c.pins[id]) == 0 {
			continue
		}
		seen[id] = true
		u := ReleaseUsage{Title: r.experience.Title, Size: r.experience.Size, Plays: r.experience.Plays, LastPlayed: r.experience.LastPlayed, ReleaseID: id, Bytes: r.bytes, Segments: len(r.segs), Pins: sortedReasons(c.pins[id])}
		if r.lastUsed > 0 {
			u.LastUsed = time.Unix(0, r.lastUsed)
		}
		out = append(out, u)
	}
	for id, set := range c.pins {
		if !seen[id] {
			out = append(out, ReleaseUsage{ReleaseID: id, Pins: sortedReasons(set)})
		}
	}
	c.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if !out[i].LastUsed.Equal(out[j].LastUsed) {
			return out[i].LastUsed.After(out[j].LastUsed)
		}
		return out[i].ReleaseID < out[j].ReleaseID
	})
	return out
}

// Close marks the cache closed. Everything is already durable (pins are
// written eagerly), so Close only stops further use. It is idempotent.
func (c *Cache) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

// ---------------------------------------------------------------------------
// internals

func validKey(releaseID string, segment int) error {
	if releaseID == "" {
		return errors.New("segcache: empty release id")
	}
	if segment < 0 {
		return fmt.Errorf("segcache: negative segment %d", segment)
	}
	return nil
}

func (c *Cache) stripe(releaseID string, segment int) *sync.RWMutex {
	h := maphash.String(c.seed, releaseID) ^ (uint64(segment) * 0x9e3779b97f4a7c15)
	h ^= h >> 29
	return &c.stripes[h%numStripes]
}

func (c *Cache) releaseLocked(releaseID string) *release {
	r := c.rels[releaseID]
	if r == nil {
		r = &release{
			id:   releaseID,
			dir:  filepath.Join(c.dataDir, releaseDirRel(releaseID)),
			segs: make(map[int]*entry),
		}
		c.rels[releaseID] = r
	}
	return r
}

func (c *Cache) ensureReleaseDir(r *release, force bool) error {
	r.metaMu.Lock()
	defer r.metaMu.Unlock()
	if !force {
		c.mu.Lock()
		done := r.metaOK
		c.mu.Unlock()
		if done { // another Put wrote it while we waited
			return nil
		}
	}
	if err := writeReleaseMeta(r.dir, r.id); err != nil {
		return fmt.Errorf("create release dir: %w", err)
	}
	c.mu.Lock()
	r.metaOK = true
	c.mu.Unlock()
	return nil
}

func (c *Cache) listOf(e *entry) *lruList {
	if e.parked {
		return &c.parked
	}
	return &c.lru
}

func (c *Cache) insertLocked(r *release, seg int, size, nowNs int64) {
	e := r.segs[seg]
	if e != nil {
		c.used -= e.size
		r.bytes -= e.size
		c.listOf(e).remove(e)
		r.removeEntry(e)
	} else {
		e = &entry{rel: r, seg: seg}
		r.segs[seg] = e
		c.segCount++
	}
	e.size = size
	e.lastUsed = nowNs
	e.touched = nowNs
	e.parked = false
	c.used += size
	r.bytes += size
	r.lastUsed = max(r.lastUsed, nowNs)
	c.lru.pushFront(e)
	r.pushEntry(e)
}

// dropLocked removes e from the index and accounting (not from disk).
func (c *Cache) dropLocked(e *entry) {
	c.listOf(e).remove(e)
	e.parked = false
	e.rel.removeEntry(e)
	delete(e.rel.segs, e.seg)
	c.used -= e.size
	e.rel.bytes -= e.size
	c.segCount--
}

func (c *Cache) overBudgetLocked() bool { return c.budget > 0 && c.used > c.budget }

func (c *Cache) pinnedLocked(releaseID string) bool { return len(c.pins[releaseID]) > 0 }

// evict brings usage down to the budget, skipping pinned and protected
// releases. Skipped entries are parked so later passes do not rescan them;
// they are merged back (by recency) when protection may have shrunk.
func (c *Cache) evict() {
	if c.shared != nil {
		c.shared.sweep(false)
		return
	}
	c.mu.Lock()
	if c.closed || !c.overBudgetLocked() {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	prot, shrank := c.protected() // user callback; never under c.mu

	c.mu.Lock()
	if shrank {
		c.parkDirty = true
	}
	if c.parkDirty {
		c.unparkLocked()
		c.parkDirty = false
	}
	var victims []*entry
	for c.overBudgetLocked() {
		e := c.victimLocked(prot)
		if e == nil {
			break
		}
		if c.pinnedLocked(e.rel.id) || prot[e.rel.id] {
			c.lru.remove(e)
			e.parked = true
			c.parked.pushFront(e)
			continue
		}
		c.metrics.EvictedBytes += e.size
		c.dropLocked(e)
		victims = append(victims, e)
	}
	if c.overBudgetLocked() {
		if now := c.now(); now.Sub(c.lastOverLog) >= overBudgetLogEvery {
			c.lastOverLog = now
			c.logger.Warn("segment cache over budget: everything left is pinned or protected",
				"used", c.used, "budget", c.budget, "segments", c.segCount)
		}
	}
	c.mu.Unlock()

	if err := c.deleteFiles(victims); err != nil {
		c.logger.Warn("evict segments", "err", err)
	}
}

// unparkLocked merges the parked list back into the LRU list, ordered by
// lastUsed (both lists are already roughly in that order).
func (c *Cache) unparkLocked() {
	if c.parked.n == 0 {
		return
	}
	var merged lruList
	for c.lru.head != nil || c.parked.head != nil {
		a, b := c.lru.head, c.parked.head
		var pick *entry
		if b == nil || (a != nil && a.lastUsed >= b.lastUsed) {
			pick = a
			c.lru.remove(a)
		} else {
			pick = b
			c.parked.remove(b)
		}
		pick.parked = false
		merged.pushBack(pick)
	}
	c.lru = merged
}

// deleteFiles unlinks the files of entries already dropped from the index,
// unless a concurrent Put re-added the key meanwhile (then the file on disk
// is the new version and must stay).
func (c *Cache) deleteFiles(victims []*entry) error {
	var errs []error
	for _, e := range victims {
		st := c.stripe(e.rel.id, e.seg)
		st.Lock()
		c.mu.Lock()
		_, readded := e.rel.segs[e.seg]
		c.mu.Unlock()
		if !readded {
			path := filepath.Join(e.rel.dir, segName(e.seg))
			if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
				errs = append(errs, err)
			}
		}
		st.Unlock()
	}
	return errors.Join(errs...)
}

// protected returns the current protected set (cached for protectTTL) and
// whether it shrank compared to the previous one.
func (c *Cache) protected() (map[string]bool, bool) {
	c.protMu.Lock()
	defer c.protMu.Unlock()
	if c.protFn == nil {
		return nil, false
	}
	now := c.now()
	if c.protValid && now.Sub(c.protAt) < c.protectTTL {
		return c.protSet, false
	}
	raw := c.protFn()
	set := make(map[string]bool, len(raw))
	for id, ok := range raw {
		if ok {
			set[id] = true
		}
	}
	shrank := false
	if c.protValid {
		for id := range c.protSet {
			if !set[id] {
				shrank = true
				break
			}
		}
	}
	c.protSet, c.protAt, c.protValid = set, now, true
	return set, shrank
}

func sortedReasons(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for r := range set {
		out = append(out, r)
	}
	slices.Sort(out)
	return out
}

func (c *Cache) pinsSnapshotLocked() map[string][]string {
	out := make(map[string][]string, len(c.pins))
	for id, set := range c.pins {
		if len(set) > 0 {
			out[id] = sortedReasons(set)
		}
	}
	return out
}

func (c *Cache) savePins() error {
	c.saveMu.Lock()
	defer c.saveMu.Unlock()
	c.mu.Lock()
	snap := c.pinsSnapshotLocked() // taken under saveMu so the last writer wins with the newest state
	c.mu.Unlock()
	b, err := json.MarshalIndent(pinsFile{Version: 1, Pins: snap}, "", "  ")
	if err != nil {
		return fmt.Errorf("segcache: encode pins: %w", err)
	}
	if err := writeFileAtomic(filepath.Join(c.dir, pinsFileName), b, true); err != nil {
		return fmt.Errorf("segcache: save pins: %w", err)
	}
	return nil
}

// loadPins reads pins.json and removes stale temp files in the cache root. A
// corrupt pins file is logged and ignored (the cache stays usable).
func (c *Cache) loadPins() error {
	if ents, err := os.ReadDir(c.dir); err == nil {
		for _, de := range ents {
			if !de.IsDir() && strings.HasSuffix(de.Name(), tmpExt) {
				c.removeStale(filepath.Join(c.dir, de.Name()))
			}
		}
	}
	path := filepath.Join(c.dir, pinsFileName)
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("segcache: read pins: %w", err)
	}
	var pf pinsFile
	if err := json.Unmarshal(b, &pf); err != nil {
		c.logger.Warn("ignoring corrupt pins file", "path", path, "err", err)
		return nil
	}
	for id, reasons := range pf.Pins {
		for _, r := range reasons {
			if id == "" || r == "" {
				continue
			}
			if c.pins[id] == nil {
				c.pins[id] = make(map[string]struct{})
			}
			c.pins[id][r] = struct{}{}
		}
	}
	return nil
}

func (c *Cache) removeStale(path string) {
	if err := os.RemoveAll(path); err != nil {
		c.logger.Warn("remove stale cache file", "path", path, "err", err)
	} else {
		c.logger.Debug("removed stale cache file", "path", path)
	}
}

// rebuild walks the data directory once, rebuilding the index with file
// mtimes as last-used times and cleaning temp files, torn segment files and
// release directories without valid metadata or segments.
func (c *Cache) rebuild() error {
	fans, err := os.ReadDir(c.dataDir)
	if err != nil {
		return fmt.Errorf("segcache: open: read %s: %w", c.dataDir, err)
	}
	var all []*entry
	for _, fan := range fans {
		fanPath := filepath.Join(c.dataDir, fan.Name())
		if !fan.IsDir() {
			if strings.HasSuffix(fan.Name(), tmpExt) {
				c.removeStale(fanPath)
			}
			continue
		}
		rds, err := os.ReadDir(fanPath)
		if err != nil {
			c.logger.Warn("skip unreadable cache dir", "path", fanPath, "err", err)
			continue
		}
		for _, rd := range rds {
			p := filepath.Join(fanPath, rd.Name())
			if !rd.IsDir() {
				if strings.HasSuffix(rd.Name(), tmpExt) {
					c.removeStale(p)
				}
				continue
			}
			all = c.loadReleaseDir(p, filepath.Join(fan.Name(), rd.Name()), all)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].lastUsed < all[j].lastUsed })
	c.mu.Lock()
	for _, e := range all {
		e.rel.pushEntry(e)
		c.lru.pushFront(e) // oldest first, so the newest ends up at the head
	}
	c.mu.Unlock()
	return nil
}

func (c *Cache) loadReleaseDir(dir, rel string, all []*entry) []*entry {
	des, err := os.ReadDir(dir)
	if err != nil {
		c.logger.Warn("skip unreadable release dir", "path", dir, "err", err)
		return all
	}
	id, err := readReleaseMeta(dir)
	if err != nil || releaseDirRel(id) != rel {
		c.logger.Warn("removing cache dir without valid metadata", "path", dir, "err", err)
		c.removeStale(dir)
		return all
	}
	r := &release{id: id, dir: dir, segs: make(map[int]*entry), metaOK: true}
	if b, err := os.ReadFile(filepath.Join(dir, "experience.json")); err == nil {
		if err := json.Unmarshal(b, &r.experience); err != nil {
			c.logger.Debug("load release experience", "path", dir, "err", err)
		}
	}
	for _, de := range des {
		name := de.Name()
		p := filepath.Join(dir, name)
		if strings.HasSuffix(name, tmpExt) {
			c.removeStale(p)
			continue
		}
		seg, ok := parseSegName(name)
		if !ok || de.IsDir() {
			continue
		}
		info, err := de.Info()
		if err != nil {
			continue // vanished meanwhile
		}
		if info.Size() < headerSize {
			c.removeStale(p)
			continue
		}
		mt := info.ModTime().UnixNano()
		e := &entry{rel: r, seg: seg, size: info.Size() - headerSize, lastUsed: mt, touched: mt}
		r.segs[seg] = e
		r.bytes += e.size
		r.lastUsed = max(r.lastUsed, mt)
		all = append(all, e)
	}
	if len(r.segs) == 0 {
		c.removeStale(dir)
		return all
	}
	c.mu.Lock()
	c.rels[id] = r
	c.used += r.bytes
	c.segCount += len(r.segs)
	c.mu.Unlock()
	return all
}
