package plex

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/plex/model"
)

// Play queues are what every Plex app plays from: the app POSTs
// /playQueues with a content URI, then plays the selected item and walks
// the queue for "next". They are per-device and short-lived, so a
// standalone server keeps them in memory. A clustered node stores them in
// plex_playqueues (read-through/write-through, with this map as a hot
// cache) so a client that moves to another node keeps its queue: IDs come
// from the table's identity column and the plex_counters row, edits are
// optimistic (a row revision, retried on conflict) and other nodes drop
// their cached copy when told over the bus.

type pqItem struct {
	id  int64
	key core.RatingKey
}

type playQueue struct {
	id            int64
	userID        int64
	clientID      string
	items         []pqItem
	original      []pqItem // order before shuffle
	selected      int      // index into items
	shuffled      bool
	sourceURI     string
	playlistID    core.RatingKey
	playlistTitle string
	version       int
	updatedAt     time.Time
	liveSession   string // live queues resolve through LiveTV, never the catalogue

	rev      int64     // clustered: stored row revision
	loadedAt time.Time // clustered: when the cached copy was read or written
}

type playQueueStore struct {
	mu     sync.Mutex
	nextID int64
	nextIt int64
	queues map[int64]*playQueue // standalone: the queues; clustered: a cache
	cl     *clusterState        // nil on a standalone server
	now    func() time.Time
}

const maxQueues = 2000

// queueCacheTTL bounds how long a clustered node trusts its cached copy of
// a queue without re-reading it (bus invalidations normally refresh it
// sooner).
const queueCacheTTL = 30 * time.Second

// maxQueueAttempts bounds optimistic retries of one clustered edit.
const maxQueueAttempts = 6

func newPlayQueueStore() *playQueueStore {
	return &playQueueStore{nextID: 1, nextIt: 1, queues: map[int64]*playQueue{}, now: time.Now}
}

func (s *playQueueStore) newItemsLocked(keys []core.RatingKey) []pqItem {
	out := make([]pqItem, len(keys))
	for i, k := range keys {
		out[i] = pqItem{id: s.nextIt, key: k}
		s.nextIt++
	}
	return out
}

// preallocate reserves item IDs for keys before an edit on a clustered
// node (nil on a standalone server, which numbers items inside the edit).
func (s *playQueueStore) preallocate(keys []core.RatingKey) ([]pqItem, error) {
	if s.cl == nil || len(keys) == 0 {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()
	first, err := s.cl.counter(ctx, "playqueue_item", len(keys))
	if err != nil {
		return nil, err
	}
	out := make([]pqItem, len(keys))
	for i, k := range keys {
		out[i] = pqItem{id: first + int64(i), key: k}
	}
	return out, nil
}

// createWith creates a queue of keys for the user's client, applies fn to it
// and returns a snapshot.
func (s *playQueueStore) createWith(userID int64, clientID string, keys []core.RatingKey, now time.Time, fn func(q *playQueue)) (playQueue, error) {
	if s.cl != nil {
		return s.createShared(userID, clientID, keys, now, fn)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queues) >= maxQueues {
		s.evictOldestLocked()
	}
	q := &playQueue{id: s.nextID, userID: userID, clientID: clientID, items: s.newItemsLocked(keys), version: 1, updatedAt: now}
	s.nextID++
	s.queues[q.id] = q
	if fn != nil {
		fn(q)
	}
	return q.snapshot(), nil
}

func (s *playQueueStore) evictOldestLocked() {
	var oldest *playQueue
	for _, q := range s.queues {
		if oldest == nil || q.updatedAt.Before(oldest.updatedAt) ||
			(q.updatedAt.Equal(oldest.updatedAt) && q.id < oldest.id) {
			oldest = q
		}
	}
	if oldest != nil {
		delete(s.queues, oldest.id)
	}
}

// with runs fn on the queue and returns a snapshot (fn nil: read only).
func (s *playQueueStore) with(id, userID int64, now time.Time, fn func(q *playQueue)) (playQueue, bool) {
	if s.cl != nil {
		return s.withShared(id, userID, now, fn)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[id]
	if !ok || q.userID != userID {
		return playQueue{}, false
	}
	if fn != nil {
		fn(q)
		q.updatedAt = now
	}
	return q.snapshot(), true
}

func (q *playQueue) snapshot() playQueue {
	cp := *q
	cp.items = append([]pqItem(nil), q.items...)
	cp.original = append([]pqItem(nil), q.original...)
	return cp
}

func (s *playQueueStore) get(id, userID int64) (playQueue, bool) {
	return s.with(id, userID, time.Time{}, nil)
}

// selectItem marks the item a client reports playing (timeline).
func (s *playQueueStore) selectItem(userID, itemID int64) {
	if s.cl != nil {
		s.selectShared(userID, itemID)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, q := range s.queues {
		if q.userID != userID {
			continue
		}
		for i, it := range q.items {
			if it.id == itemID {
				q.selected = i
				q.updatedAt = s.now()
				return
			}
		}
	}
}

func (s *playQueueStore) expire(now time.Time, ttl time.Duration) {
	s.mu.Lock()
	for id, q := range s.queues {
		if now.Sub(q.updatedAt) > ttl {
			delete(s.queues, id)
		}
	}
	s.mu.Unlock()
	if s.cl != nil {
		// Idempotent on every node; no leader needed.
		ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
		defer cancel()
		if _, err := s.cl.db.ExecContext(ctx, `DELETE FROM plex_playqueues WHERE updated_at<?`, now.Add(-ttl).UnixMilli()); err != nil {
			s.cl.expireFail.Fail(err)
		} else {
			s.cl.expireFail.OK()
		}
	}
}

// addItems appends items for keys (or inserts them after the selected
// item). pre holds IDs preallocated on a clustered node; otherwise the
// caller holds s.mu (it runs inside with) and IDs are numbered here.
func (s *playQueueStore) addItems(q *playQueue, keys []core.RatingKey, pre []pqItem, next bool) {
	items := pre
	if items == nil {
		items = s.newItemsLocked(keys)
	}
	items = append([]pqItem(nil), items...)
	if next && len(q.items) > 0 {
		at := q.selected + 1
		q.items = append(q.items[:at], append(items, q.items[at:]...)...)
	} else {
		q.items = append(q.items, items...)
	}
	if q.shuffled {
		q.original = append(q.original, items...)
	}
	q.version++
}

// forget drops a cached queue another node changed.
func (s *playQueueStore) forget(id int64) {
	s.mu.Lock()
	delete(s.queues, id)
	s.mu.Unlock()
}

// dropCache forgets every cached queue (bus resync).
func (s *playQueueStore) dropCache() {
	if s.cl == nil {
		return
	}
	s.mu.Lock()
	s.queues = map[int64]*playQueue{}
	s.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Clustered storage

type storedItem struct {
	ID  int64          `json:"i"`
	Key core.RatingKey `json:"k"`
}

type storedQueue struct {
	ClientID      string         `json:"clientID,omitempty"`
	Items         []storedItem   `json:"items"`
	Original      []storedItem   `json:"original,omitempty"`
	Selected      int            `json:"selected"`
	Shuffled      bool           `json:"shuffled,omitempty"`
	SourceURI     string         `json:"sourceURI,omitempty"`
	PlaylistID    core.RatingKey `json:"playlistID,omitempty"`
	PlaylistTitle string         `json:"playlistTitle,omitempty"`
	Version       int            `json:"version"`
	LiveSession   string         `json:"liveSession,omitempty"`
}

func toStored(items []pqItem) []storedItem {
	out := make([]storedItem, len(items))
	for i, it := range items {
		out[i] = storedItem{ID: it.id, Key: it.key}
	}
	return out
}

func fromStored(items []storedItem) []pqItem {
	if len(items) == 0 {
		return nil
	}
	out := make([]pqItem, len(items))
	for i, it := range items {
		out[i] = pqItem{id: it.ID, key: it.Key}
	}
	return out
}

func encodeQueue(q *playQueue) (string, error) {
	b, err := json.Marshal(storedQueue{ClientID: q.clientID, Items: toStored(q.items), Original: toStored(q.original),
		Selected: q.selected, Shuffled: q.shuffled, SourceURI: q.sourceURI, PlaylistID: q.playlistID,
		PlaylistTitle: q.playlistTitle, Version: q.version, LiveSession: q.liveSession})
	return string(b), err
}

func decodeQueue(id, userID, rev, updated int64, data string) (*playQueue, error) {
	var sq storedQueue
	if err := json.Unmarshal([]byte(data), &sq); err != nil {
		return nil, err
	}
	return &playQueue{id: id, userID: userID, clientID: sq.ClientID, items: fromStored(sq.Items),
		original: fromStored(sq.Original), selected: sq.Selected, shuffled: sq.Shuffled, sourceURI: sq.SourceURI,
		playlistID: sq.PlaylistID, playlistTitle: sq.PlaylistTitle, version: sq.Version, liveSession: sq.LiveSession,
		updatedAt: time.UnixMilli(updated), rev: rev}, nil
}

func (s *playQueueStore) cache(q *playQueue) {
	cp := q.snapshot()
	cp.loadedAt = s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.queues[cp.id]; !ok && len(s.queues) >= maxQueues {
		s.evictOldestLocked()
	}
	s.queues[cp.id] = &cp
}

func (s *playQueueStore) cached(id int64) (*playQueue, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[id]
	if !ok || s.now().Sub(q.loadedAt) > queueCacheTTL {
		return nil, false
	}
	cp := q.snapshot()
	return &cp, true
}

func (s *playQueueStore) load(ctx context.Context, id int64) (*playQueue, error) {
	var userID, rev, updated int64
	var data string
	if err := s.cl.db.QueryRowContext(ctx, `SELECT user_id, rev, data, updated_at FROM plex_playqueues WHERE id=?`, id).
		Scan(&userID, &rev, &data, &updated); err != nil {
		return nil, queueRowError(err)
	}
	q, err := decodeQueue(id, userID, rev, updated, data)
	if err != nil {
		return nil, err
	}
	s.cache(q)
	return q, nil
}

func (s *playQueueStore) createShared(userID int64, clientID string, keys []core.RatingKey, now time.Time, fn func(q *playQueue)) (playQueue, error) {
	items, err := s.preallocate(keys)
	if err != nil {
		return playQueue{}, err
	}
	q := &playQueue{userID: userID, clientID: clientID, items: items, version: 1, updatedAt: now, rev: 1}
	if fn != nil {
		fn(q)
	}
	data, err := encodeQueue(q)
	if err != nil {
		return playQueue{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()
	if err := s.cl.db.QueryRowContext(ctx, `INSERT INTO plex_playqueues(user_id, rev, data, updated_at) VALUES (?, ?, ?, ?) RETURNING id`,
		userID, q.rev, data, now.UnixMilli()).Scan(&q.id); err != nil {
		return playQueue{}, err
	}
	s.cache(q)
	return q.snapshot(), nil
}

func (s *playQueueStore) withShared(id, userID int64, now time.Time, fn func(q *playQueue)) (playQueue, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*dbTimeout)
	defer cancel()
	for attempt := 0; attempt < maxQueueAttempts; attempt++ {
		q, ok := s.cached(id)
		if !ok || attempt > 0 {
			var err error
			if q, err = s.load(ctx, id); err != nil {
				if !errors.Is(err, core.ErrNotFound) {
					s.cl.warn("read play queue", err)
				}
				return playQueue{}, false
			}
		}
		if q.userID != userID {
			return playQueue{}, false
		}
		if fn == nil {
			return q.snapshot(), true
		}
		fn(q)
		q.updatedAt = now
		data, err := encodeQueue(q)
		if err != nil {
			return playQueue{}, false
		}
		res, err := s.cl.db.ExecContext(ctx, `UPDATE plex_playqueues SET data=?, rev=?, updated_at=? WHERE id=? AND rev=?`,
			data, q.rev+1, now.UnixMilli(), id, q.rev)
		if err != nil {
			s.cl.warn("save play queue", err)
			return playQueue{}, false
		}
		if n, _ := res.RowsAffected(); n == 0 {
			s.forget(id) // another node edited it (or it expired): re-read and retry
			continue
		}
		q.rev++
		s.cache(q)
		s.cl.publish("invalidate:playqueue", queueEvent{ID: id})
		return q.snapshot(), true
	}
	s.cl.warn("save play queue", errQueueConflict)
	return playQueue{}, false
}

// selectShared records the playing item and periodically renews its idle TTL.
// Repeated timelines for the same item must keep long sessions alive, but
// need not write the shared database on every client heartbeat.
func (s *playQueueStore) selectShared(userID, itemID int64) {
	if itemID <= 0 {
		return
	}
	find := func(q *playQueue) int {
		if q.userID != userID {
			return -1
		}
		return indexOfItem(q.items, itemID)
	}
	var qid int64
	s.mu.Lock()
	for _, q := range s.queues {
		if i := find(q); i >= 0 {
			if q.selected == i && s.now().Sub(q.updatedAt) <= queueCacheTTL {
				s.mu.Unlock()
				return
			}
			qid = q.id
			break
		}
	}
	s.mu.Unlock()
	if qid == 0 {
		ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
		rows, err := s.cl.db.QueryContext(ctx, `SELECT id, rev, data, updated_at FROM plex_playqueues WHERE user_id=? ORDER BY updated_at DESC LIMIT 50`, userID)
		if err != nil {
			cancel()
			s.cl.warn("find play queue item", err)
			return
		}
		for rows.Next() {
			var id, rev, updated int64
			var data string
			if rows.Scan(&id, &rev, &data, &updated) != nil {
				continue
			}
			q, err := decodeQueue(id, userID, rev, updated, data)
			if err != nil {
				continue
			}
			if i := find(q); i >= 0 {
				s.cache(q)
				if q.selected != i || s.now().Sub(q.updatedAt) > queueCacheTTL {
					qid = id
				}
				break
			}
		}
		if err := rows.Err(); err != nil {
			s.cl.warn("find play queue item", err)
		}
		rows.Close()
		cancel()
		if qid == 0 {
			return
		}
	}
	s.withShared(qid, userID, s.now(), func(q *playQueue) {
		if i := indexOfItem(q.items, itemID); i >= 0 {
			q.selected = i
		}
	})
}

// ---------------------------------------------------------------------------
// Handlers

func (s *Server) handlePlayQueueCreate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pr := FromContext(ctx)
	if pr.User == nil {
		writeHTTPError(w, http.StatusUnauthorized)
		return
	}
	q := r.URL.Query()
	if t := q.Get("type"); t != "" && t != "video" {
		writeHTTPError(w, http.StatusBadRequest)
		return
	}
	var keys []core.RatingKey
	var plKey core.RatingKey
	var plTitle string
	uri := q.Get("uri")
	switch {
	case q.Get("playlistID") != "":
		k, err := core.ParseRatingKey(q.Get("playlistID"))
		if err != nil {
			writeHTTPError(w, http.StatusBadRequest)
			return
		}
		pl, err := s.playlist(ctx, pr.User, k)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		keys, plKey, plTitle = append([]core.RatingKey(nil), pl.Items...), pl.Key, pl.Title
		if uri == "" {
			uri = "library://playlist/item/%2Fplaylists%2F" + pl.Key.String()
		}
	case uri != "":
		var err error
		keys, err = s.continuousKeys(ctx, pr, uri, q.Get("continuous") == "1")
		if err != nil {
			s.log.Debug("play queue uri", "uri", uri, "err", err)
			if errors.Is(err, core.ErrNotFound) {
				writeHTTPError(w, http.StatusNotFound)
			} else {
				writeHTTPError(w, http.StatusBadRequest)
			}
			return
		}
	default:
		writeHTTPError(w, http.StatusBadRequest)
		return
	}
	if len(keys) == 0 {
		writeHTTPError(w, http.StatusBadRequest)
		return
	}
	// Cinema Trailers: prepend trailers of other movies before the
	// feature and warm the feature while they play.
	prefixAt := -1
	if n, err := strconv.Atoi(q.Get("extrasPrefixCount")); err == nil && n > 0 && q.Get("shuffle") != "1" {
		featureIdx := 0
		if start := keyFromPath(q.Get("key")); start != 0 {
			for i, k := range keys {
				if k == start {
					featureIdx = i
					break
				}
			}
		}
		if keys[featureIdx].Kind() == core.KindMovie {
			if prefix := s.cinemaTrailerKeys(ctx, pr, keys[featureIdx], n); len(prefix) > 0 {
				feature := keys[featureIdx]
				merged := append([]core.RatingKey(nil), keys[:featureIdx]...)
				merged = append(append(merged, prefix...), keys[featureIdx:]...)
				keys = merged
				prefixAt = featureIdx
				s.prewarmFeature(ctx, pr, feature, pr.ClientCaps(s.cfg().Transcode.RemoteMaxBitrateKbps))
			}
		}
	}
	snap, err := s.queues.createWith(pr.User.ID, pr.ClientID, keys, s.now(), func(p *playQueue) {
		p.sourceURI = uri
		p.playlistID, p.playlistTitle = plKey, plTitle
		if start := keyFromPath(q.Get("key")); start != 0 {
			for i, it := range p.items {
				if it.key == start {
					p.selected = i
					break
				}
			}
		}
		if prefixAt >= 0 {
			p.selected = prefixAt // playback starts with the first trailer
		}
		if q.Get("shuffle") == "1" {
			shuffleQueue(p)
		}
	})
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writePlayQueue(w, r, snap)
}

// continuousKeys resolves a play queue URI; with continuous=1 a single
// episode expands to the rest of its show.
func (s *Server) continuousKeys(ctx context.Context, pr *Request, uri string, continuous bool) ([]core.RatingKey, error) {
	keys, err := s.resolveURI(ctx, pr, uri)
	if err != nil {
		return nil, err
	}
	if !continuous || len(keys) != 1 || keys[0].Kind() != core.KindEpisode {
		return keys, nil
	}
	ep := keys[0]
	show := ep.Parent().Parent()
	eps, err := s.metadataGrandchildren(ctx, pr, show)
	if err != nil {
		return keys, nil
	}
	for i, e := range eps {
		if e.Key == ep {
			return capKeys(titleKeys(s.visibleTitles(pr.User, eps[i:]))), nil
		}
	}
	return keys, nil
}

// keyFromPath extracts a rating key from "/library/metadata/123" or "123".
func keyFromPath(p string) core.RatingKey {
	if p == "" {
		return 0
	}
	if i := strings.Index(p, "/library/metadata/"); i >= 0 {
		p = p[i+len("/library/metadata/"):]
	}
	p, _, _ = strings.Cut(p, "/")
	p, _, _ = strings.Cut(p, "?")
	k, err := core.ParseRatingKey(p)
	if err != nil {
		return 0
	}
	return k
}

func shuffleQueue(p *playQueue) {
	if p.shuffled || len(p.items) < 2 {
		return
	}
	p.original = append([]pqItem(nil), p.items...)
	sel := p.items[p.selected]
	rest := append(append([]pqItem(nil), p.items[:p.selected]...), p.items[p.selected+1:]...)
	rand.Shuffle(len(rest), func(i, j int) { rest[i], rest[j] = rest[j], rest[i] })
	p.items = append([]pqItem{sel}, rest...)
	p.selected = 0
	p.shuffled = true
	p.version++
}

func unshuffleQueue(p *playQueue) {
	if !p.shuffled {
		return
	}
	var sel int64
	if p.selected < len(p.items) {
		// Removing every item leaves no selection to restore.
		sel = p.items[p.selected].id
	}
	present := map[int64]bool{}
	for _, it := range p.items {
		present[it.id] = true
	}
	var items []pqItem
	for _, it := range p.original {
		if present[it.id] {
			items = append(items, it)
		}
	}
	p.items, p.original, p.shuffled = items, nil, false
	for i, it := range p.items {
		if it.id == sel {
			p.selected = i
		}
	}
	p.version++
}

func (s *Server) pqParam(w http.ResponseWriter, r *http.Request) (int64, *core.User, bool) {
	u := FromContext(r.Context()).User
	id, err := strconv.ParseInt(chi.URLParam(r, "pq"), 10, 64)
	if err != nil || u == nil {
		writeHTTPError(w, http.StatusNotFound)
		return 0, nil, false
	}
	return id, u, true
}

func (s *Server) handlePlayQueue(w http.ResponseWriter, r *http.Request) {
	id, u, ok := s.pqParam(w, r)
	if !ok {
		return
	}
	snap, ok := s.queues.get(id, u.ID)
	if !ok {
		writeHTTPError(w, http.StatusNotFound)
		return
	}
	s.writePlayQueue(w, r, snap)
}

// handlePlayQueueMutate covers PUT /playQueues/{id} (add) and the
// shuffle/unshuffle/reset/clear/remove/move sub-resources.
func (s *Server) handlePlayQueueMutate(op string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, u, ok := s.pqParam(w, r)
		if !ok {
			return
		}
		ctx := r.Context()
		pr := FromContext(ctx)
		q := r.URL.Query()
		if existing, found := s.queues.get(id, u.ID); found && existing.liveSession != "" {
			writeHTTPError(w, http.StatusBadRequest)
			return
		}
		var addKeys []core.RatingKey
		if op == "add" {
			var err error
			switch {
			case q.Get("uri") != "":
				addKeys, err = s.resolveURI(ctx, pr, q.Get("uri"))
			case q.Get("playlistID") != "":
				var k core.RatingKey
				if k, err = core.ParseRatingKey(q.Get("playlistID")); err == nil {
					addKeys, err = s.expandKey(ctx, pr, k)
				}
			}
			if err != nil {
				writeHTTPError(w, http.StatusBadRequest)
				return
			}
		}
		pre, err := s.queues.preallocate(addKeys)
		if err != nil {
			s.writeErr(w, r, err)
			return
		}
		itemID, _ := strconv.ParseInt(chi.URLParam(r, "item"), 10, 64)
		var bad bool
		snap, ok := s.queues.with(id, u.ID, s.now(), func(p *playQueue) {
			bad = false // a clustered edit may run again after a conflict
			switch op {
			case "add":
				s.queues.addItems(p, addKeys, pre, q.Get("next") == "1")
			case "shuffle":
				shuffleQueue(p)
			case "unshuffle":
				unshuffleQueue(p)
			case "reset":
				p.selected = 0
				p.version++
			case "clear":
				// Keep the playing item, drop the rest.
				if len(p.items) > 0 {
					p.items = []pqItem{p.items[p.selected]}
				}
				p.selected = 0
				p.original = nil
				p.version++
			case "remove":
				idx := indexOfItem(p.items, itemID)
				if idx < 0 {
					bad = true
					return
				}
				p.items = append(p.items[:idx], p.items[idx+1:]...)
				if idx < p.selected || p.selected >= len(p.items) {
					p.selected = max(0, p.selected-1)
				}
				p.version++
			case "move":
				idx := indexOfItem(p.items, itemID)
				if idx < 0 {
					bad = true
					return
				}
				selID := p.items[p.selected].id
				it := p.items[idx]
				rest := append(append([]pqItem(nil), p.items[:idx]...), p.items[idx+1:]...)
				to := 0
				if after, err := strconv.ParseInt(q.Get("after"), 10, 64); err == nil && after > 0 {
					if a := indexOfItem(rest, after); a >= 0 {
						to = a + 1
					}
				}
				p.items = append(rest[:to], append([]pqItem{it}, rest[to:]...)...)
				p.selected = max(0, indexOfItem(p.items, selID))
				p.version++
			}
		})
		if !ok || bad {
			writeHTTPError(w, http.StatusNotFound)
			return
		}
		s.writePlayQueue(w, r, snap)
	}
}

func indexOfItem(items []pqItem, id int64) int {
	for i, it := range items {
		if it.id == id {
			return i
		}
	}
	return -1
}

// writePlayQueue renders a queue window around the selected item.
func (s *Server) writePlayQueue(w http.ResponseWriter, r *http.Request, pq playQueue) {
	if pq.liveSession != "" {
		s.writeLivePlayQueue(w, r, pq)
		return
	}
	ctx := r.Context()
	pr := FromContext(ctx)
	q := r.URL.Query()
	window := 50
	if n, err := strconv.Atoi(q.Get("window")); err == nil && n > 0 {
		window = min(n, 200)
	}
	lo := 0
	if q.Get("includeBefore") != "0" {
		lo = max(0, pq.selected-window/2)
	} else {
		lo = pq.selected
	}
	hi := min(len(pq.items), lo+window)
	if q.Get("includeAfter") == "0" {
		hi = min(len(pq.items), pq.selected+1)
	}
	if lo > len(pq.items) {
		lo = len(pq.items)
	}
	slice := pq.items[lo:hi]
	keys := make([]core.RatingKey, len(slice))
	for i, it := range slice {
		keys[i] = it.key
	}
	titles := s.visibleTitles(pr.User, s.getTitles(ctx, dedupeKeys(keys)))
	byKey := make(map[core.RatingKey]*core.Title, len(titles))
	for i := range titles {
		byKey[titles[i].Key] = &titles[i]
	}
	state := s.stateFor(ctx, pr, titles)

	var selItem pqItem
	if pq.selected < len(pq.items) {
		selItem = pq.items[pq.selected]
	}
	mc := libraryContainer()
	mc.Extra = []model.Attr{
		model.A("playQueueID", pq.id),
		model.A("playQueueSelectedItemID", selItem.id),
		model.A("playQueueSelectedItemOffset", pq.selected),
		model.A("playQueueSelectedMetadataItemID", selItem.key.String()),
		model.A("playQueueShuffled", pq.shuffled),
		model.A("playQueueSourceURI", pq.sourceURI),
		model.A("playQueueTotalCount", len(pq.items)),
		model.A("playQueueVersion", pq.version),
	}
	if pq.playlistID != 0 {
		mc.Extra = append(mc.Extra, model.A("playQueuePlaylistID", pq.playlistID.String()), model.A("playQueuePlaylistTitle", pq.playlistTitle))
	}
	for _, it := range slice {
		if it.key.Kind() == core.KindExtra {
			if md, ok := s.extraMetadataByKey(ctx, pr, it.key); ok {
				md.PlayQueueItemID = it.id
				mc.Metadata = append(mc.Metadata, md)
			}
			continue
		}
		t, ok := byKey[it.key]
		if !ok {
			continue
		}
		md := s.metadata(ctx, pr, t, renderOpts{detail: true, media: true, state: state})
		md.PlayQueueItemID = it.id
		mc.Metadata = append(mc.Metadata, md)
	}
	mc.Size = len(mc.Metadata)
	s.write(w, r, mc)
}

func dedupeKeys(ks []core.RatingKey) []core.RatingKey {
	seen := make(map[core.RatingKey]bool, len(ks))
	out := ks[:0:0]
	for _, k := range ks {
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}
