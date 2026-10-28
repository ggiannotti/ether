package plex

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/plex/model"
)

// ---------------------------------------------------------------------------
// Play session tracking (timeline-driven; feeds /status/sessions and
// "playing" notifications).

type playEntry struct {
	id              string // userID/clientID/sessionIdentifier
	sessionKey      int
	user            core.User
	client          Request // product/platform/device snapshot (no User)
	key             core.RatingKey
	state           string
	offset          time.Duration
	duration        time.Duration
	playQueueItemID int64
	transcodeID     string
	startedAt       time.Time
	updatedAt       time.Time

	// Set on entries another cluster node serves (read from plex_sessions).
	node     string
	remoteTS *model.TranscodeSess
}

type sessionTracker struct {
	mu      sync.Mutex
	next    int
	entries map[string]*playEntry
}

func newSessionTracker() *sessionTracker { return &sessionTracker{entries: map[string]*playEntry{}} }

func trackID(userID int64, clientID, sessionID string) string {
	return fmt.Sprintf("%d/%s/%s", userID, clientID, sessionID)
}

// has reports whether id is tracked.
func (t *sessionTracker) has(id string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.entries[id]
	return ok
}

// ensure starts tracking id with sessionKey key (allocated cluster-wide);
// key 0, or an id already tracked, leaves numbering to update.
func (t *sessionTracker) ensure(id string, key int) {
	if key <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.entries[id]; !ok {
		t.entries[id] = &playEntry{id: id, sessionKey: key}
	}
}

// update records a timeline event and returns a copy of the entry.
func (t *sessionTracker) update(id string, fn func(e *playEntry)) playEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.entries[id]
	if !ok {
		t.next++
		e = &playEntry{id: id, sessionKey: t.next}
		t.entries[id] = e
	}
	fn(e)
	return *e
}

func (t *sessionTracker) remove(id string) (playEntry, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.entries[id]
	if !ok {
		return playEntry{}, false
	}
	delete(t.entries, id)
	return *e, true
}

func (t *sessionTracker) list() []playEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]playEntry, 0, len(t.entries))
	for _, e := range t.entries {
		out = append(out, *e)
	}
	return out
}

// expire drops entries without a timeline update for ttl and returns them.
func (t *sessionTracker) expire(now time.Time, ttl time.Duration) []playEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []playEntry
	for id, e := range t.entries {
		if now.Sub(e.updatedAt) > ttl {
			out = append(out, *e)
			delete(t.entries, id)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Notification fan-out

type subscriber struct {
	userID  int64
	owner   bool
	ch      chan notification
	filters map[string]bool // empty = all
}

type notification struct {
	typ  string
	data []byte   // JSON NotificationContainer (websocket)
	sse  [][]byte // one bare entry object per event (eventsource)
}

func newNotification(typ string, data []byte) notification {
	return notification{typ: typ, data: data, sse: sseEntries(data)}
}

// sseEntries splits a JSON NotificationContainer into the payloads PMS sends
// on /:/eventsource/notifications: one {"<Entry>":{...}} object per entry,
// with no container (recorded from PMS 1.43). Clients that read the
// eventsource stream ignore the websocket container, so library changes
// never reached their cached hubs.
func sseEntries(data []byte) [][]byte {
	var root struct {
		NotificationContainer map[string]json.RawMessage
	}
	if json.Unmarshal(data, &root) != nil {
		return nil
	}
	keys := make([]string, 0, len(root.NotificationContainer))
	for k, v := range root.NotificationContainer {
		if len(v) > 0 && v[0] == '[' {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var out [][]byte
	for _, k := range keys {
		var items []json.RawMessage
		if json.Unmarshal(root.NotificationContainer[k], &items) != nil {
			continue
		}
		for _, item := range items {
			if b, err := json.Marshal(map[string]json.RawMessage{k: item}); err == nil {
				out = append(out, b)
			}
		}
	}
	return out
}

type notifier struct {
	mu     sync.Mutex
	subs   map[*subscriber]struct{}
	closed bool
	done   chan struct{}
}

func newNotifier() *notifier {
	return &notifier{subs: map[*subscriber]struct{}{}, done: make(chan struct{})}
}

func (n *notifier) subscribe(filters []string, user *core.User) (*subscriber, bool) {
	sub := &subscriber{userID: user.ID, owner: user.IsOwner, ch: make(chan notification, 64), filters: map[string]bool{}}
	for _, f := range filters {
		if f = strings.TrimSpace(f); f != "" {
			sub.filters[f] = true
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil, false
	}
	n.subs[sub] = struct{}{}
	return sub, true
}

func (n *notifier) unsubscribe(sub *subscriber) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.subs, sub)
}

// encode serialises a notification for subscribers.
func encodeNotification(nc model.NotificationContainer) (notification, bool) {
	data, err := model.Marshal(nc, model.JSON)
	if err != nil {
		return notification{}, false
	}
	data = []byte(strings.TrimSuffix(string(data), "\n"))
	return newNotification(nc.Type, data), true
}

// publish delivers to every interested subscriber, dropping for slow ones.
func (n *notifier) publish(nc model.NotificationContainer, userID int64) {
	if msg, ok := encodeNotification(nc); ok {
		n.deliver(msg, userID)
	}
}

// deliver hands an encoded notification to every interested subscriber.
func (n *notifier) deliver(msg notification, userID int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for sub := range n.subs {
		if userID != 0 && sub.userID != userID && !sub.owner {
			continue
		}
		if len(sub.filters) > 0 && !sub.filters[msg.typ] {
			continue
		}
		select {
		case sub.ch <- msg:
		default:
		}
	}
}

func (n *notifier) close() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.closed {
		n.closed = true
		close(n.done)
	}
}

func notificationFilters(r *http.Request) []string {
	q := r.URL.Query()
	v := q.Get("filters")
	if v == "" {
		v = q.Get("filter")
	}
	if v == "" {
		return nil
	}
	return strings.Split(v, ",")
}

// handleWebsocket serves /:/websockets/notifications.
func (s *Server) handleWebsocket(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Plex Web connects from app.plex.tv; tokens, not origins, gate access.
		InsecureSkipVerify: true,
	})
	if err != nil {
		s.log.Debug("websocket accept failed", "err", err)
		return
	}
	defer c.CloseNow()
	sub, ok := s.notifier.subscribe(notificationFilters(r), FromContext(r.Context()).User)
	if !ok {
		c.Close(websocket.StatusGoingAway, "server shutting down")
		return
	}
	defer s.notifier.unsubscribe(sub)

	ctx := c.CloseRead(r.Context())
	ping := time.NewTicker(30 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.notifier.done:
			c.Close(websocket.StatusGoingAway, "server shutting down")
			return
		case <-ping.C:
			if s.activeViewer(r.Context(), sub.userID) == nil {
				return
			}
			pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := c.Ping(pctx)
			cancel()
			if err != nil {
				return
			}
		case msg := <-sub.ch:
			if s.activeViewer(r.Context(), sub.userID) == nil {
				return
			}
			wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := c.Write(wctx, websocket.MessageText, msg.data)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

// sseWriteTimeout bounds one Server-Sent Events write. Pings go out every
// 10s, so a healthy client never comes close.
const sseWriteTimeout = 2 * time.Minute

// handleEventSource serves /:/eventsource/notifications (Server-Sent Events).
func (s *Server) handleEventSource(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		writeHTTPError(w, http.StatusInternalServerError)
		return
	}
	sub, ok := s.notifier.subscribe(notificationFilters(r), FromContext(r.Context()).User)
	if !ok {
		writeHTTPError(w, http.StatusServiceUnavailable)
		return
	}
	defer s.notifier.unsubscribe(sub)
	// No server WriteTimeout covers this long-lived response; arm a deadline
	// before each write so a client that stops reading errors out instead
	// of parking this goroutine and its subscription forever.
	rc := http.NewResponseController(w)
	arm := func() { _ = rc.SetWriteDeadline(time.Now().Add(sseWriteTimeout)) }
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	arm()
	fmt.Fprint(w, "retry: 5000\n\n")
	fl.Flush()
	ping := time.NewTicker(10 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.notifier.done:
			return
		case <-ping.C:
			if s.activeViewer(r.Context(), sub.userID) == nil {
				return
			}
			arm()
			if _, err := fmt.Fprint(w, "event: ping\ndata: {}\n\n"); err != nil {
				return
			}
			fl.Flush()
		case msg := <-sub.ch:
			if s.activeViewer(r.Context(), sub.userID) == nil {
				return
			}
			arm()
			payloads := msg.sse
			if len(payloads) == 0 {
				payloads = [][]byte{msg.data}
			}
			for _, data := range payloads {
				if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", msg.typ, data); err != nil {
					return
				}
			}
			fl.Flush()
		}
	}
}

// notifyPlaying publishes a PlaySessionStateNotification.
func (s *Server) notifyPlaying(e playEntry, state string) {
	if state == "" {
		state = e.state
	}
	s.publishNotification(model.NotificationContainer{
		Type: "playing",
		Size: 1,
		PlaySessionStateNotification: []model.PlaySessionStateNotification{{
			SessionKey:       fmt.Sprint(e.sessionKey),
			ClientIdentifier: e.client.ClientID,
			GUID:             "",
			RatingKey:        e.key.String(),
			URL:              "",
			Key:              "/library/metadata/" + e.key.String(),
			ViewOffset:       ms(e.offset),
			PlayQueueItemID:  e.playQueueItemID,
			State:            state,
			TranscodeSession: e.transcodeID,
		}},
	}, e.user.ID)
}

// WatchStateChanged invalidates client library metadata after a committed watch
// update. A playing notification only updates session status; clients can keep
// their cached Continue Watching shelf until a library timeline event arrives.
func (s *Server) WatchStateChanged(userID int64, key core.RatingKey) {
	var kind core.MediaType
	switch key.Kind() {
	case core.KindMovie:
		kind = core.TypeMovie
	case core.KindEpisode:
		kind = core.TypeEpisode
	default:
		return
	}
	s.notifyTimeline(key, kind, "", userID)
}

// notifyTimeline publishes a library timeline entry (watched-state change).
func (s *Server) notifyTimeline(key core.RatingKey, t core.MediaType, title string, userID int64) {
	sec, _ := s.sectionFor(t)
	typeNum := map[core.MediaType]int{core.TypeMovie: 1, core.TypeShow: 2, core.TypeSeason: 3, core.TypeEpisode: 4}[t]
	// PMS names an episode's season and show, and a season's show, so a
	// client can refresh the show's rows (Continue Watching, On Deck).
	var parent, root string
	switch key.Kind() {
	case core.KindEpisode:
		parent, root = key.Parent().String(), core.ShowKey(key.TMDB()).String()
	case core.KindSeason:
		parent = key.Parent().String()
	}
	s.publishNotification(model.NotificationContainer{
		Type: "timeline",
		Size: 1,
		TimelineEntry: []model.TimelineEntry{{
			Identifier:   model.LibraryIdentifier,
			SectionID:    fmt.Sprint(sec.ID),
			ItemID:       key.String(),
			ParentItemID: parent,
			RootItemID:   root,
			Type:         typeNum,
			Title:        title,
			State:        5,
			UpdatedAt:    s.now().Unix(),
		}},
	}, userID)
}

// publishNotification delivers a notification to this node's subscribers
// and, on a clustered node, to every other node's.
func (s *Server) publishNotification(nc model.NotificationContainer, userID int64) {
	msg, ok := encodeNotification(nc)
	if !ok {
		return
	}
	s.notifier.deliver(msg, userID)
	if s.cl != nil {
		s.cl.shareNotification(msg, userID)
	}
}
