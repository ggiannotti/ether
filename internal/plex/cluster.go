package plex

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/braedonsaunders/ether/internal/clusterbus"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/faillog"
	"github.com/braedonsaunders/ether/internal/plex/model"
	"github.com/braedonsaunders/ether/internal/store"
)

// Clustering. A clustered node (Deps.Cluster set) shares with the others:
//
//   - its timeline-driven play sessions (plex_sessions), so /status/sessions
//     on any node lists every stream. Each node rewrites its own rows every
//     sessionHeartbeat and on every start/stop/state change, deletes them
//     when sessions end and, at startup, only its own leftovers; readers
//     skip rows older than sessionTTL and rows of nodes that are not live;
//   - termination requests ("session:terminate"): a kill for a session
//     another node serves is applied by that node through the same code a
//     local kill uses;
//   - notifications ("plex:notify"): every websocket/SSE notification is
//     also delivered to subscribers connected to the other nodes;
//   - play queues (plex_playqueues; see playqueue.go), so a client that
//     moves to another node keeps its queue.
//
// A standalone server (no Deps.Cluster) does none of this.
func init() {
	store.Register("plex", 2, `
CREATE TABLE plex_sessions (
	id          TEXT    PRIMARY KEY,
	node_id     TEXT    NOT NULL,
	user_id     INTEGER NOT NULL,
	session_key INTEGER NOT NULL,
	data        TEXT    NOT NULL,
	updated_at  INTEGER NOT NULL
);
CREATE INDEX plex_sessions_node ON plex_sessions(node_id);
CREATE TABLE plex_counters (
	name  TEXT    PRIMARY KEY,
	value INTEGER NOT NULL
);
INSERT INTO plex_counters(name, value) VALUES ('session_key', 0);
INSERT INTO plex_counters(name, value) VALUES ('playqueue_item', 0);
CREATE TABLE plex_playqueues (
	id         INTEGER PRIMARY KEY,
	user_id    INTEGER NOT NULL,
	rev        INTEGER NOT NULL,
	data       TEXT    NOT NULL,
	updated_at INTEGER NOT NULL
);
CREATE INDEX plex_playqueues_user ON plex_playqueues(user_id);
CREATE INDEX plex_playqueues_updated ON plex_playqueues(updated_at);
`)
}

// Cluster topics.
const (
	topicNotify    = "plex:notify"
	topicTerminate = "session:terminate"
)

const (
	sessionHeartbeat = 10 * time.Second
	sessionTTL       = 45 * time.Second
	liveCacheTTL     = 2 * time.Second
	dbTimeout        = 5 * time.Second
)

type clusterState struct {
	s      *Server
	db     *sql.DB
	node   clusterbus.Node
	self   string
	origin string
	unsubs []func()

	heartbeat time.Duration
	ttl       time.Duration

	kick chan struct{}
	stop chan struct{}
	done chan struct{}

	liveMu sync.Mutex
	liveAt time.Time
	live   map[string]bool

	warnMu   sync.Mutex
	lastWarn time.Time

	// syncFail and expireFail rate-limit the periodic loops' outage
	// warnings (first failure, a summary a minute, the recovery).
	syncFail   *faillog.Tracker
	expireFail *faillog.Tracker
}

func newClusterState(s *Server, db *sql.DB, node clusterbus.Node) *clusterState {
	return &clusterState{s: s, db: db, node: node, self: node.NodeID(), origin: clusterbus.NewOrigin(),
		heartbeat: cmp.Or(s.d.SessionHeartbeat, sessionHeartbeat), ttl: sessionTTL,
		kick: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
		syncFail:   faillog.New(s.log, "plex: cluster: heartbeat sessions"),
		expireFail: faillog.New(s.log, "plex: cluster: expire play queues")}
}

func (c *clusterState) start() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if _, err := c.db.ExecContext(ctx, `DELETE FROM plex_sessions WHERE node_id=?`, c.self); err != nil {
		c.warn("clear own stale sessions", err)
	}
	cancel()
	c.unsubs = []func(){
		c.node.Subscribe(topicNotify, c.onNotify),
		c.node.Subscribe(topicTerminate, c.onTerminate),
		c.node.Subscribe("invalidate:playqueue", c.onQueueChanged),
		c.node.Subscribe(clusterbus.TopicResync, func([]byte) { c.s.queues.dropCache() }),
	}
	go c.loop()
}

func (c *clusterState) close() {
	for _, u := range c.unsubs {
		u()
	}
	close(c.stop)
	<-c.done
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()
	if _, err := c.db.ExecContext(ctx, `DELETE FROM plex_sessions WHERE node_id=?`, c.self); err != nil {
		c.warn("remove own sessions", err)
	}
}

func (c *clusterState) warn(what string, err error) {
	c.warnMu.Lock()
	defer c.warnMu.Unlock()
	if time.Since(c.lastWarn) < time.Minute {
		return
	}
	c.lastWarn = time.Now()
	c.s.log.Warn("plex: cluster: "+what, "err", err)
}

// publish sends an origin-tagged event; failures are logged.
func (c *clusterState) publish(topic string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	msg := clusterbus.Tag(c.origin, b)
	if len(msg) > clusterbus.MaxPayload {
		c.s.log.Debug("plex: cluster event too large to share", "topic", topic, "bytes", len(msg))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), clusterbus.PublishTimeout)
	defer cancel()
	if err := c.node.Publish(ctx, topic, msg); err != nil {
		c.warn("publish "+topic, err)
	}
}

// remote unwraps an event from another node (ok=false for our own).
func (c *clusterState) remote(msg []byte, v any) bool {
	origin, payload := clusterbus.Untag(msg)
	if origin == c.origin {
		return false
	}
	return json.Unmarshal(payload, v) == nil
}

// counter atomically reserves n consecutive values of a plex_counters row
// and returns the first.
func (c *clusterState) counter(ctx context.Context, name string, n int) (int64, error) {
	var last int64
	err := c.db.QueryRowContext(ctx, `UPDATE plex_counters SET value=value+? WHERE name=? RETURNING value`, n, name).Scan(&last)
	if err != nil {
		return 0, err
	}
	return last - int64(n) + 1, nil
}

func (c *clusterState) liveNodes(ctx context.Context) map[string]bool {
	c.liveMu.Lock()
	if c.live != nil && time.Since(c.liveAt) < liveCacheTTL {
		live := c.live
		c.liveMu.Unlock()
		return live
	}
	c.liveMu.Unlock()
	live, err := c.node.LiveNodes(ctx)
	if err != nil {
		return nil
	}
	c.liveMu.Lock()
	c.live, c.liveAt = live, time.Now()
	c.liveMu.Unlock()
	return live
}

// ---------------------------------------------------------------------------
// Shared play sessions

// sharedEntry is a playEntry as other nodes see it.
type sharedEntry struct {
	ID          string               `json:"id"`
	SessionKey  int                  `json:"sessionKey"`
	User        core.User            `json:"user"`
	Client      sharedClient         `json:"client"`
	Key         core.RatingKey       `json:"key"`
	State       string               `json:"state"`
	OffsetMS    int64                `json:"offset"`
	DurationMS  int64                `json:"duration"`
	PQItemID    int64                `json:"playQueueItemID,omitempty"`
	TranscodeID string               `json:"transcodeID,omitempty"`
	Transcode   *model.TranscodeSess `json:"transcode,omitempty"`
	StartedAt   time.Time            `json:"startedAt"`
	UpdatedAt   time.Time            `json:"updatedAt"`
}

type sharedClient struct {
	ClientID        string `json:"clientID"`
	Product         string `json:"product,omitempty"`
	Version         string `json:"version,omitempty"`
	Platform        string `json:"platform,omitempty"`
	PlatformVersion string `json:"platformVersion,omitempty"`
	Device          string `json:"device,omitempty"`
	DeviceName      string `json:"deviceName,omitempty"`
	Model           string `json:"model,omitempty"`
	Vendor          string `json:"vendor,omitempty"`
	SessionID       string `json:"sessionID,omitempty"`
	RemoteIP        string `json:"remoteIP,omitempty"`
	Remote          bool   `json:"remote,omitempty"`
}

func (s *Server) toShared(e playEntry) sharedEntry {
	se := sharedEntry{ID: e.id, SessionKey: e.sessionKey, User: e.user, Key: e.key, State: e.state,
		OffsetMS: e.offset.Milliseconds(), DurationMS: e.duration.Milliseconds(), PQItemID: e.playQueueItemID,
		TranscodeID: e.transcodeID, StartedAt: e.startedAt, UpdatedAt: e.updatedAt,
		Client: sharedClient{ClientID: e.client.ClientID, Product: e.client.Product, Version: e.client.Version,
			Platform: e.client.Platform, PlatformVersion: e.client.PlatformVersion, Device: e.client.Device,
			DeviceName: e.client.DeviceName, Model: e.client.Model, Vendor: e.client.Vendor,
			SessionID: e.client.SessionID, Remote: e.client.Remote}}
	if e.client.RemoteIP.IsValid() {
		se.Client.RemoteIP = e.client.RemoteIP.String()
	}
	if e.transcodeID != "" {
		if te, ok := s.transcodes.get(e.transcodeID); ok {
			ts := transcodeSessionModel(te)
			se.Transcode = &ts
		}
	}
	return se
}

func (se sharedEntry) entry(node string) playEntry {
	e := playEntry{id: se.ID, sessionKey: se.SessionKey, user: se.User, key: se.Key, state: se.State,
		offset: time.Duration(se.OffsetMS) * time.Millisecond, duration: time.Duration(se.DurationMS) * time.Millisecond,
		playQueueItemID: se.PQItemID, transcodeID: se.TranscodeID, startedAt: se.StartedAt, updatedAt: se.UpdatedAt,
		node: node, remoteTS: se.Transcode,
		client: Request{ClientID: se.Client.ClientID, Product: se.Client.Product, Version: se.Client.Version,
			Platform: se.Client.Platform, PlatformVersion: se.Client.PlatformVersion, Device: se.Client.Device,
			DeviceName: se.Client.DeviceName, Model: se.Client.Model, Vendor: se.Client.Vendor,
			SessionID: se.Client.SessionID, Remote: se.Client.Remote}}
	e.client.RemoteIP = parseAddr(se.Client.RemoteIP)
	return e
}

// poke schedules a session sync soon.
func (c *clusterState) poke() {
	if c == nil {
		return
	}
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

func (c *clusterState) loop() {
	defer close(c.done)
	t := time.NewTicker(c.heartbeat)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
		case <-c.kick:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		if err := c.syncSessions(ctx); err != nil {
			c.syncFail.Fail(err)
		} else {
			c.syncFail.OK()
		}
		cancel()
	}
}

// syncSessions makes this node's plex_sessions rows match its tracker.
func (c *clusterState) syncSessions(ctx context.Context) error {
	entries := c.s.sessions.list()
	now := c.s.now().UnixMilli()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	args := []any{c.self}
	for _, e := range entries {
		b, err := json.Marshal(c.s.toShared(e))
		if err != nil {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO plex_sessions(id, node_id, user_id, session_key, data, updated_at) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET node_id=excluded.node_id, user_id=excluded.user_id, session_key=excluded.session_key,
	data=excluded.data, updated_at=excluded.updated_at`, e.id, c.self, e.user.ID, e.sessionKey, string(b), now); err != nil {
			return err
		}
		args = append(args, e.id)
	}
	q := `DELETE FROM plex_sessions WHERE node_id=?`
	if len(entries) > 0 {
		q += ` AND id NOT IN (` + strings.TrimSuffix(strings.Repeat("?,", len(entries)), ",") + `)`
	}
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM plex_sessions WHERE updated_at<?`, now-(10*time.Minute).Milliseconds()); err != nil {
		return err
	}
	return tx.Commit()
}

// remoteEntries lists the play sessions other live nodes are serving.
func (c *clusterState) remoteEntries(ctx context.Context) []playEntry {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()
	live := c.liveNodes(ctx)
	rows, err := c.db.QueryContext(ctx, `SELECT node_id, data FROM plex_sessions WHERE node_id<>? AND updated_at>=?`,
		c.self, c.s.now().Add(-c.ttl).UnixMilli())
	if err != nil {
		c.warn("read other nodes' sessions", err)
		return nil
	}
	defer rows.Close()
	var out []playEntry
	for rows.Next() {
		var node, data string
		if rows.Scan(&node, &data) != nil {
			continue
		}
		if live != nil && !live[node] {
			continue
		}
		var se sharedEntry
		if json.Unmarshal([]byte(data), &se) != nil {
			continue
		}
		out = append(out, se.entry(node))
	}
	if err := rows.Err(); err != nil {
		c.warn("read other nodes' sessions", err)
	}
	return out
}

// allocSessionKey reserves a cluster-unique sessionKey (0 if the database
// is unreachable: the tracker then numbers locally).
func (c *clusterState) allocSessionKey() int {
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()
	k, err := c.counter(ctx, "session_key", 1)
	if err != nil {
		c.warn("allocate session key", err)
		return 0
	}
	return int(k)
}

// sessionEntries is this node's sessions plus, when clustered, every other
// live node's. A client that moved here mid-play is listed once (this
// node's entry wins) until the old node's entry expires.
func (s *Server) sessionEntries(ctx context.Context) []playEntry {
	entries := s.sessions.list()
	if s.cl == nil {
		return entries
	}
	local := make(map[string]bool, len(entries))
	for _, e := range entries {
		local[e.id] = true
	}
	for _, e := range s.cl.remoteEntries(ctx) {
		if !local[e.id] {
			entries = append(entries, e)
		}
	}
	return entries
}

// endLocalSession applies a "stopped" timeline another node received for a
// session this node serves, as handleTimeline does for a local one.
func (s *Server) endLocalSession(tid string, offset time.Duration) {
	e, ok := s.sessions.remove(tid)
	if !ok {
		return
	}
	e.offset = offset
	s.notifyPlaying(e, "stopped")
	s.cl.poke()
	if s.d.Playback != nil {
		if ps, ok := s.d.Playback.SessionFor(e.user.ID, e.client.ClientID, e.key); ok {
			s.d.Playback.Stop(ps.ID)
		}
	}
	for _, id := range s.transcodes.forClient(e.user.ID, e.client.ClientID, e.key) {
		if s.d.Transcoder != nil {
			s.d.Transcoder.Stop(id)
		}
		s.transcodes.remove(id)
	}
}

// ---------------------------------------------------------------------------
// Termination

// terminateEvent asks the node serving a session to end it.
type terminateEvent struct {
	Scope  string `json:"scope"` // "plex": a /status/sessions/terminate match; "playback": an owner stop by playback/live TV session ID
	ID     string `json:"id"`
	Reason string `json:"reason"`
	Node   string `json:"node,omitempty"` // the node believed to serve it (informational)
	// OffsetMS is the final position of a "stopped" forward.
	OffsetMS int64 `json:"offset,omitempty"`
}

// forwardStop hands a client's "stopped" timeline to the node that serves
// its session (the client reached this node instead).
func (c *clusterState) forwardStop(ctx context.Context, tid string, offset time.Duration) {
	for _, e := range c.remoteEntries(ctx) {
		if e.id == tid {
			c.publish(topicTerminate, terminateEvent{Scope: "stopped", ID: tid, Node: e.node, OffsetMS: offset.Milliseconds()})
			return
		}
	}
}

func (c *clusterState) onTerminate(msg []byte) {
	var ev terminateEvent
	if !c.remote(msg, &ev) {
		return
	}
	// Bus callbacks must not block: stopping touches transcoders.
	go func() {
		switch ev.Scope {
		case "plex":
			if c.s.terminateLocal(ev.ID, ev.Reason) {
				c.s.log.Info("ended stream at another node's request", "session", ev.ID)
			}
		case "playback":
			if c.s.stopLocalPlayback(ev.ID, ev.Reason) {
				c.s.log.Info("ended stream at another node's request", "session", ev.ID)
			}
		case "stopped":
			c.s.endLocalSession(ev.ID, time.Duration(ev.OffsetMS)*time.Millisecond)
		}
	}()
}

// ---------------------------------------------------------------------------
// Notifications

type notifyEvent struct {
	UserID int64           `json:"u"`
	Type   string          `json:"t"`
	Data   json.RawMessage `json:"d"`
}

func (c *clusterState) shareNotification(msg notification, userID int64) {
	c.publish(topicNotify, notifyEvent{UserID: userID, Type: msg.typ, Data: json.RawMessage(msg.data)})
}

func (c *clusterState) onNotify(msg []byte) {
	var ev notifyEvent
	if !c.remote(msg, &ev) || ev.Type == "" {
		return
	}
	c.s.notifier.deliver(newNotification(ev.Type, []byte(ev.Data)), ev.UserID)
}

// ---------------------------------------------------------------------------
// Play queues

func (c *clusterState) onQueueChanged(msg []byte) {
	var ev queueEvent
	if !c.remote(msg, &ev) {
		return
	}
	c.s.queues.forget(ev.ID)
}

type queueEvent struct {
	ID int64 `json:"id"`
}

var errQueueConflict = errors.New("plex: play queue changed concurrently")

func queueRowError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return core.ErrNotFound
	}
	return fmt.Errorf("plex: play queue store: %w", err)
}

func parseAddr(s string) netip.Addr {
	a, _ := netip.ParseAddr(s)
	return a
}
