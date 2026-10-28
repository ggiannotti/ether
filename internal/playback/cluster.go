package playback

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/braedonsaunders/ether/internal/clusterbus"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/faillog"
	"github.com/braedonsaunders/ether/internal/store"
)

// Cluster-wide session registry.
//
// Each node's sessions live in its own memory (they hold open sources); a
// clustered node also mirrors them into playback_live_sessions so stream
// limits and the owner's "now playing" span the cluster. One row is one
// viewing unit on one node:
//
//	session  a prepared source (id = session ID; grp = its channel group)
//	pending  a Prepare that passed admission but is still resolving
//	group    a live TV viewing unit reserved through BeginChannel
//
// The owning node rewrites its rows every SessionHeartbeat and whenever its
// sessions change, deletes them when they end, and at startup deletes only
// its own leftovers. Readers ignore rows not refreshed within SessionTTL and
// rows of nodes the cluster no longer sees alive, so a crashed node's
// streams stop counting within seconds.
//
// Admission (Prepare, BeginChannel) first applies the limits to this node's
// sessions exactly as a standalone server does, then — in one transaction
// serialised cluster-wide by store.LockKey — counts the other nodes' live
// units against the same limits and inserts its own pending/group row, so
// two nodes can never both admit the last slot. If the database cannot be
// reached the stream is admitted (fail open) and a warning logged.
func init() {
	store.Register("playback", 2, `
CREATE TABLE playback_live_sessions (
	id         TEXT    PRIMARY KEY,
	node_id    TEXT    NOT NULL,
	user_id    INTEGER NOT NULL,
	client_id  TEXT    NOT NULL DEFAULT '',
	grp        TEXT    NOT NULL DEFAULT '',
	kind       TEXT    NOT NULL,
	data       TEXT    NOT NULL DEFAULT '',
	started_at INTEGER NOT NULL DEFAULT 0,
	updated_at INTEGER NOT NULL
);
CREATE INDEX playback_live_sessions_node ON playback_live_sessions(node_id);
CREATE INDEX playback_live_sessions_updated ON playback_live_sessions(updated_at);
`)
}

// Registry row kinds.
const (
	unitSession = "session"
	unitPending = "pending"
	unitGroup   = "group"
)

// admitLockKey serialises admission across the cluster. One key (not one
// per user) because the server-wide limit spans every user; admissions are
// rare and the critical section is two short statements.
const admitLockKey = "playback:admit"

// staleRowAge is when any node may delete another node's forgotten rows.
const staleRowAge = 10 * time.Minute

// admitTimeout bounds the cluster-wide admission check; past it the
// stream is admitted (fail open) so a database outage never stalls a play.
const admitTimeout = 3 * time.Second

// liveNodesTTL caches the cluster's liveness view between reads.
const liveNodesTTL = 2 * time.Second

type registry struct {
	m    *Manager
	db   *sql.DB
	d    store.Dialect
	node clusterbus.Node
	self string
	log  *slog.Logger

	kick chan struct{}
	stop chan struct{}
	done chan struct{}

	liveMu sync.Mutex
	liveAt time.Time
	live   map[string]bool

	warnMu   sync.Mutex
	lastWarn time.Time

	// syncFail rate-limits the heartbeat loop's outage warnings.
	syncFail *faillog.Tracker
}

type unitRow struct {
	id, node, client, grp, kind string
	user                        int64
	data                        string
	started                     int64
}

func newRegistry(m *Manager, db *sql.DB, node clusterbus.Node) *registry {
	return &registry{m: m, db: db, d: store.DialectOf(db), node: node, self: node.NodeID(), log: m.log,
		kick: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
		syncFail: faillog.New(m.log, "playback: cluster session registry: heartbeat sessions")}
}

// start clears this node's leftovers from a previous run (never another
// node's) and begins heartbeating.
func (r *registry) start() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if _, err := r.db.ExecContext(ctx, `DELETE FROM playback_live_sessions WHERE node_id=?`, r.self); err != nil {
		r.warn("clear own stale sessions", err)
	}
	cancel()
	go r.loop()
}

// close stops heartbeating and removes this node's rows.
func (r *registry) close() {
	close(r.stop)
	<-r.done
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := r.db.ExecContext(ctx, `DELETE FROM playback_live_sessions WHERE node_id=?`, r.self); err != nil {
		r.warn("remove own sessions", err)
	}
}

// poke schedules a sync soon (sessions changed).
func (r *registry) poke() {
	if r == nil {
		return
	}
	select {
	case r.kick <- struct{}{}:
	default:
	}
}

func (r *registry) loop() {
	defer close(r.done)
	t := time.NewTicker(r.m.opts.SessionHeartbeat)
	defer t.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-t.C:
		case <-r.kick:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		if err := r.sync(ctx); err != nil {
			r.syncFail.Fail(err)
		} else {
			r.syncFail.OK()
		}
		cancel()
	}
}

// warn logs database trouble at most once a minute.
func (r *registry) warn(what string, err error) {
	r.warnMu.Lock()
	defer r.warnMu.Unlock()
	if time.Since(r.lastWarn) < time.Minute {
		return
	}
	r.lastWarn = time.Now()
	r.log.Warn("playback: cluster session registry: "+what, "err", err)
}

func pendingID(node string, id identity) string {
	return fmt.Sprintf("pending/%s/%d/%s/%d", node, id.userID, id.clientID, id.key)
}

func groupID(group string) string { return "group/" + group }

// sharedSnapshot is the JSON stored for other nodes' status pages: no open
// source and no NZB or alternates.
func sharedSnapshot(ps *core.PlaySession) string {
	if ps == nil {
		return ""
	}
	cp := *ps
	cp.Source = nil
	if cp.Resolution != nil {
		res := core.Resolution{Title: cp.Resolution.Title, Release: cp.Resolution.Release, ResolvedAt: cp.Resolution.ResolvedAt}
		cp.Resolution = &res
	}
	b, err := json.Marshal(&cp)
	if err != nil {
		return ""
	}
	return string(b)
}

// desired is the set of rows this node's current state should have.
func (r *registry) desired() []unitRow {
	m := r.m
	type ext struct {
		group string
		slot  externalGroup
	}
	m.mu.Lock()
	sessions := make([]*session, 0, len(m.sessions))
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	var rows []unitRow
	for id := range m.reservations {
		rows = append(rows, unitRow{id: pendingID(r.self, id), user: id.userID, client: id.clientID,
			grp: m.channelGroups[id], kind: unitPending})
	}
	exts := make([]ext, 0, len(m.externalGroups))
	for g, slot := range m.externalGroups {
		exts = append(exts, ext{g, slot})
	}
	m.mu.Unlock()
	now := m.now()
	for _, s := range sessions {
		ps := s.snapshot(now)
		rows = append(rows, unitRow{id: ps.ID, user: ps.UserID, client: ps.ClientID, grp: ps.PlaybackGroup,
			kind: unitSession, data: sharedSnapshot(ps), started: ps.StartedAt.UnixMilli()})
	}
	for _, e := range exts {
		var ps *core.PlaySession
		if m.deps.DescribeGroup != nil {
			ps = m.deps.DescribeGroup(e.group)
		}
		if ps == nil {
			ps = &core.PlaySession{ID: e.group, UserID: e.slot.user.ID, ClientID: e.slot.caps.ClientID,
				Username: e.slot.user.Username, Client: e.slot.caps}
		}
		rows = append(rows, unitRow{id: groupID(e.group), user: e.slot.user.ID, client: e.slot.caps.ClientID,
			grp: e.group, kind: unitGroup, data: sharedSnapshot(ps), started: ps.StartedAt.UnixMilli()})
	}
	return rows
}

// sync makes this node's rows match its state and refreshes them.
func (r *registry) sync(ctx context.Context) error {
	rows := r.desired()
	now := r.m.now().UnixMilli()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, u := range rows {
		if err := upsertUnit(ctx, tx, r.self, u, now); err != nil {
			return err
		}
	}
	args := []any{r.self}
	q := `DELETE FROM playback_live_sessions WHERE node_id=?`
	if len(rows) > 0 {
		q += ` AND id NOT IN (` + strings.TrimSuffix(strings.Repeat("?,", len(rows)), ",") + `)`
		for _, u := range rows {
			args = append(args, u.id)
		}
	}
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		return err
	}
	// Any node may sweep rows nobody has refreshed for a long time (a node
	// that died and never came back).
	if _, err := tx.ExecContext(ctx, `DELETE FROM playback_live_sessions WHERE updated_at<?`,
		now-staleRowAge.Milliseconds()); err != nil {
		return err
	}
	return tx.Commit()
}

func upsertUnit(ctx context.Context, tx *sql.Tx, node string, u unitRow, now int64) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO playback_live_sessions(id, node_id, user_id, client_id, grp, kind, data, started_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET node_id=excluded.node_id, user_id=excluded.user_id, client_id=excluded.client_id,
	grp=excluded.grp, kind=excluded.kind, data=excluded.data, started_at=excluded.started_at, updated_at=excluded.updated_at`,
		u.id, node, u.user, u.client, u.grp, u.kind, u.data, u.started, now)
	return err
}

// liveNodes is the cluster's liveness view, cached briefly. On error it
// returns nil: callers then trust row freshness alone.
func (r *registry) liveNodes(ctx context.Context) map[string]bool {
	r.liveMu.Lock()
	if r.live != nil && time.Since(r.liveAt) < liveNodesTTL {
		live := r.live
		r.liveMu.Unlock()
		return live
	}
	r.liveMu.Unlock()
	live, err := r.node.LiveNodes(ctx)
	if err != nil {
		return nil
	}
	r.liveMu.Lock()
	r.live, r.liveAt = live, time.Now()
	r.liveMu.Unlock()
	return live
}

// remoteRows reads the other nodes' live units.
func (r *registry) remoteRows(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, live map[string]bool, withData bool) ([]unitRow, error) {
	cols := `id, node_id, user_id, client_id, grp, kind, started_at`
	if withData {
		cols += `, data`
	}
	rows, err := q.QueryContext(ctx, `SELECT `+cols+` FROM playback_live_sessions WHERE node_id<>? AND updated_at>=?`,
		r.self, r.m.now().Add(-r.m.opts.SessionTTL).UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []unitRow
	for rows.Next() {
		var u unitRow
		dest := []any{&u.id, &u.node, &u.user, &u.client, &u.grp, &u.kind, &u.started}
		if withData {
			dest = append(dest, &u.data)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		if live != nil && !live[u.node] {
			continue // the owning node is gone; its streams ended with it
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// remoteCounts counts the other nodes' viewing units the way
// reservationCountsLocked counts this node's: a channel group is one unit,
// and a session of the same user and client is replaceable (the client
// moved to this node).
func remoteCounts(rows []unitRow, id identity) (user, total int) {
	units := map[string]int64{}
	for _, u := range rows {
		if u.kind == unitSession && u.user == id.userID && u.client == id.clientID {
			continue
		}
		unit := u.id
		if u.grp != "" {
			unit = fmt.Sprintf("group/%d/%s", u.user, u.grp)
		}
		units[unit] = u.user
	}
	for _, uid := range units {
		total++
		if uid == id.userID {
			user++
		}
	}
	return
}

// admit applies the limits to the whole cluster and records this node's
// claim (row) atomically. localUser/localTotal are this node's counts
// (excluding the claim itself), already checked against the limits.
func (r *registry) admit(ctx context.Context, user *core.User, id identity, claim unitRow, localUser, localTotal, userLimit, serverLimit int) error {
	if userLimit <= 0 && serverLimit <= 0 {
		// Nothing to enforce; still record the claim for status pages.
		r.poke()
		return nil
	}
	if r.m.deps.Availability.Down() {
		// Known outage: fail open at once rather than wait on the database.
		r.warn("cluster-wide stream limit check skipped (database unreachable); admitting", errors.New("database unreachable"))
		r.poke()
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, admitTimeout)
	defer cancel()
	live := r.liveNodes(ctx)
	err := func() error {
		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := store.LockKey(ctx, r.d, tx, admitLockKey); err != nil {
			return err
		}
		rows, err := r.remoteRows(ctx, tx, live, false)
		if err != nil {
			return err
		}
		ru, rt := remoteCounts(rows, id)
		if userLimit > 0 && localUser+ru >= userLimit {
			return fmt.Errorf("%w: %s is using %d of %d streams across the cluster", core.ErrStreamLimit, userLabel(user), localUser+ru, userLimit)
		}
		if serverLimit > 0 && localTotal+rt >= serverLimit {
			return fmt.Errorf("%w: server is at its limit of %d streams", core.ErrStreamLimit, serverLimit)
		}
		if err := upsertUnit(ctx, tx, r.self, claim, r.m.now().UnixMilli()); err != nil {
			return err
		}
		return tx.Commit()
	}()
	if errors.Is(err, core.ErrStreamLimit) {
		return err
	}
	if err != nil {
		// Fail open: a database outage must not stop people watching.
		r.m.deps.Availability.Report(err)
		r.warn("cluster-wide stream limit check failed; admitting", err)
	}
	return nil
}

// remoteSessions returns the other live nodes' sessions and live TV viewing
// units (which replace their programme sources), oldest first, each tagged
// with its node.
func (r *registry) remoteSessions(ctx context.Context) []*core.PlaySession {
	rows, err := r.remoteRows(ctx, r.db, r.liveNodes(ctx), true)
	if err != nil {
		r.warn("read other nodes' sessions", err)
		return nil
	}
	groups := map[string]bool{}
	for _, u := range rows {
		if u.kind == unitGroup {
			groups[u.grp] = true
		}
	}
	var out []*core.PlaySession
	for _, u := range rows {
		if u.kind == unitPending || u.data == "" || (u.kind == unitSession && u.grp != "" && groups[u.grp]) {
			continue
		}
		var ps core.PlaySession
		if json.Unmarshal([]byte(u.data), &ps) != nil {
			continue
		}
		ps.Node = u.node
		out = append(out, &ps)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].StartedAt.Equal(out[j].StartedAt) {
			return out[i].StartedAt.Before(out[j].StartedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// RemoteSessions returns the sessions other cluster nodes are serving, as
// the owner's status page presents them: a live TV viewing unit stands for
// its programme sources. Each has Node set. Nil on a standalone server.
func (m *Manager) RemoteSessions(ctx context.Context) []*core.PlaySession {
	if m.reg == nil {
		return nil
	}
	return m.reg.remoteSessions(ctx)
}
