package catalog_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/braedonsaunders/ether/internal/catalog"
	"github.com/braedonsaunders/ether/internal/config"
	"github.com/braedonsaunders/ether/internal/core"
)

// errIter is the sentinel iteration error delivered by the stub driver
// after one valid row: database/sql surfaces it through Rows.Err.
var errIter = errors.New("stub: iteration failed")

// trajStubRows yields its rows, then errIter (when fail is set) or EOF.
type trajStubRows struct {
	cols []string
	data [][]driver.Value
	idx  int
	fail bool
}

func (r *trajStubRows) Columns() []string { return r.cols }
func (r *trajStubRows) Close() error      { return nil }
func (r *trajStubRows) Next(dest []driver.Value) error {
	if r.idx < len(r.data) {
		copy(dest, r.data[r.idx])
		r.idx++
		return nil
	}
	if r.fail {
		return errIter
	}
	return io.EOF
}

type trajStubTx struct{}

func (trajStubTx) Commit() error   { return nil }
func (trajStubTx) Rollback() error { return nil }

// trajStubConn serves the three ShowTrajectories queries from canned
// rows. seasonFail / episodeFail make that cursor deliver one valid row
// and then errIter from Next.
type trajStubConn struct {
	showKey    int64
	seasonFail bool
	epFail     bool
}

func (c *trajStubConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("stub: prepare not supported")
}
func (c *trajStubConn) Close() error              { return nil }
func (c *trajStubConn) Begin() (driver.Tx, error) { return trajStubTx{}, nil }
func (c *trajStubConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return trajStubTx{}, nil
}

func (c *trajStubConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "type='episode'"):
		return &trajStubRows{
			cols: []string{"show_key", "season", "rating"},
			data: [][]driver.Value{{c.showKey, int64(1), 7.0}},
			fail: c.epFail,
		}, nil
	case strings.Contains(query, "type='season'"):
		data := [][]driver.Value{
			{c.showKey, int64(1), 8.0},
			{c.showKey, int64(2), 9.0},
		}
		if c.seasonFail {
			data = data[:1] // one valid row, then errIter from Next
		}
		return &trajStubRows{
			cols: []string{"show_key", "season", "rating"},
			data: data,
			fail: c.seasonFail,
		}, nil
	default: // SELECT key, status, rating FROM catalog_titles WHERE key IN (...)
		return &trajStubRows{
			cols: []string{"key", "status", "rating"},
			data: [][]driver.Value{{c.showKey, "Ended", 8.5}},
		}, nil
	}
}

type trajStubDriver struct{ conn *trajStubConn }

func (d *trajStubDriver) Open(string) (driver.Conn, error) { return d.conn, nil }

type trajStubConnector struct{ conn *trajStubConn }

func (c *trajStubConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c *trajStubConnector) Driver() driver.Driver {
	return &trajStubDriver{conn: c.conn}
}

func newTrajStubCatalog(t *testing.T, seasonFail, epFail bool) *catalog.Catalog {
	t.Helper()
	show := core.ShowKey(100)
	conn := &trajStubConn{showKey: int64(show), seasonFail: seasonFail, epFail: epFail}
	db := sql.OpenDB(&trajStubConnector{conn: conn})
	t.Cleanup(func() { db.Close() })
	c, err := catalog.New(db, nil, config.Catalog{}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestShowTrajectoriesSeasonRowsErrFailsClosed(t *testing.T) {
	c := newTrajStubCatalog(t, true, false)
	out, err := c.ShowTrajectories(context.Background(), []core.RatingKey{core.ShowKey(100)})
	if !errors.Is(err, errIter) {
		t.Fatalf("err = %v, want errors.Is(errIter)", err)
	}
	if out != nil {
		t.Fatalf("map = %v, want nil on core cursor failure", out)
	}
}

func TestShowTrajectoriesEpisodeRowsErrKeepsSeasons(t *testing.T) {
	c := newTrajStubCatalog(t, false, true)
	out, err := c.ShowTrajectories(context.Background(), []core.RatingKey{core.ShowKey(100)})
	if err != nil {
		t.Fatalf("err = %v, want nil (episode augmentation is optional)", err)
	}
	traj, ok := out[core.ShowKey(100)]
	if !ok {
		t.Fatalf("trajectory missing, map = %v", out)
	}
	if traj.RatedSeasons != 2 || traj.EarlyRating != 8.0 || traj.FinaleRating != 9.0 || traj.PeakRating != 9.0 {
		t.Fatalf("trajectory = %+v, want season-based {RatedSeasons:2 Early:8 Finale:9 Peak:9}", traj)
	}
	if !traj.SticksLanding {
		t.Fatalf("trajectory = %+v, want SticksLanding for Ended show with finale 9.0", traj)
	}
}
