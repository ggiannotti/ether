package store

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
)

// errIter is the sentinel iteration error delivered by the stub driver
// after one valid row: database/sql surfaces it through Rows.Err.
var errIter = errors.New("stub: iteration failed")

// logicalStubRows yields its rows, then errIter (when fail is set) or EOF.
type logicalStubRows struct {
	cols []string
	data [][]driver.Value
	idx  int
	fail bool
}

func (r *logicalStubRows) Columns() []string { return r.cols }
func (r *logicalStubRows) Close() error      { return nil }
func (r *logicalStubRows) Next(dest []driver.Value) error {
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

type logicalStubTx struct{}

func (logicalStubTx) Commit() error   { return nil }
func (logicalStubTx) Rollback() error { return nil }

// logicalStubConn serves the schema_migrations cursor with one valid
// row followed by errIter; every other query returns an empty cursor so
// ExportLogical can proceed (and would flush output) if the
// rows.Err check were missing.
type logicalStubConn struct{}

func (logicalStubConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("stub: prepare not supported")
}
func (logicalStubConn) Close() error              { return nil }
func (logicalStubConn) Begin() (driver.Tx, error) { return logicalStubTx{}, nil }
func (logicalStubConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return logicalStubTx{}, nil
}

func (logicalStubConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(query, "FROM schema_migrations") {
		return &logicalStubRows{
			cols: []string{"component", "version"},
			data: [][]driver.Value{{"catalog", int64(1)}},
			fail: true,
		}, nil
	}
	return &logicalStubRows{cols: []string{"name"}}, nil
}

type logicalStubDriver struct{}

func (logicalStubDriver) Open(string) (driver.Conn, error) { return logicalStubConn{}, nil }

type logicalStubConnector struct{}

func (logicalStubConnector) Connect(context.Context) (driver.Conn, error) {
	return logicalStubConn{}, nil
}
func (logicalStubConnector) Driver() driver.Driver { return logicalStubDriver{} }

func TestExportLogicalMigrationsRowsErrAbortsBeforeWrite(t *testing.T) {
	db := sql.OpenDB(logicalStubConnector{})
	t.Cleanup(func() { db.Close() })
	var buf bytes.Buffer
	err := ExportLogical(context.Background(), db, &buf)
	if !errors.Is(err, errIter) {
		t.Fatalf("err = %v, want errors.Is(errIter)", err)
	}
	if buf.Len() != 0 {
		t.Errorf("wrote %d bytes before aborting on rows.Err: %q", buf.Len(), buf.String())
	}
}
