package store

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Logical dumps are engine-neutral JSON lines, used to back up PostgreSQL
// (where VACUUM INTO and file copies do not apply) and usable to move data
// between SQLite and PostgreSQL. The stream is:
//
//	{"format":"ether-logical","version":1,"dialect":"postgres","created":"...","migrations":{"auth":1,...}}
//	{"table":"auth_users","columns":["id",...],"types":["integer",...]}
//	[1,"alice",...]                          one JSON array per row
//	...
//	{"end":true,"tables":42,"rows":1234}
//
// Tables appear parents-first (foreign keys), so a restore can insert in
// file order. Blob values are base64 strings. schema_migrations is carried
// in the header rather than as a table.
const logicalFormat = "ether-logical"

// LogicalHeader is the first line of a logical dump.
type LogicalHeader struct {
	Format     string         `json:"format"`
	Version    int            `json:"version"`
	Dialect    Dialect        `json:"dialect"`
	Created    time.Time      `json:"created"`
	Migrations map[string]int `json:"migrations"`
}

type logicalTable struct {
	Table   string   `json:"table"`
	Columns []string `json:"columns"`
	Types   []string `json:"types"`
}

type logicalEnd struct {
	End    bool  `json:"end"`
	Tables int   `json:"tables"`
	Rows   int64 `json:"rows"`
}

// Column kinds in a dump.
const (
	kindInteger = "integer"
	kindReal    = "real"
	kindText    = "text"
	kindBlob    = "blob"
)

func columnKind(dbType string) string {
	t := strings.ToUpper(dbType)
	switch {
	case strings.Contains(t, "INT"):
		return kindInteger
	case strings.Contains(t, "REAL"), strings.Contains(t, "FLOA"), strings.Contains(t, "DOUB"), strings.Contains(t, "NUMERIC"):
		return kindReal
	case strings.Contains(t, "BLOB"), strings.Contains(t, "BYTEA"):
		return kindBlob
	}
	return kindText
}

// queryer is satisfied by *sql.DB and *sql.Tx.
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func listTables(ctx context.Context, q queryer, d Dialect) ([]string, error) {
	query := `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite\_%' ESCAPE '\' ORDER BY name`
	if d == Postgres {
		query = `SELECT table_name FROM information_schema.tables WHERE table_schema=current_schema() AND table_type='BASE TABLE' ORDER BY table_name`
	}
	rows, err := q.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name != "schema_migrations" {
			out = append(out, name)
		}
	}
	return out, rows.Err()
}

// parentTables lists the tables table references through foreign keys.
func parentTables(ctx context.Context, q queryer, d Dialect, table string) ([]string, error) {
	query := `SELECT DISTINCT "table" FROM pragma_foreign_key_list(?)`
	if d == Postgres {
		query = `SELECT DISTINCT cl.relname FROM pg_constraint c JOIN pg_class cl ON cl.oid=c.confrelid
			WHERE c.contype='f' AND c.conrelid=(SELECT oid FROM pg_class WHERE relname=? AND relnamespace=current_schema()::regnamespace)`
	}
	rows, err := q.QueryContext(ctx, query, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// orderedTables returns tables sorted so referenced tables come first.
func orderedTables(ctx context.Context, q queryer, d Dialect) ([]string, error) {
	tables, err := listTables(ctx, q, d)
	if err != nil {
		return nil, err
	}
	deps := map[string][]string{}
	for _, t := range tables {
		if deps[t], err = parentTables(ctx, q, d, t); err != nil {
			return nil, fmt.Errorf("foreign keys of %s: %w", t, err)
		}
	}
	var out []string
	state := map[string]int{} // 1 visiting, 2 done
	var visit func(string)
	visit = func(t string) {
		if state[t] != 0 {
			return
		}
		state[t] = 1
		for _, p := range deps[t] {
			if p != t {
				visit(p)
			}
		}
		state[t] = 2
		out = append(out, t)
	}
	for _, t := range tables {
		visit(t)
	}
	return out, nil
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// ExportLogical writes a consistent logical dump of every table in db to w.
func ExportLogical(ctx context.Context, db *sql.DB, w io.Writer) error {
	d := DialectOf(db)
	opts := &sql.TxOptions{ReadOnly: true}
	if d == Postgres {
		opts.Isolation = sql.LevelRepeatableRead
	}
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	bw := bufio.NewWriterSize(w, 256<<10)
	enc := json.NewEncoder(bw)
	enc.SetEscapeHTML(false)

	h := LogicalHeader{Format: logicalFormat, Version: 1, Dialect: d, Created: time.Now().UTC(), Migrations: map[string]int{}}
	mrows, err := tx.QueryContext(ctx, `SELECT component, MAX(version) FROM schema_migrations GROUP BY component`)
	if err != nil {
		return err
	}
	for mrows.Next() {
		var c string
		var v int
		if err := mrows.Scan(&c, &v); err != nil {
			mrows.Close()
			return err
		}
		h.Migrations[c] = v
	}
	if err := mrows.Err(); err != nil {
		mrows.Close()
		return err
	}
	mrows.Close()
	if err := enc.Encode(h); err != nil {
		return err
	}
	tables, err := orderedTables(ctx, tx, d)
	if err != nil {
		return err
	}
	var total int64
	for _, t := range tables {
		n, err := exportTable(ctx, tx, enc, t)
		if err != nil {
			return fmt.Errorf("export %s: %w", t, err)
		}
		total += n
	}
	if err := enc.Encode(logicalEnd{End: true, Tables: len(tables), Rows: total}); err != nil {
		return err
	}
	return bw.Flush()
}

func exportTable(ctx context.Context, tx *sql.Tx, enc *json.Encoder, table string) (int64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT * FROM `+quoteIdent(table))
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	cts, err := rows.ColumnTypes()
	if err != nil {
		return 0, err
	}
	head := logicalTable{Table: table, Columns: cols, Types: make([]string, len(cols))}
	for i, ct := range cts {
		head.Types[i] = columnKind(ct.DatabaseTypeName())
	}
	if err := enc.Encode(head); err != nil {
		return 0, err
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	out := make([]any, len(cols))
	var n int64
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return n, err
		}
		for i, v := range vals {
			out[i] = encodeValue(v, head.Types[i])
		}
		if err := enc.Encode(out); err != nil {
			return n, err
		}
		n++
	}
	return n, rows.Err()
}

func encodeValue(v any, kind string) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		if kind == kindBlob {
			return base64.StdEncoding.EncodeToString(x)
		}
		return string(x)
	case bool:
		if x {
			return 1
		}
		return 0
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return strconv.FormatFloat(x, 'g', -1, 64)
		}
		return x
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	default:
		return x
	}
}

// ErrNotEmpty means a logical restore target already holds data.
var ErrNotEmpty = errors.New("store: restore target database is not empty")

// ImportLogical loads a logical dump into db, which must be migrated and
// hold no rows in any table the dump contains. It runs in one transaction.
func ImportLogical(ctx context.Context, db *sql.DB, r io.Reader) (err error) {
	d := DialectOf(db)
	dec := newLogicalDecoder(r)
	h, err := dec.header()
	if err != nil {
		return err
	}
	have, err := AppliedMigrations(ctx, db)
	if err != nil {
		return err
	}
	for c, v := range h.Migrations {
		if have[c] < v {
			return fmt.Errorf("store: backup has %s schema version %d but this server has %d; restore with a newer Ether", c, v, have[c])
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()
	targetTables, err := listTables(ctx, tx, d)
	if err != nil {
		return err
	}
	seeded := seededTables()
	for _, t := range targetTables {
		var any int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM (SELECT 1 FROM `+quoteIdent(t)+` LIMIT 1) x`).Scan(&any); err != nil {
			return err
		}
		if any > 0 && !seeded[t] {
			return fmt.Errorf("%w (table %s has rows)", ErrNotEmpty, t)
		}
	}
	known := map[string]bool{}
	for _, t := range targetTables {
		known[t] = true
	}
	var tables int
	var total int64
	for {
		head, end, err := dec.next()
		if err != nil {
			return err
		}
		if end != nil {
			if end.Tables != tables || end.Rows != total {
				return fmt.Errorf("store: dump trailer mismatch (tables %d/%d, rows %d/%d)", tables, end.Tables, total, end.Rows)
			}
			break
		}
		if !known[head.Table] {
			return fmt.Errorf("store: backup table %s does not exist on this server", head.Table)
		}
		kinds, err := targetKinds(ctx, tx, head.Table, head.Columns)
		if err != nil {
			return err
		}
		// Seed rows written by migrations (singleton settings) are replaced.
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+quoteIdent(head.Table)); err != nil {
			return err
		}
		n, err := importRows(ctx, tx, dec, head, kinds)
		if err != nil {
			return fmt.Errorf("restore %s: %w", head.Table, err)
		}
		tables++
		total += n
	}
	if d == Postgres {
		if err := resetIdentities(ctx, tx); err != nil {
			return err
		}
	}
	return tx.Commit()
}

var reSeedInsert = regexp.MustCompile(`(?i)\bINSERT\s+INTO\s+"?([A-Za-z_][A-Za-z0-9_]*)`)

// seededTables lists tables that migrations themselves insert rows into
// (singleton settings); a fresh database already holds those rows, so they
// do not make it "not empty" and the restore replaces them.
func seededTables() map[string]bool {
	out := map[string]bool{}
	for _, m := range registered() {
		for _, sql := range append([]string{m.sql}, mapValues(m.overrides)...) {
			for _, sm := range reSeedInsert.FindAllStringSubmatch(sql, -1) {
				out[sm[1]] = true
			}
		}
	}
	return out
}

func mapValues(m map[Dialect]string) []string {
	out := make([]string, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

func targetKinds(ctx context.Context, tx *sql.Tx, table string, cols []string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT * FROM `+quoteIdent(table)+` WHERE 1=0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	cts, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	byName := map[string]string{}
	for i, n := range names {
		byName[n] = columnKind(cts[i].DatabaseTypeName())
	}
	out := make([]string, len(cols))
	for i, c := range cols {
		k, ok := byName[c]
		if !ok {
			return nil, fmt.Errorf("store: backup column %s.%s does not exist on this server", table, c)
		}
		out[i] = k
	}
	return out, nil
}

func importRows(ctx context.Context, tx *sql.Tx, dec *logicalDecoder, head *logicalTable, kinds []string) (int64, error) {
	quoted := make([]string, len(head.Columns))
	marks := make([]string, len(head.Columns))
	for i, c := range head.Columns {
		quoted[i] = quoteIdent(c)
		marks[i] = "?"
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO `+quoteIdent(head.Table)+`(`+strings.Join(quoted, ",")+`) VALUES(`+strings.Join(marks, ",")+`)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	args := make([]any, len(head.Columns))
	var n int64
	for {
		row, ok, err := dec.row()
		if err != nil {
			return n, err
		}
		if !ok {
			return n, nil
		}
		if len(row) != len(head.Columns) {
			return n, fmt.Errorf("row %d has %d values, want %d", n+1, len(row), len(head.Columns))
		}
		for i, raw := range row {
			if args[i], err = decodeValue(raw, kinds[i]); err != nil {
				return n, fmt.Errorf("row %d column %s: %w", n+1, head.Columns[i], err)
			}
		}
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return n, err
		}
		n++
	}
}

func decodeValue(raw json.RawMessage, kind string) (any, error) {
	if string(raw) == "null" {
		return nil, nil
	}
	if len(raw) > 0 && raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		switch kind {
		case kindBlob:
			return base64.StdEncoding.DecodeString(s)
		case kindInteger:
			if v, err := strconv.ParseInt(s, 10, 64); err == nil {
				return v, nil
			}
		case kindReal:
			if v, err := strconv.ParseFloat(s, 64); err == nil {
				return v, nil
			}
		}
		return s, nil
	}
	num := json.Number(raw)
	switch kind {
	case kindInteger:
		if v, err := num.Int64(); err == nil {
			return v, nil
		}
		f, err := num.Float64()
		if err != nil {
			return nil, err
		}
		if f == math.Trunc(f) && math.Abs(f) < 1<<53 {
			return int64(f), nil
		}
		return f, nil
	case kindReal:
		return num.Float64()
	default:
		return num.String(), nil
	}
}

// resetIdentities moves every identity sequence past the restored ids.
func resetIdentities(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT table_name, column_name FROM information_schema.columns
		WHERE table_schema=current_schema() AND is_identity='YES' ORDER BY table_name`)
	if err != nil {
		return err
	}
	type col struct{ table, column string }
	var cols []col
	for rows.Next() {
		var c col
		if err := rows.Scan(&c.table, &c.column); err != nil {
			rows.Close()
			return err
		}
		cols = append(cols, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, c := range cols {
		q := `SELECT setval(pg_get_serial_sequence(?, ?), COALESCE(MAX(` + quoteIdent(c.column) + `), 0) + 1, false) FROM ` + quoteIdent(c.table)
		if _, err := tx.ExecContext(ctx, q, quoteIdent(c.table), c.column); err != nil {
			return fmt.Errorf("reset identity %s.%s: %w", c.table, c.column, err)
		}
	}
	return nil
}

// ValidateLogical reads a whole dump and checks its structure and trailer
// without touching a database. It returns the header.
func ValidateLogical(r io.Reader) (LogicalHeader, error) {
	dec := newLogicalDecoder(r)
	h, err := dec.header()
	if err != nil {
		return LogicalHeader{}, err
	}
	seen := map[string]bool{}
	var tables int
	var total int64
	for {
		head, end, err := dec.next()
		if err != nil {
			return h, err
		}
		if end != nil {
			if end.Tables != tables || end.Rows != total {
				return h, errors.New("store: logical dump is truncated or inconsistent")
			}
			return h, nil
		}
		if seen[head.Table] || len(head.Columns) != len(head.Types) || len(head.Columns) == 0 {
			return h, fmt.Errorf("store: malformed table header for %q", head.Table)
		}
		seen[head.Table] = true
		tables++
		for {
			row, ok, err := dec.row()
			if err != nil {
				return h, err
			}
			if !ok {
				break
			}
			if len(row) != len(head.Columns) {
				return h, fmt.Errorf("store: %s row has %d values, want %d", head.Table, len(row), len(head.Columns))
			}
			total++
		}
	}
}

// logicalDecoder reads the line-oriented dump.
type logicalDecoder struct {
	sc      *bufio.Scanner
	pending []byte // a line read by row() that starts the next section
}

func newLogicalDecoder(r io.Reader) *logicalDecoder {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), 256<<20)
	return &logicalDecoder{sc: sc}
}

func (d *logicalDecoder) line() ([]byte, error) {
	if d.pending != nil {
		l := d.pending
		d.pending = nil
		return l, nil
	}
	for d.sc.Scan() {
		if l := d.sc.Bytes(); len(strings.TrimSpace(string(l))) > 0 {
			return append([]byte(nil), l...), nil
		}
	}
	if err := d.sc.Err(); err != nil {
		return nil, err
	}
	return nil, io.ErrUnexpectedEOF
}

func (d *logicalDecoder) header() (LogicalHeader, error) {
	var h LogicalHeader
	l, err := d.line()
	if err != nil {
		return h, fmt.Errorf("store: empty logical dump: %w", err)
	}
	if err := json.Unmarshal(l, &h); err != nil || h.Format != logicalFormat {
		return h, errors.New("store: not an Ether logical dump")
	}
	if h.Version != 1 {
		return h, fmt.Errorf("store: unsupported logical dump version %d", h.Version)
	}
	return h, nil
}

// next returns the next table header, or the trailer.
func (d *logicalDecoder) next() (*logicalTable, *logicalEnd, error) {
	l, err := d.line()
	if err != nil {
		return nil, nil, fmt.Errorf("store: logical dump is truncated: %w", err)
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(l, &probe); err != nil {
		return nil, nil, fmt.Errorf("store: malformed logical dump line: %w", err)
	}
	if _, ok := probe["end"]; ok {
		var e logicalEnd
		if err := json.Unmarshal(l, &e); err != nil {
			return nil, nil, err
		}
		return nil, &e, nil
	}
	var t logicalTable
	if err := json.Unmarshal(l, &t); err != nil || t.Table == "" {
		return nil, nil, errors.New("store: malformed logical dump table header")
	}
	return &t, nil, nil
}

// row returns the next row of the current table; ok is false at the end of
// the table (the following header is kept for next).
func (d *logicalDecoder) row() ([]json.RawMessage, bool, error) {
	l, err := d.line()
	if err != nil {
		return nil, false, fmt.Errorf("store: logical dump is truncated: %w", err)
	}
	if len(l) == 0 || l[0] != '[' {
		d.pending = l
		return nil, false, nil
	}
	var row []json.RawMessage
	if err := json.Unmarshal(l, &row); err != nil {
		return nil, false, fmt.Errorf("store: malformed row: %w", err)
	}
	return row, true, nil
}
