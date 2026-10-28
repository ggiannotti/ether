package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestPostgresEnv names the environment variable that points package tests
// at a PostgreSQL server instead of in-memory SQLite, e.g.
//
//	ETHER_TEST_POSTGRES='postgres://ether:ether@127.0.0.1:5432/ether?sslmode=disable' go test ./...
//
// Each test database is a fresh, randomly named schema that is dropped when
// the *sql.DB is closed.
const TestPostgresEnv = "ETHER_TEST_POSTGRES"

// testSchemaPrefix marks throwaway schemas; the name embeds the creation
// time so abandoned schemas (a test that never closed its DB, a killed run)
// can be swept later.
const testSchemaPrefix = "ether_test_"

// OpenMemory is a migrated, empty database for tests: in-memory SQLite (a
// single connection), or a fresh PostgreSQL schema when TestPostgresEnv is
// set.
func OpenMemory(ctx context.Context) (*sql.DB, error) {
	return OpenPath(ctx, ":memory:")
}

// TB is the part of testing.TB that OpenTest uses.
type TB interface {
	Helper()
	Fatalf(format string, args ...any)
	Cleanup(func())
}

// OpenTest is OpenMemory for a test: it fails the test on error and closes
// (on PostgreSQL: drops) the database when the test ends.
func OpenTest(t TB) *sql.DB {
	t.Helper()
	db, err := OpenMemory(context.Background())
	if err != nil {
		t.Fatalf("store: open test database: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

var sweepOnce sync.Once

// OpenTestPostgres creates a uniquely named schema on the server at rawURL,
// migrates it and returns a *sql.DB whose connections use it as their
// search_path. Closing the DB drops the schema.
func OpenTestPostgres(ctx context.Context, rawURL string) (*sql.DB, error) {
	cfg, err := pgx.ParseConfig(rawURL)
	if err != nil {
		return nil, fmt.Errorf("store: invalid %s URL %s", TestPostgresEnv, RedactURL(rawURL))
	}
	sweepOnce.Do(func() { sweepTestSchemas(ctx, cfg) })
	var rnd [6]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return nil, err
	}
	schema := testSchemaPrefix + strconv.FormatInt(time.Now().Unix(), 10) + "_" + hex.EncodeToString(rnd[:])
	if err := adminExec(ctx, cfg, `CREATE SCHEMA `+schema); err != nil {
		return nil, fmt.Errorf("store: create test schema: %w", err)
	}
	conf := cfg.Copy()
	if conf.RuntimeParams == nil {
		conf.RuntimeParams = map[string]string{}
	}
	conf.RuntimeParams["search_path"] = schema
	conf.RuntimeParams["application_name"] = "ether-test"
	drop := func() error {
		dctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return adminExec(dctx, cfg, `SET lock_timeout = '5s'; DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	}
	db := sql.OpenDB(newPGConnector(conf, drop))
	// Keep the footprint small: tests that never Close their DB must not
	// exhaust the server's connections, so idle ones are reaped quickly.
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(2 * time.Second)
	if err := preparePostgres(ctx, db, slog.New(slog.NewTextHandler(io.Discard, nil)), nil); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func adminExec(ctx context.Context, cfg *pgx.ConnConfig, sql string) error {
	conn, err := pgx.ConnectConfig(ctx, cfg.Copy())
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, sql)
	return err
}

// sweepTestSchemas drops throwaway schemas older than two hours.
func sweepTestSchemas(ctx context.Context, cfg *pgx.ConnConfig) {
	conn, err := pgx.ConnectConfig(ctx, cfg.Copy())
	if err != nil {
		return
	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(ctx, `SELECT nspname FROM pg_namespace WHERE nspname LIKE 'ether\_test\_%'`)
	if err != nil {
		return
	}
	var stale []string
	cutoff := time.Now().Add(-2 * time.Hour).Unix()
	for rows.Next() {
		var name string
		if rows.Scan(&name) != nil {
			continue
		}
		ts, _, _ := strings.Cut(strings.TrimPrefix(name, testSchemaPrefix), "_")
		if n, err := strconv.ParseInt(ts, 10, 64); err == nil && n < cutoff {
			stale = append(stale, name)
		}
	}
	if err := rows.Err(); err != nil {
		slog.Default().Warn("sweep test schemas", "err", err)
	}
	rows.Close()
	for _, name := range stale {
		_, _ = conn.Exec(ctx, `SET lock_timeout = '2s'; DROP SCHEMA IF EXISTS `+pgx.Identifier{name}.Sanitize()+` CASCADE`)
	}
}
