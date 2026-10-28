package cardigann

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/braedonsaunders/ether/internal/config"
	"github.com/braedonsaunders/ether/internal/store"
)

func upgradedSealer(t *testing.T) *config.Store {
	t.Helper()
	s, err := config.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UpgradeCredentials(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

func rawCookies(t *testing.T, db *sql.DB, id string) (string, bool) {
	t.Helper()
	var raw string
	err := db.QueryRow(`SELECT cookies FROM cardigann_sessions WHERE indexer_id=?`, id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false
	}
	if err != nil {
		t.Fatal(err)
	}
	return raw, true
}

func TestSessionCookiesSealedAndBoundToOrigin(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenMemory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &sessionStore{db: db, sealer: upgradedSealer(t), now: time.Now}
	if _, err := s.save(ctx, "ix", "https://site.test", []savedCookie{{Name: "uid", Value: "session-cookie-value"}}, "UA"); err != nil {
		t.Fatal(err)
	}
	if raw, _ := rawCookies(t, db, "ix"); strings.Contains(raw, "session-cookie-value") || !config.IsSealed(raw) {
		t.Fatalf("cookies at rest: %s", raw)
	}
	row, ok, err := s.load(ctx, "ix", "https://site.test")
	if err != nil || !ok || row.Cookies[0].Value != "session-cookie-value" {
		t.Fatalf("load: %+v %v", row, err)
	}
	// The indexer repointed to another site: its cookies do not open, and
	// the runner treats that as no session (the next login replaces it).
	if _, _, err := s.load(ctx, "ix", "https://evil.test"); !errors.Is(err, errSessionUnreadable) {
		t.Fatalf("cookies opened for another origin: %v", err)
	}
	evil, _ := url.Parse("https://evil.test/")
	r := &Runner{id: "ix", site: evil, sessions: s, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if _, ok, err := r.loadSession(ctx); ok || err != nil {
		t.Fatalf("runner reused another origin's session: ok=%v err=%v", ok, err)
	}
}

func TestSessionCorruptSealedCookiesUnreadable(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenMemory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &sessionStore{db: db, sealer: upgradedSealer(t), now: time.Now}
	if _, err := s.save(ctx, "ix", "https://site.test", []savedCookie{{Name: "uid", Value: "session-cookie-value"}}, "UA"); err != nil {
		t.Fatal(err)
	}
	// A valid seal around corrupted JSON: OpenWith succeeds, but the
	// payload cannot unmarshal, so the session is unreadable.
	corrupt, err := config.SealWith(s.sealer, "{corrupt-json", cookieContext("ix", "https://site.test")...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE cardigann_sessions SET cookies=? WHERE indexer_id=?`, corrupt, "ix"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.load(ctx, "ix", "https://site.test"); !errors.Is(err, errSessionUnreadable) {
		t.Fatalf("corrupt sealed cookies: %v", err)
	}
	// The runner treats an unreadable session as no session, so the next
	// login replaces it.
	site, _ := url.Parse("https://site.test/")
	r := &Runner{id: "ix", site: site, sessions: s, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if _, ok, err := r.loadSession(ctx); ok || err != nil {
		t.Fatalf("runner reused corrupt session: ok=%v err=%v", ok, err)
	}
}

func TestSessionCookieMigration(t *testing.T) {
	ctx := context.Background()
	db, err := store.OpenMemory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	legacy := &sessionStore{db: db, now: time.Now}
	for _, id := range []string{"lazy", "ix", "unknown-origin", "removed"} {
		if _, err := legacy.save(ctx, id, "", []savedCookie{{Name: "uid", Value: "plain-" + id}}, "UA"); err != nil {
			t.Fatal(err)
		}
	}
	s := &sessionStore{db: db, sealer: upgradedSealer(t), now: time.Now}
	// A plaintext row read by a configured runner is resealed in place.
	row, ok, err := s.load(ctx, "lazy", "https://lazy.test")
	if err != nil || !ok || row.Cookies[0].Value != "plain-lazy" || row.Version != 1 {
		t.Fatalf("legacy load: %+v %v", row, err)
	}
	if raw, _ := rawCookies(t, db, "lazy"); strings.Contains(raw, "plain-lazy") {
		t.Fatal("legacy row not resealed on read")
	}
	n, waiting, err := s.migrate(ctx, map[string]string{"lazy": "https://lazy.test", "ix": "https://ix.test", "unknown-origin": ""})
	if err != nil || n != 2 || len(waiting) != 1 || waiting[0] != "unknown-origin" {
		t.Fatalf("migrate: %d %v %v", n, waiting, err)
	}
	if raw, _ := rawCookies(t, db, "ix"); strings.Contains(raw, "plain-ix") {
		t.Fatal("configured row not sealed")
	}
	if raw, _ := rawCookies(t, db, "unknown-origin"); raw == "" || config.IsSealed(raw) {
		t.Fatal("row with an unknown origin should wait for a later run")
	}
	if _, found := rawCookies(t, db, "removed"); found {
		t.Fatal("plaintext cookies of a removed indexer kept")
	}
	// Sealed rows (even ones this key cannot open) are never rewritten.
	before, _ := rawCookies(t, db, "ix")
	other := &sessionStore{db: db, sealer: upgradedSealer(t), now: time.Now}
	if _, _, err := other.migrate(ctx, map[string]string{}); err != nil {
		t.Fatal(err)
	}
	if after, found := rawCookies(t, db, "ix"); !found || after != before {
		t.Fatal("sealed row rewritten or deleted")
	}
	if n, _, err := s.migrate(ctx, map[string]string{"lazy": "https://lazy.test", "ix": "https://ix.test", "unknown-origin": ""}); err != nil || n != 0 {
		t.Fatalf("rerun: %d %v", n, err)
	}
}
