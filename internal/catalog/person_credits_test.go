package catalog_test

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/braedonsaunders/ether/internal/catalog"
	"github.com/braedonsaunders/ether/internal/config"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/store"
	"github.com/braedonsaunders/ether/internal/tmdb"
)

// installSecondCreditFailure makes the second credit insert (ord=1) for
// person 777 abort, so ensurePersonCredits' transaction must roll back.
// SQLite-only: exercita o caminho via trigger RAISE(ABORT) (store.OpenMemory).
func installSecondCreditFailure(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `CREATE TRIGGER fail_second_person_credit BEFORE INSERT ON catalog_credits WHEN NEW.person_tmdb=777 AND NEW.ord=1 BEGIN SELECT RAISE(ABORT, 'stub: second credit insert fails'); END`); err != nil {
		t.Fatalf("install credit failure trigger: %v", err)
	}
}

func dropSecondCreditFailure(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `DROP TRIGGER IF EXISTS fail_second_person_credit`); err != nil {
		t.Fatalf("drop credit failure trigger: %v", err)
	}
}

func countPersonCredits(t *testing.T, db *sql.DB, personID int64) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM catalog_credits WHERE person_tmdb=?`, personID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// A failing second credit insert must roll back the whole transaction
// (zero rows persisted), and the failed hydration must not be stamped
// as done: once the cause is removed, a retry must persist the credits.
func TestEnsurePersonCreditsInsertFailureRollsBackAndRetries(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/person/777/combined_credits":
			fmt.Fprint(w, `{"id":777,"cast":[
				{"id":601,"media_type":"movie","title":"Credit One","release_date":"2020-01-01","popularity":50.0,"vote_average":8.0,"vote_count":5000,"original_language":"en","character":"Lead"},
				{"id":602,"media_type":"movie","title":"Credit Two","release_date":"2021-01-01","popularity":50.0,"vote_average":8.0,"vote_count":5000,"original_language":"en","character":"Support"}
			],"crew":[]}`)
		case "/person/777":
			fmt.Fprint(w, `{"id":777,"name":"Retry Person","profile_path":"/rp.jpg"}`)
		default:
			w.WriteHeader(404)
			fmt.Fprint(w, `{"status_message":"not found"}`)
		}
	}))
	t.Cleanup(srv.Close)

	db, err := store.OpenMemory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	client := tmdb.New("testkey", slog.Default(), tmdb.WithBaseURL(srv.URL), tmdb.WithCacheTTL(0))
	t.Cleanup(client.Close)
	c, err := catalog.New(db, client, config.Catalog{Language: "en-US", Region: "US", MinPopularity: 5, MinVotes: 100, RefreshInterval: config.Duration(24 * time.Hour)}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}

	installSecondCreditFailure(t, db)
	if _, err := c.Browse(ctx, core.BrowseQuery{Filter: core.Filter{Type: core.TypeMovie, Person: 777}}); err != nil {
		t.Fatalf("browse with failing credit insert: %v", err)
	}
	if n := countPersonCredits(t, db, 777); n != 0 {
		t.Fatalf("credits persisted = %d, want 0 (insert failure must roll back the transaction)", n)
	}

	// Removing the cause must allow a retry to persist the credits: the
	// failed attempt must not stamp personHydrated for 24h.
	dropSecondCreditFailure(t, db)
	page, err := c.Browse(ctx, core.BrowseQuery{Filter: core.Filter{Type: core.TypeMovie, Person: 777}})
	if err != nil {
		t.Fatalf("browse retry: %v", err)
	}
	if n := countPersonCredits(t, db, 777); n != 2 {
		t.Fatalf("credits persisted after retry = %d, want 2", n)
	}
	if page.Total != 2 {
		t.Fatalf("browse after retry total = %d, want 2 (%+v)", page.Total, page)
	}
}
