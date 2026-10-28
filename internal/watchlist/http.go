package watchlist

import (
	"context"
	"encoding/json"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/go-chi/chi/v5"
	"net/http"
	"strconv"
	"time"
)

// Router must be mounted behind Ether's owner authentication middleware.
func (s *Service) Router() http.Handler {
	r := chi.NewRouter()
	reply := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	r.Get("/settings", func(w http.ResponseWriter, r *http.Request) {
		v, e := s.Settings(r.Context())
		if e != nil {
			http.Error(w, "Settings unavailable", 500)
			return
		}
		reply(w, v)
	})
	r.Put("/settings", func(w http.ResponseWriter, r *http.Request) {
		var v Settings
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 16384)).Decode(&v) != nil {
			http.Error(w, "Invalid settings", 400)
			return
		}
		if e := s.SaveSettings(r.Context(), v); e != nil {
			http.Error(w, e.Error(), 400)
			return
		}
		reply(w, v)
	})
	r.Route("/users/{id}", func(r chi.Router) {
		r.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				id, e := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
				if e != nil || id <= 0 {
					http.Error(w, "Invalid viewer", 400)
					return
				}
				u, e := s.user(r.Context(), id)
				if e != nil {
					http.Error(w, "Viewer unavailable", 404)
					return
				}
				next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), viewerKey{}, u)))
			})
		})
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			v, e := s.Items(r.Context(), viewer(r))
			if e != nil {
				http.Error(w, "Watchlist unavailable", 500)
				return
			}
			reply(w, v)
		})
		r.Post("/sync", func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), time.Minute)
			defer cancel()
			if e := s.Sync(ctx, viewer(r)); e != nil {
				http.Error(w, e.Error(), 502)
				return
			}
			itemsCtx, itemsCancel := context.WithTimeout(r.Context(), time.Minute)
			defer itemsCancel()
			v, e := s.Items(itemsCtx, viewer(r))
			if e != nil {
				http.Error(w, "Watchlist unavailable", 500)
				return
			}
			reply(w, v)
		})
		r.Post("/check", func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Minute)
			defer cancel()
			if e := s.Check(ctx, viewer(r)); e != nil {
				http.Error(w, "Availability check incomplete", 502)
				return
			}
			itemsCtx, itemsCancel := context.WithTimeout(r.Context(), time.Minute)
			defer itemsCancel()
			v, e := s.Items(itemsCtx, viewer(r))
			if e != nil {
				http.Error(w, "Watchlist unavailable", 500)
				return
			}
			reply(w, v)
		})
		r.Put("/items/{key}", func(w http.ResponseWriter, r *http.Request) {
			k, e := core.ParseRatingKey(chi.URLParam(r, "key"))
			if e != nil {
				http.Error(w, "Invalid title", 400)
				return
			}
			if e = s.Remind(r.Context(), viewer(r), k); e != nil {
				http.Error(w, "Title unavailable", 404)
				return
			}
			reply(w, map[string]bool{"saved": true})
		})
		r.Delete("/items/{key}", func(w http.ResponseWriter, r *http.Request) {
			k, e := core.ParseRatingKey(chi.URLParam(r, "key"))
			if e != nil {
				http.Error(w, "Invalid title", 400)
				return
			}
			if s.Remove(r.Context(), viewer(r).ID, k) != nil {
				http.Error(w, "Could not remove", 500)
				return
			}
			reply(w, map[string]bool{"removed": true})
		})
	})
	return r
}

type viewerKey struct{}

func viewer(r *http.Request) *core.User {
	u, _ := r.Context().Value(viewerKey{}).(*core.User)
	return u
}
