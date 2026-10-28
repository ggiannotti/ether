package plex

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"image"
	_ "image/jpeg" // TMDB backdrops are JPEG
	_ "image/png"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/lru"
	"github.com/braedonsaunders/ether/internal/plex/model"
	"github.com/braedonsaunders/ether/internal/store"
)

// ---------------------------------------------------------------------------
// UltraBlurColors.
//
// The newer Plex apps paint a blurred gradient behind items from four
// corner colours PMS computes from the item's background art. Ether derives
// them from the TMDB backdrop: it fetches the small (w300) variant through
// the artwork cache, averages each quadrant and maps the averages into the
// dark, saturated range PMS's colours occupy (PMS picks palette colours, so
// the hues match but are not identical). Colours are computed lazily on
// worker goroutines, so listings never wait for them: an item gets its
// colours on the first render after they are ready. Results are kept in
// memory and in the plex_ultrablur table, keyed by the item and the
// backdrop path (a new backdrop is recomputed).

func init() {
	store.Register("plex", 1, `CREATE TABLE plex_ultrablur (
		key          INTEGER PRIMARY KEY,
		backdrop     TEXT    NOT NULL,
		top_left     TEXT    NOT NULL,
		top_right    TEXT    NOT NULL,
		bottom_right TEXT    NOT NULL,
		bottom_left  TEXT    NOT NULL,
		computed_at  INTEGER NOT NULL
	)`)
}

const (
	ultraBlurImageSize   = "w300"
	ultraBlurEntries     = 50000
	ultraBlurRetry       = time.Hour
	ultraBlurTimeout     = 30 * time.Second
	ultraBlurWorkers     = 2
	ultraBlurQueueLen    = 512
	ultraBlurPreloadMax  = 500 // keys per batched DB lookup
	ultraBlurDBQueryTime = 2 * time.Second
)

type ultraBlurEntry struct {
	key      core.RatingKey
	path     string
	colors   *model.UltraBlurColors // nil after a failure
	failedAt time.Time
}

type ultraBlurCache struct {
	db      *sql.DB // optional
	q       *bgQueue
	now     func() time.Time
	log     *slog.Logger
	compute func(ctx context.Context, path string) (model.UltraBlurColors, error)

	mu    sync.Mutex
	items *lru.Cache[core.RatingKey, *ultraBlurEntry]
}

func newUltraBlurCache(db *sql.DB, q *bgQueue, now func() time.Time, log *slog.Logger, compute func(context.Context, string) (model.UltraBlurColors, error)) *ultraBlurCache {
	return &ultraBlurCache{db: db, q: q, now: now, log: log, compute: compute, items: lru.New(lru.Options[core.RatingKey, *ultraBlurEntry]{MaxEntries: ultraBlurEntries})}
}

// lookup returns the colours of key's backdrop path, or nil when they are
// not known yet (scheduling their computation).
func (c *ultraBlurCache) lookup(key core.RatingKey, path string) *model.UltraBlurColors {
	if path == "" || key == 0 {
		return nil
	}
	c.mu.Lock()
	if e, ok := c.items.Peek(key); ok {
		if e.path == path {
			c.items.Get(key)
			if e.colors != nil {
				cp := *e.colors
				c.mu.Unlock()
				return &cp
			}
			if c.now().Sub(e.failedAt) < ultraBlurRetry {
				c.mu.Unlock()
				return nil
			}
		}
	}
	c.mu.Unlock()
	c.schedule(key, path)
	return nil
}

func (c *ultraBlurCache) schedule(key core.RatingKey, path string) {
	c.q.submit("ultrablur:"+strconv.FormatInt(int64(key), 10), func(ctx context.Context) {
		if colors, ok := c.loadOne(ctx, key, path); ok {
			c.set(key, path, &colors)
			return
		}
		cctx, cancel := context.WithTimeout(ctx, ultraBlurTimeout)
		defer cancel()
		colors, err := c.compute(cctx, path)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.Debug("ultrablur compute failed", "key", key, "path", path, "err", err)
			c.set(key, path, nil)
			return
		}
		c.set(key, path, &colors)
		c.save(ctx, key, path, colors)
	})
}

func (c *ultraBlurCache) set(key core.RatingKey, path string, colors *model.UltraBlurColors) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := &ultraBlurEntry{key: key, path: path, colors: colors}
	if colors == nil {
		e.failedAt = c.now()
	}
	c.items.Add(key, e)
}

// ultraBlurWant is one item a listing is about to render.
type ultraBlurWant struct {
	key  core.RatingKey
	path string
}

// preload fills the memory cache from the database for items it doesn't
// hold yet, in one query, so a restart doesn't recompute anything and a
// listing's first render already has colours computed earlier.
func (c *ultraBlurCache) preload(ctx context.Context, wants []ultraBlurWant) {
	if c.db == nil || len(wants) == 0 {
		return
	}
	c.mu.Lock()
	missing := map[core.RatingKey]string{}
	for _, w := range wants {
		if w.path == "" || w.key == 0 {
			continue
		}
		if e, ok := c.items.Peek(w.key); ok && e.path == w.path {
			continue
		}
		missing[w.key] = w.path
		if len(missing) >= ultraBlurPreloadMax {
			break
		}
	}
	c.mu.Unlock()
	if len(missing) == 0 {
		return
	}
	args := make([]any, 0, len(missing))
	for k := range missing {
		args = append(args, int64(k))
	}
	qctx, cancel := context.WithTimeout(ctx, ultraBlurDBQueryTime)
	defer cancel()
	rows, err := c.db.QueryContext(qctx, `SELECT key, backdrop, top_left, top_right, bottom_right, bottom_left FROM plex_ultrablur WHERE key IN (`+placeholders(len(args))+`)`, args...)
	if err != nil {
		if ctx.Err() == nil {
			c.log.Debug("ultrablur preload", "err", err)
		}
		return
	}
	defer rows.Close()
	for rows.Next() {
		var k int64
		var path string
		var u model.UltraBlurColors
		if err := rows.Scan(&k, &path, &u.TopLeft, &u.TopRight, &u.BottomRight, &u.BottomLeft); err != nil {
			return
		}
		if missing[core.RatingKey(k)] == path {
			c.set(core.RatingKey(k), path, &u)
		}
	}
	if err := rows.Err(); err != nil {
		c.log.Debug("ultrablur preload", "err", err)
	}
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat("?,", n-1) + "?"
}

func (c *ultraBlurCache) loadOne(ctx context.Context, key core.RatingKey, path string) (model.UltraBlurColors, bool) {
	var u model.UltraBlurColors
	if c.db == nil {
		return u, false
	}
	qctx, cancel := context.WithTimeout(ctx, ultraBlurDBQueryTime)
	defer cancel()
	err := c.db.QueryRowContext(qctx, `SELECT top_left, top_right, bottom_right, bottom_left FROM plex_ultrablur WHERE key = ? AND backdrop = ?`, int64(key), path).
		Scan(&u.TopLeft, &u.TopRight, &u.BottomRight, &u.BottomLeft)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) && ctx.Err() == nil {
			c.log.Debug("ultrablur load", "key", key, "err", err)
		}
		return u, false
	}
	return u, true
}

func (c *ultraBlurCache) save(ctx context.Context, key core.RatingKey, path string, u model.UltraBlurColors) {
	if c.db == nil {
		return
	}
	qctx, cancel := context.WithTimeout(ctx, ultraBlurDBQueryTime)
	defer cancel()
	_, err := c.db.ExecContext(qctx, `INSERT INTO plex_ultrablur (key, backdrop, top_left, top_right, bottom_right, bottom_left, computed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET backdrop = excluded.backdrop, top_left = excluded.top_left, top_right = excluded.top_right,
			bottom_right = excluded.bottom_right, bottom_left = excluded.bottom_left, computed_at = excluded.computed_at`,
		int64(key), path, u.TopLeft, u.TopRight, u.BottomRight, u.BottomLeft, c.now().Unix())
	if err != nil && ctx.Err() == nil {
		c.log.Warn("ultrablur save", "key", key, "err", err)
	}
}

// computeUltraBlur fetches a backdrop's small variant through the artwork
// cache and derives its corner colours.
func (s *Server) computeUltraBlur(ctx context.Context, path string) (model.UltraBlurColors, error) {
	// The artwork cache finishes a fetch even if its first caller goes
	// away (others may be waiting on it), so wait here with ctx: shutdown
	// must not block on a slow CDN.
	type result struct {
		img *cachedImage
		err error
	}
	ch := make(chan result, 1)
	go func() {
		img, err := s.images.get(ctx, s.d.ImageURL(path, ultraBlurImageSize), s.now())
		ch <- result{img, err}
	}()
	var img *cachedImage
	select {
	case r := <-ch:
		if r.err != nil {
			return model.UltraBlurColors{}, r.err
		}
		img = r.img
	case <-ctx.Done():
		return model.UltraBlurColors{}, ctx.Err()
	}
	decoded, _, err := image.Decode(bytes.NewReader(img.data))
	if err != nil {
		return model.UltraBlurColors{}, fmt.Errorf("decode %s: %w", path, err)
	}
	return ultraBlurFromImage(decoded)
}

// ultraBlurFromImage averages each quadrant of img and tones the averages
// like PMS's UltraBlur palette.
func ultraBlurFromImage(img image.Image) (model.UltraBlurColors, error) {
	b := img.Bounds()
	w, h := b.Dx(), b.Dy()
	if w < 2 || h < 2 {
		return model.UltraBlurColors{}, errors.New("image too small")
	}
	type rect struct{ x0, y0, x1, y1 int }
	quads := [4]rect{
		{0, 0, w / 2, h / 2}, // top left
		{w / 2, 0, w, h / 2}, // top right
		{w / 2, h / 2, w, h}, // bottom right
		{0, h / 2, w / 2, h}, // bottom left
	}
	var hex [4]string
	for i, q := range quads {
		var r, g, bl, n float64
		for y := q.y0; y < q.y1; y++ {
			for x := q.x0; x < q.x1; x++ {
				cr, cg, cb, _ := img.At(b.Min.X+x, b.Min.Y+y).RGBA()
				r += float64(cr)
				g += float64(cg)
				bl += float64(cb)
				n++
			}
		}
		// RGBA() is 16-bit per channel.
		hex[i] = ultraBlurTone(r/n/257, g/n/257, bl/n/257)
	}
	return model.UltraBlurColors{TopLeft: hex[0], TopRight: hex[1], BottomRight: hex[2], BottomLeft: hex[3]}, nil
}

// ultraBlurTone maps an average colour (0-255 channels) into the range PMS's
// UltraBlur colours occupy: lightness compressed to roughly 0.10-0.45 and
// saturation lifted, keeping the hue. Near-grey averages stay near grey.
func ultraBlurTone(r, g, b float64) string {
	h, s, l := rgbToHSL(r/255, g/255, b/255)
	l = 0.10 + 0.35*l
	if s > 0.03 {
		s = math.Min(1, s*1.4+0.15)
	}
	rr, gg, bb := hslToRGB(h, s, l)
	return fmt.Sprintf("%02x%02x%02x", clampByte(rr*255), clampByte(gg*255), clampByte(bb*255))
}

func clampByte(v float64) int {
	return int(math.Max(0, math.Min(255, math.Round(v))))
}

// rgbToHSL converts 0-1 channels to hue (0-360), saturation and lightness.
func rgbToHSL(r, g, b float64) (h, s, l float64) {
	mx := math.Max(r, math.Max(g, b))
	mn := math.Min(r, math.Min(g, b))
	l = (mx + mn) / 2
	if mx == mn {
		return 0, 0, l
	}
	d := mx - mn
	s = d / (1 - math.Abs(2*l-1))
	switch mx {
	case r:
		h = math.Mod((g-b)/d, 6)
	case g:
		h = (b-r)/d + 2
	default:
		h = (r-g)/d + 4
	}
	h *= 60
	if h < 0 {
		h += 360
	}
	return h, s, l
}

func hslToRGB(h, s, l float64) (r, g, b float64) {
	c := (1 - math.Abs(2*l-1)) * s
	x := c * (1 - math.Abs(math.Mod(h/60, 2)-1))
	m := l - c/2
	switch {
	case h < 60:
		r, g, b = c, x, 0
	case h < 120:
		r, g, b = x, c, 0
	case h < 180:
		r, g, b = 0, c, x
	case h < 240:
		r, g, b = 0, x, c
	case h < 300:
		r, g, b = x, 0, c
	default:
		r, g, b = c, 0, x
	}
	return r + m, g + m, b + m
}

// ultraBlurFor returns an item's colours when known (nil otherwise).
func (s *Server) ultraBlurFor(key core.RatingKey, backdrop string) *model.UltraBlurColors {
	if s.ultra == nil || backdrop == "" {
		return nil
	}
	return s.ultra.lookup(key, backdrop)
}

// preloadUltraBlur batch-loads stored colours for titles about to render.
func (s *Server) preloadUltraBlur(ctx context.Context, ts []core.Title) {
	if s.ultra == nil || s.ultra.db == nil {
		return
	}
	wants := make([]ultraBlurWant, 0, len(ts))
	for i := range ts {
		if ultraBlurType(ts[i].Type) {
			wants = append(wants, ultraBlurWant{ts[i].Key, ts[i].BackdropPath})
		}
	}
	s.ultra.preload(ctx, wants)
}

// ultraBlurType reports whether PMS sends UltraBlurColors for items of t.
func ultraBlurType(t core.MediaType) bool { return t == core.TypeMovie || t == core.TypeShow }
