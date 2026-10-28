package personalize

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/recommend"
)

// TV Shows → Recommended: series-centric lenses. A series is a commitment,
// so these rows answer the questions a viewer has about one: is there
// more of what I watch, what did I leave unfinished, what should I start,
// can I binge it to the end, how long is it.

// TMDB series statuses.
const (
	statusEnded     = "Ended"
	statusCanceled  = "Canceled"
	statusReturning = "Returning Series"
)

// showEvidence is the viewer's evidence for series they watched at least
// one episode of, most recent first.
func (b *builder) showEvidence() []Evidence {
	var out []Evidence
	for _, e := range b.taste.Evidence {
		if e.Title.Type == core.TypeShow && e.Weight > 0 && e.Completions > 0 {
			out = append(out, e)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].At.Equal(out[j].At) {
			return out[i].At.After(out[j].At)
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// newSeasonWindow is how long after its premiere a season counts as new.
const newSeasonWindow = 120 * 24 * time.Hour

// genNewSeasons: series the viewer watched with a season that premiered
// recently and that they haven't started.
func (b *builder) genNewSeasons() []*candRow {
	if b.generic || b.taste == nil || b.e.state == nil {
		return nil
	}
	type cand struct {
		show   core.Title
		season int
		at     time.Time
		w      float64
	}
	var cands []cand
	var firstEps []core.RatingKey
	for i, e := range b.showEvidence() {
		if i >= 20 || b.ctx.Err() != nil {
			break
		}
		seasons, err := b.e.catalog.Children(b.ctx, e.Key)
		if err != nil {
			continue
		}
		best := core.Title{}
		for _, s := range seasons {
			if s.Season < 2 || s.Released.IsZero() || s.Released.After(b.now) || b.now.Sub(s.Released) > newSeasonWindow {
				continue
			}
			if s.Season > best.Season {
				best = s
			}
		}
		if best.Season == 0 || !recommend.ContentRatingAllowed(e.Title.ContentRating, b.user.MaxContentRating) {
			continue
		}
		// Only for viewers caught up on what came before (most of the
		// earlier seasons' episodes); the rest is Finish What You Started.
		prior := 0
		for _, s := range seasons {
			if s.Season >= 1 && s.Season < best.Season {
				prior += s.ChildCount
			}
		}
		if prior > 0 && 4*e.Completions < 3*prior {
			continue
		}
		cands = append(cands, cand{show: e.Title, season: best.Season, at: best.Released, w: e.Weight})
		firstEps = append(firstEps, core.EpisodeKey(e.Key.TMDB(), best.Season, 1))
	}
	if len(cands) == 0 {
		return nil
	}
	started, err := b.e.state.GetMany(b.ctx, b.user.ID, firstEps)
	if err != nil {
		return nil
	}
	var items []candItem
	for i, c := range cands {
		if ws := started[firstEps[i]]; ws.ViewCount > 0 || ws.ViewOffset > 0 {
			continue
		}
		days := int(b.now.Sub(c.at).Hours() / 24)
		when := "premiered today"
		switch {
		case days == 1:
			when = "premiered yesterday"
		case days > 1 && days < 14:
			when = fmt.Sprintf("premiered %d days ago", days)
		case days >= 14:
			when = "premiered " + releasedPhrase(c.at, b.now)
		}
		score := 0.6*math.Exp(-float64(days)/45) + 0.4*clamp(c.w/1.5, 0, 1)
		items = append(items, candItem{t: c.show, base: score, fit: clamp(c.w, 0, 1), qual: quality(&c.show),
			reason: fmt.Sprintf("Season %d %s", c.season, when)})
	}
	sortItems(items)
	var shows []core.Title
	for _, it := range items {
		shows = append(shows, it.t)
	}
	b.memo["newseasons"] = shows // Finish What You Started leaves these to this row
	r := newRow(KindNewSeasons, "newseasons", "New Seasons of Your Shows", "Series you watched that have a new season out you haven't started.", items)
	r.minItems = 1
	return []*candRow{r}
}

// stalledAfter is how long a started series must sit untouched before it
// is "unfinished" rather than "in progress" (Up Next covers those).
const stalledAfter = 14 * 24 * time.Hour

// genFinishShow: series the viewer started, stalled on and hasn't
// finished, closest to finished first.
func (b *builder) genFinishShow() []*candRow {
	if b.generic || b.taste == nil {
		return nil
	}
	newSeason := map[core.RatingKey]bool{}
	for _, t := range b.memo["newseasons"] {
		newSeason[t.Key] = true
	}
	var items []candItem
	for _, e := range b.showEvidence() {
		t := e.Title
		if t.LeafCount <= 0 || e.Completions >= t.LeafCount || e.At.IsZero() || newSeason[t.Key] {
			continue
		}
		idle := b.now.Sub(e.At)
		if idle < stalledAfter || idle > 540*24*time.Hour {
			continue
		}
		if !recommend.ContentRatingAllowed(t.ContentRating, b.user.MaxContentRating) {
			continue
		}
		progress := float64(e.Completions) / float64(t.LeafCount)
		items = append(items, candItem{t: t, base: progress, fit: clamp(e.Weight, 0, 1), qual: quality(&t),
			reason: fmt.Sprintf("%d of %d episodes watched · last watched%s", e.Completions, t.LeafCount, agoPhrase(e.At, b.now))})
	}
	sortItems(items)
	r := newRow(KindFinishShow, "finish", "Finish What You Started",
		"Series you started and haven't touched in two weeks or more, closest to finished first.", items)
	r.keepOrder, r.minItems = true, 1
	return []*candRow{r}
}

// tvCandidates is the TV lenses' shared pool, with the details that decide
// them (status, seasons, episodes) fetched for the best-fitting titles
// that lack them.
func (b *builder) tvCandidates() []core.Title {
	if v, ok := b.memo["tvcands"]; ok {
		return v
	}
	src := append([]core.Title{}, b.typedPool(core.TypeShow)...)
	src = append(src, b.browse(core.BrowseQuery{Filter: core.Filter{Type: core.TypeShow, MinRating: 7}, Sort: core.SortPopularity, Desc: true, Limit: 300})...)
	b.loadStatuses(src)
	seen := map[core.RatingKey]bool{}
	var out []core.Title
	for i := range src {
		t := &src[i]
		if t.Type != core.TypeShow || seen[t.Key] || !b.fresh(t) || !b.notable(t) {
			continue
		}
		seen[t.Key] = true
		out = append(out, *t)
	}
	sort.SliceStable(out, func(i, j int) bool {
		fi, _ := b.fit(&out[i])
		fj, _ := b.fit(&out[j])
		return fi+0.5*quality(&out[i]) > fj+0.5*quality(&out[j])
	})
	if len(out) > 60 {
		out = out[:60]
	}
	out = b.hydrate(out, func(t *core.Title) bool { return t.Status == "" || t.ChildCount <= 0 || t.LeafCount <= 0 })
	b.memo["tvcands"] = out
	return out
}

func seasonsPhrase(t *core.Title) string {
	switch {
	case t.ChildCount > 1 && t.LeafCount > 0:
		return fmt.Sprintf("%d seasons, %d episodes", t.ChildCount, t.LeafCount)
	case t.LeafCount > 0:
		return fmt.Sprintf("%d episodes", t.LeafCount)
	case t.ChildCount > 1:
		return fmt.Sprintf("%d seasons", t.ChildCount)
	}
	return ""
}

func ended(t *core.Title) bool {
	return strings.EqualFold(t.Status, statusEnded) || strings.EqualFold(t.Status, statusCanceled)
}

func limited(t *core.Title) bool {
	for _, k := range t.Keywords {
		switch strings.ToLower(k) {
		case "miniseries", "mini-series", "limited series":
			return true
		}
	}
	return strings.EqualFold(t.Status, statusEnded) && t.ChildCount == 1
}

// genStartNew: series the viewer hasn't started, by fit.
func (b *builder) genStartNew() []*candRow {
	if !b.personal() {
		return nil
	}
	items := b.rankType(b.tvCandidates(), core.TypeShow, func(t *core.Title, _, _ int, fit float64, why string) (float64, string) {
		r := "Start with the pilot"
		if s := seasonsPhrase(t); s != "" {
			r += " · " + s
		}
		return 0.55*fit + 0.25*quality(t) + 0.2*popularity(t), withWhy(r, why)
	})
	kept := items[:0:0]
	for _, it := range items {
		if it.fit >= 0.05 {
			kept = append(kept, it)
		}
	}
	r := newRow(KindStartNew, "startnew", "Start Something New", "Series you haven't started that fit your taste: start with the pilot.", kept)
	r.minItems = 4
	return []*candRow{r}
}

// genBinge: ended multi-season series that fit.
func (b *builder) genBinge() []*candRow {
	if !b.personal() {
		return nil
	}
	var in []core.Title
	for _, t := range b.tvCandidates() {
		if ended(&t) && t.ChildCount >= 2 && !limited(&t) {
			in = append(in, t)
		}
	}
	items := b.rankType(in, core.TypeShow, func(t *core.Title, _, _ int, fit float64, why string) (float64, string) {
		return 0.55*fit + 0.35*quality(t) + 0.1*popularity(t), withWhy("Complete · "+seasonsPhrase(t), why)
	})
	kept := items[:0:0]
	for _, it := range items {
		if it.fit >= 0.05 {
			kept = append(kept, it)
		}
	}
	r := newRow(KindBinge, "binge", "Complete Series to Binge", "Ended series with every season out, that fit your taste: no waiting for what happens next.", kept)
	r.minItems = 4
	return []*candRow{r}
}

// genLimited: one-season stories with an ending.
func (b *builder) genLimited() []*candRow {
	if !b.personal() {
		return nil
	}
	var in []core.Title
	for _, t := range b.tvCandidates() {
		if limited(&t) {
			in = append(in, t)
		}
	}
	items := b.rankType(in, core.TypeShow, func(t *core.Title, _, _ int, fit float64, why string) (float64, string) {
		r := "Limited series"
		if t.LeafCount > 0 {
			r = fmt.Sprintf("Limited series · %d episodes", t.LeafCount)
		}
		return 0.55*fit + 0.35*quality(t) + 0.1*popularity(t), withWhy(r, why)
	})
	kept := items[:0:0]
	for _, it := range items {
		if it.fit >= 0.0 {
			kept = append(kept, it)
		}
	}
	r := newRow(KindLimited, "limited", "Limited Series for You", "One-season stories with an ending, ranked by how well they fit you.", kept)
	r.minItems = 4
	return []*candRow{r}
}

// genAiringTaste: series in an active run that match the viewer: airing an
// episode today, or returning series that premiered in the last two years.
func (b *builder) genAiringTaste() []*candRow {
	if !b.personal() {
		return nil
	}
	today := map[core.RatingKey]bool{}
	src := b.list("airing_today", core.TypeShow, 40)
	for _, t := range src {
		today[t.Key] = true
	}
	for _, t := range b.tvCandidates() {
		if strings.EqualFold(t.Status, statusReturning) && !t.Released.IsZero() && b.now.Sub(t.Released) <= 2*365*24*time.Hour {
			src = append(src, t)
		}
	}
	items := b.rankType(src, core.TypeShow, func(t *core.Title, _, _ int, fit float64, why string) (float64, string) {
		r := "Returning series · premiered " + t.Released.Format("2006")
		boost := 0.0
		if today[t.Key] {
			r, boost = "New episode today", 0.1
		}
		return 0.6*fit + 0.2*quality(t) + 0.2*popularity(t) + boost, withWhy(r, why)
	})
	kept := items[:0:0]
	for _, it := range items {
		if it.fit >= 0.08 {
			kept = append(kept, it)
		}
	}
	r := newRow(KindAiringTaste, "airing.taste", "Ongoing Series for You",
		"Series in an active run (an episode airing today, or returning and recently premiered) that match your taste.", kept)
	r.minItems = 4
	return []*candRow{r}
}

// genNetwork: more series from the networks / studios the viewer keeps
// watching.
func (b *builder) genNetwork() []*candRow {
	if !b.personal() {
		return nil
	}
	weight := map[string]float64{}
	shows := map[string]int{}
	for _, e := range b.showEvidence() {
		if e.Completions < 2 {
			continue
		}
		for i, s := range e.Title.Studios {
			if i >= 2 || s == "" {
				break
			}
			weight[s] += e.Weight
			shows[s]++
		}
	}
	var studios []string
	for s, n := range shows {
		if n >= 2 {
			studios = append(studios, s)
		}
	}
	sort.Slice(studios, func(i, j int) bool {
		if weight[studios[i]] != weight[studios[j]] {
			return weight[studios[i]] > weight[studios[j]]
		}
		return studios[i] < studios[j]
	})
	var rows []*candRow
	for _, s := range studios {
		if len(rows) >= 3 {
			break
		}
		src := b.browse(core.BrowseQuery{Filter: core.Filter{Type: core.TypeShow, Studio: s}, Sort: core.SortPopularity, Desc: true, Limit: 100})
		var in []core.Title
		for i := range src {
			for _, x := range src[i].Studios {
				if strings.EqualFold(x, s) {
					in = append(in, src[i])
					break
				}
			}
		}
		studio := s
		items := b.rankType(in, core.TypeShow, func(t *core.Title, _, _ int, fit float64, why string) (float64, string) {
			return 0.55*fit + 0.3*quality(t) + 0.15*popularity(t), withWhy("From "+studio, why)
		})
		if len(items) < 4 {
			continue
		}
		r := newRow(KindNetwork, "network."+genreSlug(s), "More from "+s,
			fmt.Sprintf("You've watched %d %s series; more from them you haven't started.", shows[s], s), items)
		r.minItems = 4
		r.scale = 0.85 + 0.15*clamp(weight[s]/3, 0, 1)
		rows = append(rows, r)
	}
	return rows
}

type trajectorySource interface {
	ShowTrajectories(ctx context.Context, shows []core.RatingKey) (map[core.RatingKey]core.ShowTrajectory, error)
}

type standaloneEpisodeSource interface {
	StandaloneEpisodes(ctx context.Context, minRating float64, limit int) ([]core.Title, error)
}

// genSticksLanding: completed multi-season series that ended on their own
// terms (Status == "Ended", never "Canceled") and whose finale season held or
// exceeded the series' early rating bar.
func (b *builder) genSticksLanding() []*candRow {
	if !b.personal() {
		return nil
	}
	cands := b.tvCandidates()
	var endedKeys []core.RatingKey
	for _, t := range cands {
		if strings.EqualFold(t.Status, statusEnded) && t.ChildCount >= 2 && !limited(&t) {
			endedKeys = append(endedKeys, t.Key)
		}
	}
	if len(endedKeys) == 0 {
		return nil
	}
	var trajs map[core.RatingKey]core.ShowTrajectory
	if ts, ok := b.e.catalog.(trajectorySource); ok {
		var err error
		trajs, err = ts.ShowTrajectories(b.ctx, endedKeys)
		if err != nil {
			// Fail-closed upstream loses every trajectory; the fallback
			// below (acclaimed Ended series) still applies, but degraded.
			b.e.log.Debug("personalize: show trajectories failed", "err", err)
		}
	}
	var in []core.Title
	for _, t := range cands {
		if !strings.EqualFold(t.Status, statusEnded) || t.ChildCount < 2 || limited(&t) {
			continue
		}
		if tr, ok := trajs[t.Key]; ok && tr.RatedSeasons > 0 {
			if tr.SticksLanding {
				in = append(in, t)
			}
			continue
		}
		// Fallback when season-level ratings are not mirrored yet: require a
		// high Bayesian series rating on an explicitly Ended (not Canceled) run.
		if b.acclaimed(&t, 1.0, 7.6) {
			in = append(in, t)
		}
	}
	items := b.rankType(in, core.TypeShow, func(t *core.Title, _, _ int, fit float64, why string) (float64, string) {
		tr := trajs[t.Key]
		r := "Sticks the landing · " + seasonsPhrase(t)
		bonus := 0.0
		if tr.FinaleRating > 0 {
			r = fmt.Sprintf("Finale season rated %.1f · %s", tr.FinaleRating, seasonsPhrase(t))
			bonus = 0.05 * clamp((tr.FinaleRating-7.5)/2.0, 0, 1)
		}
		return 0.55*fit + 0.35*quality(t) + 0.1*popularity(t) + bonus, withWhy(r, why)
	})
	kept := items[:0:0]
	for _, it := range items {
		if it.fit >= 0.05 {
			kept = append(kept, it)
		}
	}
	r := newRow(KindSticksLanding, "stickslanding", "Series That Stick the Landing",
		"Completed series that ended strong without a late-season collapse or cliffhanger cancellation.", kept)
	r.minItems = 4
	return []*candRow{r}
}

// genStandalone: acclaimed standalone and anthology episodes that can be
// watched tonight in a single sitting without committing to a full series.
func (b *builder) genStandalone() []*candRow {
	if !b.personal() {
		return nil
	}
	ss, ok := b.e.catalog.(standaloneEpisodeSource)
	if !ok {
		return nil
	}
	eps, err := ss.StandaloneEpisodes(b.ctx, 8.2, 40)
	if err != nil || len(eps) == 0 {
		return nil
	}
	b.loadStatuses(eps)
	var items []candItem
	seen := map[core.RatingKey]bool{}
	for _, ep := range eps {
		if ep.Type != core.TypeEpisode || seen[ep.Key] {
			continue
		}
		if !ep.Released.IsZero() && ep.Released.After(b.now) {
			continue
		}
		if !recommend.ContentRatingAllowed(ep.ContentRating, b.user.MaxContentRating) {
			continue
		}
		if ws := b.statuses[ep.Key]; ws.ViewCount > 0 || ws.ViewOffset > 0 {
			continue
		}
		seen[ep.Key] = true
		fit, why := b.fit(&ep)
		if fit < -0.15 {
			continue
		}
		show := ep.ShowTitle
		if show == "" {
			show = "Anthology"
		}
		r := fmt.Sprintf("%s S%dE%d · Rated %.1f", show, ep.Season, ep.Episode, ep.Rating)
		if why != "" {
			r += " · " + why
		}
		score := 0.45*clamp(fit, 0, 1) + 0.55*clamp((ep.Rating-7.5)/2.5, 0, 1)
		items = append(items, candItem{t: ep, base: score, fit: fit, qual: quality(&ep), reason: r})
	}
	sortItems(items)
	if len(items) < 4 {
		return nil
	}
	r := newRow(KindStandalone, "standalone", "Standalone & Anthology Episodes",
		"Acclaimed standalone and anthology episodes you can watch in one sitting with no multi-season commitment.", capItems(items, 30))
	r.minItems = 4
	return []*candRow{r}
}
