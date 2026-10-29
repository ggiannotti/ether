// Package trackprefs persists viewer stream policy and show-specific choices.
package trackprefs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/langtag"
	"github.com/braedonsaunders/ether/internal/store"
)

func init() {
	store.Register("trackprefs", 1, `CREATE TABLE trackprefs_rules(user_id INTEGER PRIMARY KEY, rules TEXT NOT NULL); CREATE TABLE trackprefs_memory(user_id INTEGER NOT NULL, show_id INTEGER NOT NULL, kind TEXT NOT NULL, choice TEXT NOT NULL, updated_at INTEGER NOT NULL, PRIMARY KEY(user_id,show_id,kind));`)
}

// User zero is the owner-configured fallback policy; positive IDs are overrides.
type Rules struct {
	AudioLanguages    []string `json:"audioLanguages"`
	SubtitleLanguages []string `json:"subtitleLanguages"`
	PreferOriginal    bool     `json:"preferOriginal"`
	SubtitleMode      string   `json:"subtitleMode"`  // off, forced-only, foreign-only, always
	SubtitleStyle     string   `json:"subtitleStyle"` // normal, sdh, forced
	Loudness          bool     `json:"loudness"`
	NightMode         bool     `json:"nightMode"`
	DialogueBoost     bool     `json:"dialogueBoost"`
}
type Choice struct {
	Language string `json:"language"`
	Forced   bool   `json:"forced"`
	SDH      bool   `json:"sdh"`
	Off      bool   `json:"off"`
}
type Selection struct {
	Audio    int
	Subtitle int
}
type Options struct {
	ShowName func(context.Context, int64) string
}
type Service struct {
	db      *sql.DB
	options Options
}

func New(db *sql.DB, opts ...Options) *Service {
	s := &Service{db: db}
	if len(opts) > 0 {
		s.options = opts[0]
	}
	return s
}
func Default() Rules {
	return Rules{AudioLanguages: []string{}, SubtitleLanguages: []string{}, SubtitleMode: "off", SubtitleStyle: "normal"}
}
func (r Rules) Validate() error {
	switch r.SubtitleMode {
	case "off", "forced-only", "foreign-only", "always":
	default:
		return errors.New("invalid subtitle mode")
	}
	switch r.SubtitleStyle {
	case "normal", "sdh", "forced":
	default:
		return errors.New("invalid subtitle style")
	}
	if len(r.AudioLanguages) > 20 || len(r.SubtitleLanguages) > 20 {
		return errors.New("too many languages")
	}
	for _, lang := range append(append([]string{}, r.AudioLanguages...), r.SubtitleLanguages...) {
		if len(lang) < 2 || len(lang) > 15 || strings.ContainsAny(lang, " /\\\t\r\n") {
			return errors.New("invalid language code")
		}
	}
	return nil
}
func (s *Service) PutRules(ctx context.Context, user int64, r Rules) error {
	if user < 0 {
		return errors.New("invalid user")
	}
	if err := r.Validate(); err != nil {
		return err
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO trackprefs_rules(user_id,rules) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET rules=excluded.rules`, user, string(b))
	return err
}
func (s *Service) GetRules(ctx context.Context, user int64) (Rules, error) {
	r := Default()
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT rules FROM trackprefs_rules WHERE user_id IN(0,?) ORDER BY user_id DESC LIMIT 1`, user).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return r, nil
	}
	if err != nil {
		return r, err
	}
	err = json.Unmarshal([]byte(raw), &r)
	if r.AudioLanguages == nil {
		r.AudioLanguages = []string{}
	}
	if r.SubtitleLanguages == nil {
		r.SubtitleLanguages = []string{}
	}
	return r, err
}
func (s *Service) DeleteRules(ctx context.Context, user int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM trackprefs_rules WHERE user_id=?`, user)
	return err
}
func (s *Service) Remember(ctx context.Context, user int64, key core.RatingKey, kind string, track *core.StreamInfo) error {
	if key.Kind() != core.KindEpisode {
		return nil
	}
	if user <= 0 || (kind != "audio" && kind != "subtitle") {
		return errors.New("invalid stream memory")
	}
	if track == nil && kind == "audio" {
		return nil
	}
	c := Choice{Off: track == nil}
	if track != nil {
		if track.Type != kind {
			return errors.New("stream type does not match")
		}
		c.Language = langtag.Normalize(track.Language)
		c.Forced = track.Forced
		c.SDH = sdh(*track)
	}
	b, _ := json.Marshal(c)
	_, err := s.db.ExecContext(ctx, `INSERT INTO trackprefs_memory VALUES(?,?,?,?,?) ON CONFLICT(user_id,show_id,kind) DO UPDATE SET choice=excluded.choice,updated_at=excluded.updated_at`, user, key.TMDB(), kind, string(b), time.Now().Unix())
	return err
}
func (s *Service) Forget(ctx context.Context, user, show int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM trackprefs_memory WHERE user_id=? AND show_id=?`, user, show)
	return err
}
func (s *Service) choice(ctx context.Context, user int64, key core.RatingKey, kind string) (Choice, bool, error) {
	var c Choice
	if key.Kind() != core.KindEpisode {
		return c, false, nil
	}
	var raw string
	err := s.db.QueryRowContext(ctx, `SELECT choice FROM trackprefs_memory WHERE user_id=? AND show_id=? AND kind=?`, user, key.TMDB(), kind).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return c, false, nil
	}
	if err != nil {
		return c, false, err
	}
	err = json.Unmarshal([]byte(raw), &c)
	return c, err == nil, err
}
func sdh(t core.StreamInfo) bool {
	v := strings.ToLower(t.Title)
	return strings.Contains(v, "sdh") || strings.Contains(v, "hearing impaired") || strings.Contains(v, "[cc]") || v == "cc"
}

// rankLanguage scores a track against the wanted languages via
// langtag.MatchTier. The tier penalties (40/80) exceed every
// non-language bonus in pick (default 5 + style 10 + forced-style 10),
// so for the same want a weak tier-1 track can never beat a tier-2 or
// exact tier-3 track on those bonuses alone.
func rankLanguage(code string, want []string) int {
	best := 0
	for i, v := range want {
		score := 1000 - i*20
		switch langtag.MatchTier(v, code) {
		case 3:
		case 2:
			score -= 40
		case 1:
			score -= 80
		default:
			continue
		}
		if score > best {
			best = score
		}
	}
	return best
}
func pick(streams []core.StreamInfo, kind string, want []string, style string, forcedOnly bool, memory *Choice) int {
	best, score := -1, -100000
	for i, t := range streams {
		if t.Type != kind || (forcedOnly && !t.Forced) {
			continue
		}
		n := rankLanguage(t.Language, want)
		if t.Default {
			n += 5
		}
		if kind == "subtitle" {
			if (style == "sdh") == sdh(t) {
				n += 10
			}
			if (style == "forced") == t.Forced {
				n += 10
			}
		}
		if memory != nil && langtag.Normalize(t.Language) == langtag.Normalize(memory.Language) && t.Forced == memory.Forced && sdh(t) == memory.SDH {
			n += 10000
		}
		if n > score {
			best, score = i, n
		}
	}
	return best
}

// Select returns positions in MediaInfo.Streams, never ffprobe indices.
func (s *Service) Select(ctx context.Context, user int64, key core.RatingKey, info core.MediaInfo, original string) (Selection, error) {
	out := Selection{-1, -1}
	r, err := s.GetRules(ctx, user)
	if err != nil {
		return out, err
	}
	a, hasA, err := s.choice(ctx, user, key, "audio")
	if err != nil {
		return out, err
	}
	b, hasB, err := s.choice(ctx, user, key, "subtitle")
	if err != nil {
		return out, err
	}
	want := append([]string{}, r.AudioLanguages...)
	if r.PreferOriginal && original != "" {
		want = append([]string{original}, want...)
	}
	var am *Choice
	if hasA {
		am = &a
	}
	out.Audio = pick(info.Streams, "audio", want, "", false, am)
	if hasB && b.Off {
		return out, nil
	}
	if !hasB && r.SubtitleMode == "off" {
		return out, nil
	}
	if !hasB && r.SubtitleMode == "foreign-only" && out.Audio >= 0 && rankLanguage(info.Streams[out.Audio].Language, r.SubtitleLanguages) > 0 {
		return out, nil
	}
	var bm *Choice
	if hasB {
		bm = &b
	}
	out.Subtitle = pick(info.Streams, "subtitle", r.SubtitleLanguages, r.SubtitleStyle, !hasB && r.SubtitleMode == "forced-only", bm)
	return out, nil
}

// AudioFilters returns only known ffmpeg filters; arbitrary client strings never enter the command.
func (r Rules) AudioFilters(channels int) []string {
	var f []string
	if r.DialogueBoost && channels >= 6 {
		f = append(f, "pan=stereo|FL=0.7*FL+0.7*FC|FR=0.7*FR+0.7*FC")
	}
	if r.NightMode {
		f = append(f, "acompressor=threshold=0.125:ratio=4:attack=20:release=250:makeup=2")
	}
	if r.Loudness {
		f = append(f, "loudnorm=I=-16:TP=-1.5:LRA=11")
	}
	return f
}

type SavedChoice struct {
	ShowID    int64  `json:"showId"`
	ShowTitle string `json:"showTitle,omitempty"`
	Kind      string `json:"kind"`
	Choice    Choice `json:"choice"`
	UpdatedAt int64  `json:"updatedAt"`
}

func (s *Service) Memory(ctx context.Context, user int64) ([]SavedChoice, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT show_id,kind,choice,updated_at FROM trackprefs_memory WHERE user_id=? ORDER BY show_id,kind`, user)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SavedChoice{}
	for rows.Next() {
		var v SavedChoice
		var raw string
		if err = rows.Scan(&v.ShowID, &v.Kind, &raw, &v.UpdatedAt); err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(raw), &v.Choice); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if s.options.ShowName != nil {
		for i := range out {
			out[i].ShowTitle = s.options.ShowName(ctx, out[i].ShowID)
		}
	}
	return out, nil
}
