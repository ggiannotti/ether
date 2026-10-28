package tmdb

import (
	"context"
	"fmt"
	"github.com/braedonsaunders/ether/internal/core"
	"net/url"
	"regexp"
	"strings"
)

var languageRE = regexp.MustCompile(`^[A-Za-z]{2}(-[A-Za-z]{2})?$`)

// Localized changes presentation only: certification, adult flag and identity
// remain the canonical catalogue's responsibility, never translated API fields.
func (c *Client) Localized(ctx context.Context, t *core.Title, language string) (*core.Title, error) {
	out := *t
	if !languageRE.MatchString(language) {
		return &out, nil
	}
	path, e := discoveryPath(t)
	if e != nil {
		return &out, e
	}
	var data struct {
		Title    string  `json:"title"`
		Name     string  `json:"name"`
		Overview string  `json:"overview"`
		Tagline  string  `json:"tagline"`
		Poster   string  `json:"poster_path"`
		Backdrop string  `json:"backdrop_path"`
		Images   *Images `json:"images"`
	}
	q := url.Values{"language": {language}, "append_to_response": {"images"}, "include_image_language": {strings.Split(language, "-")[0] + ",null"}}
	if e = c.getJSON(ctx, path, q, &data); e != nil {
		return &out, e
	}
	if data.Title != "" {
		out.Title = data.Title
	} else if data.Name != "" {
		out.Title = data.Name
	}
	if data.Overview != "" {
		out.Summary = data.Overview
	}
	if data.Tagline != "" {
		out.Tagline = data.Tagline
	}
	if data.Poster != "" {
		out.PosterPath = data.Poster
	}
	if data.Backdrop != "" {
		out.BackdropPath = data.Backdrop
	}
	if data.Images != nil {
		for _, img := range data.Images.Backdrops {
			if img.ISO639 == "" {
				out.BackdropPath = img.FilePath
				break
			}
		}
		for _, img := range data.Images.Logos {
			if img.ISO639 == strings.Split(language, "-")[0] {
				out.LogoPath = img.FilePath
				break
			}
		}
	}
	return &out, nil
}
func discoveryPath(t *core.Title) (string, error) {
	switch t.Type {
	case core.TypeMovie:
		return fmt.Sprintf("/movie/%d", t.IDs.TMDB), nil
	case core.TypeShow:
		return fmt.Sprintf("/tv/%d", t.IDs.TMDB), nil
	case core.TypeSeason:
		return fmt.Sprintf("/tv/%d/season/%d", t.ShowKey.TMDB(), t.Season), nil
	case core.TypeEpisode:
		return fmt.Sprintf("/tv/%d/season/%d/episode/%d", t.ShowKey.TMDB(), t.Season, t.Episode), nil
	}
	return "", fmt.Errorf("unsupported localization type")
}
func (c *Client) ArtworkVariants(ctx context.Context, t *core.Title, language string) (*Images, error) {
	p, e := discoveryPath(t)
	if e != nil {
		return nil, e
	}
	lang := strings.Split(language, "-")[0]
	if lang == "" {
		lang = "en"
	}
	var out Images
	e = c.getJSON(ctx, p+"/images", url.Values{"include_image_language": {lang + ",null"}}, &out)
	return &out, e
}
