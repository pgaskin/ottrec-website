// Package templates contains templates and functions to serve them.
package templates

import (
	"compress/gzip"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/a-h/templ"
	"github.com/klauspost/compress/zstd"
	"github.com/ottrec/website/internal/httpx"
	"github.com/ottrec/website/pkg/ottrecidx"
)

//go:generate go tool templ fmt .
//go:generate go tool templ generate -include-version=false

// TODO: refactor

type ErrorPageFunc func(title, message string) templ.Component

// Render renders a page, checking and setting ETag according to the
// binary+etagMixin+url+vary. It should be called after normalizing the URL,
// setting the Vary header to at least include Accept-Encoding (this isn't done
// in Render since it's supposed to be set the same for all responses for the
// method+path), setting Cache-Control (if you don't want the default of
// "public"), and performing any required redirects.
func Render(w http.ResponseWriter, r *http.Request, errp ErrorPageFunc, etagMixin string, fn func() (c templ.Component, status int, err error)) error {
	ctx := r.Context()

	// we support content encoding negotation
	if !slices.Contains(w.Header().Values("Vary"), "Accept-Encoding") {
		panic("vary must include accept-encoding")
	}

	// set the mimetype
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// negotiate content encoding
	encoding := httpx.NegotiateContent(r.Header.Values("Accept-Encoding"), []string{"", "gzip", "zstd"})
	if encoding != "" {
		w.Header().Set("Content-Encoding", encoding)
	}

	// compute the etag from the server hash, data hash, url, varied request
	// headers, and content encoding
	etag := httpx.NewETag().
		MixExe().
		Mix(etagMixin, r.URL.String()).
		MixVary(w.Header(), r.Header).
		Encoding(encoding).
		ETag().
		Weaken() // weak: built from the render inputs, not the response bytes

	// if a caching policy isn't already set, allow it to be cached with revalidation
	if w.Header().Get("Cache-Control") == "" {
		w.Header().Set("Cache-Control", "public")
	}

	// check etag match
	if etag.Handled(w, r) {
		return nil
	}

	// get and render body
	b := templ.GetBuffer()
	defer templ.ReleaseBuffer(b)
	status, err := func() (status int, err error) {
		defer func() {
			if x := recover(); x != nil {
				switch x := x.(type) {
				case error:
					err = fmt.Errorf("panic: %w", x)
				default:
					err = fmt.Errorf("panic: %v", x)
				}
			}
		}()
		c, s, err := fn()
		if err != nil {
			return 0, err
		}
		if err := c.Render(ctx, b); err != nil {
			return 0, err
		}
		return s, err
	}()
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("template: failed to render", "error", err, "url", r.URL.String())
			RenderError(w, r, errp, "Internal Server Error", err.Error(), http.StatusInternalServerError)
			return err
		}
		return nil
	}

	// no body for head request
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return nil
	}

	// if we don't have a content encoding, serve the body as-is
	if encoding == "" {
		w.Header().Set("Content-Length", strconv.Itoa(b.Len()))
		w.WriteHeader(status)
		w.Write(b.Bytes())
		return nil
	}

	// encode and serve the body
	zb := templ.GetBuffer()
	defer templ.ReleaseBuffer(zb)
	if err := compress(zb, encoding, b.Bytes()); err != nil {
		return err
	}
	w.Header().Set("Content-Length", strconv.Itoa(zb.Len()))
	w.WriteHeader(status)
	w.Write(zb.Bytes())
	return nil
}

// RenderError clears Content-Encoding and renders a non-cached error page.
func RenderError(w http.ResponseWriter, r *http.Request, errp ErrorPageFunc, title, message string, status int) {
	w.Header().Del("Content-Encoding")
	w.Header().Set("Cache-Control", "private, no-store")

	b := templ.GetBuffer()
	defer templ.ReleaseBuffer(b)

	if err := errp(title, message).Render(r.Context(), b); err != nil {
		b.Reset()
		b.WriteString(title)
		b.WriteString("\n\n")
		b.WriteString(message)
		b.WriteString("\n")
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Content-Length", strconv.Itoa(b.Len()))
		w.WriteHeader(status)
		w.Write(b.Bytes())
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	w.Write(b.Bytes())
}

func compress(w io.Writer, encoding string, b []byte) error {
	switch encoding {
	case "":
		if _, err := w.Write(b); err != nil {
			return err
		}
	case "gzip":
		zw := gzip.NewWriter(w)
		if _, err := zw.Write(b); err != nil {
			return fmt.Errorf("gzip: %w", err)
		}
		if err := zw.Close(); err != nil {
			return fmt.Errorf("gzip: %w", err)
		}
	case "zstd":
		zw, err := zstd.NewWriter(w)
		if err != nil {
			return fmt.Errorf("zstd: %w", err)
		}
		if _, err := zw.Write(b); err != nil {
			return fmt.Errorf("zstd: %w", err)
		}
		if err := zw.Close(); err != nil {
			return fmt.Errorf("zstd: %w", err)
		}
	default:
		return fmt.Errorf("unknown encoding %q", encoding)
	}
	return nil
}

// headExtra is raw HTML injected at the bottom of <head> on every page (e.g.,
// for analytics).
var headExtra templ.Component

// SetHeadExtra sets raw HTML to inject at the bottom of <head> on every page.
// It must be called at most once, before anything is rendered. The HTML is
// mixed into the etags so cached pages revalidate when it changes.
func SetHeadExtra(html string) {
	if html == "" {
		return
	}
	headExtra = templ.Raw(html)
	httpx.AddExeExtra(html)
}

var aboutExtra templ.Component

// SetAboutExtra sets raw HTML to inject at the bottom of <head> on every page.
// It must be called at most once, before anything is rendered. The HTML is
// mixed into the etags so cached pages revalidate when it changes.
func SetAboutExtra(html string) {
	if html == "" {
		return
	}
	aboutExtra = templ.Raw(html)
	httpx.AddExeExtra(html)
}

// homeExtra is raw HTML injected at the bottom of the homepage's main content.
var homeExtra templ.Component

// SetHomeExtra sets raw HTML to inject at the bottom of the homepage's <main>,
// after the news section. It must be called at most once, before anything is
// rendered. The HTML is mixed into the etags so cached pages revalidate when it
// changes. An injected <section> with an <h2> is styled to match the news.
func SetHomeExtra(html string) {
	if html == "" {
		return
	}
	homeExtra = templ.Raw(html)
	httpx.AddExeExtra(html)
}

// mapTiles is the basemap tile configuration shared by the map scripts (as a
// JSON island) and the about page's attribution, so both credit the same
// source. [SetMapTiles] overrides the defaults.
var mapTiles = mapTilesJSON{
	Light:       "https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png",
	Dark:        "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
	Attribution: `&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>`,
	Subdomains:  "abcd",
}

type mapTilesJSON struct {
	Light       string `json:"light"`
	Dark        string `json:"dark"`
	Attribution string `json:"attribution"`
	Subdomains  string `json:"subdomains"`
}

// SetMapTiles overrides the basemap tiles used by the Leaflet maps: the
// light/dark url templates, the tile source attribution (html, not including
// the site's own credit), and the {s} substitutions. An empty value keeps the
// default for that field. It must be called at most once, before anything is
// rendered. The values are mixed into the etags so cached pages revalidate when
// they change.
func SetMapTiles(light, dark, attribution, subdomains string) {
	if light == "" && dark == "" && attribution == "" && subdomains == "" {
		return
	}
	for dst, val := range map[*string]string{
		&mapTiles.Light:       light,
		&mapTiles.Dark:        dark,
		&mapTiles.Attribution: attribution,
		&mapTiles.Subdomains:  subdomains,
	} {
		if val != "" {
			*dst = val
		}
	}
	httpx.AddExeExtra(strings.Join([]string{light, dark, attribution, subdomains}, "\x00"))
}

func cutBefore(s, sep string) string {
	before, _, _ := strings.Cut(s, sep)
	return before
}

// localRFC3339 formats t in the schedule timezone for <time datetime>.
func localRFC3339(t time.Time) string {
	return t.In(ottrecidx.TZ).Format(time.RFC3339)
}

// localFormat formats t in the schedule timezone with the given layout.
func localFormat(t time.Time, layout string) string {
	return t.In(ottrecidx.TZ).Format(layout)
}
