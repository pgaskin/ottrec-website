package routes

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/a-h/templ"
	"github.com/pgaskin/ottrec-website/pkg/ottrecidx"
	"github.com/pgaskin/ottrec-website/static"
	"github.com/pgaskin/ottrec-website/templates"
)

type WebsiteConfig struct {
	Host string
	Data func() (ottrecidx.DataRef, bool)
}

func Website(cfg WebsiteConfig) (http.Handler, error) {
	if cfg.Host == "" {
		return nil, fmt.Errorf("no host specified")
	}
	if cfg.Data == nil {
		return nil, fmt.Errorf("no data getter specified")
	}

	base := websiteHandlerBase{
		Host: cfg.Host,
		Data: cfg.Data,
	}
	mux := http.NewServeMux()

	// TODO: favicon
	// TODO: fonts
	// TODO: base url for rel=canonical

	mux.Handle("GET /{$}", &websiteHomeHandler{
		websiteHandlerBase: base,
	})
	mux.Handle("/static/", static.Handler(static.Website))

	return commonMiddleware(mux), nil
}

type websiteHandlerBase struct {
	Host string
	Data func() (ottrecidx.DataRef, bool)
}

func (h *websiteHandlerBase) render(w http.ResponseWriter, r *http.Request, fn func(data ottrecidx.DataRef) (c templ.Component, status int, err error)) {
	var (
		data ottrecidx.DataRef
		ok   bool
	)
	if h.Data != nil {
		data, ok = h.Data()
	}
	if !ok {
		slog.Error("website: no data available")
		templates.RenderError(w, r, templates.WebsiteErrorPage, "Data Unavailable", "data not available, try again later", http.StatusServiceUnavailable)
		return
	}
	if err := templates.Render(w, r, templates.WebsiteErrorPage, data.Index().Hash(), func() (c templ.Component, status int, err error) {
		return fn(data)
	}); err != nil {
		slog.Error("website: failed to render page", "url", r.URL.String(), "error", err)
	}
}

type websiteHomeHandler struct {
	websiteHandlerBase
}

func (h *websiteHomeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Vary", "Accept-Encoding")

	if r.URL.RawQuery != "" {
		w.Header().Set("Cache-Control", "no-store")
		http.Redirect(w, r, r.URL.EscapedPath(), http.StatusTemporaryRedirect)
		return
	}

	h.render(w, r, func(data ottrecidx.DataRef) (templ.Component, int, error) {
		return templates.WebsiteHomePage(templates.WebsiteHomePageParams{
			Data: data,
		}), http.StatusOK, nil
	})
}
