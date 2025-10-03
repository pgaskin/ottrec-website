package routes

import (
	"fmt"
	"net/http"

	"github.com/pgaskin/ottrec-website/pkg/ottrecidx"
	"github.com/pgaskin/ottrec-website/static"
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

	mux := http.NewServeMux()

	// TODO: favicon
	// TODO: fonts
	// TODO: base url for rel=canonical

	mux.Handle("/static/", static.Handler(static.Website))

	return commonMiddleware(mux), nil
}
