package routes

import (
	"bufio"
	"cmp"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pgaskin/ottrec-website/internal/httpx"
	"github.com/pgaskin/ottrec-website/pkg/ottrecdata"
	"github.com/pgaskin/ottrec-website/static"
)

type DataConfig struct {
	Host  string
	Cache *ottrecdata.Cache
}

func Data(cfg DataConfig) (http.Handler, error) {
	if cfg.Host == "" {
		return nil, fmt.Errorf("no host specified")
	}
	if cfg.Cache == nil {
		return nil, fmt.Errorf("no cache specified")
	}

	mux := http.NewServeMux()

	// TODO: homepage, api doc
	// TODO: visual low-level historical diff? maybe this should be a separate service?
	// TODO: csv download (latest version only, cached)?

	mux.Handle("/v1/", &dataAPIv1{
		Base:  "/v1/",
		Cache: cfg.Cache,
	})
	mux.Handle("/static/", static.Handler(static.Data))

	return commonMiddleware(mux), nil
}

type dataAPIv1 struct {
	Base  string
	Cache *ottrecdata.Cache
}

func (h *dataAPIv1) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Robots-Tag", "noindex")

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		h.serveError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if rest, ok := strings.CutPrefix(r.URL.Path, h.Base); ok {
		if rest == "" {
			h.serveList(w, r)
			return
		}
		if spec, format, _ := strings.Cut(rest, "/"); !strings.Contains(format, "/") {
			h.serveFile(w, r, spec, format)
			return
		}
	}

	h.serveError(w, "not found", http.StatusNotFound)
}

func (h *dataAPIv1) serveError(w http.ResponseWriter, message string, code int) {
	d := w.Header()
	d.Set("Content-Length", strconv.Itoa(len(message)+1))
	d.Set("Content-Type", "text/plain; charset=utf-8")
	d.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	io.WriteString(w, message+"\n")
}

func (h *dataAPIv1) serveList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// validate query
	var (
		after           = ""
		limit, maxLimit = 25, 500
		revisions       = false
	)
	for k, v := range r.URL.Query() {
		if len(v) == 0 {
			continue
		}
		switch k {
		case "limit":
			v, err := strconv.ParseInt(v[0], 10, 64)
			if err != nil {
				h.serveError(w, "invalid limit int", http.StatusBadRequest)
				return
			}
			limit = int(v)
		case "after":
			after = v[0]
		case "revisions":
			v, err := strconv.ParseBool(v[0])
			if err != nil {
				h.serveError(w, "invalid revisions bool", http.StatusBadRequest)
				return
			}
			revisions = v
		default:
			h.serveError(w, "invalid parameter "+strconv.Quote(k), http.StatusBadRequest)
			return
		}
	}
	if limit <= 0 || limit > maxLimit {
		h.serveError(w, "limit out of range", http.StatusBadRequest)
		return
	}
	if after != "" && !ottrecdata.IsID(after) {
		h.serveError(w, "after is not a valid data id", http.StatusBadRequest)
		return
	}

	// cache the list for a minute
	w.Header().Set("Cache-Control", "public, max-age=60")

	// set the mimetype
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	// no body for head requests
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}

	// generate the json
	var (
		err       error
		wrote     bool
		bw        = bufio.NewWriterSize(w, 512)
		seenAfter bool
	)
	for prev, ver := range iterPrev(h.Cache.DataVersions(ctx)(&err)) {
		if after != "" && !seenAfter {
			if ver.ID == after {
				seenAfter = true
			}
			continue
		}
		if !revisions && prev.Updated.Equal(ver.Updated) {
			continue // this must be after the after check, or we might miss revisions
		}
		if limit--; limit < 0 {
			break
		}
		if !wrote {
			wrote = true
			bw.WriteByte('[')
		} else {
			bw.WriteByte(',')
		}
		bw.WriteString(`{"id":"`)
		bw.WriteString(ver.ID)
		bw.WriteString(`","updated":"`)
		bw.WriteString(ver.Updated.In(ottrecdata.TZ).Format(time.RFC3339))
		bw.WriteString(`","revision":`)
		bw.Write(strconv.AppendInt(bw.AvailableBuffer(), int64(ver.Revision), 10))
		bw.WriteString(`}`)
	}
	if !wrote {
		bw.WriteByte('[')
	}
	bw.WriteString("]\n")
	bw.Flush()
	if err != nil {
		if canceled := ctx.Err() != nil; !canceled {
			slog.Error("data api v1: failed to serve list", "error", err)
			if wrote {
				io.WriteString(w, "\ninternal server error: "+err.Error()+"\n")
			} else {
				h.serveError(w, "internal server error: "+err.Error(), http.StatusInternalServerError)
			}
		}
		return
	}
}

func (h *dataAPIv1) serveFile(w http.ResponseWriter, r *http.Request, spec, format string) {
	ctx := r.Context()

	// we do content encoding negotiation
	w.Header().Add("Vary", "Accept-Encoding")

	// validate query
	for k := range r.URL.Query() {
		h.serveError(w, "invalid parameter "+strconv.Quote(k), http.StatusBadRequest)
		return
	}

	// resolve the data version spec
	id, updated, ok, err := h.Cache.ResolveVersion(ctx, cmp.Or(spec, "latest"))
	if err != nil {
		slog.Error("data api v1: failed to resolve spec", "spec", spec, "error", err)
		h.serveError(w, "internal server error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if !ok {
		h.serveError(w, "invalid spec format "+strconv.Quote(spec), http.StatusBadRequest)
		return
	}

	// put data update time in a header if known
	if !updated.IsZero() {
		// note: note Last-Modified since it isn't technically correct for this
		w.Header().Set("X-Schedule-Updated", updated.UTC().Format(http.TimeFormat))
	}

	// cache data resolution for 60s
	w.Header().Set("Cache-Control", "public, max-age=60")

	// no data matching spec
	if id == "" {
		if spec == "" || spec == "latest" {
			slog.Error("data api v1: no data available")
			h.serveError(w, "no data available, try again later", http.StatusServiceUnavailable)
		} else {
			h.serveError(w, "no match for "+strconv.Quote(spec), http.StatusNotFound)
		}
		return
	}

	// redirect to canonical url for data id
	if spec != id {
		h.redirectFile(w, id, format)
		return
	}

	// redirect to pb if no format specified
	if format == "" {
		h.redirectFile(w, id, string("pb"))
		return
	}

	// validate the format and set mimetype
	switch format {
	case "pb":
		w.Header().Set("Content-Type", "application/x-protobuf")
	case "proto":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	case "json":
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	case "textpb":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	default:
		h.serveError(w, "unknown format", http.StatusNotFound)
		return
	}

	// select the format and set mimetype
	var hash string
	for h, f := range h.Cache.DataFormats(ctx, id)(&err) {
		if f == format {
			hash = h
			break
		}
	}
	if err != nil {
		slog.Error("data api v1: failed to resolve formats", "id", id, "error", err)
		h.serveError(w, "internal server error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if hash == "" {
		h.serveError(w, "format not found", http.StatusNotFound)
		return
	}

	// negotiate encoding
	encoding := httpx.NegotiateContent(r.Header.Values("Accept-Encoding"), []string{"", "gzip"})
	if encoding != "" {
		w.Header().Set("Content-Encoding", encoding)
	}

	// cache the data for longer since it's immutable (but don't say immutable
	// just in case we have bugs somewhere)
	w.Header().Set("Cache-Control", "public, max-age=604800")

	// build etag from content hash and encoding
	var etag strings.Builder
	etag.WriteString(`W/"`)
	etag.WriteString(hash)
	if encoding != "" {
		etag.WriteByte('-')
		etag.WriteString(encoding)
	}
	etag.WriteString(`"`)
	w.Header().Set("ETag", etag.String())

	// check etag match
	if slices.Contains(r.Header.Values("If-None-Match"), etag.String()) {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// no body for head requests
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}

	// serve the file
	ok, err = h.Cache.ReadBlob(ctx, hash, encoding == "gzip", func(r io.Reader, len int64) error {
		if len != -1 {
			w.Header().Set("Content-Length", strconv.FormatInt(len, 10))
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, r)
		return nil
	})
	if err != nil {
		if canceled := r.Context().Err() != nil; !canceled {
			slog.Error("data api v1: failed to serve blob", "hash", hash, "encoding", encoding, "error", err)
			h.serveError(w, "internal server error: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}
	if !ok {
		slog.Error("data api v1: missing blob", "hash", hash, "encoding", encoding)
		h.serveError(w, "internal server error: missing blob", http.StatusInternalServerError)
		return
	}
}

func (h *dataAPIv1) redirectFile(w http.ResponseWriter, spec, format string) {
	var u strings.Builder
	u.WriteString(h.Base)
	u.WriteString(spec)
	if format != "" {
		u.WriteString("/")
		u.WriteString(url.PathEscape(format))
	}
	w.Header().Set("Location", u.String())
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusTemporaryRedirect)
}
