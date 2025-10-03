// Package templates contains templates and functions to serve them.
package templates

import (
	"compress/gzip"
	"crypto/sha1"
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/a-h/templ"
	"github.com/klauspost/compress/zstd"
	"github.com/pgaskin/ottrec-website/internal/httpx"
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

	// compute the etag from the server hash, data hash, vary header, and content encoding
	var etag strings.Builder
	etag.WriteString(exehash)
	etag.WriteString(etagMixin)
	etag.WriteByte(0)
	etag.WriteString(r.URL.String())
	for _, k := range w.Header().Values("Vary") {
		etag.WriteByte(0)
		etag.WriteString(k)
		for _, v := range r.Header.Values(k) {
			etag.Write(binary.LittleEndian.AppendUint64(nil, uint64(len(v))))
			etag.WriteString(v)
		}
	}
	sum := sha1.Sum([]byte(etag.String()))
	etag.Reset()
	etag.WriteString(`W/"`)
	etag.WriteString(base32.StdEncoding.EncodeToString(sum[:]))
	if encoding != "" {
		etag.WriteByte('-')
		etag.WriteString(encoding)
	}
	etag.WriteString(`"`)
	w.Header().Set("ETag", etag.String())

	// if a caching policy isn't already set, allow it to be cached with revalidation
	if w.Header().Get("Cache-Control") != "" {
		w.Header().Set("Cache-Control", "public")
	}

	// check etag match
	if slices.Contains(r.Header.Values("If-None-Match"), etag.String()) {
		w.WriteHeader(http.StatusNotModified)
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

// exehash is a hash of the current binary for use in etags.
var exehash = func() string {
	exe, err := os.Executable()
	if err != nil {
		panic(fmt.Errorf("exehash: %w", err))
	}
	buf, err := os.ReadFile(exe)
	if err != nil {
		panic(fmt.Errorf("exehash: %w", err))
	}
	sum := sha1.Sum(buf)
	return base32.StdEncoding.EncodeToString(sum[:])
}()
