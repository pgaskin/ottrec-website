// Package httpfile contains helpers for negotiating and serving content.
package httpfile

import (
	"cmp"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"golang.org/x/text/language"
)

// Content negotiation request and response headers.
//
// https://httpwg.org/specs/rfc9110.html#content.negotiation
const (
	HeaderVary           = "Vary"
	HeaderAccept         = "Accept"
	HeaderAcceptLanguage = "Accept-Language"
	HeaderAcceptEncoding = "Accept-Encoding"

	HeaderContentType     = "Content-Type"
	HeaderContentEncoding = "Content-Encoding"
	HeaderContentLanguage = "Content-Language"
)

// Range request and response headers.
//
// https://httpwg.org/specs/rfc9110.html#range.requests
const (
	HeaderRange   = "Range"
	HeaderIfRange = "If-Range"

	HeaderAcceptRanges = "Accept-Ranges"
	HeaderContentRange = "Content-Range"
)

// ETag request and response  headers.
const (
	HeaderIfMatch     = "If-Match"
	HeaderIfNoneMatch = "If-None-Match"

	HeaderETag = "ETag"
)

// Last modified request and response headers.
const (
	HeaderIfModifiedSince   = "If-Modified-Since"
	HeaderIfUnmodifiedSince = "If-Unmodified-Since"

	HeaderLastModified = "Last-Modified"
)

// Other headers.
const (
	HeaderAllow         = "Allow"
	HeaderContentLength = "Content-Length"
)

// Some known content encodings.
const (
	CodingIdentity = "identity"
	CodingZstd     = "zstd"
	CodingDeflate  = "deflate"
	CodingGzip     = "gzip"
	CodingBrotli   = "br"
)

// ETag contains a valid ETag.
type ETag string

// ETag makes an ETag value, returning false if etag contains forbidden
// characters.
//
// https://httpwg.org/specs/rfc9110.html#field.etag
func MakeETag(tag string, weak bool) (ETag, bool) {
	for _, c := range tag {
		switch {
		case c == 0x21:
		case 0x23 <= c && c <= 0x7E:
		case 0x80 <= c && c <= 0xFF: // obs-text
		default:
			return "", false
		}
	}
	var b strings.Builder
	if weak {
		b.WriteString("W/")
	}
	b.WriteByte('"')
	b.WriteString(tag)
	b.WriteByte('"')
	return ETag(b.String()), true
}

func (e ETag) Split() (string, bool) {
	tag, weak := strings.CutPrefix(string(e), "W/")
	tag = strings.TrimPrefix(tag, "\"")
	tag = strings.TrimSuffix(tag, "\"")
	return tag, weak
}

type File struct {
	// ETag is the ETag for the file. If empty, [HeaderETag] will not be set,
	// and [HeaderIfNoneMatch] will not be supported.
	ETag ETag

	// LastModified contains the last modification date for this representation
	// of the file. If zero, [HeaderLastModified] will not be set, and
	// [HeaderIfModifiedSince] will not be supported. Do not use this unless
	// it's an accurate value, otherwise clients may cache stuff incorrectly.
	LastModified time.Time

	// Type is the mimetype of the file, if known. This may include parameters.
	Type string

	// Language is the language of the file, if known. An empty Language is
	// equivalent to [language.Und].
	Language language.Tag

	// Coding is the encoding of the file, if known. If empty, [CodingIdentity]
	// is assumed.
	Coding string

	// Open opens the file. If the returned reader implements [io.Closer], it
	// will be called.
	Open func() (io.ReadSeeker, error)
}

type handler struct {
	files     []File
	fallback  bool
	types     bool
	codings   bool
	languages language.Matcher
}

// Handler returns a handler for a collection of files.
//
// Matches are filtered for language (if any f has it set), then type (if any f
// has it set), then encoding (if any f has it set). For this reason, you should
// generally ensure you have all combinations of languages and types.
//
// If fallback is true, a file will be served even if no offers match, unless
// the client specifies a wildcard with q=0 in one of the accept headers that
// we're looking for.
//
// [CodingIdentity] is always used as the fallback coding regardless of the
// order of the files. If it isn't available, no fallback will be served if
// there isn't an acceptable encoding.
//
// BCP 47 is used to match languages.
//
// For error responses, none of the content headers will be served and the
// Cache-Control and Content-Disposition headers will also be removed.
//
// If you want to set the [Content-Disposition] header, CORS headers, and so on,
// you should do it before the handler. You can also set [HeaderVary] to *.
//
// [Content-Disposition]:
// https://httpwg.org/specs/rfc6266.html#header.field.definition
func Handler(fallback bool, f ...File) http.Handler {
	h := &handler{
		files:    f,
		fallback: fallback,
	}
	var languages []language.Tag
	for _, f := range f {
		if cmp.Or(f.Type, CodingIdentity) != CodingIdentity {
			h.types = true
		}
		if f.Coding != "" {
			h.codings = true
		}
		if f.Language != language.Und {
			if !slices.Contains(languages, f.Language) {
				languages = append(languages, f.Language)
			}
		}
	}
	if len(languages) != 0 {
		h.languages = language.NewMatcher(languages, language.PreferSameScript(true))
	}
	return h
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d := w.Header()

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		d.Add(HeaderAllow, http.MethodHead)
		d.Add(HeaderAllow, http.MethodGet)
		h.serveError(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	if h.types {
		d.Add(HeaderVary, HeaderAccept)
	}
	if h.languages != nil {
		d.Add(HeaderVary, HeaderAcceptLanguage)
	}
	if h.codings {
		d.Add(HeaderVary, HeaderAcceptEncoding)
	}

	f, ok := h.negotiate(r)
	if !ok {
		h.serveError(w, http.StatusText(http.StatusNotAcceptable), http.StatusNotAcceptable)
		return
	}

	if f.ETag != "" {
		d.Set("ETag", string(f.ETag))
	}
	if f.Type != "" {
		d.Set(HeaderContentType, f.Type)
	} else {
		d[HeaderContentType] = []string{} // so ServeContent doesn't try to sniff it
	}
	if f.Language != language.Und {
		d.Set(HeaderContentLanguage, f.Language.String())
	}
	if cmp.Or(f.Coding, CodingIdentity) != CodingIdentity {
		d.Set(HeaderContentEncoding, f.Coding)
	}

	// precedence rules: https://httpwg.org/specs/rfc9110.html#rfc.section.13.2.2

	if f.Open == nil {
		h.serveError(w, "failed to open file", http.StatusInternalServerError)
		return
	}

	fr, err := f.Open()
	if err != nil {
		h.serveError(w, "failed to open file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// this will set Last-Modified and handle If-Match, If-None-Match,
	// If-Modified-Since, If-Unmodified-Since, If-Range, and Range according to
	// fr, ETag, and the last modified time
	http.ServeContent(w, r, "", f.LastModified, fr)
}

func (h *handler) negotiate(r *http.Request) (File, bool) {
	skip := make([]bool, len(h.files))
	rejected := false

	// language MUST be first since the language matcher will return any
	// language in files, not taking into account skip
	if !rejected && h.languages != nil {
		accepts := parseQualityFactors(r.Header.Values(HeaderAcceptLanguage), 8*1024)

		slices.SortStableFunc(accepts, func(a, b qValue) int {
			return cmp.Compare(b.q, a.q)
		})
		tags := make([]language.Tag, 0, len(accepts))
		for _, val := range accepts {
			tag, err := language.Parse(val.value)
			if err != nil {
				continue
			}
			tags = append(tags, tag)
		}
		if m, _, c := h.languages.Match(tags...); c != language.No {
			// filter out all files which doesn't have the matched language
			for i, f := range h.files {
				if f.Language != m {
					skip[i] = true
				}
			}
		} else {
			for _, val := range accepts {
				if val.q == 0 && val.value == "*" {
					rejected = true // client doesn't want fallbacks
					break
				}
			}
			if !h.fallback {
				rejected = true
			}
		}
	}

	if !rejected && h.types {
		accepts := parseQualityFactors(r.Header.Values(HeaderAccept), 8*1024)

		if match := negotiateContent(accepts, func(yield func(string) bool) {
			for i, f := range h.files {
				if !skip[i] && !yield(f.Type) {
					return
				}
			}
		}); match != "" {
			// filter out all files which doesn't have the matched content type
			for i, f := range h.files {
				if f.Type != match {
					skip[i] = true
				}
			}
		} else {
			for _, val := range accepts {
				if val.q == 0 && (val.value == "*" || val.value == "*/*") {
					rejected = true // client doesn't want fallbacks
					break
				}
			}
			if !h.fallback {
				rejected = true
			}
		}
	}

	var codingMatch bool
	if !rejected && h.codings {
		accepts := parseQualityFactors(r.Header.Values(HeaderAcceptEncoding), 8*1024)

		if match := negotiateContent(accepts, func(yield func(string) bool) {
			for i, f := range h.files {
				if !skip[i] && !yield(cmp.Or(f.Coding, CodingIdentity)) {
					return
				}
			}
		}); match != "" {
			codingMatch = true
			// filter out all files which doesn't have the matched coding
			for i, f := range h.files {
				if cmp.Or(f.Coding, CodingIdentity) != match {
					skip[i] = true
				}
			}
		} else {
			for _, val := range accepts {
				if val.q == 0 && val.value == "*" {
					rejected = true // client doesn't want fallbacks
					break
				}
			}
			if !h.fallback {
				rejected = true
			}
		}
	}

	if !rejected {
		for i, f := range h.files {
			if !skip[i] {
				// only return the file if it's in a coding supported by the client or it's uncompressed
				if uncompressed := cmp.Or(f.Coding, CodingIdentity) == CodingIdentity; codingMatch || uncompressed {
					return f, true
				}
			}
		}
	}
	return File{}, false
}

func (h *handler) serveError(w http.ResponseWriter, text string, code int) {
	d := w.Header()
	d.Del("Cache-Control")
	d.Del(HeaderETag)
	d.Del(HeaderLastModified)
	d.Del(HeaderContentEncoding)
	d.Del(HeaderContentType)
	d.Del(HeaderContentLanguage)
	d.Del(HeaderContentLength)
	d.Del(HeaderContentRange)
	d.Set("Content-Type", "text/plain; charset=utf-8")
	d.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	io.WriteString(w, text+"\n")
}
