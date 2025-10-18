// Package static contains static content and functions to serve it.
package static

import (
	"crypto/sha1"
	"embed"
	"encoding/base32"
	"fmt"
	"log/slog"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pgaskin/ottrec-website/internal/httpfile"
	"github.com/pgaskin/ottrec-website/internal/postcss"
)

// TODO: refactor, compress assets in the background, support renaming assets per group

//go:generate go run fonts.go
//go:generate go run fetch.go https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.min.js lib/leaflet.js
//go:generate go run fetch.go https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.min.css lib/leaflet.css

const Base = "/static/"

var (
	AsapWOFF2         = newFile("fonts/asap.woff2")
	SourceSans3WOFF2  = newFile("fonts/source_sans_3.woff2")
	SourceSerif4WOFF2 = newFile("fonts/source_serif_4.woff2")
	SymbolsWOFF2      = newFile("fonts/symbols.woff2")

	LeafletCSS = newFile("lib/leaflet.css")
	LeafletJS  = newFile("lib/leaflet.js")

	DataCSS    = newFile("data.css")
	WebsiteCSS = newFile("website.css")

	Website = newGroup("website",
		WebsiteCSS,
		SourceSans3WOFF2,
		SourceSerif4WOFF2,
		SymbolsWOFF2,
		AsapWOFF2,
		LeafletCSS,
		LeafletJS,
	)

	Data = newGroup("data",
		DataCSS,
		SourceSans3WOFF2,
		SourceSerif4WOFF2,
	)
)

// Handler compresses all files not already compressed and returns a handler to
// be served under [Base].
func Handler(g *group) http.Handler {
	g.compress()
	return http.HandlerFunc(g.serveHTTP)
}

// Path returns the path to a file.
func Path(f *file) string {
	return Base + f.HashName
}

//go:embed *
var res embed.FS

type file struct {
	Name         string
	HashName     string
	ContentType  string
	Hash         string
	Raw          []byte
	compressed   http.Handler
	compressOnce sync.Once
}

func (f *file) compress() {
	f.compressOnce.Do(func() {
		slog.Info("static: compressing asset", "name", f.Name, "hash_name", f.HashName)
		f.compressed = httpfile.Static(f.Raw, f.ContentType, time.Time{}, "")
	})
}

var cache = map[string]*file{}

func newFile(name string) *file {
	if v, ok := cache[name]; ok {
		return v
	}
	v, err := func() (*file, error) {
		ext := path.Ext(name)

		buf, err := res.ReadFile(name)
		if err != nil {
			return nil, err
		}

		if !strings.Contains(name, "/") {
			switch ext {
			case ".css":
				css, err := postcss.Transform(string(buf), "defaults, safari > 15, chrome > 110, firefox > 110")
				if err != nil {
					return nil, fmt.Errorf("compile css: %w", err)
				}
				buf = []byte(regexp.MustCompile(`url\([^)]+\)`).ReplaceAllStringFunc(css, func(css string) string {
					return "url(" + getFile(string(css[strings.IndexByte(css, '(')+1:len(css)-1])).HashName + ")"
				}))
			}
		}

		var mimetype string
		switch ext {
		case ".woff2":
			mimetype = "font/woff2"
		case ".css":
			mimetype = "text/css; charset=utf-8"
		case ".js":
			mimetype = "application/javascript; charset=utf-8"
		default:
			return nil, fmt.Errorf("no mimetype for %q", ext)
		}

		sum := sha1.Sum(buf)
		hash := base32.StdEncoding.EncodeToString(sum[:])
		hashName := strings.TrimSuffix(name, ext) + "-" + hash[:10] + ext

		return &file{
			Name:        name,
			HashName:    hashName,
			ContentType: mimetype,
			Hash:        hash,
			Raw:         buf,
		}, nil
	}()
	if err != nil {
		panic(fmt.Errorf("static: load %q: %w", name, err))
	}
	cache[name] = v
	return v
}

func getFile(name string) *file {
	f, ok := cache[name]
	if !ok {
		panic("static: file " + strconv.Quote(name) + " not found in cache")
	}
	return f
}

type group struct {
	name  string
	load  sync.Once
	files map[string]*file
}

func newGroup(name string, f ...*file) *group {
	g := &group{
		name:  name,
		files: make(map[string]*file),
	}
	for _, f := range f {
		g.files[f.Name] = f
		g.files[f.HashName] = f
	}
	return g
}

// Compress compresses all files not already compressed.
func (g *group) compress() {
	g.load.Do(func() {
		for _, f := range g.files {
			f.compress()
		}
	})
}

func (g *group) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// match the filename
	name, ok := strings.CutPrefix(r.URL.Path, Base)
	if !ok && name == "/favicon.ico" {
		name, ok = "favicon.ico", true
	}
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	file, ok := g.files[name]
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	// redirect to the hashed filename without caching
	if name != file.HashName {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Location", Base+file.HashName)
		w.WriteHeader(http.StatusTemporaryRedirect)
		return
	}

	// cache hashed files (but don't say immutable just in case we have bugs
	// somewhere)
	w.Header().Set("Cache-Control", "public, max-age=86400")

	file.compressed.ServeHTTP(w, r)
}
