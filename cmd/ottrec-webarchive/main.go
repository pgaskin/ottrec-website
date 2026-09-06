package main

import (
	"bufio"
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	htmlpkg "html"
	"io"
	"io/fs"
	"iter"
	"log/slog"
	"maps"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"

	"github.com/lmittmann/tint"
	"github.com/ottrec/website/internal/httpx"
	"github.com/ottrec/website/internal/pflagx"
	"github.com/ottrec/website/pkg/ottrecidx"
	"github.com/ottrec/website/static"
	"github.com/spf13/pflag"
	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
	"golang.org/x/time/rate"
)

var (
	EnvPrefix    = "OTTREC_WEBARCHIVE_"
	Addr         = pflag.StringP("addr", "a", ":8085", "listen address")
	Repo         = pflag.StringP("repo", "r", "/tmp/ottrec-cache.git", "cache git repo path (will be initialized as a bare repo if empty)")
	RepoRemote   = pflag.String("repo-remote", "https://github.com/ottrec/data.git", "remote to fetch")
	RepoBranch   = pflag.String("repo-branch", "cache", "branch to fetch (will be overwritten in the local repo)")
	RepoRev      = pflag.String("repo-rev", "", "override the rev to scan (for debugging only)")
	RepoInterval = pflag.DurationP("repo-interval", "i", time.Minute*15, "poll interval for repo (0 to only pull once at startup)")
	Host         = pflag.String("host", "ottawa.ca", "host bare paths are redirected to (pages are served under /<host>/<path>)")
	Reader       = pflag.Bool("reader", true, "inject css to strip site chrome and make pages readable (and drop the page's own styling)")
	GitRate      = pflag.Float64("git-rate", 64, "sustained git processes per second for serving requests")
	GitBurst     = pflag.Int("git-burst", 128, "burst of git processes for serving requests")
	GitWait      = pflag.Duration("git-wait", time.Second*10, "how long a request waits for git quota before giving up")
	LogLevel     = pflagx.LevelP("log-level", "L", slog.LevelInfo, "log level")
	LogJSON      = pflag.Bool("log-json", false, "use json logs")
	Help         = pflag.BoolP("help", "h", false, "show this help text")
)

func main() {
	if val, ok := os.LookupEnv("PORT"); ok {
		if err := pflag.Set("addr", ":"+val); err != nil {
			panic(err)
		}
	}
	pflagx.ParseEnv(EnvPrefix)
	pflag.Parse()

	if *Help || pflag.NArg() != 0 {
		fmt.Printf("usage: %s [options]\n%s", os.Args[0], pflag.CommandLine.FlagUsages())
		if *Help {
			return
		}
		os.Exit(2)
	}

	if *LogJSON {
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: LogLevel,
		})))
	} else {
		slog.SetDefault(slog.New(tint.NewHandler(os.Stdout, &tint.Options{
			Level: LogLevel,
		})))
	}
	slog.SetLogLoggerLevel(LogLevel.Level())

	if err := run(); err != nil {
		slog.Error("failed to run server", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	if *Repo == "" {
		return fmt.Errorf("no repo path specified")
	}
	if *RepoBranch == "" {
		return fmt.Errorf("no branch specified for repo")
	}
	if _, err := os.Stat(filepath.Join(*Repo, "HEAD")); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("failed to access repo %q: %w", *Repo, err)
		}
		slog.Info("initializing git repo", "path", *Repo)
		if _, err := gitExec(ctx, "", "init", "--bare", *Repo); err != nil {
			return fmt.Errorf("initialize repo %q: %w", *Repo, err)
		}
	}

	// the history has to be there before anything can be served
	if *RepoRemote != "" {
		if err := fetch(ctx); err != nil {
			return fmt.Errorf("fetch repo: %w", err)
		}
	}

	// the settings which affect what is rendered are part of every etag
	httpx.AddExeExtra(fmt.Sprint(*Repo, "\x00", *RepoRev, "\x00", *RepoBranch, "\x00", *Host, "\x00", *Reader))

	srv, err := newServer(ctx, *Repo, cmp.Or(*RepoRev, *RepoBranch))
	if err != nil {
		return err
	}
	srv.log()

	slog.Info("updater: starting repo fetcher", "interval", *RepoInterval)
	go func() {
		ticker := time.Tick(*RepoInterval)
		for {
			if ticker == nil {
				slog.Warn("updater: repo polling disabled")
				return
			}
			<-ticker
			if *RepoRemote != "" {
				if err := fetch(context.Background()); err != nil {
					slog.Error("updater: fetch failed", "error", err)
					continue
				}
			}
			switch changed, err := srv.reload(context.Background()); {
			case err != nil:
				slog.Error("updater: reload failed", "error", err)
			case changed:
				srv.log()
			}
		}
	}()

	slog.Info("http: listening", "addr", *Addr)
	return http.ListenAndServe(*Addr, srv)
}

// fetch updates the local branch from the remote.
func fetch(ctx context.Context) error {
	slog.Info("updater: fetching repo", "remote", *RepoRemote, "branch", *RepoBranch)
	start := time.Now()
	if _, err := gitExec(ctx, "",
		"-C", *Repo,
		"fetch",
		"--no-write-fetch-head",
		"--refmap", "+refs/heads/"+*RepoBranch+":refs/heads/"+*RepoBranch, // +(force) (remote) (local)
		*RepoRemote,
		"refs/heads/"+*RepoBranch,
	); err != nil {
		return err
	}
	slog.Info("updater: fetched repo", "took", time.Since(start))
	return nil
}

const (
	// stampFormat is the version timestamp in urls. Browsing to a truncated one
	// (YYYY, YYYYMM, ...) resolves to the first version at or after it.
	stampFormat = "20060102150405"
	stampPad    = "00010101000000"

	rawParam = "__raw" // serve the stored page as a download
)

type version struct {
	Hash  string
	Time  time.Time
	Stamp string // YYYYMMDDHHMMSS[.VV], the version's id in urls
}

// etag hashes everything a response depends on, so it can be answered before
// any of the work is done. The binary's own hash is mixed in (the pages embed
// this program's css and markup), along with the settings which affect what it
// renders (see [httpx.AddExeExtra] in run).
func (s *server) etag(parts ...string) httpx.ETag {
	return httpx.NewETag().MixExe().Mix(parts...).ETag().
		Weaken() // weak: built from the render inputs, not the response bytes
}

// notModified sets the etag and reports whether the request can be answered
// with a 304.
func notModified(w http.ResponseWriter, r *http.Request, etag httpx.ETag) bool {
	h := w.Header()
	h.Set("Cache-Control", "no-cache")
	return etag.Handled(w, r)
}

// fontFace is the webarchive's only asset: everything it renders is Roboto.
var fontFace = sync.OnceValue(func() string {
	return `@font-face{font-family:'Roboto';font-style:normal;font-weight:100 900;` +
		`font-display:swap;src:url(` + static.Path(static.RobotoWOFF2) + `)}` +
		`@font-face{font-family:'Roboto';font-style:italic;font-weight:100 900;` +
		`font-display:swap;src:url(` + static.Path(static.RobotoItalicWOFF2) + `)}` +
		`@font-face{font-family:'Material Symbols Outlined';font-style:normal;` +
		`font-weight:300;src:url(` + static.Path(static.MaterialSymbolsOutlinedWOFF2) + `)}`
})

// iconCSS is the material symbols glyphs, which the subset can only be
// addressed by codepoint (ligature names aren't in it).
var iconCSS = sync.OnceValue(func() string {
	return `#__cpmb,#__cpth::before{font-family:'Material Symbols Outlined';` +
		`font-variation-settings:'FILL' 0,'wght' 300,'GRAD' 0,'opsz' 24;` +
		`font-weight:normal;font-style:normal;line-height:1;letter-spacing:normal;` +
		`text-transform:none;display:inline-block;-webkit-font-feature-settings:'liga';` +
		`-webkit-font-smoothing:antialiased}` +
		`#__cpth[data-theme=auto]::before{content:"` + static.Icon("brightness_auto") + `"}` +
		`#__cpth[data-theme=light]::before{content:"` + static.Icon("light_mode") + `"}` +
		`#__cpth[data-theme=dark]::before{content:"` + static.Icon("dark_mode") + `"}`
})

type server struct {
	repo  string
	rev   string
	limit *rate.Limiter // git processes, for work done on behalf of requests

	static http.Handler // the font, from the website's asset pipeline

	mu       sync.RWMutex   // held for the duration of a request, so the
	versions []version      // history a response is rendered from can't change
	index    map[string]int // under it (commit hash -> version index)

	trees   memo[string, map[string]string] // rev -> sha1(url) -> filename
	entries memo[string, []entry]           // rev -> cache entries, sorted by url
	urls    memo[string, map[string]string] // rev -> url -> filename
	hist    memo[string, []change]          // pathspec -> versions at which an entry changed
}

type entry struct {
	Name  string
	URL   string
	Title string // the page title, empty for non-html entries
	HTML  bool
}

// change is a version at which a cache entry changed, by how much, and how
// significant the change was.
type change struct {
	Version int `json:"v"`
	Added   int `json:"a"`
	Deleted int `json:"d"`
	Tier    int `json:"t"`

	name string // the entry filename at that version
}

// Change tiers, in increasing prominence. Only the text is compared, so markup,
// inline script and response header churn stays at tierNone.
const (
	tierGone     = iota - 1 // the entry was removed from the cache
	tierNone                // no visible text change
	tierText                // text changed outside the main content
	tierContent             // text changed in the main content
	tierSchedule            // a schedule table changed
)

// mainID is the main content block of an ottawa.ca page, matching what the
// scraper parses.
const mainID = "block-mainpagecontent"

func newServer(ctx context.Context, repo, rev string) (*server, error) {
	s := &server{
		repo:   repo,
		rev:    rev,
		limit:  rate.NewLimiter(rate.Limit(*GitRate), *GitBurst),
		static: static.Handler(static.Webarchive),
	}
	if _, err := s.reload(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// reload rescans the history, picking up anything fetched since the last time,
// and reports whether anything changed.
func (s *server) reload(ctx context.Context) (bool, error) {
	// oldest first, so seekbar position == version index
	buf, err := gitExec(ctx, s.repo, "rev-list", "--first-parent", "--reverse", "--timestamp", "--end-of-options", s.rev)
	if err != nil {
		return false, err
	}

	var versions []version
	for line := range strings.Lines(string(buf)) {
		ts, hash, ok := strings.Cut(strings.TrimSpace(line), " ")
		if !ok || !isLikelyGitHash(hash) {
			return false, fmt.Errorf("parse rev-list line %q", line)
		}
		sec, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			return false, fmt.Errorf("parse rev-list line %q: %w", line, err)
		}
		versions = append(versions, version{Hash: hash, Time: time.Unix(sec, 0)})
	}
	if len(versions) == 0 {
		return false, fmt.Errorf("no commits in %s", s.repo)
	}

	// urls address versions by time, so order by it (commit order is only
	// almost chronological) and stamp them, disambiguating shared seconds
	slices.SortStableFunc(versions, func(a, b version) int { return a.Time.Compare(b.Time) })
	for i := range versions {
		versions[i].Stamp = versions[i].Time.In(ottrecidx.TZ).Format(stampFormat)
	}
	for i := 0; i < len(versions); {
		j := i + 1
		for j < len(versions) && versions[j].Stamp == versions[i].Stamp {
			j++
		}
		if j-i > 1 {
			for n := i; n < j; n++ {
				versions[n].Stamp += fmt.Sprintf(".%02d", n-i)
			}
		}
		i = j
	}
	index := make(map[string]int, len(versions))
	for i, v := range versions {
		index[v.Hash] = i
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(versions) == len(s.versions) && versions[len(versions)-1].Hash == s.versions[len(s.versions)-1].Hash {
		return false, nil // nothing new
	}
	s.versions, s.index = versions, index
	s.hist.clear() // version indexes are baked into these; the rest is by commit
	return true, nil
}

// log reports what is being served.
func (s *server) log() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	slog.Info("history",
		"versions", len(s.versions),
		"first", s.versions[0].Time.In(ottrecidx.TZ).Format(time.DateOnly),
		"last", s.versions[len(s.versions)-1].Time.In(ottrecidx.TZ).Format(time.DateOnly))
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if r.Method == http.MethodConnect {
		http.Error(w, "ottrec-webarchive: https tunnelling is not supported; use it as an origin server instead", http.StatusNotImplemented)
		return
	}
	if strings.HasPrefix(r.URL.Path, static.Base) {
		s.static.ServeHTTP(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "ottrec-webarchive: only GET is cached", http.StatusMethodNotAllowed)
		return
	}
	latest := len(s.versions) - 1

	// an absolute-form request (a real proxy client) has nowhere to put a
	// timestamp, so it always gets the latest version
	if r.URL.IsAbs() {
		u := *r.URL
		u.Scheme, u.Fragment, u.RawFragment = "https", "", ""
		s.serveCached(w, r, latest, false, &u)
		return
	}

	// browsers and crawlers ask for these on their own; without this they look
	// like a host ("favicon.ico" has a dot in it) and cost a redirect and a
	// rendered error page each time
	switch r.URL.Path {
	case "/favicon.ico", "/robots.txt", "/.well-known/appspecific/com.chrome.devtools.json":
		http.NotFound(w, r)
		return
	}

	// there is no page at the root, so start at the index
	if r.URL.Path == "/" {
		w.Header().Set("Cache-Control", "no-store")
		http.Redirect(w, r, "/__cache", http.StatusFound)
		return
	}

	// "/<stamp>/<host>/<path>", any leading part of which may be missing
	path := strings.TrimPrefix(r.URL.EscapedPath(), "/")
	seg, rest, _ := strings.Cut(path, "/")
	cur, dated := s.parseStamp(seg)
	if !dated {
		cur, rest = latest, path
	}

	if rest == "__cache" || rest == "__cache/" {
		s.canonical(w, r, cur, dated, nil, func() { s.serveIndex(w, r, cur, dated) })
		return
	}

	h, p, _ := strings.Cut(rest, "/")
	hu, err := url.PathUnescape(h)
	if err != nil || !isHost(hu) {
		hu, p = *Host, rest // no host in the path: the default one
	}
	query, raw := r.URL.RawQuery, false
	if q := r.URL.Query(); q.Has(rawParam) {
		q.Del(rawParam)
		query, raw = q.Encode(), true
	}
	u, err := targetURL(hu, "/"+p, query)
	if err != nil {
		http.Error(w, "ottrec-webarchive: bad url", http.StatusBadRequest)
		return
	}
	if raw {
		s.serveRaw(w, r, cur, u)
		return
	}
	s.canonical(w, r, cur, dated, u, func() { s.serveCached(w, r, cur, dated, u) })
}

// canonical redirects to the canonical url of a page, or runs fn if this is it.
func (s *server) canonical(w http.ResponseWriter, r *http.Request, cur int, dated bool, u *url.URL, fn func()) {
	if want := s.pageURL(cur, dated, u); want != r.URL.RequestURI() {
		w.Header().Set("Cache-Control", "no-store")
		http.Redirect(w, r, want, http.StatusFound)
		return
	}
	fn()
}

// parseStamp resolves a url timestamp to the first version at or after it,
// clamped to the last one. A ".VV" suffix picks between versions which share a
// second.
func (s *server) parseStamp(seg string) (int, bool) {
	stamp, sub, hasSub := strings.Cut(seg, ".")
	if l := len(stamp); l < 4 || l > len(stampFormat) || l%2 != 0 || strings.Trim(stamp, "0123456789") != "" {
		return 0, false
	}
	t, err := time.ParseInLocation(stampFormat, stamp+stampPad[len(stamp):], ottrecidx.TZ)
	if err != nil {
		return 0, false
	}

	i, _ := slices.BinarySearchFunc(s.versions, t, func(v version, t time.Time) int {
		return v.Time.Compare(t)
	})
	i = min(i, len(s.versions)-1)

	if hasSub {
		n, err := strconv.Atoi(sub)
		if err != nil || n < 0 || i+n >= len(s.versions) {
			return 0, false
		}
		if s.versions[i+n].Time.Truncate(time.Second).Equal(s.versions[i].Time.Truncate(time.Second)) {
			i += n
		}
	}
	return i, true
}

// pageURL is the canonical proxy url of a page: always qualified with the
// scraped host, and with a version timestamp once one has been picked (an
// undated url follows the latest version).
func (s *server) pageURL(i int, dated bool, u *url.URL) string {
	var prefix string
	if dated {
		prefix = "/" + s.versions[i].Stamp
	}
	if u == nil {
		return prefix + "/__cache"
	}
	return prefix + "/" + u.Host + u.RequestURI()
}

// targetURL builds the url the scraper would have requested.
func targetURL(host, escapedPath, rawQuery string) (*url.URL, error) {
	u, err := url.Parse("https://" + host + cmp.Or(escapedPath, "/"))
	if err != nil {
		return nil, err
	}
	u.RawQuery = rawQuery
	return u, nil
}

// linkPrefix is what a page's own links are prefixed with, so following one
// stays at the version being viewed.
func (s *server) linkPrefix(cur int, dated bool) string {
	if !dated {
		return ""
	}
	return "/" + s.versions[cur].Stamp
}

// isHost reports whether a path segment addresses a host rather than being a
// bare path on the default one.
func isHost(seg string) bool {
	return strings.Contains(seg, ".")
}

func (s *server) serveCached(w http.ResponseWriter, r *http.Request, cur int, dated bool, u *url.URL) {
	var (
		ctx  = r.Context()
		hash = sha1.Sum([]byte(u.String()))
		key  = hex.EncodeToString(hash[:])
	)

	if notModified(w, r, s.etag("page", s.versions[cur].Hash, strconv.FormatBool(dated), u.String())) {
		return
	}

	name, err := s.resolve(ctx, cur, key, u)
	if err != nil {
		s.serveError(w, r, cur, dated, u, histSpec("", key), status(err), err.Error())
		return
	}
	if name == "" {
		// the query params may just be different (a tracking param, a page
		// number): go to the plain page, or to the only variation there is
		vs, err := s.variants(ctx, s.versions[cur].Hash, u)
		if err != nil {
			slog.Warn("find variants", "url", u, "error", err)
		}
		if to := pickVariant(vs); to != nil {
			w.Header().Set("Cache-Control", "no-store")
			http.Redirect(w, r, s.pageURL(cur, dated, to), http.StatusFound)
			return
		}

		// still show the seekbar: the page may be cached at another version, so
		// resolve its name there to get the tick marks
		latest, _ := s.resolve(ctx, len(s.versions)-1, key, u)
		s.serveError(w, r, cur, dated, u, histSpec(latest, key), http.StatusNotFound,
			"not cached at this version: "+u.String(), s.variantList(cur, dated, vs))
		return
	}
	spec := histSpec(name, key)

	buf, err := s.cat(ctx, s.versions[cur].Hash, name)
	if err != nil {
		s.serveError(w, r, cur, dated, u, spec, status(err), err.Error())
		return
	}

	_, resp, body, err := parseEntry(buf)
	if err != nil {
		s.serveError(w, r, cur, dated, u, spec, status(err), err.Error())
		return
	}

	h := w.Header()
	for _, k := range []string{"Content-Type", "Content-Language", "Date", "Last-Modified"} {
		if v := resp.Header.Get(k); v != "" {
			h.Set(k, v)
		}
	}
	h.Set("X-Ottrec-Webarchive-Entry", name)
	h.Set("X-Ottrec-Webarchive-Version", s.versions[cur].Hash)

	if strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/html") {
		if clean, err := sanitize(body, u.Host, s.linkPrefix(cur, dated)); err != nil {
			slog.Warn("sanitize", "url", u, "error", err)
		} else {
			body = clean
		}
		body = s.inject(ctx, h, body, r, cur, dated, u, spec, entryHead(buf))
	} else {
		// nothing to enhance, so lock it down entirely
		h.Set("Content-Security-Policy", "default-src 'none'; sandbox")
		h.Set("X-Robots-Tag", "noindex, noarchive, nofollow, noai, noimageai")
	}
	h.Set("Content-Length", strconv.Itoa(len(body)))

	w.WriteHeader(cmp.Or(resp.StatusCode, http.StatusOK))
	if r.Method != http.MethodHead {
		w.Write(body)
	}
}

// variants finds the entries cached for the same page with other query params.
func (s *server) variants(ctx context.Context, hash string, u *url.URL) ([]*url.URL, error) {
	es, err := s.list(ctx, hash)
	if err != nil {
		return nil, err
	}
	var vs []*url.URL
	for _, e := range es {
		v, err := url.Parse(e.URL)
		if err != nil {
			continue
		}
		if v.Host == u.Host && v.EscapedPath() == u.EscapedPath() && v.RawQuery != u.RawQuery {
			vs = append(vs, v)
		}
	}
	return vs, nil
}

// pickVariant returns the variation to redirect to: the page without any query
// params, or the only one there is. Anything more is ambiguous.
func pickVariant(vs []*url.URL) *url.URL {
	for _, v := range vs {
		if v.RawQuery == "" {
			return v
		}
	}
	if len(vs) == 1 {
		return vs[0]
	}
	return nil
}

// variantList is the "did you mean" list for the not found page.
func (s *server) variantList(cur int, dated bool, vs []*url.URL) string {
	if len(vs) == 0 {
		return ""
	}
	const max = 30

	var b strings.Builder
	fmt.Fprintf(&b, `<p style="font:14px/1.5 Roboto,system-ui,sans-serif;color:#6F6E69;margin:2rem 2rem 0">`+
		`cached with other query params (%d):</p><ul style="font:14px/1.6 Roboto,system-ui,sans-serif;margin:.4rem 2rem">`, len(vs))
	for _, v := range vs[:min(len(vs), max)] {
		fmt.Fprintf(&b, `<li><a href="%s" rel="nofollow">?%s</a></li>`,
			htmlpkg.EscapeString(s.pageURL(cur, dated, v)), htmlpkg.EscapeString(v.RawQuery))
	}
	if len(vs) > max {
		fmt.Fprintf(&b, `<li style="color:#6F6E69">and %d more</li>`, len(vs)-max)
	}
	b.WriteString(`</ul>`)
	return b.String()
}

// serveRaw hands over the stored response untouched.
func (s *server) serveRaw(w http.ResponseWriter, r *http.Request, cur int, u *url.URL) {
	ctx := r.Context()
	hash := sha1.Sum([]byte(u.String()))

	if notModified(w, r, s.etag("raw", s.versions[cur].Hash, u.String())) {
		return
	}

	name, err := s.resolve(ctx, cur, hex.EncodeToString(hash[:]), u)
	if err != nil {
		if code := status(err); code == http.StatusServiceUnavailable {
			w.Header().Set("Retry-After", "5")
			http.Error(w, "ottrec-webarchive: "+err.Error(), code)
		} else {
			http.Error(w, "ottrec-webarchive: "+err.Error(), code)
		}
		return
	}
	if name == "" {
		http.Error(w, "ottrec-webarchive: not cached at this version: "+u.String(), http.StatusNotFound)
		return
	}

	buf, err := s.cat(ctx, s.versions[cur].Hash, name)
	if err != nil {
		if code := status(err); code == http.StatusServiceUnavailable {
			w.Header().Set("Retry-After", "5")
			http.Error(w, "ottrec-webarchive: "+err.Error(), code)
		} else {
			http.Error(w, "ottrec-webarchive: "+err.Error(), code)
		}
		return
	}
	_, resp, body, err := parseEntry(buf)
	if err != nil {
		if code := status(err); code == http.StatusServiceUnavailable {
			w.Header().Set("Retry-After", "5")
			http.Error(w, "ottrec-webarchive: "+err.Error(), code)
		} else {
			http.Error(w, "ottrec-webarchive: "+err.Error(), code)
		}
		return
	}

	h := w.Header()
	h.Set("Content-Type", cmp.Or(resp.Header.Get("Content-Type"), "application/octet-stream"))
	h.Set("Content-Disposition", `attachment; filename="`+rawName(u, s.versions[cur].Stamp, resp)+`"`)
	h.Set("Content-Length", strconv.Itoa(len(body)))
	h.Set("X-Robots-Tag", "noindex, noarchive, nofollow, noai, noimageai")
	w.WriteHeader(cmp.Or(resp.StatusCode, http.StatusOK))
	if r.Method != http.MethodHead {
		w.Write(body)
	}
}

// rawName names the downloaded file after the page and the version.
func rawName(u *url.URL, stamp string, resp *http.Response) string {
	base := strings.Trim(u.Path, "/")
	if i := strings.LastIndexByte(base, '/'); i >= 0 {
		base = base[i+1:]
	}
	base = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		}
		return '-'
	}, cmp.Or(base, u.Host))

	ext := ".bin"
	if strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/html") {
		ext = ".html"
	} else if strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "json") {
		ext = ".json"
	}
	return base + "-" + stamp + ext
}

func (s *server) serveError(w http.ResponseWriter, r *http.Request, cur int, dated bool, u *url.URL, spec string, status int, msg string, extra ...string) {
	if status == http.StatusServiceUnavailable {
		// rendering the full page needs git too, which is the thing we're out of
		w.Header().Set("Retry-After", "5")
		http.Error(w, "ottrec-webarchive: "+msg, status)
		return
	}

	body := []byte(`<!DOCTYPE html><html><head><title>ottrec-webarchive</title></head><body>` +
		`<p style="font:14px/1.5 Roboto,system-ui,sans-serif;color:#6F6E69;margin:2rem">` +
		htmlpkg.EscapeString(msg) + `</p>` +
		strings.Join(extra, "") +
		`<p style="font:14px/1.5 Roboto,system-ui,sans-serif;margin:2rem"><a href="/__cache" rel="nofollow">cache index</a></p>` +
		`</body></html>`)
	h := w.Header()
	body = s.inject(r.Context(), h, body, r, cur, dated, u, spec, "")
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		w.Write(body)
	}
}

func (s *server) serveIndex(w http.ResponseWriter, r *http.Request, cur int, dated bool) {
	ctx := r.Context()

	if notModified(w, r, s.etag("index", s.versions[cur].Hash, strconv.FormatBool(dated))) {
		return
	}

	entries, err := s.list(ctx, s.versions[cur].Hash)
	if err != nil {
		s.serveError(w, r, cur, dated, nil, "", status(err), err.Error())
		return
	}

	var b strings.Builder
	b.WriteString(`<!DOCTYPE html><html><head><title>cache index</title></head>` +
		`<body style="font:14px/1.6 Roboto,system-ui,sans-serif;margin:2rem;max-width:60rem">`)
	fmt.Fprintf(&b, `<p style="color:#6F6E69">%d entries, %s</p><ul style="padding-left:1.2rem">`,
		len(entries), htmlpkg.EscapeString(s.versions[cur].Time.In(ottrecidx.TZ).Format("Mon 2006-01-02")))
	for _, e := range entries {
		fmt.Fprintf(&b, `<li><a href="%s" rel="nofollow">%s</a> <span style="color:#6F6E69">%s</span></li>`,
			htmlpkg.EscapeString(s.entryHref(e, cur, dated)), htmlpkg.EscapeString(entryPath(e)), htmlpkg.EscapeString(entryLabel(e)))
	}
	b.WriteString(`</ul></body></html>`)

	h := w.Header()
	body := s.inject(ctx, h, []byte(b.String()), r, cur, dated, nil, "", "")
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		w.Write(body)
	}
}

// parseEntry parses a cached request/response dump.
func parseEntry(buf []byte) (*http.Request, *http.Response, []byte, error) {
	r := bufio.NewReader(bytes.NewReader(buf))

	req, err := http.ReadRequest(r)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read cached request: %w", err)
	}
	req.URL.Scheme = "https"
	req.URL.Host = req.Host

	resp, err := http.ReadResponse(r, req)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read cached response: %w", err)
	}
	defer resp.Body.Close()

	var body io.Reader = resp.Body
	if strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		zr, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("read cached response: %w", err)
		}
		defer zr.Close()
		body = zr
	}

	buf, err = io.ReadAll(body)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read cached response: %w", err)
	}
	return req, resp, buf, nil
}

// startTag renders the start tag of an element for the comment which replaces
// it, with any "-->" escaped so it can't end the comment early.
func startTag(n *html.Node) string {
	var b strings.Builder
	b.WriteString("<")
	b.WriteString(n.Data)
	for _, a := range n.Attr {
		b.WriteString(" ")
		if a.Namespace != "" {
			b.WriteString(a.Namespace)
			b.WriteString(":")
		}
		b.WriteString(a.Key)
		if a.Val != "" {
			fmt.Fprintf(&b, "=%q", a.Val)
		}
	}
	b.WriteString(">")
	return strings.ReplaceAll(b.String(), "-->", "--&gt;")
}

// retitle wraps the page title, falling back to middle if the page has none.
func retitle(body []byte, prefix, middle, suffix string) []byte {
	lower := bytes.ToLower(body)
	if i := bytes.Index(lower, []byte("<title")); i >= 0 {
		if j := bytes.IndexByte(body[i:], '>'); j >= 0 {
			if k := bytes.Index(lower[i+j+1:], []byte("</title>")); k >= 0 {
				i, k = i+j+1, i+j+1+k
				return slices.Concat(body[:i], []byte(htmlpkg.EscapeString(prefix)), body[i:k],
					[]byte(htmlpkg.EscapeString(suffix)), body[k:])
			}
		}
	}
	return insertInHead(body, "<title>"+htmlpkg.EscapeString(strings.TrimSpace(prefix+middle+suffix))+"</title>")
}

// insertInBody puts a fragment at the start of the body.
func insertInBody(body []byte, frag string) []byte {
	return insertAfter(body, "<body", frag, true)
}

// insertInHead puts a fragment at the start of the head.
func insertInHead(body []byte, frag string) []byte {
	return insertAfter(body, "<head", frag, false)
}

// insertAfter puts a fragment after the given start tag, falling back to the
// start or the end of the document.
func insertAfter(body []byte, tag, frag string, prepend bool) []byte {
	if i := bytes.Index(bytes.ToLower(body), []byte(tag)); i >= 0 {
		if j := bytes.IndexByte(body[i:], '>'); j >= 0 {
			i += j + 1
			return slices.Concat(body[:i], []byte(frag), body[i:])
		}
	}
	if prepend {
		return slices.Concat([]byte(frag), body)
	}
	return slices.Concat(body, []byte(frag))
}

// stripTags are the elements which can load or run something. They're replaced
// with comment nodes, so what the page had stays visible in the source.
var stripTags = map[atom.Atom]bool{
	atom.Applet:   true,
	atom.Audio:    true,
	atom.Base:     true,
	atom.Canvas:   true,
	atom.Embed:    true,
	atom.Frame:    true,
	atom.Frameset: true,
	atom.Iframe:   true,
	atom.Link:     true,
	atom.Noscript: true,
	atom.Object:   true,
	atom.Script:   true,
	atom.Source:   true,
	atom.Style:    true,
	atom.Svg:      true,
	atom.Template: true,
	atom.Track:    true,
	atom.Video:    true,
}

// inertAttrs are attributes which load something, run something, or submit
// somewhere. They're kept as data-orig-* rather than removed, so the original
// value is still there to look at.
var inertAttrs = map[string]bool{
	"action":      true,
	"background":  true,
	"formaction":  true,
	"imagesrcset": true,
	"integrity":   true,
	"nonce":       true,
	"ping":        true,
	"srcset":      true,
}

// urlAttrs are attributes which get rewritten to point back at the proxy.
var urlAttrs = map[string]bool{
	"cite":     true,
	"data":     true,
	"href":     true,
	"longdesc": true,
	"poster":   true,
	"src":      true,
}

// sanitize strips everything which could reach the network and rewrites the
// remaining urls to point back at the proxy, so the CSP is a second line of
// defense rather than the only one.
func sanitize(body []byte, host, prefix string) ([]byte, error) {
	doc, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	var strip []*html.Node
	for n := range doc.Descendants() {
		if n.Type != html.ElementNode {
			continue
		}
		// the page's own styling is only worth keeping without the reader css;
		// its url() loads are blocked by the csp either way
		if n.DataAtom == atom.Style && !*Reader {
			sanitizeAttrs(n, host, prefix)
			continue
		}
		// a page's own meta refresh or csp would still apply through the proxy,
		// and its viewport (which may not even be there) is replaced below
		if stripTags[n.DataAtom] || (n.DataAtom == atom.Meta &&
			(attr(n, "http-equiv") != "" || strings.EqualFold(attr(n, "name"), "viewport"))) {
			strip = append(strip, n)
			continue
		}
		sanitizeAttrs(n, host, prefix)
	}
	for _, n := range strip {
		if n.Parent == nil {
			continue // nested in an already stripped element
		}
		n.Parent.InsertBefore(&html.Node{Type: html.CommentNode, Data: " ottrec-webarchive: " + startTag(n) + " "}, n)
		n.Parent.RemoveChild(n)
	}

	var b bytes.Buffer
	if err := html.Render(&b, doc); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func sanitizeAttrs(n *html.Node, host, prefix string) {
	for i, a := range n.Attr {
		key := strings.ToLower(a.Key)
		switch {
		case strings.HasPrefix(key, "on"), inertAttrs[key]:
			n.Attr[i] = origAttr(a)
		case key == "style":
			if strings.Contains(strings.ToLower(a.Val), "url(") {
				n.Attr[i] = origAttr(a)
			}
		case urlAttrs[key]:
			// offsite (or javascript:) urls keep their value, but not their
			// meaning: the link text stays readable and nothing is clickable
			if v, ok := rewriteURL(a.Val, host, prefix); ok {
				n.Attr[i].Val = v
			} else {
				n.Attr[i] = origAttr(a)
			}
		}
	}
}

// origAttr renames an attribute so it stays visible without doing anything.
func origAttr(a html.Attribute) html.Attribute {
	name := a.Key
	if a.Namespace != "" {
		name = a.Namespace + "-" + name
	}
	return html.Attribute{Key: "data-orig-" + strings.ToLower(name), Val: a.Val}
}

// entryHref links to a cache entry through the proxy.
func (s *server) entryHref(e entry, cur int, dated bool) string {
	u, err := url.Parse(e.URL)
	if err != nil {
		return "/"
	}
	return s.pageURL(cur, dated, u)
}

// entryPath is the part of a cache entry's url worth showing.
func entryPath(e entry) string {
	u, err := url.Parse(e.URL)
	if err != nil {
		return e.URL
	}
	return u.Host + u.RequestURI()
}

// entryLabel is what to call a cache entry: its page title, or its path. The
// place listing is paginated, and every page of it has the same title.
func entryLabel(e entry) string {
	if u, err := url.Parse(e.URL); err == nil && strings.HasSuffix(u.Path, "/place-listing") {
		page, _ := strconv.Atoi(u.Query().Get("page")) // 0 for the first page
		return fmt.Sprintf("%s (page %d)", cmp.Or(e.Title, "Place listing"), page+1)
	}
	if e.Title != "" {
		return e.Title
	}
	return entryPath(e)
}

// entryHead is the stored request and response headers, verbatim.
func entryHead(buf []byte) string {
	var head []byte
	for range 2 {
		i := bytes.Index(buf, []byte("\r\n\r\n"))
		if i < 0 {
			break
		}
		head = append(head, bytes.TrimRight(buf[:i], "\r\n")...)
		head = append(head, "\n\n"...)
		buf = buf[i+4:]
	}
	return strings.TrimSpace(string(head))
}

// pageTitle extracts the title of an html page, without parsing it (this runs
// over every entry of a version). The site suffixes every title with the site
// name, which is only noise here.
func pageTitle(body []byte) string {
	lower := bytes.ToLower(body)
	i := bytes.Index(lower, []byte("<title"))
	if i < 0 {
		return ""
	}
	j := bytes.IndexByte(body[i:], '>')
	if j < 0 {
		return ""
	}
	i += j + 1
	k := bytes.Index(lower[i:], []byte("</title>"))
	if k < 0 {
		return ""
	}
	t := strings.Join(strings.Fields(htmlpkg.UnescapeString(string(body[i:i+k]))), " ")
	if before, _, ok := strings.Cut(t, " | "); ok {
		t = before
	}
	return t
}

// rewriteURL points a url back at the proxy: urls on the scraped host become
// relative, and anything which would leave the proxy or run something is
// rejected.
func rewriteURL(v, host, prefix string) (string, bool) {
	s := strings.TrimSpace(v)
	if s == "" || strings.HasPrefix(s, "#") {
		return v, true
	}

	u, err := url.Parse(s)
	if err != nil {
		return "", false
	}
	switch strings.ToLower(u.Scheme) {
	case "mailto", "tel":
		return v, true // goes nowhere on its own
	case "", "http", "https":
		if u.Host == "" {
			if !strings.HasPrefix(s, "/") {
				return v, true // relative to the page, so already right
			}
			u.Host = host // root-relative: same host, but the proxy needs it
		}
		if h := strings.ToLower(u.Host); h != host && h != "www."+host && "www."+h != host {
			return "", false
		}
		u.Scheme, u.User = "", nil
		if u.Path == "" {
			u.Path = "/"
		}
		u.Path, u.RawPath = prefix+"/"+host+u.Path, ""
		u.Host = ""
		return u.String(), true
	}
	return "", false
}

// inject adds the version selector overlay and the CSP which keeps the page
// from reaching anything real, and sets the response headers it needs.
func (s *server) inject(ctx context.Context, h http.Header, body []byte, r *http.Request, cur int, dated bool, u *url.URL, spec, head string) []byte {
	var changes []change
	if spec != "" {
		var err error
		if changes, err = s.history(ctx, spec); err != nil {
			slog.Warn("read history", "spec", spec, "error", err)
		}
	}

	var buf [16]byte
	rand.Read(buf[:])
	nonce := hex.EncodeToString(buf[:])

	// 'unsafe-inline' for styles only, so the page keeps its inline styling
	// while its own scripts stay dead; nothing but data: images can load at all
	h.Set("Content-Security-Policy", "default-src 'none'; "+
		"style-src 'unsafe-inline'; "+
		"font-src 'self'; "+
		"img-src data:; "+
		"script-src 'nonce-"+nonce+"'; "+
		"connect-src 'self'; "+ // the overlay's client-side navigation
		"form-action 'none'; "+
		"base-uri 'none'; "+
		"frame-ancestors 'none'")

	var pages []entry
	if es, err := s.list(ctx, s.versions[cur].Hash); err != nil {
		slog.Warn("list entries", "version", s.versions[cur].Hash, "error", err)
	} else {
		for _, e := range es {
			if e.HTML {
				pages = append(pages, e)
			}
		}
		slices.SortFunc(pages, func(a, b entry) int {
			return cmp.Compare(strings.ToLower(entryLabel(a)), strings.ToLower(entryLabel(b)))
		})
	}

	// this is a copy of someone else's page: point at the original, and keep
	// it out of indexes and archives
	h.Set("X-Robots-Tag", "noindex, noarchive, nofollow, noai, noimageai")

	var meta strings.Builder
	meta.WriteString(`<meta name="viewport" content="width=device-width, initial-scale=1">`)
	meta.WriteString(`<meta name="robots" content="noindex,noarchive,nofollow,noai,noimageai">`)
	fmt.Fprintf(&meta, `<script nonce="%s">%s</script>`, nonce, themeJS)
	if u != nil {
		fmt.Fprintf(&meta, `<link rel="canonical" href="%s">`, htmlpkg.EscapeString(u.String()))
	}
	body = insertInHead(body, meta.String())

	// say what the tab is, and when it is from
	var source string
	if u != nil {
		source = u.Host + u.RequestURI() // the url, minus the scheme
	}
	body = retitle(body, "["+s.versions[cur].Time.In(ottrecidx.TZ).Format(time.DateOnly)+"] ", source, " (archived)")

	// the scraped url, at the top of the content rather than in the fixed bar:
	// it belongs to the page, and scrolls away with it
	if u != nil {
		raw := s.pageURL(cur, dated, u)
		if strings.Contains(raw, "?") {
			raw += "&" + rawParam
		} else {
			raw += "?" + rawParam
		}

		var bar strings.Builder
		bar.WriteString(`<input type="checkbox" id="__cpz">`)
		bar.WriteString(`<p id="__cpu">`)
		fmt.Fprintf(&bar, `<a href="%s" target="_blank" rel="nofollow noreferrer noopener">%s &#8599;</a><span id="__cpua">`,
			htmlpkg.EscapeString(u.String()), htmlpkg.EscapeString(u.String()))
		if head != "" {
			bar.WriteString(`<label for="__cpz"></label>`)
		}
		fmt.Fprintf(&bar, `<a href="%s" rel="nofollow" download>raw</a></span></p>`, htmlpkg.EscapeString(raw))
		if head != "" {
			fmt.Fprintf(&bar, `<pre id="__cpzh">%s</pre>`, htmlpkg.EscapeString(head))
		}
		body = insertInBody(body, bar.String())
	}

	overlay := s.overlay(nonce, cur, changes, dated, u, pages)
	if i := bytes.LastIndex(bytes.ToLower(body), []byte("</body>")); i >= 0 {
		return slices.Concat(body[:i], []byte(overlay), body[i:])
	}
	return slices.Concat(body, []byte(overlay))
}

// overlay renders the version seekbar: one bar per version, scaled by how much
// the page changed at it, scrolling horizontally when there are more versions
// than fit. Every bar is a plain link, so it works without javascript;
// javascript only relabels the status line while hovering and scrolls the
// current version into view.
func (s *server) overlay(nonce string, cur int, changes []change, dated bool, u *url.URL, pages []entry) string {
	data := struct {
		V       []string `json:"v"`
		Changes []change `json:"c"`
		Cur     int      `json:"cur"`
	}{Changes: changes, Cur: cur}
	for _, v := range s.versions {
		data.V = append(data.V, v.Time.In(ottrecidx.TZ).Format("Mon 2006-01-02 15:04"))
	}
	buf, _ := json.Marshal(data)
	js := strings.ReplaceAll(string(buf), "</", `<\/`)

	// bar heights are relative to the biggest change, with a floor so every
	// version stays clickable; versions where the page didn't change get a stub
	var maxLines int
	byVersion := make(map[int]change, len(changes))
	for _, c := range changes {
		byVersion[c.Version] = c
		if c.Tier > tierGone {
			maxLines = max(maxLines, c.Added+c.Deleted)
		}
	}

	var b strings.Builder
	b.WriteString(`<div id="__cp">`)
	b.WriteString(`<style>`)
	b.WriteString(fontFace())
	b.WriteString(iconCSS())
	if *Reader {
		b.WriteString(readerCSS)
	}
	b.WriteString(overlayCSS + `</style>`)
	fmt.Fprintf(&b, `<script type="application/json" id="__cpd" nonce="%s">%s</script>`, nonce, js)
	if len(changes) > 0 {
		// checked means showing them: the same markup for everyone, with the
		// preference applied by the script in the head
		b.WriteString(`<input type="checkbox" id="__cpx">`)
	}
	if len(pages) > 0 {
		b.WriteString(`<input type="checkbox" id="__cpm">`)
	}
	older, newer := "older", "newer"
	if len(changes) > 0 {
		older, newer = "older change", "newer change"
	}

	// month gridlines: anchored at the first version of each month, and — when
	// the unchanged ones are hidden — at the first one still shown, so the
	// gridline never props up a version with nothing in it
	shown := func(i int) bool {
		c, ok := byVersion[i]
		return i == cur || (ok && c.Tier > tierNone)
	}
	anchors := func(pick func(int) bool) map[int]*monthLabel {
		out := map[int]*monthLabel{}
		var (
			prev *monthLabel
			last time.Time
			span int
		)
		for i := range s.versions {
			if !pick(i) {
				continue
			}
			t := s.versions[i].Time.In(ottrecidx.TZ)
			if t.Month() != last.Month() || t.Year() != last.Year() {
				text := t.Format("Jan")
				if t.Year() != last.Year() {
					text = t.Format("Jan 2006") // the year, once a year
				}
				if prev != nil {
					prev.span = span
				}
				prev, span, last = &monthLabel{text: text}, 0, t
				out[i] = prev
			}
			span++
		}
		if prev != nil {
			prev.span = span
		}
		return out
	}
	months := anchors(func(int) bool { return true })
	monthsShown := anchors(shown)

	b.WriteString(`<div id="__cpw"><div id="__cpt" role="group" aria-label="cache version">`)
	for i := range s.versions {
		var h int
		switch c, ok := byVersion[i]; {
		case len(changes) == 0:
			h = 35 // no per-page history (the cache index), so just show the versions
		case !ok, c.Tier == tierGone:
			h = 0 // not cached at this version: a stub, still clickable
		default:
			h = barHeight(c.Added+c.Deleted, maxLines)
		}
		class := "__cpb"
		if c, ok := byVersion[i]; ok && c.Tier > tierGone {
			class += fmt.Sprintf(" __cpb-t%d", c.Tier)
		}
		if i == cur {
			class += " __cpb-cur"
		}

		fmt.Fprintf(&b, `<a class="%s" href="%s" rel="nofollow" data-i="%d" aria-label="%s">`,
			class, htmlpkg.EscapeString(s.pageURL(i, true, u)), i, s.versions[i].Time.In(ottrecidx.TZ).Format("Mon 2006-01-02"))
		switch m, f := months[i], monthsShown[i]; {
		case m != nil && f != nil && m.text == f.text:
			fmt.Fprintf(&b, `<b class="__cpm1 __cpm2" style="--s:%d;--sf:%d">%s</b>`, m.span, f.span, m.text)
		default:
			if m != nil {
				fmt.Fprintf(&b, `<b class="__cpm1" style="--s:%d">%s</b>`, m.span, m.text)
			}
			if f != nil {
				fmt.Fprintf(&b, `<b class="__cpm2" style="--sf:%d">%s</b>`, f.span, f.text)
			}
		}
		// the day only fits (and is only worth showing) once the unchanged
		// versions are hidden
		fmt.Fprintf(&b, `<i style="height:%d%%"></i><u>%d</u></a>`, h, s.versions[i].Time.In(ottrecidx.TZ).Day())
	}
	b.WriteString(`</div></div>`)
	b.WriteString(`<div id="__cph">`)
	if len(pages) > 0 {
		fmt.Fprintf(&b, `<label for="__cpm" id="__cpmb" title="pages">%s</label>`,
			htmlpkg.EscapeString(static.Icon("menu")))
	}
	fmt.Fprintf(&b, `<a class="__cpn" href="%s" rel="nofollow" title="%s">&lsaquo;</a>`,
		htmlpkg.EscapeString(s.pageURL(seekStep(changes, cur, -1, len(s.versions)), true, u)), older)
	date, change := statusLine(s.versions, cur, changes, cur)
	fmt.Fprintf(&b, `<p id="__cps"><span id="__cpsd">%s</span><span id="__cpsc">%s</span></p>`,
		htmlpkg.EscapeString(date), htmlpkg.EscapeString(change))
	if len(changes) > 0 {
		b.WriteString(`<label for="__cpx"></label>`)
	}
	fmt.Fprintf(&b, `<a class="__cpn" href="%s" rel="nofollow" title="%s">&rsaquo;</a>`,
		htmlpkg.EscapeString(s.pageURL(seekStep(changes, cur, +1, len(s.versions)), true, u)), newer)
	// it does nothing without scripts, so it stays hidden until they run
	b.WriteString(`<button type="button" id="__cpth" data-theme="auto" title="Toggle color scheme" hidden></button>`)
	b.WriteString(`</div>`)

	if len(pages) > 0 {
		b.WriteString(`<nav id="__cpsb">`)
		for _, e := range pages {
			href := s.entryHref(e, cur, dated)
			class := ""
			if u != nil && e.URL == u.String() {
				class = ` class="__cpsb-cur"`
			}
			fmt.Fprintf(&b, `<a href="%s"%s rel="nofollow" title="%s">%s</a>`,
				htmlpkg.EscapeString(href), class,
				htmlpkg.EscapeString(entryPath(e)), htmlpkg.EscapeString(entryLabel(e)))
		}
		b.WriteString(`</nav>`)
	}
	b.WriteString(`<span id="__cpp"></span>`)
	fmt.Fprintf(&b, `<script nonce="%s">%s</script>`, nonce, overlayJS)
	b.WriteString(`</div>`)
	return b.String()
}

// monthLabel is a month gridline: its text, and how many bars it has before the
// next one (so it can be clipped instead of running into it).
type monthLabel struct {
	text string
	span int
}

// barHeight scales a change to a bar height. The scale is logarithmic: a page
// rewrite is two orders of magnitude bigger than the daily churn, so a linear
// scale flattens everything but the outliers into stubs.
func barHeight(lines, max int) int {
	if lines <= 0 || max <= 0 {
		return 8
	}
	return 8 + int(92*math.Log1p(float64(lines))/math.Log1p(float64(max)))
}

// seekStep is the next version in the given direction which visibly changed, so
// the arrows skip over the runs of scrapes where nothing changed. It falls back
// to the adjacent version when there is none (or no history at all).
func seekStep(changes []change, cur, dir, n int) int {
	if dir < 0 {
		for i := len(changes) - 1; i >= 0; i-- {
			if c := changes[i]; c.Version < cur && c.Tier != tierNone {
				return c.Version
			}
		}
	} else {
		for _, c := range changes {
			if c.Version > cur && c.Tier != tierNone {
				return c.Version
			}
		}
	}
	return min(max(cur+dir, 0), n-1)
}

// statusLine describes version i, relative to the currently loaded one.
func statusLine(versions []version, cur int, changes []change, i int) (string, string) {
	date := versions[i].Time.In(ottrecidx.TZ).Format("Mon 2006-01-02 15:04")

	var b strings.Builder
	if len(changes) > 0 {
		j, found := slices.BinarySearchFunc(changes, i, func(c change, i int) int { return cmp.Compare(c.Version, i) })
		switch {
		case found && changes[j].Tier == tierGone:
			b.WriteString("  ·  cache cleared")
		case found && j == 0:
			fmt.Fprintf(&b, "  ·  first scrape  +%d/-%d", changes[j].Added, changes[j].Deleted)
		case found:
			fmt.Fprintf(&b, "  ·  %s  +%d/-%d", tierLabel(changes[j].Tier), changes[j].Added, changes[j].Deleted)
		case j == 0:
			b.WriteString("  ·  not scraped yet")
		case changes[j-1].Tier == tierGone:
			b.WriteString("  ·  not cached since ")
			b.WriteString(versions[changes[j-1].Version].Time.In(ottrecidx.TZ).Format("2006-01-02"))
		default:
			b.WriteString("  ·  unchanged since ")
			b.WriteString(versions[changes[j-1].Version].Time.In(ottrecidx.TZ).Format("2006-01-02"))
		}
	}
	return date, b.String()
}

// tierLabel describes a change tier in the status line.
func tierLabel(tier int) string {
	switch tier {
	case tierSchedule:
		return "schedule changed"
	case tierContent:
		return "content changed"
	case tierText:
		return "page text changed"
	}
	return "no text change"
}

const overlayCSS = `
html{--cp-h:6rem;--cp-w:15rem;color-scheme:light dark}
body{padding-top:var(--cp-h);padding-left:var(--cp-w)}
#__cp{position:fixed;left:0;right:0;top:0;z-index:2147483647;
 background:light-dark(#FFFCF0,#100F0F);border-bottom:1px solid light-dark(#E6E4D9,#282726);
 font:12px/1.4 Roboto,system-ui,sans-serif;padding:0 0 .45rem;
 color:light-dark(#6F6E69,#878580);-webkit-user-select:none;user-select:none}
#__cp *{box-sizing:border-box}
#__cph{display:flex;align-items:center;gap:.6rem;margin:.35rem 0 0;padding:0 .7rem}
#__cpmb{display:none;flex:none;cursor:pointer;font-size:17px;line-height:1;
 color:light-dark(#6F6E69,#878580)}
#__cpmb:hover{color:light-dark(#100F0F,#FFFCF0)}
#__cps{flex:1;min-width:0;margin:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
 font-variant-numeric:tabular-nums;min-height:1.4em}
#__cpsc{white-space:pre}
[data-cp-busy] #__cps{opacity:.45}
/* the checkboxes are only state: the label and the hamburger are the ui */
#__cpx,#__cpm{position:absolute;width:1px;height:1px;margin:0;opacity:0;pointer-events:none}
#__cp label[for="__cpx"]{flex:none;cursor:pointer;white-space:nowrap;user-select:none;
 font-size:11px;color:light-dark(#205EA6,#4385BE);text-decoration:none}
#__cp label[for="__cpx"]::after{content:"show unchanged"}
:root[data-cp-show] #__cp label[for="__cpx"]::after,
#__cpx:checked~#__cph label[for="__cpx"]::after{content:"hide unchanged"}
#__cp label[for="__cpx"]:hover{text-decoration:underline;text-underline-offset:2px}
#__cpx:focus-visible~#__cph label[for="__cpx"]{outline:1px solid light-dark(#205EA6,#4385BE);
 outline-offset:2px}
#__cp .__cpn{flex:none;color:light-dark(#6F6E69,#878580);text-decoration:none;
 font-size:15px;line-height:1}
#__cpth{flex:none;margin:-4px 0;padding:.25em;border-radius:50%;line-height:0;
 background:none;cursor:pointer;color:light-dark(#6F6E69,#878580);
 border:1px solid light-dark(#E6E4D9,#282726)}
#__cpth::before{font-size:13px}
#__cpth:hover{color:light-dark(#100F0F,#FFFCF0);border-color:light-dark(#B7B5AC,#575653)}
#__cp .__cpn:hover{color:light-dark(#100F0F,#FFFCF0)}
/* a fixed height, so the bar doesn't resize when the strip stops overflowing
   and its scrollbar goes away; the bars take whatever is left instead */
#__cpw{height:calc(3.6rem + 6px);overflow-x:auto;overflow-y:hidden;
 scrollbar-width:thin;scrollbar-color:transparent transparent;
 background:light-dark(#F2F0E5,#1C1B1A);border-bottom:1px solid light-dark(#E6E4D9,#282726)}
/* the scrollbar stays out of the way until it's being used */
#__cpw:hover,#__cpw:focus-within{scrollbar-color:light-dark(rgba(16,15,15,.22),rgba(255,252,240,.22)) transparent}
#__cpw::-webkit-scrollbar{height:6px}
#__cpw::-webkit-scrollbar-track{background:transparent}
#__cpw::-webkit-scrollbar-thumb{background:transparent;border-radius:3px}
#__cpw:hover::-webkit-scrollbar-thumb{background:light-dark(rgba(16,15,15,.22),rgba(255,252,240,.22))}
#__cpw::-webkit-scrollbar-thumb:active{background:light-dark(rgba(16,15,15,.4),rgba(255,252,240,.4))}
#__cpw.__cpw-l{-webkit-mask-image:linear-gradient(90deg,transparent 0,#000 18px);
 mask-image:linear-gradient(90deg,transparent 0,#000 18px)}
#__cpw.__cpw-r{-webkit-mask-image:linear-gradient(90deg,#000 calc(100% - 18px),transparent 100%);
 mask-image:linear-gradient(90deg,#000 calc(100% - 18px),transparent 100%)}
#__cpw.__cpw-l.__cpw-r{-webkit-mask-image:linear-gradient(90deg,transparent 0,#000 18px,#000 calc(100% - 18px),transparent 100%);
 mask-image:linear-gradient(90deg,transparent 0,#000 18px,#000 calc(100% - 18px),transparent 100%)}
#__cpt{display:flex;align-items:flex-end;gap:0;height:100%;padding:14px 0 2px;
 width:max-content;min-width:100%}
#__cpw{--bw:5px}
#__cp .__cpb{position:relative;flex:1 0 var(--bw);display:flex;align-items:flex-end;justify-content:center;
 height:100%;text-decoration:none;background:transparent;
 -webkit-user-drag:none;-webkit-touch-callout:none}
/* month gridline, labelled at its top left */
#__cp .__cpb b{position:absolute;left:0;top:-14px;bottom:0;padding-left:3px;pointer-events:none;
 border-left:1px solid light-dark(rgba(16,15,15,.18),rgba(255,252,240,.18));
 font:400 10px/14px Roboto,system-ui,sans-serif;font-variant-numeric:tabular-nums;
 color:light-dark(#6F6E69,#878580);white-space:nowrap;overflow:hidden;
 width:max(3em,calc(var(--bw)*var(--s,1) - 2px));
 -webkit-mask-image:linear-gradient(90deg,#000 calc(100% - 9px),transparent);
 mask-image:linear-gradient(90deg,#000 calc(100% - 9px),transparent)}
#__cp .__cpb i{display:block;width:100%;margin:0 .5px;min-height:2px;border-radius:1px;
 background:light-dark(#E6E4D9,#282726)}
/* change tiers: markup only, text somewhere, main content, schedule */
#__cp .__cpb-t0 i{background:light-dark(#DAD8CE,#343331)}
#__cp .__cpb-t1 i{background:light-dark(#878580,#6F6E69)}
#__cp .__cpb-t2 i{background:light-dark(#587FA6,#4F7593)}
#__cp .__cpb-t3 i{background:light-dark(#B0763F,#9C6B3D)}
#__cp .__cpb:hover{background:light-dark(rgba(16,15,15,.08),rgba(255,252,240,.1))}
#__cp .__cpb-cur{background:light-dark(rgba(88,127,166,.2),rgba(79,117,147,.28))}
#__cp .__cpb:focus-visible{outline:1px solid light-dark(#205EA6,#4385BE);outline-offset:-1px}
/* the scraped url, above the page content */
#__cpu{position:relative;display:flex;align-items:baseline;gap:1.2rem;margin:0 0 .8rem;
 padding:.35rem 1rem;
 font:11px/1.6 Roboto,system-ui,sans-serif;
 color:light-dark(#878580,#6F6E69);
 border-bottom:1px solid light-dark(#E6E4D9,#282726)}
#__cpu>a{flex:1;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
 color:inherit;text-decoration:none}
#__cpu>a:hover{color:light-dark(#100F0F,#FFFCF0);text-decoration:underline;
 text-underline-offset:2px}
#__cpua{flex:none;display:flex;gap:1rem}
#__cpua a,#__cpua label{color:light-dark(#205EA6,#4385BE);text-decoration:none;
 cursor:pointer;white-space:nowrap}
#__cpua a:hover,#__cpua label:hover{text-decoration:underline;text-underline-offset:2px}
#__cpua label::after{content:"headers"}
#__cpz:checked~#__cpu #__cpua label::after{content:"hide headers"}
#__cpz{position:absolute;width:1px;height:1px;margin:0;opacity:0;pointer-events:none}
#__cpzh{display:none;margin:-.8rem 0 .8rem;padding:.6rem 1rem;overflow-x:auto;white-space:pre;
 font:11px/1.5 Roboto,system-ui,sans-serif;font-variant-numeric:tabular-nums;
 color:light-dark(#6F6E69,#878580);background:light-dark(#F2F0E5,#1C1B1A);
 border-bottom:1px solid light-dark(#E6E4D9,#282726)}
#__cpz:checked~#__cpzh{display:block}

/* client-side navigation is fast, but not always instant: an indeterminate
   progress bar rides the bottom edge of the top bar while it isn't */
#__cpp{position:absolute;left:0;right:0;bottom:-1px;height:2px;overflow:hidden;
 opacity:0;pointer-events:none;transition:opacity .1s}
[data-cp-busy] #__cpp{opacity:1;transition-delay:.15s}
/* the animation only runs while busy, so it always starts from the left, and
   its delay matches the fade so it appears at the left edge */
#__cpp::before{content:"";position:absolute;top:0;bottom:0;left:-30%;width:30%;
 background:light-dark(#587FA6,#4F7593)}
[data-cp-busy] #__cpp::before{animation:__cpslide 1.1s ease-in-out .15s infinite}
@keyframes __cpslide{from{left:-30%}to{left:100%}}
@media (prefers-reduced-motion:reduce){#__cpp::before{animation:none;left:0;width:100%;opacity:.6}}

/* page list */
#__cpsb{position:fixed;left:0;top:var(--cp-h);bottom:0;width:var(--cp-w);z-index:2147483646;
 -webkit-user-select:none;user-select:none;
 overflow-y:auto;overscroll-behavior:contain;padding:.3rem 0;
 background:light-dark(#FFFCF0,#100F0F);border-right:1px solid light-dark(#E6E4D9,#282726)}
#__cpsb a{display:block;padding:.3rem .6rem;text-decoration:none;
 color:light-dark(#100F0F,#CECDC3);border-left:2px solid transparent;
 white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#__cpsb a:hover{background:light-dark(#F2F0E5,#1C1B1A)}
#__cpsb a.__cpsb-cur{border-left-color:light-dark(#205EA6,#4385BE);
 background:light-dark(rgba(32,94,166,.1),rgba(67,133,190,.14))}

/* hiding the versions with no visible change leaves room for wider bars; the
   current version always stays */
:root:not([data-cp-show]) #__cpx:not(:checked)~#__cpw .__cpb:not(.__cpb-t1):not(.__cpb-t2):not(.__cpb-t3):not(.__cpb-cur){display:none}
:root:not([data-cp-show]) #__cpx:not(:checked)~#__cpw{--bw:17px}
/* each view has its own gridlines: all months, or only the ones with something
   left in them once the unchanged versions are hidden */
#__cpw .__cpb b.__cpm2:not(.__cpm1){display:none}
:root:not([data-cp-show]) #__cpx:not(:checked)~#__cpw .__cpb b.__cpm1:not(.__cpm2){display:none}
:root:not([data-cp-show]) #__cpx:not(:checked)~#__cpw .__cpb b.__cpm2{display:block}
:root:not([data-cp-show]) #__cpx:not(:checked)~#__cpw .__cpb b{width:max(3em,calc(var(--bw)*var(--sf,1) - 2px))}
#__cp .__cpb u{display:none}
/* the day goes in a band under the bars, like the month labels above them */
:root:not([data-cp-show]) #__cpx:not(:checked)~#__cpw #__cpt{padding-bottom:14px}
:root:not([data-cp-show]) #__cpx:not(:checked)~#__cpw .__cpb u{display:block;position:absolute;left:0;right:0;bottom:-14px;
 text-align:center;text-decoration:none;pointer-events:none;
 font:400 10px/14px Roboto,system-ui,sans-serif;font-variant-numeric:tabular-nums;
 color:light-dark(#6F6E69,#878580)}
/* printing gets the page, not the viewer: no chrome, no dark theme, and no
   scroll containers to clip the wide tables */
@media print{
 html{color-scheme:light !important}
 body{padding:0 !important}
 #__cp,#__cpu,#__cpzh{display:none !important}
 .table-responsive{overflow:visible !important}
 .table-responsive>table{min-width:0}
}

/* touch needs a bigger target, so scroll sooner */
@media (pointer:coarse),(max-width:640px){
 #__cpw{--bw:10px}
 :root:not([data-cp-show]) #__cpx:not(:checked)~#__cpw{--bw:24px}
 /* bigger targets, and no room for what changed */
 #__cpsc{display:none}
 #__cp .__cpn,#__cpmb{display:inline-flex;align-items:center;justify-content:center;
  min-width:34px;min-height:34px;margin:-6px 0;font-size:20px}
 #__cp label[for="__cpx"]{font-size:13px;padding:.25rem 0}
 /* the page list becomes a drawer behind the hamburger */
 body{padding-left:0}
 #__cpmb{display:inline-flex}
 #__cpsb{width:min(20rem,85vw);transform:translateX(-100%);transition:transform .15s ease}
 #__cpm:checked~#__cpsb{transform:none;box-shadow:0 0 0 100vmax light-dark(rgba(16,15,15,.3),rgba(16,15,15,.6))}
}
`

const readerCSS = `
html{color-scheme:light dark;background:light-dark(#FFFCF0,#100F0F)}
body{margin:0;background:light-dark(#FFFCF0,#100F0F);
 color:light-dark(#100F0F,#CECDC3);
 font:14.5px/1.6 Roboto,system-ui,sans-serif}
#ottux-header,#ottux-footer,#ottux-topbutton,#ottca-chatbot,#popupSurveyModal,
#toolbar-administration,.breadcrumb,.skip-link,.sr-only,.visually-hidden,
[role=banner],[role=contentinfo],[role=dialog],[role=navigation],nav:not(#__cpsb),noscript,
iframe,script,style,link,svg,form.antibot{display:none!important}
#block-placesearchfacetsblock,.views-exposed-form{display:none!important}
#main-content,#content,main,.node__content{max-width:none;margin:0;padding:0 1rem}
/* the city's multi-column blocks, which its own css would have laid out */
.grid-divider.toc-selectors.layout{display:grid;gap:.4rem 2rem;margin:1em 0;
 grid-template-columns:repeat(auto-fit,minmax(18rem,1fr))}
.grid-divider.toc-selectors.layout>.layout__region{min-width:0}
h1,h2,h3,h4{line-height:1.25;margin:1.4em 0 .4em;font-weight:600}
h1{font-size:1.7rem}h2{font-size:1.3rem}h3{font-size:1.1rem}h4{font-size:1rem}
p,li{margin:.4em 0}
a{color:light-dark(#205EA6,#4385BE)}
hr{border:0;border-top:1px solid light-dark(#E6E4D9,#282726);margin:1.5em 0}
/* images never load (the csp blocks them), so mark out where they were */
img{max-width:100%;height:auto;min-width:2.5rem;min-height:2.5rem;object-fit:contain;
 font-size:11px;color:light-dark(#B7B5AC,#575653);
 border:1px solid light-dark(#E6E4D9,#282726);border-radius:2px;
 background:repeating-linear-gradient(45deg,transparent 0 5px,
  light-dark(rgba(16,15,15,.05),rgba(255,252,240,.06)) 5px 10px)}
table{border-collapse:collapse;width:100%;margin:.8em 0;font-size:.9em}
th,td{border:1px solid light-dark(#E6E4D9,#282726);padding:.3rem .5rem;text-align:left;vertical-align:top}
th{background:light-dark(#F2F0E5,#1C1B1A);font-weight:600}
caption{text-align:left;font-weight:600;padding:.4em 0}
/* wide schedule tables scroll instead of squeezing */
.table-responsive{max-width:100%;overflow-x:auto;overscroll-behavior-x:contain}
.table-responsive>table{min-width:34rem}
details{border:1px solid light-dark(#E6E4D9,#282726);border-radius:3px;
 padding:.5rem .8rem;margin:.6em 0}
/* accordion toggles: dead without scripts, so make them read as section headers
   rather than platform buttons, with their section attached underneath */
[data-toggle="collapse"]{display:block;width:100%;box-sizing:border-box;margin:0;
 padding:.4rem .7rem;text-align:left;cursor:default;font:inherit;font-weight:600;
 color:light-dark(#100F0F,#CECDC3);background:light-dark(#F2F0E5,#1C1B1A);
 border:1px solid light-dark(#E6E4D9,#282726);border-radius:3px 3px 0 0}
h1.no-toc,h2.no-toc,h3.no-toc,h4.no-toc{margin:1.4em 0 0}
.collapse-wrapper{border:1px solid light-dark(#E6E4D9,#282726);border-top:0;
 border-radius:0 0 3px 3px;padding:.6rem .8rem;margin:0 0 1em}
/* the referenced article repeats the section title the toggle already shows */
.collapse-wrapper .field--name-title{display:none}
`

// themeJS applies the saved color scheme in the head, before anything paints.
const themeJS = `
try{
 const t=localStorage.getItem('theme')
 if(t==='light'||t==='dark')document.documentElement.style.colorScheme=t
 else if(t==='auto')document.documentElement.style.colorScheme='light dark'
 // the seekbar hides the unchanged versions unless this says otherwise; it is
 // in localstorage rather than a cookie so every response is the same bytes
 if(localStorage.getItem('unchanged')==='show')document.documentElement.dataset.cpShow='1'
}catch(e){}
`

// progressive enhancement
const overlayJS = `
(()=>{
 const $=s=>document.querySelector(s)
 let data=null

 const fit=()=>{
  const root=$('#__cp')
  if(root)document.documentElement.style.setProperty('--cp-h',root.offsetHeight+'px')
 }
 const fade=()=>{
  const w=$('#__cpw')
  if(!w)return
  w.classList.toggle('__cpw-l',w.scrollLeft>2)
  w.classList.toggle('__cpw-r',w.scrollLeft+w.clientWidth<w.scrollWidth-2)
 }
 const date=i=>data.v[i].slice(4,14)
 const showsUnchanged=()=>{
  try{
   return localStorage.getItem('unchanged')==='show'
  }catch(e){}
  return false
 }
 const applyUnchanged=show=>{
  document.documentElement.toggleAttribute('data-cp-show',show)
  const el=$('#__cpx')
  if(el)el.checked=show
 }
 const savedTheme=()=>{
  try{
   const t=localStorage.getItem('theme')
   if(t==='light'||t==='dark'||t==='auto')return t
  }catch(e){}
  return 'auto'
 }
 const applyTheme=t=>{
  document.documentElement.style.colorScheme=t==='auto'?'light dark':t
  const el=$('#__cpth')
  if(el){
   el.dataset.theme=t
   el.setAttribute('aria-label','Color scheme: '+t)
  }
 }
 const label=t=>['no text change','page text changed','content changed','schedule changed'][t]||''
 const line=i=>{
  const changes=data.c||[]
  let s=''
  if(changes.length){
   const c=changes.find(x=>x.v===i)
   if(c&&c.t<0)s+='  ·  cache cleared'
   else if(c)s+='  ·  '+(c===changes[0]?'first scrape':label(c.t))+'  +'+c.a+'/-'+c.d
   else{
    const prev=changes.filter(x=>x.v<i).pop()
    if(!prev)s+='  ·  not scraped yet'
    else s+='  ·  '+(prev.t<0?'not cached since ':'unchanged since ')+date(prev.v)
   }
  }
  return [data.v[i],s]
 }

 // bind wires up the overlay; it runs again after each client-side navigation,
 // since the whole body (overlay included) is swapped out
 const bind=at=>{
  const root=$('#__cp'), island=$('#__cpd')
  if(!root||!island)return
  data=JSON.parse(island.textContent)
  const date=$('#__cpsd'), change=$('#__cpsc'), strip=$('#__cpt'), wrap=$('#__cpw')
  if(!date||!change||!strip||!wrap)return

  // firefox has no user-drag, so dragging a bar or a page link would start a
  // link drag instead of seeking
  root.addEventListener('dragstart',e=>e.preventDefault())

  const show=i=>{
   const [d,c]=line(Math.max(0,Math.min(data.v.length-1,i)))
   date.textContent=d
   change.textContent=c
  }
  const bar=e=>e.target instanceof Element?e.target.closest('.__cpb'):null
  strip.addEventListener('pointerover',e=>{const b=bar(e);if(b)show(+b.dataset.i)})
  strip.addEventListener('focusin',e=>{const b=bar(e);if(b)show(+b.dataset.i)})
  strip.addEventListener('pointerleave',()=>show(data.cur))

  fit()
  if(at==null){
   const cur=strip.querySelector('.__cpb-cur')
   if(cur)cur.scrollIntoView({block:'nearest',inline:'center'})
  }else{
   wrap.scrollLeft=at
  }
  fade()
  wrap.addEventListener('scroll',fade,{passive:true})

  // pointing at either end of the strip scrolls it, so seeking a year back
  // doesn't need the scrollbar
  if(matchMedia('(pointer:fine)').matches){
   let vel=0,raf=0
   const tick=()=>{
    if(!vel){raf=0;return}
    wrap.scrollLeft+=vel
    raf=requestAnimationFrame(tick)
   }
   wrap.addEventListener('pointermove',e=>{
    const r=wrap.getBoundingClientRect(), z=Math.min(90,r.width/6)
    const l=e.clientX-r.left, x=r.right-e.clientX
    vel=l<z?-(1-l/z)*14:x<z?(1-x/z)*14:0
    if(vel&&!raf)raf=requestAnimationFrame(tick)
   })
   wrap.addEventListener('pointerleave',()=>{vel=0})
  }

  // modelled on the website's navbar toggle: light -> dark -> follow the os
  const theme=$('#__cpth')
  if(theme){
   theme.addEventListener('click',()=>{
    const next=({auto:'light',light:'dark',dark:'auto'})[savedTheme()]
    try{localStorage.setItem('theme',next)}catch(e){}
    applyTheme(next)
   })
   theme.hidden=false
   applyTheme(savedTheme())
  }

  const filter=$('#__cpx')
  if(filter){
   filter.checked=showsUnchanged()
   filter.addEventListener('change',()=>{
    try{localStorage.setItem('unchanged',filter.checked?'show':'hide')}catch(e){}
    applyUnchanged(filter.checked)
    const cur=strip.querySelector('.__cpb-cur')
    if(cur)cur.scrollIntoView({block:'nearest',inline:'center'})
    fade()
   })
  }
 }

 // topmost identified element and where it sits, to put the content back
 // roughly where it was after a swap
 const anchor=()=>{
  let best=null
  for(const el of document.querySelectorAll('[id]')){
   if(el.closest('#__cp'))continue
   const r=el.getBoundingClientRect()
   if(r.height&&r.top<=40&&(!best||r.top>best.top))best={id:el.id,top:r.top}
  }
  return best
 }

 let ctl=null
 const fail=(url,err)=>{
  console.error('ottrec-webarchive: load '+url+':',err)
  const date=$('#__cpsd'), change=$('#__cpsc')
  if(!date)return
  const was=[date.textContent,change?change.textContent:'']
  date.textContent='load failed: '+((err&&err.message)||err||'unknown')
  if(change)change.textContent=''
  setTimeout(()=>{
   if(date.isConnected&&date.textContent.startsWith('load failed')){
    date.textContent=was[0]
    if(change)change.textContent=was[1]
   }
  },4000)
 }

 const go=async(href,pop)=>{
  const url=new URL(href,location.href)
  ctl?.abort() // a newer click supersedes whatever is still in flight
  const c=ctl=new AbortController()
  const root=$('#__cp'), wrap=$('#__cpw'), side=$('#__cpsb')
  const at=wrap?wrap.scrollLeft:null, top=side?side.scrollTop:0
  // a version switch stays on the same page (only the timestamp segment
  // changes), so keep the reading position
  const page=p=>p.replace(/^\/\d{4,14}(\.\d+)?(?=\/)/,'')
  const keep=page(url.pathname)===page(location.pathname)?anchor():null
  document.documentElement.dataset.cpBusy='1'
  try{
   const res=await fetch(url,{credentials:'same-origin',signal:c.signal})
   if(!(res.headers.get('content-type')||'').includes('text/html')){
    location.href=url.href // not a page: let the browser deal with it
    return
   }
   const doc=new DOMParser().parseFromString(await res.text(),'text/html')
   if(!pop)history.pushState(null,'',res.url||url)
   document.title=doc.title
   document.body.replaceWith(doc.body)
   try{
    bind(at)
   }catch(err){
    console.error('ottrec-webarchive: bind:',err) // the page is still usable unbound
   }
   const sb=$('#__cpsb')
   if(sb)sb.scrollTop=top
   const el=keep&&document.getElementById(keep.id)
   scrollTo(0,el?Math.max(0,scrollY+el.getBoundingClientRect().top-keep.top):0)
  }catch(err){
   if(c.signal.aborted)return
   fail(url.href,err)
  }finally{
   if(ctl===c)ctl=null
   if(!ctl)delete document.documentElement.dataset.cpBusy
  }
 }

 addEventListener('click',e=>{
  if(e.defaultPrevented||e.button||e.metaKey||e.ctrlKey||e.shiftKey||e.altKey)return
  const a=e.target instanceof Element?e.target.closest('a[href]'):null
  if(!a||a.target||a.hasAttribute('download'))return
  const url=new URL(a.getAttribute('href'),location.href)
  if(url.origin!==location.origin)return
  if(url.hash&&url.pathname===location.pathname&&url.search===location.search)return
  e.preventDefault()
  go(url.href)
 })
 // the drawer's scrim is only a shadow, so closing it needs a hand
 addEventListener('click',e=>{
  const menu=$('#__cpm')
  if(!menu||!menu.checked||!(e.target instanceof Element))return
  if(e.target.closest('#__cpsb,#__cpmb,label[for="__cpm"]'))return
  menu.checked=false
 })
 addEventListener('keydown',e=>{
  const menu=$('#__cpm')
  if(e.key==='Escape'&&menu&&menu.checked)menu.checked=false
 })
 addEventListener('storage',e=>{
  if(e.key==='theme'||e.key===null)applyTheme(savedTheme())
  if(e.key==='unchanged'||e.key===null)applyUnchanged(showsUnchanged())
 })
 addEventListener('popstate',()=>go(location.href,true))
 addEventListener('resize',()=>{fit();fade()})
 bind(null)
})()
`

// git plumbing, adapted from website/internal/gitsh.

// errBusy is returned when a request waited too long for git quota.
var errBusy = errors.New("busy")

// wait blocks until there is quota for another git process. Only work done on
// behalf of a request is metered; reading the history at startup isn't.
func (s *server) wait(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, *GitWait)
	defer cancel()
	if err := s.limit.Wait(ctx); err != nil {
		return fmt.Errorf("%w: waited %s for git quota: %w", errBusy, *GitWait, err)
	}
	return nil
}

// status is the response status for an error.
func status(err error) int {
	if errors.Is(err, errBusy) {
		return http.StatusServiceUnavailable
	}
	return http.StatusInternalServerError
}

// git runs a git command for a request, within the rate limit.
func (s *server) git(ctx context.Context, arg ...string) ([]byte, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return gitExec(ctx, s.repo, arg...)
}

func gitExec(ctx context.Context, dir string, arg ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", arg...)
	cmd.Dir = dir
	cmd.Stdin = nil

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, transformError(err, stderr.Bytes())
	}
	return stdout.Bytes(), nil
}

// tree indexes the cache entries of a commit by the sha1 of their url.
func (s *server) tree(ctx context.Context, hash string) (map[string]string, error) {
	return s.trees.get(hash, func(hash string) (map[string]string, error) {
		buf, err := s.git(ctx, "ls-tree", "-r", "--name-only", "--end-of-options", hash)
		if err != nil {
			return nil, err
		}
		m := map[string]string{}
		for line := range strings.Lines(string(buf)) {
			name := strings.TrimSpace(line)
			if _, key, ok := strings.Cut(name, "-"); ok && len(key) == hex.EncodedLen(sha1.Size) {
				m[key] = name
			}
		}
		return m, nil
	})
}

// list gets the cached urls of a commit, sorted.
func (s *server) list(ctx context.Context, hash string) ([]entry, error) {
	return s.entries.get(hash, func(hash string) ([]entry, error) {
		tree, err := s.tree(ctx, hash)
		if err != nil {
			return nil, err
		}
		names := slices.Sorted(maps.Values(tree))

		specs := make([]string, len(names))
		for i, name := range names {
			specs[i] = hash + ":" + name
		}

		bufs, err := s.catBatch(ctx, specs)
		if err != nil {
			return nil, err
		}
		es := make([]entry, 0, len(names))
		for i, buf := range bufs {
			req, resp, body, err := parseEntry(buf)
			if err != nil {
				continue
			}
			e := entry{Name: names[i], URL: req.URL.String()}
			if resp.StatusCode == http.StatusOK && strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/html") {
				e.HTML, e.Title = true, pageTitle(body)
			}
			es = append(es, e)
		}
		slices.SortFunc(es, func(a, b entry) int { return cmp.Compare(a.URL, b.URL) })
		return es, nil
	})
}

// histSpec is the pathspec matching an entry across the whole history. Entries
// are matched by url hash, since the category prefix has changed over time. The
// filename is only used for old entries cached under a url which differs from
// the one in their stored request.
func histSpec(name, key string) string {
	if name == "" || strings.HasSuffix(name, "-"+key) {
		return ":(glob)*-" + key
	}
	return name
}

// history gets the versions at which a cache entry changed, ascending, with the
// size of each change (used for the seekbar bar heights).
func (s *server) history(ctx context.Context, spec string) ([]change, error) {
	return s.hist.get(spec, func(spec string) ([]change, error) {
		buf, err := s.git(ctx, "log", "--first-parent", "--no-renames", "--format=%x00%H", "--numstat",
			"--end-of-options", s.versions[len(s.versions)-1].Hash, "--", spec)
		if err != nil {
			return nil, err
		}
		var cs []change
		for line := range strings.Lines(string(buf)) {
			line = strings.TrimRight(line, "\n")
			if hash, ok := strings.CutPrefix(line, "\x00"); ok {
				if i, ok := s.index[hash]; ok {
					cs = append(cs, change{Version: i})
				}
				continue
			}
			// `<added>\t<deleted>\t<path>`, or `-` for binary files
			if len(cs) == 0 || line == "" {
				continue
			}
			add, rest, ok := strings.Cut(line, "\t")
			if !ok {
				continue
			}
			del, name, _ := strings.Cut(rest, "\t")
			c := &cs[len(cs)-1]
			if n, err := strconv.Atoi(add); err == nil {
				c.Added += n
			}
			if n, err := strconv.Atoi(del); err == nil {
				c.Deleted += n
			}
			c.name = strings.TrimSpace(name)
		}
		slices.SortFunc(cs, func(a, b change) int { return cmp.Compare(a.Version, b.Version) })

		if err := s.classify(ctx, cs); err != nil {
			slog.Warn("classify changes", "spec", spec, "error", err)
		}
		return cs, nil
	})
}

// classify fills in the tier of each change by comparing the text of the
// response with the previous one.
func (s *server) classify(ctx context.Context, cs []change) error {
	if len(cs) == 0 {
		return nil
	}

	specs := make([]string, len(cs))
	for i, c := range cs {
		specs[i] = s.versions[c.Version].Hash + ":" + c.name
	}
	bufs, err := s.catBatch(ctx, specs)
	if err != nil {
		return err
	}

	// parsing a few hundred pages, so spread it over the cpus
	ds := make([]digest, len(bufs))
	for i, buf := range bufs {
		if buf == nil {
			cs[i].Tier = tierGone // the cache was cleared at this version
		}
	}
	var (
		wg  sync.WaitGroup
		sem = make(chan struct{}, runtime.GOMAXPROCS(0))
	)
	for i, buf := range bufs {
		if buf == nil {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			ds[i] = digestEntry(buf)
		}()
	}
	wg.Wait()

	// compare against the last version the entry was actually in, so a scrape
	// after a cache clear is measured against the content before it
	var prev *digest
	for i := range cs {
		if cs[i].Tier == tierGone {
			continue
		}
		if prev == nil {
			cs[i].Tier = tierSchedule // first scrape: all of it is new
		} else {
			cs[i].Tier = ds[i].tier(*prev)
		}
		prev = &ds[i]
	}
	return nil
}

// digest fingerprints the parts of a cached response which changes are
// classified by.
type digest struct {
	full  string // all text on the page
	main  string // text in the main content block
	sched string // text in the main content tables (drop-in schedules)
}

func digestEntry(buf []byte) digest {
	_, resp, body, err := parseEntry(buf)
	if err != nil {
		return digest{full: hashBytes(buf)}
	}
	if !strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "html") {
		return digest{full: hashBytes(body)}
	}

	doc, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return digest{full: hashBytes(body)}
	}

	d := digest{full: hashText(doc)}
	if main := findID(doc, mainID); main != nil {
		d.main = hashText(main)
		d.sched = hashText(slices.Collect(elements(main, atom.Table))...)
	}
	return d
}

// tier is how significant the change from prev to d is.
func (d digest) tier(prev digest) int {
	switch {
	case d.sched != prev.sched:
		return tierSchedule
	case d.main != prev.main:
		return tierContent
	case d.full != prev.full:
		return tierText
	}
	return tierNone
}

func hashText(ns ...*html.Node) string {
	if len(ns) == 0 {
		return ""
	}
	var b strings.Builder
	for _, n := range ns {
		for t := range texts(n) {
			b.WriteString(t)
			b.WriteByte(' ')
		}
	}
	return hashBytes([]byte(strings.Join(strings.Fields(b.String()), " ")))
}

func hashBytes(buf []byte) string {
	h := sha1.Sum(buf)
	return string(h[:])
}

// html helpers.

// elements iterates over the descendants of n with the given tag.
func elements(n *html.Node, a atom.Atom) iter.Seq[*html.Node] {
	return func(yield func(*html.Node) bool) {
		for d := range n.Descendants() {
			if d.DataAtom == a && !yield(d) {
				return
			}
		}
	}
}

// findID gets the first element with the given id.
func findID(n *html.Node, id string) *html.Node {
	for d := range n.Descendants() {
		if d.Type == html.ElementNode && attr(d, "id") == id {
			return d
		}
	}
	return nil
}

// attr gets an attribute value, or an empty string.
func attr(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if strings.EqualFold(a.Key, key) {
			return a.Val
		}
	}
	return ""
}

// texts iterates over the visible text under n.
func texts(n *html.Node) iter.Seq[string] {
	return func(yield func(string) bool) {
		var rec func(*html.Node) bool
		rec = func(n *html.Node) bool {
			for c := range n.ChildNodes() {
				switch c.Type {
				case html.TextNode:
					if !yield(c.Data) {
						return false
					}
				case html.ElementNode:
					if !noText[c.DataAtom] && !rec(c) {
						return false
					}
				}
			}
			return true
		}
		rec(n)
	}
}

// noText is elements whose text isn't page content (script and style text is
// also full of per-response tokens).
var noText = map[atom.Atom]bool{
	atom.Script:   true,
	atom.Style:    true,
	atom.Noscript: true,
	atom.Template: true,
	atom.Svg:      true,
}

// resolve finds the cache entry for a url at a version, returning an empty name
// if it isn't cached there. Entries are normally named after the sha1 of the
// url, but some old ones were cached under a different url than the one in the
// stored request, so it falls back to indexing the requests themselves.
func (s *server) resolve(ctx context.Context, cur int, key string, u *url.URL) (string, error) {
	hash := s.versions[cur].Hash

	tree, err := s.tree(ctx, hash)
	if err != nil {
		return "", err
	}
	if name, ok := tree[key]; ok {
		return name, nil
	}

	byURL, err := s.urlIndex(ctx, hash)
	if err != nil {
		return "", err
	}
	return byURL[u.String()], nil
}

// urlIndex indexes the cache entries of a commit by their request url.
func (s *server) urlIndex(ctx context.Context, hash string) (map[string]string, error) {
	return s.urls.get(hash, func(hash string) (map[string]string, error) {
		es, err := s.list(ctx, hash)
		if err != nil {
			return nil, err
		}
		m := make(map[string]string, len(es))
		for _, e := range es {
			m[e.URL] = e.Name
		}
		return m, nil
	})
}

func (s *server) cat(ctx context.Context, hash, name string) ([]byte, error) {
	buf, err := s.git(ctx, "cat-file", "blob", "--end-of-options", hash+":"+name)
	if err != nil {
		if msg := err.Error(); strings.Contains(msg, " does not exist in ") || strings.Contains(msg, " exists on disk, but not in ") {
			err = fmt.Errorf("%w: %v", fs.ErrNotExist, err)
		}
		return nil, err
	}
	return buf, nil
}

// catBatch is like cat, but reads multiple `<commit>:<path>` specs with a single
// process, returning nil for missing ones.
func (s *server) catBatch(ctx context.Context, specs []string) ([][]byte, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}

	var stdin bytes.Buffer
	for _, spec := range specs {
		stdin.WriteString(spec)
		stdin.WriteString("\n")
	}

	cmd := exec.CommandContext(ctx, "git", "cat-file", "--batch")
	cmd.Dir = s.repo
	cmd.Stdin = &stdin

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// `<oid> <type> <size>\n<contents>\n` or `<input> missing\n`
	bufs := make([][]byte, 0, len(specs))
	r := bufio.NewReader(stdout)
	for range specs {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		_, rest, ok := strings.Cut(strings.TrimSuffix(line, "\n"), " ") // oid
		if !ok {
			bufs = append(bufs, nil)
			continue
		}
		_, sizeStr, ok := strings.Cut(rest, " ") // type
		if !ok {
			bufs = append(bufs, nil) // "<input> missing"
			continue
		}
		size, err := strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			cmd.Process.Kill()
			cmd.Wait()
			return nil, fmt.Errorf("parse cat-file header %q: %w", line, err)
		}
		buf := make([]byte, size+1) // includes the trailing newline
		if _, err := io.ReadFull(r, buf); err != nil {
			cmd.Process.Kill()
			cmd.Wait()
			return nil, fmt.Errorf("read cat-file contents: %w", err)
		}
		bufs = append(bufs, buf[:size])
	}
	if err := cmd.Wait(); err != nil {
		return nil, transformError(err, stderr.Bytes())
	}
	if len(bufs) != len(specs) {
		return nil, fmt.Errorf("read %d of %d objects", len(bufs), len(specs))
	}
	return bufs, nil
}

func transformError(err error, stderr []byte) error {
	var xx *exec.ExitError
	if errors.As(err, &xx) {
		if stderr == nil {
			stderr = xx.Stderr
		}
		for msg := range bytes.Lines(bytes.TrimSpace(stderr)) {
			return fmt.Errorf("git (%s): %s", xx.ProcessState, msg)
		}
	}
	return err
}

func isLikelyGitHash(hash string) bool {
	return len(hash) >= 40 && strings.Trim(hash, "0123456789abcdef") == ""
}

// memo caches expensive lookups for the lifetime of the process (the history is
// immutable, so nothing needs invalidating).
type memo[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]V
}

func (m *memo[K, V]) clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.m = nil
}

func (m *memo[K, V]) get(k K, fn func(K) (V, error)) (V, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.m[k]; ok {
		return v, nil
	}
	v, err := fn(k)
	if err != nil {
		return v, err
	}
	if m.m == nil {
		m.m = map[K]V{}
	}
	m.m[k] = v
	return v, nil
}
