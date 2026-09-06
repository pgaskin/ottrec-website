package main

import (
	"bufio"
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	htmlpkg "html"
	"io"
	"io/fs"
	"iter"
	"log/slog"
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
	"sync/atomic"
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
	NoPrecache   = pflag.Bool("no-precache", false, "don't precompute each page's history in the background while nothing else is being served")
	CacheDir     = pflag.StringP("cache-dir", "c", "", "directory to keep what was precomputed in, so a restart doesn't work it out again (empty to keep it in memory only)")
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
		ctx, cancel := context.WithTimeout(ctx, updateTimeout)
		err := fetch(ctx)
		cancel()
		if err != nil {
			return fmt.Errorf("fetch repo: %w", err)
		}
	}

	// the settings which affect what is rendered are part of every etag
	httpx.AddExeExtra(fmt.Sprint(*Repo, "\x00", *RepoRev, "\x00", *RepoBranch, "\x00", *Host, "\x00", *Reader))

	srv, err := newServer(ctx, *Repo, cmp.Or(*RepoRev, *RepoBranch))
	if err != nil {
		return err
	}
	if *CacheDir != "" {
		srv.useCache(ctx, *CacheDir)
	}
	srv.log()
	srv.precacheStart()

	slog.Info("updater: starting repo fetcher", "interval", *RepoInterval)
	go func() {
		ticker := time.Tick(*RepoInterval)
		for {
			if ticker == nil {
				slog.Warn("updater: repo polling disabled")
				return
			}
			<-ticker
			update(srv)
		}
	}()

	slog.Info("http: listening", "addr", *Addr)
	return http.ListenAndServe(*Addr, srv)
}

// update fetches and rescans, once.
func update(srv *server) {
	if *RepoRemote != "" {
		ctx, cancel := context.WithTimeout(context.Background(), updateTimeout)
		err := fetch(ctx)
		cancel()
		if err != nil {
			slog.Error("updater: fetch failed", "error", err)
			return
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), updateTimeout)
	defer cancel()

	switch changed, err := srv.reload(ctx); {
	case err != nil:
		slog.Error("updater: reload failed", "error", err)
	case changed:
		srv.log()
		srv.precacheStart()
	}
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

	// updateTimeout bounds one fetch and rescan. Without it a hung fetch stalls
	// every later one, since they run one after another.
	updateTimeout = time.Minute * 10
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
func (s *view) etag(parts ...string) httpx.ETag {
	// the whole history is part of every page (the seekbar, the arrows, the
	// json island), so the tip is mixed in, not just the version being viewed
	return httpx.NewETag().MixExe().
		Mix(s.tip, strconv.Itoa(len(s.versions))).
		Mix(parts...).ETag().
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

	seen    atomic.Int64 // when the last request finished, for the precache
	preMu   sync.Mutex
	preStop context.CancelFunc
	preDone chan struct{} // closed when the sweep preStop cancels has returned

	mu       sync.RWMutex   // guards the scanned history, which reload
	tip      string         // replaces wholesale: the branch tip, the versions
	versions []version      // in display order, and commit hash -> version
	index    map[string]int // index

	trees   memo[string, commitTree]        // rev -> what is in it
	entries memo[string, []entry]           // rev -> cache entries, sorted by url
	urls    memo[string, map[string]string] // rev -> url -> filename
	hist    memo[string, []change]          // page -> versions at which it changed
	scans   memo[string, struct{}]          // tip -> the changelog covers it

	changes changelog   // what changed in each commit, by entry
	digests digestCache // blob -> what it says, for classifying changes
	metas   metaCache   // blob -> what it says it is, for listing entries
	cache   *cacheFile  // where both are kept between runs, nil if nowhere
}

// view is one request's read of the history. The scanned history is immutable
// once published, so a request takes a reference to it instead of holding a
// lock: a slow client can't block reload, and reload can't change the history
// out from under a half-rendered response.
type view struct {
	*server
	tip      string
	versions []version
	index    map[string]int
}

// snapshot takes a view of the history as it is now.
func (s *server) snapshot() *view {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &view{server: s, tip: s.tip, versions: s.versions, index: s.index}
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
	oid  string // the blob it became, empty if it was removed
}

// pageRef identifies a cache entry across the whole history. Entries are
// matched by the sha1 of their url, since the category prefix in the filename
// has changed over time; the filename is only used for old entries cached
// under a url which differs from the one in their stored request.
type pageRef struct {
	Name string // the entry's filename at the version being viewed, if known
	Key  string // sha1(url)
}

// named reports whether the entry has to be followed by filename rather than
// by url.
func (r pageRef) named() bool {
	return r.Name != "" && !strings.HasSuffix(r.Name, "-"+r.Key)
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

// memoCommits is how many commits the per-commit memos keep. Walking a page's
// seekbar visits every version, and each one's tree and entry list is ~150KB.
const memoCommits = 64

func newServer(ctx context.Context, repo, rev string) (*server, error) {
	s := &server{
		repo:   repo,
		rev:    rev,
		limit:  rate.NewLimiter(rate.Limit(*GitRate), *GitBurst),
		static: static.Handler(static.Webarchive),
	}
	s.trees.max, s.entries.max, s.urls.max = memoCommits, memoCommits, memoCommits
	s.scans.max = 4
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
	tip := versions[len(versions)-1].Hash // in rev-list order, before sorting

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
	if tip == s.tip && len(versions) == len(s.versions) {
		return false, nil // nothing new
	}
	s.tip, s.versions, s.index = tip, versions, index
	s.hist.clear() // version indexes are baked into these; the rest is by commit
	return true, nil
}

// precacheStart restarts the background precache, which is only worth anything
// after the history it precomputed against has changed.
func (s *server) precacheStart() {
	if *NoPrecache {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	s.preMu.Lock()
	stop, stopped := s.preStop, s.preDone
	s.preStop, s.preDone = cancel, done
	s.preMu.Unlock()

	go func() {
		defer cancel()
		defer close(done)
		if stop != nil {
			// the sweep this one replaces precomputed against an older history,
			// and its git processes are quota this one needs
			stop()
			<-stopped
		}
		s.precache(ctx)
	}()
}

// precache computes what the first visit to each page would otherwise pay for,
// one page at a time, and only while nothing else is being served.
func (s *server) precache(ctx context.Context) {
	const idle = time.Second * 2

	start := time.Now()
	v := s.snapshot()
	hash := v.versions[len(v.versions)-1].Hash

	// the whole-history scan comes first: every page's seekbar is read out of it
	if err := s.precacheIdle(ctx, idle); err != nil {
		return
	}
	if err := v.scanned(ctx); err != nil {
		slog.Debug("precache: scan history", "error", err)
		return
	}
	es, err := s.list(ctx, hash)
	if err != nil {
		slog.Debug("precache: list entries", "error", err)
		return
	}

	var n int
	for _, e := range es {
		if !e.HTML {
			continue
		}
		if err := s.precacheIdle(ctx, idle); err != nil {
			return
		}
		sum := sha1.Sum([]byte(e.URL))

		_, err := v.history(ctx, pageRef{Name: e.Name, Key: hex.EncodeToString(sum[:])})
		if err != nil {
			slog.Debug("precache: read history", "entry", e.Name, "error", err)
			continue
		}
		n++
	}
	slog.Info("precache: precomputed page histories", "pages", n, "took", time.Since(start))
}

// precacheIdle waits until nothing has been served for d.
func (s *server) precacheIdle(ctx context.Context, d time.Duration) error {
	for {
		if wait := d - time.Since(time.Unix(0, s.seen.Load())); wait > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
				continue
			}
		}
		return ctx.Err()
	}
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
	// stamped at both ends: the precache waits for a lull in what is being
	// served, and a long request is a lull only once it's done
	s.seen.Store(time.Now().UnixNano())
	defer func() { s.seen.Store(time.Now().UnixNano()) }()

	s.snapshot().serve(w, r)
}

func (s *view) serve(w http.ResponseWriter, r *http.Request) {
	// neither form of proxying works: a page renders with links, fonts and
	// fetches pointed at the proxy's own origin, which for a proxy client is
	// the archived site
	if r.Method == http.MethodConnect || r.URL.IsAbs() {
		http.Error(w, "ottrec-webarchive: proxying is not supported; use it as an origin server instead", http.StatusNotImplemented)
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
	hu = strings.ToLower(hu) // canonical: everything downstream compares it
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
func (s *view) canonical(w http.ResponseWriter, r *http.Request, cur int, dated bool, u *url.URL, fn func()) {
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
func (s *view) parseStamp(seg string) (int, bool) {
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
func (s *view) pageURL(i int, dated bool, u *url.URL) string {
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
func (s *view) linkPrefix(cur int, dated bool) string {
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

func (s *view) serveCached(w http.ResponseWriter, r *http.Request, cur int, dated bool, u *url.URL) {
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
		s.serveError(w, r, cur, dated, u, pageRef{Key: key}, status(err), err.Error())
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
		s.serveError(w, r, cur, dated, u, pageRef{Name: latest, Key: key}, http.StatusNotFound,
			"not cached at this version: "+u.String(), s.variantList(cur, dated, vs))
		return
	}
	ref := pageRef{Name: name, Key: key}

	buf, err := s.cat(ctx, s.versions[cur].Hash, name)
	if err != nil {
		s.serveError(w, r, cur, dated, u, ref, status(err), err.Error())
		return
	}

	_, resp, body, err := parseEntry(buf)
	if err != nil {
		s.serveError(w, r, cur, dated, u, ref, status(err), err.Error())
		return
	}

	h := w.Header()
	for _, k := range []string{"Content-Type", "Content-Language", "Last-Modified"} {
		if v := resp.Header.Get(k); v != "" {
			h.Set(k, v)
		}
	}
	// the stored Date is a year in the past: it describes the scrape, not this
	// response, and net/http sets a real one
	if v := resp.Header.Get("Date"); v != "" {
		h.Set("X-Archived-Date", v)
	}
	h.Set("X-Ottrec-Webarchive-Entry", name)
	h.Set("X-Ottrec-Webarchive-Version", s.versions[cur].Hash)

	// a cached redirect replays as one, pointed back at the proxy at this
	// version; if the target can't be (offsite, or there isn't one), the stored
	// body is shown instead, with the archived status in a header
	code := cmp.Or(resp.StatusCode, http.StatusOK)
	if code >= 300 && code < 400 {
		if loc, ok := rewriteURL(resp.Header.Get("Location"), u.Host, s.linkPrefix(cur, dated)); ok && loc != "" {
			h.Set("Location", loc)
		} else {
			h.Set("X-Ottrec-Webarchive-Status", strconv.Itoa(code))
			code = http.StatusOK
		}
	}

	if strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/html") {
		if clean, err := sanitize(body, u.Host, s.linkPrefix(cur, dated)); err != nil {
			slog.Warn("sanitize", "url", u, "error", err)
		} else {
			body = clean
		}
		body = s.inject(ctx, h, body, r, cur, dated, u, ref, entryHead(buf))
	} else {
		// nothing to enhance, so lock it down entirely
		h.Set("Content-Security-Policy", "default-src 'none'; sandbox")
		h.Set("X-Robots-Tag", "noindex, noarchive, nofollow, noai, noimageai")
	}
	h.Set("Content-Length", strconv.Itoa(len(body)))

	w.WriteHeader(code)
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
func (s *view) variantList(cur int, dated bool, vs []*url.URL) string {
	if len(vs) == 0 {
		return ""
	}
	const limit = 30

	var b strings.Builder
	fmt.Fprintf(&b, `<p style="font:14px/1.5 Roboto,system-ui,sans-serif;color:#6F6E69;margin:2rem 2rem 0">`+
		`cached with other query params (%d):</p><ul style="font:14px/1.6 Roboto,system-ui,sans-serif;margin:.4rem 2rem">`, len(vs))
	for _, v := range vs[:min(len(vs), limit)] {
		fmt.Fprintf(&b, `<li><a href="%s" rel="nofollow">?%s</a></li>`,
			htmlpkg.EscapeString(s.pageURL(cur, dated, v)), htmlpkg.EscapeString(v.RawQuery))
	}
	if len(vs) > limit {
		fmt.Fprintf(&b, `<li style="color:#6F6E69">and %d more</li>`, len(vs)-limit)
	}
	b.WriteString(`</ul>`)
	return b.String()
}

// serveRaw hands over the stored response as a download: the body as it was
// scraped (gunzipped), without the overlay, but with the proxy's own headers.
func (s *view) serveRaw(w http.ResponseWriter, r *http.Request, cur int, u *url.URL) {
	ctx := r.Context()
	hash := sha1.Sum([]byte(u.String()))

	if notModified(w, r, s.etag("raw", s.versions[cur].Hash, u.String())) {
		return
	}

	name, err := s.resolve(ctx, cur, hex.EncodeToString(hash[:]), u)
	if err != nil {
		httpError(w, err.Error(), status(err))
		return
	}
	if name == "" {
		httpError(w, "not cached at this version: "+u.String(), http.StatusNotFound)
		return
	}

	buf, err := s.cat(ctx, s.versions[cur].Hash, name)
	if err != nil {
		httpError(w, err.Error(), status(err))
		return
	}
	_, resp, body, err := parseEntry(buf)
	if err != nil {
		httpError(w, err.Error(), status(err))
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

func (s *view) serveError(w http.ResponseWriter, r *http.Request, cur int, dated bool, u *url.URL, ref pageRef, status int, msg string, extra ...string) {
	if status == http.StatusServiceUnavailable {
		// rendering the full page needs git too, which is the thing we're out of
		httpError(w, msg, status)
		return
	}

	body := []byte(`<!DOCTYPE html><html><head><title>ottrec-webarchive</title></head><body>` +
		`<p style="font:14px/1.5 Roboto,system-ui,sans-serif;color:#6F6E69;margin:2rem">` +
		htmlpkg.EscapeString(msg) + `</p>` +
		strings.Join(extra, "") +
		`<p style="font:14px/1.5 Roboto,system-ui,sans-serif;margin:2rem"><a href="/__cache" rel="nofollow">cache index</a></p>` +
		`</body></html>`)
	h := w.Header()
	body = s.inject(r.Context(), h, body, r, cur, dated, u, ref, "")
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		w.Write(body)
	}
}

func (s *view) serveIndex(w http.ResponseWriter, r *http.Request, cur int, dated bool) {
	ctx := r.Context()

	if notModified(w, r, s.etag("index", s.versions[cur].Hash, strconv.FormatBool(dated))) {
		return
	}

	entries, err := s.list(ctx, s.versions[cur].Hash)
	if err != nil {
		s.serveError(w, r, cur, dated, nil, pageRef{}, status(err), err.Error())
		return
	}

	var b strings.Builder
	b.WriteString(`<!DOCTYPE html><html><head><title>cache index</title></head>` +
		`<body style="font:14px/1.6 Roboto,system-ui,sans-serif;margin:min(2rem,5vw);` +
		`max-width:60rem;overflow-wrap:break-word">`)
	fmt.Fprintf(&b, `<p style="color:#6F6E69">%d entries, %s</p><ul style="padding-left:1.2rem">`,
		len(entries), htmlpkg.EscapeString(s.versions[cur].Time.In(ottrecidx.TZ).Format("Mon 2006-01-02")))
	for _, e := range entries {
		fmt.Fprintf(&b, `<li><a href="%s" rel="nofollow">%s</a> <span style="color:#6F6E69">%s</span></li>`,
			htmlpkg.EscapeString(s.entryHref(e, cur, dated)), htmlpkg.EscapeString(entryPath(e)), htmlpkg.EscapeString(entryLabel(e)))
	}
	b.WriteString(`</ul></body></html>`)

	h := w.Header()
	body := s.inject(ctx, h, []byte(b.String()), r, cur, dated, nil, pageRef{}, "")
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
// it. Escaping is the renderer's job: it escapes comment data on the way out.
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
	return b.String()
}

// retitle wraps the page title, falling back to middle if the page has none.
func retitle(body []byte, prefix, middle, suffix string) []byte {
	if i := indexFold(body, []byte("<title")); i >= 0 {
		if j := bytes.IndexByte(body[i:], '>'); j >= 0 {
			if k := indexFold(body[i+j+1:], []byte("</title>")); k >= 0 {
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
	if i := indexFold(body, []byte(tag)); i >= 0 {
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
	doc, err := html.Parse(bytes.NewReader(body)) // utf-8 is assumed, as ottawa.ca is
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
		case key == "src" && n.DataAtom == atom.Img:
			// nothing but data: images can load, so an image url would only be
			// a request the csp blocks; the alt text is what's left
			n.Attr[i] = origAttr(a)
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
func (s *view) entryHref(e entry, cur int, dated bool) string {
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

// secretHead are headers whose value isn't shown. The scraper doesn't send
// anything secret at a public site, but this is on every archived page.
var secretHead = map[string]bool{
	"authorization":       true,
	"cookie":              true,
	"proxy-authorization": true,
	"set-cookie":          true,
	"x-scraper-secret":    true,
}

// entryHead is the stored request and response headers, with the credentials
// among them redacted.
func entryHead(buf []byte) string {
	var head []byte
	for range 2 {
		i := bytes.Index(buf, []byte("\r\n\r\n"))
		if i < 0 {
			break
		}
		for line := range bytes.Lines(bytes.TrimRight(buf[:i], "\r\n")) {
			line = bytes.TrimRight(line, "\r\n")
			if name, _, ok := bytes.Cut(line, []byte(":")); ok && secretHead[strings.ToLower(string(name))] {
				line = append(slices.Clone(name), ": (redacted)"...)
			}
			head = append(head, line...)
			head = append(head, '\n')
		}
		head = append(head, '\n')
		buf = buf[i+4:]
	}
	return strings.TrimSpace(string(head))
}

// indexFold is like [bytes.Index], but matches ASCII letters in either case.
// sub must be lowercase.
func indexFold(b, sub []byte) int {
	if len(sub) == 0 {
		return 0
	}
	lo := sub[0]
	up := lo
	if lo >= 'a' && lo <= 'z' {
		up = lo - ('a' - 'A')
	}
	for i := 0; i+len(sub) <= len(b); i++ {
		rest := b[i : len(b)-len(sub)+1]
		j := bytes.IndexByte(rest, lo)
		if up != lo {
			if k := bytes.IndexByte(rest, up); k >= 0 && (j < 0 || k < j) {
				j = k
			}
		}
		if j < 0 {
			return -1
		}
		if i += j; bytes.EqualFold(b[i:i+len(sub)], sub) {
			return i
		}
	}
	return -1
}

// lastIndexFold is like [bytes.LastIndex], but matches ASCII letters in either
// case. sub must be lowercase.
func lastIndexFold(b, sub []byte) int {
	if len(sub) == 0 {
		return len(b)
	}
	lo := sub[0]
	up := lo
	if lo >= 'a' && lo <= 'z' {
		up = lo - ('a' - 'A')
	}
	for i := len(b) - len(sub); i >= 0; {
		j := bytes.LastIndexByte(b[:i+1], lo)
		if up != lo {
			if k := bytes.LastIndexByte(b[:i+1], up); k > j {
				j = k
			}
		}
		if j < 0 {
			return -1
		}
		if bytes.EqualFold(b[j:j+len(sub)], sub) {
			return j
		}
		i = j - 1
	}
	return -1
}

// pageTitle extracts the title of an html page, without parsing it or copying
// it (this runs over every blob in the repo). The site suffixes every title
// with the site name, which is only noise here.
func pageTitle(body []byte) string {
	i := indexFold(body, []byte("<title"))
	if i < 0 {
		return ""
	}
	j := bytes.IndexByte(body[i:], '>')
	if j < 0 {
		return ""
	}
	i += j + 1
	k := indexFold(body[i:], []byte("</title>"))
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
	host = strings.ToLower(host)
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
		// the escaping is kept: re-escaping the decoded path would turn
		// "/a%2Fb" into "/a/b", which is a different resource
		path, escaped := u.Path, u.EscapedPath()
		if path == "" {
			path, escaped = "/", "/"
		}
		u.Path, u.RawPath = prefix+"/"+host+path, prefix+"/"+host+escaped
		u.Host = ""
		return u.String(), true
	}
	return "", false
}

// inject adds the version selector overlay and the CSP which keeps the page
// from reaching anything real, and sets the response headers it needs.
func (s *view) inject(ctx context.Context, h http.Header, body []byte, r *http.Request, cur int, dated bool, u *url.URL, ref pageRef, head string) []byte {
	var changes []change
	if ref.Key != "" {
		var err error
		if changes, err = s.history(ctx, ref); err != nil {
			slog.Warn("read history", "entry", ref.Name, "url", u, "error", err)
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
	if i := lastIndexFold(body, []byte("</body>")); i >= 0 {
		return slices.Concat(body[:i], []byte(overlay), body[i:])
	}
	return slices.Concat(body, []byte(overlay))
}

// overlay renders the version seekbar: one bar per version, scaled by how much
// the page changed at it, scrolling horizontally when there are more versions
// than fit. Every bar is a plain link, so it works without javascript;
// javascript only relabels the status line while hovering and scrolls the
// current version into view.
func (s *view) overlay(nonce string, cur int, changes []change, dated bool, u *url.URL, pages []entry) string {
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
		c, ok := byVersion[i]

		var h int
		switch {
		case len(changes) == 0:
			h = 35 // no per-page history (the cache index), so just show the versions
		case !ok, c.Tier == tierGone:
			h = 0 // not cached at this version: a stub, still clickable
		default:
			h = barHeight(c.Added+c.Deleted, maxLines)
		}
		class := "__cpb"
		if ok && c.Tier > tierGone {
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
	date, change := statusLine(s.versions, changes, cur)
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
func barHeight(lines, peak int) int {
	if lines <= 0 || peak <= 0 {
		return 8
	}
	return 8 + int(92*math.Log1p(float64(lines))/math.Log1p(float64(peak)))
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
func statusLine(versions []version, changes []change, i int) (string, string) {
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
/* the scroll padding keeps a fragment jump from landing under the fixed bar */
html{--cp-h:6rem;--cp-w:15rem;color-scheme:light dark;scroll-padding-top:calc(var(--cp-h) + .5rem)}
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
/* a swipe far enough to seek takes the content with it, so the finger has said
   something before it is lifted. the bar itself stays where it is */
:root[data-cp-swipe] body>*:not(#__cp){transform:translateX(var(--cp-sx,0));opacity:var(--cp-so,1)}
:root[data-cp-swipe="settle"] body>*:not(#__cp){transition:transform .2s ease-out,opacity .2s ease-out}
:root[data-cp-swipe="out"] body>*:not(#__cp){transition:transform .25s ease-in,opacity .25s ease-in}
/* clip rather than hidden: the nudge mustn't turn the page into something which
   scrolls sideways by the width of it */
:root[data-cp-swipe]{overflow-x:clip}
@media (prefers-reduced-motion:reduce){
 :root[data-cp-swipe] body>*:not(#__cp){transform:none}
}
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
 // the overlay is the last thing in the body, and an archived page is free to
 // define the same ids, so it is found by taking the last match and everything
 // else is looked up within it
 let root=null
 const q=s=>root?root.querySelector(s):null
 let data=null

 const fit=()=>{
  if(root)document.documentElement.style.setProperty('--cp-h',root.offsetHeight+'px')
 }
 const fade=()=>{
  const w=q('#__cpw')
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
  const el=q('#__cpx')
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
  const el=q('#__cpth')
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

 // edge auto-scroll state, outside bind so a body swap can stop a frame which
 // is still running: the element it scrolls is gone, and its pointerleave
 // never fires
 let vel=0,raf=0

 // bind wires up the overlay; it runs again after each client-side navigation,
 // since the whole body (overlay included) is swapped out
 const bind=at=>{
  if(raf)cancelAnimationFrame(raf)
  vel=raf=0
  root=[...document.querySelectorAll('#__cp')].pop()||null
  const island=q('#__cpd')
  if(!root||!island)return
  data=JSON.parse(island.textContent)
  const date=q('#__cpsd'), change=q('#__cpsc'), strip=q('#__cpt'), wrap=q('#__cpw')
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
  const theme=q('#__cpth')
  if(theme){
   theme.addEventListener('click',()=>{
    const next=({auto:'light',light:'dark',dark:'auto'})[savedTheme()]
    try{localStorage.setItem('theme',next)}catch(e){}
    applyTheme(next)
   })
   theme.hidden=false
   applyTheme(savedTheme())
  }

  const filter=q('#__cpx')
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

 // the page which is currently rendered: popstate fires after location has
 // already changed, so the outgoing one can't be read back off the url
 const loc=u=>u.pathname+u.search
 let here=loc(location)
 // a version switch stays on the same page: only the timestamp segment changes
 const page=p=>p.replace(/^\/\d{4,14}(\.\d+)?(?=\/)/,'')

 // the scroll position rides along in the history entry: a client-side swap
 // lands long after the popstate, so the browser can't restore it itself (and
 // is told not to try). it is written on a trailing timer rather than on every
 // scroll, since safari rate-limits replacestate
 let syt=0
 const savePos=()=>{
  clearTimeout(syt)
  syt=0
  try{history.replaceState({y:Math.round(scrollY)},'')}catch(e){}
 }
 const scrollHash=hash=>{
  if(!hash)return null
  let el=null
  try{el=document.getElementById(decodeURIComponent(hash.slice(1)))}catch(e){}
  if(el)el.scrollIntoView()
  return el
 }
 addEventListener('scroll',()=>{
  if(!syt)syt=setTimeout(savePos,400)
 },{passive:true})

 let ctl=null
 const fail=(url,err)=>{
  console.error('ottrec-webarchive: load '+url+':',err)
  const date=q('#__cpsd'), change=q('#__cpsc')
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

 const go=async(href,pop,y)=>{
  const url=new URL(href,location.href)
  ctl?.abort() // a newer click supersedes whatever is still in flight
  const c=ctl=new AbortController()
  const wrap=q('#__cpw'), side=q('#__cpsb')
  // the reading position and the seekbar scroll are only worth keeping across
  // a version switch; a different page re-centers on the version being viewed
  const same=page(loc(url))===page(here)
  const at=same&&wrap?wrap.scrollLeft:null, top=side?side.scrollTop:0
  const keep=same&&!pop?anchor():null
  document.documentElement.dataset.cpBusy='1'
  try{
   const res=await fetch(url,{credentials:'same-origin',signal:c.signal})
   if(!(res.headers.get('content-type')||'').includes('text/html')){
    c.abort() // not a page: let the browser deal with it, and only once
    location.href=url.href
    return
   }
   const doc=new DOMParser().parseFromString(await res.text(),'text/html')
   if(!pop){
    savePos() // the outgoing entry, before the body it belongs to goes away
    history.pushState(null,'',res.url||url)
   }
   document.title=doc.title
   const was=document.querySelector('link[rel="canonical"]'), now=doc.querySelector('link[rel="canonical"]')
   if(was)was.remove()
   if(now)document.head.appendChild(document.importNode(now,true))
   document.body.replaceWith(doc.body)
   try{
    bind(at)
   }catch(err){
    console.error('ottrec-webarchive: bind:',err) // the page is still usable unbound
   }
   const sb=q('#__cpsb')
   if(sb)sb.scrollTop=top
   const el=keep&&document.getElementById(keep.id)
   if(el)scrollTo(0,Math.max(0,scrollY+el.getBoundingClientRect().top-keep.top))
   else if(pop)scrollTo(0,y||0)
   else if(!scrollHash(url.hash))scrollTo(0,0)
  }catch(err){
   if(c.signal.aborted)return
   fail(url.href,err)
  }finally{
   here=loc(location)
   if(ctl===c){
    ctl=null
    nudge(null) // not if a newer one superseded this, since it may have nudged
   }
   if(!ctl)delete document.documentElement.dataset.cpBusy
  }
 }

 addEventListener('click',e=>{
  if(e.defaultPrevented||e.button||e.metaKey||e.ctrlKey||e.shiftKey||e.altKey)return
  const a=e.target instanceof Element?e.target.closest('a[href]'):null
  if(!a||a.target||a.hasAttribute('download'))return
  const url=new URL(a.getAttribute('href'),location.href)
  if(url.origin!==location.origin)return
  // an in-page jump is the browser's to make; the entry it leaves behind still
  // needs its position, since the pending write would land on the new one
  if(url.hash&&url.pathname===location.pathname&&url.search===location.search){
   savePos()
   return
  }
  e.preventDefault()
  go(url.href)
 })
 // the drawer's scrim is only a shadow, so closing it needs a hand. the
 // hamburger is a label, and the click it forwards to its checkbox bubbles up
 // here too, from a target outside the drawer: ignoring it is what stops
 // opening the drawer from closing it again
 addEventListener('click',e=>{
  const menu=q('#__cpm')
  if(!menu||!menu.checked||!(e.target instanceof Element))return
  if(e.target===menu||e.target.closest('#__cpsb,#__cpmb,label[for="__cpm"]'))return
  menu.checked=false
 })
 addEventListener('keydown',e=>{
  const menu=q('#__cpm')
  if(e.key==='Escape'&&menu&&menu.checked)menu.checked=false
 })

 // swiping sideways seeks, the same way the arrows do: left for newer, the way
 // the page would move. nothing is prevented, so scrolling, zooming and text
 // selection carry on as they were
 let swipe=null, settle=0

 // nudge moves the content along with a swipe which has gone far enough to
 // seek: past the threshold it follows the finger, at half its speed
 const nudge=(px,opacity)=>{
  const r=document.documentElement
  clearTimeout(settle)
  if(px===null){
   r.removeAttribute('data-cp-swipe')
   r.style.removeProperty('--cp-sx')
   r.style.removeProperty('--cp-so')
   return
  }
  r.setAttribute('data-cp-swipe','')
  r.style.setProperty('--cp-sx',px.toFixed(1)+'px')
  r.style.setProperty('--cp-so',opacity.toFixed(3))
 }
 // let go past the threshold: the rest of the way out, since what the swipe
 // asked for is on its way in. it stays gone until the new page replaces it
 const finish=dir=>{
  const r=document.documentElement
  if(r.getAttribute('data-cp-swipe')!=='')return
  clearTimeout(settle)
  r.setAttribute('data-cp-swipe','out')
  r.style.setProperty('--cp-sx',(dir<0?-30:30)+'vw')
  r.style.setProperty('--cp-so','0')
 }
 // and back where it was, for a swipe which came to nothing
 const unnudge=()=>{
  const r=document.documentElement
  if(r.getAttribute('data-cp-swipe')!=='')return // not nudged, or already going back
  r.setAttribute('data-cp-swipe','settle')
  r.style.setProperty('--cp-sx','0px')
  r.style.setProperty('--cp-so','1')
  settle=setTimeout(()=>nudge(null),200)
 }
 const scrollsX=el=>{
  for(let n=el instanceof Element?el:null;n&&n!==document.body;n=n.parentElement){
   if(n.scrollWidth>n.clientWidth+2&&/auto|scroll/.test(getComputedStyle(n).overflowX))return true
  }
  return false
 }
 addEventListener('touchstart',e=>{
  const menu=q('#__cpm')
  // one finger, not on the seekbar or a wide table, and not over the drawer
  if(e.touches.length!==1||(menu&&menu.checked)||scrollsX(e.target)){
   swipe=null
   return
  }
  swipe={x:e.touches[0].clientX,y:e.touches[0].clientY,at:Date.now()}
 },{passive:true})
 addEventListener('touchmove',e=>{
  if(e.touches.length!==1){ // a second finger is a pinch, not a swipe
   swipe=null
   unnudge()
   return
  }
  if(!swipe)return
  const t=e.touches[0], dx=t.clientX-swipe.x, dy=t.clientY-swipe.y
  // the shift goes as far as the finger does. the fade does most of its work
  // over the first 60px past the threshold, then carries on at a quarter of
  // the rate, so there is always a little of the page left
  const over=Math.abs(dx)<Math.abs(dy)*1.5?0:Math.max(0,Math.abs(dx)-50)
  const f=Math.min(.95,over<=60?over/60*.6:.6+(over-60)/400)
  if(over)nudge(Math.sign(dx)*over*.5,1-f)
  else unnudge()
 },{passive:true})
 addEventListener('touchcancel',()=>{
  swipe=null
  unnudge()
 },{passive:true})
 addEventListener('touchend',e=>{
  const s=swipe, t=e.changedTouches[0]
  swipe=null
  if(!s||!t){
   unnudge()
   return
  }
  const dx=t.clientX-s.x, dy=t.clientY-s.y
  const seek=root?root.querySelectorAll('.__cpn'):[]
  const a=dx<0?seek[1]:seek[0] // older, newer, in the order they're rendered
  if(Date.now()-s.at>700||Math.abs(dx)<50||Math.abs(dx)<Math.abs(dy)*1.5||!a){
   unnudge()
   return
  }
  finish(dx)
  go(a.href) // what it left behind stays gone until the new page is in
 },{passive:true})
 addEventListener('storage',e=>{
  if(e.key==='theme'||e.key===null)applyTheme(savedTheme())
  if(e.key==='unchanged'||e.key===null)applyUnchanged(showsUnchanged())
 })
 addEventListener('popstate',()=>{
  // the pending write belongs to the entry we just left, which can no longer
  // be addressed; and the position to restore has to be read now, before the
  // swap gives anything else a chance to overwrite it
  clearTimeout(syt)
  syt=0
  const y=(history.state&&history.state.y)||0
  // a hash-only step stays on the rendered page: re-fetching it would only
  // throw away the reading position
  if(loc(location)===here){
   if(!scrollHash(location.hash))scrollTo(0,y)
   return
  }
  go(location.href,true,y)
 })
 addEventListener('resize',()=>{fit();fade()})
 try{
  bind(null)
 }catch(err){
  console.error('ottrec-webarchive: bind:',err) // the page is still usable unbound
 }
 try{history.scrollRestoration='manual'}catch(e){}
 // a reload or a bfcache miss comes back through here rather than popstate
 if(!location.hash&&history.state&&history.state.y)scrollTo(0,history.state.y)
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

// httpError writes a plain error response, asking for a retry when the thing
// which failed was git quota.
func httpError(w http.ResponseWriter, msg string, code int) {
	if code == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "5")
	}
	http.Error(w, "ottrec-webarchive: "+msg, code)
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

// treeEntry is one cache entry of a commit.
type treeEntry struct {
	name string
	oid  string // the blob it is, which is what everything about it is keyed by
}

// commitTree is what a commit has in it: its entries in name order, and where
// to find one by the url it was cached under.
type commitTree struct {
	entries []treeEntry
	byKey   map[string]string // sha1(url) -> filename
}

// tree lists the cache entries of a commit.
func (s *server) tree(ctx context.Context, hash string) (commitTree, error) {
	return s.trees.get(ctx, hash, func(hash string) (commitTree, error) {
		buf, err := s.git(ctx, "ls-tree", "-r", "--end-of-options", hash)
		if err != nil {
			return commitTree{}, err
		}
		t := commitTree{byKey: map[string]string{}}
		for line := range strings.Lines(string(buf)) {
			// `<mode> <type> <oid>\t<name>`
			info, name, ok := strings.Cut(strings.TrimRight(line, "\n"), "\t")
			if !ok {
				continue
			}
			f := strings.Fields(info)
			if len(f) != 3 || f[1] != "blob" {
				continue
			}
			t.entries = append(t.entries, treeEntry{name: name, oid: f[2]})
			if key, ok := entryKey(name); ok {
				t.byKey[key] = name
			}
		}
		slices.SortFunc(t.entries, func(a, b treeEntry) int { return cmp.Compare(a.name, b.name) })
		return t, nil
	})
}

// list gets the cached urls of a commit, sorted.
func (s *server) list(ctx context.Context, hash string) ([]entry, error) {
	return s.entries.get(ctx, hash, func(hash string) ([]entry, error) {
		t, err := s.tree(ctx, hash)
		if err != nil {
			return nil, err
		}

		// most of a commit's entries are the same blobs as the commit before
		// it, and everything the precache has been through is already known
		var (
			es   = make([]entry, 0, len(t.entries))
			need []string             // blobs still to read
			at   = map[string][]int{} // blob -> the entries waiting on it
		)
		for i, te := range t.entries {
			if m, ok := s.metas.get(te.oid); ok {
				if m.URL != "" { // an empty one isn't a stored response
					es = append(es, entry{Name: te.name, URL: m.URL, Title: m.Title, HTML: m.HTML})
				}
				continue
			}
			if _, waiting := at[te.oid]; !waiting {
				need = append(need, te.oid)
			}
			at[te.oid] = append(at[te.oid], i)
		}
		if err := s.catEach(ctx, need, func(i int, buf []byte) error {
			oid := need[i]
			m := entryMeta(buf)
			s.metas.put(oid, m)
			for _, j := range at[oid] {
				if m.URL != "" {
					es = append(es, entry{Name: t.entries[j].name, URL: m.URL, Title: m.Title, HTML: m.HTML})
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
		slices.SortFunc(es, func(a, b entry) int { return cmp.Compare(a.URL, b.URL) })
		return es, nil
	})
}

// entryKey is the sha1(url) a cache entry's filename ends with. The category
// prefix has changed over time, and has had a hyphen in it, so only the last
// one separates it.
func entryKey(name string) (string, bool) {
	if i := strings.LastIndexByte(name, '-'); i >= 0 {
		if key := name[i+1:]; len(key) == hex.EncodedLen(sha1.Size) {
			return key, true
		}
	}
	return "", false
}

// rec is one cache entry's change in one commit.
type rec struct {
	commit  string
	name    string // the entry's filename in it
	oid     string // the blob it became, empty if it was removed
	added   int32
	deleted int32
}

// changelog is what changed in every commit, indexed by entry. It is keyed by
// commit rather than by version, so a reload doesn't invalidate it, and it is
// scanned in one pass: walking the whole history costs about what a dozen
// per-page walks do, and it covers every page.
type changelog struct {
	mu     sync.RWMutex
	tip    string              // what the last scan ended at, to extend from
	seen   map[string]struct{} // the commits it covers
	recs   []rec
	byKey  map[string][]int32 // sha1(url) -> indexes into recs
	byName map[string][]int32 // filename -> indexes into recs
}

// from is the commit the next scan can pick up at.
func (cl *changelog) from() string {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return cl.tip
}

// merge records a scan, skipping the commits already in it (two scans can
// overlap when the history moves under one of them), and reports the commits it
// added.
func (cl *changelog) merge(tip string, hashes []string, batch []rec) []string {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	if cl.seen == nil {
		cl.seen = map[string]struct{}{}
		cl.byKey, cl.byName = map[string][]int32{}, map[string][]int32{}
	}
	var added []string
	fresh := make(map[string]bool, len(hashes))
	for _, h := range hashes {
		if _, ok := cl.seen[h]; !ok {
			cl.seen[h], fresh[h] = struct{}{}, true
			added = append(added, h)
		}
	}
	for _, r := range batch {
		if !fresh[r.commit] {
			continue
		}
		i := int32(len(cl.recs))
		cl.recs = append(cl.recs, r)
		if key, ok := entryKey(r.name); ok {
			cl.byKey[key] = append(cl.byKey[key], i)
		}
		cl.byName[r.name] = append(cl.byName[r.name], i)
	}
	cl.tip = tip
	return added
}

// get returns the recorded changes for one page, in scan order.
func (cl *changelog) get(ref pageRef) []rec {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	idx := cl.byKey[ref.Key]
	if ref.named() {
		idx = cl.byName[ref.Name]
	}
	out := make([]rec, len(idx))
	for i, j := range idx {
		out[i] = cl.recs[j]
	}
	return out
}

// scanned makes sure the changelog covers the history being served. The scan is
// shared: the first caller runs it and the rest wait for it.
func (s *view) scanned(ctx context.Context) error {
	_, err := s.scans.get(ctx, s.tip, func(tip string) (struct{}, error) {
		return struct{}{}, s.rescan(ctx, tip)
	})
	return err
}

// rescan records what changed in each commit which hasn't been scanned yet.
func (s *server) rescan(ctx context.Context, tip string) error {
	if err := s.wait(ctx); err != nil {
		return err
	}
	start := time.Now()

	// the scan covers a first-parent prefix of the history, so it only has to
	// be extended, as long as what it ended at is still part of it
	rev := tip
	if from := s.changes.from(); from != "" {
		if _, err := gitExec(ctx, s.repo, "merge-base", "--is-ancestor", "--end-of-options", from, tip); err == nil {
			rev = from + ".." + tip
		} else {
			slog.Warn("scan: the history no longer contains the last scan, reading all of it", "from", from)
		}
	}

	// `--raw` names the blob each entry became, so a change can be classified
	// without reading anything which has been read before
	cmd := exec.CommandContext(ctx, "git", "log", "--first-parent", "--no-renames",
		"--raw", "--numstat", "--no-abbrev", "--format=%x00%H", "--end-of-options", rev)
	cmd.Dir = s.repo
	cmd.Stdin = nil

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}

	var (
		commit string
		hashes []string
		batch  []rec
		names  = map[string]string{} // a few hundred filenames over ~80k records
		at     = map[string]int{}    // filename -> index into batch, per commit
	)
	sc := bufio.NewScanner(stdout)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
		case strings.HasPrefix(line, "\x00"):
			commit = line[1:]
			hashes = append(hashes, commit)
			clear(at)
		case strings.HasPrefix(line, ":"):
			// `:<mode> <mode> <src> <dst> <status>\t<path>`
			meta, path, ok := strings.Cut(line, "\t")
			if !ok || commit == "" {
				continue
			}
			f := strings.Fields(meta)
			if len(f) < 5 {
				continue
			}
			oid := f[3]
			if strings.Trim(oid, "0") == "" {
				oid = "" // the entry was removed
			}
			name, ok := names[path]
			if !ok {
				name, names[path] = path, path
			}
			at[name] = len(batch)
			batch = append(batch, rec{commit: commit, name: name, oid: oid})
		default:
			// `<added>\t<deleted>\t<path>`, or `-` for binary files
			add, rest, ok := strings.Cut(line, "\t")
			if !ok {
				continue
			}
			del, path, ok := strings.Cut(rest, "\t")
			if !ok {
				continue
			}
			i, ok := at[path]
			if !ok {
				continue // no raw line for it, so there is nothing to count
			}
			if n, err := strconv.Atoi(add); err == nil {
				batch[i].added = int32(n)
			}
			if n, err := strconv.Atoi(del); err == nil {
				batch[i].deleted = int32(n)
			}
		}
	}
	if err := sc.Err(); err != nil {
		cmd.Process.Kill()
		cmd.Wait()
		return fmt.Errorf("read git log: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return transformError(err, stderr.Bytes())
	}

	fresh := s.changes.merge(tip, hashes, batch)
	if s.cache != nil {
		s.cache.putScan(fresh, batch, tip)
	}
	slog.Info("scan: read what changed in each commit", "commits", len(fresh), "changes", len(batch), "took", time.Since(start))
	return nil
}

// history gets the versions at which a cache entry changed, ascending, with the
// size of each change (used for the seekbar bar heights).
func (s *view) history(ctx context.Context, ref pageRef) ([]change, error) {
	if ref.Key == "" {
		return nil, nil
	}
	if err := s.scanned(ctx); err != nil {
		return nil, err
	}
	// version indexes are baked into the result, so the tip is part of the key:
	// a request still rendering against an older history can't poison it
	return s.hist.get(ctx, s.tip+" "+ref.Name+" "+ref.Key, func(string) ([]change, error) {
		var (
			cs []change
			at = map[int]int{} // version -> index into cs
		)
		for _, r := range s.changes.get(ref) {
			i, ok := s.index[r.commit]
			if !ok {
				continue // not part of the history being served
			}
			j, ok := at[i]
			if !ok {
				j, at[i] = len(cs), len(cs)
				cs = append(cs, change{Version: i})
			}
			// a commit can touch the entry under more than one name while the
			// category prefix is changing; what it ended up as is the last one
			cs[j].Added += int(r.added)
			cs[j].Deleted += int(r.deleted)
			cs[j].name, cs[j].oid = r.name, r.oid
		}
		slices.SortFunc(cs, func(a, b change) int { return cmp.Compare(a.Version, b.Version) })

		// an unclassified history isn't worth keeping: every change would read
		// as "no text change", and the default filter would hide all of them
		if err := s.classify(ctx, cs); err != nil {
			return nil, fmt.Errorf("classify changes: %w", err)
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

	// a blob says the same thing wherever it turns up, so only the ones which
	// haven't been read before are read at all
	ds := make([]digest, len(cs))
	var (
		need []string             // blobs still to read
		at   = map[string][]int{} // blob -> the changes waiting on it
	)
	for i := range cs {
		switch d, ok := s.digests.get(cs[i].oid); {
		case cs[i].oid == "":
			cs[i].Tier = tierGone // the cache was cleared at this version
		case ok:
			ds[i] = d
		default:
			if _, waiting := at[cs[i].oid]; !waiting {
				need = append(need, cs[i].oid)
			}
			at[cs[i].oid] = append(at[cs[i].oid], i)
		}
	}

	// parsing a few hundred pages, so spread it over the cpus; each one is
	// digested as it is read, so they aren't all in memory at once
	var (
		wg  sync.WaitGroup
		sem = make(chan struct{}, runtime.GOMAXPROCS(0))
	)
	err := s.catEach(ctx, need, func(i int, buf []byte) error {
		oid := need[i]
		if buf == nil {
			return fmt.Errorf("blob %s is missing", oid) // it was named by the scan
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			d, m := digestEntry(buf)
			s.digests.put(oid, d)
			s.metas.put(oid, m) // read once, and listing a commit needs it too
			for _, j := range at[oid] {
				ds[j] = d
			}
		}()
		return nil
	})
	wg.Wait()
	if err != nil {
		return err
	}

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

// digestCache holds the fingerprint of a blob, which is what classifying a
// change costs. Keyed by the blob, so it is shared by every page and every
// version with the same content, and outlives a reload: the day's new commit
// only adds what it actually changed.
type digestCache struct {
	mu   sync.Mutex
	m    map[string]digest
	file *cacheFile // where they're kept between runs, nil if nowhere
}

func (dc *digestCache) get(oid string) (digest, bool) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	d, ok := dc.m[oid]
	return d, ok
}

// add remembers a digest for as long as the process lives.
func (dc *digestCache) add(oid string, d digest) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if dc.m == nil {
		dc.m = map[string]digest{}
	}
	dc.m[oid] = d
}

// put remembers a digest, and writes it out so the next run doesn't have to
// work it out again.
func (dc *digestCache) put(oid string, d digest) {
	dc.add(oid, d)

	dc.mu.Lock()
	file := dc.file
	dc.mu.Unlock()

	if file != nil {
		file.putDigest(oid, d)
	}
}

// digest fingerprints the parts of a cached response which changes are
// classified by.
type digest struct {
	full  string // all text on the page
	main  string // text in the main content block
	sched string // text in the main content tables (drop-in schedules)
}

func digestEntry(buf []byte) (digest, meta) {
	req, resp, body, err := parseEntry(buf)
	if err != nil {
		return digest{full: hashBytes(buf)}, meta{}
	}
	m := entryMetaOf(req, resp, body)

	if !strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "html") {
		return digest{full: hashBytes(body)}, m
	}
	doc, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return digest{full: hashBytes(body)}, m
	}

	d := digest{full: hashText(doc)}
	if main := findID(doc, mainID); main != nil {
		d.main = hashText(main)
		d.sched = hashText(slices.Collect(elements(main, atom.Table))...)
	}
	return d, m
}

// meta is what a stored response says about itself, which is everything the
// entry listing and the page sidebar need.
type meta struct {
	URL   string
	Title string
	HTML  bool
}

// entryMeta reads a stored response's metadata, without digesting it.
func entryMeta(buf []byte) meta {
	req, resp, body, err := parseEntry(buf)
	if err != nil {
		return meta{} // missing, or not a stored response
	}
	return entryMetaOf(req, resp, body)
}

func entryMetaOf(req *http.Request, resp *http.Response, body []byte) meta {
	m := meta{URL: req.URL.String()}
	if resp.StatusCode == http.StatusOK && strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/html") {
		m.HTML, m.Title = true, pageTitle(body)
	}
	return m
}

// metaCache holds what a blob says it is, so listing a commit's entries doesn't
// have to read them. Keyed by the blob, like the digests; the urls and titles
// are shared, since a few hundred of them cover every version of every page.
type metaCache struct {
	mu   sync.Mutex
	m    map[string]meta
	strs map[string]string
	file *cacheFile // where they're kept between runs, nil if nowhere
}

func (mc *metaCache) get(oid string) (meta, bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	m, ok := mc.m[oid]
	return m, ok
}

// add remembers what a blob is for as long as the process lives.
func (mc *metaCache) add(oid string, m meta) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.m == nil {
		mc.m, mc.strs = map[string]meta{}, map[string]string{}
	}
	m.URL, m.Title = mc.internLocked(m.URL), mc.internLocked(m.Title)
	mc.m[oid] = m
}

// put remembers what a blob is, and writes it out so the next run doesn't have
// to read it again.
func (mc *metaCache) put(oid string, m meta) {
	mc.add(oid, m)

	mc.mu.Lock()
	file := mc.file
	mc.mu.Unlock()

	if file != nil {
		file.putMeta(oid, m)
	}
}

// internLocked shares one copy of a string. The lock is held.
func (mc *metaCache) internLocked(s string) string {
	if s == "" {
		return ""
	}
	if v, ok := mc.strs[s]; ok {
		return v
	}
	mc.strs[s] = s
	return s
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
func (s *view) resolve(ctx context.Context, cur int, key string, u *url.URL) (string, error) {
	hash := s.versions[cur].Hash

	t, err := s.tree(ctx, hash)
	if err != nil {
		return "", err
	}
	if name, ok := t.byKey[key]; ok {
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
	return s.urls.get(ctx, hash, func(hash string) (map[string]string, error) {
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

// catEach is like cat, but reads multiple `<commit>:<path>` specs with a single
// process, calling fn with the contents of each in turn (nil for a missing
// one). Only what fn keeps stays in memory: a commit's entries are ~14MB
// together, and a page's history ~30MB.
func (s *server) catEach(ctx context.Context, specs []string, fn func(i int, buf []byte) error) error {
	if len(specs) == 0 {
		return nil
	}
	if err := s.wait(ctx); err != nil {
		return err
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
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	// leaving early means leaving objects unread, which Wait would block on
	abort := func(err error) error {
		cmd.Process.Kill()
		cmd.Wait()
		return err
	}

	// `<oid> <type> <size>\n<contents>\n` or `<input> missing\n`
	var n int
	r := bufio.NewReader(stdout)
	for ; n < len(specs); n++ {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		_, rest, ok := strings.Cut(strings.TrimSuffix(line, "\n"), " ") // oid
		if ok {
			_, rest, ok = strings.Cut(rest, " ") // type
		}
		if !ok {
			if err := fn(n, nil); err != nil { // "<input> missing"
				return abort(err)
			}
			continue
		}
		size, err := strconv.ParseInt(rest, 10, 64)
		if err != nil {
			return abort(fmt.Errorf("parse cat-file header %q: %w", line, err))
		}
		buf := make([]byte, size+1) // includes the trailing newline
		if _, err := io.ReadFull(r, buf); err != nil {
			return abort(fmt.Errorf("read cat-file contents: %w", err))
		}
		if err := fn(n, buf[:size]); err != nil {
			return abort(err)
		}
	}
	if err := cmd.Wait(); err != nil {
		return transformError(err, stderr.Bytes())
	}
	if n != len(specs) {
		return fmt.Errorf("read %d of %d objects", n, len(specs))
	}
	return nil
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

// The disk cache.
//
// What the server works out about the repo - every blob's digest, and what
// changed in each commit - takes minutes to derive and is what a restart would
// otherwise pay for again. All of it is keyed by content, so it stays valid
// across reloads, rebuilds of this program and re-clones of the repo: only a
// change to how something is derived invalidates it, which is what the format
// line covers. Nothing which is keyed by version index goes in it, since those
// move when the history does.
//
// The file is a format line followed by `<kind><length><payload>` records,
// appended as things are worked out. A torn tail from a crash is truncated when
// it's read back, and a commit is a single record, so half a scan can't be
// mistaken for a whole one. One writer is expected: a second one risks losing
// records, never mixing them up.
const (
	// cacheFormat is the file's layout, and the version of each thing in it. A
	// version is bumped when what it holds is derived differently:
	//   digests - digestEntry, hashText, texts, noText, mainID
	//   commits - what rescan reads out of git log
	//   metas   - entryMetaOf, pageTitle
	// A file which doesn't match is written again from scratch.
	cacheFormat = "ottrec-webarchive cache: format 1, digests 1, commits 1, metas 1\n"

	kindDigest = 1 // one blob's digest
	kindCommit = 2 // what one commit changed
	kindScan   = 3 // the commit the scan had reached, after the commits it covers
	kindMeta   = 4 // what one blob says it is

	cacheFlushEvery = time.Second * 5
	cacheFlushSize  = 1 << 20
	cacheMaxRecord  = 1 << 24 // an impossible length is a corrupt file, not an allocation
)

// zeroOID is the null blob: an entry which was removed.
var zeroOID [sha1.Size]byte

type cacheFile struct {
	path string

	mu      sync.Mutex
	f       *os.File
	pending []byte // records waiting to be written
	scratch []byte // reused while encoding one
	broken  bool   // writing failed, so nothing more is kept
}

// openCache opens the cache file in dir, creating it if it isn't there.
func openCache(dir string) (*cacheFile, error) {
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "webarchive.cache")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o666)
	if err != nil {
		return nil, err
	}
	return &cacheFile{path: path, f: f}, nil
}

// useCache reads back what earlier runs worked out, and starts keeping what
// this one does. It runs before anything is served, so nothing else is looking
// at what it fills in.
func (s *server) useCache(ctx context.Context, dir string) {
	start := time.Now()

	c, err := openCache(dir)
	if err != nil {
		slog.Error("cache: carrying on without one", "dir", dir, "error", err)
		return
	}
	n, err := c.load(&s.digests, &s.metas, &s.changes)
	if err != nil {
		slog.Error("cache: carrying on without one", "path", c.path, "error", err)
		return
	}
	s.digests.file, s.metas.file, s.cache = c, c, c
	go c.run(ctx)

	slog.Info("cache: read", "path", c.path,
		"digests", n[kindDigest], "metas", n[kindMeta], "commits", n[kindCommit], "took", time.Since(start))
}

// run writes out what has been recorded, until ctx is done.
func (c *cacheFile) run(ctx context.Context) {
	t := time.NewTicker(cacheFlushEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			c.flush()
			return
		case <-t.C:
			c.flush()
		}
	}
}

// load reads the file, reporting how much was in it. Anything it can't read is
// dropped: this is a cache, so it is simply worked out again.
func (c *cacheFile) load(dc *digestCache, mc *metaCache, cl *changelog) (n map[byte]int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	n = map[byte]int{}
	if _, err := c.f.Seek(0, io.SeekStart); err != nil {
		return n, err
	}
	r := bufio.NewReader(c.f)

	line, err := r.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return n, err
	}
	if line != cacheFormat {
		if line != "" {
			slog.Info("cache: written by a different version, starting it again", "path", c.path)
		}
		return n, c.resetLocked()
	}

	var (
		off    = int64(len(line)) // the end of the last whole record
		hashes []string
		batch  []rec
		names  = map[string]string{} // a few hundred filenames over ~80k records
		tip    string
		bad    bool
	)
	for !bad {
		kind, err := r.ReadByte()
		if err != nil {
			break
		}
		size, err := binary.ReadUvarint(r)
		if err != nil || size > cacheMaxRecord {
			break
		}
		p := make([]byte, size)
		if _, err := io.ReadFull(r, p); err != nil {
			break
		}
		switch kind {
		case kindDigest:
			oid, d, ok := parseDigest(p)
			if !ok {
				bad = true
				continue
			}
			dc.add(oid, d)
			n[kindDigest]++
		case kindMeta:
			oid, m, ok := parseMeta(p)
			if !ok {
				bad = true
				continue
			}
			mc.add(oid, m)
			n[kindMeta]++
		case kindCommit:
			hash, rs, ok := parseCommit(p, names)
			if !ok {
				bad = true
				continue
			}
			hashes = append(hashes, hash)
			batch = append(batch, rs...)
			n[kindCommit]++
		case kindScan:
			if len(p) == sha1.Size {
				tip = hex.EncodeToString(p)
			}
		}
		off += 1 + uvarintLen(size) + int64(size)
	}
	if len(hashes) > 0 {
		cl.merge(tip, hashes, batch)
	}

	// whatever is past the last whole record is a write which didn't finish
	if st, err := c.f.Stat(); err == nil && st.Size() > off {
		slog.Warn("cache: dropping an unfinished write at the end", "path", c.path, "bytes", st.Size()-off)
		if err := c.f.Truncate(off); err != nil {
			return n, err
		}
	}
	return n, nil
}

// resetLocked empties the file and writes the format line. The lock is held.
func (c *cacheFile) resetLocked() error {
	if err := c.f.Truncate(0); err != nil {
		return err
	}
	c.pending = append(c.pending[:0], cacheFormat...)
	return c.flushLocked()
}

// putDigest records what a blob says.
func (c *cacheFile) putDigest(oid string, d digest) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var ok bool
	if c.scratch, ok = appendDigest(c.scratch[:0], oid, d); ok {
		c.record(kindDigest, c.scratch)
	}
	if len(c.pending) >= cacheFlushSize {
		c.flushLocked()
	}
}

// putMeta records what a blob says it is.
func (c *cacheFile) putMeta(oid string, m meta) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var ok bool
	if c.scratch, ok = appendMeta(c.scratch[:0], oid, m); ok {
		c.record(kindMeta, c.scratch)
	}
	if len(c.pending) >= cacheFlushSize {
		c.flushLocked()
	}
}

// putScan records what a scan found in the commits it added, and where it got
// to. The mark goes last, so a write which didn't finish leaves the next run
// scanning again from further back rather than trusting half a scan.
func (c *cacheFile) putScan(fresh []string, batch []rec, tip string) {
	if len(fresh) == 0 {
		return
	}
	keep := make(map[string]bool, len(fresh))
	for _, h := range fresh {
		keep[h] = true
	}
	byCommit := make(map[string][]rec, len(fresh))
	for _, r := range batch {
		if keep[r.commit] {
			byCommit[r.commit] = append(byCommit[r.commit], r)
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, h := range fresh {
		var ok bool
		if c.scratch, ok = appendCommit(c.scratch[:0], h, byCommit[h]); ok {
			c.record(kindCommit, c.scratch)
		}
	}
	if raw, err := hex.DecodeString(tip); err == nil && len(raw) == sha1.Size {
		c.record(kindScan, raw)
	}
	c.flushLocked() // a scan is worth more than the rest, and is written rarely
}

// record adds a record to what is waiting to be written. The lock is held.
func (c *cacheFile) record(kind byte, payload []byte) {
	if c.broken {
		return
	}
	c.pending = append(c.pending, kind)
	c.pending = binary.AppendUvarint(c.pending, uint64(len(payload)))
	c.pending = append(c.pending, payload...)
}

func (c *cacheFile) flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flushLocked()
}

// flushLocked writes what is waiting, in one go so a second writer can only
// interleave whole records. The lock is held.
func (c *cacheFile) flushLocked() error {
	if c.broken || len(c.pending) == 0 {
		return nil
	}
	if _, err := c.f.Write(c.pending); err != nil {
		slog.Error("cache: write failed, keeping the rest in memory", "path", c.path, "error", err)
		c.broken, c.pending = true, nil
		return err
	}
	c.pending = c.pending[:0]
	return nil
}

// appendDigest encodes a blob's digest: the blob, which of the three hashes are
// there, and the hashes themselves.
func appendDigest(buf []byte, oid string, d digest) ([]byte, bool) {
	raw, err := hex.DecodeString(oid)
	if err != nil || len(raw) != sha1.Size {
		return buf, false
	}
	buf = append(buf, raw...)

	hs := [3]string{d.full, d.main, d.sched}
	var flags byte
	for i, h := range hs {
		if len(h) == sha1.Size {
			flags |= 1 << i
		}
	}
	buf = append(buf, flags)
	for _, h := range hs {
		if len(h) == sha1.Size {
			buf = append(buf, h...)
		} else {
			buf = append(buf, zeroOID[:]...)
		}
	}
	return buf, true
}

func parseDigest(p []byte) (string, digest, bool) {
	if len(p) != sha1.Size*4+1 {
		return "", digest{}, false
	}
	var (
		d     digest
		flags = p[sha1.Size]
		hs    = [3]*string{&d.full, &d.main, &d.sched}
	)
	for i, h := range hs {
		if flags&(1<<i) != 0 {
			at := sha1.Size + 1 + i*sha1.Size
			*h = string(p[at : at+sha1.Size])
		}
	}
	return hex.EncodeToString(p[:sha1.Size]), d, true
}

// appendMeta encodes what a blob says it is. An empty one is a blob which
// isn't a stored response, and is worth recording so it isn't read again.
func appendMeta(buf []byte, oid string, m meta) ([]byte, bool) {
	raw, err := hex.DecodeString(oid)
	if err != nil || len(raw) != sha1.Size {
		return buf, false
	}
	buf = append(buf, raw...)
	if m.HTML {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	for _, s := range [2]string{m.URL, m.Title} {
		buf = binary.AppendUvarint(buf, uint64(len(s)))
		buf = append(buf, s...)
	}
	return buf, true
}

func parseMeta(p []byte) (string, meta, bool) {
	if len(p) < sha1.Size+1 {
		return "", meta{}, false
	}
	m := meta{HTML: p[sha1.Size] == 1}
	oid, p := hex.EncodeToString(p[:sha1.Size]), p[sha1.Size+1:]

	for _, s := range [2]*string{&m.URL, &m.Title} {
		size, w := binary.Uvarint(p)
		if w <= 0 || size > uint64(len(p)-w) {
			return "", meta{}, false
		}
		*s, p = string(p[w:w+int(size)]), p[w+int(size):]
	}
	return oid, m, true
}

// appendCommit encodes what one commit changed. It is one record, so it is
// either all there or not there at all.
func appendCommit(buf []byte, hash string, rs []rec) ([]byte, bool) {
	raw, err := hex.DecodeString(hash)
	if err != nil || len(raw) != sha1.Size {
		return buf, false
	}
	buf = append(buf, raw...)
	buf = binary.AppendUvarint(buf, uint64(len(rs)))
	for _, r := range rs {
		if oid, err := hex.DecodeString(r.oid); err == nil && len(oid) == sha1.Size {
			buf = append(buf, oid...)
		} else {
			buf = append(buf, zeroOID[:]...) // the entry was removed
		}
		buf = binary.AppendUvarint(buf, uint64(max(r.added, 0)))
		buf = binary.AppendUvarint(buf, uint64(max(r.deleted, 0)))
		buf = binary.AppendUvarint(buf, uint64(len(r.name)))
		buf = append(buf, r.name...)
	}
	return buf, true
}

func parseCommit(p []byte, names map[string]string) (string, []rec, bool) {
	if len(p) < sha1.Size {
		return "", nil, false
	}
	hash := hex.EncodeToString(p[:sha1.Size])
	p = p[sha1.Size:]

	n, w := binary.Uvarint(p)
	if w <= 0 || n > uint64(len(p)) {
		return "", nil, false
	}
	p = p[w:]

	rs := make([]rec, 0, n)
	for range n {
		if len(p) < sha1.Size {
			return "", nil, false
		}
		var oid string
		if !isZero(p[:sha1.Size]) {
			oid = hex.EncodeToString(p[:sha1.Size])
		}
		p = p[sha1.Size:]

		var ns [3]uint64
		for i := range ns {
			v, w := binary.Uvarint(p)
			if w <= 0 {
				return "", nil, false
			}
			ns[i], p = v, p[w:]
		}
		if ns[2] > uint64(len(p)) {
			return "", nil, false
		}
		name := string(p[:ns[2]])
		p = p[ns[2]:]
		if s, ok := names[name]; ok {
			name = s
		} else {
			names[name] = name
		}
		rs = append(rs, rec{
			commit:  hash,
			name:    name,
			oid:     oid,
			added:   int32(ns[0]),
			deleted: int32(ns[1]),
		})
	}
	return hash, rs, true
}

func isZero(p []byte) bool {
	for _, b := range p {
		if b != 0 {
			return false
		}
	}
	return true
}

// uvarintLen is how many bytes [binary.AppendUvarint] writes for n.
func uvarintLen(n uint64) int64 {
	var b [binary.MaxVarintLen64]byte
	return int64(binary.PutUvarint(b[:], n))
}

// memo caches expensive lookups (what they describe is immutable, so nothing
// needs invalidating; max bounds how many are kept). The lock is only held
// around the bookkeeping, never the lookup itself: concurrent callers of
// different keys don't serialize, and concurrent callers of the same one wait
// for the first rather than repeating it. Failures aren't cached.
type memo[K comparable, V any] struct {
	max int // most entries to keep, least recently used first out (0 for all)

	mu   sync.Mutex
	m    map[K]*memoEntry[V]
	tick int64
}

// memoEntry is one lookup, in flight or done.
type memoEntry[V any] struct {
	done chan struct{} // closed once v and err are set
	used int64         // when it was last handed out
	v    V
	err  error
}

func (m *memo[K, V]) clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.m = nil
}

func (m *memo[K, V]) get(ctx context.Context, k K, fn func(K) (V, error)) (V, error) {
	m.mu.Lock()
	m.tick++
	if e, ok := m.m[k]; ok {
		e.used = m.tick
		m.mu.Unlock()
		select {
		case <-e.done:
			return e.v, e.err
		case <-ctx.Done():
			var zero V
			return zero, ctx.Err()
		}
	}
	e := &memoEntry[V]{done: make(chan struct{}), used: m.tick}
	if m.m == nil {
		m.m = map[K]*memoEntry[V]{}
	}
	m.m[k] = e
	m.evict()
	m.mu.Unlock()

	e.v, e.err = fn(k)
	close(e.done)

	if e.err != nil {
		// a transient failure (a cancelled request, git quota) would otherwise
		// be the answer forever
		m.mu.Lock()
		if m.m[k] == e {
			delete(m.m, k)
		}
		m.mu.Unlock()
	}
	return e.v, e.err
}

// evict drops the least recently used entries until the memo is within its
// limit. It runs with the lock held.
func (m *memo[K, V]) evict() {
	for m.max > 0 && len(m.m) > m.max {
		var (
			old   K
			used  int64
			found bool
		)
		for k, e := range m.m {
			if !found || e.used < used {
				old, used, found = k, e.used, true
			}
		}
		delete(m.m, old)
	}
}
