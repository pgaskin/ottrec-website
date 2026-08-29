// Command ottrec-website serves ottrec.ca.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"
	"unicode/utf8"

	"github.com/lmittmann/tint"
	"github.com/ottrec/data-enrichment/enrich"
	"github.com/ottrec/data-enrichment/enrichidx"
	"github.com/ottrec/website/internal/pflagx"
	"github.com/ottrec/website/pkg/ottrecidx"
	"github.com/ottrec/website/routes"
	"github.com/spf13/pflag"
)

var (
	EnvPrefix           = "OTTREC_WEBSITE_"
	Addr                = pflag.StringP("addr", "a", ":8083", "listen address")
	Data                = pflag.StringP("data", "d", "http://localhost:8082/v1/latest/pb", "url or path to data protobuf")
	DataInterval        = pflag.DurationP("data-interval", "i", time.Minute*15, "poll interval for data")
	NoEnrich            = pflag.Bool("no-enrich", false, "don't derive schedule-change enrichment from the data for the today page")
	HeadHTML            = pflag.String("head-html", "", "raw html to inject at the bottom of <head> on every page")
	AboutHTML           = pflag.String("about-html", "", "raw html to inject in the about page")
	HomeHTML            = pflag.String("home-html", "", "raw html to inject at the bottom of the homepage main content")
	MapTilesLight       = pflag.String("map-tiles-light", "", "leaflet url template for the light basemap tiles (default: carto voyager)")
	MapTilesDark        = pflag.String("map-tiles-dark", "", "leaflet url template for the dark basemap tiles (default: carto dark_all)")
	MapTilesAttribution = pflag.String("map-tiles-attribution", "", "raw html crediting the basemap tile source (default: openstreetmap + carto)")
	MapTilesSubdomains  = pflag.String("map-tiles-subdomains", "", "{s} substitutions for the basemap tile url templates (default: abcd)")
	LogLevel            = pflagx.LevelP("log-level", "L", slog.LevelInfo, "log level")
	LogJSON             = pflag.Bool("log-json", false, "use json logs")
	Help                = pflag.BoolP("help", "h", false, "show this help text")
)

// TODO: http logs, request id

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

	if *Data == "" {
		fmt.Fprintf(os.Stderr, "error: no data uri specified\n")
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
	// snapshot pairs a loaded data index with the enrichment derived from it,
	// swapped atomically so /today never applies enrichment across versions.
	type snapshot struct {
		idx    *ottrecidx.Index
		enrich enrichidx.Ref
	}
	getData, getEnrich := func() (func() (ottrecidx.DataRef, bool), func(ottrecidx.DataRef) enrichidx.Ref) {
		var (
			update     = time.Tick(*DataInterval)
			backoffMin = time.Second
			backoffMax = time.Minute * 3
			backoff    time.Duration
			dbMu       sync.Mutex
			dbPtr      *snapshot
		)
		go func() {
			for {
				slog.Info("db: updating data", "uri", *Data, "interval", *DataInterval)
				if err := func() error {
					ctx := context.Background()
					ctx, cancel := context.WithTimeout(ctx, time.Second*15)
					defer cancel()

					db, err := loadData(ctx, *Data)
					if err != nil {
						return err
					}

					snap := &snapshot{idx: db}
					if !*NoEnrich {
						snap.enrich = buildEnrichment(db)
					}

					dbMu.Lock()
					defer dbMu.Unlock()
					dbPtr = snap

					return nil
				}(); err != nil {
					backoff = max(backoff, backoffMin)
					backoff += backoff / 2
					backoff = min(backoff, backoffMax)
					slog.Error("db: failed to load data", "error", err, "retry_after", backoff.Truncate(time.Second/4))
					time.Sleep(backoff)
					continue
				}
				slog.Info("db: updated data")
				backoff = 0
				<-update
			}
		}()
		return func() (ottrecidx.DataRef, bool) {
				dbMu.Lock()
				defer dbMu.Unlock()
				if dbPtr == nil {
					return ottrecidx.DataRef{}, false
				}
				return dbPtr.idx.Data(), true
			}, func(data ottrecidx.DataRef) enrichidx.Ref {
				dbMu.Lock()
				defer dbMu.Unlock()
				if dbPtr == nil || dbPtr.idx != data.Index() {
					return enrichidx.Ref{}
				}
				return dbPtr.enrich
			}
	}()

	handler, err := routes.Website(routes.WebsiteConfig{
		Data:                getData,
		Enrich:              getEnrich,
		HeadHTML:            *HeadHTML,
		AboutHTML:           *AboutHTML,
		HomeHTML:            *HomeHTML,
		MapTilesLight:       *MapTilesLight,
		MapTilesDark:        *MapTilesDark,
		MapTilesAttribution: *MapTilesAttribution,
		MapTilesSubdomains:  *MapTilesSubdomains,
	})
	if err != nil {
		return fmt.Errorf("initialize routes: %w", err)
	}

	slog.Info("http: listening", "addr", *Addr)
	return http.ListenAndServe(*Addr, handler)
}

// buildEnrichment derives the today-page enrichment from freshly loaded data.
// It is a progressive enhancement: any failure just disables it for this
// snapshot.
func buildEnrichment(idx *ottrecidx.Index) (ref enrichidx.Ref) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("db: failed to derive enrichment", "panic", r)
			ref = enrichidx.Ref{}
		}
	}()
	start := time.Now()
	out := enrich.EnrichVersion("", idx.Data())
	ref = enrichidx.Join(out)
	slog.Info("db: derived enrichment", "objects", len(out.GetObjects()), "took", time.Since(start).Truncate(time.Millisecond))
	return ref
}

func loadData(ctx context.Context, uri string) (*ottrecidx.Index, error) {
	var pb []byte
	if strings.Contains(uri, "://") {
		var err error
		if pb, err = fetch(ctx, uri); err != nil {
			return nil, fmt.Errorf("fetch %q: %w", uri, err)
		}
	} else {
		var err error
		if pb, err = os.ReadFile(uri); err != nil {
			return nil, fmt.Errorf("read %q: %w", uri, err)
		}
	}
	idx, err := new(ottrecidx.Indexer).Load(pb)
	if err != nil {
		return nil, fmt.Errorf("load %q: %w", uri, err)
	}
	return idx, nil
}

func fetch(ctx context.Context, uri string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "ottrec")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if buf, _ := io.ReadAll(io.LimitReader(resp.Body, 1024)); utf8.Valid(buf) {
			return nil, fmt.Errorf("response status %d (body: %q)", resp.StatusCode, buf)
		}
		return nil, fmt.Errorf("response status %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}
