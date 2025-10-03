// Command ottrec-data serves data.ottrec.ca.
package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"net/http"
	"os"
	"time"
	_ "time/tzdata"

	"github.com/lmittmann/tint"
	_ "github.com/ncruces/go-sqlite3/embed"
	"github.com/pgaskin/ottrec-website/internal/gitsh"
	"github.com/pgaskin/ottrec-website/internal/pflagx"
	"github.com/pgaskin/ottrec-website/pkg/ottrecdata"
	"github.com/pgaskin/ottrec-website/routes"
	"github.com/spf13/pflag"
)

// note: if the repo gets force-pushed over, old data won't be automatically cleaned up (TODO: maybe we should drop all rows, re-insert, and vacuum?)

var (
	EnvPrefix    = "OTTREC_DATA_"
	Addr         = pflag.StringP("addr", "a", ":8082", "listen address")
	Host         = pflag.StringP("host", "H", "data.ottrec.localhost", "canonical url host")
	Cache        = pflag.StringP("cache", "c", "/tmp/ottrec-data.db", "cache database path (will be wiped and recreated if doesn't exist or outdated)")
	Repo         = pflag.StringP("repo", "r", "/tmp/ottrec-data.git", "data git repo path (if not set, db will be treated as read-only) (will be initialized as a bare repo if empty)")
	RepoRemote   = pflag.String("repo-remote", "https://github.com/pgaskin/ottrec-data.git", "remote to fetch")
	RepoBranch   = pflag.String("repo-branch", "v1", "branch to fetch (will be overwriten in the local repo)")
	RepoRev      = pflag.String("repo-rev", "", "override the rev to scan (for debugging only)")
	RepoInterval = pflag.DurationP("repo-interval", "i", time.Minute*15, "poll interval for repo (0 to only pull once at startup)")
	LogLevel     = pflagx.LevelP("log-level", "L", slog.LevelInfo, "log level")
	LogJSON      = pflag.Bool("log-json", false, "use json logs")
	Help         = pflag.BoolP("help", "h", false, "show this help text")
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

	if *Cache == "" {
		fmt.Fprintf(os.Stderr, "error: no cache path specified\n")
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
	var readonly bool
	if *Repo != "" {
		if *RepoBranch == "" {
			return fmt.Errorf("no branch specified for repo")
		}
		if _, err := gitsh.GitDir(context.Background(), *Repo); err != nil {
			if _, err := os.Stat(*Repo); err == nil {
				return fmt.Errorf("invalid repo path %q: %w", *Repo, err)
			} else if errors.Is(err, os.ErrNotExist) {
				slog.Info("initializing git repo", "path", *Repo)
				if err := gitsh.Exec(context.Background(), "", func(lines iter.Seq[string]) {
					for line := range lines {
						slog.Info("git: " + line)
					}
				}, "init", "--bare", *Repo); err != nil {
					return fmt.Errorf("initialize repo %q: %w", *Repo, err)
				}
			} else {
				return fmt.Errorf("failed to access repo %q: %w", *Repo, err)
			}
		}
	} else {
		slog.Warn("no repo path specified, running in read-only mode")
		readonly = true
	}

	slog.Info("opening cache", "path", *Cache)
	cache, err := ottrecdata.OpenCache(*Cache, false)
	if !readonly && errors.Is(err, ottrecdata.ErrUnsupportedSchema) {
		slog.Warn("unsupported cache schema version, resetting")
		cache, err = ottrecdata.OpenCache(*Cache, true)
	}
	if err != nil {
		return fmt.Errorf("open cache: %w", err)
	}
	defer cache.Close()

	if !readonly {
		slog.Info("updater: starting repo fetcher", "interval", *RepoInterval)
		go func() {
			ticker := time.Tick(*RepoInterval)
			for {
				if *RepoRemote != "" {
					slog.Info("updater: fetching repo")
					// TODO: fetch timeout
					if err := gitsh.Exec(context.Background(), *Repo, func(lines iter.Seq[string]) {
						for line := range lines {
							slog.Info("updater: git fetch: " + line)
						}
					},
						"fetch",
						"--verbose",
						"--no-write-fetch-head",
						"--refmap", "+refs/heads/"+*RepoBranch+":refs/heads/"+*RepoBranch+"", // +(force) (remote) (local)
						*RepoRemote,
						"refs/heads/"+*RepoBranch,
					); err != nil {
						slog.Error("updater: fetch failed", "error", err)
					}
				}
				slog.Info("updater: updating cache")
				if err := cache.Import(context.Background(), slog.Default(), *Repo, cmp.Or(*RepoRev, *RepoBranch)); err != nil {
					slog.Error("updater: cache update failed", "error", err)
				}
				if ticker == nil {
					slog.Warn("updater: repo polling disabled")
					return
				}
				<-ticker
			}
		}()
	}

	handler, err := routes.Data(routes.DataConfig{
		Host:  *Host,
		Cache: cache,
	})
	if err != nil {
		return fmt.Errorf("initialize routes: %w", err)
	}

	slog.Info("http: listening", "addr", *Addr)
	return http.ListenAndServe(*Addr, handler)
}
