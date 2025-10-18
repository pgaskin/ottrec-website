//go:build ignore

package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"unicode/utf8"

	"github.com/evanw/esbuild/pkg/api"
)

// note: unlike the other stuff I do in the static package, this one isn't fully
// reproducible since versions may resolve differently

const (
	PostCSSVersion          = "8.5.6"
	PostCSSPresetEnvVersion = "10.4.0"
)

const Source = `
	export { default as postcss } from 'https://esm.sh/postcss@` + PostCSSVersion + `?dev&target=es2015'
	export { default as postcssPresetEnv } from 'https://esm.sh/postcss-preset-env@` + PostCSSPresetEnvVersion + `?dev&target=es2015&deps=postcss@` + PostCSSVersion + `'
`

func main() {
	name := "bundle.js"

	tmp, err := os.MkdirTemp("", "")
	if err != nil {
		slog.Error("failed to create temp dir", "error", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmp)

	if err := os.WriteFile(filepath.Join(tmp, name), []byte(Source), 0644); err != nil {
		slog.Error("failed to write entry source", "error", err)
		os.Exit(1)
	}

	result := api.Build(api.BuildOptions{
		LogLevel:      api.LogLevelError,
		AbsWorkingDir: tmp,
		EntryPoints:   []string{name},
		Bundle:        true,
		Drop:          api.DropConsole,
		LegalComments: api.LegalCommentsEndOfFile,
		Sourcemap:     api.SourceMapInline,
		Target:        api.ES2015,
		Platform:      api.PlatformNeutral,
		Format:        api.FormatIIFE,
		GlobalName:    "bundle",
		Plugins: []api.Plugin{
			httpPlugin,
		},
	})
	if len(result.Errors) != 0 {
		slog.Error("failed to build package")
		os.Exit(1)
	}
	if len(result.OutputFiles) != 1 {
		slog.Error("expected exactly one output file", "n", len(result.OutputFiles))
		os.Exit(1)
	}

	buf := result.OutputFiles[0].Contents
	if err := os.WriteFile(name, buf, 0644); err != nil {
		slog.Error("failed to write output", "error", err)
		os.Exit(1)
	}
	slog.Info("bundle built", "name", name, "size", len(buf))
}

var httpPlugin = api.Plugin{
	Name: "http",
	Setup: func(build api.PluginBuild) {
		const namespace = "http-url"

		build.OnResolve(api.OnResolveOptions{
			Filter: `^https?://`,
		}, func(args api.OnResolveArgs) (api.OnResolveResult, error) {
			u, err := url.Parse(args.Path)
			return api.OnResolveResult{
				Path:      u.String(),
				Namespace: namespace,
			}, err
		})

		build.OnResolve(api.OnResolveOptions{
			Filter:    `.*`,
			Namespace: namespace,
		}, func(args api.OnResolveArgs) (api.OnResolveResult, error) {
			u, err := url.Parse(args.Importer)
			if err != nil {
				panic(err)
			}
			u, err = u.Parse(args.Path)
			return api.OnResolveResult{
				Path:      u.String(),
				Namespace: namespace,
			}, err
		})

		build.OnLoad(api.OnLoadOptions{
			Filter:    `.*`,
			Namespace: namespace,
		}, func(args api.OnLoadArgs) (api.OnLoadResult, error) {
			slog.Info("fetching", "url", args.Path)

			resp, err := http.Get(args.Path)
			if err != nil {
				return api.OnLoadResult{}, err
			}
			defer resp.Body.Close()

			buf, err := io.ReadAll(resp.Body)
			if err != nil {
				return api.OnLoadResult{}, err
			}
			str := string(buf)

			if resp.StatusCode != http.StatusOK {
				if utf8.ValidString(str) {
					return api.OnLoadResult{}, fmt.Errorf("response status %d (body: %q)", resp.StatusCode, str)
				}
				return api.OnLoadResult{}, fmt.Errorf("response status %d", resp.StatusCode)
			}

			return api.OnLoadResult{
				Contents: &str,
			}, nil
		})
	},
}
