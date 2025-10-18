// Package postcss embeds PostCSS with postcss-preset-env.
package postcss

import (
	_ "embed"
	"encoding/base64"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/fastschema/qjs"
)

// I do the frontend CSS this way since we're primarly server-rendered, and this
// means we don't need a complex frontend build process.

//go:generate go run bundle.go
//go:embed bundle.js
var bundleJS []byte

var wrapperJS = `
	const { postcss, postcssPresetEnv, postcssMinify } = bundle

	async function transform(css, browsers) {
		const proc = postcss([
			postcssPresetEnv({
				browsers: [browsers],
			}),
			postcssMinify(),
		])
		const res = await proc.process(css)
		return res.css
	}
`

var noop, _ = strconv.ParseBool(os.Getenv("DEBUG_POSTCSS_NOOP"))

var transform func(css, browsers string) (string, error)

func init() {
	if noop {
		return
	}
	slog.Info("initializing postcss")

	vm, err := qjs.New()
	if err != nil {
		panic(fmt.Errorf("postcss: initialize: quickjs: %w", err))
	}
	vm.Context().SetFunc("btoa", func(ctx *qjs.This) (*qjs.Value, error) {
		if len(ctx.Args()) <= 1 {
			return nil, fmt.Errorf("missing argument")
		}
		src := ctx.Args()[1].String()
		res := base64.StdEncoding.EncodeToString([]byte(src))
		return ctx.Context().NewString(res), nil
	})
	vm.Context().SetFunc("atob", func(ctx *qjs.This) (*qjs.Value, error) {
		if len(ctx.Args()) <= 1 {
			return nil, fmt.Errorf("missing argument")
		}
		src := ctx.Args()[1].String()
		res, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(src, "="))
		if err != nil {
			return nil, err
		}
		return ctx.Context().NewString(string(res)), nil
	})
	if _, err := vm.Eval("bundle.js", qjs.Code(string(bundleJS))); err != nil {
		panic(fmt.Errorf("postcss: initialize: bundle: %w", err))
	}
	if _, err := vm.Eval("wrapper.js", qjs.Code(string(wrapperJS))); err != nil {
		panic(fmt.Errorf("postcss: initialize: bundle: %w", err))
	}

	var mu sync.Mutex // the initialization and per-instance cost of each new instance far exceeds the time to do a single transform
	transform = func(css, browsers string) (string, error) {
		mu.Lock()
		defer mu.Unlock()

		cssObj := vm.Context().NewString(css)
		defer cssObj.Free()

		browsersObj := vm.Context().NewString(browsers)
		defer browsersObj.Free()

		res, err := vm.Context().Global().InvokeJS("transform", cssObj, browsersObj)
		if err != nil {
			return "", err
		}
		//defer res.Free() // this causes a wasm segfault for some reason (idk if the problem is in qjs or how I'm using it)

		res, err = res.Await()
		if err != nil {
			return "", err
		}
		defer res.Free()

		return res.String(), nil
	}

	// the first one takes a while
	if _, err := transform("html{}", "defaults"); err != nil {
		panic(fmt.Errorf("postcss: initialize: transform: %w", err))
	}
	slog.Info("postcss initialized")
}

func Transform(css, browsers string) (string, error) {
	if noop {
		return css, nil
	}
	return transform(css, browsers)
}
