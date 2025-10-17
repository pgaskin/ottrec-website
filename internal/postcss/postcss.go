// Package postcss embeds PostCSS with postcss-preset-env.
package postcss

import (
	_ "embed"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"

	"github.com/dop251/goja"
)

// I do the frontend CSS this way since we're primarly server-rendered, and this
// means we don't need a complex frontend build process.

//go:generate go run bundle.go
//go:embed bundle.js
var bundleJS []byte

var wrapperJS = `
	const { postcss, postcssPresetEnv } = bundle
	
	function transform(css, browsers) {
		const proc = postcss([
			postcssPresetEnv({
				browsers: [browsers],
			}),
		])
		return proc.process(css).then(res => res.css)
	}
`

var noop, _ = strconv.ParseBool(os.Getenv("DEBUG_POSTCSS_NOOP"))

var transform func(css, browsers string) (string, error)

func init() {
	if noop {
		return
	}
	slog.Info("initializing postcss")
	vm := goja.New()
	if err := initialize(vm); err != nil {
		panic(fmt.Errorf("postcss: initialize: %w", err))
	}
	fn, ok := goja.AssertFunction(vm.GlobalObject().Get("transform"))
	if !ok {
		panic(fmt.Errorf("postcss: initialize: transform is not a function"))
	}
	var mu sync.Mutex // the initialization and per-instance cost of each new instance far exceeds the time to do a single transform
	transform = func(css, browsers string) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		res, err := fn(nil, vm.ToValue(css), vm.ToValue(browsers))
		if err != nil {
			return "", fullException(err)
		}
		promise, ok := res.Export().(*goja.Promise)
		if !ok {
			panic("transform did not return a promise")
		}
		switch promise.State() {
		case goja.PromiseStateFulfilled:
			return promise.Result().String(), nil
		case goja.PromiseStateRejected:
			return "", errors.New(promise.Result().String())
		default:
			panic("promise not fulfilled") // this should never happen since we don't have an event loop
		}
	}
	// the first one takes a while
	if _, err := transform("", "defaults"); err != nil {
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

func initialize(vm *goja.Runtime) error {
	if err := errors.Join(
		vm.GlobalObject().Set("btoa", func(call goja.FunctionCall) goja.Value {
			return vm.ToValue(base64.RawStdEncoding.EncodeToString([]byte(call.Arguments[0].String())))
		}),
		vm.GlobalObject().Set("atob", func(call goja.FunctionCall) (goja.Value, error) {
			str, err := base64.RawStdEncoding.DecodeString(call.Arguments[0].String())
			return vm.ToValue(string(str)), err
		}),
	); err != nil {
		return fmt.Errorf("polyfills: %w", fullException(err))
	}
	if _, err := vm.RunScript("bundle.js", string(bundleJS)); err != nil {
		return fmt.Errorf("bundle: %w", fullException(err))
	}
	if _, err := vm.RunScript("wrapper.js", wrapperJS); err != nil {
		return fmt.Errorf("wrapper: %w", fullException(err))
	}
	return nil
}

func fullException(err error) error {
	if xx, ok := err.(*goja.Exception); ok {
		return errors.New(xx.String())
	}
	return err
}
