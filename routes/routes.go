// Package routes contains handlers.
package routes

import (
	"crypto/sha1"
	"encoding/base32"
	"fmt"
	"iter"
	"net/http"
	"os"
)

func commonMiddleware(next http.Handler) http.Handler {
	// TODO: request ID, etc
	return next
}

func iterPrev[T any](seq iter.Seq[T]) iter.Seq2[T, T] {
	return func(yield func(T, T) bool) {
		var x T
		for y := range seq {
			if !yield(x, y) {
				return
			}
			x = y
		}
	}
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
