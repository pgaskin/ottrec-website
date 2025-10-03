// Package routes contains handlers.
package routes

import (
	"iter"
	"net/http"
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
