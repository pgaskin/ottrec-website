// Package routes contains handlers.
package routes

import (
	"net/http"
)

func commonMiddleware(next http.Handler) http.Handler {
	// TODO: request ID, etc
	return next
}
