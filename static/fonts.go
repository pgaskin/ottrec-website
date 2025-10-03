//go:build ignore

package main

import (
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

func main() {
	// Asap (schedules)
	// - fixed width numbers
	// - readable while small
	font("fonts/asap.woff2", url.Values{
		"family": {"Asap:wdth,wght@75..100,100..900"},
		"text": {runes(
			33, 126, // ascii printable
			'\u2002', '\u201E', // spaces, smart punctuation
			'\u2022', '\u2022', // bullet
			'\u2026', '\u2026', // ellipsis
		)},
	})

	// Material Symbols Outlined
	font("fonts/symbols.woff2", url.Values{
		"family": {"Material Symbols Outlined:opsz,wght,FILL,GRAD@24,300,0,0"},
		"text": {string([]rune{
			'\ue8b6', // search
			'\ueb48', // pool
			'\ue566', // directions_run
			'\ue192', // schedule
			'\ue55f', // location_on
			'\ue152', // filter_list
			'\ueb57', // filter_list_off
			'\ue55b', // map
			'\ue87a', // explore
			'\ue538', // explore_nearby
			'\ue5cd', // close
			'\uf508', // close_small
			'\uf1be', // table_view
			'\ue8ec', // view_column
			'\ue5d2', // menu
			'\ue80d', // share
			'\ue89e', // open_in_new
		})},
	})

	// Source Serif 4 (headers)
	// - serif
	font("fonts/source_serif_4.woff2", url.Values{
		"family": {"Source Serif 4:ital,opsz,wght@0,8..60,200..900"},
		"text": {runes(
			33, 126, // ascii printable
		)},
	})

	// Source Sans 3 (body)
	font("fonts/source_sans_3.woff2", url.Values{
		"family": {"Source Sans 3:ital,wght@0,200..900"},
		"text": {runes(
			33, 126, // ascii printable
			'\u2002', '\u201E', // spaces, smart punctuation
			'\u2022', '\u2022', // bullet
			'\u2026', '\u2026', // ellipsis
		)},
	})

	// note: use https://wakamaifondue.com/ to see font details
}

func runes(ranges ...rune) string {
	if len(ranges)%2 != 0 {
		panic("odd number of arguments")
	}
	var b strings.Builder
	for r := range slices.Chunk(ranges, 2) {
		for c := r[0]; c <= r[1]; c++ {
			b.WriteRune(c)
		}
	}
	return b.String()
}

func font(name string, params url.Values) {
	css, err := css2(params)
	if err != nil {
		slog.Error("failed to fetch font css", "error", err)
		os.Exit(1)
	}

	if n := strings.Count(css, "@font-face"); n != 1 {
		slog.Error("expected a single variable font-face declaration, got "+strconv.Itoa(n), "css", css)
		os.Exit(1)
	}

	var u string
	if m := regexp.MustCompile(`url\((.+?)\)`).FindStringSubmatch(css); m == nil {
		slog.Error("failed to extract font url", "css", css)
		os.Exit(1)
	} else {
		u = m[1]
	}

	buf, err := woff2(u)
	if err != nil {
		slog.Error("failed to fetch font file", "error", err)
		os.Exit(1)
	}

	if err := os.WriteFile(name, buf, 0644); err != nil {
		slog.Error("failed to write font file", "error", err)
		os.Exit(1)
	}

	slog.Info("done", "name", name, "size", len(buf))
}

func css2(p url.Values) (string, error) {
	slog.Info("fetching font css", "params", p.Encode())

	req, err := http.NewRequest(http.MethodGet, "https://fonts.googleapis.com/css2?"+p.Encode(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0") // supports variable woff2

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("response status %d", resp.StatusCode)
	}

	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func woff2(u string) ([]byte, error) {
	slog.Info("fetching font woff2", "url", u)

	resp, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("response status %d", resp.StatusCode)
	}

	if ct := resp.Header.Get("Content-Type"); ct != "" {
		if mt, _, _ := mime.ParseMediaType(ct); mt != "font/woff2" {
			return nil, fmt.Errorf("unexpected mimetype %q", ct)
		}
	}

	return io.ReadAll(resp.Body)
}
