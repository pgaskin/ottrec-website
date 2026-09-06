// Package static embeds the website's static assets, configures how each is
// compiled (CSS via lightningcss, TypeScript via esbuild), and groups them for
// serving. The asset pipeline and HTTP serving live in internal/asset.
package static

import (
	"bytes"
	"embed"
	"fmt"
	"io/fs"
	"iter"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"unicode"

	"github.com/ottrec/website/internal/asset"
	"github.com/ottrec/website/internal/esbuild"
	"github.com/ottrec/website/internal/npm"
	"github.com/pgaskin/go-gfsubsets"
	"github.com/pgaskin/go-hbsubset"
	"github.com/pgaskin/go-lightningcss"
	"github.com/pgaskin/go-woff2"
	"golang.org/x/text/unicode/rangetable"
)

//go:generate go run vendor.go
//go:generate go run fetch.go https://github.com/Omnibus-Type/Asap/raw/ca471c0ccf90a5c66155d4bcaa020859804ffd00/fonts/variable/Asap%5Bwdth%2Cwght%5D.ttf fonts/asap.ttf
//go:generate go run fetch.go https://github.com/Omnibus-Type/Asap/raw/ca471c0ccf90a5c66155d4bcaa020859804ffd00/fonts/variable/Asap-Italic%5Bwdth%2Cwght%5D.ttf fonts/asapit.ttf
//go:generate go run fetch.go https://github.com/adobe-fonts/source-serif/raw/5f220b17d27ed64873f22cde0dd593685387bd19/VAR/SourceSerif4Variable-Roman.ttf fonts/sourceserif4.ttf
//go:generate go run fetch.go https://github.com/adobe-fonts/source-serif/raw/5f220b17d27ed64873f22cde0dd593685387bd19/VAR/SourceSerif4Variable-Italic.ttf fonts/sourceserif4it.ttf
//go:generate go run fetch.go https://github.com/adobe-fonts/source-sans/raw/87b37a2daaed80fcb8e8ccb0085c4d72ddade12e/VF/SourceSans3VF-Upright.ttf fonts/sourcesans3.ttf
//go:generate go run fetch.go https://github.com/adobe-fonts/source-sans/raw/87b37a2daaed80fcb8e8ccb0085c4d72ddade12e/VF/SourceSans3VF-Italic.ttf fonts/sourcesans3it.ttf
//go:generate go run fetch.go https://github.com/google/fonts/raw/6183fc0d26361f6ddfd6f6b7a736e1467c6d8a43/ofl/roboto/Roboto%5Bwdth%2Cwght%5D.ttf fonts/roboto.ttf
//go:generate go run fetch.go https://github.com/google/fonts/raw/6183fc0d26361f6ddfd6f6b7a736e1467c6d8a43/ofl/roboto/Roboto-Italic%5Bwdth%2Cwght%5D.ttf fonts/robotoit.ttf
//go:generate go run fetch.go https://github.com/google/material-design-icons/raw/fe742c4072d4e3b8b899170109d9f710e89f082e/variablefont/MaterialSymbolsOutlined%5BFILL%2CGRAD%2Copsz%2Cwght%5D.ttf fonts/materialsymbolsoutlined.ttf

// Base is the path prefix under which assets are served.
const Base = "/static/"

//go:embed *
var res embed.FS

// assets holds every embedded static asset.
var assets = asset.NewSet(res, mimeType)

// vendored exposes the npm packages vendored into lib/ (as tar archives) as
// filesystems, and provides the esbuild plugin that bundles them.
var vendored = npm.NewStore(must(fs.Sub(res, "lib"))).
	Entry("leaflet", "dist/leaflet-src.esm.js") // force leaflet to use ESM instead of CJS

// localFiles lets bundled .ts entry modules import sibling source files (e.g.
// theme.ts importing ./theme-apply) from the embedded FS.
var localFiles = esbuild.LocalFS(res)

// pkgFS returns the filesystem of a vendored npm package.
func pkgFS(name string) fs.FS { return must(vendored.FS(name)) }

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// Assets iterates over every registered asset, for tooling that builds or
// inspects them outside of serving (e.g. cmd/ottrec-website-check).
func Assets() iter.Seq[*asset.Asset] { return assets.All() }

// Path returns the public, content-addressed path at which a is served.
func Path(a *asset.Asset) string {
	b, err := a.Built()
	if err != nil {
		panic(err)
	}
	return Base + b.Name
}

// InlineJS returns the compiled JavaScript of a, for inlining into a page (e.g.
// a pre-paint <script> in the head). The asset is not served separately.
func InlineJS(a *asset.Asset) string {
	return string(must(a.Built()).Data)
}

// InlineCSS returns the compiled stylesheet of a, for inlining into a page (e.g.
// to produce a self-contained HTML file). url() references still point at the
// content-addressed /static/ paths, so fonts and images load only when served.
func InlineCSS(a *asset.Asset) string {
	return string(must(a.Built()).Data)
}

// Handler builds and warms g, then returns it as an [http.Handler]. Compile
// failures are fatal: they are deterministic build bugs that should surface at
// startup.
func Handler(g *asset.Group) http.Handler {
	if err := g.Warm(); err != nil {
		panic(err)
	}
	return g
}

func mimeType(ext string) string {
	switch ext {
	case ".woff2":
		return "font/woff2"
	case ".css":
		return "text/css; charset=utf-8"
	case ".js":
		return "application/javascript; charset=utf-8"
	case ".svg":
		return "image/svg+xml"
	case ".ico":
		return "image/x-icon"
	case ".png":
		return "image/png"
	}
	return ""
}

var cssURL = regexp.MustCompile(`url\([^)]+\)`)

// compileCSS minifies a stylesheet with lightningcss and rewrites its url()
// references to the content-addressed names of the assets they point at.
func compileCSS(name string, data []byte, resolve func(string) (string, error)) ([]byte, string, error) {
	// expand icon(name) references first, so lightningcss only sees a plain
	// string token and the expansion also applies under DEBUG_POSTCSS_NOOP
	data, err := expandCSSIcons(data)
	if err != nil {
		return nil, "", err
	}
	if noop, _ := strconv.ParseBool(os.Getenv("DEBUG_POSTCSS_NOOP")); !noop {
		res, err := lightningcss.Transform(data, &lightningcss.Options{
			Filename: name,
			Minify:   true,
			Nesting:  true,
			Targets: lightningcss.Targets{
				Chrome:  lightningcss.Version(110, 0, 0),
				Safari:  lightningcss.Version(15, 0, 0),
				Firefox: lightningcss.Version(110, 0, 0),
			},
		})
		if err != nil {
			return nil, "", err
		}
		data = res.Code
	}
	var rerr error
	out := cssURL.ReplaceAllFunc(data, func(m []byte) []byte {
		ref := string(m[bytes.IndexByte(m, '(')+1 : len(m)-1])
		hashed, err := resolve(ref)
		if err != nil {
			rerr = err
			return m
		}
		return []byte("url(" + hashed + ")")
	})
	if rerr != nil {
		return nil, "", rerr
	}
	return out, ".css", nil
}

// compileTS bundles a TypeScript module to minified JavaScript, resolving
// imports of vendored npm packages.
func compileTS(name string, data []byte, _ func(string) (string, error)) ([]byte, string, error) {
	js, err := esbuild.Transform(name, string(data), vendored.Plugin(), localFiles)
	if err != nil {
		return nil, "", err
	}
	return []byte(js), ".js", nil
}

func webfont(u *unicode.RangeTable) asset.Option {
	return asset.Compile(func(name string, data []byte, _ func(string) (string, error)) ([]byte, string, error) {
		if noop, _ := strconv.ParseBool(os.Getenv("DEBUG_WEBFONT_NOOP")); noop {
			return data, ".ttf", nil
		}

		slog.Info("static: subsetting font", "name", name)
		sub, err := hbsubset.Subset(data, 0, &hbsubset.Options{
			UnicodeRanges:       u,
			PinAllAxesToDefault: true,
			AxisRanges: map[hbsubset.Tag]hbsubset.AxisRange{
				hbsubset.MakeTag("wght"): {Min: 100, Max: 900, Default: 400},
			},
			LayoutFeatures: []hbsubset.Tag{
				hbsubset.MakeTag("kern"),
				hbsubset.MakeTag("mkmk"),
				hbsubset.MakeTag("size"),
				hbsubset.MakeTag("liga"),
			},
			NameIDs: []hbsubset.NameID{
				hbsubset.NameUniqueID,
				hbsubset.NameCopyright,
				hbsubset.NameFontFamily,
				hbsubset.NameFontSubfamily,
				hbsubset.NameTypographicFamily,
				hbsubset.NameTypographicSubfamily,
				hbsubset.NameFullName,
				hbsubset.NameMacFullName,
				hbsubset.NamePostscriptName,
				hbsubset.NameManufacturer,
				hbsubset.NameDescription,
				hbsubset.NameVariationsPSPrefix,
			},
			LayoutScripts: []hbsubset.Tag{
				hbsubset.MakeTag("latn"), // latin
			},
			NameLanguages: []uint32{
				1033, // english
			},
		})
		if err != nil {
			return nil, "", fmt.Errorf("subset: %w", err)
		}

		if os.Getenv("DEBUG_WEBFONT_NOOP") == "2" {
			slog.Info("static: finished font", "name", name, slog.Group("size", "sfnt", len(data), "sub", len(sub)))
			return sub, ".ttf", nil
		}

		slog.Info("static: compressing font", "name", name)
		enc, err := woff2.Encode(sub, nil)
		if err != nil {
			return nil, "", fmt.Errorf("woff2: %w", err)
		}

		slog.Info("static: finished font", "name", name, slog.Group("size", "sfnt", len(data), "sub", len(sub), "woff2", len(enc)))
		return enc, ".woff2", nil
	})
}

func iconfont(a map[hbsubset.Tag]hbsubset.AxisRange, p map[hbsubset.Tag]float32) asset.Option {
	return asset.Compile(func(name string, data []byte, _ func(string) (string, error)) ([]byte, string, error) {
		if noop, _ := strconv.ParseBool(os.Getenv("DEBUG_WEBFONT_NOOP")); noop {
			return data, ".ttf", nil
		}

		r, err := subsetRunes()
		if err != nil {
			return nil, "", err
		}

		slog.Info("static: subsetting font", "name", name)
		sub, err := hbsubset.Subset(data, 0, &hbsubset.Options{
			Unicodes:            r,
			PinAllAxesToDefault: true,
			AxisRanges:          a,
			PinAxes:             p,
			DropTables: []hbsubset.Tag{
				hbsubset.MakeTag("GSUB"),
			},
			NameIDs: []hbsubset.NameID{
				hbsubset.NameUniqueID,
				hbsubset.NameCopyright,
				hbsubset.NameFontFamily,
				hbsubset.NameFontSubfamily,
				hbsubset.NameTypographicFamily,
				hbsubset.NameTypographicSubfamily,
				hbsubset.NameFullName,
				hbsubset.NameMacFullName,
				hbsubset.NamePostscriptName,
				hbsubset.NameManufacturer,
				hbsubset.NameDescription,
				hbsubset.NameVariationsPSPrefix,
			},
			NameLanguages: []uint32{
				1033, // english
			},
		})
		if err != nil {
			return nil, "", fmt.Errorf("subset: %w", err)
		}

		if os.Getenv("DEBUG_WEBFONT_NOOP") == "2" {
			slog.Info("static: finished font", "name", name, slog.Group("size", "sfnt", len(data), "sub", len(sub)))
			return sub, ".ttf", nil
		}

		slog.Info("static: compressing font", "name", name)
		enc, err := woff2.Encode(sub, nil)
		if err != nil {
			return nil, "", fmt.Errorf("woff2: %w", err)
		}

		slog.Info("static: finished font", "name", name, slog.Group("size", "sfnt", len(data), "sub", len(sub), "woff2", len(enc)))
		return enc, ".woff2", nil
	})
}

// Assets and the groups they are served in. Top-level stylesheets and scripts
// are compiled; vendored files under a subdirectory are served as-is.
var (
	css = asset.Compile(compileCSS)
	ts  = asset.Compile(compileTS)

	scheduleFontSubset = rangetable.Merge(
		&unicode.RangeTable{
			R16: []unicode.Range16{
				{Stride: 1, Lo: 33, Hi: 126},            // ascii printable
				{Stride: 1, Lo: 0xC0, Hi: 0xFF},         // latin-1 accented letters (é, à, ç, ô, ...)
				{Stride: 1, Lo: 'Œ', Hi: 'œ'},           // Œ œ
				{Stride: 1, Lo: 'Ÿ', Hi: 'Ÿ'},           // Ÿ
				{Stride: 1, Lo: '\u2002', Hi: '\u201E'}, // spaces, smart punctuation
				{Stride: 1, Lo: '\u2022', Hi: '\u2022'}, // bullet
				{Stride: 1, Lo: '\u2026', Hi: '\u2026'}, // ellipsis
			},
		},
	)

	// the Material Symbols icons subset into the icon font, by name (see
	// icons.go); referenced as icon(name) in stylesheets and via Icon in
	// templates, both of which fail loudly on icons not listed here
	subsetIcons = []string{
		"search",
		"pool",
		"directions_run",
		"schedule",
		"location_on",
		"filter_list",
		"filter_list_off",
		"map",
		"explore",
		"explore_nearby",
		"close",
		"close_small",
		"table_view",
		"view_column",
		"menu",
		"share",
		"open_in_new",
		"overview",
		"ice_skating",
		"water",
		"sports_and_outdoors",
		"fitness_center",
		"more_vert",
		"more_horiz",
		"history",
		"trending_up",
		"code_blocks",
		"calendar_month",
		"sports_hockey",
		"sports_basketball",
		"sports_volleyball",
		"badminton",
		"pickleball",
		"sports_tennis",
		"database",
		"star",
		"warning",
		"info",
		"lightbulb",
		"arrow_drop_down",
		"expand_more",
		"expand_less",
		"arrow_right_alt",
		"light_mode",
		"dark_mode",
		"brightness_auto",
	}

	textFontSubset = rangetable.Merge(
		gfsubsets.Latin,
		&unicode.RangeTable{
			R16: []unicode.Range16{
				{Stride: 1, Lo: 0xC0, Hi: 0xFF},         // latin-1 accented letters (é, à, ç, ô, ...)
				{Stride: 1, Lo: 'Œ', Hi: 'œ'},           // Œ œ
				{Stride: 1, Lo: 'Ÿ', Hi: 'Ÿ'},           // Ÿ
				{Stride: 1, Lo: '\u2002', Hi: '\u201E'}, // spaces, smart punctuation
				{Stride: 1, Lo: '\u2022', Hi: '\u2022'}, // bullet
				{Stride: 1, Lo: '\u2026', Hi: '\u2026'}, // ellipsis
			},
		},
	)

	AsapWOFF2                    = assets.Register("fonts/asap.ttf", webfont(scheduleFontSubset))
	AsapItalicWOFF2              = assets.Register("fonts/asapit.ttf", webfont(scheduleFontSubset))
	SourceSans3WOFF2             = assets.Register("fonts/sourcesans3.ttf", webfont(textFontSubset))
	SourceSans3ItalicWOFF2       = assets.Register("fonts/sourcesans3it.ttf", webfont(textFontSubset))
	SourceSerif4WOFF2            = assets.Register("fonts/sourceserif4.ttf", webfont(textFontSubset))
	SourceSerif4ItalicWOFF2      = assets.Register("fonts/sourceserif4it.ttf", webfont(textFontSubset))
	RobotoWOFF2                  = assets.Register("fonts/roboto.ttf", webfont(textFontSubset))
	RobotoItalicWOFF2            = assets.Register("fonts/robotoit.ttf", webfont(textFontSubset))
	MaterialSymbolsOutlinedWOFF2 = assets.Register("fonts/materialsymbolsoutlined.ttf", iconfont(
		map[hbsubset.Tag]hbsubset.AxisRange{
			hbsubset.MakeTag("FILL"): {Min: 0, Max: 1, Default: 0},
		},
		map[hbsubset.Tag]float32{
			hbsubset.MakeTag("opsz"): 24,
			hbsubset.MakeTag("wght"): 300,
			hbsubset.MakeTag("GRAD"): 0,
		},
	))

	// leaflet's JS is imported and bundled by map.ts; only its stylesheet is
	// served, read straight from the vendored package. leaflet-theme.css is the
	// shared flexoki restyling layered on it by the map and regions pages.
	LeafletCSS      = assets.RegisterFS("lib/leaflet.css", pkgFS("leaflet"), "dist/leaflet.css")
	LeafletThemeCSS = assets.Register("leaflet-theme.css", css)

	FaviconSVG        = assets.Register("favicon.svg")
	FaviconICO        = assets.Register("favicon.ico")
	AppleTouchIconPNG = assets.Register("apple-touch-icon.png")
	SocialCardPNG     = assets.Register("social-card.png") // og:image / twitter:image; see social-card.py

	DataCSS         = assets.Register("data.css", css)
	WebsiteCSS      = assets.Register("website.css", css)
	FacilityCSS     = assets.Register("facility.css", css)
	MapCSS          = assets.Register("map.css", css)
	MapJS           = assets.Register("map.ts", ts)
	TodayCSS        = assets.Register("today.css", css)
	TodayJS         = assets.Register("today.ts", ts)
	SchedulesCSS    = assets.Register("schedules.css", css)
	SchedulesJS     = assets.Register("schedules.ts", ts)
	SearchCSS       = assets.Register("search.css", css)
	OttrecqlCSS     = assets.Register("ottrecql.css", css)
	TimemachineCSS  = assets.Register("timemachine.css", css)
	TimemachineJS   = assets.Register("timemachine.ts", ts)
	AboutCSS        = assets.Register("about.css", css)
	AboutContentCSS = assets.Register("about-content.css", css)
	RegionsCSS      = assets.Register("regions.css", css)
	RegionsJS       = assets.Register("regions.ts", ts)
	HomeCSS         = assets.Register("home.css", css)
	ActivityCSS     = assets.Register("activity.css", css)
	ActivityMapJS   = assets.Register("activitymap.ts", ts)
	ActivityTodayJS = assets.Register("activitytoday.ts", ts)
	RelTimeJS       = assets.Register("reltime.ts", ts)
	ThemeJS         = assets.Register("theme.ts", ts)
	StarredJS       = assets.Register("starred.ts", ts)
	FacilityModalJS = assets.Register("facilitymodal.ts", ts)

	// advanced search editor component
	OttrecqlEditorJS = assets.Register("ottrecql-editor/index.ts", ts)

	// compiled and inlined into the head (see InlineJS), not served on its own
	ThemeApplyJS = assets.Register("theme-apply.ts", ts)
)

var Website = assets.
	NewGroup(Base,
		WebsiteCSS,
		FacilityCSS,
		MapCSS,
		MapJS,
		TodayCSS,
		TodayJS,
		SchedulesCSS,
		SchedulesJS,
		SearchCSS,
		OttrecqlCSS,
		OttrecqlEditorJS,
		TimemachineCSS,
		TimemachineJS,
		AboutCSS,
		AboutContentCSS,
		RegionsCSS,
		RegionsJS,
		HomeCSS,
		ActivityCSS,
		ActivityMapJS,
		ActivityTodayJS,
		RelTimeJS,
		ThemeJS,
		StarredJS,
		FacilityModalJS,
		SourceSans3WOFF2,
		SourceSans3ItalicWOFF2,
		SourceSerif4WOFF2,
		SourceSerif4ItalicWOFF2,
		MaterialSymbolsOutlinedWOFF2,
		AsapWOFF2,
		AsapItalicWOFF2,
		LeafletCSS,
		LeafletThemeCSS,
		FaviconSVG,
		FaviconICO,
		AppleTouchIconPNG,
		SocialCardPNG,
	).
	Cache("public, max-age=86400").
	Alias("/favicon.ico", FaviconICO)

var Webarchive = assets.
	NewGroup(Base,
		RobotoWOFF2,
		RobotoItalicWOFF2,
		MaterialSymbolsOutlinedWOFF2,
	).
	Cache("public, max-age=86400")

var Data = assets.
	NewGroup(Base,
		// the data site now shares the main site's chrome (flexoki theme,
		// navbar, theme toggle, icons); data.css layers on website.css
		WebsiteCSS,
		DataCSS,
		ThemeJS,
		SourceSans3WOFF2,
		SourceSans3ItalicWOFF2,
		SourceSerif4WOFF2,
		SourceSerif4ItalicWOFF2,
		MaterialSymbolsOutlinedWOFF2,
		AsapWOFF2,
		AsapItalicWOFF2,
		FaviconSVG,
		FaviconICO,
		AppleTouchIconPNG,
	).
	Cache("public, max-age=86400").
	Alias("/favicon.ico", FaviconICO)
