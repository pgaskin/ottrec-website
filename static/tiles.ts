// Basemap tile configuration, shared by every Leaflet map on the site. The
// server emits it as a JSON island alongside each map script (see the
// --map-tiles-* flags).

const tiles = JSON.parse(document.getElementById('map-tiles')!.textContent!) as {
	light: string
	dark: string
	attribution: string
	subdomains: string
}

/** tileURL returns the tile url template for the given color scheme. */
export const tileURL = (dark: boolean) => dark ? tiles.dark : tiles.light

/** tileAttribution credits the tile source and its map data (not the site). */
export const tileAttribution = tiles.attribution

/** tileSubdomains is the set of {s} substitutions for the tile url. */
export const tileSubdomains = tiles.subdomains
