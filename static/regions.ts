'use strict'
import * as L from 'leaflet'
import {tileAttribution, tileSubdomains, tileURL} from './tiles'

// the JSON island embedded by the page (see templates/regions.go)
interface RegionsData {
	bounds: [number, number, number, number] // [south, west, north, east]
	overlayLight: string // the server-rendered region shading PNGs, per theme
	overlayDark: string
	regions: { name: string; class: string; lat: number; lng: number }[]
	facilities: { name: string; lat: number; lng: number }[]
	sectors: { westLng: number; southLat: number; eastLng: number }
}

const data = JSON.parse(document.getElementById('regions-data')!.textContent!) as RegionsData
const [south, west, north, east] = data.bounds

// a text label anchored (and centred) on a point. Leaflet positions the marker
// root with an inline transform, so the label is wrapped in an inner span that
// we centre with our own transform; iconSize [0,0] keeps Leaflet from setting a
// box or anchor margins.
const escapeText = (s: string) => { const d = document.createElement('div'); d.textContent = s; return d.innerHTML }
const textLabel = (className: string, name: string, lat: number, lng: number) =>
	L.marker([lat, lng], {
		interactive: false,
		keyboard: false,
		icon: L.divIcon({ className, html: '<span>' + escapeText(name) + '</span>', iconSize: [0, 0] }),
	})

const map = L.map('region-map', {
	maxBounds: [[south, west], [north, east]],
	maxBoundsViscosity: 1,
	minZoom: 10,
}).fitBounds([[south, west], [north, east]])

// light/dark tiles following the effective color scheme (the navbar theme
// toggle override, else the browser preference) — same as the facility map
const darkQuery = window.matchMedia('(prefers-color-scheme: dark)')
const effectiveDark = () => {
	const cs = document.documentElement.style.colorScheme
	if (cs === 'dark') return true
	if (cs === 'light') return false
	return darkQuery.matches
}
const tiles = L.tileLayer(tileURL(effectiveDark()), {
	subdomains: tileSubdomains,
	maxZoom: 20,
	attribution: tileAttribution + ' &copy; <a href="https://github.com/pgaskin">Patrick Gaskin</a>',
}).addTo(map)
darkQuery.addEventListener('change', () => tiles.setUrl(tileURL(effectiveDark())))
window.addEventListener('themechange', () => tiles.setUrl(tileURL(effectiveDark())))

// regions: the server-rendered shading PNG (colors baked opaque; we set the
// layer opacity so the basemap reads through), plus place-name labels. The PNG
// is rendered in Web Mercator over exactly these bounds, so it lines up.
const overlayURL = (dark: boolean) => (dark ? data.overlayDark : data.overlayLight)
const shading = L.imageOverlay(overlayURL(effectiveDark()), [[south, west], [north, east]], {
	opacity: 0.32,
	interactive: false,
})
darkQuery.addEventListener('change', () => shading.setUrl(overlayURL(effectiveDark())))
window.addEventListener('themechange', () => shading.setUrl(overlayURL(effectiveDark())))
const regionLabels: L.Marker[] = []
for (const r of data.regions) {
	// prominent places are always labelled; villages are minor and only shown
	// once zoomed in (CSS hides .region-label-minor until #region-map.zoomed)
	const minor = r.class === 'village'
	regionLabels.push(textLabel('region-label' + (minor ? ' region-label-minor' : ''), r.name, r.lat, r.lng))
}
const regionsLayer = L.layerGroup([shading, ...regionLabels])

// village labels would overlap badly when zoomed out, so reveal them only when
// zoomed in; a class on the container drives it from CSS
const syncZoom = () => map.getContainer().classList.toggle('zoomed', map.getZoom() >= 11)
map.on('zoomend', syncZoom)
syncZoom()

// sectors
const { westLng, southLat, eastLng } = data.sectors
const lineOpts: L.PolylineOptions = { color: '#1c1b1a', weight: 2, opacity: 0.7, dashArray: '6 5', interactive: false }
const sectorLines = [
	L.polyline([[south, westLng], [north, westLng]], lineOpts), // West | rest
	L.polyline([[southLat, westLng], [southLat, east]], lineOpts), // South | Central+East
	L.polyline([[southLat, eastLng], [north, eastLng]], lineOpts), // Central | East
]
const sectorLabels = [
	textLabel('sector-label', 'West', (south + north) / 2, (west + westLng) / 2),
	textLabel('sector-label', 'Central', (southLat + north) / 2, (westLng + eastLng) / 2),
	textLabel('sector-label', 'East', (southLat + north) / 2, (eastLng + east) / 2),
	textLabel('sector-label', 'South', (south + southLat) / 2, (westLng + east) / 2),
]
const sectorsLayer = L.layerGroup([...sectorLines, ...sectorLabels])

// facilities
const facilityDots: L.CircleMarker[] = []
for (const f of data.facilities) {
	facilityDots.push(
		L.circleMarker([f.lat, f.lng], {
			radius: 2.5,
			stroke: false,
			fillColor: '#1a4f8c', // flexoki blue-600
			fillOpacity: 1,
		}).bindTooltip(f.name, { direction: 'top' }),
	)
}
const facilitiesLayer = L.layerGroup(facilityDots)

regionsLayer.addTo(map)
sectorsLayer.addTo(map)
facilitiesLayer.addTo(map)

L.control.layers(undefined, {
	'Regions': regionsLayer,
	'Sectors': sectorsLayer,
	'Facilities': facilitiesLayer,
}, { collapsed: false }).addTo(map)
