'use strict'

// Minimal facility map for the activity landing pages: just markers for the
// facilities offering the activity, no filtering (that's what the full /map is
// for). Progressive enhancement over the server-rendered facility list; a
// noscript fallback links to /map. Marker popups reuse the facility modal
// (facilitymodal.ts) via the data-facility-modal link.

import * as L from 'leaflet'
import {pinIcon} from './pin'
import {tileAttribution, tileSubdomains, tileURL} from './tiles'

interface Fac {
	name: string
	slug: string
	lat: number
	lng: number
}

const el = document.getElementById('activity-map')
const dataEl = document.getElementById('activity-map-data')
if (el && dataEl) {
	const facs = JSON.parse(dataEl.textContent!) as Fac[]

	// theme-aware tiles, matching the full map (navbar override, else the browser
	// preference)
	const darkQuery = window.matchMedia('(prefers-color-scheme: dark)')
	const effectiveDark = () => {
		const cs = document.documentElement.style.colorScheme
		if (cs === 'dark') return true
		if (cs === 'light') return false
		return darkQuery.matches
	}
	const map = L.map(el, {
		maxBounds: [[44.8, -76.6], [45.7, -75.0]],
		maxBoundsViscosity: 1,
		minZoom: 10,
		scrollWheelZoom: false, // don't hijack page scroll in an embedded widget
		dragging: !L.Browser.mobile, // on touch, let a single-finger drag scroll the page
	}).setView([45.4215, -75.6972], 11)

	const tiles = L.tileLayer(tileURL(effectiveDark()), {
		subdomains: tileSubdomains,
		maxZoom: 20,
		attribution: tileAttribution,
	}).addTo(map)
	darkQuery.addEventListener('change', () => tiles.setUrl(tileURL(effectiveDark())))
	window.addEventListener('themechange', () => tiles.setUrl(tileURL(effectiveDark())))

	const bounds: L.LatLngTuple[] = []
	for (const f of facs) {
		if (!f.lat && !f.lng) continue
		const marker = L.marker([f.lat, f.lng], {icon: pinIcon(false)}).addTo(map)
		marker.bindTooltip(f.name, {direction: 'top'})
		// open the facility schedule modal directly (no intermediate popup)
		marker.on('click', () => {
			const open = (window as unknown as {ottrecFacilityModal?: (slug: string) => void}).ottrecFacilityModal
			if (open) open(f.slug)
			else location.href = '/schedules/facility/' + f.slug
		})
		bounds.push([f.lat, f.lng])
	}
	if (bounds.length) map.fitBounds(bounds, {padding: [30, 30], maxZoom: 14})
}
