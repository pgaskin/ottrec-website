'use strict'
import * as L from 'leaflet'
import {normalizeText} from './text'
import {pinHTML, pinIcon} from './pin'
import {tileAttribution, tileSubdomains, tileURL} from './tiles'

// the shared starred facility store (starred.ts) is a global; see ottrecstarred.d.ts

// error banner shown at the bottom of the page if a js error occurs

function showError(msg: string) {
	let banner = document.getElementById('js-error-banner')
	if (!banner) {
		banner = document.createElement('div')
		banner.id = 'js-error-banner'
		const text = document.createElement('span')
		const close = document.createElement('button')
		close.type = 'button'
		close.textContent = '×'
		close.title = 'Dismiss'
		close.addEventListener('click', () => banner!.remove())
		banner.append(text, close)
		document.body.append(banner)
	}
	banner.firstChild!.textContent = 'Something went wrong: ' + msg
}
window.addEventListener('error', (ev) => showError(ev.message || 'unknown error'))
window.addEventListener('unhandledrejection', (ev) => showError((ev.reason && ev.reason.message) || String(ev.reason)))

// transient toast over the map (e.g., when stale url filters are reset)

function showToast(msg: string) {
	document.getElementById('map-toast')?.remove()
	const toast = document.createElement('div')
	toast.id = 'map-toast'
	toast.textContent = msg
	toast.addEventListener('click', () => toast.remove())
	document.body.append(toast)
	setTimeout(() => toast.remove(), 8000)
}

/**
 * A filter selects facilities by day, time slot, category, activity, and name.
 *
 *   days        Set of enabled weekday indices (0 = Sunday); empty = all
 *   slots       Set of enabled time slot indices (into .slots); empty = all
 *   categories  Set of enabled category indices (into .categories); empty = all
 *   activities  Set of enabled activity indices (into .activities); empty = all
 *   name        substring to match against the facility name, case-insensitive
 *   starredOnly only match starred facilities
 *   hideEmpty   hide facilities with no drop-in activity data at all
 */
interface Filter {
	days: Set<number>
	slots: Set<number>
	categories: Set<number>
	activities: Set<number>
	name: string
	starredOnly: boolean
	hideEmpty: boolean
}

// the JSON data island embedded in the page
interface DataJSON {
	updated: string
	days: string[]
	slots: string[]      // stable 24h keys used in the URL filter state
	slotLabels: string[] // am/pm labels shown in the UI
	categories: string[]
	activities: string[]
	activityCategories: number[]
	facilities: {
		slug: string
		name: string
		address?: string
		lat?: number
		lng?: number
		mask?: string
	}[]
}

interface Facility {
	index: number
	slug: string
	name: string
	address: string
	lat: number
	lng: number
}

// the internal selection representation of a Filter
interface Query {
	name: string
	activities: Set<number> | null
	cats: number
	days: Set<number>
	mask: Uint8Array
	timeFiltered: boolean
	hideEmpty: boolean
	starred: Set<number> | null // starred facility indices, if filtering by starred
}

/**
 * FacilityData wraps the JSON data island embedded in the page and answers
 * queries about facilities, activities, and availability.
 */
class FacilityData {
	updated: string
	days: string[]
	slots: string[]      // stable 24h keys used in the URL filter state
	slotLabels: string[] // am/pm labels shown in the UI
	categories: string[]
	activities: string[]
	facilities: Facility[]

	// per-(facility, activity) availability entries
	#entryActivity: Uint16Array // activity index of entry e
	#entryMask: Uint8Array      // 7 bytes per entry; byte d bit s = available on weekday d during slot s
	#entryStart: Uint32Array    // facility i owns entries entryStart[i] ... entryStart[i+1]-1
	#activityCats: Uint16Array  // category bitmask per activity
	#nameLower: string[]        // normalized facility names for matching (lowercased, diacritics stripped)

	constructor(json: DataJSON) {
		this.updated = json.updated
		this.days = json.days
		this.slots = json.slots
		this.slotLabels = json.slotLabels
		this.categories = json.categories
		this.activities = json.activities
		this.#activityCats = Uint16Array.from(json.activityCategories)
		this.facilities = json.facilities.map((f, i) => ({
			index: i,
			slug: f.slug,
			name: f.name,
			address: f.address || '',
			lat: f.lat || 0,
			lng: f.lng || 0,
		}))
		this.#nameLower = this.facilities.map((f) => normalizeText(f.name))

		const packed = json.facilities.map((f) => atob(f.mask || ''))
		const total = packed.reduce((n, p) => n + p.length / 9, 0)
		this.#entryActivity = new Uint16Array(total)
		this.#entryMask = new Uint8Array(total * 7)
		this.#entryStart = new Uint32Array(packed.length + 1)
		let e = 0
		packed.forEach((p, i) => {
			this.#entryStart[i] = e
			for (let o = 0; o + 9 <= p.length; o += 9, e++) {
				this.#entryActivity[e] = p.charCodeAt(o) | (p.charCodeAt(o + 1) << 8)
				for (let k = 0; k < 7; k++)
					this.#entryMask[e * 7 + k] = p.charCodeAt(o + 2 + k)
			}
		})
		this.#entryStart[packed.length] = e
	}

	// #prepare converts a filter into the internal selection representation.
	#prepare(filter: Filter): Query {
		const slotBits = filter.slots.size
			? [...filter.slots].reduce((m, s) => m | (1 << s), 0)
			: (1 << this.slots.length) - 1
		const days = filter.days.size ? filter.days : new Set(this.days.map((_, i) => i))
		const mask = new Uint8Array(7)
		for (const d of days) mask[d] = slotBits
		return {
			name: normalizeText(filter.name.trim()),
			activities: filter.activities.size ? filter.activities : null,
			cats: [...filter.categories].reduce((m, c) => m | (1 << c), 0),
			days,
			mask,
			timeFiltered: filter.slots.size > 0 || filter.days.size > 0,
			hideEmpty: filter.hideEmpty,
			starred: filter.starredOnly
				? new Set(this.facilities.filter((f) => ottrecStarred.has(f.slug)).map((f) => f.index))
				: null,
		}
	}

	// #activityAllowed reports whether activity a passes the activity and
	// category parts of the filter.
	#activityAllowed(a: number, q: Query): boolean {
		if (q.activities && !q.activities.has(a)) return false
		if (q.cats && !(this.#activityCats[a]! & q.cats)) return false
		return true
	}

	#entryTimeMatches(e: number, q: Query): boolean {
		let any = false
		for (let k = 0; k < 7; k++) {
			const m = this.#entryMask[e * 7 + k]!
			if (m & q.mask[k]!) return true
			if (m) any = true
		}
		// entries with no parsed times match as long as no time filter is active
		return !any && !q.timeFiltered
	}

	// #facilityPrefilter applies the parts of the query that gate a facility
	// independently of its activity entries: the starred-only and name filters.
	#facilityPrefilter(i: number, q: Query): boolean {
		if (q.starred && !q.starred.has(i)) return false
		if (q.name && !this.#nameLower[i]!.includes(q.name)) return false
		return true
	}

	#facilityMatches(i: number, q: Query): boolean {
		if (!this.#facilityPrefilter(i, q)) return false
		const start = this.#entryStart[i]!, end = this.#entryStart[i + 1]!
		if (start === end) // no activity data at all; show unless hidden or filtering by activity, category, or time
			return !q.hideEmpty && !q.activities && !q.cats && !q.timeFiltered
		for (let e = start; e < end; e++) {
			if (!this.#activityAllowed(this.#entryActivity[e]!, q)) continue
			if (this.#entryTimeMatches(e, q)) return true
		}
		return false
	}

	// matchingFacilities returns the indices of facilities matching the filter.
	matchingFacilities(filter: Filter): number[] {
		const q = this.#prepare(filter)
		const out = []
		for (let i = 0; i < this.facilities.length; i++)
			if (this.#facilityMatches(i, q)) out.push(i)
		return out
	}

	// activityInCategories reports whether activity a is in any of the given
	// categories (an empty set matches all).
	activityInCategories(a: number, categories: Set<number>): boolean {
		if (!categories.size) return true
		for (const c of categories)
			if (this.#activityCats[a]! & (1 << c)) return true
		return false
	}

	// activityCounts returns, for each activity, the number of facilities which
	// would match the filter if only that activity were selected (i.e., the
	// activity part of the filter itself is ignored, but the categories are
	// still applied).
	activityCounts(filter: Filter): Uint32Array {
		const q = this.#prepare(filter)
		const counts = new Uint32Array(this.activities.length)
		for (let i = 0; i < this.facilities.length; i++) {
			if (!this.#facilityPrefilter(i, q)) continue
			for (let e = this.#entryStart[i]!; e < this.#entryStart[i + 1]!; e++) {
				const a = this.#entryActivity[e]!
				if (q.cats && !(this.#activityCats[a]! & q.cats)) continue
				if (this.#entryTimeMatches(e, q)) counts[a]!++
			}
		}
		return counts
	}

	// categoryCounts returns, for each category, the number of facilities which
	// would match the filter if only that category were selected (i.e., the
	// category and activity parts of the filter are ignored).
	categoryCounts(filter: Filter): Uint32Array {
		const q = this.#prepare(filter)
		const counts = new Uint32Array(this.categories.length)
		for (let i = 0; i < this.facilities.length; i++) {
			if (!this.#facilityPrefilter(i, q)) continue
			let bits = 0
			for (let e = this.#entryStart[i]!; e < this.#entryStart[i + 1]!; e++)
				if (this.#entryTimeMatches(e, q))
					bits |= this.#activityCats[this.#entryActivity[e]!]!
			for (let c = 0; c < this.categories.length; c++)
				if (bits & (1 << c)) counts[c]!++
		}
		return counts
	}

	// slotCounts returns, for each time slot, the number of facilities which
	// would match the filter if only that slot were selected (i.e., the slot
	// part of the filter itself is ignored).
	slotCounts(filter: Filter): Uint32Array {
		const q = this.#prepare(filter)
		const counts = new Uint32Array(this.slots.length)
		for (let i = 0; i < this.facilities.length; i++) {
			if (!this.#facilityPrefilter(i, q)) continue
			let slotBits = 0
			for (let e = this.#entryStart[i]!; e < this.#entryStart[i + 1]!; e++) {
				if (!this.#activityAllowed(this.#entryActivity[e]!, q)) continue
				for (const d of q.days) slotBits |= this.#entryMask[e * 7 + d]!
			}
			for (let s = 0; s < this.slots.length; s++)
				if (slotBits & (1 << s)) counts[s]!++
		}
		return counts
	}
}

const data = new FacilityData(JSON.parse(document.getElementById('map-data')!.textContent!))

const filter: Filter = {
	days: new Set(),
	slots: new Set(),
	categories: new Set(),
	activities: new Set(),
	name: '',
	starredOnly: false,
	hideEmpty: false,
}
let order = 'alpha'
let visible: number[] = []

const listEl = document.getElementById('fac-list')!
const searchEl = document.getElementById('fac-search') as HTMLInputElement
const mobileQuery = window.matchMedia('(max-width: 900px)')
const darkQuery = window.matchMedia('(prefers-color-scheme: dark)')
const facCountEl = document.getElementById('fac-count')!
const sheetToggleEl = document.getElementById('fac-sheet-toggle')!
const filterChipsEl = document.getElementById('filter-chips')!

// the chips live in the mobile filter bar on narrow screens and overlaid on
// the map on wide ones
function placeChips() {
	if (mobileQuery.matches) document.querySelector('.map-filterbar')!.append(filterChipsEl)
	else document.getElementById('map-chips')!.append(filterChipsEl)
}
mobileQuery.addEventListener('change', () => {
	placeChips()
	// the detail and filter panels are mobile-only overlays; when growing to a
	// wide layout they no longer apply (filters become a sidebar), so dismiss
	// them and let scheduleOverlaySync unwind any history entry we pushed for them
	if (!mobileQuery.matches) {
		document.body.classList.remove('detail-open')
		document.body.classList.remove('filters-open')
	}
	scheduleOverlaySync()
})
placeChips()
const activitiesFilteredEl = document.getElementById('filter-activities-filtered')!
const starredSectionEl = document.getElementById('filter-starred')!
const starredOnlyEl = document.getElementById('filter-starred-only') as HTMLInputElement
const hideEmptyEl = document.getElementById('filter-hide-empty') as HTMLInputElement

// the starred-only filter is only shown once there's something to filter by
function syncStarredFilter() {
	const any = ottrecStarred.count() > 0
	starredSectionEl.hidden = !any
	if (!any) filter.starredOnly = false
}

// map

// keep the view within the greater Ottawa area (all facilities are here);
// hardcoded SW/NE corners, generous enough not to clip outlying facilities
const MAX_BOUNDS: L.LatLngBoundsExpression = [[44.8, -76.6], [45.7, -75.0]]
const map = L.map('map', {
	maxBounds: MAX_BOUNDS,
	maxBoundsViscosity: 1, // solid edges; can't drag past them
	minZoom: 10,
}).setView([45.4215, -75.6972], 11)

// light/dark tiles following the effective color scheme (the navbar toggle
// override from theme.js, else the browser preference)
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

// Leaflet suppresses tooltips while the map is dragging, but then fires a
// tooltipopen for every pin the cursor passed over in a burst right after
// dragend (with no matching mouseover/mouseout), leaving several stuck open. To
// guard against this, only allow a tooltip to open for the pin the cursor is
// genuinely over: hoveredIndex tracks that from real mouseover/mouseout events.
let dragging = false
let hoveredIndex: number | null = null
map.on('dragstart', () => {
	dragging = true
	for (const m of markers.values()) m.closeTooltip()
})
map.on('dragend', () => { dragging = false })

const markers = new Map<number, L.Marker>()   // facility index -> marker
const pinIcons = new Map<number, L.DivIcon>()  // facility index -> pin icon
const popupCache = new Map<string, Promise<string>>() // slug -> popup content
for (const f of data.facilities) {
	if (!f.lat && !f.lng) continue
	const icon = pinIcon(ottrecStarred.has(f.slug))
	const marker = L.marker([f.lat, f.lng], {icon})
	// register this before bindTooltip so hoveredIndex is current by the time
	// Leaflet's own mouseover handler opens the tooltip and fires tooltipopen
	// (listeners run in registration order) otherwise the tooltipopen guard
	// below sees a stale hoveredIndex and suppresses every genuine hover
	marker.on('mouseover', () => { hoveredIndex = f.index; setHighlight(f.index, true) })
	marker.bindTooltip(f.name, {direction: 'top'})
	marker.on('tooltipopen', () => {
		// suppress tooltips opened while dragging or replayed afterwards for pins
		// the cursor isn't actually over (see hoveredIndex above)
		if (dragging || hoveredIndex !== f.index) marker.closeTooltip()
	})
	marker.bindPopup('<div class="fac-popup-loading">Loading…</div>', {
		minWidth: 400,
		maxWidth: 600,
		maxHeight: 520,
		autoPan: false, // don't move the map on open; only list clicks pan (focusFacility)
	})
	marker.on('popupopen', (ev: L.PopupEvent) => {
		const popup = ev.popup
		if (mobileQuery.matches) {
			// on mobile, the details are shown in a panel over the map instead
			// of an anchored popup, so they can't get cut off
			map.closePopup(popup)
			// a tap synthesizes a hover that never gets a mouseout, so close the
			// tooltip and clear the highlight here or they'd stick behind the panel
			marker.closeTooltip()
			setHighlight(f.index, false)
			openDetail(f)
		} else {
			// let the popup grow to fit the schedule tables if there's room
			const size = map.getSize()
			popup.options.maxWidth = Math.max(400, Math.min(880, size.x - 120))
			popup.options.maxHeight = Math.max(320, Math.min(680, size.y - 120))
			loadPopup(f, popup)
		}
	})
	marker.on('mouseout', () => {
		if (hoveredIndex === f.index) hoveredIndex = null
		setHighlight(f.index, false)
	})
	markers.set(f.index, marker)
	pinIcons.set(f.index, icon)
}

// fetchFacility fetches (and caches) the facility popup content.
function fetchFacility(f: Facility): Promise<string> {
	if (!popupCache.has(f.slug))
		popupCache.set(f.slug, fetch('/map/facility/' + encodeURIComponent(f.slug)).then((resp) => {
			if (!resp.ok) throw new Error('status ' + resp.status)
			return resp.text()
		}))
	return popupCache.get(f.slug)!
}

// renderFacility fetches the facility content and hands it to setContent, then
// wires up its star button. Both the anchored popup and the mobile detail panel
// go through here; isStale lets each skip the write if a newer open has
// superseded it (the panel is reused across facilities, and a popup may close
// mid-fetch). On error the cache entry is dropped so a reopen refetches.
async function renderFacility(f: Facility, setContent: (html: string) => void, isStale: () => boolean) {
	let html
	try {
		html = await fetchFacility(f)
	} catch {
		popupCache.delete(f.slug)
		html = '<div class="fac-popup-error">Failed to load facility info.</div>'
	}
	if (isStale()) return
	setContent(html)
	ottrecStarred.sync() // wire up the star button in the inserted content
}

// ensurePopupInView shifts an open popup by the minimum needed to bring it fully
// into the map viewport, without moving the map (autoPan is disabled and only
// focusFacility pans). It does nothing when the popup already fits, so opening a
// marker that's comfortably on screen doesn't move it.
//
// Leaflet anchors the container with inline `left`/`bottom`, so we shift it with
// margins (which Leaflet never touches, so they survive map moves): margin-left
// moves it horizontally, and margin-bottom vertically — but since `bottom`
// offsets the margin edge, a *larger* margin-bottom moves the box *up*. A
// margin-top would do nothing here (the auto `top` absorbs it). When shifted the
// tip no longer points at the marker, so it's hidden.
function ensurePopupInView(popup: L.Popup) {
	const el = popup.getElement()
	if (!el) return
	const tip = el.querySelector<HTMLElement>('.leaflet-popup-tip-container')
	el.style.marginLeft = el.style.marginBottom = '' // measure at the unshifted baseline
	if (tip) tip.style.display = ''
	const baseBottom = parseFloat(getComputedStyle(el).marginBottom) || 0
	const m = map.getContainer().getBoundingClientRect()
	const p = el.getBoundingClientRect()
	const pad = 12
	let dx = 0, dy = 0
	if (p.left < m.left + pad) dx = p.left - (m.left + pad)
	else if (p.right > m.right - pad) dx = p.right - (m.right - pad)
	if (p.top < m.top + pad) dy = p.top - (m.top + pad)
	else if (p.bottom > m.bottom - pad) dy = p.bottom - (m.bottom - pad)
	if (dx) el.style.marginLeft = -dx + 'px'
	if (dy) el.style.marginBottom = baseBottom + dy + 'px' // dy<0 (top cut) shrinks it → moves down
	if (tip) tip.style.display = dx || dy ? 'none' : ''
}

function loadPopup(f: Facility, popup: L.Popup) {
	renderFacility(f, (html) => {
		popup.setContent(html)
		// the popup grows to fit the loaded content; bring it on screen after layout
		requestAnimationFrame(() => { if (popup.isOpen()) ensurePopupInView(popup) })
	}, () => !popup.isOpen())
}

// facility details panel over the map for mobile

const detailContentEl = document.getElementById('fac-detail-content')!
let detailToken = 0

function openDetail(f: Facility) {
	const token = ++detailToken
	detailContentEl.innerHTML = '<div class="fac-popup-loading">Loading…</div>'
	document.body.classList.add('detail-open')
	scheduleOverlaySync()
	renderFacility(f, (html) => detailContentEl.innerHTML = html, () => token !== detailToken)
}

function closeDetail() {
	document.body.classList.remove('detail-open')
	scheduleOverlaySync()
}

// closing an overlay (a facility popup, the mobile detail panel, or the mobile
// filter panel) with Back
//
// While any overlay is open we keep one extra history entry on the stack, so
// pressing Back closes the overlay instead of navigating away (or undoing a
// filter change). The entry is added/removed only on the real edges of "is any
// overlay open", coalesced across a frame so switching markers (close-then-open)
// and the mobile popup→panel handoff don't churn the history. Open/close is
// tracked with plain flags rather than history.state, since syncURL's
// replaceState clobbers the state object.
let popupVisible = false       // a leaflet popup is currently open
let overlayPushed = false      // we have a history entry for the overlay
let overlaySyncRAF = 0
let overlayBackPending = false // our own history.back() is in flight

// overlayOpen reports whether any back-closable overlay is showing. The filter
// panel is a full-screen overlay only on mobile; on wider screens the filters
// are a permanent sidebar (and #btn-filters is hidden), so it doesn't count.
function overlayOpen(): boolean {
	return popupVisible
		|| document.body.classList.contains('detail-open')
		|| (mobileQuery.matches && document.body.classList.contains('filters-open'))
}

// closeOverlays dismisses every overlay; used when a real Back press lands while
// an overlay is open.
function closeOverlays() {
	popupVisible = false
	map.closePopup()
	document.body.classList.remove('detail-open')
	document.body.classList.remove('filters-open')
}

// scheduleOverlaySync reconciles the history entry with whether an overlay is
// open, at most once per frame.
function scheduleOverlaySync() {
	if (overlaySyncRAF) return
	overlaySyncRAF = requestAnimationFrame(() => {
		overlaySyncRAF = 0
		const open = overlayOpen()
		renderEdgeArrows() // show/hide the edge arrows to match the overlay state
		if (open === overlayPushed) return
		if (open) {
			overlayPushed = true
			history.pushState(null, '', location.href)
		} else {
			// closed via the UI (popup ×, map click, panel close/done button) or a
			// resize: unwind our entry; the resulting popstate restores the filter URL
			overlayPushed = false
			overlayBackPending = true
			history.back()
		}
	})
}

map.on('popupopen', () => { popupVisible = true; scheduleOverlaySync() })
map.on('popupclose', () => { popupVisible = false; scheduleOverlaySync() })

window.addEventListener('popstate', () => {
	if (overlayBackPending) {
		// our own history.back() from a UI-driven close; the overlay is already gone
		overlayBackPending = false
	} else if (overlayPushed) {
		// a real Back press while an overlay is open: close it instead of leaving
		overlayPushed = false
		closeOverlays()
	}
	// Filters live in the JS filter state and are only read from the URL at load,
	// so back/forward never change them; keep the URL matching the live filters
	// wherever we land. Without this, forward onto the entry pushed when the
	// overlay opened would show that entry's stale (pre-filter-change) URL.
	writeFilterURL()
})

function setHighlight(i: number, on: boolean) {
	const marker = markers.get(i)
	if (marker) {
		const el = marker.getElement()
		if (el) el.classList.toggle('hl', on)
		marker.setZIndexOffset(on ? 1000 : 0)
	}
	const item = listEl.querySelector('li[data-index="' + i + '"]')
	if (item) {
		item.classList.toggle('hl', on)
		if (on) item.scrollIntoView({block: 'nearest'})
	}
}

function focusFacility(i: number) {
	const f = data.facilities[i]!
	const marker = markers.get(i)
	if (!marker) return
	document.body.classList.remove('list-open')
	if (!map.getBounds().contains([f.lat, f.lng]))
		map.setView([f.lat, f.lng], Math.max(map.getZoom(), 14))
	marker.openPopup()
}

// filter controls

// a checkbox filter row, keeping its input and count cell for cheap updates
interface CheckRow {
	row: HTMLLabelElement
	input: HTMLInputElement
	count: HTMLElement
}

let dayBtns: HTMLButtonElement[] = [], slotRows: CheckRow[] = [], catRows: CheckRow[] = [], actRows: CheckRow[] = []

function buildFilters() {
	const daysEl = document.getElementById('filter-days')!
	dayBtns = data.days.map((label, d) => {
		const btn = document.createElement('button')
		btn.type = 'button'
		btn.textContent = label
		btn.addEventListener('click', () => {
			if (filter.days.has(d)) filter.days.delete(d)
			else filter.days.add(d)
			syncControls()
			update()
		})
		daysEl.append(btn)
		return btn
	})
	slotRows = buildCheckList(document.getElementById('filter-slots')!, data.slotLabels, filter.slots)
	catRows = buildCheckList(document.getElementById('filter-categories')!, data.categories, filter.categories, applyCategorySelection)
	actRows = buildCheckList(document.getElementById('filter-activities')!, data.activities, filter.activities)
}

// applyCategorySelection forces the activity selection to match the selected
// categories: activities in a selected category are checked, and the rest are
// unchecked. The user can still uncheck individual activities afterwards.
function applyCategorySelection() {
	filter.activities.clear()
	if (filter.categories.size)
		for (let a = 0; a < data.activities.length; a++)
			if (data.activityInCategories(a, filter.categories))
				filter.activities.add(a)
}

function buildCheckList(el: HTMLElement, labels: string[], set: Set<number>, changed?: () => void): CheckRow[] {
	return labels.map((label, i) => {
		const row = document.createElement('label')
		row.className = 'check'
		const input = document.createElement('input')
		input.type = 'checkbox'
		input.addEventListener('change', () => {
			if (input.checked) set.add(i)
			else set.delete(i)
			if (changed) changed()
			syncControls()
			update()
		})
		const name = document.createElement('span')
		name.className = 'name'
		name.textContent = label
		const count = document.createElement('span')
		count.className = 'count'
		row.append(input, name, count)
		el.append(row)
		return {row, input, count}
	})
}

function syncControls() {
	starredOnlyEl.checked = filter.starredOnly
	hideEmptyEl.checked = filter.hideEmpty
	dayBtns.forEach((btn, d) => btn.classList.toggle('on', filter.days.has(d)))
	slotRows.forEach((r, i) => r.input.checked = filter.slots.has(i))
	catRows.forEach((r, i) => r.input.checked = filter.categories.has(i))
	actRows.forEach((r, i) => r.input.checked = filter.activities.has(i))
	searchEl.value = filter.name
}

function applyCounts(rows: CheckRow[], counts: Uint32Array) {
	rows.forEach((r, i) => {
		r.count.textContent = String(counts[i])
		r.row.classList.toggle('zero', counts[i] === 0)
	})
}

// syncActivityVisibility limits the visible activity filter options to the
// selected categories (still showing explicitly selected activities).
function syncActivityVisibility() {
	const catFiltered = filter.categories.size > 0
	actRows.forEach((r, a) => {
		r.row.hidden = catFiltered && !data.activityInCategories(a, filter.categories) && !filter.activities.has(a)
	})
	activitiesFilteredEl.hidden = !catFiltered
}

// url filter state

// filterURL builds the page URL reflecting the current filter state in the
// query parameters, all prefixed with "f-" (the page's canonical URL keeps them
// out of indexing). f-v versions the format and f-t records the data date the
// filters were applied against, in case they're needed to interpret old links
// later.
function filterURL(): string {
	const params = new URLSearchParams()
	if (filter.days.size)
		params.set('f-days', [...filter.days].sort((a, b) => a - b).map((d) => data.days[d]!).join(','))
	if (filter.slots.size)
		params.set('f-times', [...filter.slots].sort((a, b) => a - b).map((s) => data.slots[s]!).join(','))
	if (filter.categories.size) {
		params.set('f-cat', [...filter.categories].sort((a, b) => a - b).map((c) => data.categories[c]!).join(','))
		// with categories selected, store the activities unchecked from
		// them instead of the checked ones
		for (let a = 0; a < data.activities.length; a++)
			if (data.activityInCategories(a, filter.categories) && !filter.activities.has(a))
				params.append('f-xact', data.activities[a]!)
	} else {
		for (const a of [...filter.activities].sort((x, y) => x - y))
			params.append('f-act', data.activities[a]!)
	}
	if (filter.name)
		params.set('f-q', filter.name)
	if (filter.starredOnly)
		params.set('f-starred', '1')
	if (filter.hideEmpty)
		params.set('f-has-act', '1')
	if (!params.keys().next().done) {
		params.set('f-v', '1')
		params.set('f-t', data.updated)
	}
	const qs = params.toString().replace(/%3A/gi, ':').replace(/%2C/gi, ',')
	return location.pathname + (qs ? '?' + qs : '') + location.hash
}

// writeFilterURL replaces the current history entry's URL with the filter URL,
// without touching the entry's state or adding to the history. It deliberately
// passes null state so it never resurrects an overlay's pushed marker state
// (see the overlay history handling below).
function writeFilterURL() {
	const url = filterURL()
	if (url !== location.pathname + location.search + location.hash)
		history.replaceState(null, '', url)
}

// syncURL reflects the filter state in the URL. Debounced since Safari
// rate-limits history.replaceState.
let urlTimer: number | undefined
function syncURL() {
	clearTimeout(urlTimer)
	urlTimer = setTimeout(writeFilterURL, 300)
}

// loadURLState restores the filter state from the query parameters. If any
// stored days, times, or categories no longer exist (i.e., the available
// options changed since the link was made), that filter is reset so results
// aren't unexpectedly missing, and a toast is shown.
function loadURLState() {
	const params = new URLSearchParams(location.search)
	const reset: string[] = []
	// an unrecognized f-* param means the link is from a newer format; reset
	// everything rather than restoring it partially
	const known = ['f-v', 'f-t', 'f-days', 'f-times', 'f-cat', 'f-act', 'f-xact', 'f-q', 'f-starred', 'f-has-act']
	for (const key of params.keys())
		if (key.startsWith('f-') && !known.includes(key)) {
			showToast('Some filters were reset because the schedule data changed (all).')
			return
		}
	const restore = (param: string, what: string, labels: string[], set: Set<number>) => {
		const v = params.get(param)
		if (!v) return
		const idx = v.split(',').map((label) => labels.indexOf(label))
		if (idx.includes(-1)) reset.push(what)
		else idx.forEach((i) => set.add(i))
	}
	restore('f-days', 'weekdays', data.days, filter.days)
	restore('f-times', 'times', data.slots, filter.slots)
	restore('f-cat', 'categories', data.categories, filter.categories)
	if (filter.categories.size) {
		// the url stores the activities unchecked from the selected categories
		applyCategorySelection()
		for (const name of params.getAll('f-xact')) {
			const a = data.activities.indexOf(name)
			if (a >= 0) filter.activities.delete(a)
			else if (!reset.includes('activities')) // a previously unchecked activity is gone
				reset.push('activities')
		}
	} else {
		// without valid categories, any stored exclusions can't be applied
		if (params.getAll('f-xact').length && !reset.includes('activities'))
			reset.push('activities')
		for (const name of params.getAll('f-act')) {
			const a = data.activities.indexOf(name)
			if (a >= 0) filter.activities.add(a)
			else if (!reset.includes('activities')) // a previously selected activity is gone
				reset.push('activities')
		}
	}
	filter.name = params.get('f-q') || ''
	filter.starredOnly = params.get('f-starred') === '1'
	filter.hideEmpty = params.get('f-has-act') === '1'
	if (reset.length)
		showToast('Some filters were reset because the schedule data changed (' + reset.join(', ') + ').')
}

// rendering

function update() {
	visible = data.matchingFacilities(filter)
	sortVisible()
	renderList()
	updateMarkers()
	renderEdgeArrows()
	applyCounts(slotRows, data.slotCounts(filter))
	applyCounts(catRows, data.categoryCounts(filter))
	applyCounts(actRows, data.activityCounts(filter))
	syncActivityVisibility()
	renderChips()
	const count = visible.length + '/' + data.facilities.length + ' facilit' + (data.facilities.length === 1 ? 'y' : 'ies')
	facCountEl.textContent = count
	sheetToggleEl.textContent = count + (document.body.classList.contains('list-open') ? ' ▾' : ' ▴')
	syncURL()
}

function cmpName(a: number, b: number) {
	return data.facilities[a]!.name.localeCompare(data.facilities[b]!.name)
}

function cmpStarred(a: number, b: number) {
	return Number(ottrecStarred.has(data.facilities[b]!.slug)) - Number(ottrecStarred.has(data.facilities[a]!.slug))
}

function sortVisible() {
	if (order === 'distance') {
		const c = map.getCenter()
		const kx = Math.cos(c.lat * Math.PI / 180)
		const dist = (i: number) => {
			const f = data.facilities[i]!
			if (!f.lat && !f.lng) return Infinity
			const dx = (f.lng - c.lng) * kx, dy = f.lat - c.lat
			return dx * dx + dy * dy
		}
		visible.sort((a, b) => dist(a) - dist(b) || cmpName(a, b))
	} else {
		// starred facilities first, then alphabetical
		visible.sort((a, b) => cmpStarred(a, b) || cmpName(a, b))
	}
}

function renderList() {
	const frag = document.createDocumentFragment()
	for (const i of visible) {
		const f = data.facilities[i]!
		const li = document.createElement('li')
		li.dataset['index'] = String(i)
		li.tabIndex = 0
		const h = document.createElement('h2')
		// the name links to the full schedule so middle/ctrl/cmd-click and "open
		// in new tab" work and crawlers see a real href; a plain click shows the
		// popup instead
		const link = document.createElement('a')
		link.href = '/schedules/facility/' + encodeURIComponent(f.slug)
		link.textContent = f.name
		link.addEventListener('click', (ev) => {
			ev.stopPropagation() // don't also trigger the list item's click
			if (ev.ctrlKey || ev.metaKey || ev.shiftKey) return // let the browser open it
			ev.preventDefault()
			focusFacility(i)
		})
		h.append(link)
		const star = document.createElement('button')
		star.type = 'button'
		star.className = 'fac-star msym'
		star.dataset['facStar'] = f.slug
		h.append(star)
		const addr = document.createElement('p')
		addr.className = 'addr'
		addr.textContent = f.address
		li.append(h, addr)
		li.addEventListener('mouseenter', () => setHighlight(i, true))
		li.addEventListener('mouseleave', () => setHighlight(i, false))
		li.addEventListener('click', () => focusFacility(i))
		frag.append(li)
	}
	listEl.replaceChildren(frag)
	ottrecStarred.sync() // wire up and set the state of the new star buttons
}

function updateMarkers() {
	const visibleSet = new Set(visible)
	for (const [i, marker] of markers) {
		const want = visibleSet.has(i)
		if (want && !map.hasLayer(marker)) marker.addTo(map)
		else if (!want && map.hasLayer(marker)) {
			// if the marker is removed while hovered, its mouseout never fires, so
			// close the tooltip and clear the highlight here or they'd stick
			marker.closeTooltip()
			setHighlight(i, false)
			marker.remove()
		}
	}
}

// edge arrows: a blue arrow at the map edge for each visible facility that's
// off-screen, pointing toward it; clicking it pans the map to that facility.
const edgeArrowsEl = document.createElement('div')
edgeArrowsEl.className = 'edge-arrows'
map.getContainer().append(edgeArrowsEl)

// the arrow glyph points east (angle 0); the button is rotated toward the facility
const edgeArrowSVG = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M9 4l8 8-8 8z"/></svg>'

let edgeArrowsRAF = 0
function scheduleEdgeArrows() {
	if (edgeArrowsRAF) return
	edgeArrowsRAF = requestAnimationFrame(() => { edgeArrowsRAF = 0; renderEdgeArrows() })
}

function renderEdgeArrows() {
	const size = map.getSize()
	const inset = 18 // keep arrows just inside the map edge
	// the overlay sits above the map panes, so hide it while a popup/detail panel
	// covers the map (and when the map is too small to have a meaningful interior)
	if (overlayOpen() || size.x < 2 * inset || size.y < 2 * inset) { edgeArrowsEl.replaceChildren(); return }
	const cx = size.x / 2, cy = size.y / 2
	const minX = inset, maxX = size.x - inset, minY = inset, maxY = size.y - inset
	// bucket off-screen facilities by edge position so a crowded edge stays
	// legible; each bucket keeps the facility nearest the viewport (and a count)
	const buckets = new Map<string, {i: number, dist: number, x: number, y: number, angle: number, count: number}>()
	for (const i of visible) {
		const f = data.facilities[i]!
		if (!f.lat && !f.lng) continue
		const p = map.latLngToContainerPoint([f.lat, f.lng])
		if (p.x >= minX && p.x <= maxX && p.y >= minY && p.y <= maxY) continue // on-screen
		const dx = p.x - cx, dy = p.y - cy
		// scale the center->facility ray to where it crosses the inset edge
		let t = Infinity
		if (dx > 0) t = Math.min(t, (maxX - cx) / dx)
		else if (dx < 0) t = Math.min(t, (minX - cx) / dx)
		if (dy > 0) t = Math.min(t, (maxY - cy) / dy)
		else if (dy < 0) t = Math.min(t, (minY - cy) / dy)
		if (!isFinite(t) || t <= 0) continue
		const x = cx + dx * t, y = cy + dy * t
		const dist = Math.hypot(dx, dy)
		const key = Math.round(x / 30) + ':' + Math.round(y / 30)
		const prev = buckets.get(key)
		if (!prev) buckets.set(key, {i, dist, x, y, angle: Math.atan2(dy, dx), count: 1})
		else if (prev.count++, dist < prev.dist) {
			prev.i = i, prev.dist = dist, prev.x = x, prev.y = y, prev.angle = Math.atan2(dy, dx)
		}
	}
	const frag = document.createDocumentFragment()
	for (const b of buckets.values()) {
		const f = data.facilities[b.i]!
		const more = b.count > 1 ? ' (+' + (b.count - 1) + ' more nearby)' : ''
		const btn = document.createElement('button')
		btn.type = 'button'
		btn.className = 'edge-arrow'
		btn.title = f.name + more
		btn.setAttribute('aria-label', 'Pan to ' + f.name + more)
		btn.style.left = b.x + 'px'
		btn.style.top = b.y + 'px'
		btn.style.transform = 'translate(-50%, -50%) rotate(' + b.angle + 'rad)'
		btn.innerHTML = edgeArrowSVG
		const ll: L.LatLngTuple = [f.lat, f.lng]
		// just bring the facility into view; don't open its popup
		btn.addEventListener('click', () => map.panTo(ll))
		frag.append(btn)
	}
	edgeArrowsEl.replaceChildren(frag)
}

map.on('move zoom resize', scheduleEdgeArrows)

function renderChips() {
	const chips: {label: string, clear: () => void}[] = []
	if (filter.starredOnly)
		chips.push({label: 'Starred', clear: () => filter.starredOnly = false})
	if (filter.hideEmpty)
		chips.push({label: 'Has activities', clear: () => filter.hideEmpty = false})
	if (filter.days.size)
		chips.push({
			label: [...filter.days].sort((a, b) => a - b).map((d) => data.days[d]).join(', '),
			clear: () => filter.days.clear(),
		})
	for (const s of [...filter.slots].sort((a, b) => a - b))
		chips.push({label: data.slotLabels[s]!, clear: () => filter.slots.delete(s)})
	for (const c of [...filter.categories].sort((x, y) => x - y))
		chips.push({label: data.categories[c]!, clear: () => {
			filter.categories.delete(c)
			applyCategorySelection()
		}})
	for (const a of [...filter.activities].sort((x, y) => x - y))
		if (!data.activityInCategories(a, filter.categories) || !filter.categories.size)
			chips.push({label: data.activities[a]!, clear: () => filter.activities.delete(a)})
	if (filter.name.trim())
		chips.push({label: '“' + filter.name.trim() + '”', clear: () => filter.name = ''})
	filterChipsEl.replaceChildren(...chips.map((c) => {
		const btn = document.createElement('button')
		btn.type = 'button'
		btn.className = 'fchip'
		btn.textContent = c.label
		btn.addEventListener('click', () => {
			c.clear()
			syncControls()
			update()
		})
		return btn
	}))
}

// wiring

searchEl.addEventListener('input', () => {
	filter.name = searchEl.value
	update()
})
starredOnlyEl.addEventListener('change', () => {
	filter.starredOnly = starredOnlyEl.checked
	update()
})
hideEmptyEl.addEventListener('change', () => {
	filter.hideEmpty = hideEmptyEl.checked
	update()
})
// keep the markers, list order, starred-only filter, and any open popup in
// sync when stars change (including from another tab)
ottrecStarred.onchange(() => {
	for (const [i, marker] of markers) {
		const html = pinHTML(ottrecStarred.has(data.facilities[i]!.slug))
		pinIcons.get(i)!.options.html = html // for markers (re-)added later
		const el = marker.getElement()
		if (el) el.innerHTML = html
	}
	syncStarredFilter()
	syncControls()
	update()
})
document.getElementById('fac-order')!.addEventListener('change', (ev) => {
	order = (ev.target as HTMLSelectElement).value
	update()
})
map.on('moveend', () => {
	if (order === 'distance') update()
})
document.getElementById('filter-days-all')!.addEventListener('click', () => {
	filter.days = new Set(data.days.map((_, i) => i))
	syncControls()
	update()
})
document.getElementById('filter-days-none')!.addEventListener('click', () => {
	filter.days.clear()
	syncControls()
	update()
})
document.getElementById('filter-slots-all')!.addEventListener('click', () => {
	filter.slots = new Set(data.slots.map((_, i) => i))
	syncControls()
	update()
})
document.getElementById('filter-slots-none')!.addEventListener('click', () => {
	filter.slots.clear()
	syncControls()
	update()
})
document.getElementById('btn-filters')!.addEventListener('click', () => {
	// the filter panel covers the map, so close any open facility popup/detail
	map.closePopup()
	document.body.classList.remove('detail-open')
	document.body.classList.add('filters-open')
	scheduleOverlaySync()
})
document.getElementById('btn-filters-done')!.addEventListener('click', () => {
	document.body.classList.remove('filters-open')
	scheduleOverlaySync()
})
document.getElementById('fac-detail-close')!.addEventListener('click', closeDetail)
activitiesFilteredEl.addEventListener('click', () => {
	// clear the category filters, but leave the checked activities alone
	filter.categories.clear()
	syncControls()
	update()
})
sheetToggleEl.addEventListener('click', () => {
	document.body.classList.toggle('list-open')
	update()
})

buildFilters()
loadURLState()
syncStarredFilter()
syncControls()
update()
