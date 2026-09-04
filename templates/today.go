package templates

import (
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ottrec/data-enrichment/enrichidx"
	"github.com/ottrec/scraper/schema"
	"github.com/ottrec/website/pkg/ottrecidx"
	"github.com/ottrec/website/pkg/ottregions"
)

// todayWindowDays is how many days the chronological feed runs, including
// today. A week gives a feed that's worth scrolling without being endless; the
// filters do the narrowing.
const todayWindowDays = 7

// todayBadgeWindow is how far (in days) on either side of the feed a fixed-date
// schedule still counts as a "nearby" holiday/special-date for the strong
// badge. Matches the ±1 week the prevalence in PLAN.md was measured against.
const todayBadgeWindow = 7

// timeNow is the wall clock used to anchor "today", overridable in tests.
var timeNow = time.Now

func init() {
	if v := os.Getenv("OTTREC_FAKE_TODAY"); v != "" {
		t, err := time.ParseInLocation("2006-01-02", v, ottrecidx.TZ)
		if err != nil {
			panic(err)
		}
		t = t.Add(6 * time.Hour)
		timeNow = func() time.Time { return t }
	}
}

// TodayFeedDate is the date (in the dataset timezone) the today feed is
// anchored to, for mixing into the /today ETag.
func TodayFeedDate() string {
	return timeNow().In(ottrecidx.TZ).Format(time.DateOnly)
}

// WebsiteTodayParams parameterizes the "what's on" feed page. The simple filters
// are client-side (the pills in today.ts); the advanced mode is server-side: it
// runs an ottrecql query and builds the feed from the filtered data, replacing
// the pills with the query box (mirroring the schedules advanced search).
type WebsiteTodayParams struct {
	Base       string
	Data       ottrecidx.DataRef // full data, for slugs and the updated timestamp
	Filtered   ottrecidx.DataRef // data to build the feed from (== Data unless advanced)
	Enrich     enrichidx.Ref     // schedule-change enrichment (zero = unavailable)
	Advanced   bool              // advanced (ottrecql) search mode
	Query      string            // current query box contents
	QueryError string            // query parse/limit error to show instead of the feed
}

// todaySession is one placed drop-in session in the feed: a single parsed
// clock range for an activity at a facility, on a concrete date. Recurring
// weekday schedules and published fixed-date (holiday) schedules are both
// placed by their own date semantics, independently — we never merge or
// override across them (see PLAN.md "flag, never resolve").
type todaySession struct {
	Start     int    // start minutes from midnight (for sorting/filtering)
	End       int    // end minutes from midnight (may exceed 1440 for overnight)
	Time      string // human clock-range label
	Activity  string
	Facility  string
	Slug      string
	Region    string
	Sector    string // sector display label ("Central"…/"Other")
	Cats      int    // bitmask of [ScheduleCategories] indexes (+ Other bit)
	Weekday   int    // 0 = Sunday, for the weekday filter
	Qual      string // date-range qualifier, shown only for bounded/seasonal schedules
	Fixed     bool   // a published fixed-date (holiday/special) session
	SourceURL string // City of Ottawa facility page (for warning lines + the source link)
	GroupKey  string // the session's schedule group key (see [ScheduleGroupKey]; for /api/changes)

	// warning flags (per facility/group), each shown as a warning line under
	// the session opening a modal sourced from /api/changes or
	// /api/holiday-schedules. Enriched-prefixed flags are solely
	// enrichment-derived and stay false without enrichment.
	Holiday              bool // facility has a fixed-date schedule near the feed
	EnrichedSeeSchedule  bool // posted changes defer to a separate holiday/event schedule (shown instead of Holiday)
	Changes              bool // posted changes/special hours may affect this group during the feed
	EnrichedNotice       bool // facility-wide notices apply, but nothing schedule-affecting
	EnrichedOtherChanges bool // posted changes/notices exist, but enrichment placed none in the feed window
	Incomplete           bool // the facility has scrape errors

	// enrichment-derived session states (see enrichidx). EnrichedCancelled
	// wins over EnrichedScopeCancelled and EnrichedTimeChanged;
	// Time/Start/End hold the trimmed effective time when one was derived,
	// with the published one kept in OldTime.
	EnrichedCancelled      bool   // a validated notice cancels/closes this exact session
	EnrichedScopeCancelled bool   // a whole-scope (group/facility) cancellation may apply on this date
	EnrichedAdded          bool   // this session comes from a notice, not the published schedule
	EnrichedTimeChanged    bool   // a time-change notice affects this session
	OldTime                string // the published clock label when the time was trimmed (struck out below)

	// reservation note (per activity), shown as a grey boxed note below the
	// warnings opening a modal sourced from /api/reservations
	Reservations bool // the activity requires or may require a reservation
	ResvDefinite bool // the requirement is definite (vs. "may be required")
}

// todayHourGroup is the sessions in a day that start within the same clock
// hour, the feed's within-day grouping.
type todayHourGroup struct {
	Hour     int    // 0-23, the start hour
	Label    string // "9 AM", "12 PM"
	Sessions []todaySession
}

// todayFeedDay is one calendar day's worth of sessions in the feed, grouped by
// start hour.
type todayFeedDay struct {
	DateISO string // "2026-06-15"
	Weekday string // "Sunday"
	WdIndex int    // 0 = Sunday
	Month   string // "June 15"
	Rel     string // "Today", "Tomorrow", or ""
	Hours   []todayHourGroup
	Empty   bool // no sessions at all
}

// todayClockLabel formats a clock range like [clockRangeParts] but elides the
// minutes when they're zero ("9–11am" instead of "9:00–11:00am"), for the
// denser today feed.
func todayClockLabel(r schema.ClockRange) string {
	_, sh, sm := r.Start.Split()
	_, eh, em := r.End.Split()
	sdh := sh % 12
	if sdh == 0 {
		sdh = 12
	}
	edh := eh % 12
	if edh == 0 {
		edh = 12
	}
	t := func(h, m int, suf string) string {
		if m == 0 {
			return fmt.Sprintf("%d%s", h, suf)
		}
		return fmt.Sprintf("%d:%02d%s", h, m, suf)
	}
	esuf := "am"
	if eh >= 12 {
		esuf = "pm"
	}
	var st string
	if (sh < 12) == (eh < 12) {
		st = t(sdh, sm, "")
	} else {
		ssuf := "am"
		if sh >= 12 {
			ssuf = "pm"
		}
		st = t(sdh, sm, ssuf)
	}
	return st + "–" + t(edh, em, esuf)
}

// todayHourLabel formats an hour (0-23) as a 12-hour label like "9 AM".
func todayHourLabel(h int) string {
	ap := "AM"
	if h >= 12 {
		ap = "PM"
	}
	hh := h % 12
	if hh == 0 {
		hh = 12
	}
	return fmt.Sprintf("%d %s", hh, ap)
}

// todayFeed is the whole server-rendered feed plus the metadata island the
// client needs to drive the filter pills.
type todayFeed struct {
	Days []todayFeedDay
	JSON todayDataJSON
}

// todayDataJSON is embedded into the page as a JSON island and consumed by the
// filter pills in static/today.ts.
type todayDataJSON struct {
	Updated    string              `json:"updated"`
	Weekdays   []string            `json:"weekdays"`   // Sun..Sat
	Categories []string            `json:"categories"` // [ScheduleCategories] + Other
	Sectors    []string            `json:"sectors"`    // group order for the facility pill
	Facilities []todayFacilityJSON `json:"facilities"`
}

type todayFacilityJSON struct {
	Slug   string `json:"slug"`
	Name   string `json:"name"`
	Sector string `json:"sector"`
}

// ScheduleGroupKey returns a short URL-safe identifier for a facility's
// schedule group, derived from its raw label. Unlike a document-order index,
// it stays valid across dataset updates, so modal fetches from stale pages
// still resolve the right group (as long as the label survives).
func ScheduleGroupKey(grp ottrecidx.ScheduleGroupRef) string {
	h := fnv.New64a()
	io.WriteString(h, grp.GetLabel())
	return strconv.FormatUint(h.Sum64(), 32)
}

// FacilityGroupByKey returns the facility schedule group matching a
// [ScheduleGroupKey], and whether one was found. Bare numeric values (from
// pages rendered when groups were addressed by document-order index) fall
// back to positional lookup; a key match always wins.
func FacilityGroupByKey(fac ottrecidx.FacilityRef, key string) (ottrecidx.ScheduleGroupRef, bool) {
	if key == "" {
		return ottrecidx.ScheduleGroupRef{}, false
	}
	idx, idxErr := strconv.Atoi(key)
	var atIdx ottrecidx.ScheduleGroupRef
	atIdxOK := false
	n := 0
	for g := range fac.ScheduleGroups() {
		if ScheduleGroupKey(g) == key {
			return g, true
		}
		if idxErr == nil && n == idx {
			atIdx, atIdxOK = g, true
		}
		n++
	}
	return atIdx, atIdxOK
}

// todayHasChangesContent reports whether the schedule-changes modal would show
// anything (so the empty state can be rendered otherwise).
func todayHasChangesContent(fac ottrecidx.FacilityRef, grp ottrecidx.ScheduleGroupRef, hasGroup bool) bool {
	if hasGroup && strings.TrimSpace(grp.GetScheduleChangesHTML()) != "" {
		return true
	}
	return strings.TrimSpace(fac.GetNotificationsHTML()) != "" ||
		strings.TrimSpace(fac.GetSpecialHoursHTML()) != ""
}

// todayRangeIntersects reports whether the effective date range er (0 = open
// end) overlaps the inclusive window [from, to], at day granularity.
func todayRangeIntersects(er schema.DateRange, from, to schema.Date) bool {
	if !er.From.IsZero() && int(er.From)/10 > int(to)/10 {
		return false
	}
	if !er.To.IsZero() && int(er.To)/10 < int(from)/10 {
		return false
	}
	return true
}

// todaySectorLabel returns the facility's sector display label, mapping the
// unknown sector to the trailing "Other" group used elsewhere on the site.
func todaySectorLabel(s ottregions.Sector) string {
	if s == ottregions.SectorUnknown {
		return "Other"
	}
	return s.String()
}

// buildTodayFeed places every parseable drop-in session over the next
// [todayWindowDays] days into a single chronological feed, anchored at now (in
// Ottawa time). It is anchored at the wall clock rather than the data date so
// "today" is correct; cached pages may drift but self-correct when the data
// updates daily (as scheduleClass does).
//
// slug assigns each facility its page slug; pass [MapFacilitySlugger] over the
// full (unfiltered) data so slugs stay stable when data is an ottrecql-filtered
// subset (the advanced search), matching the schedules pages.
//
// enrich (optional; zero = unavailable) narrows the changes warning to groups
// where posted content may actually apply during the feed, adds the milder
// facility-notice warning, marks validated per-session cancellations, and
// injects sessions added by notices.
func buildTodayFeed(data ottrecidx.DataRef, enrich enrichidx.Ref, slug func(string) string, now time.Time) todayFeed {
	loc := ottrecidx.TZ
	now = now.In(loc)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)

	// the window dates, and a lookup from a weekday-stripped date to its index
	dates := make([]time.Time, todayWindowDays)
	dayIndex := map[schema.Date]int{}
	for i := range dates {
		d := today.AddDate(0, 0, i)
		dates[i] = d
		dayIndex[schema.MakeDateFromGo(d)/10] = i
	}
	winStart := schema.MakeDateFromGo(today.AddDate(0, 0, -todayBadgeWindow))
	winEnd := schema.MakeDateFromGo(today.AddDate(0, 0, todayWindowDays-1+todayBadgeWindow))

	// the feed window itself; enrichment warnings and lookups key off these,
	// not the wider badge window
	feedFrom := schema.MakeDateFromGo(dates[0])
	feedTo := schema.MakeDateFromGo(dates[len(dates)-1])
	enOK := enrich.OK()

	daySessions := make([][]todaySession, todayWindowDays)

	type facMeta struct {
		slug, region, sector string
	}
	facList := []todayFacilityJSON{}
	facSeen := map[string]bool{}

	for fac := range data.Facilities() {
		meta := facMeta{slug: slug(fac.GetName())}
		if r := fac.Region(); r != ottregions.RegionUnknown {
			meta.region = r.Name()
		}
		meta.sector = todaySectorLabel(fac.Sector())
		sourceURL := fac.GetSourceURL()

		incomplete := hasFacilityErrors(fac)

		// facility-scoped notices (special hours/notifications) apply to
		// every group's sessions
		enFac := enrich.Facility(fac.GetName())
		facWarn := enrichidx.WarnNone
		if enOK {
			facWarn = enFac.Warning(feedFrom, feedTo)
		}

		// if a likely holiday schedule has a date range or specific dates for a
		// feed day (or if there's a likely holiday schedule for which we can't
		// properly parse the date range), show a strong warning for all
		// activities
		var holidayDay [todayWindowDays]bool
		var holidayAll bool
		for s := range fac.Schedules() {
			if !s.LikelyHolidaySchedule() {
				continue
			}
			er, ok := s.ComputeEffectiveDateRange()
			if !ok {
				holidayAll = true
				break
			}
			for i, d := range dates {
				dd := schema.MakeDateFromGo(d)
				if todayRangeIntersects(er, dd, dd) {
					holidayDay[i] = true
				}
			}
		}
		// posted "See <holiday> schedule" notices are the same signal, stated
		// outright by the city even when the schedule itself isn't published;
		// same nearby window as the badge
		facSee := enOK && enFac.SeeSchedule(winStart, winEnd)

		facHasSession := false
		for grp := range fac.ScheduleGroups() {
			gk := ScheduleGroupKey(grp)

			// with enrichment, warn only when posted content may actually
			// apply during the feed window (the milder notice tier covers
			// content that shouldn't affect the times); without it, fall
			// back to warning whenever a schedule-changes block exists
			enGrp := enFac.Group(grp.GetLabel())
			warn := facWarn
			if enOK {
				warn = max(warn, enGrp.Warning(feedFrom, feedTo))
			} else if grp.GetScheduleChangesHTML() != "" {
				warn = enrichidx.WarnChanges
			}
			changes := warn == enrichidx.WarnChanges
			notice := warn == enrichidx.WarnNotice

			seeSched := facSee
			if enOK && !seeSched && enGrp.SeeSchedule(winStart, winEnd) {
				seeSched = true
			}

			// if an enriched see-schedule notice has a specific date, only show
			// it for that one, not every day on the feed
			var seeSchedDay [todayWindowDays]bool
			if seeSched {
				for i, d := range dates {
					dd := schema.MakeDateFromGo(d)
					seeSchedDay[i] = facSee && enFac.SeeSchedule(dd, dd) ||
						enGrp.SeeSchedule(dd, dd)
				}
			}

			// posted content exists but enrichment placed none of it in the
			// feed window: offer a muted link to the changes modal in place of
			// the warning line it suppressed
			otherChanges := enOK && warn == enrichidx.WarnNone && !seeSched &&
				todayHasChangesContent(fac, grp, true)

			var added []enrichidx.AddedSession
			var actByLabel map[string]ottrecidx.ActivityRef
			if enOK {
				if added = enGrp.Added(feedFrom, feedTo); len(added) > 0 {
					actByLabel = map[string]ottrecidx.ActivityRef{}
				}
			}

			for sch := range grp.Schedules() {
				er, erOK := sch.ComputeEffectiveDateRange()

				// the seasonal/bounded qualifier carried with each session, so
				// the date semantics aren't flattened away
				var qual string
				if erOK && (!er.From.IsZero() || !er.To.IsZero()) {
					qual = scheduleDateRangeLabel(sch)
				}

				for act := range sch.Activities() {
					label := activityLabel(act)
					if label == "" {
						continue
					}
					rawLabel := act.GetLabel() // the enrichment join key
					if actByLabel != nil {
						if _, ok := actByLabel[rawLabel]; !ok {
							actByLabel[rawLabel] = act
						}
					}
					cats := activityCategoryMask(mapActivityName(act))
					resvReq, resvDef := act.GuessReservationRequirement()
					for tm := range act.Times() {
						r, ok := tm.GetRange()
						if !ok {
							continue // can't place on a timeline
						}
						base := todaySession{
							Start:                int(r.Start),
							End:                  int(r.End),
							Time:                 todayClockLabel(r),
							Activity:             label,
							Facility:             fac.GetName(),
							Slug:                 meta.slug,
							Region:               meta.region,
							Sector:               meta.sector,
							Cats:                 cats,
							Qual:                 qual,
							SourceURL:            sourceURL,
							GroupKey:             gk,
							Changes:              changes,
							EnrichedNotice:       notice,
							EnrichedOtherChanges: otherChanges,
							Incomplete:           incomplete,

							Reservations: resvReq,
							ResvDefinite: resvDef,
						}

						place := func(i int, wd time.Weekday, day schema.Date) {
							s := base
							s.Weekday = int(wd)
							s.Holiday = holidayAll || holidayDay[i]
							s.EnrichedSeeSchedule = seeSchedDay[i]
							if enOK {
								m := enGrp.Session(rawLabel, day, s.Start, s.End)
								s.EnrichedCancelled = m.Cancelled ||
									// a whole-scope notice that says a
									// cancellation happened ("The facility is
									// closed and all programs cancelled.")
									// takes the sessions under it with it: the
									// scope is still an inference but the
									// cancellation is the city's own word
									enFac.ScopeCancelledStated(day, s.Start, s.End) ||
									enGrp.ScopeCancelledStated(day, s.Start, s.End)
								if !s.EnrichedCancelled {
									// the rest of the whole-scope tier is a
									// closure the scope only implies, which
									// gets the softer likely-cancelled warning
									// rather than the strike
									s.EnrichedScopeCancelled = enFac.ScopeCancelled(day, s.Start, s.End) ||
										enGrp.ScopeCancelled(day, s.Start, s.End)
									s.EnrichedTimeChanged = m.TimeChange
									if m.NewTime {
										// the trimmed effective time, with the
										// published one struck out below
										s.OldTime = s.Time
										s.Start, s.End = m.NewStart, m.NewEnd
										s.Time = todayClockLabel(schema.ClockRange{Start: schema.ClockTime(m.NewStart), End: schema.ClockTime(m.NewEnd)})
									}
								}
								if s.EnrichedCancelled || s.EnrichedScopeCancelled || s.EnrichedTimeChanged {
									// those warning lines already open the modal
									s.EnrichedOtherChanges = false
								}
							}
							daySessions[i] = append(daySessions[i], s)
							facHasSession = true
						}

						// published non-recurring special-date times
						if d, ok := tm.SingleDate(); ok {
							// a fixed-date session is already pinned to its day
							base.Fixed = true
							base.Qual = ""

							// pin to the concrete date the column lists.
							if i, in := dayIndex[d/10]; in {
								wd, _ := d.Weekday()
								place(i, wd, d)
							}
							continue
						}

						// recurring: show on every matching weekday the
						// schedule covers, without suppressing it because some
						// holiday schedule exists (note: this is signaled to
						// the user by the warnings we add for all times where a
						// possible holiday schedule exists, and we don't want
						// to accidentally exclude a non-overridden time
						// incorrectly, so we leave it to the user to decide).
						wd, ok := tm.GetWeekday()
						if !ok {
							continue
						}
						for i, d := range dates {
							if d.Weekday() != wd {
								continue
							}
							dd := schema.MakeDateFromGo(d)
							if erOK {
								if !er.From.IsZero() && int(er.From)/10 > int(dd)/10 {
									continue
								}
								if !er.To.IsZero() && int(er.To)/10 < int(dd)/10 {
									continue
								}
							}
							place(i, wd, dd)
						}
					}
				}
			}

			// inject sessions added by notices (rendered with the green
			// note); non-novel activities reuse the published activity's
			// display name and category, novel ones only exist in the notice
			for _, ad := range added {
				i, in := dayIndex[ad.Date/10]
				if !in {
					continue
				}
				label, cats := ad.ActivityLabel, 0
				if act, ok := actByLabel[ad.ActivityLabel]; ok {
					label = activityLabel(act)
					cats = activityCategoryMask(mapActivityName(act))
				} else if !ad.Novel {
					continue // not in this (possibly filtered) data
				} else {
					cats = activityCategoryMask(strings.ToLower(strings.Join(strings.Fields(label), " ")))
				}
				if label == "" {
					continue
				}
				daySessions[i] = append(daySessions[i], todaySession{
					Start:               ad.Start,
					End:                 ad.End,
					Time:                todayClockLabel(schema.ClockRange{Start: schema.ClockTime(ad.Start), End: schema.ClockTime(ad.End)}),
					Activity:            label,
					Facility:            fac.GetName(),
					Slug:                meta.slug,
					Region:              meta.region,
					Sector:              meta.sector,
					Cats:                cats,
					Weekday:             int(dates[i].Weekday()),
					SourceURL:           sourceURL,
					GroupKey:            gk,
					Holiday:             holidayAll || holidayDay[i],
					EnrichedSeeSchedule: seeSchedDay[i],
					Incomplete:          incomplete,
					EnrichedAdded:       true,
				})
				facHasSession = true
			}
		}

		if facHasSession && !facSeen[meta.slug] {
			facSeen[meta.slug] = true
			facList = append(facList, todayFacilityJSON{
				Slug:   meta.slug,
				Name:   fac.GetName(),
				Sector: meta.sector,
			})
		}
	}

	// assemble the days, sorting each day's sessions chronologically
	days := make([]todayFeedDay, 0, todayWindowDays)
	for i, d := range dates {
		ss := daySessions[i]
		sort.SliceStable(ss, func(a, b int) bool {
			if ss[a].Start != ss[b].Start {
				return ss[a].Start < ss[b].Start
			}
			if ss[a].Facility != ss[b].Facility {
				return ss[a].Facility < ss[b].Facility
			}
			return ss[a].Activity < ss[b].Activity
		})
		var rel string
		switch i {
		case 0:
			rel = "Today"
		case 1:
			rel = "Tomorrow"
		}
		// group the (already start-sorted) sessions by start hour
		var hours []todayHourGroup
		for _, s := range ss {
			h := s.Start / 60
			if h > 23 {
				h = 23
			}
			if len(hours) == 0 || hours[len(hours)-1].Hour != h {
				hours = append(hours, todayHourGroup{Hour: h, Label: todayHourLabel(h)})
			}
			hours[len(hours)-1].Sessions = append(hours[len(hours)-1].Sessions, s)
		}
		days = append(days, todayFeedDay{
			DateISO: d.Format("2006-01-02"),
			Weekday: d.Weekday().String(),
			WdIndex: int(d.Weekday()),
			Month:   d.Format("January 2"),
			Rel:     rel,
			Hours:   hours,
			Empty:   len(ss) == 0,
		})
	}

	// facility pill options, grouped by sector then alphabetical
	sort.SliceStable(facList, func(a, b int) bool {
		return facList[a].Name < facList[b].Name
	})
	sectors := make([]string, 0, len(activitySectorOrder)+1)
	for _, s := range activitySectorOrder {
		sectors = append(sectors, s.String())
	}
	sectors = append(sectors, "Other")

	feed := todayFeed{
		Days: days,
		JSON: todayDataJSON{
			Updated:    data.Index().Updated().In(loc).Format("2006-01-02"),
			Weekdays:   slices.Clone(mapDays),
			Categories: categoryNames(),
			Sectors:    sectors,
			Facilities: facList,
		},
	}
	return feed
}
