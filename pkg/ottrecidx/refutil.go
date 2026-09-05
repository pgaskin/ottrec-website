package ottrecidx

import (
	"time"

	"github.com/ottrec/scraper/schema"
	"github.com/ottrec/website/pkg/ottregions"
)

// this file contains additional helpers to perform computations on refs, possibly with optimizations

// Region returns the region facility is in, from its coordinates, or
// [ottregions.RegionUnknown] if it has none.
func (ref FacilityRef) Region() ottregions.Region {
	if idx := ref.index(); idx.cached_FacilityRef_RegionSector {
		return idx.cached_FacilityRef_RegionSector_r[ref.nthOfType()]
	}
	lng, lat, ok := ref.GetLngLat()
	if !ok {
		return ottregions.RegionUnknown
	}
	return ottregions.RegionAt(float64(lat), float64(lng))
}

// Sector returns the sector the city the facility is in, from its coordinates,
// or [ottregions.SectorUnknown] if it has none.
func (ref FacilityRef) Sector() ottregions.Sector {
	if idx := ref.index(); idx.cached_FacilityRef_RegionSector {
		return idx.cached_FacilityRef_RegionSector_s[ref.nthOfType()]
	}
	lng, lat, ok := ref.GetLngLat()
	if !ok {
		return ottregions.SectorUnknown
	}
	return ottregions.SectorAt(float64(lat), float64(lng))
}

// GuessReservationRequirement attempts to guess if reservations are required:
func (ref ActivityRef) GuessReservationRequirement() (required bool, definite bool) {
	if idx := ref.index(); idx.cached_ActivityRef_GuessReservationRequirement {
		required = ref.idx.cached_ActivityRef_GuessReservationRequirement_required.Contains(ref.object())
		definite = ref.idx.cached_ActivityRef_GuessReservationRequirement_definite.Contains(ref.object())
		return
	}

	actResv, actResvExplicit := ref.GetResv()

	if actResvExplicit {
		// stated explicitly in the activity label
		required = actResv
		definite = true
		return
	}

	grp := ref.ScheduleGroup()

	grpNoResv := grp.GetNoResv()

	var grpHasLink bool
	for range grp.GetReservationLinks() {
		grpHasLink = true
		break
	}

	var grpExplicitYes, grpExplicitNo bool
	for e := range grp.Activities() {
		if resv, ok := e.GetResv(); ok {
			if resv {
				grpExplicitYes = true
			} else {
				grpExplicitNo = true
			}
			if grpExplicitYes && grpExplicitNo {
				break
			}
		}
	}

	if grpNoResv {
		// if the group explicitly states reservations not required at the
		// top-level, go with that, and count it as definite if nothing else
		// implies it might be a mistake (like the presence of reservation links
		// with no explicit reservation requirement text)
		required = false
		definite = !(grpHasLink && !grpExplicitYes)
		return
	}

	if !grpExplicitYes && !grpExplicitNo {
		// if none are explicitly marked, assume we need reservation if and only
		// if we have a link
		required = grpHasLink
		definite = false
		return
	}

	if grpExplicitYes && grpExplicitNo {
		// if we have a link and the ones not needing it and ones not needing
		// are explicitly marked, but we aren't, it's ambiguous, so assume we
		// need reservation
		required = true
		definite = false
		return
	}

	if !grpExplicitYes && grpExplicitNo {
		// if the ones not needing it are explicitly marked, we definitely need
		// a reservation if we have a link, and might need if if we don't
		required = true
		definite = grpHasLink
		return
	}

	if grpExplicitYes && !grpExplicitNo {
		// if the ones needing it are explicitly marked, we probably don't need
		// a reservation
		required = false
		definite = false
		return
	}

	// this should have covered all cases, but assume not just in case
	panic("wtf")
}

// ComputeEffectiveDateRange attempts to compute a date range for the schedule,
// starting at from until to (inclusive). If a side is open, it will be
// [schema.Date.IsZero]. If the range is ambiguous or missing, ok will be false.
func (ref ScheduleRef) ComputeEffectiveDateRange() (er schema.DateRange, ok bool) {
	if idx := ref.index(); idx.cached_ScheduleRef_ComputeEffectiveDateRange {
		i := ref.nthOfType()
		er = ref.idx.cached_ScheduleRef_ComputeEffectiveDateRange_er[i]
		ok = (er != schema.DateRange{From: -1, To: -1})
		return
	}

	// note: -1 is invalid, 0 is open
	defer func() {
		if ok != (er != schema.DateRange{From: -1, To: -1}) {
			panic("wtf")
		}
	}()

	// get the schedule date
	var scheduleDate time.Time
	if t := ref.index().Updated(); !t.IsZero() {
		scheduleDate = t
	}
	if t := ref.Facility().GetSourceDate(); !t.IsZero() {
		scheduleDate = t
	}

	// get the parsed date range
	r, ok := ref.GetDateRange()
	if !ok {
		// some holiday and short-term schedules don't have a date in the
		// caption... if all columns are single dates (potentially with gaps),
		// it implies a date range (this isn't criticial, but it makes
		// filtering work better)
		r, ok = ref.impliedSingleDateRange()
		if !ok {
			return schema.DateRange{From: -1, To: -1}, false
		}
	}

	var hadExplicitFromOrToYear bool

	// parse the from date
	if x := r.From; !x.IsZero() {
		var (
			year, yearOK   = x.Year()
			month, monthOK = x.Month()
			day, dayOK     = x.Day()
		)
		// if it's not valid, skip it
		if !x.IsValid() {
			return schema.DateRange{From: -1, To: -1}, false
		}
		// if there's no month set, skip it
		if !monthOK {
			return schema.DateRange{From: -1, To: -1}, false
		}
		// if there's no year set, use the schedule year
		if !yearOK {
			if scheduleDate.IsZero() {
				return schema.DateRange{From: -1, To: -1}, false // no current year
			}
			year, yearOK = scheduleDate.Year(), true
		} else {
			hadExplicitFromOrToYear = true
		}
		// if there's no day set, use 1
		if !dayOK {
			day, dayOK = 1, true
		}
		// compute the date
		er.From = schema.MakeDate(year, month, day, weekday(year, month, day, TZ))
	} else {
		// open range
		er.From = 0
	}

	// parse the to date
	if x := r.To; !x.IsZero() {
		var (
			year, yearOK   = x.Year()
			month, monthOK = x.Month()
			day, dayOK     = x.Day()
		)
		// if it's not valid, skip it
		if !x.IsValid() {
			return schema.DateRange{From: -1, To: -1}, false
		}
		// if there's no month set, and there's no year or the from year is equal, use the from month
		if !monthOK && !er.From.IsZero() && (!yearOK || expectOK(er.From.Year()) == year) {
			month, monthOK = expectOK(er.From.Month()), true
		}
		// if there's still no month set, skip it
		if !monthOK {
			return schema.DateRange{From: -1, To: -1}, false
		}
		// if there's no year set, figure it out
		if !yearOK {
			// from the from date (or the schedule date if no from)
			if !er.From.IsZero() {
				year, yearOK = expectOK(er.From.Year()), true
			} else {
				if scheduleDate.IsZero() {
					return schema.DateRange{From: -1, To: -1}, false
				}
				year, yearOK = scheduleDate.Year(), true
			}
			// if the year is the same as the from one (or the schedule one if
			// no from), and the month is in the past, increase the year (we
			// don't want to be too general about this and just check if from is
			// after to as that could allow typos)
			if !er.From.IsZero() && expectOK(er.From.Year()) == year {
				if month < expectOK(er.From.Month()) {
					year++
				}
			} else if !scheduleDate.IsZero() && scheduleDate.Year() == year {
				if month < scheduleDate.Month() {
					year++
				}
			}
		} else {
			hadExplicitFromOrToYear = true
		}
		// if there's no day set, use the last day of the month
		if !dayOK {
			day, dayOK = daysInMonth(year, month), true
		}
		// compute the date
		er.To = schema.MakeDate(year, month, day, weekday(year, month, day, TZ))
	} else {
		// open range
		er.To = 0
	}

	// to handle cases like (note: 2025-12-24 is a good dataset to test on):
	//  - schedule date "2025-12-19"
	//  - one schedule "September 15 to December 21" (implied 2025)
	//  - another schedule "January 3 to March 22" (implied 2026)
	//
	// if all the following are true:
	//  - neither the schedule from nor to had an explicit year
	//  - the start date assuming the schedule year is in the past
	//  - the start date being after the current schedule year would be less
	//    than an arbitrary threshold of 6 months from the current schedule date
	//  - the end date being the current schedule year would be further from the
	//    schedule date than the distance of the start date being after the
	//    current schedule date (to account for cases where the schedule date
	//    spans the majority of a year)
	//  - if we don't have an end date, then take the one closest to the
	//    schedule date instead
	//
	// then increment the assumed year by one
	from, ok := er.From.GoTime(TZ)
	if !ok {
		panic("wtf")
	}
	to, ok := er.To.GoTime(TZ)
	if !ok {
		panic("wtf")
	}
	if !hadExplicitFromOrToYear && !er.From.IsZero() {
		if fromInPast := from.Before(scheduleDate); fromInPast {
			// note: since we had no explicit year, this will always be the schedule year + 1
			fromInc := from.AddDate(1, 0, 0)
			if toNewFrom := fromInc.Sub(scheduleDate); toNewFrom < 6*30*24*time.Hour { // approximate (and we're assuming that they wouldn't post a schedule more than 6 months ahead or leave an old schedule up that long)
				if !to.IsZero() {
					if toOldTo := abs(scheduleDate.Sub(to)); toOldTo > toNewFrom {
						// note: since we had no explicit year, this will always the same as the from year, or the next if the day/month is earlier
						toInc := to.AddDate(1, 0, 0)
						from, to = fromInc, toInc
					}
				} else {
					if toOldFrom := scheduleDate.Sub(from); toNewFrom < toOldFrom {
						from = fromInc
					}
				}
			}
		}
	}

	// if the year was assumed from the schedule date (especially if the range
	// was assumed by [ScheduleRef.impliedSingleDateRange]), it might be one too
	// high if the range spans a year (e.g., Dec 29 to Jan 4) once into the next
	// one, so subtract a year from both sides if doing so would make the range
	// cover the schedule date
	if !hadExplicitFromOrToYear && !er.From.IsZero() && !er.To.IsZero() {
		if scheduleDate.Before(from) || scheduleDate.After(to) {
			fromDec, toDec := from.AddDate(-1, 0, 0), to.AddDate(-1, 0, 0)
			if !scheduleDate.Before(fromDec) && !scheduleDate.After(toDec) {
				from, to = fromDec, toDec
			}
		}
	}

	// ... and so it doesn't jump forwards afterwards if the city doesn't remove
	// it immediately, if it wouldn't cover the schedule date either way and
	// subtracting a year would make the end date at least 6 months closer (so
	// it doesn't accidentally apply to one in the near future assuming they
	// remove the old schedule within 6 months and also don't post new schedules
	// that much earlier; the longest they've axtually done that from Sep
	// 2025-2026 is 37 days), also do that
	if !hadExplicitFromOrToYear && !er.From.IsZero() && !er.To.IsZero() {
		if scheduleDate.Before(from) || scheduleDate.After(to) {
			fromDec, toDec := from.AddDate(-1, 0, 0), to.AddDate(-1, 0, 0)
			if abs(scheduleDate.Sub(toDec))+6*30*24*time.Hour <= abs(scheduleDate.Sub(from)) { // approximate, as above
				from, to = fromDec, toDec
			}
		}
	}

	er.From = schema.MakeDateFromGo(from)
	er.To = schema.MakeDateFromGo(to)

	// if the range is empty, skip it
	if er.From.IsZero() && er.To.IsZero() {
		return schema.DateRange{From: -1, To: -1}, false
	}

	// if the range is backwards, skip it (but not if one side is zero)
	if !er.From.IsZero() && !er.To.IsZero() && from.After(to) {
		return schema.DateRange{From: -1, To: -1}, false
	}

	// otherwise, return it
	return er, true
}

// impliedSingleDateRange returns the date range implied by the day headers if
// all day headers specify at least a month and day (i.e., a holiday or
// short-term schedule).
//
// It uses the first/last column rather than the min/max date so ones spanning
// years are correct (from Sep 2025-2026, the city has never not ordered the
// table chronologically).
//
// If there is a gap greater than 7 days, it does not return the assumed range
// (since it might mean the days aren't actually in chronological order or span
// years unexpectedly).
func (ref ScheduleRef) impliedSingleDateRange() (r schema.DateRange, ok bool) {
	n := ref.NumDays()
	if n == 0 {
		return r, false
	}
	var prev time.Time
	for i := range n {
		d, ok := ref.GetDayDate(i)
		if !ok {
			return schema.DateRange{}, false
		}
		month, ok := d.Month()
		if !ok {
			return schema.DateRange{}, false
		}
		day, ok := d.Day()
		if !ok {
			return schema.DateRange{}, false
		}

		// check the gap (adding a year if backwards)
		cur := time.Date(1, month, day, 0, 0, 0, 0, time.UTC)
		if i > 0 {
			if cur.Before(prev) {
				cur = cur.AddDate(1, 0, 0)
			}
			if cur.Sub(prev) > 7*24*time.Hour {
				return schema.DateRange{}, false
			}
		}
		prev = cur

		if i == 0 {
			r.From = d
		}
		if i == n-1 {
			r.To = d
		}
	}
	return r, true
}

// SingleDayDate returns true and a date if the [ScheduleRef.GetDayDate] index
// represents a single date rather than a weekday. This should be given more
// precedence than [ScheduleRef.ComputeEffectiveDateRange], as they sometimes
// make mistakes in the date range for the special short-term schedules, but
// still put the correct date in the day header.
//
// This is intended for more accurate filtering. If you just want to display the
// day date header for all times in a day, you probably want to use
// [ScheduleRef.GetDayDate], which includes whichver of day/month/year/weekday
// were explicitly specified in the scraped column header.
//
// The returned date will be valid with all components set (year, month, day,
// weekday) if ok is true.
func (ref ScheduleRef) SingleDayDate(i int) (schema.Date, bool) {
	d, ok := ref.GetDayDate(i)
	if !ok {
		return -1, false
	}

	if idx := ref.index(); idx.cached_ScheduleRef_SingleDayDate {
		j := ref.nthOfType()
		t := ref.idx.cached_ScheduleRef_SingleDayDate_t[j][i] // GetDayDate already checked the index
		if t <= 0 {
			return -1, false
		}
		return t, true
	}

	month, hasMonth := d.Month()
	if !hasMonth {
		return -1, false
	}

	day, hasDay := d.Day()
	if !hasDay {
		return -1, false
	}

	year, hasYear := d.Year()
	if !hasYear {
		if er, ok := ref.ComputeEffectiveDateRange(); ok {
			if er.From.IsZero() || er.To.IsZero() || expectOK(er.From.Year()) == expectOK(er.To.Year()) {
				// assume whichever year we have
				if er.From.IsZero() {
					year, hasYear = expectOK(er.To.Year()), true
				} else {
					year, hasYear = expectOK(er.From.Year()), true
				}
			} else {
				// expectOK is fine since we already checked for IsZero and ComputeEffectiveDateRange returns full dates
				fromYear, fromMonth, fromDay := expectOK(er.From.Year()), expectOK(er.From.Month()), expectOK(er.From.Day())
				toYear, toMonth, toDay := expectOK(er.To.Year()), expectOK(er.To.Month()), expectOK(er.To.Day())
				if fromYear+1 == toYear {
					// assume the from year if we're not before that date, otherwise the to year, as long as it's one more than the from year
					if (month < fromMonth || (month == fromMonth && day < fromDay)) && (month < toMonth || (month == toMonth && day < toDay)) {
						year, hasYear = fromYear+1, true
					} else {
						year, hasYear = fromYear, true
					}
				}
			}
		}
	}
	if !hasYear {
		return -1, false
	}

	return schema.MakeDate(year, month, day, weekday(year, month, day, TZ)), true
}

// SingleDate is a helper for getting [ScheduleRef.SingleDayDate] for the time.
// See the documentation for [ScheduleRef.SingleDayDate] for more information.
func (ref TimeRef) SingleDate() (schema.Date, bool) {
	return ref.Schedule().SingleDayDate(ref.GetScheduleDayIndex())
}

// LikelyHolidaySchedule returns true (on a best-effort basis) if this schedule
// contains at least one single date rather than a weekday name. This heuristic
// is usually pretty good for detecting holiday schedules, but does not catch
// all of them (e.g., some holiday schedules will use regular weekdays over a
// short time range rather than explicit dates).
func (ref ScheduleRef) LikelyHolidaySchedule() bool {
	for i := range ref.NumDays() {
		if _, ok := ref.SingleDayDate(i); ok {
			return true
		}
	}
	return false
}

func expectOK[T any](x T, ok bool) T {
	if !ok {
		panic("wtf")
	}
	return x
}

func weekday(year int, month time.Month, day int, loc *time.Location) time.Weekday {
	return time.Date(year, month, day, 0, 0, 0, 0, loc).Weekday()
}

func daysInMonth(year int, month time.Month) int {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

func abs[T ~int | ~int8 | ~int16 | ~int32 | ~int64](x T) T {
	if x < 0 {
		return -x
	}
	return x
}
