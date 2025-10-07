package ottrecidx

import (
	"time"
)

// this file contains additional helpers to perform computations on refs, possibly with optimizations

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
// [time.Time.IsZero].  If the range is ambiguous or missing, ok will be false.
func (ref ScheduleRef) ComputeEffectiveDateRange() (from time.Time, to time.Time, ok bool) {
	if idx := ref.index(); idx.cached_ScheduleRef_ComputeEffectiveDateRange {
		i := ref.nthOfType()
		from = ref.idx.cached_ScheduleRef_ComputeEffectiveDateRange_from[i]
		to = ref.idx.cached_ScheduleRef_ComputeEffectiveDateRange_to[i]
		ok = ref.idx.cached_ScheduleRef_ComputeEffectiveDateRange_ok.Contains(ref.object())
		return
	}

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
		return from, to, false
	}

	// parse the from date
	if x := r.From; !x.IsZero() {
		var (
			year, yearOK   = x.Year()
			month, monthOK = x.Month()
			day, dayOK     = x.Day()
		)
		// if it's not valid, skip it
		if !x.IsValid() {
			return from, to, false
		}
		// if there's no month set, skip it
		if !monthOK {
			return from, to, false
		}
		// if there's no year set, use the schedule year
		if !yearOK {
			if scheduleDate.IsZero() {
				return from, to, false // no current year
			}
			year, yearOK = scheduleDate.Year(), true
		}
		// if there's no day set, use 1
		if !dayOK {
			day, dayOK = 1, true
		}
		// compute the date
		from = time.Date(year, month, day, 0, 0, 0, 0, TZ)
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
			return from, to, false
		}
		// if there's no month set, and there's no year or the from year is equal, use the from month
		if !monthOK && !from.IsZero() && (!yearOK || from.Year() == year) {
			month, monthOK = from.Month(), true
		}
		// if there's still no month set, skip it
		if !monthOK {
			return from, to, false
		}
		// if there's no year set, figure it out
		if !yearOK {
			// from the from date (or the schedule date if no from)
			if !from.IsZero() {
				year, yearOK = from.Year(), true
			} else {
				if scheduleDate.IsZero() {
					return from, to, false
				}
				year, yearOK = scheduleDate.Year(), true
			}
			// if the year is the same as the from one (or the schedule one if
			// no from), and the month is in the past, increase the year (we
			// don't want to be too general about this and just check if from is
			// after to as that could allow typos)
			if !from.IsZero() && from.Year() == year {
				if month < from.Month() {
					year++
				}
			} else if !scheduleDate.IsZero() && scheduleDate.Year() == year {
				if month < scheduleDate.Month() {
					year++
				}
			}
		}
		// if there's no day set, use the last day of the month
		if !dayOK {
			day, dayOK = daysInMonth(year, month), true
		}
		// compute the date
		to = time.Date(year, month, day+1, 0, 0, 0, 0, TZ).Add(-time.Nanosecond)
	}

	// if the range is empty, skip it
	if from.IsZero() && to.IsZero() {
		return from, to, false
	}

	// if the range is backwards, skip it
	if from.After(to) {
		return from, to, false
	}

	// otherwise, return it
	return from, to, true
}

// SingleDate returns true and a date if the activity date represents a single
// date rather than a weekday. This should be given more precedence than
// [ScheduleRef.ComputeEffectiveDateRange], as they sometimes make mistakes in
// the date range for the special short-term schedules, but still put the
// correct date in the day header.
func (ref TimeRef) SingleDate() (time.Time, bool) {
	sch := ref.Schedule()

	d, ok := sch.GetDayDate(ref.GetScheduleDayIndex())
	if !ok {
		return time.Time{}, false
	}

	month, hasMonth := d.Month()
	if !hasMonth {
		return time.Time{}, false
	}

	day, hasDay := d.Day()
	if !hasDay {
		return time.Time{}, false
	}

	year, hasYear := d.Year()
	if !hasYear {
		if from, to, ok := sch.ComputeEffectiveDateRange(); ok {
			if from.IsZero() || to.IsZero() || from.Year() == to.Year() {
				// assume whichever year we have
				if from.IsZero() {
					year, hasYear = to.Year(), true
				} else {
					year, hasYear = from.Year(), true
				}
			} else {
				fromYear, fromMonth, fromDay := from.Date()
				toYear, toMonth, toDay := from.Date()
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
		return time.Time{}, false
	}

	return time.Date(year, month, day, 0, 0, 0, 0, TZ), true

}

func daysInMonth(year int, month time.Month) int {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}
