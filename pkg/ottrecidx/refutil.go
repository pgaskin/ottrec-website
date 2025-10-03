package ottrecidx

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
