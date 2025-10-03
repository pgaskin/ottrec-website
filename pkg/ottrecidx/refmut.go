package ottrecidx

// this file contains the user-facing interface to create filtered copies of the indexed data

// MutableDataRef lets you create a mutated DataRef.
type MutableDataRef struct {
	// mutating this while using it directly is only safe if you do not keep old
	// refs around, and you only remove refs you have already iterated over
	// (c.f., how the bitmap range func doesn't re-access the underling uint64s
	// for every bit)
	unsafe DataRef
}

// Mutate allows you to mutate a copy of ref.
func (ref DataRef) Mutate() MutableDataRef {
	return MutableDataRef{DataRef{ref.withFilter()}}
}

// Data returns a copy of the ref with the mutations applied.
func (mut MutableDataRef) Data() DataRef {
	// the copy is important so future mutations can't break references made
	// from the returned ref
	return DataRef{mut.unsafe.withFilter()}
}

// mutRemoveRef clears filter bits in mut from the start of ref up to and not
// including the next of its type or any parent type, returning true if ref was
// present to be removed.
func mutRemoveRef[T schemaObj](mut *MutableDataRef, ref typedRef[T]) bool {
	if mut.unsafe.idx != ref.idx {
		return false // different indexes, or ref is nil
	}
	// check and start at ref
	start := ref.object()
	if start.isSpecial() {
		panic("wtf")
	}
	// if it's already removed, it won't have children
	if !mut.unsafe.flt.Contains(start) {
		return false
	}
	// find the end of ref's children, otherwise the reset of the objects
	var until refObj
	if next, ok := ref.typeNotChildBitmap().Next(start + 1); ok {
		until = next // next sibling or a different parent
	} else {
		until = refObj(len(ref.idx.obj)) // end
	}
	// TODO: optimize
	// remove it and all remaining children
	for obj := start; obj < until; obj++ {
		mut.unsafe.flt.Remove(obj)
	}
	return true
}
func (mut *MutableDataRef) RemoveFacility(ref FacilityRef) bool {
	return mutRemoveRef(mut, ref.typedRef)
}
func (mut *MutableDataRef) RemoveScheduleGroup(ref ScheduleGroupRef) bool {
	return mutRemoveRef(mut, ref.typedRef)
}
func (mut *MutableDataRef) RemoveSchedule(ref ScheduleRef) bool {
	return mutRemoveRef(mut, ref.typedRef)
}
func (mut *MutableDataRef) RemoveActivity(ref ActivityRef) bool {
	return mutRemoveRef(mut, ref.typedRef)
}
func (mut *MutableDataRef) RemoveTime(ref TimeRef) bool {
	return mutRemoveRef(mut, ref.typedRef)
}

// note: this in-place removal during iteration is only safe since we're only
// removing one on or before the current one at any point in the iteration

func (mut *MutableDataRef) FilterFacilities(fn func(ref FacilityRef) bool) int {
	var n int
	for ref := range mut.unsafe.Facilities() {
		if !fn(ref) {
			if !mut.RemoveFacility(ref) {
				panic("wtf") // it should never fail to remove something we know is there
			}
			n++
		}
	}
	return n
}
func (mut *MutableDataRef) FilterScheduleGroups(fn func(ref ScheduleGroupRef) bool) int {
	var n int
	for ref := range mut.unsafe.ScheduleGroups() {
		if !fn(ref) {
			if !mut.RemoveScheduleGroup(ref) {
				panic("wtf") // it should never fail to remove something we know is there
			}
			n++
		}
	}
	return n
}
func (mut *MutableDataRef) FilterSchedules(fn func(ref ScheduleRef) bool) int {
	var n int
	for ref := range mut.unsafe.Schedules() {
		if !fn(ref) {
			if !mut.RemoveSchedule(ref) {
				panic("wtf") // it should never fail to remove something we know is there
			}
			n++
		}
	}
	return n
}
func (mut *MutableDataRef) FilterActivities(fn func(ref ActivityRef) bool) int {
	var n int
	for ref := range mut.unsafe.Activities() {
		if !fn(ref) {
			if !mut.RemoveActivity(ref) {
				panic("wtf") // it should never fail to remove something we know is there
			}
			n++
		}
	}
	return n
}
func (mut *MutableDataRef) FilterTimes(fn func(ref TimeRef) bool) int {
	var n int
	for ref := range mut.unsafe.Times() {
		if !fn(ref) {
			if !mut.RemoveTime(ref) {
				panic("wtf") // it should never fail to remove something we know is there
			}
			n++
		}
	}
	return n
}

func (mut *MutableDataRef) Elide() {
	mut.ElideActivities()
	mut.ElideSchedules()
	mut.ElideScheduleGroups()
	mut.ElideFacilities()
}
func (mut *MutableDataRef) ElideFacilities() int {
	var n int
	for x := range mut.unsafe.Facilities() {
		if x.ScheduleGroups().Empty() {
			mut.RemoveFacility(x)
			n++
		}
	}
	return n
}
func (mut *MutableDataRef) ElideScheduleGroups() int {
	var n int
	for x := range mut.unsafe.ScheduleGroups() {
		if x.Schedules().Empty() {
			mut.RemoveScheduleGroup(x)
			n++
		}
	}
	return n
}
func (mut *MutableDataRef) ElideSchedules() int {
	var n int
	for x := range mut.unsafe.Schedules() {
		if x.Activities().Empty() {
			mut.RemoveSchedule(x)
			n++
		}
	}
	return n
}
func (mut *MutableDataRef) ElideActivities() int {
	var n int
	for x := range mut.unsafe.Activities() {
		if x.Times().Empty() {
			mut.RemoveActivity(x)
			n++
		}
	}
	return n
}
