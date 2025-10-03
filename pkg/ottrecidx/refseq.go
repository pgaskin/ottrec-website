package ottrecidx

import (
	"iter"
	"slices"
	"time"

	"github.com/pgaskin/ottrec/schema"
)

// this file implements higher-level operations on schema object iterators

type (
	FacilitySeq      iter.Seq[FacilityRef]
	ScheduleGroupSeq iter.Seq[ScheduleGroupRef]
	ScheduleSeq      iter.Seq[ScheduleRef]
	ActivitySeq      iter.Seq[ActivityRef]
	TimeSeq          iter.Seq[TimeRef]
)

func facilitySeq(seq iter.Seq[typedRef[xFacility]]) FacilitySeq {
	return func(yield func(FacilityRef) bool) {
		for ref := range seq {
			if !yield(FacilityRef{ref}) {
				return
			}
		}
	}
}

func scheduleGroupSeq(seq iter.Seq[typedRef[xScheduleGroup]]) ScheduleGroupSeq {
	return func(yield func(ScheduleGroupRef) bool) {
		for ref := range seq {
			if !yield(ScheduleGroupRef{ref}) {
				return
			}
		}
	}
}

func scheduleSeq(seq iter.Seq[typedRef[xSchedule]]) ScheduleSeq {
	return func(yield func(ScheduleRef) bool) {
		for ref := range seq {
			if !yield(ScheduleRef{ref}) {
				return
			}
		}
	}
}

func activitySeq(seq iter.Seq[typedRef[xActivity]]) ActivitySeq {
	return func(yield func(ActivityRef) bool) {
		for ref := range seq {
			if !yield(ActivityRef{ref}) {
				return
			}
		}
	}
}

func timeSeq(seq iter.Seq[typedRef[xTime]]) TimeSeq {
	return func(yield func(TimeRef) bool) {
		for ref := range seq {
			if !yield(TimeRef{ref}) {
				return
			}
		}
	}
}

func (seq FacilitySeq) Iter() iter.Seq[FacilityRef]           { return iter.Seq[FacilityRef](seq) }
func (seq ScheduleGroupSeq) Iter() iter.Seq[ScheduleGroupRef] { return iter.Seq[ScheduleGroupRef](seq) }
func (seq ScheduleSeq) Iter() iter.Seq[ScheduleRef]           { return iter.Seq[ScheduleRef](seq) }
func (seq ActivitySeq) Iter() iter.Seq[ActivityRef]           { return iter.Seq[ActivityRef](seq) }
func (seq TimeSeq) Iter() iter.Seq[TimeRef]                   { return iter.Seq[TimeRef](seq) }

func iterEmpty[T any](seq iter.Seq[T]) bool {
	for range seq {
		return false
	}
	return true
}

// TODO: optimize this to use the bitmap directly
func (seq FacilitySeq) Empty() bool      { return iterEmpty(seq.Iter()) }
func (seq ScheduleGroupSeq) Empty() bool { return iterEmpty(seq.Iter()) }
func (seq ScheduleSeq) Empty() bool      { return iterEmpty(seq.Iter()) }
func (seq ActivitySeq) Empty() bool      { return iterEmpty(seq.Iter()) }
func (seq TimeSeq) Empty() bool          { return iterEmpty(seq.Iter()) }

// TODO: optimize this to use the bitmap directly
func (seq FacilitySeq) Len() int      { return iterCount(seq.Iter()) }
func (seq ScheduleGroupSeq) Len() int { return iterCount(seq.Iter()) }
func (seq ScheduleSeq) Len() int      { return iterCount(seq.Iter()) }
func (seq ActivitySeq) Len() int      { return iterCount(seq.Iter()) }
func (seq TimeSeq) Len() int          { return iterCount(seq.Iter()) }

func (seq TimeSeq) Weekday(includeUnknown bool, or ...time.Weekday) TimeSeq {
	return TimeSeq(func(yield func(TimeRef) bool) {
		for tm := range seq {
			w, ok := tm.GetWeekday()
			if !ok && !includeUnknown {
				continue
			}
			if ok && !slices.Contains(or, w) {
				continue
			}
			if !yield(tm) {
				return
			}
		}
	})
}

func (seq TimeSeq) Overlapping(includeUnknown bool, or ...schema.ClockRange) TimeSeq {
	return TimeSeq(func(yield func(TimeRef) bool) {
		for tm := range seq {
			r, ok := tm.GetRange()
			if !ok && !includeUnknown {
				continue
			}
			if ok && !slices.ContainsFunc(or, r.Overlaps) {
				continue
			}
			if !yield(tm) {
				return
			}
		}
	})
}
