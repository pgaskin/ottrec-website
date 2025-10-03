package ottrecidx

import (
	"iter"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/pgaskin/ottrec/schema"
)

// this file contains the user-facing interface to the indexed data

// we use getters and iterators to ensure stuff can't be directly mutated by
// accident, and to give us more flexibility about the underlying implementation
// for future optimization

// a filter must never mask out parent schema objects without also masking out
// the children (this helps keep the logic easy to reason about and reduces the
// chance of having subtle bugs which don't panic)

// in general, it should not be possible for a user of the package to obtain an
// invalid ref using the provided APIs except where it's the result of trying to
// find a non-existent obj or something similar

// all of these nested structs and generics seems complicated, but it's much
// less error prone as we can enforce almost everything except for parent-child
// relationships though the type system.

// refObj is an index into the objects array, or a high value for special
// objects not directly part of the schedule tree.
type refObj uint32

const (
	refObjTest refObj = math.MaxUint32 - iota // not actually used, just a placeholder for if we need this in the future
	refObjSpecialMin
)

func (obj refObj) String() string {
	switch obj {
	case refObjTest:
		return "TEST"
	}
	if obj > refObjSpecialMin {
		panic("wtf") // missing case
	}
	return strconv.FormatUint(uint64(obj), 10)
}

func (obj refObj) isSpecial() bool {
	return obj > refObjSpecialMin
}

// anyRef is an interface implemented by all levels of abstraction of a reference.
type anyRef interface {
	reflect() baseRef
}

// baseRef contains the underlying structure of a reference.
type baseRef struct {
	idx *Index
	flt bitmap[refObj]
	obj refObj
}

func (ref baseRef) reflect() baseRef {
	return ref
}

// Valid returns true if the ref is not a nil reference. If the ref is not
// valid, this (and String/GoString) are the only functions which can be safely
// called without panicking.
func (ref baseRef) Valid() bool {
	return ref.idx != nil
}

// index gets the underlying index the ref came from.
func (ref baseRef) index() *Index {
	if !ref.Valid() {
		panic("cannot get index of nil reference")
	}
	return ref.idx
}

// object gets the object the ref points to.
func (ref baseRef) object() refObj {
	if !ref.Valid() {
		panic("cannot get object of nil reference")
	}
	return ref.obj
}

// applyFilter returns a newly allocated bitmap of the specified bitmap AND'd with
// the applyFilter.
func (ref baseRef) applyFilter(b bitmap[refObj]) bitmap[refObj] {
	b = b.Clone(nil)
	if !ref.flt.IsNil() {
		b.And(ref.flt)
	}
	return b
}

// withFilter returns a copy of ref with a clone of the withFilter, or a new
// filter including everything.
func (ref baseRef) withFilter() baseRef {
	if ref.flt.IsNil() {
		ref.flt = makeBitmap[refObj](len(ref.index().obj))
		ref.flt.Ones()
	} else {
		ref.flt = ref.flt.Clone(nil)
	}
	return ref
}

// deref returns the schema object the ref points to.
func (ref baseRef) deref() any {
	if !ref.Valid() {
		panic("cannot deref nil reference")
	}
	if ref.obj.isSpecial() {
		switch ref.obj {
		//case refObjTest:
		//	return ref.idx.test
		}
		panic("wtf: missing special case in deref")
	}
	if !ref.flt.IsNil() && !ref.flt.Contains(ref.obj) {
		// at first, you might think this shouldn't be a panic, and should
		// be a supported condition, but consider that if we didn't have all
		// this ref stuff, it'd essentially be like trying to access a
		// deleted item
		panic("invalid ref: references filtered obj") // maybe someone accidentally mutated the filter in-place, or maybe they forgot to apply the filter?
	}
	return ref.idx.obj[ref.obj]
}

// typedRef extends [baseRef] with type-safe helpers and additional checks.
type typedRef[T schemaObj] struct {
	baseRef
}

// typeBitmap returns the index bitmap for objects of the specified type, or a nil
// bitmap if it's a special object.
func typeBitmap[T schemaObj](idx *Index) bitmap[refObj] {
	switch any((*T)(nil)).(type) {
	case *xData:
		return idx.bData
	case *xFacility:
		return idx.bFacility
	case *xScheduleGroup:
		return idx.bScheduleGroup
	case *xSchedule:
		return idx.bSchedule
	case *xActivity:
		return idx.bActivity
	case *xTime:
		return idx.bTime
	}
	return nilBitmap[refObj]()
}

// typeNotChildBitmap returns the bitmap of all objects at or above the
// specified type..
func typeNotChildBitmap[T schemaObj](idx *Index) bitmap[refObj] {
	switch any((*T)(nil)).(type) {
	case *xData:
		return idx.bDataNotChild
	case *xFacility:
		return idx.bFacilityNotChild
	case *xScheduleGroup:
		return idx.bScheduleGroupNotChild
	case *xSchedule:
		return idx.bScheduleNotChild
	case *xActivity:
		return idx.bActivityNotChild
	case *xTime:
		return idx.bTimeNotChild
	}
	return nilBitmap[refObj]()
}

// reference checks and creates a reference from an existing reference.
func reference[T schemaObj](ref anyRef, obj refObj) typedRef[T] {
	oref := ref.reflect()
	_ = oref.deref()
	nref := typedRef[T]{baseRef: baseRef{oref.idx, oref.flt, obj}}
	_ = nref.deref()
	return nref
}

// deref returns the schema object the ref points to.
func (ref typedRef[T]) deref() *T {
	v := ref.baseRef.deref()
	if !ref.obj.isSpecial() && !ref.typeBitmap().Contains(ref.obj) {
		panic("invalid ref: obj type doesn't match typedRef type")
	}
	x, ok := v.(*T)
	if !ok {
		panic("wtf: inconsistent index bitmap or baseRef.deref implementation")
	}
	return x
}

// typeBitmap returns the bitmap of all other objects of the current type.
func (ref typedRef[T]) typeBitmap() bitmap[refObj] {
	return typeBitmap[T](ref.index())
}

// typeNotChildBitmap returns the bitmap of all objects at or above the
// current type..
func (ref typedRef[T]) typeNotChildBitmap() bitmap[refObj] {
	return typeNotChildBitmap[T](ref.index())
}

// withFilter returns a copy of ref with a clone of the filter, or a new filter
// including everything.
func (ref typedRef[T]) withFilter() typedRef[T] {
	return typedRef[T]{ref.baseRef.withFilter()}
}

// Boxed typedRefs with exposed getters, setters, and iterators.
type (
	DataRef          struct{ typedRef[xData] }
	FacilityRef      struct{ typedRef[xFacility] }
	ScheduleGroupRef struct{ typedRef[xScheduleGroup] }
	ScheduleRef      struct{ typedRef[xSchedule] }
	ActivityRef      struct{ typedRef[xActivity] }
	TimeRef          struct{ typedRef[xTime] }
)

func (ref DataRef) Index() *Index          { return ref.index() }
func (ref FacilityRef) Index() *Index      { return ref.index() }
func (ref ScheduleGroupRef) Index() *Index { return ref.index() }
func (ref ScheduleRef) Index() *Index      { return ref.index() }
func (ref ActivityRef) Index() *Index      { return ref.index() }
func (ref TimeRef) Index() *Index          { return ref.index() }

func (ref DataRef) GetAttribution() iter.Seq[string] { return slices.Values(ref.deref().Attribution) }

func (ref FacilityRef) GetName() string          { return ref.deref().Name }
func (ref FacilityRef) GetSourceURL() string     { return ref.deref().SourceURL }
func (ref FacilityRef) GetSourceDate() time.Time { return ref.deref().SourceDate }
func (ref FacilityRef) GetAddress() string       { return ref.deref().Address }
func (ref FacilityRef) GetLngLat() (lng float32, lat float32, ok bool) {
	x := ref.deref()
	lng, lat = x.Longitude, x.Latitude
	ok = lng != 0 || lat != 0
	return
}
func (ref FacilityRef) GetNotificationsHTML() string { return ref.deref().NotificationsHTML }
func (ref FacilityRef) GetSpecialHoursHTML() string  { return ref.deref().SpecialHoursHTML }
func (ref FacilityRef) GetErrors() iter.Seq[string]  { return slices.Values(ref.deref().Errors) }

func (ref ScheduleGroupRef) GetLabel() string { return ref.deref().Label }
func (ref ScheduleGroupRef) GetTitle() string { return ref.deref().Title }
func (ref ScheduleGroupRef) GetReservationLinks() iter.Seq[ReservationLink] {
	return slices.Values(ref.deref().ReservationLinks)
}
func (ref ScheduleGroupRef) GetScheduleChangesHTML() string { return ref.deref().ScheduleChangesHTML }

func (ref ScheduleRef) GetCaption() string { return ref.deref().Caption }
func (ref ScheduleRef) GetName() string    { return ref.deref().Name }
func (ref ScheduleRef) GetDate() string    { return ref.deref().Date }
func (ref ScheduleRef) GetDateRange() (schema.DateRange, bool) {
	v := ref.deref().DateRange
	return v, v.From != 0 || v.To != 0
}
func (ref ScheduleRef) NumDays() int        { return len(ref.deref().Days) }
func (ref ScheduleRef) GetDay(i int) string { return ref.deref().Days[i] }

func (ref ActivityRef) GetLabel() string { return ref.deref().Label }
func (ref ActivityRef) GetName() string  { return ref.deref().Name }
func (ref ActivityRef) GetResv() (bool, bool) {
	v := ref.deref()
	return v.Resv, v.HasResv
}

func (ref TimeRef) GetScheduleDayIndex() int { return ref.deref().ScheduleDay }
func (ref TimeRef) GetLabel() string         { return ref.deref().Label }
func (ref TimeRef) GetWeekday() (time.Weekday, bool) {
	v := ref.deref().Weekday
	return v, v != -1
}
func (ref TimeRef) GetRange() (schema.ClockRange, bool) {
	v := ref.deref().Range
	return v, v.Start != 0 || v.End != 0
}

// parentRef returns a ref to the parent of the specified object. It assumes
// that T is a child of U, and will silently misbehave if it isn't.
func parentRef[T, U schemaObj](ref typedRef[T]) typedRef[U] {
	if bm := typeBitmap[U](ref.index()); !bm.IsNil() {
		return reference[U](ref, mustOK(bm.Prev(ref.object())))
	}
	panic("cannot get parent reference of special object")
}
func (ref FacilityRef) Data() DataRef {
	return DataRef{parentRef[xFacility, xData](ref.typedRef)}
}
func (ref ScheduleGroupRef) Data() DataRef {
	return DataRef{parentRef[xScheduleGroup, xData](ref.typedRef)}
}
func (ref ScheduleGroupRef) Facility() FacilityRef {
	return FacilityRef{parentRef[xScheduleGroup, xFacility](ref.typedRef)}
}
func (ref ScheduleRef) Data() DataRef {
	return DataRef{parentRef[xSchedule, xData](ref.typedRef)}
}
func (ref ScheduleRef) Facility() FacilityRef {
	return FacilityRef{parentRef[xSchedule, xFacility](ref.typedRef)}
}
func (ref ScheduleRef) ScheduleGroup() ScheduleGroupRef {
	return ScheduleGroupRef{parentRef[xSchedule, xScheduleGroup](ref.typedRef)}
}
func (ref ActivityRef) Data() DataRef {
	return DataRef{parentRef[xActivity, xData](ref.typedRef)}
}
func (ref ActivityRef) Facility() FacilityRef {
	return FacilityRef{parentRef[xActivity, xFacility](ref.typedRef)}
}
func (ref ActivityRef) ScheduleGroup() ScheduleGroupRef {
	return ScheduleGroupRef{parentRef[xActivity, xScheduleGroup](ref.typedRef)}
}
func (ref ActivityRef) Schedule() ScheduleRef {
	return ScheduleRef{parentRef[xActivity, xSchedule](ref.typedRef)}
}
func (ref TimeRef) Data() DataRef {
	return DataRef{parentRef[xTime, xData](ref.typedRef)}
}
func (ref TimeRef) Facility() FacilityRef {
	return FacilityRef{parentRef[xTime, xFacility](ref.typedRef)}
}
func (ref TimeRef) ScheduleGroup() ScheduleGroupRef {
	return ScheduleGroupRef{parentRef[xTime, xScheduleGroup](ref.typedRef)}
}
func (ref TimeRef) Schedule() ScheduleRef {
	return ScheduleRef{parentRef[xTime, xSchedule](ref.typedRef)}
}
func (ref TimeRef) Activity() ActivityRef {
	return ActivityRef{parentRef[xTime, xActivity](ref.typedRef)}
}

// childRefSeq yields filtered references for objects of type U up to the next
// T.
func childRefSeq[T, U schemaObj](ref typedRef[T]) iter.Seq[typedRef[U]] {
	return func(yield func(typedRef[U]) bool) {
		// check and start at ref
		start := ref.object()
		if start.isSpecial() {
			panic("wtf: T is a special object")
		}
		// find the end of ref's children, otherwise the reset of the objects
		var until refObj
		if next, ok := ref.typeNotChildBitmap().Next(start + 1); ok {
			until = next // next sibling or a different parent
		} else {
			until = refObj(len(ref.idx.obj)) // end
		}
		if mask := typeBitmap[U](ref.index()); !mask.IsNil() {
			for obj := range ref.applyFilter(mask).RangeBetween(start, until) {
				if !yield(reference[U](ref, obj)) {
					return
				}
			}
		} else {
			panic("wtf: U is a special object")
		}
	}
}
func (ref DataRef) Facilities() FacilitySeq {
	return facilitySeq(childRefSeq[xData, xFacility](ref.typedRef))
}
func (ref DataRef) ScheduleGroups() ScheduleGroupSeq {
	return scheduleGroupSeq(childRefSeq[xData, xScheduleGroup](ref.typedRef))
}
func (ref DataRef) Schedules() ScheduleSeq {
	return scheduleSeq(childRefSeq[xData, xSchedule](ref.typedRef))
}
func (ref DataRef) Activities() ActivitySeq {
	return activitySeq(childRefSeq[xData, xActivity](ref.typedRef))
}
func (ref DataRef) Times() TimeSeq {
	return timeSeq(childRefSeq[xData, xTime](ref.typedRef))
}
func (ref FacilityRef) ScheduleGroups() ScheduleGroupSeq {
	return scheduleGroupSeq(childRefSeq[xFacility, xScheduleGroup](ref.typedRef))
}
func (ref FacilityRef) Schedules() ScheduleSeq {
	return scheduleSeq(childRefSeq[xFacility, xSchedule](ref.typedRef))
}
func (ref FacilityRef) Activities() ActivitySeq {
	return activitySeq(childRefSeq[xFacility, xActivity](ref.typedRef))
}
func (ref FacilityRef) Times() TimeSeq {
	return timeSeq(childRefSeq[xFacility, xTime](ref.typedRef))
}
func (ref ScheduleGroupRef) Schedules() ScheduleSeq {
	return scheduleSeq(childRefSeq[xScheduleGroup, xSchedule](ref.typedRef))
}
func (ref ScheduleGroupRef) Activities() ActivitySeq {
	return activitySeq(childRefSeq[xScheduleGroup, xActivity](ref.typedRef))
}
func (ref ScheduleGroupRef) Times() TimeSeq {
	return timeSeq(childRefSeq[xScheduleGroup, xTime](ref.typedRef))
}
func (ref ScheduleRef) Activities() ActivitySeq {
	return activitySeq(childRefSeq[xSchedule, xActivity](ref.typedRef))
}
func (ref ScheduleRef) Times() TimeSeq {
	return timeSeq(childRefSeq[xSchedule, xTime](ref.typedRef))
}
func (ref ActivityRef) Times() TimeSeq {
	return timeSeq(childRefSeq[xActivity, xTime](ref.typedRef))
}

func (ref TimeRef) GetScheduleDay() string {
	return ref.Schedule().GetDay(ref.deref().ScheduleDay)
}

func (ref ActivityRef) DayTimes(i int) TimeSeq {
	return TimeSeq(func(yield func(TimeRef) bool) {
		for tm := range ref.Times() {
			if tm.GetScheduleDayIndex() == i {
				if !yield(tm) {
					return
				}
			}
		}
	})
}

// TODO: more helpers

func mustOK[T any](x T, ok bool) T {
	if !ok {
		panic("wtf")
	}
	return x
}
