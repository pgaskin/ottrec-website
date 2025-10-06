// Package ottrecidx implements an efficient in-memory bitmap-based data
// structure for storing and querying many City of Ottawa recreation schedules.
package ottrecidx

import (
	"crypto/sha1"
	"encoding/base32"
	"iter"
	"slices"
	"time"

	"github.com/pgaskin/ottrec/schema"
	"google.golang.org/protobuf/proto"
)

// TODO: unit tests
// TODO: test round-trip back to protobuf

// this file contains the main index logic

var TZ *time.Location

func init() {
	if tz, err := time.LoadLocation("America/Toronto"); err != nil {
		panic(err)
	} else {
		TZ = tz
	}
}

// Indexer contains shared memory for indexed data. It is not safe for
// concurrent use (but the indexed schedules are).
type Indexer struct {
	idx map[string]*Index

	// most of the interning logic is quadratic complexity, but it isn't a big
	// deal for now as real data is highly dedupable and relatively low
	// cardinality
	//
	// over 192 real schedules (2025-04-14 to 2025-10-03), some example timings:
	//	- 0 Index{hash:54XKXUIV obj:3665 scan:40.106µs import:14ms precompute:1ms dataUpdated:2025-10-03}
	//	- 1 Index{hash:MELXJCMI obj:3641 scan:51.448µs import:3ms precompute:2ms dataUpdated:2025-10-02}
	//	- 2 Index{hash:AZEJHBXA obj:3641 scan:41.108µs import:3ms precompute:1ms dataUpdated:2025-10-02}
	//	- 100 Index{hash:5NL3AZHM obj:2840 scan:44.274µs import:3ms precompute:1ms dataUpdated:2025-07-08}
	//	- 150 Index{hash:ND4ZTKUS obj:3687 scan:59.834µs import:22ms precompute:1ms dataUpdated:2025-05-20}
	//	- 191 Index{hash:HONIS5GL obj:4148 scan:62.8µs import:11ms precompute:1ms dataUpdated:2025-04-14}
	init bool
	a    *arena              // this had 34946608 bytes (raw protobufs were 37376209 bytes, in-memory was more) over 2 chunks (ratio 0.016)
	sa   stringInterner      // this had 406524 bytes over 2 chunks (ratio 0.020)
	act  interner[xActivity] // this had 533 items (ratio 0.005)
	tm   interner[xTime]     // this has 3431 items (ratio 0.007)
}

type Index struct {
	a *arena // keep a pointer to it so it doesn't get finalized while we are using objects from it

	hash string

	// base object array and bitmaps
	obj            []any          // flattened data->facility[]->schedule_group[]->schedule[]->activity[]->time[]
	bData          bitmap[refObj] // will only have the first bit set since there's only one data (it's easier to do this than to special case it everywhere)
	bFacility      bitmap[refObj] // obj[i].(type) == *xFacility
	bScheduleGroup bitmap[refObj] // obj[i].(type) == *xScheduleGroup
	bSchedule      bitmap[refObj] // obj[i].(type) == *xSchedule
	bActivity      bitmap[refObj] // obj[i].(type) == *xActivity
	bTime          bitmap[refObj] // obj[i].(type) == *xTime

	// bitmaps for optimizing children queries
	bDataNotChild          bitmap[refObj] // bData
	bFacilityNotChild      bitmap[refObj] // bData|bFacility
	bScheduleGroupNotChild bitmap[refObj] // bData|bFacility|bScheduleGroup
	bScheduleNotChild      bitmap[refObj] // bData|bFacility|bScheduleGroup|bSchedule
	bActivityNotChild      bitmap[refObj] // bData|bFacility|bScheduleGroup|bSchedule|bActivity
	bTimeNotChild          bitmap[refObj] // bData|bFacility|bScheduleGroup|bSchedule|bActivity|bTime

	// precomputed: ActivityRef.GuessReservationRequirement
	cached_ActivityRef_GuessReservationRequirement          bool
	cached_ActivityRef_GuessReservationRequirement_required bitmap[refObj]
	cached_ActivityRef_GuessReservationRequirement_definite bitmap[refObj]

	// precomputed: ScheduleRef.ComputeEffectiveDateRange
	cached_ScheduleRef_ComputeEffectiveDateRange      bool
	cached_ScheduleRef_ComputeEffectiveDateRange_from []time.Time
	cached_ScheduleRef_ComputeEffectiveDateRange_to   []time.Time
	cached_ScheduleRef_ComputeEffectiveDateRange_ok   bitmap[refObj]

	// precomputed: Index.Updated
	updated time.Time

	// stats
	durScan        time.Duration
	durImport      time.Duration
	durSanityCheck time.Duration
	durPrecompute  time.Duration
}

// Load loads data from a binary protobuf. Note that this has quadratic
// complexity, as the indexer focuses on optimizing memory usage and read-only
// queries.
func (dxr *Indexer) Load(pb []byte) (*Index, error) {
	if !dxr.init {
		dxr.idx = make(map[string]*Index)
		dxr.a = newArena()
		dxr.sa.arena = dxr.a
		dxr.sa.Cache(4096)
		dxr.init = true
	}
	sum := sha1.Sum(pb)
	hash := base32.StdEncoding.EncodeToString(sum[:])
	idx, ok := dxr.idx[hash]
	if !ok {
		var msg schema.Data
		if err := proto.Unmarshal(pb, &msg); err != nil {
			return nil, err
		}
		idx = dxr.index(hash, &msg)
		dxr.idx[hash] = idx
	}
	return idx, nil
}

func (dxr *Indexer) index(hash string, data *schema.Data) *Index {
	now := time.Now()

	var n, nFac, nGrp, nSch, nAct int
	n++
	for _, fac := range data.GetFacilities() {
		n++
		nFac++
		for _, grp := range fac.GetScheduleGroups() {
			n++
			nGrp++
			for _, sch := range grp.GetSchedules() {
				n++
				nSch++
				for _, act := range sch.GetActivities() {
					n++
					nAct++
					for _, day := range act.GetDays() {
						// no increment
						for range day.GetTimes() {
							n++
						}
					}
				}
			}
		}
	}

	idx := &Index{
		a:    dxr.a,
		hash: hash,

		obj:            make([]any, 0, n),
		bData:          makeBitmap[refObj](n),
		bFacility:      makeBitmap[refObj](n),
		bScheduleGroup: makeBitmap[refObj](n),
		bSchedule:      makeBitmap[refObj](n),
		bActivity:      makeBitmap[refObj](n),
		bTime:          makeBitmap[refObj](n),

		bDataNotChild:          makeBitmap[refObj](n),
		bFacilityNotChild:      makeBitmap[refObj](n),
		bScheduleGroupNotChild: makeBitmap[refObj](n),
		bScheduleNotChild:      makeBitmap[refObj](n),
		bActivityNotChild:      makeBitmap[refObj](n),
		bTimeNotChild:          makeBitmap[refObj](n),

		cached_ActivityRef_GuessReservationRequirement_required: makeBitmap[refObj](n),
		cached_ActivityRef_GuessReservationRequirement_definite: makeBitmap[refObj](n),

		cached_ScheduleRef_ComputeEffectiveDateRange_from: make([]time.Time, nSch),
		cached_ScheduleRef_ComputeEffectiveDateRange_to:   make([]time.Time, nSch),
		cached_ScheduleRef_ComputeEffectiveDateRange_ok:   makeBitmap[refObj](n),
	}

	idx.durScan, now = time.Since(now), time.Now()

	addObj(idx, newData(dxr.a, &dxr.sa, data))
	for _, fac := range data.GetFacilities() {
		addObj(idx, newFacility(dxr.a, &dxr.sa, fac))
		for _, grp := range fac.GetScheduleGroups() {
			addObj(idx, newScheduleGroup(dxr.a, &dxr.sa, grp))
			for _, sch := range grp.GetSchedules() {
				addObj(idx, newSchedule(dxr.a, &dxr.sa, sch))
				for _, act := range sch.GetActivities() {
					addObj(idx, dxr.act.Intern(newActivity(dxr.a, &dxr.sa, act)))
					for i, day := range act.GetDays() {
						for _, tm := range day.GetTimes() {
							addObj(idx, dxr.tm.Intern(newTime(dxr.a, &dxr.sa, i, tm)))
						}
					}
				}
			}
		}
	}

	idx.bDataNotChild.Or(idx.bData)
	idx.bFacilityNotChild.Or(idx.bData, idx.bFacility)
	idx.bScheduleGroupNotChild.Or(idx.bData, idx.bFacility, idx.bScheduleGroup)
	idx.bScheduleNotChild.Or(idx.bData, idx.bFacility, idx.bScheduleGroup, idx.bSchedule)
	idx.bActivityNotChild.Or(idx.bData, idx.bFacility, idx.bScheduleGroup, idx.bSchedule, idx.bActivity)
	idx.bTimeNotChild.Or(idx.bData, idx.bFacility, idx.bScheduleGroup, idx.bSchedule, idx.bActivity, idx.bTime)

	idx.durImport, now = time.Since(now), time.Now()

	if enableIndexerSanityCheck {
		sanityCheck(idx, n)
		sanityCheck1(idx, data)

		idx.durSanityCheck, now = time.Since(now), time.Now()
	}

	for act := range idx.Data().Activities() {
		required, definite := act.GuessReservationRequirement()
		if required {
			idx.cached_ActivityRef_GuessReservationRequirement_required.Set(act.object())
		}
		if definite {
			idx.cached_ActivityRef_GuessReservationRequirement_definite.Set(act.object())
		}
	}
	idx.cached_ActivityRef_GuessReservationRequirement = true

	for act := range idx.Data().Schedules() {
		i := act.nthOfType()
		from, to, ok := act.ComputeEffectiveDateRange()
		idx.cached_ScheduleRef_ComputeEffectiveDateRange_from[i] = from
		idx.cached_ScheduleRef_ComputeEffectiveDateRange_to[i] = to
		if ok {
			idx.cached_ScheduleRef_ComputeEffectiveDateRange_ok.Set(act.object())
		}
	}
	idx.cached_ScheduleRef_ComputeEffectiveDateRange = true

	for fac := range idx.Data().Facilities() {
		if d := fac.GetSourceDate(); !d.IsZero() && d.After(idx.updated) {
			idx.updated = d
		}
	}

	idx.durPrecompute, now = time.Since(now), time.Now()

	if enableIndexerSanityCheck {
		sanityCheck2(idx)

		idx.durSanityCheck += time.Since(now)
		now = time.Now()
	}

	_ = now
	return idx
}

func addObj[T schemaObj](idx *Index, x *T) refObj {
	i := refObj(len(idx.obj))
	idx.obj = append(idx.obj, x)
	bm := typeBitmap[T](idx)
	if bm.IsNil() {
		panic("wtf: cannot add special object to array")
	}
	bm.Set(i)
	return i
}

// Data returns a reference to the data.
func (idx *Index) Data() DataRef {
	return DataRef{
		typedRef: typedRef[xData]{
			baseRef: baseRef{
				idx: idx,
				obj: 0,
			},
		},
	}
}

// Hash returns an ASCII string representing a hash of the raw protobuf.
func (idx *Index) Hash() string {
	return idx.hash
}

// Updated returns the timestamp of the most recently updated facility in the
// database.
func (idx *Index) Updated() time.Time {
	return idx.updated
}

func sanityCheck(idx *Index, n int) {
	if !idx.bData.Contains(0) {
		panic("wtf: xData must be the 0th item")
	}
	if idx.bData.Count() != 1 {
		panic("wtf: there must only be one xData")
	}
	if len(idx.obj) != n {
		panic("wtf: the object array must be the expected size")
	}
	var (
		total int
		all   = makeBitmap[refObj](len(idx.obj))
		bms   = []bitmap[refObj]{idx.bData, idx.bFacility, idx.bScheduleGroup, idx.bSchedule, idx.bActivity, idx.bTime}
	)
	for _, bm := range bms {
		total += bm.Count()
		all.Or(bm)
		if len(bm.kb) != len(all.kb) {
			panic("wtf: the bitmaps should not have grown (which would happen if a bit out of range was manipulated)")
		}
	}
	if total != n {
		panic("wtf: the total number of bits set must equal the number of objects")
	}
	if all.Count() != n {
		panic("wtf: every bit should be set in exactly one bitmap")
	}
}

func sanityCheck1(idx *Index, data *schema.Data) {
	req := func(a ...anyRef) {
		if slices.ContainsFunc(a, func(b anyRef) bool {
			ar, br := a[0].reflect(), b.reflect()
			eq := ar.idx == br.idx && slices.Equal(ar.flt.kb, br.flt.kb) && ar.obj == br.obj
			return !eq
		}) {
			panic("wtf")
		}
	}
	ieq := func(a ...int) {
		if slices.ContainsFunc(a, func(b int) bool {
			return a[0] != b
		}) {
			panic("wtf")
		}
	}
	var nfac, ngrp, nsch, nact, ntm int
	for _, fac := range data.GetFacilities() {
		nfac++
		for _, grp := range fac.GetScheduleGroups() {
			ngrp++
			for _, sch := range grp.GetSchedules() {
				nsch++
				for _, act := range sch.GetActivities() {
					nact++
					for _, day := range act.GetDays() {
						for range day.GetTimes() {
							ntm++
						}
					}
				}
			}
		}
	}
	var dat_fac, dat_grp, dat_sch, dat_act, dat_tm int
	dat := idx.Data()
	for fac := range dat.Facilities() {
		if fac.nthOfType() != dat_fac {
			panic("wtf")
		}
		dat_fac++
		var fac_grp, fac_sch, fac_act, fac_tm int
		for grp := range fac.ScheduleGroups() {
			if grp.nthOfType() != dat_grp {
				panic("wtf")
			}
			dat_grp++
			fac_grp++
			var grp_sch, grp_act, grp_tm int
			for sch := range grp.Schedules() {
				if sch.nthOfType() != dat_sch {
					panic("wtf")
				}
				dat_sch++
				fac_sch++
				grp_sch++
				var sch_act, sch_tm int
				for act := range sch.Activities() {
					if act.nthOfType() != dat_act {
						panic("wtf")
					}
					dat_act++
					fac_act++
					grp_act++
					sch_act++
					for tm := range act.Times() {
						if tm.nthOfType() != dat_tm {
							panic("wtf")
						}
						dat_tm++
						fac_tm++
						grp_tm++
						sch_tm++
						// ensure parents are all consistently resolved
						req(act, tm.Activity())
						req(sch, act.Schedule(), tm.Schedule())
						req(grp, sch.ScheduleGroup(), act.ScheduleGroup(), tm.ScheduleGroup())
						req(fac, grp.Facility(), sch.Facility(), act.Facility(), tm.Facility())
						req(dat, fac.Data(), grp.Data(), sch.Data(), act.Data(), tm.Data())
					}
				}
				// ensure iterating over skipped levels are consistent
				ieq(sch_tm, iterCount(sch.Times().Iter()))
				ieq(sch_act, iterCount(sch.Activities().Iter()))
			}
			ieq(grp_tm, iterCount(grp.Times().Iter()))
			ieq(grp_act, iterCount(grp.Activities().Iter()))
			ieq(grp_sch, iterCount(grp.Schedules().Iter()))
		}
		ieq(fac_tm, iterCount(fac.Times().Iter()))
		ieq(fac_act, iterCount(fac.Activities().Iter()))
		ieq(fac_sch, iterCount(fac.Schedules().Iter()))
		ieq(fac_grp, iterCount(fac.ScheduleGroups().Iter()))
	}
	ieq(nfac, dat_fac, iterCount(dat.Facilities().Iter()))
	ieq(ngrp, dat_grp, iterCount(dat.ScheduleGroups().Iter()))
	ieq(nsch, dat_sch, iterCount(dat.Schedules().Iter()))
	ieq(nact, dat_act, iterCount(dat.Activities().Iter()))
	ieq(ntm, dat_tm, iterCount(dat.Times().Iter()))
}

func sanityCheck2(idx *Index) {
	if !idx.cached_ActivityRef_GuessReservationRequirement {
		panic("wtf")
	}
	for ref := range idx.Data().Activities() {
		a1, b1 := ref.GuessReservationRequirement()
		idx.cached_ActivityRef_GuessReservationRequirement = false
		a2, b2 := ref.GuessReservationRequirement()
		idx.cached_ActivityRef_GuessReservationRequirement = true
		if a1 != a2 || b1 != b2 {
			panic("wtf")
		}
	}

	if !idx.cached_ScheduleRef_ComputeEffectiveDateRange {
		panic("wtf")
	}
	for ref := range idx.Data().Schedules() {
		a1, b1, c1 := ref.ComputeEffectiveDateRange()
		idx.cached_ScheduleRef_ComputeEffectiveDateRange = false
		a2, b2, c2 := ref.ComputeEffectiveDateRange()
		idx.cached_ScheduleRef_ComputeEffectiveDateRange = true
		if a1 != a2 || b1 != b2 || c1 != c2 {
			panic("wtf")
		}
	}
}

func iterCount[T any](seq iter.Seq[T]) int {
	var n int
	for range seq {
		n++
	}
	return n
}
