// Package ottrecidx implements an efficient in-memory bitmap-based data
// structure for storing and querying many City of Ottawa recreation schedules.
package ottrecidx

import (
	"crypto/sha1"
	"encoding/base32"
	"hash/maphash"
	"iter"
	"math"
	"slices"
	"time"

	"github.com/ottrec/scraper/schema"
	"github.com/ottrec/website/pkg/ottregions"
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

	// real data is highly dedupable and relatively low cardinality
	//
	// over 177 real schedules (2025-10-07 to 2026-03-20), some example timings:
	//  - 0 Index{hash:ZQ4PBS3Y obj:5612 scan:88.249µs import:15ms precompute:3ms dataUpdated:2026-03-20}
	//  - 1 Index{hash:GF2X52JV obj:5548 scan:104.579µs import:2ms precompute:3ms dataUpdated:2026-03-19}
	//  - 2 Index{hash:FK3YD6EN obj:5528 scan:77.363µs import:0s precompute:3ms dataUpdated:2026-03-18}
	//  - 3 Index{hash:HFABNLRS obj:5464 scan:75.7µs import:0s precompute:2ms dataUpdated:2026-03-17}
	//  - 4 Index{hash:Z5UZWDF3 obj:6484 scan:103.336µs import:1ms precompute:5ms dataUpdated:2026-03-16}
	//  - 170 Index{hash:XDLUZE7R obj:3682 scan:68.696µs import:27ms precompute:1ms dataUpdated:2025-10-14}
	//  - 171 Index{hash:OZ7DYQOX obj:3682 scan:73.375µs import:0s precompute:1ms dataUpdated:2025-10-13}
	//  - 172 Index{hash:I3XTO3RT obj:3702 scan:74.89µs import:0s precompute:3ms dataUpdated:2025-10-12}
	//  - 173 Index{hash:DQHVZUJ7 obj:3714 scan:62.88µs import:0s precompute:2ms dataUpdated:2025-10-11}
	init bool
	ia   *arena           // this had 9772068 bytes (bitmaps and precomputed values; mostly evenly split across schedules)
	a    *arena           // this had 13612204 bytes (data; amortized across schedules due to deduplication: raw protobufs were 41646361 bytes, in-memory unmarshaled protobufs take more space)
	sa   stringInterner   // this had 386179 bytes over 2 chunks (ratio 0.020)
	ac   activityInterner // this had 522 items over 522 chunks (ratio 0.004)
	tc   timeInterner     // this has 2794 items over 655 chunks (ratio 0.005)
}

type Index struct {
	ia, a *arena // keep a pointer to it so it doesn't get finalized while we are using objects from it

	hash     string
	hashCode uint64

	// base object array and bitmaps
	obj            []objRef       // flattened data->facility[]->schedule_group[]->schedule[]->activity[]->time[]
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
	cached_ScheduleRef_ComputeEffectiveDateRange    bool
	cached_ScheduleRef_ComputeEffectiveDateRange_er []schema.DateRange

	// precomputed: ScheduleRef.SingleDayDate
	cached_ScheduleRef_SingleDayDate   bool
	cached_ScheduleRef_SingleDayDate_t [][]schema.Date

	// precomputed: FacilityRef.Region / FacilityRef.Sector
	cached_FacilityRef_RegionSector   bool
	cached_FacilityRef_RegionSector_r []ottregions.Region
	cached_FacilityRef_RegionSector_s []ottregions.Sector

	// precomputed: Index.Updated
	updated time.Time

	// stats
	durScan        time.Duration
	durImport      time.Duration
	durSanityCheck time.Duration
	durPrecompute  time.Duration
}

var hashCodeSeed = maphash.MakeSeed()

// Load loads data from a binary protobuf. Note that this has quadratic
// complexity, as the indexer focuses on optimizing memory usage and read-only
// queries.
func (dxr *Indexer) Load(pb []byte) (*Index, error) {
	if !dxr.init {
		dxr.idx = make(map[string]*Index)
		dxr.ia = newArena() // for index metadata and caches
		dxr.a = newArena()  // for data
		dxr.sa.arena = dxr.a
		dxr.sa.Cache(4096)
		dxr.ac.a = dxr.a
		dxr.ac.sa = &dxr.sa
		dxr.tc.a = dxr.a
		dxr.tc.sa = &dxr.sa
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
		fixLngLatRegression(&msg)
		idx = dxr.index(hash, maphash.Bytes(hashCodeSeed, pb), &msg)
		dxr.idx[hash] = idx
	}
	return idx, nil
}

// fixLngLatRegression fixes the longitude/latitude mixup in the scraper from
// bc9be9b7098f8daaba3121daa564fcbeb4b85784 (2025-11-18) to
// 6ec0cae178db0f405b6c9451a43e53566e541e22 (2026-03-09).
func fixLngLatRegression(msg *schema.Data) {
	for _, fac := range msg.GetFacilities() {
		if lnglat := fac.GetXLnglat(); lnglat != nil {
			if lng, lat := lnglat.GetLng(), lnglat.GetLat(); math.Trunc(float64(lng)/10) == 4 && math.Trunc(float64(lat)/10) == -7 {
				lnglat.SetLat(lng)
				lnglat.SetLng(lat)
			}
		}
	}
}

func (dxr *Indexer) index(hash string, hashCode uint64, data *schema.Data) *Index {
	now := time.Now()

	var n, nFac, nGrp, nSch, nAct, nTm int
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
							nTm++
						}
					}
				}
			}
		}
	}

	idx := &Index{
		ia: dxr.ia,
		a:  dxr.a,

		hash:     hash,
		hashCode: hashCode,

		obj:            dxr.ia.MakeSlice[objRef](0, n),
		bData:          dxr.ia.MakeBitmap[refObj](n),
		bFacility:      dxr.ia.MakeBitmap[refObj](n),
		bScheduleGroup: dxr.ia.MakeBitmap[refObj](n),
		bSchedule:      dxr.ia.MakeBitmap[refObj](n),
		bActivity:      dxr.ia.MakeBitmap[refObj](n),
		bTime:          dxr.ia.MakeBitmap[refObj](n),

		bDataNotChild:          dxr.ia.MakeBitmap[refObj](n),
		bFacilityNotChild:      dxr.ia.MakeBitmap[refObj](n),
		bScheduleGroupNotChild: dxr.ia.MakeBitmap[refObj](n),
		bScheduleNotChild:      dxr.ia.MakeBitmap[refObj](n),
		bActivityNotChild:      dxr.ia.MakeBitmap[refObj](n),
		bTimeNotChild:          dxr.ia.MakeBitmap[refObj](n),

		cached_ActivityRef_GuessReservationRequirement_required: dxr.ia.MakeBitmap[refObj](n),
		cached_ActivityRef_GuessReservationRequirement_definite: dxr.ia.MakeBitmap[refObj](n),

		cached_ScheduleRef_ComputeEffectiveDateRange_er: dxr.ia.MakeSlice[schema.DateRange](nSch, nSch),

		cached_ScheduleRef_SingleDayDate_t: dxr.ia.MakeSlice[[]schema.Date](nSch, nSch),

		cached_FacilityRef_RegionSector_r: dxr.ia.MakeSlice[ottregions.Region](nFac, nFac),
		cached_FacilityRef_RegionSector_s: dxr.ia.MakeSlice[ottregions.Sector](nFac, nFac),
	}

	idx.durScan, now = time.Since(now), time.Now()

	idx.addObj(newData(dxr.a, &dxr.sa, data))
	for _, fac := range data.GetFacilities() {
		idx.addObj(newFacility(dxr.a, &dxr.sa, fac))
		for _, grp := range fac.GetScheduleGroups() {
			idx.addObj(newScheduleGroup(dxr.a, &dxr.sa, grp))
			for _, sch := range grp.GetSchedules() {
				idx.addObj(newSchedule(dxr.a, &dxr.sa, sch))
				for _, act := range sch.GetActivities() {
					idx.addObj(dxr.ac.newActivity(act))
					for i, day := range act.GetDays() {
						for _, tm := range day.GetTimes() {
							idx.addObj(dxr.tc.newTime(i, tm))
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

	for fac := range idx.Data().Facilities() {
		if d := fac.GetSourceDate(); !d.IsZero() && d.After(idx.updated) {
			idx.updated = d
		}
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
		er, _ := act.ComputeEffectiveDateRange()
		idx.cached_ScheduleRef_ComputeEffectiveDateRange_er[i] = er
	}
	idx.cached_ScheduleRef_ComputeEffectiveDateRange = true

	for sch := range idx.Data().Schedules() {
		j := sch.nthOfType()
		n := sch.NumDays()
		v := dxr.ia.MakeSlice[schema.Date](n, n)
		for i := range v {
			t, ok := sch.SingleDayDate(i)
			if ok {
				v[i] = t
			} else {
				v[i] = 0
			}
		}
		idx.cached_ScheduleRef_SingleDayDate_t[j] = v
	}
	idx.cached_ScheduleRef_SingleDayDate = true

	for fac := range idx.Data().Facilities() {
		i := fac.nthOfType()
		idx.cached_FacilityRef_RegionSector_r[i] = fac.Region()
		idx.cached_FacilityRef_RegionSector_s[i] = fac.Sector()
	}
	idx.cached_FacilityRef_RegionSector = true

	idx.durPrecompute, now = time.Since(now), time.Now()

	if enableIndexerSanityCheck {
		sanityCheck2(idx)

		idx.durSanityCheck += time.Since(now)
		now = time.Now()
	}

	_ = now
	return idx
}

func (idx *Index) addObj[T schemaObj](x *T) refObj {
	i := refObj(len(idx.obj))
	idx.obj = append(idx.obj, objRef(x))
	bm := idx.typeBitmap[T]()
	if bm.IsNil() {
		panic("wtf: cannot add special object to array")
	}
	bm.Set(i)
	return i
}

// Data returns a reference to the data.
func (idx *Index) Data() DataRef {
	return DataRef{
		idx: idx,
		obj: 0,
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
		a1, b1 := ref.ComputeEffectiveDateRange()
		idx.cached_ScheduleRef_ComputeEffectiveDateRange = false
		a2, b2 := ref.ComputeEffectiveDateRange()
		idx.cached_ScheduleRef_ComputeEffectiveDateRange = true
		if a1 != a2 || b1 != b2 {
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
