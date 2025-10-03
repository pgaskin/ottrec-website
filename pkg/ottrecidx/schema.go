package ottrecidx

import (
	"time"

	"github.com/pgaskin/ottrec/schema"
)

// this file contains the internal representation of data based on the schema

type schemaObj interface {
	xData | xFacility | xScheduleGroup | xSchedule | xActivity | xTime
}

type xData struct {
	Attribution []string
}

type xFacility struct {
	Name              string
	Description       string
	SourceURL         string
	SourceDate        time.Time
	Address           string
	Longitude         float32 // geocoded (lng+lat zero if not present)
	Latitude          float32 // geocoded (lng+lat zero if not present)
	NotificationsHTML string
	SpecialHoursHTML  string
	Errors            []string
}

type xScheduleGroup struct {
	Label               string
	Title               string // derived
	ReservationLinks    []ReservationLink
	ScheduleChangesHTML string
}

type xSchedule struct {
	Caption   string
	Name      string           // derived
	Date      string           // derived
	DateRange schema.DateRange // derived (from+to zero if not parsed)
	Days      []string
}

type xActivity struct {
	Label         string
	Name          string // derived
	Resv, HasResv bool   // derived
}

type xTime struct {
	ScheduleDay int // index into xSchedule.Days
	Label       string
	Weekday     time.Weekday      // derived (-1 if not parsed)
	Range       schema.ClockRange // derived (start+end zero if not parsed)
}

// note: don't count simple data holders which:
// 	- are returned directly;
//	- don't need to be able to backref;
//	- and don't have children
// as schema objects since that will add unnecessary complexity and overhead

type ReservationLink struct {
	Label string
	URL   string
}

func newData(a *arena, sa *stringInterner, data *schema.Data) *xData {
	x := arenaNew[xData](a)
	x.Attribution = mapSlice(a, data.GetAttribution(), sa.InternFast)
	return x
}

func newFacility(a *arena, sa *stringInterner, fac *schema.Facility) *xFacility {
	x := arenaNew[xFacility](a)
	x.Name = sa.Intern(fac.GetName())
	x.Description = sa.InternFast(fac.GetDescription())
	if src := fac.GetSource(); src != nil {
		x.SourceURL = sa.InternFast(src.GetUrl())
		if v := src.GetXDate(); v != nil {
			x.SourceDate = v.AsTime()
		}
	}
	x.Address = sa.InternFast(fac.GetAddress())
	if ll := fac.GetXLnglat(); ll != nil {
		x.Longitude = ll.GetLng()
		x.Latitude = ll.GetLat()
	}
	x.NotificationsHTML = sa.InternFast(fac.GetNotificationsHtml())
	x.SpecialHoursHTML = sa.InternFast(fac.GetSpecialHoursHtml())
	return x
}

func newScheduleGroup(a *arena, sa *stringInterner, grp *schema.ScheduleGroup) *xScheduleGroup {
	x := arenaNew[xScheduleGroup](a)
	x.Label = sa.Intern(grp.GetLabel())
	x.Title = sa.Intern(grp.GetXTitle())
	x.ReservationLinks = mapSlice(a, grp.GetReservationLinks(), func(lnk *schema.ReservationLink) ReservationLink {
		return makeReservationLink(sa, lnk)
	})
	x.ScheduleChangesHTML = sa.Intern(grp.GetScheduleChangesHtml())
	return x
}

func newSchedule(a *arena, sa *stringInterner, grp *schema.Schedule) *xSchedule {
	x := arenaNew[xSchedule](a)
	x.Caption = sa.Intern(grp.GetCaption())
	x.Name = sa.Intern(grp.GetXName())
	x.Date = sa.Intern(grp.GetXDate())
	if v, ok := grp.AsXParsedDate(); ok {
		x.DateRange = v
	}
	x.Days = mapSlice(a, grp.GetDays(), sa.InternFast)
	return x
}

func newActivity(a *arena, sa *stringInterner, act *schema.Schedule_Activity) *xActivity {
	x := arenaNew[xActivity](a)
	x.Label = sa.Intern(act.GetLabel())
	x.Name = sa.Intern(act.GetXName())
	x.Resv = act.GetXResv()
	x.HasResv = act.HasXResv()
	return x
}

func newTime(a *arena, sa *stringInterner, scheduleDay int, tm *schema.TimeRange) *xTime {
	x := arenaNew[xTime](a)
	x.ScheduleDay = scheduleDay
	x.Label = sa.Intern(tm.GetLabel())
	if w, r, ok := tm.AsXParsed(); ok {
		x.Weekday = w
		x.Range = r
	} else {
		x.Weekday = -1
	}
	return x
}

func makeReservationLink(sa *stringInterner, lnk *schema.ReservationLink) ReservationLink {
	return ReservationLink{
		Label: sa.InternFast(lnk.GetLabel()),
		URL:   sa.InternFast(lnk.GetUrl()),
	}
}

func mapSlice[T, U any](a *arena, s []T, fn func(T) U) []U {
	if s == nil {
		return nil
	}
	x := arenaMakeSlice[U](a, len(s), cap(s))
	for i, v := range s {
		x[i] = fn(v)
	}
	return x
}
