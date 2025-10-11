// Package ottrecsimple produces a simplified denormalized dataset for
// recreation schedules. Unlike to the scraper schema, this format makes
// higher-level conclusions and does not try to preserve the original structure.
package ottrecsimple

import (
	"bufio"
	"bytes"
	"io"
	"slices"
	"strings"

	"github.com/pgaskin/ottrec-website/pkg/ottrecidx"
)

type Data struct {
	Facility    []*Facility `sjson:"facility" scsv:"facility" doc:"facility information"`
	Activity    []*Activity `sjson:"activity" scsv:"activity" doc:"activity information"`
	Error       []*Error    `sjson:"error" scsv:"error" doc:"errors which occured while scraping the facility pages"`
	HTML        []*HTML     `sjson:"html" scsv:"html" doc:"longer snippets of html referenced from facility/activity"`
	Attribution []string    `sjson:"attribution" scsv:"attribution" doc:"attribution text"`
}

type Facility struct {
	URL               string  `sjson:"url" scsv:"facility_url" doc:"city of ottawa facility page url"`
	ScrapedAt         string  `sjson:"scrapedAt" scsv:"facility_scraped_at" doc:"date (YYYY-MM-DD) the date for the facility was scraped at"`
	Name              string  `sjson:"name" scsv:"facility_name" doc:"name of the facility"`
	Address           string  `sjson:"address" scsv:"facility_address" doc:"the address of the facility"`
	Longitude         float32 `sjson:"longitude,nullzero" scsv:"facility_longitude,emptyzero" doc:"facility longitude (may not be set if geocoding failed)"`
	Latitude          float32 `sjson:"latitude,nullzero" scsv:"facility_latitude,emptyzero" doc:"facility latitude (may not be set if geocoding failed)"`
	SpecialHoursHTML  int     `sjson:"specialHoursHtmlId" scsv:"facility_special_hours_html_id" doc:"html for special hours"`
	NotificationsHTML int     `sjson:"notificationsHtmlId" scsv:"facility_notifications_html_id" doc:"html for notifications"`
}

type Activity struct {
	FacilityURL string `sjson:"facilityUrl" scsv:"facility_url" doc:"facility url for the activity"`

	StartDate           string   `sjson:"startDate,nullzero" scsv:"activity_date_start,emptyzero" doc:"start date (YYYY-MM-DD), inclusive (may not be set if parsing failed or there's no range)"`
	EndDate             string   `sjson:"endDate,nullzero" scsv:"activity_date_end,emptyzero" doc:"end date (YYYY-MM-DD), inclusive (may not be set if parsing failed or there's no range)"`
	Weekday             string   `sjson:"weekday,nullzero" scsv:"activity_weekday,emptyzero" doc:"weekday (lowercase, long-form) or single date (YYYY-MM-DD) (may not be set if parsing failed)"`
	StartTime           string   `sjson:"startTime,nullzero" scsv:"activity_time_start,emptyzero" doc:"start time (HH:MM), inclusive (may not be set if parsing failed)"`
	EndTime             string   `sjson:"endTime,nullzero" scsv:"activity_time_end,emptyzero" doc:"end time (HH:MM), exclusive (may not be set if parsing failed)"`
	Name                string   `sjson:"name" scsv:"activity_name" doc:"activity name, normalized"`
	ReservationRequired bool     `sjson:"reservationRequired" scsv:"activity_reservation_required" doc:"whether reservation is required, best-effort"`
	ReservationLinks    []string `sjson:"reservationLinks" scsv:"activity_reservation_links" doc:"reservation urls (comma-separated for csv)"`
	ExceptionsHTML      int      `sjson:"exceptionsHtmlId" scsv:"activity_exceptions_html_id" doc:"html for schedule exceptions"`

	RawScheduleGroup string `sjson:"rawScheduleGroup" scsv:"activity_raw_group" doc:"raw schedule group text (this field is not stable)"`
	RawSchedule      string `sjson:"rawSchedule" scsv:"activity_raw_schedule" doc:"raw schedule caption text (this field is not stable)"`
	RawDay           string `sjson:"rawDay" scsv:"activity_raw_day" doc:"raw schedule activity day (this field is not stable)"`
	RawActivity      string `sjson:"rawActivity" scsv:"activity_raw_activity" doc:"raw schedule activity label (this field is not stable)"`
	RawTime          string `sjson:"rawTime" scsv:"activity_raw_time" doc:"raw schedule activity time (this field is not stable)"`
}

type Error struct {
	FacilityURL string `sjson:"facilityUrl" scsv:"facility_url" doc:"facility url the error occured while scraping"`
	Error       string `sjson:"error" scsv:"error" doc:"error message"`
}

type HTML struct {
	ID   int    `sjson:"id" scsv:"id" doc:"index for cross-referencing, not stable"`
	HTML string `sjson:"html" scsv:"html" doc:"raw html"` // note: 0th is always the empty string
}

const dateFormat = "2006-01-02"

func New(data ottrecidx.DataRef) (*Data, error) {
	result := &Data{
		Facility:    make([]*Facility, 0, data.Facilities().Len()),
		Activity:    make([]*Activity, 0, data.Times().Len()),
		HTML:        []*HTML{{0, ""}},
		Attribution: slices.Collect(data.GetAttribution()),
	}
	htmlID := map[string]int{}
	addHTML := func(s string) int {
		id, ok := htmlID[s]
		if !ok {
			id = len(result.HTML)
			result.HTML = append(result.HTML, &HTML{id, s})
			htmlID[s] = id
		}
		return id
	}
	for fac := range data.Facilities() {
		var rf Facility
		rf.URL = fac.GetSourceURL()
		if t := fac.GetSourceDate(); !t.IsZero() {
			rf.ScrapedAt = t.Format(dateFormat)
		}
		rf.Name = fac.GetName()
		rf.Address = strings.ReplaceAll(fac.GetAddress(), "\n", ", ")
		if lng, lat, ok := fac.GetLngLat(); ok {
			rf.Longitude = lng
			rf.Latitude = lat
		}
		if s := fac.GetSpecialHoursHTML(); s != "" {
			rf.SpecialHoursHTML = addHTML(strings.ReplaceAll(s, "\n", ""))
		}
		if s := fac.GetNotificationsHTML(); s != "" {
			rf.NotificationsHTML = addHTML(strings.ReplaceAll(s, "\n", ""))
		}
		for e := range fac.GetErrors() {
			result.Error = append(result.Error, &Error{
				FacilityURL: rf.URL,
				Error:       e,
			})
		}
		for tm := range fac.Times() {
			var ra Activity
			ra.FacilityURL = rf.URL
			if from, to, ok := tm.Schedule().ComputeEffectiveDateRange(); ok {
				if !from.IsZero() {
					ra.StartDate = from.Format(dateFormat)
				}
				if !to.IsZero() {
					ra.EndDate = to.Format(dateFormat)
				}
			}
			if d, ok := tm.SingleDate(); ok {
				ra.Weekday = d.Format(dateFormat)
			} else if w, ok := tm.GetWeekday(); ok {
				ra.Weekday = strings.ToLower(w.String())
			}
			if r, ok := tm.GetRange(); ok {
				if r.Start.IsValid() {
					ra.StartTime = r.Start.Format(false)
				}
				if r.End.IsValid() {
					ra.EndTime = r.End.Format(false)
				}
			}
			ra.Name = tm.Activity().GetName()
			if r, _ := tm.Activity().GuessReservationRequirement(); r {
				ra.ReservationRequired = true
				for lnk := range tm.ScheduleGroup().GetReservationLinks() {
					if lnk.URL != "" {
						ra.ReservationLinks = append(ra.ReservationLinks, strings.ReplaceAll(lnk.URL, ",", "%2C"))
					}
				}
			}
			if s := tm.ScheduleGroup().GetScheduleChangesHTML(); s != "" {
				ra.ExceptionsHTML = addHTML(s)
			}
			ra.RawScheduleGroup = tm.ScheduleGroup().GetLabel()
			ra.RawSchedule = tm.Schedule().GetCaption()
			ra.RawDay = tm.GetScheduleDay()
			ra.RawActivity = tm.Activity().GetLabel()
			ra.RawTime = tm.GetLabel()
			result.Activity = append(result.Activity, &ra)
		}
		result.Facility = append(result.Facility, &rf)
	}
	return result, nil
}

type BufferedWriter interface {
	Write([]byte) (int, error)
	WriteByte(byte) error
	WriteString(string) (int, error)
	AvailableBuffer() []byte
}

var (
	_ BufferedWriter = (*bufio.Writer)(nil)
	_ BufferedWriter = (*bytes.Buffer)(nil)
)

func newBufferedWriter(w io.Writer) BufferedWriter {
	if bw, ok := w.(BufferedWriter); ok {
		return bw
	}
	return bufio.NewWriter(w)
}

// TODO: csv
