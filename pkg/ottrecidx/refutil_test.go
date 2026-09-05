package ottrecidx

import (
	"testing"
	"time"

	"github.com/ottrec/scraper/schema"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestComputeEffectiveDateRange(t *testing.T) {
	for _, tc := range []struct {
		name     string
		now      string // scrape time, default 2026-06-01, "none" for none, "other-facility-has-one" to only have it on a separate facility
		date     string
		from, to int32   // scraped (MMDDW), 0 if open
		unset    bool    // range not parsed
		days     []int32 // schedule table days (MMDDW), 0 if unparsed

		expOK   bool
		expFrom schema.Date
		expTo   schema.Date
	}{
		{"bounded", "", "September 8 to December 20", 9_08_0, 12_20_0, false, nil, true, 2026_09_08_3, 2026_12_20_1},
		{"until", "", "until June 26", 0, 6_26_0, false, nil, true, 0, 2026_06_26_6},
		{"starting", "", "starting September 8", 9_08_0, 0, false, nil, true, 2026_09_08_3, 0},
		{"none", "", "", 0, 0, true, nil, false, 0, 0},

		// day headers as range iff no caption and all columns are at least month/day and
		// mostly contiguous
		{"headers/single", "", "", 0, 0, true, []int32{9_07_2}, true, 2026_09_07_2, 2026_09_07_2},
		{"headers/span", "", "", 0, 0, true, []int32{9_05_7, 9_06_1, 9_07_2}, true, 2026_09_05_7, 2026_09_07_2},
		{"headers/weekdays-only", "", "", 0, 0, true, []int32{0, 0, 0}, false, 0, 0},
		{"headers/partly-dated", "", "", 0, 0, true, []int32{9_05_7, 0}, false, 0, 0},
		{"headers/no-columns", "", "", 0, 0, true, []int32{}, false, 0, 0},
		{"headers/new-year", "", "", 0, 0, true, []int32{12_29_3, 12_30_4, 1_04_2}, true, 2026_12_29_3, 2027_01_04_2},                         // first/last != min/max
		{"headers/caption-wins", "", "September 8 to December 20", 9_08_0, 12_20_0, false, []int32{9_05_7}, true, 2026_09_08_3, 2026_12_20_1}, // not if caption
		{"headers/contiguous-at-limit", "", "", 0, 0, true, []int32{9_07_2, 9_14_2}, true, 2026_09_07_2, 2026_09_14_2},
		{"headers/contiguous-past-limit", "", "", 0, 0, true, []int32{9_07_2, 9_15_3}, false, 0, 0},
		{"headers/contiguous-not", "", "", 0, 0, true, []int32{9_07_2, 10_13_3}, false, 0, 0},
		{"headers/missing-day", "", "", 0, 0, true, []int32{9_00_0, 9_08_3}, false, 0, 0},
		{"headers/missing-month", "", "", 0, 0, true, []int32{70 /* day 7, no month */, 9_08_3}, false, 0, 0},

		// ranges spanning years
		{"new-year/before", "2026-12-30", "December 29 to January 4", 12_29_0, 1_04_0, false, nil, true, 2026_12_29_3, 2027_01_04_2},                 // schedule year decrement
		{"new-year/during", "2027-01-02", "December 29 to January 4", 12_29_0, 1_04_0, false, nil, true, 2026_12_29_3, 2027_01_04_2},                 // ^
		{"new-year/season", "2026-01-08", "September 1 to June 17", 9_01_0, 6_17_0, false, nil, true, 2025_09_01_2, 2026_06_17_4},                    // same for a longer multi-season schedule
		{"new-year/future-across-new-year", "2026-12-20", "December 29 to January 4", 12_29_0, 1_04_0, false, nil, true, 2026_12_29_3, 2027_01_04_2}, // should be correct even when schedule added early
		{"new-year/day after", "2027-01-05", "December 29 to January 4", 12_29_0, 1_04_0, false, nil, true, 2026_12_29_3, 2027_01_04_2},              // should not jump after schedule end before removal
		{"new-year/week after", "2027-01-11", "December 29 to January 4", 12_29_0, 1_04_0, false, nil, true, 2026_12_29_3, 2027_01_04_2},             // ^
		{"new-year/long-expired", "2027-06-01", "December 29 to January 4", 12_29_0, 1_04_0, false, nil, true, 2027_12_29_4, 2028_01_04_3},           // but if not removed for too long, it looks like a future schedule
		{"new-year/future-short", "2026-01-10", "March 14 to 22", 3_14_0, 3_22_0, false, nil, true, 2026_03_14_7, 2026_03_22_1},
		{"new-year/future-season", "2026-08-20", "September 8 to December 20", 9_08_0, 12_20_0, false, nil, true, 2026_09_08_3, 2026_12_20_1},
		{"new-year/forward-shift", "2025-12-19", "January 3 to March 22", 1_03_0, 3_22_0, false, nil, true, 2026_01_03_7, 2026_03_22_1},   // regression test
		{"new-year/headers-day-after", "2027-01-05", "", 0, 0, true, []int32{12_29_4, 12_30_5, 1_04_2}, true, 2026_12_29_3, 2027_01_04_2}, // ^

		// season schedules aren't published that early, if outside the range,
		// it's almost certainly not a future one
		{"season/just-ended", "2026-04-04", "September 9 to March 31", 9_09_0, 3_31_0, false, nil, true, 2025_09_09_3, 2026_03_31_3},
		{"season/last-day", "2026-03-31", "September 9 to March 31", 9_09_0, 3_31_0, false, nil, true, 2025_09_09_3, 2026_03_31_3},
		{"season/during-season", "2026-01-15", "September 9 to March 31", 9_09_0, 3_31_0, false, nil, true, 2025_09_09_3, 2026_03_31_3},
		{"season/published-early", "2026-08-25", "September 9 to March 31", 9_09_0, 3_31_0, false, nil, true, 2026_09_09_4, 2027_03_31_4},      // unless close to the start date
		{"season/published-very-early", "2026-09-27", "December 1 to April 30", 12_01_0, 4_30_0, false, nil, true, 2026_12_01_3, 2027_04_30_6}, // regression test

		// thresholds are based on the actual calendar/timezone
		{"calendar/leap-year-during", "2027-04-19", "September 9 to March 15", 9_09_0, 3_15_0, false, nil, true, 2027_09_09_5, 2028_03_15_4},
		{"calendar/leap-year-ended", "2027-04-18", "September 9 to March 15", 9_09_0, 3_15_0, false, nil, true, 2026_09_09_4, 2027_03_15_2},

		// year only on to date
		{"year-to-not-from/span-year-before", "2025-12-20", "October 7 to March 10, 2026", 10_07_0, 2026_03_10_0, false, nil, true, 2025_10_07_3, 2026_03_10_3},
		{"year-to-not-from/span-year-ended", "2026-01-15", "October 7 to March 10, 2026", 10_07_0, 2026_03_10_0, false, nil, true, 2025_10_07_3, 2026_03_10_3},
		{"year-to-not-from/span-year-later", "2026-03-18", "October 7 to March 10, 2026", 10_07_0, 2026_03_10_0, false, nil, true, 2025_10_07_3, 2026_03_10_3},
		{"year-to-not-from/new-year-before", "2025-12-01", "December 25 to January 10, 2026", 12_25_0, 2026_01_10_0, false, nil, true, 2025_12_25_5, 2026_01_10_7},
		{"year-to-not-from/new-year-during", "2026-01-05", "December 25 to January 10, 2026", 12_25_0, 2026_01_10_0, false, nil, true, 2025_12_25_5, 2026_01_10_7},
		{"year-to-not-from/new-year-later", "2026-06-01", "December 25 to January 10, 2026", 12_25_0, 2026_01_10_0, false, nil, true, 2025_12_25_5, 2026_01_10_7},
		{"year-to-not-from/before-next-year", "2026-06-01", "October 7 to December 30, 2025", 10_07_0, 2025_12_30_0, false, nil, true, 2025_10_07_3, 2025_12_30_3},

		// year only on from date
		{"year-from-not-to", "", "October 7, 2025 to March 10", 2025_10_07_0, 3_10_0, false, nil, true, 2025_10_07_3, 2026_03_10_3},

		// partially resolved underspecified effective date ranges
		{"partial/from-no-day", "", "October to December 30", 10_00_0, 12_30_0, false, nil, true, 2026_10_01_5, 2026_12_30_4},
		{"partial/to-no-month", "", "October 7 to 30", 10_07_0, 300 /* day 30, no month; a leading 0 would be octal */, false, nil, true, 2026_10_07_4, 2026_10_30_6},
		{"partial/to-no-day", "", "October 7 to December", 10_07_0, 12_00_0, false, nil, true, 2026_10_07_4, 2026_12_31_5},
		{"partial/to-no-day-year-from-to", "", "October 7 to March 2026", 10_07_0, 2026_03_00_0, false, nil, true, 2025_10_07_3, 2026_03_31_3},

		// malformed ranges
		{"invalid/from-not-a-date", "", "June 38 to July 5", 6_38_0, 7_05_0, false, nil, false, 0, 0},
		{"invalid/from-no-month", "", "7 to July 5", 70 /* day 7, no month */, 7_05_0, false, nil, false, 0, 0},
		{"invalid/to-no-month-no-from", "", "until 30", 0, 300 /* day 30, no month */, false, nil, false, 0, 0},
		{"invalid/explicitly-backwards", "", "October 7, 2026 to March 10, 2025", 2026_10_07_0, 2025_03_10_0, false, nil, false, 0, 0},

		// dataset time fallback when no facility scrape time
		{"no-scrape-time/from", "none", "September 8 to December 20", 9_08_0, 12_20_0, false, nil, false, 0, 0},                                                // cannot assume year
		{"no-scrape-time/to", "none", "until March 10", 0, 3_10_0, false, nil, false, 0, 0},                                                                    // ^
		{"no-scrape-time/from-dataset", "other-facility-has-one", "September 8 to December 20", 9_08_0, 12_20_0, false, nil, true, 2026_09_08_3, 2026_12_20_1}, // special case (see below)

		// some more edge cases
		{"misc/month-before-scrape-month", "2026-06-01", "until March 10", 0, 3_10_0, false, nil, true, 0, 2027_03_10_4},
		{"misc/to-invalid", "", "October 7 to June 38", 10_07_0, 6_38_0, false, nil, false, 0, 0},
		{"misc/from-no-day-year-from-to", "", "October to March 10, 2026", 10_00_0, 2026_03_10_0, false, nil, true, 2025_10_01_4, 2026_03_10_3},
		{"misc/shifts-forward-open-ended", "2026-12-01", "starting February 1", 2_01_0, 0, false, nil, true, 2027_02_01_2, 0}, // range with no to, but a potentially future from, always is in the future
	} {
		t.Run(tc.name, func(t *testing.T) {
			var pb []byte
			{
				var sch schema.Schedule_builder
				sch.Caption = "Test Facility - test - " + tc.date
				sch.XDate = tc.date
				if !tc.unset {
					// scraper always sets both sides if parsed, even if one is zero
					sch.XFrom = new(tc.from)
					sch.XTo = new(tc.to)
				}
				for _, d := range tc.days {
					sch.Days = append(sch.Days, schema.Date(d).String())
					sch.XDaydates = append(sch.XDaydates, d)
				}

				var grp schema.ScheduleGroup_builder
				grp.Label = "test"
				grp.Schedules = []*schema.Schedule{sch.Build()}

				var src schema.Source_builder
				now := time.Date(2026, time.June, 1, 0, 0, 0, 0, TZ)
				if tc.now != "" && tc.now != "none" && tc.now != "other-facility-has-one" {
					var err error
					if now, err = time.ParseInLocation(time.DateOnly, tc.now, TZ); err != nil {
						panic(err)
					}
				}
				if tc.now != "none" && tc.now != "other-facility-has-one" {
					src.XDate = timestamppb.New(now)
				}

				var fac schema.Facility_builder
				fac.Name = "Test Facility"
				fac.Source = src.Build()
				fac.ScheduleGroups = []*schema.ScheduleGroup{grp.Build()}

				var data schema.Data_builder
				data.Facilities = []*schema.Facility{fac.Build()}
				if tc.now == "other-facility-has-one" {
					// special case: other facility has a date
					var osrc schema.Source_builder
					osrc.XDate = timestamppb.New(now)
					var other schema.Facility_builder
					other.Name = "Other Facility"
					other.Source = osrc.Build()
					data.Facilities = append(data.Facilities, other.Build())
				}

				buf, err := proto.Marshal(data.Build())
				if err != nil {
					panic(err)
				}
				pb = buf
			}

			idx, err := new(Indexer).Load(pb)
			if err != nil {
				t.Fatal(err)
			}
			for ref := range idx.Data().Schedules() {
				er, ok := ref.ComputeEffectiveDateRange()
				if ok != tc.expOK {
					t.Fatalf("ok = %v, want %v (%v)", ok, tc.expOK, er)
				}
				if !ok {
					return
				}
				if er.From != tc.expFrom || er.To != tc.expTo {
					t.Errorf("range = %d..%d (%s to %s), want %d..%d", er.From, er.To, er.From, er.To, tc.expFrom, tc.expTo)
				}
			}
		})
	}
}
