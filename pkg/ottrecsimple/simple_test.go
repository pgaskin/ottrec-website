package ottrecsimple

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"iter"
	"testing"
)

func init() {
	JSONSchemaID = "https://example.com/schema.json"
}

// EmptyData contains one empty row for each table.
var EmptyData = &Data{
	Facility: Table[Facility]{{
		ScrapedAt: dateFormat, // so the schema validates
	}},
	Activity:    Table[Activity]{{}},
	Error:       Table[Error]{{}},
	HTML:        Table[HTML]{{}},
	Attribution: Table[Attribution]{{}},
}

// DummyData contains one row with all columns set to a value for each table.
var DummyData = &Data{
	Facility: Table[Facility]{{
		URL:               "DummyURL",
		ScrapedAt:         dateFormat,
		Name:              "DummyName",
		Address:           "DummyAddress",
		Longitude:         123.456,
		Latitude:          234.567,
		SpecialHoursHTML:  1,
		NotificationsHTML: 2,
	}},
	Activity: Table[Activity]{{
		FacilityURL:         "DummyFacilityURL",
		StartDate:           dateFormat,
		EndDate:             dateFormat,
		Weekday:             "sunday",
		StartTime:           "23:59",
		EndTime:             "23:59",
		Name:                "DummyName",
		ReservationRequired: true,
		ReservationLinks:    []string{"DummyReservationLink1", "DummyReservationLink2"},
		ExceptionsHTML:      3,
		RawScheduleGroup:    "DummyRawScheduleGroup",
		RawSchedule:         "DummyRawSchedule",
		RawDay:              "DummyRawDay",
		RawActivity:         "DummyRawActivity",
		RawTime:             "DummyRawTime",
	}},
	Error: Table[Error]{{
		FacilityURL: "DummyFacilityURL",
		Error:       "DummyError",
	}},
	HTML: Table[HTML]{
		{0, ""},
		{1, "HTML1"},
		{2, "HTML2"},
		{3, "HTML3"},
	},
	Attribution: Table[Attribution]{{
		Text: "DummyText",
	}, {
		Text: "escape test !@#$%^&*():, \\ \n\r\t\v\f \u2028\u2029 \u00a0 \"",
	}},
}

func testdata() iter.Seq2[string, *Data] {
	return func(yield func(string, *Data) bool) {
		if !yield("Empty", EmptyData) {
			return
		}
		if !yield("Dummy", DummyData) {
			return
		}
	}
}

func TestNew(t *testing.T) {
	t.SkipNow() // TODO
}

func TestBufferedWriter(t *testing.T) {
	if newStickyBufferedWriter(nil) != nil {
		t.Errorf("newBufferedWriter should preserve nil-ness")
	}
	if w := bytes.NewBuffer(nil); newStickyBufferedWriter(w).w != w {
		t.Errorf("newBufferedWriter(%T) should use the buffer as-is", w)
	}
	if w := bufio.NewWriter(new(dummyWriter)); newStickyBufferedWriter(w).w != w || newStickyBufferedWriter(w).f == nil {
		t.Errorf("newBufferedWriter(%T) should use the buffer as-is and not have a nil flush", w)
	}
	if w := newStickyBufferedWriter(new(dummyWriter)); w.f == nil {
		t.Errorf("newBufferedWriter should not have a nil flush")
	}
}

func sha1sum(buf []byte) string {
	sum := sha1.Sum(buf)
	return hex.EncodeToString(sum[:])
}

func catch(fn func()) (err error) {
	defer func() {
		if x := recover(); x != nil {
			if e, ok := x.(error); ok {
				err = fmt.Errorf("panic: %w", e)
			} else {
				err = fmt.Errorf("panic: %v", x)
			}
		}
	}()
	fn()
	return
}

func catch1[T any](fn func() T) (res T, err error) {
	err = catch(func() {
		res = fn()
	})
	return
}

func catchSeq2[K, V any](seq iter.Seq2[K, V]) func(*error) iter.Seq2[K, V] {
	return func(err *error) iter.Seq2[K, V] {
		return func(yield func(K, V) bool) {
			*err = func() error {
				return catch(func() {
					for k, v := range seq {
						if !yield(k, v) {
							return
						}
					}
				})
			}()
		}
	}
}

type dummyWriter struct{}

func (*dummyWriter) Write(p []byte) (int, error) {
	return len(p), nil
}
