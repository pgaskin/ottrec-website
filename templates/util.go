package templates

import (
	"strings"

	"github.com/pgaskin/ottrec/schema"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

func capitalizeFirst(s string) string {
	if s != "" {
		s = strings.ToUpper(s[:1]) + s[1:]
	}
	return s
}

var titleCaseReplacer = strings.NewReplacer(" And ", " and ", " Or ", " or ")

func titleCase(s string) string {
	return titleCaseReplacer.Replace(cases.Title(language.English).String(s))
}

func prettyTimeRange(r schema.ClockRange) string {
	if !r.IsValid() {
		return "invalid"
	}
	prettyTime := func(t schema.ClockTime) string {
		if t == 12*60 {
			return "noon"
		}
		if t == 0 || t == 24*60 {
			return "midnight"
		}
		var b strings.Builder
		_, hh, mm := t.Split()
		ap := byte('a')
		if hh >= 12 {
			ap = 'p'
			hh -= 12
		}
		if hh == 0 {
			b.WriteByte('1')
			b.WriteByte('2')
		} else {
			if hh >= 10 {
				b.WriteByte('0' + byte(hh/10))
			}
			b.WriteByte('0' + byte(hh%10))
		}
		if mm != 0 {
			b.WriteByte(':')
			b.WriteByte('0' + byte(mm/10))
			b.WriteByte('0' + byte(mm%10))
		}
		b.WriteByte(' ')
		b.WriteByte(ap)
		b.WriteByte('m')
		return b.String()
	}
	x := prettyTime(r.Start)
	y := prettyTime(r.End)
	if x[len(x)-2] == y[len(y)-2] {
		x = x[:len(x)-3]
	}
	return x + " - " + y
}
