// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// based on https://go-review.googlesource.com/c/go/+/699455/7

package httpfile

import (
	"iter"
	"strings"

	"golang.org/x/net/http/httpguts"
)

// negotiateContent returns the best content to offer from a set of possible
// values, based on the preferences represented by the accept values.
//
// For example, negotiateContent can be used on the HTTP servers to find the
// best "Content-Type" to provide to HTTP user agents, based on the "Accept"
// request header. This is also known as a proactive negotiation (or "server-driven"
// negotiation).
//
// negotiateContent may be used to negotiate several fields, e.g.:
//
//	Response field    Request header
//	--------------    --------------
//	Content-Type      Accept
//	Content-Charset   Accept-Charset
//	Content-Encoding  Accept-Encoding
//	Content-Language  Accept-Language
//
// The accepts parameter is the appropriate request header value(s) and is
// interpreted as in RFC 9110 section 12. The offers parameter is a list of
// possible values to offer. The offers parameter does not support wildcards,
// e.g. text/* offer will match only accepts like *, */* and text/*, it will not
// match e.g. text/plain.
//
// There are following notable edge cases:
// * If no offers match, empty string is returned.
// * If more than one offer matches with equal weight and specificity, earliest
// offer in the offers list is returned.
// * If no accept values are provided, it represents no preference case
// (as per RFC 9110 section 12.4.1) which makes all the offers to match.
func negotiateContent(accepts []qValue, offers iter.Seq[string]) string {
	var (
		bestOffer = ""
		bestQ     = -1.0
		bestWild  = 3
	)
	if len(accepts) == 0 {
		// As per RFC 9110 section 12.4.1, no headers means no preference.
		for offer := range offers {
			return offer
		}
		return ""
	}
	for offer := range offers {
		for _, acc := range accepts {
			switch {
			case acc.q == 0.0, acc.q < bestQ:
				// Ignore.
			case acc.value == "*/*" || acc.value == "*":
				if acc.q > bestQ || bestWild > 2 {
					bestQ = acc.q
					bestWild = 2
					bestOffer = offer
				}
			case strings.HasSuffix(acc.value, "/*"):
				if strings.HasPrefix(offer, acc.value[:len(acc.value)-1]) &&
					(acc.q > bestQ || bestWild > 1) {
					bestQ = acc.q
					bestWild = 1
					bestOffer = offer
				}
			default:
				if acc.value == offer &&
					(acc.q > bestQ || bestWild > 0) {
					bestQ = acc.q
					bestWild = 0
					bestOffer = offer
				}
			}
		}
	}
	return bestOffer
}

// isOWS reports whether b is an optional whitespace byte,
// as defined by RFC 9110 section 5.6.3
func isOWS(b byte) bool {
	return b == ' ' || b == '\t' // SP or HTAB.
}

func skipOWS(s string) string {
	i := 0
	for ; i < len(s); i++ {
		b := s[i]
		if !isOWS(b) {
			break
		}
	}
	return s[i:]
}

func indexMediaRange(s string) (i int) {
	i = 0
	for ; i < len(s); i++ {
		b := s[i]
		if !httpguts.IsTokenRune(rune(b)) && b != '/' {
			break
		}
	}
	return i
}

func indexParam(s string) (i int) {
	i = 0
	for ; i < len(s); i++ {
		b := s[i]
		if !httpguts.IsTokenRune(rune(b)) && b != '=' {
			break
		}
	}
	return i
}

// expectQualityValue parses quality value as per RFC 9110 section 12.4.2.
func expectQualityValue(s string) (q float64, rest string) {
	switch {
	case len(s) == 0:
		return -1, ""
	case s[0] == '0':
		q = 0
	case s[0] == '1':
		q = 1
	default:
		return -1, ""
	}
	s = s[1:]
	if !strings.HasPrefix(s, ".") {
		return q, s
	}
	s = s[1:]
	i := 0
	n := 0
	d := 1
	// Only 3 fraction digits are allowed.
	for ; i < 3 && i < len(s); i++ {
		b := s[i]
		if b < '0' || b > '9' {
			break
		}
		n = n*10 + int(b) - '0'
		d *= 10
	}
	if q == 1 {
		// qvalue that starts with 1 may not have any non-0 digits
		// in the fractional component. Normalize to 1, but consume the
		// potential non-zero digits from the input.
		return 1, s[i:]
	}
	return q + float64(n)/float64(d), s[i:]
}

// qValue separates quality factor ("q") and rest of the media-range and params.
type qValue struct {
	value string // Accept* header value without "q" param.
	q     float64
}

// parseQualityFactors parses Accept* header values into qValues.
func parseQualityFactors(values []string, maxTotalLen int) (vals []qValue) {
loop:
	for _, s := range values {
		maxTotalLen -= len(s)
		if maxTotalLen <= 0 {
			return vals
		}

		for {
			var (
				v       qValue
				builder strings.Builder
			)

			s = skipOWS(s)
			i := strings.Index(s, ",")
			if i > 0 {
				builder.Grow(i)
			}

			// Mandatory media-range.
			i = indexMediaRange(s)
			if i == 0 {
				// Malformed media-range, ignore the entry.
				continue loop
			}
			builder.WriteString(s[:i])
			s = s[i:]

			// Optional accept-params are added to the v.value,
			// unless it's the quality factor "q". qvalue does not need
			// to be last, although it SHOULD.
			v.q = 1.0
			s = skipOWS(s)
			for strings.HasPrefix(s, ";") {
				s = skipOWS(s[1:])
				// RFC 9110 12.4.2 mentions "q" is case-insensitive, so "Q" is also
				// supported.
				if !strings.HasPrefix(s, "q=") && !strings.HasPrefix(s, "Q=") {
					i = indexParam(s)
					if i == 0 {
						// Malformed param, ignore the entry.
						continue loop
					}
					builder.WriteRune(';')
					builder.WriteString(s[:i])
					s = s[i:]
					continue
				}
				v.q, s = expectQualityValue(s[2:])
				if v.q < 0.0 {
					// Malformed quality factor, ignore the entry.
					continue loop
				}
			}

			v.value = builder.String()
			vals = append(vals, v)

			s = skipOWS(s)
			if !strings.HasPrefix(s, ",") {
				continue loop
			}
			s = s[1:]
		}
	}
	return vals
}
