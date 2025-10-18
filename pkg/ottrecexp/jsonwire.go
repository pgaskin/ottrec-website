// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// go@1.25.3/encoding/json/internal/jsonwire

package ottrecexp

import (
	"errors"
	"math"
	"slices"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"
)

func appendFloatJSON(dst []byte, src float64, bits int) []byte {
	if bits == 32 {
		src = float64(float32(src))
	}

	abs := math.Abs(src)
	fmt := byte('f')
	if abs != 0 {
		if bits == 64 && (float64(abs) < 1e-6 || float64(abs) >= 1e21) ||
			bits == 32 && (float32(abs) < 1e-6 || float32(abs) >= 1e21) {
			fmt = 'e'
		}
	}
	dst = strconv.AppendFloat(dst, src, fmt, -1, bits)
	if fmt == 'e' {
		// Clean up e-09 to e-9.
		n := len(dst)
		if n >= 4 && dst[n-4] == 'e' && dst[n-3] == '-' && dst[n-2] == '0' {
			dst[n-2] = dst[n-1]
			dst = dst[:n-1]
		}
	}
	return dst
}

var escapeASCII = [...]uint8{
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // escape control characters
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // escape control characters
	0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, // escape '"' and '&'
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, // escape '<' and '>'
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, // escape '\\'
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
}

func appendQuoteJSON[Bytes ~[]byte | ~string](dst []byte, src Bytes) ([]byte, error) {
	const (
		escapeForHTML    = false
		escapeForJS      = false
		allowInvalidUTF8 = true
	)
	var i, n int
	var hasInvalidUTF8 bool
	dst = slices.Grow(dst, len(`"`)+len(src)+len(`"`))
	dst = append(dst, '"')
	for uint(len(src)) > uint(n) {
		if c := src[n]; c < utf8.RuneSelf {
			// Handle single-byte ASCII.
			n++
			if escapeASCII[c] == 0 {
				continue // no escaping possibly needed
			}
			// Handle escaping of single-byte ASCII.
			if !(c == '<' || c == '>' || c == '&') || escapeForHTML {
				dst = append(dst, src[i:n-1]...)
				dst = appendEscapedASCII(dst, c)
				i = n
			}
		} else {
			// Handle multi-byte Unicode.
			r, rn := utf8.DecodeRuneInString(string(truncateMaxUTF8(src[n:])))
			n += rn
			if r != utf8.RuneError && r != '\u2028' && r != '\u2029' {
				continue // no escaping possibly needed
			}
			// Handle escaping of multi-byte Unicode.
			switch {
			case isInvalidUTF8(r, rn):
				hasInvalidUTF8 = true
				dst = append(dst, src[i:n-rn]...)
				dst = append(dst, "\ufffd"...)
				i = n
			case (r == '\u2028' || r == '\u2029') && escapeForJS:
				dst = append(dst, src[i:n-rn]...)
				dst = appendEscapedUnicode(dst, r)
				i = n
			}
		}
	}
	dst = append(dst, src[i:n]...)
	dst = append(dst, '"')
	if hasInvalidUTF8 && !allowInvalidUTF8 {
		return dst, errors.New("invalid UTF-8")
	}
	return dst, nil
}

func appendEscapedASCII(dst []byte, c byte) []byte {
	switch c {
	case '"', '\\':
		dst = append(dst, '\\', c)
	case '\b':
		dst = append(dst, "\\b"...)
	case '\f':
		dst = append(dst, "\\f"...)
	case '\n':
		dst = append(dst, "\\n"...)
	case '\r':
		dst = append(dst, "\\r"...)
	case '\t':
		dst = append(dst, "\\t"...)
	default:
		dst = appendEscapedUTF16(dst, uint16(c))
	}
	return dst
}

func appendEscapedUnicode(dst []byte, r rune) []byte {
	if r1, r2 := utf16.EncodeRune(r); r1 != '\ufffd' && r2 != '\ufffd' {
		dst = appendEscapedUTF16(dst, uint16(r1))
		dst = appendEscapedUTF16(dst, uint16(r2))
	} else {
		dst = appendEscapedUTF16(dst, uint16(r))
	}
	return dst
}

func appendEscapedUTF16(dst []byte, x uint16) []byte {
	const hex = "0123456789abcdef"
	return append(dst, '\\', 'u', hex[(x>>12)&0xf], hex[(x>>8)&0xf], hex[(x>>4)&0xf], hex[(x>>0)&0xf])
}

func isInvalidUTF8(r rune, rn int) bool {
	return r == utf8.RuneError && rn == 1
}

func truncateMaxUTF8[Bytes ~[]byte | ~string](b Bytes) Bytes {
	if len(b) > utf8.UTFMax {
		return b[:utf8.UTFMax]
	}
	return b
}
