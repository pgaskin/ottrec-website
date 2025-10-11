package ottrecsimple

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

func JSON(x *Data) []byte {
	var b bytes.Buffer
	if err := writeDataJSON(&b, x); err != nil {
		panic(err)
	}
	return nil
}

// WriteJSON writes the data as JSON to w. If w implements [BufferedWriter]
// (like [bytes.Buffer] or [bufio.Writer]), it will be used directly.
func WriteJSON(x *Data, w io.Writer) error {
	return writeDataJSON(newBufferedWriter(w), x)
}

func WriteTableJSON[T Row](x Table[T], w io.Writer) error {
	val := reflect.ValueOf(x)
	typ := val.Type()
	return writeTableRowsJSON(newBufferedWriter(w), typ, val)
}

func WriteRowJSON[T Row](x *T, w io.Writer) error {
	val := reflect.ValueOf(x)
	typ := val.Type()
	return writeRowJSON(newBufferedWriter(w), typ, val)
}

func writeDataJSON(w BufferedWriter, data any) error {
	if err := w.WriteByte('{'); err != nil {
		return err
	}
	var (
		val = reflect.ValueOf(data)
		typ = val.Type()
	)
	if typ.Kind() == reflect.Pointer {
		if val.IsNil() {
			return fmt.Errorf("is nil")
		}
		typ = typ.Elem()
		val = val.Elem()
	}
	for i := range typ.NumField() {
		if i != 0 {
			if err := w.WriteByte(','); err != nil {
				return err
			}
		}
		if err := writeTableJSON(w, typ.Field(i), val.Field(i)); err != nil {
			return fmt.Errorf("write table %s: %w", typ.Field(i).Name, err)
		}

	}
	if err := w.WriteByte('}'); err != nil {
		return err
	}
	return nil
}

func writeTableJSON(w BufferedWriter, typ reflect.StructField, val reflect.Value) error {
	tag, ok := typ.Tag.Lookup("sjson")
	if !ok || tag == "" {
		return fmt.Errorf("missing or invalid tag")
	}

	name, args, _ := strings.Cut(tag, ",")
	if args != "" {
		for arg := range strings.SplitSeq(args, ",") {
			return fmt.Errorf("invalid tag arg %q", arg)
		}
	}

	if _, err := w.Write(appendStringJSON(w.AvailableBuffer(), name)); err != nil {
		return err
	}
	if err := w.WriteByte(':'); err != nil {
		return err
	}
	return writeTableRowsJSON(w, typ.Type, val)
}

func writeTableRowsJSON(w BufferedWriter, typ reflect.Type, val reflect.Value) error {
	if err := w.WriteByte('['); err != nil {
		return err
	}
	if typ.Kind() != reflect.Slice {
		return fmt.Errorf("unsupported type %s", typ)
	}
	for j := range val.Len() {
		if j != 0 {
			if err := w.WriteByte(','); err != nil {
				return err
			}
		}
		if err := writeRowJSON(w, typ.Elem(), val.Index(j)); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
	}
	if err := w.WriteByte(']'); err != nil {
		return err
	}
	return nil
}

func writeRowJSON(w BufferedWriter, typ reflect.Type, val reflect.Value) error {
	if typ.Kind() == reflect.Pointer {
		if val.IsNil() {
			return fmt.Errorf("is nil")
		}
		typ = typ.Elem()
		val = val.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("unsupported type %s", typ)
	}
	if err := w.WriteByte('{'); err != nil {
		return err
	}
	for k := range typ.NumField() {
		if k != 0 {
			if err := w.WriteByte(','); err != nil {
				return err
			}
		}
		if err := writeColumnJSON(w, typ.Field(k), val.Field(k)); err != nil {
			return fmt.Errorf("write column %q: %w", typ.Field(k).Name, err)
		}
	}
	if err := w.WriteByte('}'); err != nil {
		return err
	}
	return nil
}

func writeColumnJSON(w BufferedWriter, typ reflect.StructField, val reflect.Value) error {
	tag, ok := typ.Tag.Lookup("sjson")
	if !ok || tag == "" {
		return fmt.Errorf("missing or invalid tag")
	}

	var (
		colArgNullZero bool
	)
	name, args, _ := strings.Cut(tag, ",")
	if args != "" {
		for arg := range strings.SplitSeq(args, ",") {
			switch arg {
			case "nullzero":
				colArgNullZero = true
			default:
				return fmt.Errorf("invalid tag arg %q", arg)
			}
		}
	}

	if _, err := w.Write(appendStringJSON(w.AvailableBuffer(), name)); err != nil {
		return err
	}
	if err := w.WriteByte(':'); err != nil {
		return err
	}

	if colArgNullZero {
		switch typ.Type.Kind() {
		case reflect.Slice, reflect.Pointer:
			if val.IsNil() {
				if _, err := w.WriteString("null"); err != nil {
					return err
				}
				return nil
			}
		default:
			if !val.Comparable() {
				return fmt.Errorf("cannot nullzero if not comparable")
			}
			if val.IsZero() {
				if _, err := w.WriteString("null"); err != nil {
					return err
				}
				return nil
			}
		}
	}

	if typ.Type.Kind() == reflect.Slice {
		if err := w.WriteByte('['); err != nil {
			return err
		}
		for i := range val.Len() {
			if i != 0 {
				if err := w.WriteByte(','); err != nil {
					return err
				}
			}
			if err := writeFieldJSON(w, typ.Type.Elem(), val.Index(i)); err != nil {
				return fmt.Errorf("write field item %s: %w", typ.Name, err)
			}
		}
		if err := w.WriteByte(']'); err != nil {
			return err
		}
		return nil
	}

	if err := writeFieldJSON(w, typ.Type, val); err != nil {
		return fmt.Errorf("write field %s: %w", typ.Name, err)
	}
	return nil
}

func writeFieldJSON(w BufferedWriter, typ reflect.Type, val reflect.Value) error {
	switch typ.Kind() {
	case reflect.String:
		if _, err := w.Write(appendStringJSON(w.AvailableBuffer(), val.Interface().(string))); err != nil {
			return err
		}
	case reflect.Bool:
		if val.Bool() {
			w.WriteString("true")
		} else {
			w.WriteString("false")
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if _, err := w.Write(strconv.AppendInt(w.AvailableBuffer(), val.Int(), 10)); err != nil {
			return err
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if _, err := w.Write(strconv.AppendUint(w.AvailableBuffer(), val.Uint(), 10)); err != nil {
			return err
		}
	case reflect.Float32:
		if _, err := w.Write(strconv.AppendFloat(w.AvailableBuffer(), val.Float(), 'f', -1, 32)); err != nil {
			return err
		}
	case reflect.Float64:
		if _, err := w.Write(strconv.AppendFloat(w.AvailableBuffer(), val.Float(), 'f', -1, 64)); err != nil {
			return err
		}
	case reflect.Struct:
		switch colVal := val.Interface().(type) {
		default:
			_ = colVal
			return fmt.Errorf("unsupported type %s", typ)
		}
	default:
		return fmt.Errorf("unsupported type %s", typ)
	}
	return nil
}

// jsonSafeSet is encoding/json.safeSet.
var jsonSafeSet = [utf8.RuneSelf]bool{
	' ':      true,
	'!':      true,
	'"':      false,
	'#':      true,
	'$':      true,
	'%':      true,
	'&':      true,
	'\'':     true,
	'(':      true,
	')':      true,
	'*':      true,
	'+':      true,
	',':      true,
	'-':      true,
	'.':      true,
	'/':      true,
	'0':      true,
	'1':      true,
	'2':      true,
	'3':      true,
	'4':      true,
	'5':      true,
	'6':      true,
	'7':      true,
	'8':      true,
	'9':      true,
	':':      true,
	';':      true,
	'<':      true,
	'=':      true,
	'>':      true,
	'?':      true,
	'@':      true,
	'A':      true,
	'B':      true,
	'C':      true,
	'D':      true,
	'E':      true,
	'F':      true,
	'G':      true,
	'H':      true,
	'I':      true,
	'J':      true,
	'K':      true,
	'L':      true,
	'M':      true,
	'N':      true,
	'O':      true,
	'P':      true,
	'Q':      true,
	'R':      true,
	'S':      true,
	'T':      true,
	'U':      true,
	'V':      true,
	'W':      true,
	'X':      true,
	'Y':      true,
	'Z':      true,
	'[':      true,
	'\\':     false,
	']':      true,
	'^':      true,
	'_':      true,
	'`':      true,
	'a':      true,
	'b':      true,
	'c':      true,
	'd':      true,
	'e':      true,
	'f':      true,
	'g':      true,
	'h':      true,
	'i':      true,
	'j':      true,
	'k':      true,
	'l':      true,
	'm':      true,
	'n':      true,
	'o':      true,
	'p':      true,
	'q':      true,
	'r':      true,
	's':      true,
	't':      true,
	'u':      true,
	'v':      true,
	'w':      true,
	'x':      true,
	'y':      true,
	'z':      true,
	'{':      true,
	'|':      true,
	'}':      true,
	'~':      true,
	'\u007f': true,
}

// appendStringJSON is based on encoding/json.encodeState.stringBytes.
//
// TODO: replace with encoding/json/jsontext.AppendQuote when it's released
func appendStringJSON(e []byte, s string) []byte {
	const hex = "0123456789abcdef"

	e = append(e, '"')
	start := 0
	for i := range len(s) {
		if b := s[i]; b < utf8.RuneSelf {
			if jsonSafeSet[b] {
				i++
				continue
			}
			if start < i {
				e = append(e, s[start:i]...)
			}
			e = append(e, '\\')
			switch b {
			case '\\', '"':
				e = append(e, b)
			case '\n':
				e = append(e, 'n')
			case '\r':
				e = append(e, 'r')
			case '\t':
				e = append(e, 't')
			default:
				// This encodes bytes < 0x20 except for \t, \n and \r.
				// If escapeHTML is set, it also escapes <, >, and &
				// because they can lead to security holes when
				// user-controlled strings are rendered into JSON
				// and served to some browsers.
				e = append(e, `u00`...)
				e = append(e, hex[b>>4])
				e = append(e, hex[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRune([]byte(s[i:]))
		if c == utf8.RuneError && size == 1 {
			if start < i {
				e = append(e, s[start:i]...)
			}
			e = append(e, `\ufffd`...)
			i += size
			start = i
			continue
		}
		// U+2028 is LINE SEPARATOR.
		// U+2029 is PARAGRAPH SEPARATOR.
		// They are both technically valid characters in JSON strings,
		// but don't work in JSONP, which has to be evaluated as JavaScript,
		// and can lead to security holes there. It is valid JSON to
		// escape them, so we do so unconditionally.
		// See http://timelessrepo.com/json-isnt-a-javascript-subset for discussion.
		if c == '\u2028' || c == '\u2029' {
			if start < i {
				e = append(e, s[start:i]...)
			}
			e = append(e, `\u202`...)
			e = append(e, hex[c&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	if start < len(s) {
		e = append(e, s[start:]...)
	}
	e = append(e, '"')
	return e
}
