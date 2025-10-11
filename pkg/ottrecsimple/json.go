package ottrecsimple

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"
)

// JSONSchemaID, if set, is used as the ID of the JSON schema, and is included
// in the JSON and JSON schema output. This should be a URL to the schema.
var JSONSchemaID string

func JSON(x *Data) []byte {
	if x == nil {
		return nil
	}
	var b bytes.Buffer
	if err := WriteJSON(x, &b); err != nil {
		panic(err)
	}
	return b.Bytes()
}

func JSONSchema() []byte {
	var buf bytes.Buffer
	if err := WriteJSONSchema(&buf); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// WriteJSON writes the data as JSON to w. If w implements [BufferedWriter]
// (like [bytes.Buffer] or [bufio.Writer]), it will be used directly.
func WriteJSON(x *Data, w io.Writer) error {
	return writeDataJSON(newStickyBufferedWriter(newBufferedWriter(w)), x)
}

func WriteJSONSchema(w io.Writer) error {
	return writeDataJSONSchema(newStickyBufferedWriter(newBufferedWriter(w)), new(Data))
}

func WriteTableJSON[T Row](x Table[T], w io.Writer) error {
	val := reflect.ValueOf(x)
	typ := val.Type()
	return writeTableRowsJSON(newStickyBufferedWriter(newBufferedWriter(w)), typ, val)
}

func WriteRowJSON[T Row](x *T, w io.Writer) error {
	val := reflect.ValueOf(x)
	typ := val.Type()
	return writeRowJSON(newStickyBufferedWriter(newBufferedWriter(w)), typ, val)
}

func writeDataJSON(w *stickyBufferedWriter, data any) error {
	w.Byte('{')
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
	if JSONSchemaID != "" {
		w.KeyValueJSON(false, "$schema", JSONSchemaID)
		w.Byte(',')
	}
	for i := range typ.NumField() {
		if i != 0 {
			w.Byte(',')
		}
		if err := writeTableJSON(w, typ.Field(i), val.Field(i)); err != nil {
			return fmt.Errorf("write table %s: %w", typ.Field(i).Name, err)
		}

	}
	w.Byte('}')
	return w.Err()
}

func writeDataJSONSchema(w *stickyBufferedWriter, data any) error {
	w.Byte('{')
	var (
		typ = reflect.TypeOf(data)
	)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	w.KeyValueJSON(false, "$schema", "https://json-schema.org/draft/2020-12/schema")
	if JSONSchemaID != "" {
		w.KeyValueJSON(true, "$id", JSONSchemaID)
	}
	w.KeyValueJSON(true, "title", "Ottawa Recreation Schedules")
	w.KeyValueJSON(true, "description", "Scraped City of Ottawa recreation schedule data")
	w.KeyValueJSON(true, "type", "object")
	w.KeyJSON(true, "properties")
	w.Byte('{')
	for i := range typ.NumField() {
		if i != 0 {
			w.Byte(',')
		}
		if err := writeTableJSONSchema(w, typ.Field(i)); err != nil {
			return fmt.Errorf("write table %s: %w", typ.Field(i).Name, err)
		}
	}
	w.Byte('}')
	w.Byte('}')
	return w.Err()
}

func writeTableJSON(w *stickyBufferedWriter, typ reflect.StructField, val reflect.Value) error {
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

	w.KeyJSON(false, name)
	return writeTableRowsJSON(w, typ.Type, val)
}

func writeTableJSONSchema(w *stickyBufferedWriter, typ reflect.StructField) error {
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

	doc, ok := typ.Tag.Lookup("doc")
	if !ok {
		return fmt.Errorf("missing doc tag")
	}

	w.KeyJSON(false, name)
	w.Byte('{')
	w.KeyValueJSON(false, "type", "array")
	w.KeyValueJSON(true, "description", doc)
	w.KeyJSON(true, "items")
	if err := writeTableRowsJSONSchema(w, typ.Type); err != nil {
		return err
	}
	w.Byte('}')
	return w.Err()
}

func writeTableRowsJSON(w *stickyBufferedWriter, typ reflect.Type, val reflect.Value) error {
	w.Byte('[')
	if typ.Kind() != reflect.Slice {
		return fmt.Errorf("unsupported type %s", typ)
	}
	for j := range val.Len() {
		if j != 0 {
			w.Byte(',')
		}
		if err := writeRowJSON(w, typ.Elem(), val.Index(j)); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
	}
	w.Byte(']')
	return w.Err()
}

func writeTableRowsJSONSchema(w *stickyBufferedWriter, typ reflect.Type) error {
	if typ.Kind() != reflect.Slice {
		return fmt.Errorf("unsupported type %s", typ)
	}
	w.Byte('{')
	w.KeyValueJSON(false, "type", "object")
	w.KeyJSON(true, "properties")
	w.Byte('{')
	if err := writeRowJSONSchema(w, typ.Elem()); err != nil {
		return fmt.Errorf("write row: %w", err)
	}
	w.Byte('}')
	w.Byte('}')
	return w.Err()
}

func writeRowJSON(w *stickyBufferedWriter, typ reflect.Type, val reflect.Value) error {
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
	w.Byte('{')
	for k := range typ.NumField() {
		if k != 0 {
			w.Byte(',')
		}
		if err := writeColumnJSON(w, typ.Field(k), val.Field(k)); err != nil {
			return fmt.Errorf("write column %q: %w", typ.Field(k).Name, err)
		}
	}
	w.Byte('}')
	return w.Err()
}

func writeRowJSONSchema(w *stickyBufferedWriter, typ reflect.Type) error {
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("unsupported type %s", typ)
	}
	for k := range typ.NumField() {
		if k != 0 {
			w.Byte(',')
		}
		if err := writeColumnJSONSchema(w, typ.Field(k)); err != nil {
			return fmt.Errorf("write column %q: %w", typ.Field(k).Name, err)
		}
	}
	return w.Err()
}

func writeColumnJSON(w *stickyBufferedWriter, typ reflect.StructField, val reflect.Value) error {
	tag, ok := typ.Tag.Lookup("sjson")
	if !ok || tag == "" {
		return fmt.Errorf("missing or invalid tag")
	}

	var (
		nullzero bool
	)
	name, args, _ := strings.Cut(tag, ",")
	if args != "" {
		for arg := range strings.SplitSeq(args, ",") {
			switch arg {
			case "nullzero":
				nullzero = true
			default:
				return fmt.Errorf("invalid tag arg %q", arg)
			}
		}
	}

	w.KeyJSON(false, name)

	if nullzero {
		switch typ.Type.Kind() {
		case reflect.Slice, reflect.Pointer:
			if val.IsNil() {
				w.String("null")
				return w.Err()
			}
		default:
			if !val.Comparable() {
				return fmt.Errorf("cannot nullzero if not comparable")
			}
			if val.IsZero() {
				w.String("null")
				return w.Err()
			}
		}
	}

	if typ.Type.Kind() == reflect.Slice {
		w.Byte('[')
		for i := range val.Len() {
			if i != 0 {
				w.Byte(',')
			}
			if err := writeFieldJSON(w, typ.Type.Elem(), val.Index(i)); err != nil {
				return fmt.Errorf("write field item %s: %w", typ.Name, err)
			}
		}
		w.Byte(']')
		return w.Err()
	}

	if err := writeFieldJSON(w, typ.Type, val); err != nil {
		return fmt.Errorf("write field %s: %w", typ.Name, err)
	}
	return w.Err()
}

func writeColumnJSONSchema(w *stickyBufferedWriter, typ reflect.StructField) error {
	tag, ok := typ.Tag.Lookup("sjson")
	if !ok || tag == "" {
		return fmt.Errorf("missing or invalid tag")
	}

	var (
		nullzero bool
	)
	name, args, _ := strings.Cut(tag, ",")
	if args != "" {
		for arg := range strings.SplitSeq(args, ",") {
			switch arg {
			case "nullzero":
				nullzero = true
			default:
				return fmt.Errorf("invalid tag arg %q", arg)
			}
		}
	}

	doc, ok := typ.Tag.Lookup("doc")
	if !ok {
		return fmt.Errorf("missing doc tag")
	}

	pattern, _ := typ.Tag.Lookup("pattern")

	w.KeyJSON(false, name)
	w.Byte('{')
	w.KeyValueJSON(false, "description", doc)
	if typ.Type.Kind() == reflect.Slice {
		w.KeyValueJSON(true, "type", "array")
		w.KeyJSON(true, "items")
		w.Byte('{')
		w.KeyJSON(false, "type")
		if err := writeFieldJSONSchema(w, typ.Type.Elem(), nullzero); err != nil {
			return fmt.Errorf("write field item %s: %w", typ.Name, err)
		}
		if pattern != "" {
			w.KeyValueJSON(true, "pattern", pattern)
		}
		w.Byte('}')
	} else {
		w.KeyJSON(true, "type")
		if err := writeFieldJSONSchema(w, typ.Type, nullzero); err != nil {
			return fmt.Errorf("write field %s: %w", typ.Name, err)
		}
		if pattern != "" {
			w.KeyValueJSON(true, "pattern", pattern)
		}
	}
	w.Byte('}')
	return w.Err()
}

func writeFieldJSON(w *stickyBufferedWriter, typ reflect.Type, val reflect.Value) error {
	switch typ.Kind() {
	case reflect.String:
		w.StringJSON(val.Interface().(string))
	case reflect.Bool:
		if val.Bool() {
			w.String("true")
		} else {
			w.String("false")
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		w.Int(val.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		w.Uint(val.Uint(), 10)
	case reflect.Float32:
		w.Float(val.Float(), 'f', -1, 32)
	case reflect.Float64:
		w.Float(val.Float(), 'f', -1, 64)
	case reflect.Struct:
		switch colVal := val.Interface().(type) {
		default:
			_ = colVal
			return fmt.Errorf("unsupported type %s", typ)
		}
	default:
		return fmt.Errorf("unsupported type %s", typ)
	}
	return w.Err()
}

func writeFieldJSONSchema(w *stickyBufferedWriter, typ reflect.Type, nullable bool) error {
	if nullable {
		w.Byte('[')
	}
	switch typ.Kind() {
	case reflect.String:
		w.StringJSON("string")
	case reflect.Bool:
		w.StringJSON("boolean")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		w.StringJSON("integer")
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		w.StringJSON("integer")
	case reflect.Float32:
		w.StringJSON("number")
	case reflect.Float64:
		w.StringJSON("number")
	case reflect.Struct:
		switch reflect.New(typ).Elem().Interface().(type) {
		default:
			return fmt.Errorf("unsupported type %s", typ)
		}
	default:
		return fmt.Errorf("unsupported type %s", typ)
	}
	if nullable {
		w.Byte(',')
		w.StringJSON("null")
		w.Byte(']')
	}
	return w.Err()
}

func (w *stickyBufferedWriter) KeyJSON(comma bool, key string) {
	if comma {
		w.Byte(',')
	}
	w.StringJSON(key)
	w.Byte(':')
}

func (w *stickyBufferedWriter) KeyValueJSON(comma bool, key string, value string) {
	w.KeyJSON(comma, key)
	w.StringJSON(value)
}

func (w *stickyBufferedWriter) StringJSON(s string) {
	w.Write(appendStringJSON(w.AvailableBuffer(), s))
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
