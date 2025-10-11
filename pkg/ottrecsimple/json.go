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

// JSONSchemaID, if set, is used as the ID of the JSON schema, and is included
// in the JSON and JSON schema output. This should be a URL to the schema.
var JSONSchemaID string

func JSON(x *Data) []byte {
	if x == nil {
		return nil
	}
	var b bytes.Buffer
	if err := writeDataJSON(&b, x); err != nil {
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
	return writeDataJSON(newBufferedWriter(w), x)
}

func WriteJSONSchema(w io.Writer) error {
	return writeDataJSONSchema(newBufferedWriter(w), new(Data))
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
	if JSONSchemaID != "" {
		if err := writeKeyValueJSON(w, false, "$schema", JSONSchemaID); err != nil {
			return err
		}
		if err := w.WriteByte(','); err != nil {
			return err
		}
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

func writeDataJSONSchema(w BufferedWriter, data any) error {
	if err := w.WriteByte('{'); err != nil {
		return err
	}
	var (
		typ = reflect.TypeOf(data)
	)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if err := writeKeyValueJSON(w, false, "$schema", "https://json-schema.org/draft/2020-12/schema"); err != nil {
		return err
	}
	if JSONSchemaID != "" {
		if err := writeKeyValueJSON(w, true, "$id", JSONSchemaID); err != nil {
			return err
		}
	}
	if err := writeKeyValueJSON(w, true, "title", "Ottawa Recreation Schedules"); err != nil {
		return err
	}
	if err := writeKeyValueJSON(w, true, "description", "Simplified dataset of City of Ottawa recreation schedules"); err != nil {
		return err
	}
	if err := writeKeyValueJSON(w, true, "type", "object"); err != nil {
		return err
	}
	if err := writeKeyJSON(w, true, "properties"); err != nil {
		return err
	}
	if err := w.WriteByte('{'); err != nil {
		return err
	}
	for i := range typ.NumField() {
		if i != 0 {
			if err := w.WriteByte(','); err != nil {
				return err
			}
		}
		if err := writeTableJSONSchema(w, typ.Field(i)); err != nil {
			return fmt.Errorf("write table %s: %w", typ.Field(i).Name, err)
		}
	}
	if err := w.WriteByte('}'); err != nil {
		return err
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

func writeTableJSONSchema(w BufferedWriter, typ reflect.StructField) error {
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

	if _, err := w.Write(appendStringJSON(w.AvailableBuffer(), name)); err != nil {
		return err
	}
	if err := w.WriteByte(':'); err != nil {
		return err
	}
	if err := w.WriteByte('{'); err != nil {
		return err
	}
	if err := writeKeyValueJSON(w, false, "type", "array"); err != nil {
		return err
	}
	if err := writeKeyValueJSON(w, true, "description", doc); err != nil {
		return err
	}
	if err := writeKeyJSON(w, true, "items"); err != nil {
		return err
	}
	if err := writeTableRowsJSONSchema(w, typ.Type); err != nil {
		return err
	}
	if err := w.WriteByte('}'); err != nil {
		return err
	}
	return nil
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

func writeTableRowsJSONSchema(w BufferedWriter, typ reflect.Type) error {
	if typ.Kind() != reflect.Slice {
		return fmt.Errorf("unsupported type %s", typ)
	}
	if err := w.WriteByte('{'); err != nil {
		return err
	}
	if err := writeKeyValueJSON(w, false, "type", "object"); err != nil {
		return err
	}
	if err := writeKeyJSON(w, true, "properties"); err != nil {
		return err
	}
	if err := w.WriteByte('{'); err != nil {
		return err
	}
	if err := writeRowJSONSchema(w, typ.Elem()); err != nil {
		return fmt.Errorf("write row: %w", err)
	}
	if err := w.WriteByte('}'); err != nil {
		return err
	}
	if err := w.WriteByte('}'); err != nil {
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

func writeRowJSONSchema(w BufferedWriter, typ reflect.Type) error {
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return fmt.Errorf("unsupported type %s", typ)
	}
	for k := range typ.NumField() {
		if k != 0 {
			if err := w.WriteByte(','); err != nil {
				return err
			}
		}
		if err := writeColumnJSONSchema(w, typ.Field(k)); err != nil {
			return fmt.Errorf("write column %q: %w", typ.Field(k).Name, err)
		}
	}
	return nil
}

func writeColumnJSON(w BufferedWriter, typ reflect.StructField, val reflect.Value) error {
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

	if err := writeKeyJSON(w, false, name); err != nil {
		return err
	}

	if nullzero {
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

func writeColumnJSONSchema(w BufferedWriter, typ reflect.StructField) error {
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

	if err := writeKeyJSON(w, false, name); err != nil {
		return err
	}
	if err := w.WriteByte('{'); err != nil {
		return err
	}
	if err := writeKeyValueJSON(w, false, "description", doc); err != nil {
		return err
	}
	if typ.Type.Kind() == reflect.Slice {
		if err := writeKeyValueJSON(w, true, "type", "array"); err != nil {
			return nil
		}
		if err := writeKeyJSON(w, true, "items"); err != nil {
			return nil
		}
		if err := w.WriteByte('{'); err != nil {
			return err
		}
		if err := writeKeyJSON(w, false, "type"); err != nil {
			return nil
		}
		if err := writeFieldJSONSchema(w, typ.Type.Elem(), nullzero); err != nil {
			return fmt.Errorf("write field item %s: %w", typ.Name, err)
		}
		if pattern != "" {
			if err := writeKeyValueJSON(w, true, "pattern", pattern); err != nil {
				return err
			}
		}
		if err := w.WriteByte('}'); err != nil {
			return err
		}
	} else {
		if err := writeKeyJSON(w, true, "type"); err != nil {
			return nil
		}
		if err := writeFieldJSONSchema(w, typ.Type, nullzero); err != nil {
			return fmt.Errorf("write field %s: %w", typ.Name, err)
		}
		if pattern != "" {
			if err := writeKeyValueJSON(w, true, "pattern", pattern); err != nil {
				return err
			}
		}
	}
	if err := w.WriteByte('}'); err != nil {
		return err
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
			if _, err := w.WriteString("true"); err != nil {
				return err
			}
		} else {
			if _, err := w.WriteString("false"); err != nil {
				return err
			}
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

func writeFieldJSONSchema(w BufferedWriter, typ reflect.Type, nullable bool) error {
	if nullable {
		if err := w.WriteByte('['); err != nil {
			return err
		}
	}
	switch typ.Kind() {
	case reflect.String:
		if _, err := w.Write(appendStringJSON(w.AvailableBuffer(), "string")); err != nil {
			return err
		}
	case reflect.Bool:
		if _, err := w.Write(appendStringJSON(w.AvailableBuffer(), "boolean")); err != nil {
			return err
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if _, err := w.Write(appendStringJSON(w.AvailableBuffer(), "integer")); err != nil {
			return err
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if _, err := w.Write(appendStringJSON(w.AvailableBuffer(), "integer")); err != nil {
			return err
		}
	case reflect.Float32:
		if _, err := w.Write(appendStringJSON(w.AvailableBuffer(), "number")); err != nil {
			return err
		}
	case reflect.Float64:
		if _, err := w.Write(appendStringJSON(w.AvailableBuffer(), "number")); err != nil {
			return err
		}
	case reflect.Struct:
		switch reflect.New(typ).Elem().Interface().(type) {
		default:
			return fmt.Errorf("unsupported type %s", typ)
		}
	default:
		return fmt.Errorf("unsupported type %s", typ)
	}
	if nullable {
		if err := w.WriteByte(','); err != nil {
			return err
		}
		if _, err := w.Write(appendStringJSON(w.AvailableBuffer(), "null")); err != nil {
			return err
		}
		if err := w.WriteByte(']'); err != nil {
			return err
		}
	}
	return nil
}

func writeKeyJSON(w BufferedWriter, comma bool, key string) error {
	if comma {
		if err := w.WriteByte(','); err != nil {
			return err
		}
	}
	if _, err := w.Write(appendStringJSON(w.AvailableBuffer(), key)); err != nil {
		return err
	}
	if err := w.WriteByte(':'); err != nil {
		return err
	}
	return nil
}

func writeKeyValueJSON(w BufferedWriter, comma bool, key string, value any) error {
	val := reflect.ValueOf(value)
	typ := val.Type()
	if err := writeKeyJSON(w, comma, key); err != nil {
		return err
	}
	if err := writeFieldJSON(w, typ, val); err != nil {
		return err
	}
	return nil
}

// TODO: support writing json schema

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
