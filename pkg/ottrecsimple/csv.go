package ottrecsimple

import (
	"bytes"
	"fmt"
	"io"
	"iter"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	crlfCSV  = true
	commaCSV = ','
)

func CSV(x *Data) iter.Seq2[string, []byte] {
	if x == nil {
		return nil
	}
	return func(yield func(string, []byte) bool) {
		var buf bytes.Buffer
		var err error
		for table, val := range iterTablesCSV(x)(&err) {
			typ := val.Type()
			if err := writeTableRowsCSV(&buf, typ, val); err != nil {
				panic(err)
			}
			if !yield(table, slices.Clone(buf.Bytes())) {
				return
			}
			buf.Reset()
		}
		if err != nil {
			panic(err)
		}
	}
}

func CSVSchema() []byte {
	var buf bytes.Buffer
	if err := WriteCSVSchema(&buf); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func TableCSV[T Row](x Table[T]) []byte {
	val := reflect.ValueOf(x)
	typ := val.Type()
	var buf bytes.Buffer
	if err := writeTableRowsCSV(&buf, typ, val); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// WriteCSV writes the data as CSV, calling fn for each table to get w. If w is
// nil, the table is skipped. If w implements [BufferedWriter] (like
// [bytes.Buffer] or [bufio.Writer]), it will be used directly.
func WriteCSV(x *Data, fn func(string) io.Writer) error {
	var err error
	for table, val := range iterTablesCSV(x)(&err) {
		typ := val.Type()
		if w := fn(table); w != nil {
			if err := writeTableRowsCSV(newBufferedWriter(w), typ, val); err != nil {
				return fmt.Errorf("write table %s: %w", table, err)
			}
		}
	}
	if err != nil {
		return err
	}
	return nil
}

func WriteCSVSchema(w io.Writer) error {
	return writeDataCSVSchema(newBufferedWriter(w), new(Data))
}

func WriteTableCSV[T Row](x Table[T], w io.Writer) error {
	val := reflect.ValueOf(x)
	typ := val.Type()
	return writeTableRowsCSV(newBufferedWriter(w), typ, val)
}

func WriteRowCSV[T Row](x *T, w io.Writer) error {
	val := reflect.ValueOf(x)
	typ := val.Type()
	return writeRowCSV(newBufferedWriter(w), typ, val, false)
}

func iterTablesCSV(x any) func(*error) iter.Seq2[string, reflect.Value] {
	return func(err *error) iter.Seq2[string, reflect.Value] {
		return func(yield func(string, reflect.Value) bool) {
			*err = func() error {
				var (
					val = reflect.ValueOf(x)
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
					ttyp := typ.Field(i)
					tval := val.Field(i)

					tag, ok := ttyp.Tag.Lookup("scsv")
					if !ok || tag == "" {
						return fmt.Errorf("missing or invalid tag")
					}

					name, args, _ := strings.Cut(tag, ",")
					if args != "" {
						for arg := range strings.SplitSeq(args, ",") {
							return fmt.Errorf("invalid tag arg %q", arg)
						}
					}
					if !yield(name, tval) {
						return nil
					}
				}
				return nil
			}()
		}
	}
}

func writeDataCSVSchema(w BufferedWriter, x any) error {
	if _, err := w.WriteString(`table,column,description`); err != nil {
		return nil
	}
	if crlfCSV {
		if err := w.WriteByte('\r'); err != nil {
			return err
		}
	}
	if err := w.WriteByte('\n'); err != nil {
		return err
	}
	var err error
	for table, val := range iterTablesCSV(x)(&err) {
		typ := val.Type()
		if typ.Kind() != reflect.Slice {
			return fmt.Errorf("table %q: unsupported type %s", table, typ)
		}
		typ = typ.Elem().Elem()
		for j := range typ.NumField() {
			row := typ.Field(j)

			name, ok := row.Tag.Lookup("scsv")
			if !ok || name == "" {
				return fmt.Errorf("table %q: missing or invalid tag", table)
			}
			name, _, _ = strings.Cut(name, ",")

			doc, ok := row.Tag.Lookup("doc")
			if !ok {
				return fmt.Errorf("table %q: missing doc tag", table)
			}

			if err := writeStringCSV(w, table); err != nil {
				return err
			}
			if err := w.WriteByte(','); err != nil {
				return err
			}
			if err := writeStringCSV(w, name); err != nil {
				return err
			}
			if err := w.WriteByte(','); err != nil {
				return err
			}
			if err := writeStringCSV(w, doc); err != nil {
				return err
			}
			if crlfCSV {
				if err := w.WriteByte('\r'); err != nil {
					return err
				}
			}
			if err := w.WriteByte('\n'); err != nil {
				return err
			}
		}
	}
	return err
}

func writeTableRowsCSV(w BufferedWriter, typ reflect.Type, val reflect.Value) error {
	if typ.Kind() != reflect.Slice {
		return fmt.Errorf("unsupported type %s", typ)
	}
	for j := range val.Len() {
		if j == 0 {
			if err := writeRowCSV(w, typ.Elem(), val.Index(j), true); err != nil {
				return fmt.Errorf("write row: %w", err)
			}
		}
		if err := writeRowCSV(w, typ.Elem(), val.Index(j), false); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
	}
	return nil
}

func writeRowCSV(w BufferedWriter, typ reflect.Type, val reflect.Value, header bool) error {
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
	for k := range typ.NumField() {
		if k != 0 {
			if err := w.WriteByte(byte(commaCSV)); err != nil {
				return err
			}
		}
		if err := writeColumnCSV(w, typ.Field(k), val.Field(k), header); err != nil {
			return fmt.Errorf("write column %q: %w", typ.Field(k).Name, err)
		}
	}
	if crlfCSV {
		if err := w.WriteByte('\r'); err != nil {
			return err
		}
	}
	if err := w.WriteByte('\n'); err != nil {
		return err
	}

	return nil
}

func writeColumnCSV(w BufferedWriter, typ reflect.StructField, val reflect.Value, header bool) error {
	tag, ok := typ.Tag.Lookup("scsv")
	if !ok || tag == "" {
		return fmt.Errorf("missing or invalid tag")
	}

	var (
		emptyzero bool
	)
	name, args, _ := strings.Cut(tag, ",")
	if args != "" {
		for arg := range strings.SplitSeq(args, ",") {
			switch arg {
			case "emptyzero":
				emptyzero = true
			default:
				return fmt.Errorf("invalid tag arg %q", arg)
			}
		}
	}

	if header {
		if err := writeStringCSV(w, name); err != nil {
			return err
		}
		return nil
	}

	if emptyzero {
		switch typ.Type.Kind() {
		case reflect.Slice, reflect.Pointer:
			if val.IsNil() {
				return nil
			}
		default:
			if !val.Comparable() {
				return fmt.Errorf("cannot nullzero if not comparable")
			}
			if val.IsZero() {
				return nil
			}
		}
	}

	if typ.Type.Kind() == reflect.Slice {
		if val.Len() != 0 {
			if err := w.WriteByte('"'); err != nil {
				return err
			}
			for i := range val.Len() {
				if i != 0 {
					if err := w.WriteByte(','); err != nil {
						return err
					}
				}
				if err := writeFieldCSV(w, typ.Type.Elem(), val.Index(i), true); err != nil {
					return fmt.Errorf("write field item %s: %w", typ.Name, err)
				}
			}
			if err := w.WriteByte('"'); err != nil {
				return err
			}
		}
		return nil
	}

	if err := writeFieldCSV(w, typ.Type, val, false); err != nil {
		return fmt.Errorf("write field %s: %w", typ.Name, err)
	}
	return nil
}

func writeFieldCSV(w BufferedWriter, typ reflect.Type, val reflect.Value, arr bool) error {
	switch typ.Kind() {
	case reflect.String:
		if arr {
			if strings.ContainsRune(val.Interface().(string), ',') {
				return fmt.Errorf("array item %q contains comma", val.Interface().(string))
			}
			return writeStringQuotedCSV(w, val.Interface().(string))
		}
		return writeStringCSV(w, val.Interface().(string))
	case reflect.Bool:
		if val.Bool() {
			if _, err := w.WriteString("1"); err != nil {
				return err
			}
		} else {
			if _, err := w.WriteString("0"); err != nil {
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

// writeStringCSV is based on encoding/csv.Writer.Write
func writeStringCSV(w BufferedWriter, field string) error {
	if !fieldNeedsQuotesCSV(field, commaCSV) {
		if _, err := w.WriteString(field); err != nil {
			return err
		}
		return nil
	}

	if err := w.WriteByte('"'); err != nil {
		return err
	}
	if err := writeStringQuotedCSV(w, field); err != nil {
		return err
	}
	if err := w.WriteByte('"'); err != nil {
		return err
	}
	return nil
}

// writeStringQuotedCSV is based on encoding/csv.Writer.Write
func writeStringQuotedCSV(w BufferedWriter, field string) error {
	for len(field) > 0 {
		// Search for special characters.
		i := strings.IndexAny(field, "\"\r\n")
		if i < 0 {
			i = len(field)
		}

		// Copy verbatim everything before the special character.
		if _, err := w.WriteString(field[:i]); err != nil {
			return err
		}
		field = field[i:]

		// Encode the special character.
		if len(field) > 0 {
			var err error
			switch field[0] {
			case '"':
				_, err = w.WriteString(`""`)
			case '\r':
				if crlfCSV {
					err = w.WriteByte('\r')
				}
			case '\n':
				if crlfCSV {
					_, err = w.WriteString("\r\n")
				} else {
					err = w.WriteByte('\n')
				}
			}
			field = field[1:]
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// fieldNeedsQuotesCSV is based on encoding/csv.Writer.fieldNeedsQuotes
func fieldNeedsQuotesCSV(field string, comma rune) bool {
	if field == "" {
		return false
	}

	if field == `\.` {
		return true
	}

	if comma < utf8.RuneSelf {
		for i := 0; i < len(field); i++ {
			c := field[i]
			if c == '\n' || c == '\r' || c == '"' || c == byte(comma) {
				return true
			}
		}
	} else {
		if strings.ContainsRune(field, comma) || strings.ContainsAny(field, "\"\r\n") {
			return true
		}
	}

	r1, _ := utf8.DecodeRuneInString(field)
	return unicode.IsSpace(r1)
}
