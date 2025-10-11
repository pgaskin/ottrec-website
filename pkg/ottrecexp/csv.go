package ottrecexp

import (
	"bytes"
	"fmt"
	"io"
	"iter"
	"reflect"
	"slices"
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
			if err := writeTableRowsCSV(newStickyBufferedWriter(&buf), typ, val); err != nil {
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
	if err := writeTableRowsCSV(newStickyBufferedWriter(&buf), typ, val); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// WriteCSV writes the data as CSV, calling fn for each table to get w. If w is
// nil, the table is skipped.
func WriteCSV(x *Data, fn func(string) io.Writer) error {
	var err error
	for table, val := range iterTablesCSV(x)(&err) {
		typ := val.Type()
		if w := fn(table); w != nil {
			bw := newStickyBufferedWriter(w)
			if err := writeTableRowsCSV(bw, typ, val); err != nil {
				return fmt.Errorf("write table %s: %w", table, err)
			}
			if err := bw.Flush(); err != nil {
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
	bw := newStickyBufferedWriter(w)
	if err := writeDataCSVSchema(bw, new(Data)); err != nil {
		return err
	}
	return bw.Flush()
}

func WriteTableCSV[T Row](x Table[T], w io.Writer) error {
	bw := newStickyBufferedWriter(w)
	val := reflect.ValueOf(x)
	typ := val.Type()
	if err := writeTableRowsCSV(bw, typ, val); err != nil {
		return err
	}
	return bw.Flush()
}

func WriteRowCSV[T Row](x *T, w io.Writer) error {
	bw := newStickyBufferedWriter(w)
	val := reflect.ValueOf(x)
	typ := val.Type()
	if err := writeRowCSV(bw, typ, val, false); err != nil {
		return err
	}
	return bw.Flush()
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

func writeDataCSVSchema(w *stickyBufferedWriter, x any) error {
	w.StringCSV(false, "table")
	w.StringCSV(true, "column")
	w.StringCSV(true, "description")
	if crlfCSV {
		w.Byte('\r')
	}
	w.Byte('\n')

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

			w.StringCSV(false, table)
			w.StringCSV(true, name)
			w.StringCSV(true, doc)
			if crlfCSV {
				w.Byte('\r')
			}
			w.Byte('\n')
		}
	}
	if err != nil {
		return err
	}

	return w.Err()
}

func writeTableRowsCSV(w *stickyBufferedWriter, typ reflect.Type, val reflect.Value) error {
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
	return w.Err()
}

func writeRowCSV(w *stickyBufferedWriter, typ reflect.Type, val reflect.Value, header bool) error {
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
			w.Byte(commaCSV)
		}
		if err := writeColumnCSV(w, typ.Field(k), val.Field(k), header); err != nil {
			return fmt.Errorf("write column %q: %w", typ.Field(k).Name, err)
		}
	}
	if crlfCSV {
		w.Byte('\r')
	}
	w.Byte('\n')

	return w.Err()
}

func writeColumnCSV(w *stickyBufferedWriter, typ reflect.StructField, val reflect.Value, header bool) error {
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
		w.StringCSV(false, name)
		return w.Err()
	}

	if emptyzero {
		switch typ.Type.Kind() {
		case reflect.Slice, reflect.Pointer:
			if val.IsNil() {
				return w.Err()
			}
		default:
			if !val.Comparable() {
				return fmt.Errorf("cannot nullzero if not comparable")
			}
			if val.IsZero() {
				return w.Err()
			}
		}
	}

	if typ.Type.Kind() == reflect.Slice {
		if val.Len() != 0 {
			w.Byte('"')
			for i := range val.Len() {
				if i != 0 {
					w.Byte(',')
				}
				if err := writeFieldCSV(w, typ.Type.Elem(), val.Index(i), true); err != nil {
					return fmt.Errorf("write field item %s: %w", typ.Name, err)
				}
			}
			w.Byte('"')
		}
		return w.Err()
	}

	if err := writeFieldCSV(w, typ.Type, val, false); err != nil {
		return fmt.Errorf("write field %s: %w", typ.Name, err)
	}
	return w.Err()
}

func writeFieldCSV(w *stickyBufferedWriter, typ reflect.Type, val reflect.Value, arr bool) error {
	switch typ.Kind() {
	case reflect.String:
		if arr {
			if strings.ContainsRune(val.Interface().(string), ',') {
				return fmt.Errorf("array item %q contains comma", val.Interface().(string))
			}
			w.StringInQuotesCSV(val.Interface().(string))
		} else {
			w.StringCSV(false, val.Interface().(string))
		}
	case reflect.Bool:
		if val.Bool() {
			w.Byte('1')
		} else {
			w.Byte('0')
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

// writeStringCSV is based on encoding/csv.Writer.Write
func (w *stickyBufferedWriter) StringCSV(comma bool, field string) {
	if comma {
		w.Byte(',')
	}
	if !fieldNeedsQuotesCSV(field, commaCSV) {
		w.String(field)
	} else {
		w.Byte('"')
		w.StringInQuotesCSV(field)
		w.Byte('"')
	}
}

// writeStringQuotedCSV is based on encoding/csv.Writer.Write
func (w *stickyBufferedWriter) StringInQuotesCSV(field string) {
	for len(field) > 0 {
		// Search for special characters.
		i := strings.IndexAny(field, "\"\r\n")
		if i < 0 {
			i = len(field)
		}

		// Copy verbatim everything before the special character.
		w.String(field[:i])
		field = field[i:]

		// Encode the special character.
		if len(field) > 0 {
			switch field[0] {
			case '"':
				w.String(`""`)
			case '\r':
				if crlfCSV {
					w.Byte('\r')
				}
			case '\n':
				if crlfCSV {
					w.String("\r\n")
				} else {
					w.Byte('\n')
				}
			}
			field = field[1:]
		}
	}
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
