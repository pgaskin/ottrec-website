package ottrecsimple

import (
	"bytes"
	"encoding/csv"
	"flag"
	"io"
	"iter"
	"testing"
)

var LogCSV = flag.Bool("log-csv", false, "always log CSV in tests")

func TestCSV(t *testing.T) {
	for name, data := range testdata() {
		name, data := name, data
		t.Run(name, func(t *testing.T) {
			seq, err := catch1(func() iter.Seq2[string, []byte] {
				return CSV(data)
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for table, buf := range catchSeq2(seq)(&err) {
				if len(buf) == 0 {
					t.Fatalf("table %q: empty csv", table)
				}
				logCSV(t, true, table, buf)

				if err := validCSV(buf); err != nil {
					logCSV(t, false, table, buf)
					t.Fatalf("table %q: invalid csv: %v", table, err)
				}

				// TODO: parse schema and ensure tables/headers match

				switch data {
				case EmptyData:
					if sha := sha1sum(buf); sha != map[string]string{
						"facility":    "0cb5e85f0e3c9c2aea18ff0dae8f46345c1a82cd",
						"activity":    "fe8a08310eddfe6d20479c72e688037a33e2ce22",
						"error":       "5441d9ab6a74517681827f05ae4da06b07293257",
						"html":        "3c193f3628a0ec52fc7ea7efe2cca136e1c7504a",
						"attribution": "fce2f18d64f0e436dc8ce88f815ad9b2902d02a8",
					}[table] {
						logJSON(t, false, buf)
						t.Errorf("table %q: incorrect output or outdated test (sha: %s)", table, sha)
					}
				case DummyData:
					if sha := sha1sum(buf); sha != map[string]string{
						"facility":    "0a8d3acd0b1db3157e467fb63bde6e896739a70c",
						"activity":    "9bd1fc1fde1d0e57c0603f281aa9205dbfe4df62",
						"error":       "484964de6b1eab8e4704806b78f68bbdd6dd99ec",
						"html":        "c9cc1815fef07d65670de69747b5d5abf4557771",
						"attribution": "64c53be844ef8855bbb2287440c7815947775898",
					}[table] {
						logJSON(t, false, buf)
						t.Errorf("table %q: incorrect output or outdated test (sha: %s)", table, sha)
					}
				}
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestCSVSchema(t *testing.T) {
	buf, err := catch1(func() []byte {
		return CSVSchema()
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(buf) == 0 {
		t.Fatalf("empty csv")
	}
	logCSV(t, true, "schema", buf)

	if err := validCSV(buf); err != nil {
		logCSV(t, false, "schema", buf)
		t.Fatalf("invalid csv: %v", err)
	}
	// TODO: test structure
}

func validCSV(buf []byte) error {
	r := csv.NewReader(bytes.NewReader(buf))
	r.ReuseRecord = true
	r.Comma = commaCSV
	for {
		_, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func logCSV(t *testing.T, first bool, table string, buf []byte) {
	if first == *LogCSV {
		t.Log("output: table " + table + "\n" + string(buf))
	}
}
