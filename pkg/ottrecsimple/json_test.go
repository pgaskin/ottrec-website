package ottrecsimple

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

var LogJSON = flag.Bool("log-json", false, "always log json in tests")

func TestJSON(t *testing.T) {
	var schema *jsonschema.Schema
	if buf, err := catch1(JSONSchema); err == nil {
		if sch, err := compileSchema(JSONSchemaID, buf); err == nil {
			schema = sch
		}
	}
	if schema == nil {
		t.Logf("not validating json schema")
	}
	for name, data := range testdata() {
		name, data := name, data
		t.Run(name, func(t *testing.T) {
			buf, err := catch1(func() []byte {
				return JSON(data)
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(buf) == 0 {
				t.Fatalf("empty json")
			}
			logJSON(t, true, buf)

			obj, err := jsonschema.UnmarshalJSON(bytes.NewReader(buf))
			if err != nil {
				if !*LogJSON {
					t.Log("output:\n" + string(indentJSON(buf, "", "  ")))
				}
				t.Fatalf("invalid json: %v", err)
			}
			if schema != nil {
				if err := schema.Validate(obj); err != nil {
					t.Fatalf("failed to validate json against schema: %v", err)
				}
			}

			switch data {
			case EmptyData:
				if sha := sha1sum(buf); sha != "e6b245bd98849b97d072131dba29bde97f5380b0" {
					logJSON(t, false, buf)
					t.Errorf("incorrect output or outdated test (sha: %s)", sha)
				}
			case DummyData:
				if sha := sha1sum(buf); sha != "f7ff14f76d74152eb975a35faffc62d364839a80" {
					logJSON(t, false, buf)
					t.Errorf("incorrect output or outdated test (sha: %s)", sha)
				}
			}
		})
	}
}

func TestJSONSchema(t *testing.T) {
	buf, err := catch1(func() []byte {
		return JSONSchema()
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(buf) == 0 {
		t.Fatalf("empty json")
	}
	logJSON(t, true, buf)

	sch, err := compileSchema(JSONSchemaID, buf)
	if err != nil {
		logJSON(t, false, buf)
		t.Fatalf("unexpected error: %v", err)
	}
	_ = sch
}

func compileSchema(url string, buf []byte) (*jsonschema.Schema, error) {
	obj, err := jsonschema.UnmarshalJSON(bytes.NewReader(buf))
	if err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	cmp := jsonschema.NewCompiler()
	if err := cmp.AddResource(url, obj); err != nil {
		return nil, fmt.Errorf("add resource: %w", err)
	}
	sch, err := cmp.Compile(url)
	if err != nil {
		return nil, fmt.Errorf("compile: %w", err)
	}
	return sch, nil
}

func logJSON(t *testing.T, first bool, buf []byte) {
	if first == *LogJSON {
		t.Log("output:\n" + string(indentJSON(buf, "", "  ")))
	}
}

func indentJSON(src []byte, prefix, indent string) []byte {
	var buf bytes.Buffer
	if err := json.Indent(&buf, src, prefix, indent); err != nil {
		return src
	}
	return buf.Bytes()
}
