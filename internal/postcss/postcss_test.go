package postcss

import (
	"strings"
	"testing"
)

func TestPostCSS(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		if res, err := Transform(``, "defaults"); err != nil {
			t.Errorf("unexpected error: %v", err)
		} else if res != "" {
			t.Errorf("incorrect result: %q", res)
		}
	})
	t.Run("Invalid", func(t *testing.T) {
		if _, err := Transform(`{{{`, "defaults"); err == nil {
			t.Errorf("expected error")
		} else if !strings.HasPrefix(err.Error(), "CssSyntaxError:") {
			t.Errorf("incorrect error: %v", err)
		}
	})
	t.Run("Simple", func(t *testing.T) {
		if res, err := Transform(`html { body { color: rgb(0 0 100% / 90%); @media print { color: #fff } } }`, "chrome 50"); err != nil {
			t.Errorf("unexpected error: %v", err)
		} else if res != "html body{color:rgba(0,0,255,0.9)}@media print{html body{color:#fff}}" {
			t.Errorf("incorrect result: %q", res) // note: this may need to be updated if postcss is upgraded
		}
	})
}
