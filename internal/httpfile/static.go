package httpfile

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
)

// Static is a helper to create a handler for static content. The mimetype,
// modified, and etag parameters are optional. If etag is not specified, an etag
// derived from a cryptographic hash of b (strong for the uncompressed file,
// weak for the compressed). If compression fails, it panics.
//
// The byte slice passed in is not retained.
//
// The format of the etag and the exact list of codings is an implementation
// detail and is subject to change.
func Static(b []byte, mimetype string, modified time.Time, etag ETag) http.Handler {
	if etag == "" {
		s := sha256.Sum256(b)
		etag, _ = MakeETag(base64.RawStdEncoding.EncodeToString(s[:])[:15], false)
	}
	c := []string{CodingIdentity, CodingZstd, CodingGzip}
	f := make([]File, len(c))
	buf := bytes.NewBuffer(make([]byte, len(b)))
	for i, coding := range c {
		if err := encode(buf, bytes.NewReader(b), coding); err != nil {
			panic(fmt.Errorf("compress %s: %w", coding, err))
		}
		e := slices.Clone(buf.Bytes())
		buf.Reset()
		tag, weak := etag.Split()
		if coding != CodingIdentity {
			tag += "-" + coding
			weak = true
		}
		f[i].ETag, _ = MakeETag(tag, weak)
		f[i].Coding = coding
		f[i].Type = mimetype
		f[i].LastModified = modified
		f[i].Open = func() (io.ReadSeeker, error) { return bytes.NewReader(e), nil }
	}
	return Handler(true, f...)
}

func encode(w io.Writer, r io.Reader, coding string) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("%s: %w", cmp.Or(coding, "none"), err)
		}
	}()
	switch coding {
	case CodingIdentity:
		if _, err := io.Copy(w, r); err != nil {
			return err
		}
		return nil

	case CodingZstd:
		zw, err := zstd.NewWriter(w)
		if err != nil {
			return fmt.Errorf("%s: %w", coding, err)
		}
		if _, err := io.Copy(zw, r); err != nil {
			return err
		}
		if err := zw.Close(); err != nil {
			return err
		}
		return nil

	case CodingGzip:
		zw := gzip.NewWriter(w)
		if _, err := io.Copy(zw, r); err != nil {
			return err
		}
		if err := zw.Close(); err != nil {
			return err
		}
		return nil

	case CodingDeflate:
		zw, err := flate.NewWriter(w, flate.DefaultCompression)
		if err != nil {
			return fmt.Errorf("%s: %w", coding, err)
		}
		if _, err := io.Copy(zw, r); err != nil {
			return err
		}
		if err := zw.Close(); err != nil {
			return err
		}
		return nil

	default:
		return fmt.Errorf("%s: %w", coding, errors.ErrUnsupported)
	}
}
