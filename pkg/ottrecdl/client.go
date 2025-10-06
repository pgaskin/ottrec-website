// Package ottrecdl fetches recreation schedule data from the data v1 api.
package ottrecdl

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type Client struct {
	Client    *http.Client
	Base      string
	UserAgent string
}

type DataVersion struct {
	ID       string    `json:"id"`
	Updated  time.Time `json:"updated"`
	Revision int       `json:"revision"`
}

// List lists all data versions.
func (c *Client) List(ctx context.Context, revisions bool, after string) func(*error) iter.Seq[DataVersion] {
	return errSeq(func(yield func(DataVersion) bool) error {
		var a []DataVersion
		for {
			if err := func() error {
				resp, err := c.fetch(ctx, "/v1/?revisions="+strconv.FormatBool(revisions)+"&after="+url.QueryEscape(after))
				if err != nil {
					return err
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					return statusCodeError(resp)
				}

				a = a[:0]
				if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
					return err
				}
				return nil
			}(); err != nil {
				return err
			}
			if len(a) == 0 {
				return nil
			}
			for _, v := range a {
				if !yield(v) {
					return nil
				}
			}
			after = a[len(a)-1].ID
		}
	})
}

// Latest gets the latest data file.
func (c *Client) Latest(ctx context.Context, format string) ([]byte, error) {
	return c.Get(ctx, "latest", format)
}

// On gets the latest data file on the specified date.
func (c *Client) On(ctx context.Context, year int, month time.Month, day int, format string) ([]byte, error) {
	if day == 0 {
		return c.Get(ctx, fmt.Sprintf("%04d-%02d", year, month), format)
	}
	return c.Get(ctx, fmt.Sprintf("%04d-%02d-%02d", year, month, day), format)
}

// Get gets a data file.
func (c *Client) Get(ctx context.Context, spec, format string) ([]byte, error) {
	resp, err := c.fetch(ctx, "/v1/"+url.PathEscape(spec)+"/"+url.PathEscape(format))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := statusCodeError(resp)
		if resp.StatusCode == http.StatusNotFound {
			err = fmt.Errorf("%w: %v", fs.ErrNotExist, err)
		}
		return nil, err
	}

	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (c *Client) fetch(ctx context.Context, path string) (*http.Response, error) {
	u := strings.TrimRight(c.Base, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("fetch %q: %w", u, err)
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}
	resp, err := cmp.Or(c.Client, http.DefaultClient).Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch %q: %w", u, err)
	}
	return resp, nil
}

func statusCodeError(resp *http.Response) error {
	if buf, _ := io.ReadAll(io.LimitReader(resp.Body, 1024)); len(buf) != 0 && utf8.Valid(buf) {
		return fmt.Errorf("response status %d (body: %q)", resp.StatusCode, buf)
	}
	return fmt.Errorf("response status %d", resp.StatusCode)
}

// errSeq creates an [iter.Seq] which can return errors.
func errSeq[T any](fn func(yield func(T) bool) error) func(*error) iter.Seq[T] {
	return func(err *error) iter.Seq[T] {
		return func(yield func(T) bool) {
			*err = fn(yield)
		}
	}
}
