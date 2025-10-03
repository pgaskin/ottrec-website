//go:build ignore

package main

import (
	"cmp"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"iter"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pgaskin/ottrec-website/pkg/ottrecidx"
)

var (
	Base       = flag.String("base", "http://data.ottrec.localhost:8082/", "base url for data api")
	MemStats   = flag.Bool("memstats", false, "print memory statistics")
	MemProfile = flag.String("memprofile", "", "write memory profile")
	CPUProfile = flag.String("cpuprofile", "", "write cpu profile")
	Check      = flag.Bool("check", false, "enable indexer sanity checking")
	Quiet      = flag.Bool("quiet", false, "do not print progress info")
	Limit      = flag.Int("limit", 0, "maximum number of schedules to import")
)

func main() {
	flag.Parse()

	if flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}

	if *Check {
		ottrecidx.EnableIndexerSanityCheck()
		progress("enabled indexer sanity checking")
	}

	if *CPUProfile != "" {
		f, err := os.Create(*CPUProfile)
		if err != nil {
			panic(err)
		}
		defer f.Close()

		runtime.SetCPUProfileRate(400) // Hz

		if err := pprof.StartCPUProfile(f); err != nil {
			panic(err)
		}
	}

	dxr := func() *ottrecidx.Indexer {
		var (
			dxr   ottrecidx.Indexer
			tb    int
			err   error
			limit = cmp.Or(*Limit, -1)
		)
		for off, buf := range pbs(*Base)(&err) {
			if limit >= 0 {
				if limit--; limit < 0 {
					break
				}
			}
			x, err := dxr.Load(buf)
			if err != nil {
				panic(err)
			}
			tb += len(buf)
			progress(off, x)
		}
		if err != nil {
			panic(err)
		}
		progress("imported", tb, "bytes")
		return &dxr
	}()

	if *CPUProfile != "" {
		pprof.StopCPUProfile()
		progress("wrote cpu profile", *CPUProfile)
	}

	if *MemStats {
		debug.FreeOSMemory()

		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)

		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("  ", "  ")
		enc.Encode(ms)
	}

	if *MemProfile != "" {
		debug.FreeOSMemory()

		if f, err := os.Create(*MemProfile); err != nil {
			panic(err)
		} else if err := pprof.Lookup("allocs").WriteTo(f, 0); err != nil {
			f.Close()
			panic(err)
		} else if err := f.Close(); err != nil {
			panic(err)
		}
		progress("wrote memory profile", *CPUProfile)
	}

	progress(ottrecidx.DebugIndexer(dxr, false))

	runtime.KeepAlive(dxr)
	runtime.KeepAlive(ottrecidx.DebugIndexer(dxr, true))
}

// progress prints a progress line.
func progress(a ...any) {
	if *Quiet {
		return
	}
	fmt.Println(a...)
}

// pbs efficiently iterates over all schedule protobuf from the ottrec-data api.
func pbs(base string) func(*error) iter.Seq2[int, []byte] {
	return func(err *error) iter.Seq2[int, []byte] {
		return func(yield func(int, []byte) bool) {
			*err = func() error {
				cl := &http.Client{
					Transport: &http.Transport{
						Proxy:              http.ProxyFromEnvironment,
						DisableCompression: true,
						MaxIdleConns:       1,
					},
				}
				var (
					buf = make([]byte, 0, 4*1024*1024)
					url = make([]byte, 0, len(base)+100)
				)
				for off := 0; ; off-- {
					url = url[:0]
					url = append(url, strings.TrimSuffix(base, "/")...)
					url = append(url, "/v1/latest"...)
					if off != 0 {
						url = strconv.AppendInt(url, int64(off), 10)
					}
					url = append(url, "/pb"...)

					resp, err := cl.Get(string(url))
					if err != nil {
						return fmt.Errorf("get %s: %w", url, err)
					}

					if resp.ContentLength < 0 {
						return fmt.Errorf("get %s: no content length", url)
					}
					if resp.ContentLength > int64(cap(buf)) {
						return fmt.Errorf("get %s: too long (%d > %d)", url, resp.ContentLength, cap(buf))
					}
					buf = buf[:resp.ContentLength]

					_, err = io.ReadFull(resp.Body, buf)
					resp.Body.Close()
					if err != nil {
						return fmt.Errorf("get %s: %w", url, err)
					}

					if resp.StatusCode != http.StatusOK {
						if off != 0 && resp.StatusCode == http.StatusNotFound {
							return nil
						}
						if utf8.Valid(buf) {
							return fmt.Errorf("get %s: response status %d (body: %q)", url, resp.StatusCode, buf)
						}
						return fmt.Errorf("get %s: response status %d", url, resp.StatusCode)
					}

					if !yield(off, buf) {
						return nil
					}
				}
			}()
		}
	}
}
