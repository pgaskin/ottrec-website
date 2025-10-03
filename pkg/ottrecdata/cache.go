// Package ottrecdata loads, indexes, and caches scraped City of Ottawa
// recreation schedules.
package ottrecdata

import (
	"bytes"
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/gzip"
	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/driver"
	"github.com/pgaskin/ottrec-website/internal/gitsh"
	"github.com/pgaskin/ottrec/schema"
	"google.golang.org/protobuf/proto"
)

// Cache indexes and stores schedule data.
type Cache struct {
	db *sql.DB
}

// SchemaVersion should be incremented if we change the schema, how import
// works, or what gets imported.
const SchemaVersion, schemaOptions, schemaDDL = 4, `
PRAGMA journal_mode=wal; -- so it's faster and writes/reads don't block each other
PRAGMA busy_timeout=10000; -- avoid spurious database is locked errors
PRAGMA cache_size = 4096; -- so we can fit more blobs in memory
PRAGMA auto_vacuum = OFF; -- we don't delete stuff, so not vacuuming doesn't lose us much, and it's more predictable
PRAGMA automatic_index = OFF; -- so it's more predictable
PRAGMA foreign_keys = ON;
`, `
PRAGMA encoding = 'UTF-8';

CREATE TABLE commits ( -- commit metadata
	hash TEXT NOT NULL, -- git commit hash
	date REAL NOT NULL, -- unix fractional timestamp
	PRIMARY KEY(hash)
) STRICT, WITHOUT ROWID;

CREATE TABLE data ( -- data metadata
	id TEXT NOT NULL, -- opaque identifier, usually base32-encoded sha1
	hash TEXT NOT NULL, -- git commit hash
	updated REAL NOT NULL, -- unix fractional timestamp
	revision INTEGER NOT NULL, -- positive integer
	PRIMARY KEY(id),
	FOREIGN KEY(hash) REFERENCES commits(hash),
	UNIQUE(updated DESC, revision DESC),
	UNIQUE(hash)
) STRICT, WITHOUT ROWID;

CREATE TABLE files ( -- data file
	id TEXT NOT NULL,
	format TEXT NOT NULL,
	hash TEXT, -- base32-encoded sha1
	PRIMARY KEY(id, format),
	FOREIGN KEY(id) REFERENCES data(id),
	FOREIGN KEY(hash) REFERENCES blobs(hash),
	CHECK(format IN ('pb','textpb','proto','json'))
) STRICT, WITHOUT ROWID;

CREATE TABLE blobs ( -- data file contents
	hash TEXT NOT NULL, -- base32-encoded sha1 of unencoded data
	size INTEGER NOT NULL, -- uncompressed data length
	data BLOB NOT NULL, -- gzipped data
	PRIMARY KEY(hash)
) STRICT;
`

var TZ *time.Location

func init() {
	if tz, err := time.LoadLocation("America/Toronto"); err != nil {
		panic(err)
	} else {
		TZ = tz
	}
}

var ErrUnsupportedSchema = errors.New("unsupported schema version")

// OpenCache opens a cache. If the schema version does not match, an error
// matching [ErrUnsupportedSchema] is returned. If reset is true, the database
// is cleared.
func OpenCache(name string, reset bool) (*Cache, error) {
	db, err := driver.Open("file:"+escapeSqlitePath(name), sqliteRegisterGzip)
	if err != nil {
		return nil, err
	}
	idx := &Cache{db: db}
	if err := idx.initialize(reset); err != nil {
		idx.db.Close()
		return nil, err
	}
	return idx, nil
}

// Close closes the cache.
func (db *Cache) Close() error {
	return db.db.Close()
}

// initialize sets up the database.
func (db *Cache) initialize(reset bool) error {
	var current int
	if !reset {
		if err := db.db.QueryRow(`PRAGMA user_version`).Scan(&current); err != nil {
			return fmt.Errorf("get version: %w", err)
		}
		if current == SchemaVersion {
			return nil
		}
		if current != 0 {
			return fmt.Errorf("%w: unsupported version %d (wanted %d)", ErrUnsupportedSchema, current, SchemaVersion)
		}
	}
	if current == 0 {
		if err := sqliteResetDatabase(db.db); err != nil {
			return fmt.Errorf("reset database: %w", err)
		}
		if _, err := db.db.Exec(schemaDDL); err != nil {
			return fmt.Errorf("create tables: %w", err)
		}
		if _, err := db.db.Exec(`PRAGMA user_version = ` + strconv.Itoa(SchemaVersion)); err != nil {
			return fmt.Errorf("update version: %w", err)
		}
	}
	if _, err := db.db.Exec(schemaOptions); err != nil {
		return fmt.Errorf("set options: %w", err)
	}
	return nil
}

type DataVersion struct {
	ID        string
	Commit    string
	Committed time.Time
	Updated   time.Time
	Revision  int
}

// DataVersions iterates over available versions, from most recently updated to
// the lest recently updated.
func (db *Cache) DataVersions(ctx context.Context) func(*error) iter.Seq[DataVersion] {
	return errSeq(func(yield func(DataVersion) bool) error {
		rows, err := db.db.QueryContext(ctx, `SELECT data.id, commits.hash, commits.date, data.updated, data.revision FROM data LEFT JOIN commits ON commits.hash = data.hash ORDER BY data.updated DESC, data.revision DESC`)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var ver DataVersion
			if err := rows.Scan(&ver.ID, &ver.Commit, sqlite3.TimeFormatUnixFrac.Scanner(&ver.Committed), sqlite3.TimeFormatUnixFrac.Scanner(&ver.Updated), &ver.Revision); err != nil {
				return err
			}
			if !yield(ver) {
				return nil
			}
		}
		return nil
	})
}

func IsID(s string) bool {
	return len(s) == base32.StdEncoding.EncodedLen(sha1.Size)
}

// ResolveVersion resolves a version.
func (db *Cache) ResolveVersion(ctx context.Context, spec string) (string, time.Time, bool, error) {
	getOne := func(where string, a ...any) (string, time.Time, bool, error) {
		var (
			id      string
			updated time.Time
		)
		if err := db.db.QueryRowContext(ctx, `SELECT id, updated FROM data `+where, a...).Scan(&id, sqlite3.TimeFormatUnixFrac.Scanner(&updated)); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return "", time.Time{}, true, nil
			}
			return "", time.Time{}, true, err
		}
		return id, updated, true, nil
	}
	if IsID(spec) {
		return getOne(`WHERE id = ?`, spec)
	}
	if offset, ok := strings.CutPrefix(spec, "latest"); ok {
		if offset == "" {
			return getOne(`ORDER BY updated DESC, revision DESC LIMIT 1`)
		}
		if n, err := strconv.ParseInt(offset, 10, 64); err == nil && n < 0 {
			return getOne(`ORDER BY updated DESC, revision DESC LIMIT 1 OFFSET ` + strconv.FormatInt(-n, 10))
		}
	}
	var upper time.Time
	if fmt := "2006-01"; len(spec) == len(fmt) {
		if t, err := time.ParseInLocation(fmt, spec, TZ); err == nil {
			y, m, _ := t.Date()
			upper = time.Date(y, m+1, 0, 0, 0, 0, 0, TZ)
		}
	}
	if fmt := "2006-01-02"; len(spec) == len(fmt) {
		if t, err := time.ParseInLocation(fmt, spec, TZ); err == nil {
			y, m, d := t.Date()
			upper = time.Date(y, m, d+1, 0, 0, 0, 0, TZ)
		}
	}
	if fmt := "2006-01-02T03:04"; len(spec) == len(fmt) {
		if t, err := time.ParseInLocation(fmt, spec, TZ); err == nil {
			y, m, d := t.Date()
			hh, mm := t.Hour(), t.Minute()
			upper = time.Date(y, m, d, hh, mm+1, 0, 0, TZ)
		}
	}
	if fmt := "2006-01-02T03:04:05"; len(spec) == len(fmt) {
		if t, err := time.ParseInLocation(fmt, spec, TZ); err == nil {
			y, m, d := t.Date()
			hh, mm, ss := t.Hour(), t.Minute(), t.Second()
			upper = time.Date(y, m, d, hh, mm, ss+1, 0, TZ)
		}
	}
	if !upper.IsZero() {
		return getOne(`WHERE updated < ? ORDER BY updated DESC, revision DESC LIMIT 1`, sqlite3.TimeFormatUnixFrac.Encode(upper))
	}
	return "", time.Time{}, false, nil
}

// DataFormats iterates over formats (hash, format) available for the specified
// version ID.
func (db *Cache) DataFormats(ctx context.Context, id string) func(*error) iter.Seq2[string, string] {
	return errSeq2(func(yield func(string, string) bool) error {
		rows, err := db.db.QueryContext(ctx, `SELECT hash, format FROM files WHERE id = ?`, id)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var hash, format string
			if err := rows.Scan(&hash, &format); err != nil {
				return err
			}
			if !yield(hash, format) {
				return nil
			}
		}
		return nil
	})
}

// ReadBlob reads a blob by the hash. If it doesn't exist, (false, nil) is
// returned.
func (db *Cache) ReadBlob(ctx context.Context, hash string, gzipped bool, fn func(io.Reader, int64) error) (bool, error) {
	var rowid, size int64
	if err := db.db.QueryRowContext(ctx, `SELECT rowid, size FROM blobs WHERE hash = ? LIMIT 1`, hash).Scan(&rowid, &size); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}

	conn, err := db.db.Conn(ctx)
	if err != nil {
		return true, err
	}
	defer conn.Close()

	return true, conn.Raw(func(driverConn any) error {
		blob, err := driverConn.(driver.Conn).Raw().OpenBlob("main", "blobs", "data", rowid, false)
		if err != nil {
			return err
		}
		defer blob.Close()

		var (
			r io.Reader = blob
			n int64     = blob.Size()
		)
		if !gzipped {
			zr, err := gzip.NewReader(blob)
			if err != nil {
				return err
			}
			r, n = zr, size
		}
		return fn(r, n)
	})
}

// Import imports data from a git repository, skipping any commit hashes already
// imported.
func (db *Cache) Import(ctx context.Context, logger *slog.Logger, repo, rev string) error {
	slog := logger

	slog.Info("cache: importing data", "repo", repo, "rev", rev)

	// resolve the rev to a commit hash
	head, err := gitsh.RevCommit(ctx, repo, rev)
	if err != nil {
		slog.Error("cache: failed to resolve git commit", "error", err)
		return err
	}
	slog.Info("cache: resolved rev", "rev", rev, "commit", head)

	// short-circuit optimization if we already have all commits
	var upToDate bool
	if err := db.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM commits WHERE hash = ?)`, head).Scan(&upToDate); err != nil {
		return fmt.Errorf("check if up-to-date: %w", err)
	}
	if upToDate {
		slog.Info("cache: nothing to do, already up-to-date")
		return nil
	}

	// add commits from oldest to newest by commit date (note: we do need to
	// start walking from the beginning since a backdated commit could have been
	// added)
	for commitHash, commitDate := range gitsh.CommitsAscFirstParent(ctx, repo, head)(&err) {
		// each commit is self-contained, we go from oldest to newest, and we
		// assume commits are all on the same timeline, so it's safe for each
		// addition to be its own transaction (it won't mess up the revision
		// numbers)
		if skip, err := db.importCommit(ctx, slog.With("commit", commitHash), repo, commitHash, commitDate); err != nil {
			slog.Error("cache: failed to import commit", "error", err)
			return fmt.Errorf("import commit %q (%s): %w", commitHash, commitDate, err)
		} else if skip != nil {
			slog.Warn("cache: skipping commit", "error", skip)
		}
	}
	if err != nil {
		slog.Error("cache: failed to list commits", "error", err)
		return err
	}

	slog.Info("cache: import finished")
	return nil
}

// importCommit imports a commit. Since it automatically calculates the
// revision, it must be called from oldest to newest.
func (db *Cache) importCommit(ctx context.Context, logger *slog.Logger, repo string, commitHash string, commitDate time.Time) (skip, err error) {
	slog := logger

	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO commits (hash, date) VALUES (:hash, :date)`,
		sql.Named("hash", commitHash),
		sql.Named("date", sqlite3.TimeFormatUnixFrac.Encode(commitDate)),
	); err != nil {
		return nil, fmt.Errorf("insert commit: %w", err)
	} else if rows, err := res.RowsAffected(); err != nil {
		return nil, fmt.Errorf("insert commit: %w", err)
	} else if rows == 0 {
		return nil, nil // already imported or skipped before
	}
	slog.Info("cache: import", "date", commitDate)

	formats := []string{"pb", "textpb", "proto", "json"} // increment the schema version if we add more required formats
	required := len(formats)
	//formats = append(formats) // more optional formats if needed in the future
	contents := make([][]byte, len(formats))

	for i, format := range formats {
		var name string
		switch format {
		default:
			name = "data." + string(format)
		}
		buf, err := gitsh.CatFile(ctx, repo, commitHash, name)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) && i >= required {
				slog.Warn("cache: missing optional format", "format", format)
				continue
			}
			return err, nil
		}
		contents[i] = buf
	}

	pb := contents[0]
	id := base32sha1(pb)

	var dup bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM data WHERE id = ?)`, id).Scan(&dup); err != nil {
		return nil, fmt.Errorf("check if duplicate: %w", err)
	}
	if dup {
		old := id
		id = base32sha1(contents...) // just sum all of it so it's deterministic
		id = "9" + id[1:]            // 9 isn't in the base32 charset, and this lets us distinguish it later for debugging
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM data WHERE id = ?)`, id).Scan(&dup); err != nil {
			return nil, fmt.Errorf("check if duplicate: %w", err)
		}
		if dup {
			return errors.New("is duplicate"), nil // it's actually a duplicate, ignore it
		}
		slog.Info("cache: duplicate data.pb but other files changed, derived new ID from all files", "old_id", old, "new_id", id)
	}

	var data schema.Data
	if err := proto.Unmarshal(pb, &data); err != nil {
		return nil, fmt.Errorf("unmarshal data.pb: %w", err)
	}

	var (
		updated time.Time
		nodate  int
		yesdate int
	)
	for _, fac := range data.GetFacilities() {
		if src := fac.GetSource(); src != nil {
			if x := src.GetXDate(); x != nil {
				yesdate++
				if t := x.AsTime(); t.After(updated) {
					updated = t
				}
				continue
			}
		}
		nodate++
	}
	if updated.IsZero() {
		return errors.New("no facilities in data.pb with source date set"), nil
	}
	if nodate != 0 {
		slog.Warn("cache: some facilities had no source._date set", "without_date", nodate, "with_date", yesdate)
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO data (id, hash, updated, revision) VALUES (:id, :hash, :updated,
					1+coalesce((SELECT revision FROM data WHERE updated = :updated ORDER BY revision DESC LIMIT 1), 0))`,
		sql.Named("id", id),
		sql.Named("hash", commitHash),
		sql.Named("updated", sqlite3.TimeFormatUnixFrac.Encode(updated)),
	); err != nil {
		return nil, fmt.Errorf("insert data: %w", err)
	}
	for format, buf := range iterTranspose(formats, contents) {
		if buf != nil {
			if err := db.insertFile(ctx, tx, id, format, buf); err != nil {
				return nil, fmt.Errorf("insert file: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}
	return nil, nil
}

func (db *Cache) insertFile(ctx context.Context, tx *sql.Tx, id string, format string, buf []byte) error {
	hash := base32sha1(buf)
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO blobs (hash, size, data) VALUES (:hash, :size, gzip(:data, 9))`,
		sql.Named("hash", hash),
		sql.Named("size", len(buf)),
		sql.Named("data", buf),
	); err != nil {
		return fmt.Errorf("insert blob: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO files (id, format, hash) VALUES (:id, :format, :hash)`,
		sql.Named("id", id),
		sql.Named("format", format),
		sql.Named("hash", hash),
	); err != nil {
		return fmt.Errorf("insert file: %w", err)
	}
	return nil
}

var sqliteURIEscaper = strings.NewReplacer("?", "%3f", "#", "%23")

func escapeSqlitePath(path string) string {
	return sqliteURIEscaper.Replace(path)
}

// sqliteResetDatabase resets a database.
func sqliteResetDatabase(db *sql.DB) error {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.Raw(func(driverConn any) error {
		conn, ok := driverConn.(driver.Conn)
		if !ok {
			return errors.New("not a sqlite3 database")
		}
		if _, err := conn.Raw().Config(sqlite3.DBCONFIG_RESET_DATABASE, true); err != nil {
			return err
		}
		if err := conn.Raw().Exec("VACUUM"); err != nil {
			return err
		}
		if _, err := conn.Raw().Config(sqlite3.DBCONFIG_RESET_DATABASE, false); err != nil {
			return err
		}
		return nil
	})
}

// sqliteRegisterGzip registers a gzip function in c.
//
//	gzip(blob) blob
//	gzip(blob, level) blob
//	gzip(blob, -window) blob
func sqliteRegisterGzip(c *sqlite3.Conn) error {
	fn := func(ctx sqlite3.Context, arg ...sqlite3.Value) {
		var (
			buf = arg[0].RawBlob()
			out = bytes.NewBuffer(make([]byte, 0, len(buf)))
			gzw *gzip.Writer
			err error
		)
		if len(arg) < 2 {
			gzw, err = gzip.NewWriter(out), nil
		} else if n := arg[1].Int(); n < -gzip.MinCustomWindowSize {
			gzw, err = gzip.NewWriterWindow(out, -n)
		} else {
			gzw, err = gzip.NewWriterLevel(out, n)
		}
		if err != nil {
			ctx.ResultError(err)
			return
		}
		if _, err := gzw.Write(buf); err != nil {
			ctx.ResultError(err)
			return
		}
		if err := gzw.Close(); err != nil {
			ctx.ResultError(err)
			return
		}
		ctx.ResultBlob(out.Bytes())
	}
	return errors.Join(
		c.CreateFunction("gzip", 1, 0, fn),
		c.CreateFunction("gzip", 2, 0, fn))
}

// base32sha1 calculates the base32-encoded sha1 of b.
func base32sha1(b ...[]byte) string {
	s := sha1.New()
	for _, b := range b {
		s.Write(b)
	}
	return base32.StdEncoding.EncodeToString(s.Sum(nil))
}

// iterTranspose zips the two parameters, which must be the same length.
func iterTranspose[T, U any](k []T, v []U) iter.Seq2[T, U] {
	_, _ = k[len(v)-1], v[len(k)-1] // bounds check
	return func(yield func(T, U) bool) {
		for i, k := range k {
			if !yield(k, v[i]) {
				return
			}
		}
	}
}

// errSeq creates an [iter.Seq] which can return errors.
func errSeq[T any](fn func(yield func(T) bool) error) func(*error) iter.Seq[T] {
	return func(err *error) iter.Seq[T] {
		return func(yield func(T) bool) {
			*err = fn(yield)
		}
	}
}

// errSeq2 creates an [iter.Seq2] which can return errors.
func errSeq2[T, U any](fn func(yield func(T, U) bool) error) func(*error) iter.Seq2[T, U] {
	return func(err *error) iter.Seq2[T, U] {
		return func(yield func(T, U) bool) {
			*err = fn(yield)
		}
	}
}
