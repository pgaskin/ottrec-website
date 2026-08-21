// Package gitsh wraps some of the git shell commands.
package gitsh

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Git is the git binary to use.
var Git = "git"

// GitDir gets the git directory for a repo.
func GitDir(ctx context.Context, repo string) (string, error) {
	cmd := exec.CommandContext(ctx, Git, "rev-parse", "--absolute-git-dir")
	cmd.Dir = repo
	cmd.Stdin = nil

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", TransformError(err, stderr.Bytes())
	}
	return strings.TrimSpace(stdout.String()), nil
}

// Exec runs a git command, streaming the combined stdout/stderr to fn if not nil.
func Exec(ctx context.Context, repo string, output func(iter.Seq[string]), arg ...string) error {
	cmd := exec.CommandContext(ctx, Git, arg...)
	cmd.Dir = repo
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	if output != nil {
		r, w, err := os.Pipe()
		if err != nil {
			return err
		}
		defer r.Close()

		cmd.Stdout = w
		cmd.Stderr = w

		var (
			done   = make(chan struct{})
			outErr error
		)
		go func() {
			defer close(done)
			output(readLinesSeq(r)(&outErr))
			io.Copy(io.Discard, r) // don't block the command if output stopped early
		}()

		err = cmd.Run()
		w.Close() // so the reader sees EOF
		<-done    // and has drained everything before r is closed
		if err != nil {
			return err
		}
		return outErr
	}
	return cmd.Run()
}

// RevCommit resolves a rev into a commit hash.
func RevCommit(ctx context.Context, repo, rev string) (string, error) {
	cmd := exec.CommandContext(ctx, Git, "rev-parse", "--verify", "--end-of-options", rev+"^{commit}")
	cmd.Dir = repo
	cmd.Stdin = nil

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", TransformError(err, stderr.Bytes())
	}

	hash := strings.TrimSpace(stdout.String())
	if !IsLikelyGitHash(hash) {
		return "", fmt.Errorf("invalid commit hash %q", hash)
	}
	return hash, nil
}

// CatFile gets the contents of a file. As a special case, if the file
// doesn't exist, it returns an error matching [fs.ErrNotExist].
func CatFile(ctx context.Context, repo, treeish, path string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, Git, "cat-file", "blob", "--end-of-options", treeish+":"+path)
	cmd.Dir = repo
	cmd.Stdin = nil

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		err = TransformError(err, stderr.Bytes())
		if msg := err.Error(); strings.Contains(msg, " does not exist in ") || strings.Contains(msg, " exists on disk, but not in ") {
			err = fmt.Errorf("%w: %v", fs.ErrNotExist, err)
		}
		return nil, TransformError(err, stderr.Bytes())
	}
	return stdout.Bytes(), nil
}

// CatFileBatch is like [CatFile], but reads multiple files with a single
// process, returning nil for nonexistent files.
func CatFileBatch(ctx context.Context, repo, treeish string, paths []string) ([][]byte, error) {
	var stdin bytes.Buffer
	for _, path := range paths {
		stdin.WriteString(treeish)
		stdin.WriteString(":")
		stdin.WriteString(path)
		stdin.WriteString("\n")
	}

	cmd := exec.CommandContext(ctx, Git, "cat-file", "--batch")
	cmd.Dir = repo
	cmd.Stdin = &stdin

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// `<oid> <type> <size>\n<contents>\n` or `<input> missing\n`
	bufs := make([][]byte, 0, len(paths))
	r := bufio.NewReader(stdout)
	for range paths {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimSuffix(line, "\n")

		_, rest, ok := strings.Cut(line, " ") // oid
		if !ok {
			bufs = append(bufs, nil)
			continue
		}
		_, sizeStr, ok := strings.Cut(rest, " ") // type
		if !ok {
			bufs = append(bufs, nil) // "<input> missing"
			continue
		}
		size, err := strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			cmd.Process.Kill()
			cmd.Wait()
			return nil, fmt.Errorf("parse cat-file header %q: %w", line, err)
		}
		buf := make([]byte, size+1) // includes the trailing newline
		if _, err := io.ReadFull(r, buf); err != nil {
			cmd.Process.Kill()
			cmd.Wait()
			return nil, fmt.Errorf("read cat-file contents: %w", err)
		}
		bufs = append(bufs, buf[:size])
	}

	if err := cmd.Wait(); err != nil {
		return nil, TransformError(err, stderr.Bytes())
	}
	if len(bufs) != len(paths) {
		return nil, fmt.Errorf("read %d of %d objects", len(bufs), len(paths))
	}
	return bufs, nil
}

// CommitsAscFirstParent iterates over commits hashes and dates in the specified
// repository from oldest to newest up to rev, following the first parent of
// each one.
func CommitsAscFirstParent(ctx context.Context, repo, rev string) func(*error) iter.Seq2[string, time.Time] {
	return errSeq2(func(yield func(string, time.Time) bool) error {
		cmd := exec.CommandContext(ctx, Git, "rev-list", "--date-order", "--timestamp", "--first-parent", "--reverse", "--end-of-options", rev)
		cmd.Dir = repo
		cmd.Stdin = nil

		var stderr bytes.Buffer
		cmd.Stderr = &stderr

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return err
		}
		if err := cmd.Start(); err != nil {
			return err
		}

		var stopped bool
		for line := range readLinesSeq(stdout)(&err) {
			if len(line) == 0 {
				continue
			}
			var (
				date time.Time
				hash string
			)
			for i, f := range iterEnumerate(strings.SplitSeq(line, " ")) {
				switch i {
				case 0:
					v, err := strconv.ParseInt(f, 10, 64)
					if err != nil {
						return fmt.Errorf("parse line %q: invalid timestamp %q", line, f)
					}
					date = time.Unix(v, 0)
				case 1:
					if !IsLikelyGitHash(f) {
						return fmt.Errorf("parse line %q: invalid commit hash %q", line, f)
					}
					hash = f
				default:
					return fmt.Errorf("parse line %q: too many fields", line)
				}
			}
			if !yield(hash, date) {
				stopped = true
				cmd.Process.Kill()
				break
			}
		}
		_ = err

		if err := cmd.Wait(); err != nil && !stopped {
			return TransformError(err, stderr.Bytes())
		}
		return nil
	})
}

// TransformError transforms an error from [exec.Cmd.Wait].
func TransformError(err error, stderr []byte) error {
	var xx *exec.ExitError
	if errors.As(err, &xx) {
		if stderr == nil {
			stderr = xx.Stderr
		}
		if msg, _ := iterFirst(bytes.Lines(bytes.TrimSpace(stderr))); len(msg) != 0 {
			return fmt.Errorf("git (%s): %s", xx.ProcessState, msg)
		}
	}
	return err
}

// IsLikelyGitHash returns true if hash is probably a full git commit hash.
func IsLikelyGitHash(hash string) bool {
	return len(hash) >= 40 && strings.Trim(hash, "0123456789abcdef") == ""
}

func readLinesSeq(r io.Reader) func(*error) iter.Seq[string] {
	return errSeq(func(yield func(string) bool) error {
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			if !yield(sc.Text()) {
				return nil
			}
		}
		return sc.Err()
	})
}

// iterFirst returns the iterFirst value from seq.
func iterFirst[T any](seq iter.Seq[T]) (T, bool) {
	var z T
	for v := range seq {
		return v, true
	}
	return z, false
}

// iterEnumerate adds indexes to seq.
func iterEnumerate[T any](seq iter.Seq[T]) iter.Seq2[int, T] {
	return func(yield func(int, T) bool) {
		var i int
		for x := range seq {
			if !yield(i, x) {
				return
			}
			i++
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
