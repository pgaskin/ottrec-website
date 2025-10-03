package ottrecidx

import (
	"bytes"
	"iter"
	"slices"
	"unsafe"

	kbitmap "github.com/kelindar/bitmap"
)

// this file contains data structures used for optimizing the index

// stringInterner interns strings backed by a single buffer (which is optionally
// backed by an arena). It is designed for memory efficiency rather than write
// performance. It never shrinks, grows in chunks, and memory usage is
// unbounded.
type stringInterner struct {
	arena    *arena
	buf      [][]byte // chunks must never be reallocated since we make strings out of it
	cache    map[string][2]uint32
	interned int64
}

// Intern interns a string. Note that this is quadratic complexity in the
// worst case.
func (a *stringInterner) Intern(s string) string {
	a.interned += int64(len(s))
	if len(s) == 0 {
		return ""
	}
	if i, j, ok := a.lookup(s); ok {
		return a.get(i, j, len(s))
	}
	if i, j, ok := a.scan(s); ok {
		s = a.get(i, j, len(s))
		if a.cache != nil {
			a.cache[s] = [2]uint32{uint32(i), uint32(j)}
		}
		return s
	}
	return a.put(s)
}

// InternFast interns a string. It does not scan for the string if there is not
// already an exact match in the cache and the cache is enabled.
func (a *stringInterner) InternFast(s string) string {
	a.interned += int64(len(s))
	if len(s) == 0 {
		return ""
	}
	if a.cache != nil {
		if i, j, ok := a.lookup(s); ok {
			return a.get(i, j, len(s))
		}
	}
	return a.Intern(s)
}

// Cache enables or disables the cache, setting the initial map capacity. If the
// cache is disabled, all queries may have quadratic time complexity.
func (a *stringInterner) Cache(cap int) {
	if cap != 0 {
		a.cache = make(map[string][2]uint32, cap)
	} else {
		a.cache = nil
	}
}

func (a *stringInterner) allocate(n int) (int, int) {
	const defaultChunkSize = 256 * 1024
	for i, b := range a.buf {
		if j := len(b); n <= cap(b)-j {
			a.buf[i] = b[:j+n]
			return i, j
		}
	}
	var b []byte
	if a.arena != nil {
		b = arenaMakeSlice[byte](a.arena, n, max(n, defaultChunkSize))
	} else {
		b = make([]byte, n, max(n, defaultChunkSize))
	}
	i, j := len(a.buf), 0
	a.buf = append(a.buf, b)
	return i, j
}

func (a *stringInterner) get(i, j, n int) string {
	return unsafe.String(&a.buf[i][j], n)
}

func (a *stringInterner) put(s string) string {
	n := len(s)
	i, j := a.allocate(n)
	copy(a.buf[i][j:j+n], s)
	s = a.get(i, j, n)
	if a.cache != nil {
		a.cache[s] = [2]uint32{uint32(i), uint32(j)}
	}
	return s
}

func (a *stringInterner) lookup(s string) (int, int, bool) {
	if v, ok := a.cache[s]; ok {
		i := v[0]
		j := v[1]
		return int(i), int(j), true
	}
	return 0, 0, false
}

func (a *stringInterner) scan(s string) (int, int, bool) {
	for i, b := range a.buf {
		if j := bytes.Index(b, unsafe.Slice(unsafe.StringData(s), len(s))); j != -1 {
			return i, j, true
		}
	}
	return 0, 0, false
}

// interner interns pointers to comparable values. Note that it currently has
// worst-case quadratic time complexity (TODO: deal with this).
type interner[T comparable] struct {
	a []*T
	n int64
}

func (n *interner[T]) Intern(x *T) *T {
	n.n++
	if i := slices.IndexFunc(n.a, func(e *T) bool { return *x == *e }); i != -1 {
		return n.a[i]
	}
	n.a = append(n.a, x)
	return x
}

// bitmap wraps a [kbitmap.Bitmap] to be generic and provides additional
// methods.
type bitmap[T ~uint32] struct {
	kb kbitmap.Bitmap
}

func kbs[T ~uint32](x ...bitmap[T]) []kbitmap.Bitmap {
	m := make([]kbitmap.Bitmap, len(x))
	for i, x := range x {
		m[i] = x.kb
	}
	return m
}

func makeBitmap[T ~uint32](n int) bitmap[T] {
	return bitmap[T]{make(kbitmap.Bitmap, (n>>6)+1)}
}

func nilBitmap[T ~uint32]() bitmap[T] {
	return bitmap[T]{nil}
}

func (dst bitmap[T]) IsNil() bool {
	return dst.kb == nil
}

func (dst *bitmap[T]) kbmut() *kbitmap.Bitmap {
	if dst == nil {
		return (*kbitmap.Bitmap)(nil)
	}
	return (*kbitmap.Bitmap)(&dst.kb)
}

func (dst *bitmap[T]) Set(v T) {
	dst.kbmut().Set(uint32(v))
}

func (dst *bitmap[T]) Remove(v T) {
	dst.kbmut().Remove(uint32(v))
}

func (dst *bitmap[T]) Ones() {
	dst.kbmut().Ones()
}

func (dst *bitmap[T]) Or(other bitmap[T], extra ...bitmap[T]) {
	dst.kbmut().Or(other.kb, kbs(extra...)...)
}

func (dst *bitmap[T]) And(other bitmap[T], extra ...bitmap[T]) {
	dst.kbmut().And(other.kb, kbs(extra...)...)
}

func (dst bitmap[T]) Count() int {
	return dst.kb.Count()
}

func (dst bitmap[T]) Clone(into *bitmap[T]) bitmap[T] {
	return bitmap[T]{dst.kb.Clone(into.kbmut())}
}

func (dst bitmap[T]) Contains(x T) bool {
	return dst.kb.Contains(uint32(x))
}

func (dst bitmap[T]) Min() (T, bool) {
	v, ok := dst.kb.Min()
	return T(v), ok
}

func (dst bitmap[T]) Max() (T, bool) {
	v, ok := dst.kb.Max()
	return T(v), ok
}

func (dst bitmap[T]) MaxZero() (T, bool) {
	v, ok := dst.kb.MaxZero()
	return T(v), ok
}

// Range is an iterator over the bitmap. Based on [kbitmap.Bitmap.Range].
func (dst bitmap[T]) Range() iter.Seq[T] {
	return func(yield func(T) bool) {
		for blkAt := range dst.kb {
			blk := dst.kb[blkAt]
			if blk == 0x0 {
				continue // Skip the empty page
			}

			// Iterate in a 4-bit chunks so we can reduce the number of function calls and skip
			// the bits for which we should not call our range function.
			offset := T(blkAt << 6)
			for ; blk > 0; blk = blk >> 4 {
				switch blk & 0b1111 {
				case 0b0001:
					if !yield(offset + 0) {
						return
					}
				case 0b0010:
					if !yield(offset + 1) {
						return
					}
				case 0b0011:
					if !yield(offset + 0) {
						return
					}
					if !yield(offset + 1) {
						return
					}
				case 0b0100:
					if !yield(offset + 2) {
						return
					}
				case 0b0101:
					if !yield(offset + 0) {
						return
					}
					if !yield(offset + 2) {
						return
					}
				case 0b0110:
					if !yield(offset + 1) {
						return
					}
					if !yield(offset + 2) {
						return
					}
				case 0b0111:
					if !yield(offset + 0) {
						return
					}
					if !yield(offset + 1) {
						return
					}
					if !yield(offset + 2) {
						return
					}
				case 0b1000:
					if !yield(offset + 3) {
						return
					}
				case 0b1001:
					if !yield(offset + 0) {
						return
					}
					if !yield(offset + 3) {
						return
					}
				case 0b1010:
					if !yield(offset + 1) {
						return
					}
					if !yield(offset + 3) {
						return
					}
				case 0b1011:
					if !yield(offset + 0) {
						return
					}
					if !yield(offset + 1) {
						return
					}
					if !yield(offset + 3) {
						return
					}
				case 0b1100:
					if !yield(offset + 2) {
						return
					}
					if !yield(offset + 3) {
						return
					}
				case 0b1101:
					if !yield(offset + 0) {
						return
					}
					if !yield(offset + 2) {
						return
					}
					if !yield(offset + 3) {
						return
					}
				case 0b1110:
					if !yield(offset + 1) {
						return
					}
					if !yield(offset + 2) {
						return
					}
					if !yield(offset + 3) {
						return
					}
				case 0b1111:
					if !yield(offset + 0) {
						return
					}
					if !yield(offset + 1) {
						return
					}
					if !yield(offset + 2) {
						return
					}
					if !yield(offset + 3) {
						return
					}
				}
				offset += 4
			}
		}
	}
}

// RangeBetween is like [bitmapExt.Range], but only returns start <= v < end.
func (dst bitmap[T]) RangeBetween(start, end T) iter.Seq[T] {
	// TODO: optimize
	return func(yield func(T) bool) {
		for v := range dst.Range() {
			if v < start {
				continue
			}
			if v >= end {
				break
			}
			if !yield(v) {
				return
			}
		}
	}
}

// Prev gets the index of the one <= i. If not found, it returns 0 and false.
func (dst bitmap[T]) Prev(i T) (T, bool) {
	// TODO: optimize
	for lower, ok := dst.Min(); ok && i >= lower; i-- {
		if dst.Contains(i) {
			return i, true
		}
	}
	return 0, false
}

// Next gets the index of the one >= i. If not found, it returns the index of
// the last zero and false.
func (dst bitmap[T]) Next(i T) (T, bool) {
	// TODO: optimize
	for upper, ok := dst.Max(); ok && i <= upper; i++ {
		if dst.Contains(i) {
			return i, true
		}
	}
	upper, _ := dst.MaxZero()
	return upper, false
}
