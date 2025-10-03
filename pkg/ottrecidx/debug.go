package ottrecidx

import (
	"encoding/hex"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
)

// this file contains methods only used for debugging

var enableIndexerSanityCheck bool

// EnableIndexerSanityCheck enables the indexer sanity checking. This isn't
// necessary as it's only there to detect bugs, but you should run profile.go on
// a bunch of schedules with it enabled during development.
func EnableIndexerSanityCheck() {
	enableIndexerSanityCheck = true
}

// DebugIndexer dumps information about allocations made by the indexer.
func DebugIndexer(dxr *Indexer, includeIndexes bool) string {
	var s strings.Builder
	s.WriteString(dxr.a.String())
	s.WriteString("\n")
	s.WriteString(dxr.sa.String())
	s.WriteString("\n")
	s.WriteString(dxr.act.String())
	s.WriteString("\n")
	s.WriteString(dxr.tm.String())
	if includeIndexes {
		for _, hash := range slices.Sorted(maps.Keys(dxr.idx)) {
			s.WriteString("\n")
			s.WriteString(dxr.idx[hash].String())
		}
	}
	return s.String()
}

// debug
func (idx *Index) dbgHash() string {
	return idx.hash[:8]
}

// debug
func (idx *Index) String() string {
	var s strings.Builder
	s.WriteString("Index{")
	s.WriteString("hash:")
	s.WriteString(idx.dbgHash())
	s.WriteString(" obj:")
	s.WriteString(strconv.Itoa(len(idx.obj)))
	s.WriteString(" scan:")
	s.WriteString(idx.durScan.Truncate(time.Nanosecond).String())
	s.WriteString(" import:")
	s.WriteString(idx.durImport.Truncate(time.Millisecond).String())
	if idx.durSanityCheck != 0 {
		s.WriteString(" sanityCheck:")
		s.WriteString(idx.durSanityCheck.Truncate(time.Millisecond).String())
	}
	s.WriteString(" precompute:")
	s.WriteString(idx.durPrecompute.Truncate(time.Millisecond).String())
	s.WriteString(" dataUpdated:")
	s.WriteString(idx.updated.Format("2006-01-02"))
	s.WriteString("}")
	return s.String()
}

// debug
func (ref baseRef) dbgType() string {
	if !ref.flt.IsNil() {
		return "FilteredRef"
	}
	return "Ref"
}

// debug
func (ref typedRef[T]) dbgType() string {
	t, ok := strings.CutPrefix(reflect.TypeOf((*T)(nil)).Elem().Name(), "x")
	if !ok {
		panic("wtf")
	}
	return ref.baseRef.dbgType() + "[" + t + "]"
}

// debug
func (ref baseRef) dbgValue() string {
	if ref.Valid() {
		return ref.idx.dbgHash() + "+" + ref.obj.String()
	}
	return "nil"
}

// debug
func (ref baseRef) dbgData() string {
	if ref.Valid() {
		x := fmt.Sprintf("%#v", ref.deref())
		return x[strings.Index(x, "{"):]
	}
	return ""
}

// debug
func (ref baseRef) String() string {
	return ref.dbgType() + "<" + ref.dbgValue() + ">"
}

// debug
func (ref typedRef[T]) String() string {
	return ref.dbgType() + "<" + ref.dbgValue() + ">"
}

// debug
func (ref baseRef) GoString() string {
	return ref.String() + ref.dbgData()
}

// debug
func (ref typedRef[T]) GoString() string {
	return ref.String() + ref.dbgData()
}

// debug
func (mut MutableDataRef) String() string {
	return "Mutable" + mut.unsafe.String()
}

// debug
func (mut MutableDataRef) GoString() string {
	return "Mutable" + mut.unsafe.String() + "{" + mut.unsafe.flt.String() + "}"
}

// debug
func (a stringInterner) String() string {
	var s strings.Builder
	s.WriteString(reflect.TypeOf(a).String())
	s.WriteString("{n:")
	s.WriteString(strconv.Itoa(len(a.buf)))
	var tlen, tcap int
	for _, b := range a.buf {
		tlen += len(b)
		tcap += cap(b)
	}
	s.WriteString(" len:")
	s.WriteString(strconv.Itoa(tlen))
	s.WriteString(" cap:")
	s.WriteString(strconv.Itoa(tcap))
	s.WriteString(" ratio:")
	s.WriteString(strconv.FormatFloat(float64(tlen)/float64(a.interned), 'f', 3, 64))
	s.WriteString(" real_ratio:")
	s.WriteString(strconv.FormatFloat(float64(tcap)/float64(a.interned), 'f', 3, 64))
	if a.cache != nil {
		s.WriteString(" cache:")
		s.WriteString(strconv.Itoa(len(a.cache)))
	}
	s.WriteRune('}')
	return s.String()
}

// debug
func (n *interner[T]) String() string {
	return fmt.Sprintf("%T{len:%d ratio:%.3f}", n, len(n.a), float64(len(n.a))/float64(n.n))
}

// debug
func (dst bitmap[T]) String() string {
	return hex.EncodeToString(dst.kb.ToBytes())
}
