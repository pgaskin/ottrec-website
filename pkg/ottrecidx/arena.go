//go:build goexperiment.arenas

package ottrecidx

import (
	goarena "arena"
	"runtime"
	"strconv"
	"sync/atomic"
	"unsafe"
)

type arena struct {
	alloc atomic.Uint64
	arena *goarena.Arena
}

func newArena() *arena {
	a := &arena{arena: goarena.NewArena()}
	runtime.AddCleanup(a, (*goarena.Arena).Free, a.arena)
	return a
}

func arenaNew[T any](a *arena) *T {
	v := goarena.New[T](a.arena)
	a.alloc.Add(uint64(unsafe.Sizeof(*v)))
	return v
}

func arenaMakeSlice[T any](a *arena, len, cap int) []T {
	v := goarena.MakeSlice[T](a.arena, len, cap)
	a.alloc.Add(uint64(unsafeSizeofSlice(v)))
	return v
}

func (a *arena) String() string {
	return "arena[goexperiment.arenas]{alloc:" + strconv.FormatUint(a.alloc.Add(0), 10) + "}"
}

func unsafeSizeofSlice[T any](v []T) uintptr {
	if cap(v) != 0 {
		return unsafe.Sizeof(v[0]) * uintptr(cap(v))
	}
	return 0
}
