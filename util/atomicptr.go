package util

import (
	"sync/atomic"
	"unsafe"
)

type AtomicPtr[T any] struct {
	ptr *T
}

func (ap *AtomicPtr[T]) getUnsafePtr() *unsafe.Pointer {
	return (*unsafe.Pointer)(unsafe.Pointer(&ap.ptr))
}

func (ap *AtomicPtr[T]) Load() *T {
	return (*T)(atomic.LoadPointer(ap.getUnsafePtr()))
}

func (ap *AtomicPtr[T]) Store(val *T) {
	atomic.StorePointer(ap.getUnsafePtr(), unsafe.Pointer(val))
}
