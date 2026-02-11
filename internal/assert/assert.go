// Package assert provides simples assertions for tests
package assert

import (
	"reflect"
	"testing"
)

type equaler[T any] interface {
	Equal(T) bool
}
func isNil(v any) bool {
	if v == nil {
		return true
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case
		reflect.Chan, reflect.Func, reflect.Interface, 
		reflect.Map, reflect.Pointer, reflect.Slice,
		reflect.UnsafePointer:
		return rv.IsNil()
	}

	return false
}

func isEqual[T any](got, want T) bool {
	if isNil(got) && isNil(want) {
		return  true
	}

	// try using using equal if it exists
	if eq, ok := any(got).(equaler[T]); ok {
		return eq.Equal(want)
	}

	// fallback
	return reflect.DeepEqual(got, want)
}

func Equal[T any](tb testing.TB, got, want T) {
	tb.Helper()

	if !isEqual(got, want) {
		tb.Errorf("got: %v; want: %v", got, want)
	}
}

func NotEqual[T any](tb testing.TB, got, want T) {
	tb.Helper()

	if isEqual(got, want) {
		tb.Errorf("got: %v; want: different values", got)
	}
}
