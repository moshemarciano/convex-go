package baseclient

import (
	"fmt"
	"math"
	"reflect"
)

// NormalizeValue converts a Go value of an arbitrary typed shape (for example
// []string, []int64, map[string]string, []map[string]any, *string, or named
// scalar types) into one of the base shapes understood by both the canonical
// key builder and the wire encoder: nil, bool, string, int64, float64,
// []byte, []any and map[string]any. Only the top level is converted; nested
// elements are returned as-is so callers can recurse with their own switch.
func NormalizeValue(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return nil, nil
		}
		return rv.Elem().Interface(), nil
	case reflect.Bool:
		return rv.Bool(), nil
	case reflect.String:
		return rv.String(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return normalizeUint(rv.Uint())
	case reflect.Float32, reflect.Float64:
		return rv.Float(), nil
	case reflect.Slice, reflect.Array:
		return normalizeSequence(rv), nil
	case reflect.Map:
		return normalizeMap(rv)
	}
	return nil, fmt.Errorf("unsupported value type %T", v)
}

func normalizeUint(u uint64) (any, error) {
	if u > math.MaxInt64 {
		return nil, fmt.Errorf("unsigned value %d overflows int64", u)
	}
	return int64(u), nil
}

func normalizeSequence(rv reflect.Value) any {
	if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
		return rv.Bytes()
	}
	if rv.Kind() == reflect.Slice && rv.IsNil() {
		return []any{}
	}
	out := make([]any, rv.Len())
	for i := range out {
		out[i] = rv.Index(i).Interface()
	}
	return out
}

func normalizeMap(rv reflect.Value) (any, error) {
	if rv.Type().Key().Kind() != reflect.String {
		return nil, fmt.Errorf("unsupported map key type %s", rv.Type().Key())
	}
	out := make(map[string]any, rv.Len())
	iter := rv.MapRange()
	for iter.Next() {
		out[iter.Key().String()] = iter.Value().Interface()
	}
	return out, nil
}
