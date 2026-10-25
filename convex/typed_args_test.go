package convex

import (
	"encoding/json"
	"testing"
)

func TestMarshalFunctionArgsTypedValues(t *testing.T) {
	typed := map[string]any{
		"keys":  []string{"a", "b"},
		"nums":  []float64{1.5},
		"flags": []bool{true},
		"m":     map[string]string{"k": "v"},
		"rows":  []map[string]any{{"ks": []string{"z"}}},
		"i32":   []int32{4},
	}
	untyped := map[string]any{
		"keys":  []any{"a", "b"},
		"nums":  []any{1.5},
		"flags": []any{true},
		"m":     map[string]any{"k": "v"},
		"rows":  []any{map[string]any{"ks": []any{"z"}}},
		"i32":   []any{4},
	}
	got, err := marshalFunctionArgs(typed)
	if err != nil {
		t.Fatalf("typed args rejected by encoder: %v", err)
	}
	want, err := marshalFunctionArgs(untyped)
	if err != nil {
		t.Fatal(err)
	}
	if !jsonEqual(t, got, want) {
		t.Fatalf("typed wire %s != untyped wire %s", got, want)
	}
}

func TestValueEncodesTypedSlicesAndMaps(t *testing.T) {
	cases := map[string]any{
		"[]string":          []string{"a"},
		"[]int64":           []int64{1},
		"[]float64":         []float64{1},
		"[]bool":            []bool{true},
		"map[string]string": map[string]string{"a": "b"},
		"[]map":             []map[string]any{{"a": 1}},
	}
	for name, in := range cases {
		if _, err := json.Marshal(NewValue(in)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func TestValueRejectsUnsupportedTypes(t *testing.T) {
	for _, in := range []any{struct{}{}, map[int]string{1: "a"}, make(chan int)} {
		if _, err := json.Marshal(NewValue(in)); err == nil {
			t.Fatalf("expected error for %T", in)
		}
	}
}

func TestSubscribeAcceptsTypedSliceArgs(t *testing.T) {
	state := NewLocalState()
	args := map[string]any{"keys": []string{"a", "b"}}
	if _, _, _, err := state.Subscribe("classifierVerdicts:serviceGet", args); err != nil {
		t.Fatalf("subscribe rejected typed args: %v", err)
	}
}

func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var x, y any
	if err := json.Unmarshal(a, &x); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &y); err != nil {
		t.Fatal(err)
	}
	xa, _ := json.Marshal(x)
	yb, _ := json.Marshal(y)
	return string(xa) == string(yb)
}
