package baseclient

import "testing"

type namedStr string

func TestCanonicalQueryTokenTypedArgs(t *testing.T) {
	s := "x"
	var nilPtr *string
	cases := []struct {
		name string
		args map[string]any
		want string
	}{
		{"strings", map[string]any{"keys": []string{"a", "b"}}, `f|{"keys":["a","b"]}`},
		{"int64s", map[string]any{"v": []int64{1, 2}}, `f|{"v":[1,2]}`},
		{"ints32", map[string]any{"v": []int32{1, 2}}, `f|{"v":[1,2]}`},
		{"uints", map[string]any{"v": []uint{1, 2}}, `f|{"v":[1,2]}`},
		{"float64s", map[string]any{"v": []float64{1.5}}, `f|{"v":[1.5]}`},
		{"float32", map[string]any{"v": float32(0.5)}, `f|{"v":0.5}`},
		{"bools", map[string]any{"v": []bool{true, false}}, `f|{"v":[true,false]}`},
		{"mapstr", map[string]any{"m": map[string]string{"b": "2", "a": "1"}}, `f|{"m":{"a":"1","b":"2"}}`},
		{"slicemap", map[string]any{"m": []map[string]any{{"k": []string{"z"}}}}, `f|{"m":[{"k":["z"]}]}`},
		{"nested", map[string]any{"o": map[string]any{"keys": []string{"a"}}}, `f|{"o":{"keys":["a"]}}`},
		{"emptyslice", map[string]any{"v": []string{}}, `f|{"v":[]}`},
		{"nilslice", map[string]any{"v": []string(nil)}, `f|{"v":[]}`},
		{"ptr", map[string]any{"v": &s, "n": nilPtr}, `f|{"n":null,"v":"x"}`},
		{"named", map[string]any{"v": namedStr("q")}, `f|{"v":"q"}`},
		{"array", map[string]any{"v": [2]string{"a", "b"}}, `f|{"v":["a","b"]}`},
		{"bytes", map[string]any{"v": []byte{1, 2}}, `f|{"v":{"$bytes":"AQI="}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CanonicalQueryToken("f", tc.args)
			if err != nil {
				t.Fatalf("token failed: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}

func TestCanonicalQueryTokenTypedMatchesUntyped(t *testing.T) {
	typed, err := CanonicalQueryToken("f", map[string]any{"k": []string{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	untyped, err := CanonicalQueryToken("f", map[string]any{"k": []any{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if typed != untyped {
		t.Fatalf("typed %s != untyped %s", typed, untyped)
	}
}

func TestCanonicalQueryTokenExistingKeysStable(t *testing.T) {
	args := map[string]any{
		"n": nil, "t": true, "f": false, "s": "he\"llo", "fl": 1.5,
		"i": 7, "i64": int64(-3), "l": []any{1, "a", nil},
		"m": map[string]any{"z": 1, "a": []any{}},
	}
	want := `f|{"f":false,"fl":1.5,"i":7,"i64":-3,"l":[1,"a",null],"m":{"a":[],"z":1},"n":null,"s":"he\"llo","t":true}`
	got, err := CanonicalQueryToken("f", args)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("key changed: %s", got)
	}
}

func TestCanonicalQueryTokenRejectsUnsupported(t *testing.T) {
	bad := []map[string]any{
		{"v": struct{}{}},
		{"v": map[int]string{1: "a"}},
		{"v": make(chan int)},
		{"v": uint64(1) << 63},
	}
	for _, args := range bad {
		if _, err := CanonicalQueryToken("f", args); err == nil {
			t.Fatalf("expected error for %v", args)
		}
	}
}
