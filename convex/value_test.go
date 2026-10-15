package convex

import (
	"encoding/json"
	"math"
	"testing"
)

func TestValueIntegerRoundTrip(t *testing.T) {
	in := NewValue(int64(42))
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var out Value
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if got := out.Raw().(int64); got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
}

func TestValueSpecialFloatEncoding(t *testing.T) {
	in := NewValue(math.Inf(1))
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if string(raw) != `{"$float":"Infinity"}` {
		t.Fatalf("unexpected float encoding: %s", string(raw))
	}
}

func TestUnsupportedSetMap(t *testing.T) {
	var out Value
	if err := json.Unmarshal([]byte(`{"$set":[]}`), &out); err == nil {
		t.Fatalf("expected $set error")
	}
	if err := json.Unmarshal([]byte(`{"$map":[]}`), &out); err == nil {
		t.Fatalf("expected $map error")
	}
}

// Convex Int64 wire format: {"$integer": base64(8-byte little-endian int64)}.
// Matches get-convex/convex-js src/values/value.ts (DataView setBigInt64 LE).
func TestValueInt64WireEncoding(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{"int64 one", int64(1), "AQAAAAAAAAA="},
		{"int minus one", int(-1), "//////////8="},
		{"int64 min", int64(math.MinInt64), "AAAAAAAAAIA="},
		{"int64 max", int64(math.MaxInt64), "/////////38="},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(NewValue(tc.in))
			if err != nil {
				t.Fatalf("marshal failed: %v", err)
			}
			want := `{"$integer":"` + tc.want + `"}`
			if string(raw) != want {
				t.Fatalf("expected %s, got %s", want, string(raw))
			}
		})
	}
}

func TestValueInt64RoundTripBounds(t *testing.T) {
	for _, v := range []int64{math.MinInt64, math.MaxInt64, -1, 0, 1} {
		raw, err := json.Marshal(NewValue(v))
		if err != nil {
			t.Fatalf("marshal %d failed: %v", v, err)
		}
		var out Value
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("unmarshal %s failed: %v", string(raw), err)
		}
		if got, ok := out.Raw().(int64); !ok || got != v {
			t.Fatalf("round trip %d: got %#v", v, out.Raw())
		}
	}
}

func TestValueDecodesInt64WireFormat(t *testing.T) {
	var out Value
	if err := json.Unmarshal([]byte(`{"$integer":"AQAAAAAAAAA="}`), &out); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if got, ok := out.Raw().(int64); !ok || got != 1 {
		t.Fatalf("expected int64(1), got %#v", out.Raw())
	}
}

func TestValueRejectsMalformedInt64(t *testing.T) {
	cases := map[string]string{
		"decimal string (old wire format)": `{"$integer":"1"}`,
		"invalid base64":                   `{"$integer":"!!!!"}`,
		"nine bytes":                       `{"$integer":"AAAAAAAAAAAA"}`,
		"two bytes":                        `{"$integer":"MTA="}`,
	}
	for name, raw := range cases {
		var out Value
		if err := json.Unmarshal([]byte(raw), &out); err == nil {
			t.Fatalf("%s: expected error for %s", name, raw)
		}
	}
}
