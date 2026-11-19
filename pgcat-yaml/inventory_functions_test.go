package main

import (
	"encoding/json"
	"testing"
)

func TestInt32Exact(t *testing.T) {
	for _, tc := range []struct {
		input any
		want  int64
	}{
		{json.Number("20.000000"), 20},
		{json.Number("2147483647.000000"), 2147483647},
		{json.Number("-2147483648.000000"), -2147483648},
		{"-0.000000", 0},
		{int64(4), 4},
	} {
		got, err := functions["int32_exact"]([]any{tc.input})
		if err != nil || got != tc.want {
			t.Errorf("int32_exact(%v) = %v, %v; want %d", tc.input, got, err, tc.want)
		}
	}
	for _, input := range []any{"20.000001", "2147483648.000000", "-2147483649", "NaN", "Infinity", "1e2", "1.", "1.0.0", ""} {
		if _, err := int32Exact([]any{input}); err == nil {
			t.Errorf("int32_exact(%v) unexpectedly succeeded", input)
		}
	}
	if got, err := int32Exact([]any{nil}); err != nil || got != nil {
		t.Fatalf("NULL = %v, %v", got, err)
	}
	for _, args := range [][]any{nil, {1, 2}} {
		if _, err := int32Exact(args); err == nil {
			t.Fatalf("invalid arity %v succeeded", args)
		}
	}
}
