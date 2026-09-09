package parser

import "testing"

func TestGetJSONValueUsesNumericArrayIndex(t *testing.T) {
	data := map[string]any{"items": []any{map[string]any{"title": "first"}}}
	if got := GetJSONValue(data, []string{"items", "0", "title"}); got != "first" {
		t.Fatalf("got %#v, want first", got)
	}
	if got := GetJSONValue(data, []string{"items", "1"}); got != nil {
		t.Fatalf("out-of-range index returned %#v", got)
	}
}
