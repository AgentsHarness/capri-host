package acp

import (
	"reflect"
	"testing"
)

// bridge_ext_test.go — UnwrapExtResult 信封语义用例。

func TestUnwrapExtResult(t *testing.T) {
	// Envelope: {"result": <payload>, "error": ...} → the inner payload.
	payload := map[string]any{"branch": "main", "files": []any{"a.go"}}
	got := UnwrapExtResult(map[string]any{"result": payload, "error": nil})
	if !reflect.DeepEqual(got, payload) {
		t.Errorf("envelope unwrap = %v, want %v", got, payload)
	}

	// Non-envelope results pass through unchanged (same map).
	plain := map[string]any{"ok": true}
	if got := UnwrapExtResult(plain); !reflect.DeepEqual(got, plain) {
		t.Errorf("non-envelope = %v, want %v", got, plain)
	}

	// nil-safe.
	if got := UnwrapExtResult(nil); got != nil {
		t.Errorf("nil = %v, want nil", got)
	}

	// Error-only envelope ({"result": null, "error": ...}) has no map
	// payload → returned unchanged so callers can inspect the error.
	errEnv := map[string]any{"result": nil, "error": "boom"}
	if got := UnwrapExtResult(errEnv); !reflect.DeepEqual(got, errEnv) {
		t.Errorf("error-only envelope = %v, want %v", got, errEnv)
	}

	// Envelope whose payload is not a map → unchanged.
	strEnv := map[string]any{"result": "not-a-map"}
	if got := UnwrapExtResult(strEnv); !reflect.DeepEqual(got, strEnv) {
		t.Errorf("non-map payload envelope = %v, want %v", got, strEnv)
	}
}
