package acp

// UnwrapExtResult unwraps the common ExtMethodResult envelope
// {"result": <payload>, "error": ...} → the inner payload.
// Non-envelope results are returned unchanged. nil-safe.
//
// Detection mirrors the envelope shape the grok extensions return via
// to_ext_response: a map whose "result" key holds another map. Raw
// responses (e.g. x.ai/git/checkout_session_head, which the agent emits
// with to_raw_response) and error-only envelopes ({"result": null,
// "error": ...}) have no map payload and pass through untouched.
// The result may be any JSON value (object, bare array, scalar) — callers
// that need an object coerce explicitly.
func UnwrapExtResult(res any) any {
	if m, ok := res.(map[string]any); ok {
		if inner, ok := m["result"].(map[string]any); ok {
			return inner
		}
	}
	return res
}
