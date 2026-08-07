package acp

// UnwrapExtResult unwraps the common ExtMethodResult envelope
// {"result": <payload>, "error": ...} → the inner payload map.
// Non-envelope results are returned unchanged. nil-safe.
//
// Detection mirrors the envelope shape the grok extensions return via
// to_ext_response: a map whose "result" key holds another map. Raw
// responses (e.g. x.ai/git/checkout_session_head, which the agent emits
// with to_raw_response) and error-only envelopes ({"result": null,
// "error": ...}) have no map payload and pass through untouched.
func UnwrapExtResult(res map[string]any) map[string]any {
	if inner, ok := res["result"].(map[string]any); ok {
		return inner
	}
	return res
}
