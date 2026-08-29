package acp

import "context"

// ─────────────────────────────────────────────────────────────────────
// bridge_ext.go — x.ai 扩展方法的共享 helper（XaiCall 公共底座）。
//
// wire 键在 grok 源码中逐字段核验，camel/snake 混用是 agent 侧 wire 约定，
// 不是笔误。xaiCallUnwrapped 的消费者：bridge.go 内的各 x.ai/* 方法与
// bridge_ext_session.go 的 SessionLoadHistory。
// ─────────────────────────────────────────────────────────────────────

// ── typed x.ai extension helpers ─────────────────────────────────────
//
// Session rule (host convention): sessionId is OMITTED whenever grok
// declares it optional (its serde struct has no required session_id field)
// — grok falls back to the session cwd for path resolution. Methods whose
// grok request struct REQUIRES session_id pass `"sessionId": ""` so
// XaiCall fills in the active session id (HTTPError 404 when none).
//
// Per-method omission rules for optional fields (Go zero values):
//   - string "" → key omitted
//   - []string empty → key omitted
//   - *bool / *int nil → key omitted (grok applies its serde default)
//   - plain bool → always sent (false == the grok default, identical wire)

// xaiCallUnwrapped is XaiCall + UnwrapExtResult, typed to the object
// payloads the x.ai/* extension methods return. A non-object result is
// coerced to {} (the pre-any-typing behavior of request());
// workspace_list_recent is the one bare-array method and unwraps into any
// at its call site.
func (b *Bridge) xaiCallUnwrapped(ctx context.Context, method string, params map[string]any) (map[string]any, error) {
	res, err := b.XaiCall(ctx, method, params)
	if err != nil {
		return nil, err
	}
	m, ok := UnwrapExtResult(res).(map[string]any)
	if !ok {
		return map[string]any{}, nil
	}
	return m, nil
}

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
