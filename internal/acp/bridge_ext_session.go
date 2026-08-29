package acp

import (
	"context"
)

// bridge_ext_session.go — 网关会话历史直通。公共 helper 见 bridge_ext.go。

// SessionLoadHistory calls x.ai/session/load_history — fetch one older
// page of a gateway-backed conversation by client-owned cursor (grok-build
// chat_conversation_history.rs: `beforeId` → `nextBeforeId`). Wire keys:
// {beforeId?} — camelCase, OPTIONAL (first page omits it; the response's
// nextBeforeId pages further). No sessionId is sent: the conversation is
// gateway-backed (like x.ai/session/info) and the cursor addresses the
// page. The grok-build handler is currently a stub (method_not_found), so
// the result is passed through unwrapped as-is.
func (b *Bridge) SessionLoadHistory(ctx context.Context, beforeID string) (map[string]any, error) {
	params := map[string]any{}
	if beforeID != "" {
		params["beforeId"] = beforeID
	}
	return b.xaiCallUnwrapped(ctx, "x.ai/session/load_history", params)
}
