package acp

import (
	"context"
)

// bridge_ext_terminal.go — PTY 输入直通（x.ai/terminal/pty/*）。

// TerminalPtyInput sends x.ai/terminal/pty/input: {terminalId, data}
// (data is base64-encoded bytes) as a fire-and-forget NOTIFICATION —
// the agent handles it only in its ext_notification path, so a
// request-style call would fail with -32601 (mirrors TogglePlanMode).
// Returns a bare {"ok": true} without waiting for a reply.
func (b *Bridge) TerminalPtyInput(ctx context.Context, terminalID, data string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if err := b.write(map[string]any{
		"jsonrpc": "2.0",
		"method":  "_x.ai/terminal/pty/input",
		"params":  map[string]any{"terminalId": terminalID, "data": data},
	}); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true}, nil
}
