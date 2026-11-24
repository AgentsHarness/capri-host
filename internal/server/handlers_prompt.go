package server

import (
	"context"
	"github.com/AgentsHarness/capri-host/internal/acp"
	"net/http"
	"time"
)

type promptBody struct {
	Blocks []acp.ContentBlock `json:"blocks"`
	// Optional: target session (defaults to the active session).
	SessionID string `json:"sessionId,omitempty"`
	// Optional: messageId (UUID) / _meta, forwarded on the session/prompt
	// params only when set (absent key ≠ off, matching the TUI).
	MessageID string         `json:"messageId,omitempty"`
	Meta      map[string]any `json:"meta,omitempty"`
}

func (s *Server) handlePrompt(w http.ResponseWriter, r *http.Request) {
	var body promptBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if len(body.Blocks) == 0 {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "blocks 不能为空"})
		return
	}
	// 显式指向的会话必须存在（纯内存检查，无 agent 交互）：未知会话在
	// 受理前同步 404，不走受理流程。未显式指定会话（缺省 active）时
	// 跳过——无活动会话时的恢复/新建是 agent roundtrip，留在后台完成，
	// 失败经 SSE error 事件送达（bridge.PromptWithOpts 的广播）。
	if body.SessionID != "" && !s.bridge.HasSession(body.SessionID) {
		writeJSON(w, 404, map[string]any{"ok": false, "error": "会话不存在"})
		return
	}
	// 受理即返回：POST 只确认"已受理"（{ok:true}），回合在后台跑，结果
	// （done / error / cancelled + meta）全部经 live 通道（SSE/WS）送达。
	// 请求不再在反代（Cloudflare ~100s / nginx）前挂到回合结束，反代超时
	// （524/504）从根上消失。回合执行路径不变：context.Background() 使
	// 客户端断开不会取消回合——浏览器 crash / 关页后回合照常跑完，进度
	// 与结果仍走 live 通道，只有显式 /api/cancel 能停它。
	go func() {
		if s.promptFn != nil {
			// Test-injected promptFn keeps its exact contract.
			_, _ = s.promptFn(context.Background(), body.SessionID, body.Blocks)
			return
		}
		_, _, _ = s.bridge.PromptWithOpts(context.Background(), body.SessionID, body.Blocks,
			acp.PromptOpts{MessageID: body.MessageID, Meta: body.Meta})
	}()
	writeJSON(w, 200, map[string]any{"ok": true})
}

type cancelBody struct {
	// Optional: target session (defaults to the active session).
	SessionID string `json:"sessionId,omitempty"`
	// Optional session/cancel `_meta` seeds (grok reads them at
	// acp_agent.rs:2079-2108): cancelTrigger ("esc"|"ctrl_c"),
	// cancelSubagents (default true), rewindIfNoOutput / rewindIfPristine.
	// Only the keys the client sends are forwarded; absent = agent default.
	CancelTrigger    string `json:"cancelTrigger,omitempty"`
	CancelSubagents  *bool  `json:"cancelSubagents,omitempty"`
	RewindIfNoOutput *bool  `json:"rewindIfNoOutput,omitempty"`
	RewindIfPristine *bool  `json:"rewindIfPristine,omitempty"`
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	var body cancelBody
	_ = readJSON(r, &body)
	meta := map[string]any{}
	if body.CancelTrigger != "" {
		meta["cancelTrigger"] = body.CancelTrigger
	}
	if body.CancelSubagents != nil {
		meta["cancelSubagents"] = *body.CancelSubagents
	}
	if body.RewindIfNoOutput != nil {
		meta["rewindIfNoOutput"] = *body.RewindIfNoOutput
	}
	if body.RewindIfPristine != nil {
		meta["rewindIfPristine"] = *body.RewindIfPristine
	}
	s.bridge.CancelWithMeta(body.SessionID, meta)
	writeJSON(w, 200, map[string]any{"ok": true})
}

// handleAgentRestart restarts the grok agent process on user request:
// kill the current process (if any), clear host state, boot a fresh agent
// and restore the last session. The host never restarts the agent on its
// own — this endpoint is the only restart path (assume the agent is
// reliable; failures surface as errors, and the user decides when to
// restart).
func (s *Server) handleAgentRestart(w http.ResponseWriter, r *http.Request) {
	// 重启是服务端状态操作，用独立超时而非 r.Context()：客户端中途断
	// 开不应取消重启流程 —— kill 之后 boot 失败会留下无进程状态。
	// 4 分钟 = acp 包 bootTimeout(2min) × 2，覆盖杀进程 + boot + 恢复。
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if err := s.bridge.RestartAgent(ctx); err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}
