package server

import "net/http"

// ── 单会话聚合统计（composer 状态条数据源）─────────────────────────────
//
// POST /api/session-stats {cwd, sessionId} → 该会话的聚合统计：轮数 /
// 步数 / LLM 总耗时 / 工具总耗时 / 首 token 平均延迟 / 吞吐 / 缓存命中率 /
// 输入输出 token（host 侧扫描 updates.jsonl 聚合，见 bridge_ext_stats.go）。
// cwd + sessionId 必填；会话无历史时返回全零统计（ok:true）。

func (s *Server) handleSessionStats(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd       string `json:"cwd,omitempty"`
		SessionID string `json:"sessionId,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.SessionID == "" || body.Cwd == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "sessionId 与 cwd 必填"})
		return
	}
	stats, err := s.bridge.SessionStats(r.Context(), body.Cwd, body.SessionID)
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "stats": stats})
}
