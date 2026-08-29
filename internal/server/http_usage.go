package server

import "net/http"

// ── Token 用量聚合报告（宿主侧统计，非 x.ai 直通）─────────────────────
//
// POST /api/usage-report {cwd?, sessionId?, from?, to?} → 聚合 [from, to]
// 时间窗口内 grok 会话的真实 token 用量（总/输入/输出/缓存命中/缓存写入/
// 命中率），并按模型分组（usage.modelUsage；无分组数据归 "unknown"）。
// cwd/sessionId 限定扫描范围（都省略 = 全部会话；仅 cwd = 该工作区所有
// 会话；sessionId 给定时 cwd 必填）；from/to 为 unix 秒（兼容毫秒，自动
// 识别；省略 = 全量到当前时刻）。响应 {ok:true, result:{from, to,
// sessions, total, byModel}}。

func (s *Server) handleUsageReport(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cwd       string `json:"cwd,omitempty"`
		SessionID string `json:"sessionId,omitempty"`
		From      int64  `json:"from,omitempty"`
		To        int64  `json:"to,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	rep, err := s.bridge.UsageReport(r.Context(), body.Cwd, body.SessionID, body.From, body.To)
	if err != nil {
		writeJSON(w, 500, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "result": rep})
}

// registerUsageRoutes 注册本域路由（路由与实现同址）。
func (s *Server) registerUsageRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/usage-report", s.handleUsageReport)
}
