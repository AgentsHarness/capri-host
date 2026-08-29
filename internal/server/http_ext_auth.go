package server

import (
	"net/http"
)

// http_ext_auth.go — 认证与账号端点（登录登出、API key、订阅、隐私、续费规则、投放问卷）。

// ── Auth / FS / 其它 ──────────────────────────────────────────────────

func (s *Server) handleAuthInfo(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/auth/info", map[string]any{})
}

func (s *Server) handleAuthLogout(w http.ResponseWriter, r *http.Request) {
	// scope 可选项按 SPEC 省略。
	s.xaiCall(w, r, "x.ai/auth/logout", map[string]any{})
}

func (s *Server) handleAuthGetURL(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/auth/get_url", map[string]any{})
}

func (s *Server) handleAuthSubmitCode(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Code string `json:"code"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Code == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 code"})
		return
	}
	s.xaiCall(w, r, "x.ai/auth/submit_code", map[string]any{"code": body.Code})
}

func (s *Server) handleAutoTopupRule(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/auto-topup-rule", map[string]any{})
}

// ── 认证（x.ai/getApiKey / setApiKey / auth/*）───────────────────────

// handleApiKeyGet — POST /api/api-key-get → x.ai/getApiKey（无参）。
func (s *Server) handleApiKeyGet(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/getApiKey", map[string]any{})
}

// handleApiKeySet — POST /api/api-key-set {key?} → x.ai/setApiKey {key?}
// （key 缺省或空串 = 清除已存 key）。
func (s *Server) handleApiKeySet(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Key string `json:"key,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	s.xaiCall(w, r, "x.ai/setApiKey", map[string]any{"key": body.Key})
}

// handleAuthGetBearerToken — POST /api/auth/get-bearer-token →
// x.ai/auth/getBearerToken（无参）。
func (s *Server) handleAuthGetBearerToken(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/auth/getBearerToken", map[string]any{})
}

// handleAuthCancel — POST /api/auth/cancel {requestSeq?} →
// x.ai/auth/cancel {request_seq?}（SNAKE_CASE，可选；缺省取消任意进行中的
// 登录）。
func (s *Server) handleAuthCancel(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RequestSeq *uint64 `json:"requestSeq,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{}
	if body.RequestSeq != nil {
		params["request_seq"] = *body.RequestSeq
	}
	s.xaiCall(w, r, "x.ai/auth/cancel", params)
}

// handleAuthCheckSubscription — POST /api/auth/check-subscription →
// x.ai/auth/check_subscription（无参）。
func (s *Server) handleAuthCheckSubscription(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/auth/check_subscription", map[string]any{})
}

// ── 隐私 / 灰度（x.ai/privacy/*、x.ai/rollout/*）────────────────────

// handlePrivacySetCodingDataRetention — POST
// /api/privacy/set-coding-data-retention {codingDataRetentionOptOut} →
// x.ai/privacy/setCodingDataRetention {codingDataRetentionOptOut}
// （camelCase，必填 bool）。
func (s *Server) handlePrivacySetCodingDataRetention(w http.ResponseWriter, r *http.Request) {
	var body struct {
		CodingDataRetentionOptOut *bool `json:"codingDataRetentionOptOut"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.CodingDataRetentionOptOut == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 codingDataRetentionOptOut"})
		return
	}
	s.xaiCall(w, r, "x.ai/privacy/setCodingDataRetention", map[string]any{
		"codingDataRetentionOptOut": *body.CodingDataRetentionOptOut,
	})
}

// handleRolloutSurvey — POST /api/rollout/survey {preferences, feedback} →
// x.ai/rollout/survey {sessionId, preferences, feedback}（camelCase；
// preferences/feedback 必填）。
func (s *Server) handleRolloutSurvey(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Preferences []string `json:"preferences"`
		Feedback    string   `json:"feedback"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.Preferences == nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 preferences"})
		return
	}
	s.xaiCall(w, r, "x.ai/rollout/survey", map[string]any{
		"sessionId":   "",
		"preferences": body.Preferences,
		"feedback":    body.Feedback,
	})
}

// registerExtAuthRoutes 注册本域路由（路由与实现同址）。
func (s *Server) registerExtAuthRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/auth/info", s.handleAuthInfo)
	mux.HandleFunc("POST /api/auth/logout", s.handleAuthLogout)
	mux.HandleFunc("POST /api/auth/get-url", s.handleAuthGetURL)
	mux.HandleFunc("POST /api/auth/submit-code", s.handleAuthSubmitCode)
	mux.HandleFunc("POST /api/billing/auto-topup-rule", s.handleAutoTopupRule)
	mux.HandleFunc("POST /api/api-key-get", s.handleApiKeyGet)
	mux.HandleFunc("POST /api/api-key-set", s.handleApiKeySet)
	mux.HandleFunc("POST /api/auth/get-bearer-token", s.handleAuthGetBearerToken)
	mux.HandleFunc("POST /api/auth/cancel", s.handleAuthCancel)
	mux.HandleFunc("POST /api/auth/check-subscription", s.handleAuthCheckSubscription)
	mux.HandleFunc("POST /api/privacy/set-coding-data-retention", s.handlePrivacySetCodingDataRetention)
	mux.HandleFunc("POST /api/rollout/survey", s.handleRolloutSurvey)
}
