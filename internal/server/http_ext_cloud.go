package server

import (
	"net/http"
)

// http_ext_cloud.go — 云端环境端点（列表/创建/更新/删除/终止）。

func (s *Server) handleCloudEnvList(w http.ResponseWriter, r *http.Request) {
	s.xaiCall(w, r, "x.ai/cloud/env/list", map[string]any{})
}

// ── 云端沙箱（x.ai/cloud/*）──────────────────────────────────────────

// handleCloudTerminate — POST /api/cloud/terminate {sandboxId} →
// x.ai/cloud/terminate {sandbox_id}（SNAKE_CASE，必填）。
func (s *Server) handleCloudTerminate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SandboxID string `json:"sandboxId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.SandboxID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 sandboxId"})
		return
	}
	s.xaiCall(w, r, "x.ai/cloud/terminate", map[string]any{"sandbox_id": body.SandboxID})
}

// handleCloudEnvCreate — POST /api/cloud/env/create {name?, description?,
// repository?, defaultBranch?, containerImage?, setupScript?} →
// x.ai/cloud/env/create（SNAKE_CASE：default_branch / container_image /
// setup_script；均可选，空则省略）。
func (s *Server) handleCloudEnvCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name           string `json:"name,omitempty"`
		Description    string `json:"description,omitempty"`
		Repository     string `json:"repository,omitempty"`
		DefaultBranch  string `json:"defaultBranch,omitempty"`
		ContainerImage string `json:"containerImage,omitempty"`
		SetupScript    string `json:"setupScript,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	params := map[string]any{}
	if body.Name != "" {
		params["name"] = body.Name
	}
	if body.Description != "" {
		params["description"] = body.Description
	}
	if body.Repository != "" {
		params["repository"] = body.Repository
	}
	if body.DefaultBranch != "" {
		params["default_branch"] = body.DefaultBranch
	}
	if body.ContainerImage != "" {
		params["container_image"] = body.ContainerImage
	}
	if body.SetupScript != "" {
		params["setup_script"] = body.SetupScript
	}
	s.xaiCall(w, r, "x.ai/cloud/env/create", params)
}

// handleCloudEnvUpdate — POST /api/cloud/env/update {environmentId, name?,
// …} → x.ai/cloud/env/update {environment_id, …}（SNAKE_CASE，environment_id
// 必填）。
func (s *Server) handleCloudEnvUpdate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		EnvironmentID  string `json:"environmentId"`
		Name           string `json:"name,omitempty"`
		Description    string `json:"description,omitempty"`
		Repository     string `json:"repository,omitempty"`
		DefaultBranch  string `json:"defaultBranch,omitempty"`
		ContainerImage string `json:"containerImage,omitempty"`
		SetupScript    string `json:"setupScript,omitempty"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.EnvironmentID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 environmentId"})
		return
	}
	params := map[string]any{"environment_id": body.EnvironmentID}
	if body.Name != "" {
		params["name"] = body.Name
	}
	if body.Description != "" {
		params["description"] = body.Description
	}
	if body.Repository != "" {
		params["repository"] = body.Repository
	}
	if body.DefaultBranch != "" {
		params["default_branch"] = body.DefaultBranch
	}
	if body.ContainerImage != "" {
		params["container_image"] = body.ContainerImage
	}
	if body.SetupScript != "" {
		params["setup_script"] = body.SetupScript
	}
	s.xaiCall(w, r, "x.ai/cloud/env/update", params)
}

// handleCloudEnvDelete — POST /api/cloud/env/delete {environmentId} →
// x.ai/cloud/env/delete {environment_id}（SNAKE_CASE，必填）。
func (s *Server) handleCloudEnvDelete(w http.ResponseWriter, r *http.Request) {
	var body struct {
		EnvironmentID string `json:"environmentId"`
	}
	if !readBody(w, r, &body) {
		return
	}
	if body.EnvironmentID == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要 environmentId"})
		return
	}
	s.xaiCall(w, r, "x.ai/cloud/env/delete", map[string]any{"environment_id": body.EnvironmentID})
}

// registerExtCloudRoutes 注册本域路由（路由与实现同址）。
func (s *Server) registerExtCloudRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/cloud/env/list", s.handleCloudEnvList)
	mux.HandleFunc("POST /api/cloud/terminate", s.handleCloudTerminate)
	mux.HandleFunc("POST /api/cloud/env/create", s.handleCloudEnvCreate)
	mux.HandleFunc("POST /api/cloud/env/update", s.handleCloudEnvUpdate)
	mux.HandleFunc("POST /api/cloud/env/delete", s.handleCloudEnvDelete)
}
