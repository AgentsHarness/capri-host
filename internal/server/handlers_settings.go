package server

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// handleExtensions — GET /api/extensions: the LOCAL extension inventory
// (~/.grok on disk), never routed through the agent. Missing/unreadable
// paths are skipped silently; the response is always 200 with
// {hooks:[], plugins:[], skills:[]}.
func (s *Server) handleExtensions(w http.ResponseWriter, r *http.Request) {
	home, err := os.UserHomeDir()
	if err != nil {
		writeJSON(w, 200, map[string]any{"hooks": []any{}, "plugins": []any{}, "skills": []any{}})
		return
	}
	writeJSON(w, 200, scanExtensions(home))
}

// handleSettings — GET /api/settings: every scalar config.toml table, keyed
// by its TOML path, except the custom-model definitions ([model],
// [model.<id>...]). A missing file yields {}. Writes go through
// POST /api/settings (whitelist).
func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, s.settingsPayload())
}

type settingsUpdateBody struct {
	CollapsedEditBlocks   *bool                `json:"collapsed_edit_blocks"`
	PageFlipOnSend        *bool                `json:"page_flip_on_send"`
	RememberToolApprovals *bool                `json:"remember_tool_approvals"`
	PermissionMode        *string              `json:"permission_mode"`
	FollowUpBehavior      *string              `json:"follow_up_behavior"`
	ToolCallDensity       *string              `json:"tool_call_density"`
	Toolset               *settingsToolsetBody `json:"toolset"`
}

// settingsToolsetBody is the writable subset of [toolset]: only
// ask_user_question.timeout_enabled / timeout_secs (validated against
// writableToolsetKeys). A `toolset` object with an unknown sub-table or
// key is rejected before any write.
type settingsToolsetBody struct {
	AskUserQuestion *map[string]any `json:"ask_user_question"`
}

// settingsBodyKeyKnown reports whether settingsUpdateBody declares the
// key. Kept in lockstep with the struct: a key missing here would be
// silently dropped by the JSON decode and look like a successful no-op.
func settingsBodyKeyKnown(key string) bool {
	switch key {
	case "collapsed_edit_blocks", "page_flip_on_send",
		"remember_tool_approvals", "permission_mode", "follow_up_behavior",
		"tool_call_density",
		"toolset":
		return true
	}
	return false
}

// settingsToolsetKeyKnown reports whether the [toolset.ask_user_question]
// sub-key is a declared writable key (lockstep with writableToolsetKeys).
// Kept as a server-side guard so unknown nested keys get an explicit 400
// instead of being silently dropped by the struct decode.
func settingsToolsetKeyKnown(key string) bool {
	switch key {
	case "timeout_enabled", "timeout_secs":
		return true
	}
	return false
}

// handleSettingsUpdate — POST /api/settings: patch the FE-consumed [ui]
// scalars and the [toolset.ask_user_question] timeout pair into
// config.toml. Unknown keys get a 400 up front (the struct decode would
// otherwise drop them silently). Response is the same payload as GET.
func (s *Server) handleSettingsUpdate(w http.ResponseWriter, r *http.Request) {
	var raw map[string]any
	if err := readJSON(r, &raw); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "无效 JSON"})
		return
	}
	for k, v := range raw {
		if !settingsBodyKeyKnown(k) {
			writeJSON(w, 400, map[string]any{"ok": false, "error": "不允许的设置项 " + k})
			return
		}
		if k == "toolset" {
			// Nested allowlist: only ask_user_question.{timeout_enabled,
			// timeout_secs}; anything else gets an explicit 400.
			ts, ok := v.(map[string]any)
			if !ok {
				writeJSON(w, 400, map[string]any{"ok": false, "error": "toolset 必须是对象"})
				return
			}
			for sk, sv := range ts {
				if sk != "ask_user_question" {
					writeJSON(w, 400, map[string]any{"ok": false, "error": "不允许的设置项 " + sk})
					return
				}
				aq, ok := sv.(map[string]any)
				if !ok {
					writeJSON(w, 400, map[string]any{"ok": false, "error": "toolset.ask_user_question 必须是对象"})
					return
				}
				for ak := range aq {
					if !settingsToolsetKeyKnown(ak) {
						writeJSON(w, 400, map[string]any{"ok": false, "error": "不允许的设置项 " + ak})
						return
					}
				}
			}
		}
	}
	bodyBytes, err := json.Marshal(raw)
	if err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "无效 JSON"})
		return
	}
	var body settingsUpdateBody
	if err := json.Unmarshal(bodyBytes, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "无效 JSON"})
		return
	}
	patch := map[string]any{}
	if body.CollapsedEditBlocks != nil {
		patch["collapsed_edit_blocks"] = *body.CollapsedEditBlocks
	}
	if body.PageFlipOnSend != nil {
		patch["page_flip_on_send"] = *body.PageFlipOnSend
	}
	if body.RememberToolApprovals != nil {
		patch["remember_tool_approvals"] = *body.RememberToolApprovals
	}
	if body.PermissionMode != nil {
		patch["permission_mode"] = *body.PermissionMode
	}
	if body.FollowUpBehavior != nil {
		patch["follow_up_behavior"] = *body.FollowUpBehavior
	}
	if body.ToolCallDensity != nil {
		patch["tool_call_density"] = *body.ToolCallDensity
	}
	toolsetPatch := map[string]any{}
	if body.Toolset != nil && body.Toolset.AskUserQuestion != nil {
		for k, v := range *body.Toolset.AskUserQuestion {
			// Values are re-validated (bool / 1–86400 int) by
			// acp.SetToolsetSettings before anything touches disk.
			toolsetPatch[k] = v
		}
	}
	if len(patch) == 0 && len(toolsetPatch) == 0 {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "需要至少一个设置项"})
		return
	}
	if len(patch) > 0 {
		if err := s.bridge.SetUiSettings(patch); err != nil {
			writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
			return
		}
	}
	if len(toolsetPatch) > 0 {
		if err := s.bridge.SetToolsetSettings(toolsetPatch); err != nil {
			writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
			return
		}
	}
	out := s.settingsPayload()
	out["ok"] = true
	writeJSON(w, 200, out)
}

func (s *Server) settingsTOMLPath() string {
	if s.bridge != nil {
		if p, err := s.bridge.ConfigTOMLPath(); err == nil && p != "" {
			return p
		}
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".grok", "config.toml")
}

func (s *Server) settingsPayload() map[string]any {
	path := s.settingsTOMLPath()
	if path == "" {
		return map[string]any{}
	}
	sections := parseConfigTOML(path)
	out := map[string]any{}
	for name, kv := range sections {
		// Custom-model tables carry API keys and are edited through the
		// 自定义模型 panel's own endpoints; [toolset.ask_user_question] is
		// folded into `toolset` below so it is not dumped twice.
		if isCustomModelTable(name) || name == "toolset.ask_user_question" || len(kv) == 0 {
			continue
		}
		if name == "toolset" {
			// Copy: the ask_user_question merge below must not mutate the
			// parsed section map.
			merged := make(map[string]any, len(kv)+1)
			for k, v := range kv {
				merged[k] = v
			}
			out[name] = merged
			continue
		}
		out[name] = kv
	}
	// [toolset.ask_user_question] — exposed under `toolset` with ONLY the
	// two FE-consumed timeout scalars; the raw table is not dumped.
	if aq, ok := sections["toolset.ask_user_question"]; ok {
		filtered := map[string]any{}
		for _, k := range []string{"timeout_enabled", "timeout_secs"} {
			if v, has := aq[k]; has {
				filtered[k] = v
			}
		}
		if len(filtered) > 0 {
			ts, _ := out["toolset"].(map[string]any)
			if ts == nil {
				ts = map[string]any{}
			}
			ts["ask_user_question"] = filtered
			out["toolset"] = ts
		}
	}
	return out
}

// isCustomModelTable reports whether a config.toml table path is a custom-model
// definition ([model] or [model.<id>...]). `[models]` is the catalog table and
// stays visible.
func isCustomModelTable(name string) bool {
	return name == "model" || strings.HasPrefix(name, "model.")
}
