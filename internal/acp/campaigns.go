package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ── 默认模型 campaign dismiss（TUI persist_models_default 语义）───────────
//
// xAI 通过 remote settings 下发「campaigns」活动（如 grok-4.6 发布推广），
// 会把 effective config 的 models.default 临时覆盖成活动值。agent 的
// session/new 优先解析活动值（campaign_driven_models_default），因此用户
// 写入 config.toml 的默认模型对新建会话不生效——「配置变了但 agent 没变」。
//
// TUI 的解法（xai-grok-shell util/config/campaigns.rs persist_user_choice）：
// 用户选择模型时先把触及 models.default 的活动 id 写入
// $GROK_HOME/campaigns_state.json 的 dismissed_ids（"user pick wins,
// forever"），再写配置。capri-host 在 /api/set-default-model 复刻同一语义：
// 写 config.toml 前 dismiss 活动，agent 下次 reload / 新建会话即按用户
// 选择解析。
//
// 全程 best-effort：拉取/解析/写盘任一失败都只返回错误、由调用方记日志，
// 绝不阻断配置写入（TUI 同款：bookkeeping failure is logged and the
// write proceeds）。

// remoteSettingsURL returns the cli-chat-proxy remote-settings endpoint
// (env-overridable for tests / custom proxies).
func (b *Bridge) remoteSettingsURL() string {
	if u := os.Getenv("ACP_REMOTE_SETTINGS_URL"); u != "" {
		return u
	}
	return "https://cli-chat-proxy.grok.com/v1/settings"
}

// authTokenFromGrokHome reads the first usable bearer token from
// $GROK_HOME/auth.json — shape {"<auth-url>": {"key": "<jwt>", ...}, ...}.
// Empty when missing/unparsable.
func (b *Bridge) authTokenFromGrokHome() string {
	home := b.grokHome()
	if home == "" {
		return ""
	}
	raw, err := os.ReadFile(filepath.Join(home, "auth.json"))
	if err != nil {
		return ""
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return ""
	}
	for _, v := range m {
		entry, ok := v.(map[string]any)
		if !ok {
			continue
		}
		if key, _ := entry["key"].(string); key != "" {
			return key
		}
	}
	return ""
}

// campaignTouchesModelsDefault reports whether a remote campaign entry's
// patch touches `["models","default"]`. Wire shape:
// {"id": "grok-4.6-launch", "models": {"default": "grok-4.6"}} — other
// patch keys may coexist; a nested `patch` object is also accepted.
func campaignTouchesModelsDefault(entry map[string]any) bool {
	patch, ok := entry["patch"].(map[string]any)
	if !ok {
		patch = entry
	}
	models, _ := patch["models"].(map[string]any)
	if models == nil {
		return false
	}
	_, has := models["default"]
	return has
}

// dismissableModelCampaignIDs extracts ids of remote campaigns whose patch
// touches models.default (mirror of the agent's ids_touching_paths).
func dismissableModelCampaignIDs(campaigns []any) []string {
	var ids []string
	for _, raw := range campaigns {
		entry, ok := raw.(map[string]any)
		if !ok || !campaignTouchesModelsDefault(entry) {
			continue
		}
		id, _ := entry["id"].(string)
		if id = strings.TrimSpace(id); id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

// campaignsStatePath returns $GROK_HOME/campaigns_state.json.
func (b *Bridge) campaignsStatePath() (string, error) {
	home := b.grokHome()
	if home == "" {
		return "", errors.New("grok home 未知，无法定位 campaigns_state.json")
	}
	return filepath.Join(home, "campaigns_state.json"), nil
}

// readCampaignsState loads the raw table and dismissed_ids (missing file =
// empty table + empty ids). Unknown keys survive the round-trip.
func readCampaignsState(path string) (map[string]any, []string, error) {
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return map[string]any{}, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var table map[string]any
	if err := json.Unmarshal(raw, &table); err != nil {
		return nil, nil, err
	}
	var ids []string
	switch v := table["dismissed_ids"].(type) {
	case []any:
		for _, x := range v {
			if s, ok := x.(string); ok {
				ids = append(ids, s)
			}
		}
	case []string:
		ids = v
	}
	return table, ids, nil
}

// dismissCampaignIds appends ids to $GROK_HOME/campaigns_state.json
// dismissed_ids (atomic temp+rename; flock'd against the agent's own
// dismiss writer). No-op when every id is already dismissed.
func (b *Bridge) dismissCampaignIds(ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	path, err := b.campaignsStatePath()
	if err != nil {
		return err
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	// Advisory lock on campaigns_state.json.lock (the agent uses the same
	// lock file), best-effort: a lock failure still proceeds.
	release := lockCampaignsState(path)
	defer release()

	table, existing, err := readCampaignsState(path)
	if err != nil {
		return err
	}
	seen := make(map[string]bool, len(existing))
	for _, id := range existing {
		seen[id] = true
	}
	var added []string
	for _, id := range ids {
		if !seen[id] {
			existing = append(existing, id)
			seen[id] = true
			added = append(added, id)
		}
	}
	if len(added) == 0 {
		return nil
	}
	table["dismissed_ids"] = existing
	raw, err := json.Marshal(table)
	if err != nil {
		return err
	}
	var mode os.FileMode = 0o600
	if st, err := os.Stat(path); err == nil {
		mode = st.Mode().Perm()
	}
	tmp := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())
	if err := os.WriteFile(tmp, raw, mode); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	log.Printf("[capri-host] 已 dismiss 活动 campaign（models.default 覆盖解除，用户默认生效）: %v", added)
	return nil
}

// DismissModelDefaultCampaigns fetches remote settings and dismisses every
// active campaign that patches models.default, so the user's
// [models].default choice wins for new sessions (TUI persist_user_choice
// parity). Best-effort by contract: any failure returns an error and the
// caller must log it and proceed with the config write.
func (b *Bridge) DismissModelDefaultCampaigns(ctx context.Context) error {
	token := b.authTokenFromGrokHome()
	if token == "" {
		return errors.New("无可用 auth token（auth.json 缺失或未解析），跳过 campaign dismiss")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.remoteSettingsURL(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", "capri-host/1.0")
	client := &http.Client{Timeout: 8 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("remote settings 返回 %s", resp.Status)
	}
	var payload struct {
		Campaigns []any `json:"campaigns"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil {
		return err
	}
	ids := dismissableModelCampaignIDs(payload.Campaigns)
	if len(ids) == 0 {
		return nil
	}
	return b.dismissCampaignIds(ids)
}
