package acp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCampaignTouchesModelsDefault(t *testing.T) {
	cases := []struct {
		name  string
		entry map[string]any
		want  bool
	}{
		{
			name: "flat models.default",
			entry: map[string]any{
				"id":     "grok-4.6-launch",
				"models": map[string]any{"default": "grok-4.6"},
			},
			want: true,
		},
		{
			name: "nested patch",
			entry: map[string]any{
				"id":    "c2",
				"patch": map[string]any{"models": map[string]any{"default": "x"}},
			},
			want: true,
		},
		{
			name: "models without default",
			entry: map[string]any{
				"id":     "c3",
				"models": map[string]any{"web_search": "x"},
			},
			want: false,
		},
		{
			name:  "no models at all",
			entry: map[string]any{"id": "c4", "tips": []any{}},
			want: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := campaignTouchesModelsDefault(c.entry); got != c.want {
				t.Errorf("campaignTouchesModelsDefault = %v, want %v", got, c.want)
			}
		})
	}
}

func TestDismissableModelCampaignIDs(t *testing.T) {
	raw := []any{
		map[string]any{"id": "grok-4.6-launch", "models": map[string]any{"default": "grok-4.6"}},
		map[string]any{"id": "other-tips-campaign", "tips": []any{"hi"}},
		map[string]any{"id": "", "models": map[string]any{"default": "x"}}, // 无 id 忽略
		map[string]any{"id": "nested", "patch": map[string]any{"models": map[string]any{"default": "y"}}},
		"not-a-map",
	}
	ids := dismissableModelCampaignIDs(raw)
	if len(ids) != 2 || ids[0] != "grok-4.6-launch" || ids[1] != "nested" {
		t.Errorf("ids = %v, want [grok-4.6-launch nested]", ids)
	}
}

func TestDismissCampaignIdsMergeAndPreserve(t *testing.T) {
	b, cfgPath := tempGrokBridge(t)
	home := filepath.Dir(cfgPath)
	statePath := filepath.Join(home, "campaigns_state.json")
	writeTestConfig(t, statePath, `{"dismissed_ids":["grok-4.5-launch"],"future_key":1}`)

	if err := b.dismissCampaignIds([]string{"grok-4.6-launch", "grok-4.5-launch"}); err != nil {
		t.Fatalf("dismissCampaignIds: %v", err)
	}
	var table map[string]any
	raw, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &table); err != nil {
		t.Fatal(err)
	}
	ids, _ := table["dismissed_ids"].([]any)
	if len(ids) != 2 {
		t.Fatalf("dismissed_ids = %v, want 2 entries (deduped)", ids)
	}
	got := map[string]bool{}
	for _, id := range ids {
		s, _ := id.(string)
		got[s] = true
	}
	if !got["grok-4.5-launch"] || !got["grok-4.6-launch"] {
		t.Errorf("dismissed_ids = %v, want both ids", ids)
	}
	if _, ok := table["future_key"]; !ok {
		t.Errorf("unknown keys must survive the round-trip: %v", table)
	}
}

func TestDismissModelDefaultCampaignsEndToEnd(t *testing.T) {
	b, cfgPath := tempGrokBridge(t)
	home := filepath.Dir(cfgPath)
	// 种子 auth.json（fake token）与已有 dismiss 状态。
	authJSON := `{"https://auth.x.ai::test": {"key": "fake-token"}}`
	writeTestConfig(t, filepath.Join(home, "auth.json"), authJSON)
	statePath := filepath.Join(home, "campaigns_state.json")
	writeTestConfig(t, statePath, `{"dismissed_ids":["grok-4.5-launch"]}`)

	// 本地 settings 服务：一个活动覆盖 models.default，一个不相关。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fake-token" {
			t.Errorf("Authorization = %q, want Bearer fake-token", r.Header.Get("Authorization"))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"campaigns": []any{
				map[string]any{"id": "grok-4.6-launch", "models": map[string]any{"default": "grok-4.6"}},
				map[string]any{"id": "tips-campaign", "tips": []any{"hi"}},
			},
		})
	}))
	defer srv.Close()
	t.Setenv("ACP_REMOTE_SETTINGS_URL", srv.URL)

	if err := b.DismissModelDefaultCampaigns(context.Background()); err != nil {
		t.Fatalf("DismissModelDefaultCampaigns: %v", err)
	}
	raw, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	out := string(raw)
	if !strings.Contains(out, "grok-4.6-launch") {
		t.Errorf("campaigns_state.json missing grok-4.6-launch:\n%s", out)
	}
	if strings.Contains(out, "tips-campaign") {
		t.Errorf("unrelated campaign must NOT be dismissed:\n%s", out)
	}
	if !strings.Contains(out, "grok-4.5-launch") {
		t.Errorf("existing dismissal lost:\n%s", out)
	}
}

func TestDismissModelDefaultCampaignsNoTokenSkips(t *testing.T) {
	b, _ := tempGrokBridge(t) // 无 auth.json
	if err := b.DismissModelDefaultCampaigns(context.Background()); err == nil {
		t.Fatal("expected error when no auth token is available")
	}
}

func TestDismissModelDefaultCampaignsServerError(t *testing.T) {
	b, cfgPath := tempGrokBridge(t)
	home := filepath.Dir(cfgPath)
	writeTestConfig(t, filepath.Join(home, "auth.json"), `{"k":{"key":"t"}}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	t.Setenv("ACP_REMOTE_SETTINGS_URL", srv.URL)

	if err := b.DismissModelDefaultCampaigns(context.Background()); err == nil {
		t.Fatal("expected error on 500")
	}
	if _, err := os.Stat(filepath.Join(home, "campaigns_state.json")); !os.IsNotExist(err) {
		t.Errorf("campaigns_state.json must not be created on fetch failure")
	}
}
