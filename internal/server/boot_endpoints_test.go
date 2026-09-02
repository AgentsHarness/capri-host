package server

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-host/internal/config"
)

// ── 启动期身份端点：/api/hosts（免鉴权）与 /api/probe（需鉴权）──────────
//
// 前端只读 /api/hosts 这一份应答就要认出「送我这页的是哪台 host、它挂在
// 哪个 hub 上」，于是 host 的部署形态必须出现在这个永远不 401 的端点里；
// /api/probe 则专门用来回答「浏览器手里这把钥匙开不开得了这台」。

// newCfgServer builds a fake-agent server over an explicit Config (the
// access-token harness only varies AccessToken).
func newCfgServer(t *testing.T, cfg config.Config) *Server {
	t.Helper()
	t.Setenv(ACPHostFakeAgentEnv, "1")
	b := acp.NewBridge(acp.GrokConfig{
		Bin:             os.Args[0],
		HostID:          "mba",
		HostName:        "MacBook Air",
		LastSessionFile: filepath.Join(t.TempDir(), "last-session.json"),
	})
	t.Cleanup(b.Shutdown)
	cfg.GrokBin = "grok"
	cfg.Port = 8765
	return New(cfg, b)
}

// /api/hosts 带着 mode / hubUrl / hostId / port：配了 HUB_URL 的 host，
// 浏览器即使一把 host 钥匙都没有，也能凭这份应答升到 hub（局域网 IP 打开
// 内嵌前端的路径全靠它——那些请求此前会被 /api/status 的 401 挡回 local）。
func TestHostsEndpointCarriesDeployment(t *testing.T) {
	s := newCfgServer(t, config.Config{
		HubURL:      "https://hub.example",
		AccessToken: testToken,
	})

	rec := request(t, s, "GET", "/api/hosts", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/hosts = %d, want 200 (host must never 401 here)", rec.Code)
	}
	m := decodeBody(t, rec)
	if m["mode"] != "hub" {
		t.Errorf("mode = %v, want %q", m["mode"], "hub")
	}
	if m["hubUrl"] != "https://hub.example" {
		t.Errorf("hubUrl = %v, want https://hub.example", m["hubUrl"])
	}
	if m["hostId"] != "mba" {
		t.Errorf("hostId = %v, want mba", m["hostId"])
	}
	if port, ok := m["port"].(float64); !ok || int(port) != 8765 {
		t.Errorf("port = %v, want 8765", m["port"])
	}
	if m["authRequired"] != true {
		t.Errorf("authRequired = %v, want true", m["authRequired"])
	}
	hosts, ok := m["hosts"].([]any)
	if !ok || len(hosts) != 1 {
		t.Fatalf("hosts = %v, want exactly one entry", m["hosts"])
	}
	entry, _ := hosts[0].(map[string]any)
	if entry["local"] != true || entry["hostId"] != "mba" {
		t.Errorf("hosts[0] = %v, want local:true with hostId mba", hosts[0])
	}
}

// 没配 HUB_URL 的纯本机 host：mode=local 且不带 hubUrl 键（前端按「有没有
// mode 字段」判这是 host 还是 hub，按 hubUrl 是否非空决定要不要升 hub）。
func TestHostsEndpointLocalDeployment(t *testing.T) {
	s := newCfgServer(t, config.Config{})

	m := decodeBody(t, request(t, s, "GET", "/api/hosts", "", ""))
	if m["mode"] != "local" {
		t.Errorf("mode = %v, want %q", m["mode"], "local")
	}
	if _, ok := m["hubUrl"]; ok {
		t.Errorf("hubUrl = %v, want the key absent when HUB_URL is unset", m["hubUrl"])
	}
}

// /api/probe 落在常规门禁里：空手 401，带对钥匙 200 且自报 hostId——这两
// 个答案就是近路「先探再问」的全部判据。
func TestProbeEndpointIsGatedAndIdentifies(t *testing.T) {
	s := newCfgServer(t, config.Config{
		HubURL:      "https://hub.example",
		AccessToken: testToken,
	})

	if rec := request(t, s, "GET", "/api/probe", "", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET /api/probe without token = %d, want 401", rec.Code)
	}
	if rec := request(t, s, "GET", "/api/probe", "Bearer wrong-token-x", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET /api/probe with wrong token = %d, want 401", rec.Code)
	}

	rec := request(t, s, "GET", "/api/probe", "Bearer "+testToken, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/probe with token = %d, want 200", rec.Code)
	}
	m := decodeBody(t, rec)
	if m["ok"] != true || m["hostId"] != "mba" || m["mode"] != "hub" {
		t.Errorf("probe body = %v, want ok/hostId=mba/mode=hub", m)
	}
	if m["hubUrl"] != "https://hub.example" {
		t.Errorf("hubUrl = %v, want https://hub.example", m["hubUrl"])
	}
}

// 没设 FE_TOKEN 的 host 上 /api/probe 依然开放（withAuth 的既有语义），
// 且回 local —— 探路在两种部署下都不需要额外分支。
func TestProbeEndpointOpenWithoutToken(t *testing.T) {
	s := newCfgServer(t, config.Config{})

	rec := request(t, s, "GET", "/api/probe", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/probe on open host = %d, want 200", rec.Code)
	}
	if m := decodeBody(t, rec); m["mode"] != "local" {
		t.Errorf("mode = %v, want %q", m["mode"], "local")
	}
}
