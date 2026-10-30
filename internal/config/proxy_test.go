package config

import (
	"os"
	"testing"
)

// 代理可以只在 config.json 里写，且 host:port 这种简写要补成 URL。
func TestProxyURLNormalizes(t *testing.T) {
	cases := []struct {
		env, file, want string
	}{
		{"", "", ""},
		{"", "127.0.0.1:7890", "http://127.0.0.1:7890"},
		{"", "http://127.0.0.1:7890", "http://127.0.0.1:7890"},
		{"", "socks5://127.0.0.1:1080", "socks5://127.0.0.1:1080"},
		{"", "  http://p:1  ", "http://p:1"},
		{"http://env:1", "http://file:2", "http://env:1"}, // 环境变量优先
		{"", "http://user:pw@proxy.local:8080", "http://user:pw@proxy.local:8080"},
	}
	for _, tc := range cases {
		if got := proxyURL(tc.env, tc.file); got != tc.want {
			t.Errorf("proxyURL(%q, %q) = %q, want %q", tc.env, tc.file, got, tc.want)
		}
	}
}

func TestLoadProxyFromFile(t *testing.T) {
	isolateConfig(t)
	clearHostEnv(t)
	if err := SaveFile(File{Proxy: "127.0.0.1:7890", NoProxy: "localhost,127.0.0.1"}); err != nil {
		t.Fatal(err)
	}
	cfg := Load()
	if cfg.Proxy != "http://127.0.0.1:7890" {
		t.Fatalf("Proxy = %q", cfg.Proxy)
	}
	if cfg.NoProxy != "localhost,127.0.0.1" {
		t.Fatalf("NoProxy = %q", cfg.NoProxy)
	}
}

// 没有代理配置时不动环境：不写代理变量（菜单栏启动因此是直连），
// 父进程已经有的 HTTPS_PROXY 也不会被清掉。
func TestApplyProxyEnvNoopWhenUnset(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://from-parent:1")
	t.Setenv("https_proxy", "")
	Config{}.ApplyProxyEnv()
	if got := os.Getenv("HTTPS_PROXY"); got != "http://from-parent:1" {
		t.Fatalf("HTTPS_PROXY = %q, want parent value kept", got)
	}
	if got := os.Getenv("https_proxy"); got != "" {
		t.Fatalf("https_proxy = %q, want untouched", got)
	}
}

// 配了代理就大小写各写一套，agent 与 Go net/http 都能读到。
func TestApplyProxyEnvSetsBothCases(t *testing.T) {
	for _, k := range proxyEnvKeys {
		t.Setenv(k, "")
	}
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")

	Config{Proxy: "http://127.0.0.1:7890", NoProxy: "localhost"}.ApplyProxyEnv()
	for _, k := range proxyEnvKeys {
		if got := os.Getenv(k); got != "http://127.0.0.1:7890" {
			t.Errorf("%s = %q, want proxy URL", k, got)
		}
	}
	if got := os.Getenv("NO_PROXY"); got != "localhost" {
		t.Errorf("NO_PROXY = %q", got)
	}
	if got := os.Getenv("no_proxy"); got != "localhost" {
		t.Errorf("no_proxy = %q", got)
	}
}

// 两个开关各自只影响自己那一侧：关掉 Hub 代理时 Hub 侧直连，但 Agent
// 仍拿到代理地址；反过来同理。这是「开关可独立控制」的核心约束。
func TestProxySwitchesAreIndependent(t *testing.T) {
	off := false
	cfg := Config{Proxy: "http://127.0.0.1:7890", HubProxy: &off}
	if got := cfg.HubProxyURL(); got != "" {
		t.Errorf("hub proxy off: HubProxyURL = %q, want empty", got)
	}
	if got := cfg.AgentProxyURL(); got != "http://127.0.0.1:7890" {
		t.Errorf("hub proxy off must not affect agent, got %q", got)
	}
	// Agent 侧开着代理：AgentEnv 必须把地址交给子进程，而不是指望它从
	// host 进程继承（菜单栏起的 host 环境里没有代理变量）。
	env := cfg.AgentEnv()
	for _, k := range proxyEnvKeys {
		if !contains(env, k+"=http://127.0.0.1:7890") {
			t.Errorf("hub proxy off: AgentEnv missing %s: %v", k, env)
		}
	}

	agentOff := false
	cfg = Config{Proxy: "http://127.0.0.1:7890", AgentProxy: &agentOff}
	if got := cfg.AgentProxyURL(); got != "" {
		t.Errorf("agent proxy off: AgentProxyURL = %q, want empty", got)
	}
	if got := cfg.HubProxyURL(); got != "http://127.0.0.1:7890" {
		t.Errorf("agent proxy off must not affect hub, got %q", got)
	}
	agentOffEnv := cfg.AgentEnv()
	if !contains(agentOffEnv, "HTTPS_PROXY=") || !contains(agentOffEnv, "https_proxy=") {
		t.Errorf("agent proxy off: overrides missing proxy vars: %v", agentOffEnv)
	}
}

// 两个都关：两侧都直连。
func TestProxyBothOff(t *testing.T) {
	off := false
	cfg := Config{Proxy: "http://127.0.0.1:7890", AgentProxy: &off, HubProxy: &off}
	if cfg.AgentProxyURL() != "" || cfg.HubProxyURL() != "" {
		t.Fatalf("both off must be direct: agent=%q hub=%q", cfg.AgentProxyURL(), cfg.HubProxyURL())
	}
}

// 缺省（未写配置）跟随系统代理：WithSystemProxy 填上的地址两侧都生效。
func TestProxyDefaultsFollowSystemProxy(t *testing.T) {
	orig := lookupSystemProxy
	t.Cleanup(func() { lookupSystemProxy = orig })
	lookupSystemProxy = func() (string, string) { return "http://127.0.0.1:7890", "localhost" }

	cfg := Config{}.WithSystemProxy()
	if cfg.Proxy == "" {
		t.Fatal("system proxy should fill an unset Proxy")
	}
	if cfg.AgentProxyURL() != cfg.Proxy || cfg.HubProxyURL() != cfg.Proxy {
		t.Fatalf("defaults must apply to both sides: agent=%q hub=%q", cfg.AgentProxyURL(), cfg.HubProxyURL())
	}
}

// 两者都开时 AgentEnv 把代理地址交给子进程——host 自身环境里可能没有
// 代理变量（菜单栏启动），子进程不能只靠继承。
func TestAgentEnvCarriesProxyWhenOn(t *testing.T) {
	cfg := Config{Proxy: "http://127.0.0.1:7890"}
	env := cfg.AgentEnv()
	for _, k := range proxyEnvKeys {
		if !contains(env, k+"=http://127.0.0.1:7890") {
			t.Errorf("AgentEnv missing %s: %v", k, env)
		}
	}
	// 没配 NoProxy 时不写 NO_PROXY，免得压掉继承来的排除列表。
	if contains(env, "NO_PROXY=") || contains(env, "no_proxy=") {
		t.Errorf("AgentEnv wrote empty NO_PROXY with proxy on: %v", env)
	}
}

// 配了 NoProxy 就一起交给子进程，大小写各一份。
func TestAgentEnvCarriesNoProxy(t *testing.T) {
	cfg := Config{Proxy: "http://127.0.0.1:7890", NoProxy: "localhost,127.0.0.1"}
	env := cfg.AgentEnv()
	if !contains(env, "NO_PROXY=localhost,127.0.0.1") || !contains(env, "no_proxy=localhost,127.0.0.1") {
		t.Fatalf("AgentEnv missing NO_PROXY: %v", env)
	}
}

// 没配代理地址时 Close 不做无谓覆盖：AgentEnv 仍注入空值，避免从
// launchd 继承来的 HTTPS_PROXY 让 agent 偷偷走代理。
func TestAgentEnvClearsInheritedProxyWithoutConfig(t *testing.T) {
	clearHostEnv(t)
	cfg := Load()
	if env := cfg.AgentEnv(); !contains(env, "HTTPS_PROXY=") {
		t.Fatalf("AgentEnv = %v, want HTTPS_PROXY override", env)
	}
}

func contains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

// 日志里的代理地址不能带出密码。
func TestProxyDescriptionRedactsPassword(t *testing.T) {
	if got := (Config{}).ProxyDescription(); got != "" {
		t.Fatalf("no proxy → %q, want empty", got)
	}
	got := (Config{Proxy: "http://user:s3cret@proxy.local:8080"}).ProxyDescription()
	if got != "http://user:***@proxy.local:8080" {
		t.Fatalf("ProxyDescription = %q", got)
	}
	if got := (Config{Proxy: "http://p:1", NoProxy: "localhost"}).ProxyDescription(); got != "http://p:1（NO_PROXY=localhost）" {
		t.Fatalf("ProxyDescription with NO_PROXY = %q", got)
	}
	sys := Config{Proxy: "http://p:1", NoProxy: "localhost", proxyFromSystem: true}
	if got := sys.ProxyDescription(); got != "http://p:1（系统网络设置；NO_PROXY=localhost）" {
		t.Fatalf("ProxyDescription from system = %q", got)
	}
}
