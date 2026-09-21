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
