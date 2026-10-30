package hub

import (
	"net/http"
	"testing"
)

// Hub 代理关掉时必须真直连：即使进程环境里有 HTTPS_PROXY，也不能被
// http.Transport 的默认 ProxyFromEnvironment 捡起来。这条保护的是
// 「关掉开关但中继仍走代理」这个真实失败模式。
func TestProxyFuncEmptyMeansDirect(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://inherited:1")
	req, _ := http.NewRequest("POST", "https://agents.example/api/pair", nil)
	if fn := proxyFunc("", ""); fn != nil {
		t.Fatal("empty proxy must return a nil func so the transport dials direct")
	}
	// 对照：环境默认行为确实会返回代理，证明确实是 nil 挡住了它。
	u, err := http.ProxyFromEnvironment(req)
	if err != nil || u == nil {
		t.Fatalf("env default proxy = %v, %v (want the inherited proxy to prove the contrast)", u, err)
	}
}

func TestProxyFuncUsesConfiguredProxy(t *testing.T) {
	req, _ := http.NewRequest("POST", "https://agents.example/api/pair", nil)
	u, err := proxyFunc("http://127.0.0.1:7890", "")(req)
	if err != nil {
		t.Fatal(err)
	}
	if u == nil || u.Host != "127.0.0.1:7890" {
		t.Fatalf("proxy = %v, want 127.0.0.1:7890", u)
	}
}

// 排除列表要按「本域 + 子域」生效，且不能用后缀误伤 agents.example.evil。
func TestProxyFuncHonoursNoProxy(t *testing.T) {
	cases := []struct {
		url       string
		wantProxy bool
	}{
		{"https://agents.example/x", false},
		{"https://sub.agents.example/x", false},
		{"https://other.example/x", true},
		{"https://agents.example.evil/x", true},
	}
	fn := proxyFunc("http://127.0.0.1:7890", ".agents.example")
	for _, tc := range cases {
		req, _ := http.NewRequest("GET", tc.url, nil)
		u, err := fn(req)
		if err != nil {
			t.Fatalf("%s: %v", tc.url, err)
		}
		if got := u != nil; got != tc.wantProxy {
			t.Errorf("%s: proxied = %v, want %v", tc.url, got, tc.wantProxy)
		}
	}
}

// 无效代理地址要退回直连，而不是让所有中继请求报错。
func TestProxyFuncInvalidURLFallsBackToDirect(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "")
	if fn := proxyFunc("://nope", ""); fn != nil {
		t.Fatal("invalid proxy must fall back to a nil func")
	}
}
