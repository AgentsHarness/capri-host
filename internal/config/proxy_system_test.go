package config

import (
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

const scutilHTTPAndSOCKS = `<dictionary> {
  ExceptionsList : <array> {
    0 : localhost
    1 : *.local
    2 : agents.benins.cn
  }
  FTPPassive : 1
  HTTPEnable : 1
  HTTPPort : 7890
  HTTPProxy : 127.0.0.1
  HTTPSEnable : 1
  HTTPSPort : 7890
  HTTPSProxy : 127.0.0.1
  ProxyAutoConfigEnable : 0
  SOCKSEnable : 1
  SOCKSPort : 7890
  SOCKSProxy : 127.0.0.1
}
`

func TestParseScutilPrefersHTTPS(t *testing.T) {
	p, np := parseScutilProxy(scutilHTTPAndSOCKS)
	if p != "http://127.0.0.1:7890" {
		t.Fatalf("proxy = %q", p)
	}
	if np != "localhost,*.local,agents.benins.cn" {
		t.Fatalf("no_proxy = %q", np)
	}
}

func TestParseScutilSOCKSOnly(t *testing.T) {
	raw := `<dictionary> {
  HTTPEnable : 0
  HTTPSEnable : 0
  SOCKSEnable : 1
  SOCKSPort : 1080
  SOCKSProxy : 127.0.0.1
}
`
	p, np := parseScutilProxy(raw)
	if p != "socks5://127.0.0.1:1080" {
		t.Fatalf("proxy = %q", p)
	}
	if np != "" {
		t.Fatalf("no_proxy = %q, want empty", np)
	}
}

func TestParseScutilAuthAndDisabled(t *testing.T) {
	raw := `<dictionary> {
  HTTPSEnable : 1
  HTTPSPort : 8080
  HTTPSProxy : proxy.example
  HTTPSUser : alice
  HTTPSPassword : s3cret
}
`
	p, _ := parseScutilProxy(raw)
	if p != "http://alice:s3cret@proxy.example:8080" {
		t.Fatalf("proxy = %q", p)
	}
	p, np := parseScutilProxy(`<dictionary> {
  HTTPEnable : 0
  HTTPSEnable : 0
  SOCKSEnable : 0
  ProxyAutoConfigEnable : 1
  ProxyAutoConfigURLString : http://pac.example/proxy.pac
}
`)
	if p != "" || np != "" {
		t.Fatalf("PAC-only → %q %q, want empty", p, np)
	}
}

func TestWithSystemProxyFillsWhenUnset(t *testing.T) {
	orig := lookupSystemProxy
	t.Cleanup(func() { lookupSystemProxy = orig })
	lookupSystemProxy = func() (string, string) {
		return "http://127.0.0.1:7890", "localhost,agents.benins.cn"
	}

	got := (Config{}).WithSystemProxy()
	if got.Proxy != "http://127.0.0.1:7890" {
		t.Fatalf("Proxy = %q", got.Proxy)
	}
	if got.NoProxy != "localhost,agents.benins.cn" {
		t.Fatalf("NoProxy = %q", got.NoProxy)
	}
	if !got.proxyFromSystem {
		t.Fatal("proxyFromSystem should be set")
	}
	desc := got.ProxyDescription()
	if !strings.Contains(desc, "系统网络设置") || !strings.Contains(desc, "http://127.0.0.1:7890") {
		t.Fatalf("ProxyDescription = %q", desc)
	}

	kept := Config{Proxy: "http://custom:1", NoProxy: "only-me"}.WithSystemProxy()
	if kept.Proxy != "http://custom:1" || kept.NoProxy != "only-me" || kept.proxyFromSystem {
		t.Fatalf("explicit config was overwritten: %+v", kept)
	}

	npKept := Config{NoProxy: "only-me"}.WithSystemProxy()
	if npKept.NoProxy != "only-me" {
		t.Fatalf("explicit no_proxy was overwritten: %q", npKept.NoProxy)
	}
}

func TestWithSystemProxyNoopWhenSystemOff(t *testing.T) {
	orig := lookupSystemProxy
	t.Cleanup(func() { lookupSystemProxy = orig })
	lookupSystemProxy = func() (string, string) { return "", "" }
	got := (Config{}).WithSystemProxy()
	if got.Proxy != "" || got.proxyFromSystem {
		t.Fatalf("expected empty, got %+v", got)
	}
}

func TestParseLiveScutilDarwin(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("scutil is macOS-only")
	}
	out, err := exec.Command("/usr/sbin/scutil", "--proxy").Output()
	if err != nil {
		t.Fatalf("scutil --proxy: %v", err)
	}
	p, np := parseScutilProxy(string(out))
	raw := string(out)
	if strings.Contains(raw, "HTTPSEnable : 1") || strings.Contains(raw, "HTTPEnable : 1") {
		if p == "" {
			t.Fatalf("system HTTP(S) proxy is on but parse returned empty\n%s", raw)
		}
	}
	if strings.Contains(raw, "ExceptionsList") && !strings.Contains(raw, "ExceptionsList : <array> {\n  }") {
		// 有排除项时 parse 应捞到至少一项；空数组则允许空。
		if strings.Contains(raw, "0 : ") && np == "" {
			t.Fatalf("ExceptionsList had entries but no_proxy empty\n%s", raw)
		}
	}
}
