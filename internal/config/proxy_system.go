package config

import (
	"context"
	"net"
	"net/url"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

// lookupSystemProxy 读操作系统的网络代理。测试里可以换成桩。
var lookupSystemProxy = lookupSystemProxyOS

func lookupSystemProxyOS() (proxy, noProxy string) {
	if runtime.GOOS != "darwin" {
		return "", ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "/usr/sbin/scutil", "--proxy").Output()
	if err != nil {
		return "", ""
	}
	return parseScutilProxy(string(out))
}

// parseScutilProxy 解析 `scutil --proxy` 的字典输出。优先 HTTPS，其次 HTTP，
// 再不行用 SOCKS；不支持自动配置脚本（PAC）。ExceptionsList 拼成 NO_PROXY。
func parseScutilProxy(raw string) (proxy, noProxy string) {
	fields := make(map[string]string, 16)
	var exceptions []string
	inExceptions := false
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if inExceptions {
			if line == "}" {
				inExceptions = false
				continue
			}
			if _, v, ok := splitScutilKV(line); ok && v != "" {
				exceptions = append(exceptions, v)
			}
			continue
		}
		k, v, ok := splitScutilKV(line)
		if !ok {
			continue
		}
		if k == "ExceptionsList" {
			inExceptions = true
			continue
		}
		if strings.HasPrefix(v, "<") {
			continue
		}
		fields[k] = v
	}
	return pickSystemProxyURL(fields), strings.Join(exceptions, ",")
}

func splitScutilKV(line string) (k, v string, ok bool) {
	i := strings.Index(line, " : ")
	if i < 0 {
		return "", "", false
	}
	return strings.TrimSpace(line[:i]), strings.TrimSpace(line[i+3:]), true
}

func pickSystemProxyURL(fields map[string]string) string {
	try := []struct {
		enable, host, port, user, pass, scheme string
	}{
		{"HTTPSEnable", "HTTPSProxy", "HTTPSPort", "HTTPSUser", "HTTPSPassword", "http"},
		{"HTTPEnable", "HTTPProxy", "HTTPPort", "HTTPUser", "HTTPPassword", "http"},
		{"SOCKSEnable", "SOCKSProxy", "SOCKSPort", "SOCKSUser", "SOCKSPassword", "socks5"},
	}
	for _, t := range try {
		if fields[t.enable] != "1" {
			continue
		}
		if p := buildProxyURL(t.scheme, fields[t.host], fields[t.port], fields[t.user], fields[t.pass]); p != "" {
			return p
		}
	}
	return ""
}

func buildProxyURL(scheme, host, port, user, pass string) string {
	host = strings.TrimSpace(host)
	port = strings.TrimSpace(port)
	if host == "" || port == "" {
		return ""
	}
	u := url.URL{Scheme: scheme, Host: net.JoinHostPort(host, port)}
	user = strings.TrimSpace(user)
	if user != "" {
		if pass != "" {
			u.User = url.UserPassword(user, pass)
		} else {
			u.User = url.User(user)
		}
	}
	return u.String()
}
