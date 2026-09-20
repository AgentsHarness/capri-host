package config

import (
	"os"
	"strings"
)

// proxyEnvKeys 是代理地址要写入的标准变量名：大小写各一套，因为读这些
// 变量的是两拨不同习惯的程序（Go net/http 大小写都认，agent 与常见的
// shell 工具多读小写）。
var proxyEnvKeys = []string{
	"HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY",
	"https_proxy", "http_proxy", "all_proxy",
}

// ApplyProxyEnv 把配置里的代理导出成标准环境变量，让本进程（hub 的
// WebSocket / HTTP 中继）以及它拉起的 grok agent 都走代理。
//
// Proxy 为空时是 no-op：不动环境，父进程（shell / launchd）原有的
// HTTPS_PROXY 等继续生效——配置文件只是给菜单栏应用这种没有 shell
// 环境的启动方式补一条通道。
func (c Config) ApplyProxyEnv() {
	p := strings.TrimSpace(c.Proxy)
	if p == "" {
		return
	}
	for _, k := range proxyEnvKeys {
		_ = os.Setenv(k, p)
	}
	if np := strings.TrimSpace(c.NoProxy); np != "" {
		_ = os.Setenv("NO_PROXY", np)
		_ = os.Setenv("no_proxy", np)
	}
}

// ProxyDescription 是给日志用的一行摘要：回显代理地址（带凭据时只保留
// 用户名，密码打码，日志可能被贴出去），无代理返回空串。
func (c Config) ProxyDescription() string {
	raw := strings.TrimSpace(c.Proxy)
	if raw == "" {
		return ""
	}
	p := redactProxy(raw)
	if np := strings.TrimSpace(c.NoProxy); np != "" {
		return p + "（NO_PROXY=" + np + "）"
	}
	return p
}

// redactProxy 把 user:password@ 里的密码换成 ***，其余原样保留（用字符串
// 拼接而不是 url.String()，后者会把 *** 转义成 %2A%2A%2A）。
func redactProxy(raw string) string {
	scheme, rest := "", raw
	if i := strings.Index(raw, "://"); i >= 0 {
		scheme, rest = raw[:i+3], raw[i+3:]
	}
	at := strings.LastIndex(rest, "@")
	if at < 0 {
		return raw
	}
	userinfo := rest[:at]
	colon := strings.Index(userinfo, ":")
	if colon < 0 {
		return raw
	}
	return scheme + userinfo[:colon] + ":***@" + rest[at+1:]
}
