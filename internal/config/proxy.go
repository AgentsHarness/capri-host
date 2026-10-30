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

// ApplyProxyEnv 把配置里的代理导出成标准环境变量，让本进程以及它拉起的
// grok agent 都走代理。保留给不走菜单栏的装配方（命令行 / launchd）。
//
// 菜单栏应用起的 host 不用它：Agent 走 AgentEnv（子进程环境），Hub 走
// hub.Config.Proxy（自己的 transport），两个开关因此互不影响。
//
// Proxy 为空时是 no-op：不写环境变量。
func (c Config) ApplyProxyEnv() {
	if p := strings.TrimSpace(c.Proxy); p != "" {
		c.setProxyEnv(p)
	}
}

// AgentEnv 返回 agent 子进程要叠加的环境变量，两个方向都由它决定：开着
// 代理就给代理地址，关掉就给空值覆盖。不能指望 host 进程自己的环境：菜单
// 栏起的 host 没有代理变量，而关掉开关时又必须压掉从 launchd 继承来的值。
func (c Config) AgentEnv() []string {
	env := make([]string, 0, len(proxyEnvKeys)+2)
	url := c.AgentProxyURL()
	for _, k := range proxyEnvKeys {
		env = append(env, k+"="+url)
	}
	if url == "" {
		return append(env, "NO_PROXY=", "no_proxy=")
	}
	if np := strings.TrimSpace(c.NoProxy); np != "" {
		env = append(env, "NO_PROXY="+np, "no_proxy="+np)
	}
	return env
}

func (c Config) setProxyEnv(p string) {
	for _, k := range proxyEnvKeys {
		_ = os.Setenv(k, p)
	}
	if np := strings.TrimSpace(c.NoProxy); np != "" {
		_ = os.Setenv("NO_PROXY", np)
		_ = os.Setenv("no_proxy", np)
	}
}

// ClearProxyEnv 清掉进程里的代理变量。供需要「完全不读环境代理」的装配方
// 使用；菜单栏启动的 host 不依赖它（Hub 代理在 transport 层判定）。
func ClearProxyEnv() {
	for _, k := range proxyEnvKeys {
		_ = os.Unsetenv(k)
	}
}

// WithSystemProxy 在 Proxy 未手写时，用 macOS 系统网络设置里的
// HTTP/HTTPS/SOCKS 代理填上（PAC 自动配置不支持）。系统也没开则原样返回。
// 这是默认启动路径：没有配置就跟随系统代理。
func (c Config) WithSystemProxy() Config {
	if strings.TrimSpace(c.Proxy) != "" {
		return c
	}
	p, np := lookupSystemProxy()
	if p == "" {
		return c
	}
	c.Proxy = p
	c.proxyFromSystem = true
	if strings.TrimSpace(c.NoProxy) == "" {
		c.NoProxy = np
	}
	return c
}

// ProxyDescription 是给日志用的一行摘要：回显代理地址（带凭据时只保留
// 用户名，密码打码，日志可能被贴出去），无代理返回空串。
func (c Config) ProxyDescription() string {
	raw := strings.TrimSpace(c.Proxy)
	if raw == "" {
		return ""
	}
	p := redactProxy(raw)
	var extra []string
	if c.proxyFromSystem {
		extra = append(extra, "系统网络设置")
	}
	if np := strings.TrimSpace(c.NoProxy); np != "" {
		extra = append(extra, "NO_PROXY="+np)
	}
	if len(extra) == 0 {
		return p
	}
	return p + "（" + strings.Join(extra, "；") + "）"
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
