package acp

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/config"
)

// spawnEnvRecorder 是一个假 grok：把子进程实际拿到的代理变量写进文件再退出。
// 用于验证 SpawnEnv 真的作用到了 agent 进程，而不是只落在 Bridge 字段上。
// 输出路径写死在脚本里——host 传的参数是 "agent stdio"，不能拿来当路径。
const spawnEnvRecorder = `#!/bin/sh
{
  echo "HTTPS_PROXY=$HTTPS_PROXY"
  echo "https_proxy=$https_proxy"
  echo "ALL_PROXY=$ALL_PROXY"
  echo "NO_PROXY=$NO_PROXY"
} > '%s'
`

func writeRecorder(t *testing.T) (bin, out string) {
	t.Helper()
	dir := t.TempDir()
	bin = filepath.Join(dir, "fake-grok")
	out = filepath.Join(dir, "env.txt")
	script := fmt.Sprintf(spawnEnvRecorder, out)
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, out
}

// waitForFile 等假 agent 落盘（进程启动与写文件都在另一个进程里）。
func waitForFile(t *testing.T, path string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil && len(b) > 0 {
			return string(b)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("agent 没有写出环境变量快照: %s", path)
	return ""
}

// Agent 代理关闭时，子进程必须拿到空的代理变量——覆盖从 host 继承来的值。
// 这条保护的是「关掉开关但 agent 仍偷偷走代理」这个真实失败模式。
func TestSpawnEnvClearsInheritedProxy(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell 脚本假 agent 只在类 unix 上跑")
	}
	t.Setenv("HTTPS_PROXY", "http://inherited:1")
	t.Setenv("https_proxy", "http://inherited:1")
	t.Setenv("ALL_PROXY", "http://inherited:1")

	bin, out := writeRecorder(t)
	b := NewBridge(GrokConfig{
		Bin:      bin,
		SpawnEnv: []string{"HTTPS_PROXY=", "https_proxy=", "ALL_PROXY=", "NO_PROXY="},
	})
	// 假 agent 不实现 JSON-RPC，ensureProcess 起完进程即可（后续读会失败）。
	_ = b.ensureProcess()
	t.Cleanup(func() { b.Shutdown() })

	got := waitForFile(t, out)
	for _, line := range strings.Split(strings.TrimSpace(got), "\n") {
		if strings.HasSuffix(line, "=http://inherited:1") {
			t.Errorf("agent 仍带着继承来的代理: %s", line)
		}
	}
}

// Agent 代理开启时不覆盖：子进程继承 host 的代理环境变量。
func TestSpawnEnvEmptyKeepsInheritedProxy(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell 脚本假 agent 只在类 unix 上跑")
	}
	t.Setenv("HTTPS_PROXY", "http://proxy.local:7890")

	bin, out := writeRecorder(t)
	b := NewBridge(GrokConfig{Bin: bin})
	_ = b.ensureProcess()
	t.Cleanup(func() { b.Shutdown() })

	got := waitForFile(t, out)
	if !strings.Contains(got, "HTTPS_PROXY=http://proxy.local:7890") {
		t.Errorf("agent 没继承 host 的代理: %s", got)
	}
}

// 代理开关在 host 环境里没有代理变量时也要生效：菜单栏起的 host 只有
// config.json 里的地址，AgentEnv 是唯一能把它交给子进程的路径。
func TestSpawnEnvCarriesConfiguredProxy(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell 脚本假 agent 只在类 unix 上跑")
	}
	for _, k := range []string{"HTTPS_PROXY", "https_proxy", "ALL_PROXY", "NO_PROXY"} {
		t.Setenv(k, "")
	}

	bin, out := writeRecorder(t)
	cfg := config.Config{Proxy: "http://127.0.0.1:7890", NoProxy: "localhost"}
	b := NewBridge(GrokConfig{Bin: bin, SpawnEnv: cfg.AgentEnv()})
	_ = b.ensureProcess()
	t.Cleanup(func() { b.Shutdown() })

	got := waitForFile(t, out)
	for _, want := range []string{
		"HTTPS_PROXY=http://127.0.0.1:7890",
		"https_proxy=http://127.0.0.1:7890",
		"ALL_PROXY=http://127.0.0.1:7890",
		"NO_PROXY=localhost",
	} {
		if !strings.Contains(got, want+"\n") && !strings.HasSuffix(got, want) {
			t.Errorf("agent 环境缺少 %q: %s", want, got)
		}
	}
}

// 关掉 agent 代理时，AgentEnv 的空值覆盖要压掉 config 里的地址。
func TestSpawnEnvClearsConfiguredProxyWhenOff(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell 脚本假 agent 只在类 unix 上跑")
	}
	t.Setenv("HTTPS_PROXY", "http://inherited:1")

	bin, out := writeRecorder(t)
	off := false
	cfg := config.Config{Proxy: "http://127.0.0.1:7890", AgentProxy: &off}
	b := NewBridge(GrokConfig{Bin: bin, SpawnEnv: cfg.AgentEnv()})
	_ = b.ensureProcess()
	t.Cleanup(func() { b.Shutdown() })

	got := waitForFile(t, out)
	if strings.Contains(got, "7890") || strings.Contains(got, "inherited:1") {
		t.Errorf("agent 关掉代理后仍带着代理地址: %s", got)
	}
}
