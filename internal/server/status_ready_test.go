package server

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/AgentsHarness/capri-host/internal/acp"
	"github.com/AgentsHarness/capri-host/internal/config"
)

// 空机（boot 完成、一台会话都没有）也要报 ready：客户端把 Status.Ready
// 当作「agent 可用」。只报 b.ready（仅会话建立/加载后置位）的话，新配对的
// 空机在 FE 上会一直停在「连接中 / 启动中」。
func TestStatusReadyOnSessionlessBoot(t *testing.T) {
	s, b := newFakeAgentServer(t)
	if err := b.Boot(context.Background()); err != nil {
		t.Fatalf("boot: %v", err)
	}

	// 前提：这一台确实没有会话，且 boot 已经收口。
	m := decodeBody(t, getJSON(t, s, "/api/status"))
	if sid, _ := m["sessionId"].(string); sid != "" {
		t.Fatalf("sessionId = %q，本用例要的是无会话空机", sid)
	}
	if booting, _ := m["booting"].(bool); booting {
		t.Fatal("booting = true，boot 应当已经完成")
	}
	if ready, _ := m["ready"].(bool); !ready {
		t.Error("ready = false：agent 已 boot 的空机必须报 ready=true")
	}

	// 本地模式的 FE 只读 SSE hello（同一份快照），两处必须一致。
	hello := readSSEHello(t, s)
	if ready, _ := hello["ready"].(bool); !ready {
		t.Error("SSE hello.ready = false，与 /api/status 的 ready 不一致")
	}
}

// boot 没成功时不能误报 ready：进程起不来就保持 ready=false，原因走
// bootError（FE 据此显示错误态）。
func TestStatusNotReadyWhenBootFails(t *testing.T) {
	b := acp.NewBridge(acp.GrokConfig{
		Bin:             filepath.Join(t.TempDir(), "no-such-grok"),
		HostID:          "h",
		HostName:        "host",
		LastSessionFile: filepath.Join(t.TempDir(), "last-session.json"),
	})
	t.Cleanup(b.Shutdown)
	s := New(config.Config{Port: 0, GrokBin: "grok"}, b)

	if ready, _ := decodeBody(t, getJSON(t, s, "/api/status"))["ready"].(bool); ready {
		t.Error("ready = true：还没 boot 过的 host 不能报就绪")
	}

	if err := b.Boot(context.Background()); err == nil {
		t.Fatal("Bin 指向不存在的路径，Boot 应当失败")
	}
	m := decodeBody(t, getJSON(t, s, "/api/status"))
	if ready, _ := m["ready"].(bool); ready {
		t.Error("ready = true：boot 失败后不能报就绪")
	}
	if msg, _ := m["bootError"].(string); msg == "" {
		t.Error("bootError 为空：boot 失败的原因要透出给客户端")
	}
}
