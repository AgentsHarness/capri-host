package acp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestSessionUpdatesWindowReadFailureDoesNotPassthrough(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	path := writeSessionFile(t, home, "/ws", sid, []string{
		histEnvelope(sid, 0, 10, msgUserChunk("A")),
	})
	b, w := historyBridge(t, home)
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// 缓存一份指向不存在行名次的视图：读窗口必然缺信封。
	b.hist.put(path, st.Size(), st.ModTime(), &normalizedHistory{
		lines:        []updateLineMeta{{line: 99, agentTsMs: 10, isUserChunk: true}},
		order:        []int{0},
		promptStarts: []int{0},
	})
	_, err = b.SessionUpdates(context.Background(), sid, "/ws")
	if err == nil {
		t.Fatal("窗口缺行应返回错误，不得静默成功")
	}
	if !errors.Is(err, errLocalHistoryRead) {
		t.Fatalf("err = %v, want errLocalHistoryRead", err)
	}
	if len(w.lines) != 0 {
		t.Fatalf("已在 msgSeq 空间时不得透传 agent RPC（写了 %d 条）", len(w.lines))
	}
}

// 子代理会话的 updates.jsonl 落在子代理自己的 cwd 编码目录下，而 FE
// 只能传父会话 cwd。直接路径落空时必须按 sessionId 跨 cwd 目录解析，
// 否则会透传 agent RPC（无 msgSeq/promptStarts），时间线分页回到旧缺陷。
func TestSessionUpdatesResolvesSubagentSessionByID(t *testing.T) {
	home := t.TempDir()
	const sid = "01a17300-a1ff-76e0-9612-805ad8153dd7"
	const parentCwd = "/Users/dev/ccwork"
	const childCwd = "/Users/dev/ccwork/acp-hub"

	// 父 cwd 目录存在（父会话在此），但没有该子会话。
	if err := os.MkdirAll(filepath.Join(home, "sessions", EncodeCwdDirname(parentCwd)), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sessionUpdatesFile(home, parentCwd, sid)); !os.IsNotExist(err) {
		t.Fatalf("直接路径应不存在，stat err = %v", err)
	}
	writeSessionFile(t, home, childCwd, sid, []string{
		histEnvelope(sid, 0, 100, msgUserChunk("child-q")),
		histEnvelope(sid, 1, 200, msgAgentChunk("child-a")),
	})

	b, w := historyBridge(t, home)
	// 若回退透传，request 会等 agent 直到 ctx 超时——用有界 ctx 让缺陷
	// 以失败而不是挂起来暴露。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	off := int64(-100)
	lim := 100
	page, err := b.SessionUpdates(ctx, sid, parentCwd, SessionUpdatesOpts{Offset: &off, Limit: &lim})
	if err != nil {
		t.Fatalf("子代理会话应按 sessionId 找到本地文件，err = %v（可能回退透传了）", err)
	}
	if len(w.lines) != 0 {
		t.Fatalf("本地命中时不得请求 agent（写了 %d 条）", len(w.lines))
	}
	if got := pageSeqs(t, page); !reflect.DeepEqual(got, []int{0, 1}) {
		t.Errorf("msgSeq = %v, want [0 1]", got)
	}
	if !reflect.DeepEqual(page.PromptStarts, []int{0}) {
		t.Errorf("promptStarts = %v, want [0]", page.PromptStarts)
	}
	if page.TotalCount != 2 {
		t.Errorf("totalCount = %d, want 2", page.TotalCount)
	}
	if got := pageTexts(t, page); !reflect.DeepEqual(got, []string{"child-q", "child-a"}) {
		t.Errorf("服务内容 = %v, want 子会话内容", got)
	}
}

// 直接路径存在时保持原行为：即使别的 cwd 目录下有同名（更新）文件，
// 也只读直接路径，不做跨目录扫描。
func TestSessionUpdatesPrefersDirectPathWithoutScan(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	const parentCwd = "/Users/dev/ccwork"
	const otherCwd = "/Users/dev/ccwork/acp-hub"

	direct := writeSessionFile(t, home, parentCwd, sid, []string{
		histEnvelope(sid, 0, 100, msgUserChunk("direct")),
	})
	other := writeSessionFile(t, home, otherCwd, sid, []string{
		histEnvelope(sid, 0, 200, msgUserChunk("other")),
		histEnvelope(sid, 1, 300, msgAgentChunk("other-2")),
	})
	old := time.Now().Add(-time.Hour)
	future := time.Now().Add(time.Hour)
	if err := os.Chtimes(direct, old, old); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(other, future, future); err != nil {
		t.Fatal(err)
	}

	if got, ok := resolveSessionUpdatesFile(home, parentCwd, sid); !ok || got != direct {
		t.Fatalf("resolve = (%q, %v), want 直接路径 (%q, true)", got, ok, direct)
	}

	b, w := historyBridge(t, home)
	page, err := b.SessionUpdates(context.Background(), sid, parentCwd)
	if err != nil {
		t.Fatal(err)
	}
	if len(w.lines) != 0 {
		t.Fatalf("直接路径可读时不得请求 agent（写了 %d 条）", len(w.lines))
	}
	if got := pageTexts(t, page); !reflect.DeepEqual(got, []string{"direct"}) {
		t.Errorf("服务内容 = %v, want [direct]（不得采用其它 cwd 目录的新文件）", got)
	}
}

// 畸形 sessionId 不参与路径拼接：即使穿越目标文件真实存在也不得返回，
// 更不得扫描。
func TestResolveSessionUpdatesFileRejectsUnsafeSessionID(t *testing.T) {
	home := t.TempDir()
	const legit = "sess-1"
	legitPath := writeSessionFile(t, home, "/ws", legit, []string{
		histEnvelope(legit, 0, 100, msgUserChunk("ok")),
	})
	// 若 `../` 未被拒绝，join 会命中这条越界文件。
	escape := filepath.Join(home, "sessions", legit, "updates.jsonl")
	if err := os.MkdirAll(filepath.Dir(escape), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(escape, []byte(histEnvelope(legit, 0, 100, msgUserChunk("escaped"))+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, bad := range []string{"", "..", "../" + legit, "sub/../" + legit, "a/b", `a\b`, legit + "/.."} {
		if path, ok := resolveSessionUpdatesFile(home, "/ws", bad); ok || path != "" {
			t.Errorf("sessionId %q 应被拒绝，got (%q, %v)", bad, path, ok)
		}
	}
	if path, ok := resolveSessionUpdatesFile(home, "/ws", legit); !ok || path != legitPath {
		t.Errorf("合法 sessionId resolve = (%q, %v), want (%q, true)", path, ok, legitPath)
	}
}

// 扫描到多份同名会话（理论上不该发生）时取 mtime 最新的一份。
func TestResolveSessionUpdatesFilePicksNewestMatch(t *testing.T) {
	home := t.TempDir()
	const sid = "sess-1"
	older := writeSessionFile(t, home, "/older", sid, []string{
		histEnvelope(sid, 0, 100, msgUserChunk("older")),
	})
	newer := writeSessionFile(t, home, "/newer", sid, []string{
		histEnvelope(sid, 0, 200, msgUserChunk("newer")),
	})
	old := time.Now().Add(-time.Hour)
	future := time.Now().Add(time.Hour)
	if err := os.Chtimes(older, old, old); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(newer, future, future); err != nil {
		t.Fatal(err)
	}
	got, ok := resolveSessionUpdatesFile(home, "/parent-without-session", sid)
	if !ok || got != newer {
		t.Errorf("resolve = (%q, %v), want mtime 最新的 (%q, true)", got, ok, newer)
	}
}
