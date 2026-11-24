package acp

import (
	"errors"
	"os"
	"sync"
	"time"
)

// ── 归一化视图缓存 ──────────────────────────────────────────────────────

// historyCacheEntry 按 (size, mtime) 失效的缓存条目；只存行级元数据，
// 不缓存信封内容（信封每次按窗口从文件现读）。
type historyCacheEntry struct {
	size    int64
	modTime time.Time
	view    *normalizedHistory
}

// historyCache 缓存会话 updates.jsonl 的归一化视图。Bridge 持有一个
// 实例，缓存锁纪律收敛在本类型内。
type historyCache struct {
	mu      sync.Mutex
	entries map[string]*historyCacheEntry
}

// get 命中返回视图；未命中或 (size, mtime) 已变化返回 false。
func (hc *historyCache) get(path string, size int64, mod time.Time) (*normalizedHistory, bool) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	if ent, ok := hc.entries[path]; ok && ent.size == size && ent.modTime.Equal(mod) {
		return ent.view, true
	}
	return nil, false
}

func (hc *historyCache) put(path string, size int64, mod time.Time, view *normalizedHistory) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	if hc.entries == nil {
		hc.entries = make(map[string]*historyCacheEntry)
	}
	hc.entries[path] = &historyCacheEntry{size: size, modTime: mod, view: view}
}

// normalizedSessionHistory 返回该会话 updates.jsonl 的归一化视图（带
// (size, mtime) 缓存）。ok=false = 触发回退（文件不可读 / 没有任何带
// agentTimestampMs 的信封），调用方走 agent RPC 透传。
func (b *Bridge) normalizedSessionHistory(path string) (*normalizedHistory, bool) {
	st, err := os.Stat(path)
	if err != nil || st.IsDir() {
		return nil, false
	}
	size, mod := st.Size(), st.ModTime()

	if view, ok := b.hist.get(path, size, mod); ok {
		return view, true
	}

	view, err := buildNormalizedHistory(path)
	if err != nil {
		return nil, false
	}

	b.hist.put(path, size, mod, view)
	return view, true
}

// ── 本地分页服务（msgSeq 空间）─────────────────────────────────────────

// errLocalHistoryUnavailable：本地归一化不可用（无文件 / 无时间戳），
// 调用方走 agent RPC 透传。errLocalHistoryRead：已经在 msgSeq 空间里，
// 窗口读失败——返回错误，不得透传换空间。
var (
	errLocalHistoryUnavailable = errors.New("local session history unavailable")
	errLocalHistoryRead        = errors.New("local session history window read failed")
)
