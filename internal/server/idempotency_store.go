package server

import (
	"log"
	"sync"
	"time"
)

// 幂等存储的上限。TTL 覆盖「用户隔天重试」；relay 的 pending 窗口（46m）
// 只约束一次传输，不能拿来当幂等窗口。running entry 永不淘汰——淘汰它
// 会让同一个 key 的第二个请求再执行一次。
const (
	idemTTL              = 24 * time.Hour
	idemMaxEntries       = 4096
	idemMaxBytes         = 64 << 20
	idemMaxResponseBytes = 8 << 20
	idemWaitTimeout      = 30 * time.Minute
)

// idemKey 是幂等存储的实际键。HostID 防止多个 host 共用同一个
// ~/.capri-host 时串键；Scope 由 hub 按身份生成（直连默认 direct）。
type idemKey struct {
	HostID string
	Scope  string
	Key    string
}

type idemState int

const (
	idemRunning idemState = iota
	idemDone
	idemUnknown
)

// idemEntry 是一次带 key 请求的共享状态。Done 在进入终态时关闭，
// 等待者靠它醒来；running 期间 Done 保持打开。
type idemEntry struct {
	key        idemKey
	FP         string
	State      idemState
	Status     int
	Body       []byte
	CType      string
	Replayable bool
	At         time.Time
	Done       chan struct{}
}

// idemLookup 是 begin/wait 的结果分类。
type idemLookup int

const (
	// idemOwner：entry 由本次请求创建，调用方执行 handler。
	idemOwner idemLookup = iota
	// idemReplay：已有终态响应，直接回放。
	idemReplay
	// idemWait：同一 key 正在执行，等待 Done。
	idemWait
	// idemConflict：同 key 不同请求指纹。
	idemConflict
	// idemOutcomeUnknown：上次执行在 started 后中断，无法确认结果。
	idemOutcomeUnknown
	// idemResultUnavailable：终态响应超过缓存上限，无法回放。
	idemResultUnavailable
	// idemStoreUnavailable：journal 写失败或存储已满。
	idemStoreUnavailable
	// idemWaitTimedOut：等待 running entry 超时。
	idemWaitTimedOut
	// idemWaitCanceled：等待期间请求上下文结束（客户端断开）。
	idemWaitCanceled
)

type idemResult struct {
	lookup idemLookup
	entry  *idemEntry
	status int
	body   []byte
	ctype  string
}

// idemStore 是进程级 at-most-once 表。只有 begin 在锁内做查找/创建，
// running entry 不淘汰；finish/abort 只生效一次。
type idemStore struct {
	hostID  string
	journal *idemJournal
	// journalBroken 表示启动时 journal 打开或加载失败。此时不能写入：
	// 未加载就 append 会遗忘历史 key，重启后可能重复执行。带 key 的
	// begin 直接 503，直到进程重启重新加载。
	journalBroken bool

	mu      sync.Mutex
	entries map[idemKey]*idemEntry
	order   []idemKey
	bytes   int

	ttl              time.Duration
	maxEntries       int
	maxBytes         int
	maxResponseBytes int
	waitTimeout      time.Duration
	now              func() time.Time

	stop     chan struct{}
	stopOnce sync.Once
}

// newIdemStore 创建 store；path 为空表示 memory-only（测试/嵌入方）。
// journal 打开或加载失败不阻止 host 启动：带 key 的请求会以 503 结束
// （直到进程重启后重新加载成功），无 key 请求不受影响。
func newIdemStore(hostID, path string) *idemStore {
	s := &idemStore{
		hostID:           hostID,
		entries:          make(map[idemKey]*idemEntry),
		ttl:              idemTTL,
		maxEntries:       idemMaxEntries,
		maxBytes:         idemMaxBytes,
		maxResponseBytes: idemMaxResponseBytes,
		waitTimeout:      idemWaitTimeout,
		now:              time.Now,
		stop:             make(chan struct{}),
	}
	if path == "" {
		return s
	}
	j, err := openIdemJournal(path)
	if err != nil {
		log.Printf("[idem] 幂等日志打开失败，带 key 请求将 503 直到重启 path=%s: %v", path, err)
		s.journalBroken = true
		return s
	}
	recs, err := j.load()
	if err != nil {
		log.Printf("[idem] 幂等日志加载失败，带 key 请求将 503 直到重启 path=%s: %v", path, err)
		j.close()
		s.journalBroken = true
		return s
	}
	s.journal = j
	s.load(recs)
	// 启动时按 TTL 清理，并在文件超过上限时原子重写。
	s.pruneAndCompact()
	go s.pruneLoop()
	return s
}

// close 停止周期 prune 并关闭日志文件。
func (s *idemStore) close() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		close(s.stop)
		if s.journal != nil {
			s.journal.close()
		}
	})
}

func (s *idemStore) nowTime() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// begin 查找或创建 entry。journal 的 started 记录先落盘（含 fsync）
// 才允许执行 handler；写失败直接返回 idemStoreUnavailable。
func (s *idemStore) begin(hostID, scope, key, fp string) idemResult {
	k := idemKey{HostID: hostID, Scope: scope, Key: key}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.journalBroken {
		return idemResult{lookup: idemStoreUnavailable}
	}

	now := s.nowTime()
	if e, ok := s.entries[k]; ok {
		if e.State == idemRunning || now.Sub(e.At) < s.ttl {
			if e.FP != fp {
				return idemResult{lookup: idemConflict}
			}
			switch e.State {
			case idemRunning:
				return idemResult{lookup: idemWait, entry: e}
			case idemUnknown:
				return idemResult{lookup: idemOutcomeUnknown}
			default: // idemDone
				if !e.Replayable {
					return idemResult{lookup: idemResultUnavailable}
				}
				return idemResult{lookup: idemReplay, status: e.Status, body: e.Body, ctype: e.CType}
			}
		}
		s.dropEntryLocked(e)
	}

	s.pruneLocked(now)
	if !s.makeRoomLocked() {
		return idemResult{lookup: idemStoreUnavailable}
	}
	if s.journal != nil {
		rec := idemRecord{
			V: idemJournalVersion, HostID: hostID, Scope: scope, Key: key, FP: fp,
			State: idemJournalStarted, At: now,
		}
		if err := s.journal.append(rec); err != nil {
			log.Printf("[idem] started 写入失败，拒绝执行 scope=%q key=%q: %v", scope, key, err)
			return idemResult{lookup: idemStoreUnavailable}
		}
	}
	e := &idemEntry{key: k, FP: fp, State: idemRunning, At: now, Done: make(chan struct{})}
	s.entries[k] = e
	s.order = append(s.order, k)
	return idemResult{lookup: idemOwner, entry: e}
}

// finish 记录终态并唤醒等待者。响应体超过上限时不缓存内容，只留下
// done_unreplayable 标记。
func (s *idemStore) finish(e *idemEntry, status int, body []byte, ctype string, replayable bool) {
	if e == nil {
		return
	}
	if !replayable || len(body) > s.maxResponseBytes {
		replayable = false
		body = nil
	}
	s.mu.Lock()
	if e.State != idemRunning {
		s.mu.Unlock()
		return
	}
	e.State = idemDone
	e.Status = status
	e.Body = append([]byte(nil), body...)
	e.CType = ctype
	e.Replayable = replayable
	e.At = s.nowTime()
	s.bytes += len(e.Body)
	rec := idemRecord{
		V: idemJournalVersion, HostID: e.key.HostID, Scope: e.key.Scope, Key: e.key.Key, FP: e.FP,
		State: idemJournalDone, Status: status, CType: ctype,
		BodyOmitted: !replayable, At: e.At,
	}
	if replayable {
		rec.Body = encodeIdemBody(e.Body)
	}
	// done 必须与状态变更同处一个临界区：compact 的 snapshot+rewrite
	// 也在这把锁内，否则重写可能覆盖掉刚写下的 done。
	s.appendRecordLocked(rec)
	close(e.Done)
	s.mu.Unlock()
}

// abort 用于「还没进入业务 handler 就失败」：删除 entry，重试可以执行。
func (s *idemStore) abort(e *idemEntry) {
	if e == nil {
		return
	}
	s.mu.Lock()
	if e.State != idemRunning {
		s.mu.Unlock()
		return
	}
	// 等待者不能永远挂在一条已经放弃的 entry 上；按 unknown 收尾，
	// 重试请求会拿到 409，随后换一次新请求即可执行。
	e.State = idemUnknown
	e.At = s.nowTime()
	rec := idemRecord{
		V: idemJournalVersion, HostID: e.key.HostID, Scope: e.key.Scope, Key: e.key.Key, FP: e.FP,
		State: idemJournalAborted, At: e.At,
	}
	s.appendRecordLocked(rec)
	close(e.Done)
	s.dropEntryLocked(e)
	s.mu.Unlock()
}

// markUnknown 记录「handler 已开始但结果不可知」（panic / 进程中断）。
func (s *idemStore) markUnknown(e *idemEntry) {
	if e == nil {
		return
	}
	s.mu.Lock()
	if e.State != idemRunning {
		s.mu.Unlock()
		return
	}
	e.State = idemUnknown
	e.At = s.nowTime()
	rec := idemRecord{
		V: idemJournalVersion, HostID: e.key.HostID, Scope: e.key.Scope, Key: e.key.Key, FP: e.FP,
		State: idemJournalUnknown, At: e.At,
	}
	s.appendRecordLocked(rec)
	close(e.Done)
	s.mu.Unlock()
}

// appendRecordLocked 在持有 s.mu 时追加 journal。状态变更与日志行必须
// 对 compact 原子：snapshot+rewrite 也在同一把锁内完成。
func (s *idemStore) appendRecordLocked(rec idemRecord) {
	if s.journal == nil || s.journalBroken {
		return
	}
	if err := s.journal.append(rec); err != nil {
		log.Printf("[idem] journal 追加失败 scope=%q key=%q state=%s: %v", rec.Scope, rec.Key, rec.State, err)
	}
}

// outcome 读取已关闭 entry 的终态。
func (s *idemStore) outcome(e *idemEntry) idemResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch e.State {
	case idemDone:
		if !e.Replayable {
			return idemResult{lookup: idemResultUnavailable}
		}
		return idemResult{lookup: idemReplay, status: e.Status, body: e.Body, ctype: e.CType}
	case idemUnknown:
		return idemResult{lookup: idemOutcomeUnknown}
	default:
		return idemResult{lookup: idemStoreUnavailable}
	}
}

// waitWithContext 等待同一个 key 的 running entry 完成。客户端断开只
// 取消等待，不取消原始执行。
func (s *idemStore) waitWithContext(ctxDone <-chan struct{}, e *idemEntry) idemResult {
	timeout := s.waitTimeout
	if timeout <= 0 {
		timeout = idemWaitTimeout
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-e.Done:
		return s.outcome(e)
	case <-ctxDone:
		return idemResult{lookup: idemWaitCanceled}
	case <-t.C:
		return idemResult{lookup: idemWaitTimedOut}
	}
}

// pruneLocked 丢弃过期的 settled entry；running 一律保留。
func (s *idemStore) pruneLocked(now time.Time) {
	for _, e := range s.entries {
		if e.State == idemRunning || now.Sub(e.At) < s.ttl {
			continue
		}
		s.dropEntryLocked(e)
	}
}

// pruneAndCompact 在同一把 s.mu 内完成过期清理、快照与重写。快照和
// rewrite 必须与 begin/finish 的追加互斥：否则 rewrite 会用不含新
// started 的快照覆盖文件，重启后同一个 key 可能再次执行。
func (s *idemStore) pruneAndCompact() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(s.nowTime())
	s.compactJournalLocked()
}

// compactJournal 供测试与启动路径使用。
func (s *idemStore) compactJournal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.compactJournalLocked()
}

func (s *idemStore) compactJournalLocked() {
	if s.journalBroken || s.journal == nil || !s.journal.oversized() {
		return
	}
	s.journal.rewriteIfLarge(s.snapshotLocked())
}

func (s *idemStore) pruneLoop() {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.pruneAndCompact()
		}
	}
}

// snapshotLocked 导出当前 entry 对应的 journal 记录（重写文件用）。
// 调用方必须持有 s.mu。
func (s *idemStore) snapshotLocked() []idemRecord {
	recs := make([]idemRecord, 0, len(s.entries))
	for _, e := range s.entries {
		rec := idemRecord{
			V: idemJournalVersion, HostID: e.key.HostID, Scope: e.key.Scope, Key: e.key.Key,
			FP: e.FP, At: e.At,
		}
		switch e.State {
		case idemRunning:
			rec.State = idemJournalStarted
		case idemUnknown:
			rec.State = idemJournalUnknown
		default:
			rec.State = idemJournalDone
			rec.Status = e.Status
			rec.CType = e.CType
			rec.BodyOmitted = !e.Replayable
			if e.Replayable {
				rec.Body = encodeIdemBody(e.Body)
			}
		}
		recs = append(recs, rec)
	}
	return recs
}

// makeRoomLocked 在超过条目/字节上限时按 FIFO 淘汰 settled entry。
// 全部是 running 时返回 false（新请求 503，不执行）。
func (s *idemStore) makeRoomLocked() bool {
	for len(s.entries) >= s.maxEntries || s.bytes >= s.maxBytes {
		if !s.evictOneLocked() {
			return false
		}
	}
	return true
}

func (s *idemStore) evictOneLocked() bool {
	for i, k := range s.order {
		e, ok := s.entries[k]
		if !ok {
			s.order = append(s.order[:i], s.order[i+1:]...)
			return true
		}
		if e.State == idemRunning {
			continue
		}
		s.order = append(s.order[:i], s.order[i+1:]...)
		s.dropEntryLocked(e)
		return true
	}
	return false
}

func (s *idemStore) dropEntryLocked(e *idemEntry) {
	if cur, ok := s.entries[e.key]; !ok || cur != e {
		return
	}
	delete(s.entries, e.key)
	if e.State == idemDone {
		s.bytes -= len(e.Body)
		if s.bytes < 0 {
			s.bytes = 0
		}
	}
	for i, k := range s.order {
		if k == e.key {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}

// load 应用 journal 记录：同 key 最后一条为准，started 视为 unknown，
// aborted 视为不存在，过期记录忽略。
func (s *idemStore) load(recs []idemRecord) {
	now := s.nowTime()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rec := range recs {
		if rec.V != idemJournalVersion || rec.Key == "" {
			continue
		}
		k := idemKey{HostID: rec.HostID, Scope: rec.Scope, Key: rec.Key}
		if rec.State == idemJournalAborted {
			if old, ok := s.entries[k]; ok {
				s.dropEntryLocked(old)
			}
			continue
		}
		if rec.At.IsZero() || now.Sub(rec.At) >= s.ttl {
			if old, ok := s.entries[k]; ok {
				s.dropEntryLocked(old)
			}
			continue
		}
		e := &idemEntry{key: k, FP: rec.FP, At: rec.At, Done: make(chan struct{})}
		switch rec.State {
		case idemJournalStarted, idemJournalUnknown:
			e.State = idemUnknown
		case idemJournalDone:
			e.State = idemDone
			e.Status = rec.Status
			e.CType = rec.CType
			if !rec.BodyOmitted {
				if body, err := decodeIdemBody(rec.Body); err == nil {
					e.Body = body
					e.Replayable = true
				}
			}
		default:
			continue
		}
		close(e.Done)
		s.putLocked(e)
	}
	s.pruneLocked(now)
	s.makeRoomLocked()
}

func (s *idemStore) putLocked(e *idemEntry) {
	if old, ok := s.entries[e.key]; ok {
		s.dropEntryLocked(old)
	}
	s.entries[e.key] = e
	s.order = append(s.order, e.key)
	if e.State == idemDone {
		s.bytes += len(e.Body)
	}
}
