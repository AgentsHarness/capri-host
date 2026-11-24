package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AgentsHarness/capri-host/internal/config"
)

func newIdemTestServer(t *testing.T, path string) (*Server, *idemStore) {
	t.Helper()
	st := newIdemStore("h1", path)
	t.Cleanup(st.close)
	return &Server{cfg: config.Config{HostID: "h1", IdemFile: path}, idem: st}, st
}

func idemRequest(method, target, body, key, scope string) *http.Request {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if key != "" {
		req.Header.Set(idemHeaderKey, key)
	}
	if scope != "" {
		req.Header.Set(idemHeaderScope, scope)
	}
	return req
}

func idemCodeOf(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body is not JSON: %s", rec.Body.String())
	}
	return body.Code
}

func countingJSONHandler(hits *atomic.Int32) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"n":%d}`, n)
	})
}

// 同一个 key 第二次请求必须回放第一次的终态，handler 只执行一次。
func TestIdempotencyDuplicateReplaysSameResponse(t *testing.T) {
	s, _ := newIdemTestServer(t, "")
	var hits atomic.Int32
	chain := s.withIdempotency(countingJSONHandler(&hits))

	first := httptest.NewRecorder()
	chain.ServeHTTP(first, idemRequest("POST", "/api/x", `{"a":1}`, "k1", ""))
	if first.Code != 200 || first.Body.String() != `{"n":1}` {
		t.Fatalf("first = %d %s", first.Code, first.Body.String())
	}

	second := httptest.NewRecorder()
	chain.ServeHTTP(second, idemRequest("POST", "/api/x", `{"a":1}`, "k1", ""))
	if second.Code != first.Code || second.Body.String() != first.Body.String() {
		t.Fatalf("replay = %d %s, want %d %s", second.Code, second.Body.String(), first.Code, first.Body.String())
	}
	if hits.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", hits.Load())
	}
	if got := second.Header().Get(idemHeaderReplay); got != "true" {
		t.Fatalf("replay header = %q", got)
	}
	if got := second.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("replay Content-Type = %q", got)
	}
}

// 并发同 key：只执行一次，所有等待者拿到同一响应。
func TestIdempotencyConcurrentDuplicateWaits(t *testing.T) {
	s, _ := newIdemTestServer(t, "")
	var hits atomic.Int32
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		entered <- struct{}{}
		<-release
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	chain := s.withIdempotency(handler)

	firstRec := httptest.NewRecorder()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		chain.ServeHTTP(firstRec, idemRequest("POST", "/api/x", `{}`, "k-concurrent", ""))
	}()
	<-entered

	secondRec := httptest.NewRecorder()
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		chain.ServeHTTP(secondRec, idemRequest("POST", "/api/x", `{}`, "k-concurrent", ""))
	}()

	select {
	case <-secondDone:
		t.Fatal("second request returned before the first one finished")
	case <-time.After(50 * time.Millisecond):
	}
	if hits.Load() != 1 {
		t.Fatalf("handler ran %d times while one request was in flight", hits.Load())
	}
	close(release)
	<-firstDone
	<-secondDone

	if firstRec.Body.String() != `{"ok":true}` || secondRec.Body.String() != `{"ok":true}` {
		t.Fatalf("responses = %q / %q", firstRec.Body.String(), secondRec.Body.String())
	}
	if hits.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", hits.Load())
	}
	if got := secondRec.Header().Get(idemHeaderReplay); got != "true" {
		t.Fatalf("waiter replay header = %q", got)
	}
}

// 同 key 不同请求体：409，第二次绝不执行。
func TestIdempotencyFingerprintConflict(t *testing.T) {
	s, _ := newIdemTestServer(t, "")
	var hits atomic.Int32
	chain := s.withIdempotency(countingJSONHandler(&hits))

	first := httptest.NewRecorder()
	chain.ServeHTTP(first, idemRequest("POST", "/api/x", `{"a":1}`, "k-conflict", ""))
	second := httptest.NewRecorder()
	chain.ServeHTTP(second, idemRequest("POST", "/api/x", `{"a":2}`, "k-conflict", ""))

	if second.Code != http.StatusConflict || idemCodeOf(t, second) != idemCodeConflict {
		t.Fatalf("conflict = %d %s", second.Code, second.Body.String())
	}
	if hits.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", hits.Load())
	}
}

// handler panic：记录 unknown 并继续向上 panic，让外层 recover 返回 500。
func TestIdempotencyPanicMarksOutcomeUnknown(t *testing.T) {
	s, _ := newIdemTestServer(t, "")
	var hits atomic.Int32
	chain := s.withIdempotency(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		hits.Add(1)
		panic("boom")
	}))

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("handler panic did not propagate")
			}
		}()
		chain.ServeHTTP(httptest.NewRecorder(), idemRequest("POST", "/api/x", `{}`, "k-panic", ""))
	}()

	rec := httptest.NewRecorder()
	chain.ServeHTTP(rec, idemRequest("POST", "/api/x", `{}`, "k-panic", ""))
	if rec.Code != http.StatusConflict || idemCodeOf(t, rec) != idemCodeOutcomeUnknown {
		t.Fatalf("retry after panic = %d %s", rec.Code, rec.Body.String())
	}
	if hits.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", hits.Load())
	}
}

// Abort 表示确认没进入 handler：删除 entry 后同一个 key 可以重试执行。
func TestIdempotencyAbortBeforeExecutionAllowsRetry(t *testing.T) {
	_, st := newIdemTestServer(t, "")
	fp := "fp-1"
	first := st.begin("h1", idemScopeDirect, "k-abort", fp)
	if first.lookup != idemOwner {
		t.Fatalf("first begin = %v, want owner", first.lookup)
	}
	st.abort(first.entry)

	second := st.begin("h1", idemScopeDirect, "k-abort", fp)
	if second.lookup != idemOwner {
		t.Fatalf("begin after abort = %v, want owner", second.lookup)
	}
}

// journal 里只有 started：重启后视为 unknown，409 且不执行。
func TestIdempotencyStartedWithoutDoneIsUnknown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idem.jsonl")
	reqBody := `{"a":1}`
	req := idemRequest("POST", "/api/x", reqBody, "k-started", "")
	fp := idemFingerprint(req, []byte(reqBody))
	j, err := openIdemJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.append(idemRecord{
		V: idemJournalVersion, HostID: "h1", Scope: idemScopeDirect, Key: "k-started",
		FP: fp, State: idemJournalStarted, At: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	j.close()

	s, _ := newIdemTestServer(t, path)
	var hits atomic.Int32
	chain := s.withIdempotency(countingJSONHandler(&hits))
	rec := httptest.NewRecorder()
	chain.ServeHTTP(rec, idemRequest("POST", "/api/x", reqBody, "k-started", ""))

	if rec.Code != http.StatusConflict || idemCodeOf(t, rec) != idemCodeOutcomeUnknown {
		t.Fatalf("started-only retry = %d %s", rec.Code, rec.Body.String())
	}
	if hits.Load() != 0 {
		t.Fatalf("handler ran %d times, want 0", hits.Load())
	}
}

// done 记录跨 store 重建后仍然回放，handler 不再执行。
func TestIdempotencyJournalReloadReplays(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idem.jsonl")
	var firstHits atomic.Int32
	first, _ := newIdemTestServer(t, path)
	firstChain := first.withIdempotency(countingJSONHandler(&firstHits))
	rec := httptest.NewRecorder()
	firstChain.ServeHTTP(rec, idemRequest("POST", "/api/x", `{"a":1}`, "k-reload", ""))
	if rec.Code != 200 || firstHits.Load() != 1 {
		t.Fatalf("first execution = %d hits=%d", rec.Code, firstHits.Load())
	}
	first.idem.close()

	var secondHits atomic.Int32
	second, _ := newIdemTestServer(t, path)
	secondChain := second.withIdempotency(countingJSONHandler(&secondHits))
	replay := httptest.NewRecorder()
	secondChain.ServeHTTP(replay, idemRequest("POST", "/api/x", `{"a":1}`, "k-reload", ""))

	if replay.Code != 200 || replay.Body.String() != `{"n":1}` {
		t.Fatalf("reloaded replay = %d %s", replay.Code, replay.Body.String())
	}
	if secondHits.Load() != 0 {
		t.Fatalf("handler ran %d times after reload, want 0", secondHits.Load())
	}
	if got := replay.Header().Get(idemHeaderReplay); got != "true" {
		t.Fatalf("replay header = %q", got)
	}
	if got := replay.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("reloaded Content-Type = %q", got)
	}
}

// 进程内中继构造的是 client-style 请求（RequestURI 为空），指纹必须
// 退回 URL，否则不同路径会共用同一个指纹。
func TestIdempotencyFingerprintInProcessRequest(t *testing.T) {
	s, _ := newIdemTestServer(t, "")
	var hits atomic.Int32
	chain := s.withIdempotency(countingJSONHandler(&hits))

	newReq := func(path string) *http.Request {
		req, err := http.NewRequest("POST", "http://127.0.0.1"+path+"?q=1", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(idemHeaderKey, "k-inproc")
		return req
	}
	first := httptest.NewRecorder()
	chain.ServeHTTP(first, newReq("/api/x"))
	second := httptest.NewRecorder()
	chain.ServeHTTP(second, newReq("/api/x"))
	if second.Body.String() != first.Body.String() || hits.Load() != 1 {
		t.Fatalf("same path did not replay: first=%s second=%s hits=%d", first.Body.String(), second.Body.String(), hits.Load())
	}
	other := httptest.NewRecorder()
	chain.ServeHTTP(other, newReq("/api/y"))
	if other.Code != http.StatusConflict || idemCodeOf(t, other) != idemCodeConflict {
		t.Fatalf("different path = %d %s", other.Code, other.Body.String())
	}
}

// TTL 过期后同一个 key 视为新请求。
func TestIdempotencyTTLAllowsRetry(t *testing.T) {
	s, st := newIdemTestServer(t, "")
	st.ttl = 10 * time.Millisecond
	var hits atomic.Int32
	chain := s.withIdempotency(countingJSONHandler(&hits))

	chain.ServeHTTP(httptest.NewRecorder(), idemRequest("POST", "/api/x", `{}`, "k-ttl", ""))
	time.Sleep(30 * time.Millisecond)
	rec := httptest.NewRecorder()
	chain.ServeHTTP(rec, idemRequest("POST", "/api/x", `{}`, "k-ttl", ""))
	if rec.Body.String() != `{"n":2}` {
		t.Fatalf("after TTL = %s, want a new execution", rec.Body.String())
	}
	if hits.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2", hits.Load())
	}
}

// 上限满时按 FIFO 淘汰 done；running entry 永不淘汰。
func TestIdempotencyEviction(t *testing.T) {
	s, st := newIdemTestServer(t, "")
	st.maxEntries = 2
	var hits atomic.Int32
	chain := s.withIdempotency(countingJSONHandler(&hits))

	for _, key := range []string{"e1", "e2", "e3"} {
		chain.ServeHTTP(httptest.NewRecorder(), idemRequest("POST", "/api/x", `{}`, key, ""))
	}
	if hits.Load() != 3 {
		t.Fatalf("handler ran %d times, want 3", hits.Load())
	}
	// e1 已被淘汰：重试会重新执行；e2/e3 仍在表内。
	rec := httptest.NewRecorder()
	chain.ServeHTTP(rec, idemRequest("POST", "/api/x", `{}`, "e1", ""))
	if rec.Body.String() != `{"n":4}` {
		t.Fatalf("evicted key replay = %s, want a new execution", rec.Body.String())
	}
}

func TestIdempotencyRunningEntryIsNotEvicted(t *testing.T) {
	s, st := newIdemTestServer(t, "")
	st.maxEntries = 1
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	chain := s.withIdempotency(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-release
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))

	done := make(chan struct{})
	go func() {
		defer close(done)
		chain.ServeHTTP(httptest.NewRecorder(), idemRequest("POST", "/api/x", `{}`, "running", ""))
	}()
	<-entered

	rec := httptest.NewRecorder()
	chain.ServeHTTP(rec, idemRequest("POST", "/api/x", `{}`, "other", ""))
	if rec.Code != http.StatusServiceUnavailable || idemCodeOf(t, rec) != idemCodeStoreUnavailable {
		t.Fatalf("full store = %d %s", rec.Code, rec.Body.String())
	}
	close(release)
	<-done
}

// 超过响应上限：第一次完整返回，重复请求 409 result_unavailable。
func TestIdempotencyLargeResponseNotCached(t *testing.T) {
	s, st := newIdemTestServer(t, "")
	st.maxResponseBytes = 32
	body := strings.Repeat("x", 100)
	var hits atomic.Int32
	chain := s.withIdempotency(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(body))
	}))

	first := httptest.NewRecorder()
	chain.ServeHTTP(first, idemRequest("POST", "/api/x", `{}`, "k-big", ""))
	if first.Code != 200 || first.Body.String() != body {
		t.Fatalf("large first = %d (%d bytes)", first.Code, first.Body.Len())
	}
	second := httptest.NewRecorder()
	chain.ServeHTTP(second, idemRequest("POST", "/api/x", `{}`, "k-big", ""))
	if second.Code != http.StatusConflict || idemCodeOf(t, second) != idemCodeResultUnavailable {
		t.Fatalf("large replay = %d %s", second.Code, second.Body.String())
	}
	if hits.Load() != 1 {
		t.Fatalf("handler ran %d times, want 1", hits.Load())
	}
}

// journal 不可写：带 key 请求 503 且不执行；无 key 请求不受影响。
func TestIdempotencyJournalFailureReturns503(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idem")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	s, _ := newIdemTestServer(t, path)
	var hits atomic.Int32
	chain := s.withIdempotency(countingJSONHandler(&hits))

	rec := httptest.NewRecorder()
	chain.ServeHTTP(rec, idemRequest("POST", "/api/x", `{}`, "k-fail", ""))
	if rec.Code != http.StatusServiceUnavailable || idemCodeOf(t, rec) != idemCodeStoreUnavailable {
		t.Fatalf("journal failure = %d %s", rec.Code, rec.Body.String())
	}
	if hits.Load() != 0 {
		t.Fatalf("handler ran %d times, want 0", hits.Load())
	}

	plain := httptest.NewRecorder()
	chain.ServeHTTP(plain, idemRequest("POST", "/api/x", `{}`, "", ""))
	if plain.Code != 200 || hits.Load() != 1 {
		t.Fatalf("keyless request = %d hits=%d", plain.Code, hits.Load())
	}
}

// running 等待超时返回 409 in_progress，不打断原始执行。
func TestIdempotencyWaitTimeout(t *testing.T) {
	s, st := newIdemTestServer(t, "")
	st.waitTimeout = 20 * time.Millisecond
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	chain := s.withIdempotency(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-release
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))

	done := make(chan struct{})
	go func() {
		defer close(done)
		chain.ServeHTTP(httptest.NewRecorder(), idemRequest("POST", "/api/x", `{}`, "k-wait", ""))
	}()
	<-entered

	rec := httptest.NewRecorder()
	chain.ServeHTTP(rec, idemRequest("POST", "/api/x", `{}`, "k-wait", ""))
	if rec.Code != http.StatusConflict || idemCodeOf(t, rec) != idemCodeInProgress {
		t.Fatalf("wait timeout = %d %s", rec.Code, rec.Body.String())
	}
	close(release)
	<-done
}

// scope 隔离：不同用户复用同一个 key 不会互相回放或冲突。
func TestIdempotencyScopeIsolation(t *testing.T) {
	s, _ := newIdemTestServer(t, "")
	var hits atomic.Int32
	chain := s.withIdempotency(countingJSONHandler(&hits))

	chain.ServeHTTP(httptest.NewRecorder(), idemRequest("POST", "/api/x", `{}`, "same", "user:u1"))
	chain.ServeHTTP(httptest.NewRecorder(), idemRequest("POST", "/api/x", `{}`, "same", "user:u2"))
	if hits.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2 (scopes must not merge)", hits.Load())
	}
}

func TestIdempotencyInvalidKey(t *testing.T) {
	s, _ := newIdemTestServer(t, "")
	var hits atomic.Int32
	chain := s.withIdempotency(countingJSONHandler(&hits))

	rec := httptest.NewRecorder()
	chain.ServeHTTP(rec, idemRequest("POST", "/api/x", `{}`, strings.Repeat("k", idemKeyMaxLen+1), ""))
	if rec.Code != http.StatusBadRequest || idemCodeOf(t, rec) != idemCodeInvalidKey {
		t.Fatalf("invalid key = %d %s", rec.Code, rec.Body.String())
	}
	if hits.Load() != 0 {
		t.Fatalf("handler ran %d times, want 0", hits.Load())
	}
}

// 无 key 的 POST 与带 key 的 GET 都原样通过，保持零额外语义。
func TestIdempotencyPassthrough(t *testing.T) {
	s, _ := newIdemTestServer(t, "")
	var hits atomic.Int32
	chain := s.withIdempotency(countingJSONHandler(&hits))

	both := func(req *http.Request) {
		chain.ServeHTTP(httptest.NewRecorder(), req)
		chain.ServeHTTP(httptest.NewRecorder(), req)
	}
	both(idemRequest("POST", "/api/x", `{}`, "", ""))
	both(idemRequest("GET", "/api/x", ``, "k-get", ""))
	if hits.Load() != 4 {
		t.Fatalf("handler ran %d times, want 4", hits.Load())
	}
}

// 并发同 key 的写盘顺序：started 先于 handler，done 在响应返回前写入。
func TestIdempotencyJournalWriteOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idem.jsonl")
	s, _ := newIdemTestServer(t, path)
	var hits atomic.Int32
	chain := s.withIdempotency(countingJSONHandler(&hits))
	chain.ServeHTTP(httptest.NewRecorder(), idemRequest("POST", "/api/x", `{}`, "k-order", ""))

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("journal has %d lines, want started+done:\n%s", len(lines), data)
	}
	var started, done idemRecord
	if err := json.Unmarshal([]byte(lines[0]), &started); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &done); err != nil {
		t.Fatal(err)
	}
	if started.State != idemJournalStarted || done.State != idemJournalDone {
		t.Fatalf("journal states = %s / %s", started.State, done.State)
	}
	if done.Status != 200 || decodeIdemBodyOrFail(t, done.Body) != `{"n":1}` {
		t.Fatalf("done record = %+v", done)
	}
}

func decodeIdemBodyOrFail(t *testing.T, s string) string {
	t.Helper()
	b, err := decodeIdemBody(s)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// waitWithContext 在客户端断开时只放弃等待，不取消执行。
func TestIdempotencyWaitCanceled(t *testing.T) {
	s, st := newIdemTestServer(t, "")
	st.waitTimeout = time.Minute
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	chain := s.withIdempotency(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-release
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		chain.ServeHTTP(httptest.NewRecorder(), idemRequest("POST", "/api/x", `{}`, "k-cancel", ""))
	}()
	<-entered

	ctx, cancel := context.WithCancel(context.Background())
	req := idemRequest("POST", "/api/x", `{}`, "k-cancel", "").WithContext(ctx)
	cancel()
	rec := httptest.NewRecorder()
	chain.ServeHTTP(rec, req)
	if rec.Body.Len() != 0 {
		t.Fatalf("canceled waiter wrote a body: %s", rec.Body.String())
	}
	close(release)
	<-firstDone
}

// compactJournal 必须在 s.mu 内完成 snapshot+rewrite：当 rewrite 被
// journal 锁挡住时，store 锁不能被放掉，否则 begin 的 started 会落进
// 重写窗口并被旧快照覆盖。
func TestIdempotencyCompactionHoldsStoreLock(t *testing.T) {
	_, st := newIdemTestServer(t, filepath.Join(t.TempDir(), "idem.jsonl"))
	// 持住 journal 锁直到 compact 进入 rewrite，使它卡在 j.mu 上并
	// 继续持有 s.mu。
	st.journal.mu.Lock()
	st.journal.size = idemJournalMaxBytes + 1

	done := make(chan struct{})
	go func() {
		st.compactJournal()
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if !st.mu.TryLock() {
			break // compact 正持有 s.mu，且卡在 journal 锁上
		}
		st.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("compactJournal never held s.mu while blocked in the journal rewrite")
		}
		time.Sleep(time.Millisecond)
	}
	st.journal.mu.Unlock()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("compactJournal did not finish")
	}
}

// compaction 与 begin 并发时，每个 started 都必须留在日志里：丢一条
// 就意味着重启后同一个 key 可以再次执行。
func TestIdempotencyCompactionKeepsStarted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idem.jsonl")
	_, st := newIdemTestServer(t, path)

	// 放大快照，让 rewrite 有足够的序列化/写盘时间覆盖并发窗口。
	for i := 0; i < 48; i++ {
		r := st.begin("h1", idemScopeDirect, fmt.Sprintf("seed-%d", i), "fp")
		if r.lookup != idemOwner {
			t.Fatalf("seed begin = %v", r.lookup)
		}
		st.finish(r.entry, 200, bytes.Repeat([]byte("x"), 16<<10), "application/json", true)
	}

	const keys = 48
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			st.journal.mu.Lock()
			st.journal.size = idemJournalMaxBytes + 1
			st.journal.mu.Unlock()
			st.compactJournal()
		}
	}()

	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("run-%d", i)
		if r := st.begin("h1", idemScopeDirect, key, "fp"); r.lookup != idemOwner {
			t.Fatalf("begin %s = %v", key, r.lookup)
		}
	}
	close(stop)
	wg.Wait()
	st.close()

	j, err := openIdemJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	recs, err := j.load()
	j.close()
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	for _, rec := range recs {
		if rec.HostID == "h1" && rec.Scope == idemScopeDirect {
			seen[rec.Key] = true
		}
	}
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("run-%d", i)
		if !seen[key] {
			t.Fatalf("started record for %s was lost during compaction", key)
		}
	}
}

// 崩溃留下的半行尾必须被截掉；否则下一次 O_APPEND 会把它和正常记录
// 粘成一条无法解析的坏行。
func TestIdempotencyJournalTruncatesPartialTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idem.jsonl")
	const oldKey = "old-key"
	j, err := openIdemJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.append(idemRecord{
		V: idemJournalVersion, HostID: "h1", Scope: idemScopeDirect, Key: oldKey,
		FP: "fp-old", State: idemJournalStarted, At: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	j.close()

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"torn":"`); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	st := newIdemStore("h1", path)
	t.Cleanup(st.close)
	if r := st.begin("h1", idemScopeDirect, oldKey, "fp-old"); r.lookup != idemOutcomeUnknown {
		t.Fatalf("complete record before torn tail = %v, want unknown", r.lookup)
	}
	r := st.begin("h1", idemScopeDirect, "new-key", "fp-new")
	if r.lookup != idemOwner {
		t.Fatalf("begin after torn tail = %v, want owner", r.lookup)
	}
	st.finish(r.entry, 200, []byte(`{"ok":true}`), "application/json", true)
	st.close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(`"torn"`)) {
		t.Fatalf("partial tail survived:\n%s", data)
	}
	if !bytes.HasSuffix(data, []byte("\n")) {
		t.Fatalf("journal does not end with a newline:\n%s", data)
	}
	lines := bytes.Split(bytes.TrimSuffix(data, []byte("\n")), []byte("\n"))
	if len(lines) != 3 {
		t.Fatalf("journal has %d lines, want started+started+done:\n%s", len(lines), data)
	}
	for _, line := range lines {
		var rec idemRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("unparsable journal line %q: %v", line, err)
		}
	}
}

// load 失败时 store 必须整体拒绝带 key 请求；不能“没加载但继续写”，
// 否则历史 key 全部遗忘，重启后可能重复执行。
func TestIdempotencyJournalLoadFailureReturns503(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idem.jsonl")
	orig := idemReadFile
	idemReadFile = func(string) ([]byte, error) { return nil, errors.New("read failed") }
	st := newIdemStore("h1", path)
	idemReadFile = orig
	t.Cleanup(func() { idemReadFile = orig })
	t.Cleanup(st.close)

	s := &Server{cfg: config.Config{HostID: "h1", IdemFile: path}, idem: st}
	var hits atomic.Int32
	chain := s.withIdempotency(countingJSONHandler(&hits))
	rec := httptest.NewRecorder()
	chain.ServeHTTP(rec, idemRequest("POST", "/api/x", `{}`, "k-load-broken", ""))
	if rec.Code != http.StatusServiceUnavailable || idemCodeOf(t, rec) != idemCodeStoreUnavailable {
		t.Fatalf("broken journal = %d %s", rec.Code, rec.Body.String())
	}
	if hits.Load() != 0 {
		t.Fatalf("handler ran %d times, want 0", hits.Load())
	}
	if fi, err := os.Stat(path); err != nil || fi.Size() != 0 {
		t.Fatalf("broken journal file = %v size %v, want empty", err, fi)
	}
}
