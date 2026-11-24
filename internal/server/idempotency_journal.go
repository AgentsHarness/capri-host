package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// 幂等 journal 是 ~/.capri-host/relay-idempotency.jsonl 的追加日志：
// started 在 handler 执行前落盘并 fsync，done/unknown/aborted 收尾。
// 重启后同一 key 以最后一条记录为准；只有 started 说明上次进程在
// 执行中崩溃，恢复为 unknown，绝不自动重放。
const (
	idemJournalVersion  = 1
	idemJournalStarted  = "started"
	idemJournalDone     = "done"
	idemJournalUnknown  = "unknown"
	idemJournalAborted  = "aborted"
	idemJournalMaxBytes = 32 << 20
)

// idemRecord 是一行 JSONL。字段名是磁盘格式，改动需要兼容旧文件。
type idemRecord struct {
	V           int       `json:"v"`
	HostID      string    `json:"hostId"`
	Scope       string    `json:"scope"`
	Key         string    `json:"key"`
	FP          string    `json:"fp"`
	State       string    `json:"state"`
	Status      int       `json:"status,omitempty"`
	Body        string    `json:"body,omitempty"`
	BodyOmitted bool      `json:"bodyOmitted,omitempty"`
	CType       string    `json:"ctype,omitempty"`
	At          time.Time `json:"at"`
}

// idemJournal 串行化追加与重写。f 为 nil 时（构造失败或刚重写）下一次
// append 会尝试重新打开，失败即向上返回错误，让带 key 请求 503。
type idemJournal struct {
	path string

	mu   sync.Mutex
	f    *os.File
	size int64
}

func openIdemJournal(path string) (*idemJournal, error) {
	j := &idemJournal{path: path}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.openLocked(); err != nil {
		return nil, err
	}
	return j, nil
}

func (j *idemJournal) openLocked() error {
	dir := filepath.Dir(j.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	// 旧文件可能是更宽松的权限；日志里有请求体摘要与响应体。
	_ = f.Chmod(0o600)
	if st, err := f.Stat(); err == nil {
		j.size = st.Size()
	}
	j.f = f
	return nil
}

// idemReadFile 是 load 的测试缝：模拟日志不可读，验证 load 失败会进入
// journalBroken，而不是继续写入一份没有加载过的日志。
var idemReadFile = os.ReadFile

// load 读取现有日志。崩溃留下的不完整尾行（没有换行符）先截掉再开始
// append，否则 O_APPEND 会把下一条记录粘在坏行后面；中间的坏行记日志
// 后跳过，不让一行损坏毁掉整个幂等窗口。
func (j *idemJournal) load() ([]idemRecord, error) {
	data, err := idemReadFile(j.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if len(data) > 0 && data[len(data)-1] != '\n' {
		keep := int64(bytes.LastIndexByte(data, '\n') + 1)
		if j.f != nil {
			if err := j.f.Truncate(keep); err != nil {
				return nil, err
			}
			if err := j.f.Sync(); err != nil {
				return nil, err
			}
		} else if err := os.Truncate(j.path, keep); err != nil {
			return nil, err
		}
		j.size = keep
		data = data[:keep]
	}
	var out []idemRecord
	for len(data) > 0 {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			break
		}
		line := bytes.TrimSpace(data[:i])
		data = data[i+1:]
		if len(line) == 0 {
			continue
		}
		var rec idemRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			log.Printf("[idem] journal 行解析失败，已跳过: %v", err)
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

// append 追加一条记录并 fsync。只有落盘成功，调用方才允许执行 handler。
func (j *idemJournal) append(rec idemRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f == nil {
		if err := j.openLocked(); err != nil {
			return err
		}
	}
	n, err := j.f.Write(b)
	if err != nil {
		return err
	}
	if err := j.f.Sync(); err != nil {
		return err
	}
	j.size += int64(n)
	return nil
}

// oversized 报告文件是否超过重写阈值（避免无谓地生成快照）。
func (j *idemJournal) oversized() bool {
	if j == nil {
		return false
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.f != nil && j.size > idemJournalMaxBytes
}

// rewriteIfLarge 在文件超过 32MiB 时用活跃 entry 原子重写（临时文件 +
// rename）。重写与 prune 同步进行，避免日志无限增长。
func (j *idemJournal) rewriteIfLarge(recs []idemRecord) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f == nil || j.size <= idemJournalMaxBytes {
		return
	}
	if err := j.rewriteLocked(recs); err != nil {
		log.Printf("[idem] 幂等日志重写失败 path=%s: %v", j.path, err)
		if j.f == nil {
			_ = j.openLocked()
		}
	}
}

func (j *idemJournal) rewriteLocked(recs []idemRecord) error {
	var buf bytes.Buffer
	// 超过文件上限后只省略响应体，绝不丢状态记录：少一条 started
	// 就可能让重启后的同一个 key 再执行一次。
	omitBodies := false
	for _, rec := range recs {
		if !omitBodies && buf.Len() > idemJournalMaxBytes && rec.Body != "" {
			omitBodies = true
		}
		if omitBodies && rec.Body != "" {
			rec.Body = ""
			rec.BodyOmitted = true
		}
		b, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		buf.Write(b)
		buf.WriteByte('\n')
	}
	dir := filepath.Dir(j.path)
	tmp, err := os.CreateTemp(dir, ".relay-idempotency-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(buf.Bytes()); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if j.f != nil {
		_ = j.f.Close()
		j.f = nil
	}
	if err := os.Rename(tmpName, j.path); err != nil {
		return err
	}
	ok = true
	if err := j.openLocked(); err != nil {
		return err
	}
	j.size = int64(buf.Len())
	return nil
}

func (j *idemJournal) close() {
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f != nil {
		_ = j.f.Sync()
		_ = j.f.Close()
		j.f = nil
	}
}

func encodeIdemBody(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func decodeIdemBody(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}
