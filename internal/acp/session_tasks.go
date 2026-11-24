package acp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// ── Persisted task timeline ─────────────────────────────────────────────
//
// The TUI rebuilds its tasks pane from the FULL replay of a session's
// stored updates on session/load; capri-fe only replays the newest history
// page (HISTORY_PAGE_SIZE=100), so task_backgrounded events older than the
// page never materialize as rows. These helpers scan the session's
// updates.jsonl directly (same file the agent reads) and extract the task
// event timeline — the web client's equivalent of the TUI full replay.
//
// Wire tags appear verbatim in compact JSON, so a raw substring match on
// `"sessionUpdate":"<tag>"` is exact (quotes inside JSON strings are
// escaped) and doubles as a cheap pre-filter.

const (
	taskEventBackgrounded = "task_backgrounded"
	taskEventCompleted    = "task_completed"
	taskEventMonitor      = "monitor_event"
	taskEventSchedCreated = "scheduled_task_created"
	taskEventSchedDeleted = "scheduled_task_deleted"
	taskEventSchedFired   = "scheduled_task_fired"

	tagBackgrounded = `"sessionUpdate":"task_backgrounded"`
	tagCompleted    = `"sessionUpdate":"task_completed"`
	tagSchedCreated = `"sessionUpdate":"scheduled_task_created"`
	tagSchedDeleted = `"sessionUpdate":"scheduled_task_deleted"`
	tagSchedFired   = `"sessionUpdate":"scheduled_task_fired"`
)

// Byte forms of the raw-tag pre-filters for the streaming scanner paths
// (same trick as bridge_ext_usage.go: bytes.Contains on []byte avoids a
// per-line string conversion).
var (
	tagBackgroundedB = []byte(tagBackgrounded)
	tagCompletedB    = []byte(tagCompleted)
	tagSchedCreatedB = []byte(tagSchedCreated)
	tagSchedDeletedB = []byte(tagSchedDeleted)
	tagSchedFiredB   = []byte(tagSchedFired)
)

// TaskEvent is one persisted task-lifecycle event, in rewind-surviving
// sequence order (same view as msgSeq). The
// fields mirror the wire update so the frontend can reuse its live
// handlers (handleTaskBackgrounded / handleTaskCompleted) for replay.
type TaskEvent struct {
	Timestamp int64 `json:"timestamp,omitempty"`
	// Kind: task_backgrounded | task_completed | scheduled_task_*.
	Kind string `json:"kind"`
	// task_backgrounded.
	TaskID             string `json:"taskId,omitempty"`
	Command            string `json:"command,omitempty"`
	Description        string `json:"description,omitempty"`
	MonitorDescription string `json:"monitorDescription,omitempty"`
	OutputFile         string `json:"outputFile,omitempty"`
	Cwd                string `json:"cwd,omitempty"`
	// task_completed (task_snapshot).
	ExitCode  *int   `json:"exitCode,omitempty"`
	Signal    string `json:"signal,omitempty"`
	Completed bool   `json:"completed,omitempty"`
	Output    string `json:"output,omitempty"`
	// monitor_event (task stdout chunk).
	EventText string `json:"eventText,omitempty"`
	// scheduled_task_*.
	Prompt        string `json:"prompt,omitempty"`
	HumanSchedule string `json:"humanSchedule,omitempty"`
	// Running: task still running (no completion in file AND its output
	// log is held open by a live process — see probeOpenLogs).
	Running bool `json:"running,omitempty"`
	// PID of the process the kernel reports as holding the output log —
	// set only on liveness-probed results (see logHolder).
	PID int `json:"pid,omitempty"`
}

// logHolder is the open-fd attribution for one task output log: which
// process the kernel says still holds it.
type logHolder struct {
	PID     int
	Command string
}

// TaskSummary is the cheap per-session task census for the history
// sidebar [bg] badge (tag-filtered parse — no full JSON scan).
type TaskSummary struct {
	// HasTasks: the session ever backgrounded a command or created a
	// scheduled task (any task-related event).
	HasTasks bool `json:"hasTasks"`
	// BgCount: number of task_backgrounded events.
	BgCount int `json:"bgCount"`
	// BgRunning: task_backgrounded events without a matching
	// task_completed in the file whose output log is held open by a live
	// process (kernel-level liveness — the "still running" set).
	BgRunning int `json:"bgRunning"`
}

// grokHome returns the grok data dir (~/.grok by default; overridable for
// tests via GrokConfig.GrokHome).
func (b *Bridge) grokHome() string {
	if b.cfg.GrokHome != "" {
		return b.cfg.GrokHome
	}
	if b.homeDir == "" {
		return ""
	}
	return filepath.Join(b.homeDir, ".grok")
}

// EncodeCwdDirname percent-encodes a cwd exactly like the agent's
// urlencoding crate (unreserved = A-Za-z0-9-_.~, uppercase hex), so
// `sessions/<encoded>/<sessionId>/updates.jsonl` resolves to the same
// file the agent reads.
func EncodeCwdDirname(cwd string) string {
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(cwd))
	for i := 0; i < len(cwd); i++ {
		c := cwd[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hex[c>>4])
		b.WriteByte(hex[c&0x0f])
	}
	return b.String()
}

// sessionsCwdDir resolves the per-cwd sessions directory, mirroring
// xai-grok-config sessions_cwd_dir. Long cwds (encoded >255 bytes) use a
// slug-hash dirname with a `.cwd` metadata file — fall back to scanning
// those when the plain encoded name does not exist.
func sessionsCwdDir(grokHome, cwd string) string {
	if grokHome == "" {
		return ""
	}
	root := filepath.Join(grokHome, "sessions")
	enc := filepath.Join(root, EncodeCwdDirname(cwd))
	if st, err := os.Stat(enc); err == nil && st.IsDir() {
		return enc
	}
	// Hash-based dirs: find the one whose .cwd file matches.
	entries, err := os.ReadDir(root)
	if err != nil {
		return enc
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, e.Name(), ".cwd"))
		if err != nil {
			continue
		}
		if strings.TrimSpace(string(raw)) == cwd {
			return filepath.Join(root, e.Name())
		}
	}
	return enc
}

// sessionUpdatesFile returns the path of a session's updates.jsonl.
func sessionUpdatesFile(grokHome, cwd, sessionID string) string {
	dir := sessionsCwdDir(grokHome, cwd)
	if dir == "" || sessionID == "" {
		return ""
	}
	return filepath.Join(dir, sessionID, "updates.jsonl")
}

// SessionPlan 读取会话的 plan.md 正文（plan 模式产物，与 updates.jsonl 同
// 目录；TUI 的 /view-plan 读的就是盘上这个文件）。ok=false = 还没有 plan
// （文件不存在 / 目录定位不到 / sessionId 形状可疑）——不是错误，FE 据此
// 显示空态。sessionId 参与路径拼接，这里挡掉分隔符与 .. 防越界。
func (b *Bridge) SessionPlan(sessionID, cwd string) (string, bool) {
	if sessionID == "" || cwd == "" ||
		strings.ContainsAny(sessionID, `/\`) || strings.Contains(sessionID, "..") {
		return "", false
	}
	dir := sessionsCwdDir(b.grokHome(), cwd)
	if dir == "" {
		return "", false
	}
	raw, err := os.ReadFile(filepath.Join(dir, sessionID, "plan.md"))
	if err != nil {
		return "", false
	}
	return string(raw), true
}

// ── event parsing ───────────────────────────────────────────────────────

// parseTaskEvents scans a session's updates.jsonl and returns its
// task-lifecycle events on the rewind-surviving sequence (same
// sessionLineView as msgSeq / 状态条回合数)。超长行走整文件兜底。
func parseTaskEvents(path string) ([]TaskEvent, error) {
	view, err := sessionLineView(path)
	if err != nil {
		return nil, err
	}
	want := survivingFileRanks(view)
	byRank, err := collectTaskEventsByRank(path, want)
	if err != nil {
		return nil, err
	}
	out := make([]TaskEvent, 0, len(byRank))
	for _, idx := range view.order {
		if ev, ok := byRank[view.lines[idx].line]; ok {
			out = append(out, ev)
		}
	}
	return out, nil
}

func collectTaskEventsByRank(path string, want map[int]bool) (map[int]TaskEvent, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), maxUsageLineBytes)
	out := make(map[int]TaskEvent)
	rank := 0
	for sc.Scan() {
		line := sc.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		if want[rank] {
			if ev, ok := parseTaskEventLine(line); ok {
				out[rank] = ev
			}
		}
		rank++
	}
	if err := sc.Err(); err != nil {
		if !errors.Is(err, bufio.ErrTooLong) {
			return nil, err
		}
		return collectTaskEventsByRankReadAll(path, want)
	}
	return out, nil
}

func collectTaskEventsByRankReadAll(path string, want map[int]bool) (map[int]TaskEvent, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := make(map[int]TaskEvent)
	rank := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if want[rank] {
			if ev, ok := parseTaskEventLine([]byte(line)); ok {
				out[rank] = ev
			}
		}
		rank++
	}
	return out, nil
}

// parseTaskEventLine parses one storage envelope if it carries a
// task/scheduled event; ok=false otherwise.
func parseTaskEventLine(line []byte) (TaskEvent, bool) {
	var env struct {
		Timestamp int64 `json:"timestamp"`
		Params    struct {
			Update map[string]json.RawMessage `json:"update"`
		} `json:"params"`
	}
	if err := json.Unmarshal(line, &env); err != nil {
		return TaskEvent{}, false
	}
	upd := env.Params.Update
	var kind string
	if err := json.Unmarshal(upd["sessionUpdate"], &kind); err != nil {
		return TaskEvent{}, false
	}
	switch kind {
	case taskEventBackgrounded:
		ev := TaskEvent{Timestamp: env.Timestamp, Kind: kind}
		ev.TaskID = jsonStr(upd["task_id"])
		ev.Command = jsonStr(upd["command"])
		ev.Description = jsonStr(upd["description"])
		ev.MonitorDescription = jsonStr(upd["monitor_description"])
		if ev.MonitorDescription == "" {
			ev.MonitorDescription = jsonStr(upd["monitorDescription"])
		}
		ev.OutputFile = jsonStr(upd["output_file"])
		ev.Cwd = jsonStr(upd["cwd"])
		return ev, true
	case taskEventCompleted:
		var snap map[string]json.RawMessage
		_ = json.Unmarshal(upd["task_snapshot"], &snap)
		if snap == nil {
			return TaskEvent{}, false
		}
		ev := TaskEvent{Timestamp: env.Timestamp, Kind: kind}
		ev.TaskID = jsonStr(snap["task_id"])
		ev.Description = jsonStr(snap["description"])
		ev.Command = jsonStr(snap["display_command"])
		if ev.Command == "" {
			ev.Command = jsonStr(snap["command"])
		}
		ev.Signal = jsonStr(snap["signal"])
		ev.Output = jsonStr(snap["output"])
		if ec := jsonInt(snap["exit_code"]); ec != nil {
			ev.ExitCode = ec
		}
		if c := jsonBool(snap["completed"]); c != nil {
			ev.Completed = *c
		}
		return ev, true
	case taskEventMonitor:
		ev := TaskEvent{Timestamp: env.Timestamp, Kind: kind}
		ev.TaskID = jsonStr(upd["task_id"])
		ev.EventText = jsonStr(upd["event_text"])
		if ev.EventText == "" {
			ev.EventText = jsonStr(upd["eventText"])
		}
		return ev, true
	case taskEventSchedCreated, taskEventSchedDeleted, taskEventSchedFired:
		ev := TaskEvent{Timestamp: env.Timestamp, Kind: kind}
		ev.TaskID = jsonStr(upd["task_id"])
		ev.Prompt = jsonStr(upd["prompt"])
		ev.HumanSchedule = jsonStr(upd["human_schedule"])
		if ev.HumanSchedule == "" {
			ev.HumanSchedule = jsonStr(upd["humanSchedule"])
		}
		return ev, true
	default:
		return TaskEvent{}, false
	}
}

func jsonStr(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return ""
}

func jsonInt(raw json.RawMessage) *int {
	if len(raw) == 0 {
		return nil
	}
	var n int
	if json.Unmarshal(raw, &n) == nil {
		return &n
	}
	return nil
}

func jsonBool(raw json.RawMessage) *bool {
	if len(raw) == 0 {
		return nil
	}
	var b bool
	if json.Unmarshal(raw, &b) == nil {
		return &b
	}
	return nil
}
