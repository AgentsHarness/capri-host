package acp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
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
	tagRewind       = `"sessionUpdate":"rewind_marker"`
	tagUserChunk    = `"sessionUpdate":"user_message_chunk"`
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
	tagRewindB       = []byte(tagRewind)
	tagUserChunkB    = []byte(tagUserChunk)
)

// TaskEvent is one persisted task-lifecycle event, in file order. The
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

// ── rewind dead-branch filtering ────────────────────────────────────────
//
// Mirrors the agent's filter_rewind_lines: lines after a rewind_marker
// belong to a dead branch and are dropped until the target prompt run.
// Skipped entirely (zero cost) when the file has no rewind markers.

type rewindLine struct {
	line        string
	promptIndex *int64
}

func classifyRewindLine(line []byte) (isRewind bool, isUser bool, promptIdx *int64) {
	switch {
	case bytes.Contains(line, tagRewindB):
		return true, false, nil
	case bytes.Contains(line, tagUserChunkB):
		idx := extractPromptIndex(line)
		return false, true, idx
	default:
		return false, false, nil
	}
}

// extractPromptIndex reads params._meta.promptIndex from a user chunk
// envelope (best-effort).
func extractPromptIndex(line []byte) *int64 {
	// Fast path: the tag is absent — nothing to extract.
	if !bytes.Contains(line, []byte(`"promptIndex"`)) {
		return nil
	}
	var env struct {
		Params struct {
			Meta map[string]json.RawMessage `json:"_meta"`
		} `json:"params"`
	}
	if json.Unmarshal(line, &env) != nil {
		return nil
	}
	raw, ok := env.Params.Meta["promptIndex"]
	if !ok {
		return nil
	}
	var idx int64
	if json.Unmarshal(raw, &idx) != nil {
		return nil
	}
	return &idx
}

// extractRewindTarget reads the rewind marker's target_prompt_index.
func extractRewindTarget(line []byte) *int64 {
	if !bytes.Contains(line, []byte(`"target_prompt_index"`)) {
		return nil
	}
	var env struct {
		Params struct {
			Update map[string]json.RawMessage `json:"update"`
		} `json:"params"`
	}
	if json.Unmarshal(line, &env) != nil {
		return nil
	}
	raw, ok := env.Params.Update["target_prompt_index"]
	if !ok {
		return nil
	}
	var idx int64
	if json.Unmarshal(raw, &idx) != nil {
		return nil
	}
	return &idx
}

// ── event parsing ───────────────────────────────────────────────────────

// parseTaskEvents scans a session's updates.jsonl and returns its
// task-lifecycle events in file order (rewind dead branches filtered).
// Streamed line-by-line with bufio.Scanner — no ReadFile + Split
// double-copy of multi-hundred-MB files (the scanUsageFile pattern).
// A line longer than maxUsageLineBytes falls back to the whole-file
// path, which has no line-length limit.
func parseTaskEvents(path string) ([]TaskEvent, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), maxUsageLineBytes)
	var filt rewindFilter
	for sc.Scan() {
		filt.feed(sc.Bytes())
	}
	if err := sc.Err(); err != nil {
		if !errors.Is(err, bufio.ErrTooLong) {
			return nil, err
		}
		return parseTaskEventsReadAll(path)
	}
	return filt.events, nil
}

// parseTaskEventsReadAll is the whole-file fallback for parseTaskEvents,
// reached only when a single line exceeds maxUsageLineBytes. It feeds the
// same rewindFilter as the streaming path — the truncation semantics live
// in exactly one place.
func parseTaskEventsReadAll(path string) ([]TaskEvent, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var filt rewindFilter
	for _, line := range strings.Split(string(raw), "\n") {
		filt.feed([]byte(line))
	}
	return filt.events, nil
}

// rewindFilter keeps only the task events of live-branch lines (agent's
// filter_rewind_lines semantics): lines are fed one at a time and the task
// events of dead-branch lines (after a rewind_marker) are dropped. For a
// file without rewind markers every line's events are kept unchanged. Only
// the parsed events are retained (never the raw lines), so memory stays
// proportional to the event count instead of the file size.
type rewindFilter struct {
	events   []TaskEvent
	evStarts []int // len(events) at each user-message run start
	inUser   bool
	lastIdx  *int64
}

func (f *rewindFilter) feed(line []byte) {
	isRewind, isUser, promptIdx := classifyRewindLine(line)
	switch {
	case isRewind:
		target := 0
		if idx := extractRewindTarget(line); idx != nil {
			target = int(*idx)
		}
		// Truncate at the target prompt run's start. evStarts holds
		// event counts (events is 1:1 with the live-branch lines, so the
		// event index at a run start is exactly the count of events that
		// preceded it).
		trunc := len(f.events)
		if target < len(f.evStarts) {
			trunc = f.evStarts[target]
		}
		f.events = f.events[:trunc]
		f.evStarts = f.evStarts[:min(target, len(f.evStarts))]
		f.inUser = false
	case isUser:
		newRun := !f.inUser
		if promptIdx != nil && f.lastIdx != nil && *promptIdx != *f.lastIdx {
			newRun = true
		}
		if newRun {
			f.evStarts = append(f.evStarts, len(f.events))
		}
		if ev, ok := parseTaskEventLine(line); ok {
			f.events = append(f.events, ev)
		}
		f.inUser = true
		f.lastIdx = promptIdx
	default:
		if ev, ok := parseTaskEventLine(line); ok {
			f.events = append(f.events, ev)
		}
		f.inUser = false
	}
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

// ── task liveness probe ─────────────────────────────────────────────────
//
// The host cannot ask a terminal backend about the TUI's tasks (they
// belong to the TUI's process tree). The precise, write-independent
// signal is the OS file table: the spawning shell keeps the task's
// output log open for the whole task lifetime, so `lsof <log>` reports
// the owning process iff the task is still alive. No log WRITES are
// required — a silent `sleep 9999` keeps its fd open. Fallback (lsof
// missing): the log-mtime heuristic, clearly less accurate.

// lsofTimeout bounds one lsof spawn (network mounts can hang).
const lsofTimeout = 5 * time.Second

// probeOpenLogs returns the subset of paths currently held open by a
// live process (kernel-level liveness). Returns nil when lsof is
// unavailable so callers can fall back. Paths are canonicalized before
// comparison (macOS lsof reports /var/... as /private/var/...).
func probeOpenLogs(paths []string) map[string]bool {
	open := make(map[string]bool, len(paths))
	if len(paths) == 0 {
		return open
	}
	canon := func(p string) string {
		if r, err := filepath.EvalSymlinks(p); err == nil {
			return r
		}
		return filepath.Clean(p)
	}
	inputs := make([]string, len(paths))
	for i, p := range paths {
		inputs[i] = canon(p)
	}
	ctx, cancel := context.WithTimeout(context.Background(), lsofTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "lsof", inputs...).Output()
	if err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return nil // no lsof — caller falls back
		}
		// lsof's exit code is NOT a reliable "no match" signal with
		// multiple operands (it exits 1 when ANY file matches nothing,
		// while still printing the matches on stdout). Parse stdout
		// regardless; an empty match set falls out naturally.
	}
	matched := make(map[string]bool, len(paths))
	for _, line := range strings.Split(string(out), "\n") {
		f := strings.Fields(line)
		// Header row: COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME.
		if len(f) < 9 || f[0] == "COMMAND" {
			continue
		}
		matched[canon(f[len(f)-1])] = true
	}
	for _, p := range paths {
		if matched[canon(p)] {
			open[p] = true
		}
	}
	return open
}

// fallbackFreshness: mtime heuristic used only when lsof is unavailable.
const fallbackFreshness = 15 * time.Minute

// taskLogFresh is the mtime fallback for probeOpenLogs.
func taskLogFresh(outputFile string) bool {
	if outputFile == "" {
		return false
	}
	st, err := os.Stat(outputFile)
	if err != nil {
		return false
	}
	return time.Since(st.ModTime()) <= fallbackFreshness
}

// orphanedTasks returns the task_backgrounded events with no matching
// task_completed (the "orphans" — same notion as the TUI's
// find_orphaned_background_tasks).
func orphanedTasks(events []TaskEvent) []TaskEvent {
	done := make(map[string]bool, len(events))
	for _, e := range events {
		if e.Kind == taskEventCompleted && e.TaskID != "" {
			done[e.TaskID] = true
		}
	}
	var orphans []TaskEvent
	for _, e := range events {
		if e.Kind == taskEventBackgrounded && e.TaskID != "" && !done[e.TaskID] {
			orphans = append(orphans, e)
		}
	}
	return orphans
}

// runningTasks filters orphans by the open-fd liveness probe and marks
// the survivors Running.
func runningTasks(events []TaskEvent) []TaskEvent {
	orphans := orphanedTasks(events)
	if len(orphans) == 0 {
		return nil
	}
	paths := make([]string, 0, len(orphans))
	for _, e := range orphans {
		if e.OutputFile != "" {
			paths = append(paths, e.OutputFile)
		}
	}
	open := probeOpenLogs(paths)
	if open == nil {
		// No lsof: fall back to the mtime heuristic.
		open = make(map[string]bool, len(paths))
		for _, p := range paths {
			if taskLogFresh(p) {
				open[p] = true
			}
		}
	}
	out := make([]TaskEvent, 0, len(orphans))
	for _, e := range orphans {
		if e.OutputFile != "" && open[e.OutputFile] {
			e.Running = true
			out = append(out, e)
		}
	}
	return out
}

// scanTaskCensus is a tag-filtered census for the history sidebar badge:
// only lines carrying task tags are JSON-parsed (the tags are exact —
// quotes inside JSON strings are escaped — so message text cannot
// false-positive). Returns the counts plus the orphan log paths for a
// shared liveness probe (one lsof for all sessions). Streamed with
// bufio.Scanner like parseTaskEvents; a line longer than
// maxUsageLineBytes falls back to the whole-file path.
func scanTaskCensus(path string) (sum TaskSummary, orphanPaths []string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return TaskSummary{}, nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), maxUsageLineBytes)
	var events []TaskEvent
	for sc.Scan() {
		l := sc.Bytes()
		if !bytes.Contains(l, tagBackgroundedB) &&
			!bytes.Contains(l, tagCompletedB) &&
			!bytes.Contains(l, tagSchedCreatedB) &&
			!bytes.Contains(l, tagSchedDeletedB) &&
			!bytes.Contains(l, tagSchedFiredB) {
			continue
		}
		if ev, ok := parseTaskEventLine(l); ok {
			events = append(events, ev)
		}
	}
	if err := sc.Err(); err != nil {
		if !errors.Is(err, bufio.ErrTooLong) {
			return TaskSummary{}, nil, err
		}
		return scanTaskCensusReadAll(path)
	}
	sum, orphanPaths = censusEvents(events)
	return sum, orphanPaths, nil
}

// scanTaskCensusReadAll is the whole-file fallback for scanTaskCensus
// (reached only when a single line exceeds maxUsageLineBytes).
func scanTaskCensusReadAll(path string) (TaskSummary, []string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return TaskSummary{}, nil, err
	}
	var events []TaskEvent
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.Contains(line, tagBackgrounded) &&
			!strings.Contains(line, tagCompleted) &&
			!strings.Contains(line, tagSchedCreated) &&
			!strings.Contains(line, tagSchedDeleted) &&
			!strings.Contains(line, tagSchedFired) {
			continue
		}
		if ev, ok := parseTaskEventLine([]byte(line)); ok {
			events = append(events, ev)
		}
	}
	sum, orphanPaths := censusEvents(events)
	return sum, orphanPaths, nil
}

// censusEvents reduces a task-event list into the badge census counts and
// the orphan log paths (shared by the streaming and fallback paths).
func censusEvents(events []TaskEvent) (TaskSummary, []string) {
	var sum TaskSummary
	for _, e := range events {
		switch e.Kind {
		case taskEventBackgrounded:
			sum.BgCount++
		case taskEventSchedCreated, taskEventSchedDeleted, taskEventSchedFired:
			sum.HasTasks = true
		}
	}
	sum.HasTasks = sum.HasTasks || sum.BgCount > 0
	var orphanPaths []string
	for _, e := range orphanedTasks(events) {
		if e.OutputFile != "" {
			orphanPaths = append(orphanPaths, e.OutputFile)
		}
	}
	return sum, orphanPaths
}

// applyProbe fills TaskSummary.BgRunning from a pre-computed open-set.
func applyProbe(sum TaskSummary, orphanPaths []string, open map[string]bool) TaskSummary {
	for _, p := range orphanPaths {
		if open[p] {
			sum.BgRunning++
		}
	}
	return sum
}

// SessionRunningTasks returns the session's tasks that are STILL RUNNING:
// task_backgrounded events without a matching task_completed whose output
// log is held open by a live process (lsof probe). This is the web
// equivalent of the TUI's live tasks pane — the timeline is only used to
// surface current work, not to dump history into the scrollback. Missing
// files yield an empty list, not an error, so the frontend stays resilient
// when the session predates this host or lives in another workspace.
func (b *Bridge) SessionRunningTasks(sessionID, cwd string) ([]TaskEvent, error) {
	path := sessionUpdatesFile(b.grokHome(), cwd, sessionID)
	if path == "" {
		return nil, fmt.Errorf("无法解析会话目录 (home=%q)", b.grokHome())
	}
	events, err := parseTaskEvents(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []TaskEvent{}, nil
		}
		return nil, fmt.Errorf("读取会话任务时间线失败: %w", err)
	}
	return runningTasks(events), nil
}

// sessionTaskCensus computes the badge census for one session item:
// counts + the orphan log paths for the shared liveness probe.
// Failures (missing file, unreadable) are silent — the badge is cosmetic.
func sessionTaskCensus(grokHome, cwd, sessionID string) (TaskSummary, []string) {
	path := sessionUpdatesFile(grokHome, cwd, sessionID)
	if path == "" {
		return TaskSummary{}, nil
	}
	sum, orphanPaths, err := scanTaskCensus(path)
	if err != nil {
		return TaskSummary{}, nil
	}
	return sum, orphanPaths
}

// probeOrphanPaths runs the open-fd liveness probe over every session's
// orphan log paths in ONE lsof invocation and returns the open set
// (mtime fallback when lsof is unavailable).
func probeOrphanPaths(all map[censusKey][]string) map[string]bool {
	paths := make([]string, 0, 16)
	for _, ps := range all {
		paths = append(paths, ps...)
	}
	if len(paths) == 0 {
		return map[string]bool{}
	}
	open := probeOpenLogs(paths)
	if open == nil {
		open = make(map[string]bool, len(paths))
		for _, p := range paths {
			if taskLogFresh(p) {
				open[p] = true
			}
		}
	}
	return open
}

// censusKey identifies one session in the badge census / orphan probe.
type censusKey struct{ sid, cwd string }

// ── per-task log reconstruction (block viewer) ─────────────────────────
//
// The FE block viewer needs one task's FULL stdout regardless of which
// history page is loaded (pages are 100 envelopes; task_backgrounded and
// its completion often straddle pages, and the TUI-held tasks are not in
// this host's live registry at all). Reconstruct from the session's
// persisted timeline + the on-disk log — pagination never applies.

// TaskLog is one task's reconstructed log for the block viewer. Fields
// mirror the FE TaskSnapshot normalization (camelCase on the wire).
type TaskLog struct {
	TaskID      string `json:"taskId"`
	Command     string `json:"command,omitempty"`
	Description string `json:"description,omitempty"`
	OutputFile  string `json:"outputFile,omitempty"`
	Output      string `json:"output,omitempty"`
	Completed   bool   `json:"completed,omitempty"`
	Running     bool   `json:"running,omitempty"`
	Failed      bool   `json:"failed,omitempty"`
	Truncated   bool   `json:"truncated,omitempty"`
}

// taskLogMaxBytes caps one reconstructed log so a giant output file
// cannot OOM the host; the viewer scrolls whatever we return.
const taskLogMaxBytes = 32 << 20

// ErrTaskLogNotFound: the session timeline has no events for the task id.
var ErrTaskLogNotFound = errors.New("任务日志不存在（时间线中无该任务）")

// TaskLog reconstructs one task's full stdout for the block viewer.
// Source priority:
//  1. the task's output log file on disk — the authoritative full
//     stdout (same file the TUI pager writes and the liveness probe
//     watches, so this also covers TUI-held tasks);
//  2. accumulated monitor_event chunks, merged with the task_completed
//     snapshot (snapshot wins when longer — the FE's own rule).
//
// Running is kernel-liveness probed (log held open by a live process)
// when no completion exists in the timeline; the FE keeps polling while
// true so an open viewer follows a still-streaming task.
func (b *Bridge) TaskLog(sessionID, cwd, taskID string) (*TaskLog, error) {
	path := sessionUpdatesFile(b.grokHome(), cwd, sessionID)
	if path == "" {
		return nil, fmt.Errorf("无法解析会话目录 (home=%q)", b.grokHome())
	}
	events, err := parseTaskEvents(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrTaskLogNotFound
		}
		return nil, fmt.Errorf("读取会话任务时间线失败: %w", err)
	}

	var bg, done *TaskEvent
	var monitor strings.Builder
	for i := range events {
		e := &events[i]
		if e.TaskID != taskID {
			continue
		}
		switch e.Kind {
		case taskEventBackgrounded:
			bg = e
		case taskEventMonitor:
			monitor.WriteString(e.EventText)
		case taskEventCompleted:
			done = e
		}
	}
	if bg == nil && done == nil && monitor.Len() == 0 {
		return nil, ErrTaskLogNotFound
	}

	out := &TaskLog{TaskID: taskID}
	if bg != nil {
		out.Command = bg.Command
		out.Description = bg.Description
		out.OutputFile = bg.OutputFile
	}
	if done != nil {
		out.Completed = true
		out.Failed = (done.ExitCode != nil && *done.ExitCode != 0) || done.Signal != ""
	}

	// Authoritative on-disk log first; timeline accumulation as fallback.
	timeline := taskTimelineOutput(monitor.String(), done)
	if out.OutputFile != "" {
		if data, rerr := readTaskLogFile(out.OutputFile); rerr == nil && len(data) > 0 {
			out.Output = data
			out.Truncated = len(data) >= taskLogMaxBytes
		} else {
			out.Output = timeline
		}
	} else {
		out.Output = timeline
	}

	// Liveness: no completion in the file + log still held open by a
	// live process (kernel-level; mtime fallback without lsof).
	if !out.Completed && out.OutputFile != "" {
		open := probeOpenLogs([]string{out.OutputFile})
		if open == nil {
			open = map[string]bool{out.OutputFile: taskLogFresh(out.OutputFile)}
		}
		out.Running = open[out.OutputFile]
	}
	return out, nil
}

// taskTimelineOutput merges monitor stdout + the completion snapshot,
// mirroring the FE rule: the snapshot wins when it is longer.
func taskTimelineOutput(monitor string, done *TaskEvent) string {
	if done != nil && done.Output != "" && len(done.Output) >= len(monitor) {
		return done.Output
	}
	return monitor
}

// readTaskLogFile reads a task's on-disk log, capped at taskLogMaxBytes.
func readTaskLogFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, taskLogMaxBytes))
	if err != nil {
		return "", err
	}
	return string(data), nil
}
