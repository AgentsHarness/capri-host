package acp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
// live process (kernel-level liveness) mapped to the holding process.
// Used as a fallback when BackgroundTasks snapshot is not available.
func probeOpenLogs(paths []string) map[string]logHolder {
	open := make(map[string]logHolder, len(paths))
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
			return nil
		}
	}
	holders := make(map[string]logHolder, len(paths))
	for _, line := range strings.Split(string(out), "\n") {
		f := strings.Fields(line)
		if len(f) < 9 || f[0] == "COMMAND" {
			continue
		}
		path := canon(f[len(f)-1])
		if _, seen := holders[path]; seen {
			continue
		}
		h := logHolder{Command: f[0]}
		if pid, err := strconv.Atoi(f[1]); err == nil {
			h.PID = pid
		}
		holders[path] = h
	}
	for _, p := range paths {
		if h, ok := holders[canon(p)]; ok {
			open[p] = h
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
		// No lsof: fall back to the mtime heuristic (no PID attribution).
		open = make(map[string]logHolder, len(paths))
		for _, p := range paths {
			if taskLogFresh(p) {
				open[p] = logHolder{}
			}
		}
	}
	out := make([]TaskEvent, 0, len(orphans))
	for _, e := range orphans {
		if e.OutputFile == "" {
			continue
		}
		if h, alive := open[e.OutputFile]; alive {
			e.Running = true
			e.PID = h.PID
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

// applyProbe fills TaskSummary.BgRunning from a pre-computed open-set
// (membership is the liveness verdict; the holder identity is unused here).
func applyProbe(sum TaskSummary, orphanPaths []string, open map[string]logHolder) TaskSummary {
	for _, p := range orphanPaths {
		if _, alive := open[p]; alive {
			sum.BgRunning++
		}
	}
	return sum
}

// SessionRunningTasks returns the session's tasks that are STILL RUNNING.
// Authoritative source is the in-memory BackgroundTasks snapshot (SessionUpdate::BackgroundTasks);
// falls back to parsing updates.jsonl timeline if no snapshot is available.
func (b *Bridge) SessionRunningTasks(sessionID, cwd string) ([]TaskEvent, error) {
	b.mu.Lock()
	s := b.sessions[sessionID]
	var snapshot []any
	if s != nil && s.backgroundTasks != nil {
		snapshot = s.backgroundTasks
	}
	b.mu.Unlock()

	if snapshot != nil {
		out := make([]TaskEvent, 0, len(snapshot))
		for _, item := range snapshot {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			status, _ := m["status"].(string)
			if status == "running" {
				tid, _ := m["task_id"].(string)
				if tid == "" {
					tid, _ = m["taskId"].(string)
				}
				cmd, _ := m["command"].(string)
				dispCmd, _ := m["display_command"].(string)
				if dispCmd != "" {
					cmd = dispCmd
				}
				desc, _ := m["description"].(string)
				outPath, _ := m["output_file"].(string)
				taskCwd, _ := m["cwd"].(string)
				out = append(out, TaskEvent{
					Kind:        taskEventBackgrounded,
					TaskID:      tid,
					Command:     cmd,
					Description: desc,
					OutputFile:  outPath,
					Cwd:         taskCwd,
					Running:     true,
				})
			}
		}
		return out, nil
	}

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

// DetachedRunningTasks returns the still-running tasks of a session that
// the agent process does NOT hold in its live registry — background
// commands left behind by a previous grok generation (a hard-killed or
// crashed agent never ran its ProcessScope reaper) or held by another
// client's process (an open TUI on the same session).
//
// The frontend shows only registry-owned tasks (it can kill those); these
// are surfaced as a one-off hint instead, so nothing looks controllable
// that is not. Ownership can only be judged against the ACTIVE session —
// x.ai/task/list takes no session override — so probing any other session
// reports nothing rather than mislabelling that session's own tasks.
func (b *Bridge) DetachedRunningTasks(ctx context.Context, sessionID, cwd string) []TaskEvent {
	running, err := b.SessionRunningTasks(sessionID, cwd)
	if err != nil || len(running) == 0 {
		return nil
	}
	if b.ActiveSessionID() != sessionID {
		return nil
	}
	res, err := b.TaskList(ctx, sessionID)
	if err != nil {
		return nil
	}
	owned := liveRegistryTaskIDs(res)
	out := make([]TaskEvent, 0, len(running))
	for _, e := range running {
		if e.TaskID == "" || owned[e.TaskID] {
			continue
		}
		out = append(out, e)
	}
	return out
}

// liveRegistryTaskIDs walks an x.ai/task/list reply and returns every task
// id the agent's registry knows (running or still inside its completed
// tombstone TTL). A missing or garbled payload yields an empty set —
// callers treat "cannot attribute" as "report nothing".
func liveRegistryTaskIDs(res map[string]any) map[string]bool {
	ids := map[string]bool{}
	// The agent answers through the ExtMethodResult envelope and the bridge
	// may or may not have unwrapped it, so the task array can live at
	// `.tasks`, `.result.tasks` or `.result.result.tasks`.
	for node, depth := res, 0; node != nil && depth < 3; node, depth = nestedResultMap(node), depth+1 {
		raw, ok := node["tasks"].([]any)
		if !ok {
			continue
		}
		for _, item := range raw {
			t, ok := item.(map[string]any)
			if !ok {
				continue
			}
			id, _ := t["task_id"].(string)
			if id == "" {
				id, _ = t["taskId"].(string)
			}
			if id != "" {
				ids[id] = true
			}
		}
	}
	return ids
}

// nestedResultMap unwraps one {result: {...}} envelope layer, returning an
// empty map when there is none.
func nestedResultMap(res map[string]any) map[string]any {
	if res == nil {
		return map[string]any{}
	}
	inner, ok := res["result"].(map[string]any)
	if !ok {
		return map[string]any{}
	}
	return inner
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

// censusEntry is one session's cached census keyed by the source file's
// (size, mtime). An updated file always changes size, and mtime catches the
// rewrite-with-same-size case, so a hit means "this is the same timeline we
// already reduced".
type censusEntry struct {
	size    int64
	modTime time.Time
	sum     TaskSummary
	orphans []string
}

// taskCensusCache memoizes sessionTaskCensus against updates.jsonl's
// (size, mtime). ListSessions used to re-parse every session's whole
// timeline on every call: 30 sessions / 92 MB took ~0.5 s, and every
// sessions_changed event paid it again. The badge is derived data — a cache
// hit costs one stat per session.
//
// The cache is bounded by the roster, except that a deleted session file's
// entry is dropped on the next stat failure (see sessionTaskCensusCached).
type taskCensusCache struct {
	mu      sync.Mutex
	entries map[string]*censusEntry
}

func (tc *taskCensusCache) get(path string, size int64, mod time.Time) (TaskSummary, []string, bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	ent, ok := tc.entries[path]
	if !ok || ent.size != size || !ent.modTime.Equal(mod) {
		return TaskSummary{}, nil, false
	}
	return ent.sum, ent.orphans, true
}

func (tc *taskCensusCache) put(path string, size int64, mod time.Time, sum TaskSummary, orphans []string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.entries == nil {
		tc.entries = make(map[string]*censusEntry)
	}
	tc.entries[path] = &censusEntry{size: size, modTime: mod, sum: sum, orphans: orphans}
}

// drop forgets path after its file disappeared.
func (tc *taskCensusCache) drop(path string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	delete(tc.entries, path)
}

// sessionTaskCensusCached is sessionTaskCensus with the (size, mtime) cache.
// A genuinely missing file drops its cached entry; other stat errors return
// the zero summary without touching the cache.
func (b *Bridge) sessionTaskCensusCached(grokHome, cwd, sessionID string) (TaskSummary, []string) {
	path := sessionUpdatesFile(grokHome, cwd, sessionID)
	if path == "" {
		return TaskSummary{}, nil
	}
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			b.census.drop(path)
		}
		return TaskSummary{}, nil
	}
	size, mod := st.Size(), st.ModTime()
	if sum, orphans, ok := b.census.get(path, size, mod); ok {
		return sum, orphans
	}
	sum, orphanPaths, err := scanTaskCensus(path)
	if err != nil {
		return TaskSummary{}, nil
	}
	b.census.put(path, size, mod, sum, orphanPaths)
	return sum, orphanPaths
}

// probeOrphanPaths runs the open-fd liveness probe over every session's
// orphan log paths in ONE lsof invocation and returns the holders keyed by
// path (mtime fallback when lsof is unavailable, without PID attribution).
func probeOrphanPaths(all map[censusKey][]string) map[string]logHolder {
	paths := make([]string, 0, 16)
	for _, ps := range all {
		paths = append(paths, ps...)
	}
	if len(paths) == 0 {
		return map[string]logHolder{}
	}
	open := probeOpenLogs(paths)
	if open == nil {
		open = make(map[string]logHolder, len(paths))
		for _, p := range paths {
			if taskLogFresh(p) {
				open[p] = logHolder{}
			}
		}
	}
	return open
}

// censusKey identifies one session in the badge census / orphan probe.
type censusKey struct{ sid, cwd string }
