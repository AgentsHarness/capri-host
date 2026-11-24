package acp

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

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
	// PID of the process holding the log open — set only while Running
	// and only when lsof is available (see probeOpenLogs).
	PID       int  `json:"pid,omitempty"`
	Failed    bool `json:"failed,omitempty"`
	Truncated bool `json:"truncated,omitempty"`
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
			if taskLogFresh(out.OutputFile) {
				open = map[string]logHolder{out.OutputFile: {}}
			} else {
				open = map[string]logHolder{}
			}
		}
		if h, alive := open[out.OutputFile]; alive {
			out.Running = true
			out.PID = h.PID
		}
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
