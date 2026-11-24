package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/AgentsHarness/capri-host/internal/procattr"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

// shellBody is POST /api/shell — local command execution, NOT routed
// through the agent.
type shellBody struct {
	Command   string `json:"command"`
	Cwd       string `json:"cwd"`
	TimeoutMs int    `json:"timeoutMs"`
}

const (
	shellDefaultTimeout = 10 * time.Second
	shellMaxTimeout     = 60 * time.Second
	// shellMaxOutput caps each captured stream (stdout and stderr alike) so
	// a runaway command cannot balloon memory; overflow truncates the
	// response and flags it via `truncated`.
	shellMaxOutput = 16 << 20 // 16 MiB
)

// cappedBuffer is a bytes.Buffer that stores at most max bytes and records
// whether any write overflowed. Writes past the cap are dropped but report
// the full length, so exec's io.Copy keeps draining the pipe and the child
// never blocks on a full OS pipe buffer.
type cappedBuffer struct {
	buf      bytes.Buffer
	max      int
	overflow bool
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	if c.buf.Len() >= c.max {
		c.overflow = true
		return len(p), nil
	}
	room := c.max - c.buf.Len()
	if len(p) > room {
		c.buf.Write(p[:room])
		c.overflow = true
		return len(p), nil
	}
	return c.buf.Write(p)
}

// handleShell runs a command locally via os/exec (sh -c) — a pure host
// facility that never touches the agent. Body: {command, cwd?, timeoutMs?}
// (cwd defaults to the active session's cwd, timeout defaults to 10s and
// is capped at 60s). Like handlePrompt, the command runs on an independent
// context, so a browser disconnect just releases the handler while the
// command keeps running; only the timeout stops it. No stdin is attached.
func (s *Server) handleShell(w http.ResponseWriter, r *http.Request) {
	var body shellBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if strings.TrimSpace(body.Command) == "" {
		writeJSON(w, 400, map[string]any{"ok": false, "error": "command 不能为空"})
		return
	}
	cwd := body.Cwd
	if cwd == "" {
		cwd = s.bridge.Snapshot().Cwd // active session cwd
	}
	if cwd == "" {
		wd, err := os.Getwd()
		if err == nil {
			cwd = wd
		}
	}
	timeout := shellDefaultTimeout
	if body.TimeoutMs > 0 {
		t := time.Duration(body.TimeoutMs) * time.Millisecond
		if t > shellMaxTimeout {
			t = shellMaxTimeout
		}
		timeout = t
	}

	type shellResult struct {
		exitCode  int
		stdout    string
		stderr    string
		timedOut  bool
		truncated bool
	}
	done := make(chan shellResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		cmd := exec.CommandContext(ctx, "sh", "-c", body.Command)
		cmd.Dir = cwd
		// Both streams are captured below, so the child needs no console
		// window — without this the GUI build flashes a terminal per call.
		procattr.HideConsole(cmd)
		// stdout and stderr share the same 16 MiB cap; stderr is folded into
		// the same truncation accounting, so `truncated` flips when either
		// stream overflows.
		stdout := &cappedBuffer{max: shellMaxOutput}
		stderr := &cappedBuffer{max: shellMaxOutput}
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		// No stdin by design.
		res := shellResult{}
		if err := cmd.Run(); err != nil {
			var ee *exec.ExitError
			if errors.As(err, &ee) {
				res.exitCode = ee.ExitCode()
			} else {
				res.exitCode = -1
			}
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				res.timedOut = true
			}
		}
		res.stdout = stdout.buf.String()
		res.stderr = stderr.buf.String()
		res.truncated = stdout.overflow || stderr.overflow
		done <- res
	}()

	select {
	case <-r.Context().Done():
		// Client went away mid-run; the command continues in the background.
		return
	case res := <-done:
		writeJSON(w, 200, map[string]any{
			"ok":        true,
			"exitCode":  res.exitCode,
			"stdout":    res.stdout,
			"stderr":    res.stderr,
			"timedOut":  res.timedOut,
			"truncated": res.truncated,
		})
	}
}

// firstAny returns the first present value under any of the given keys.
func firstAny(m map[string]any, keys ...string) any {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			return v
		}
	}
	return nil
}

// asInt64 is the lenient int reader for JSON-decoded numbers.
func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i, true
		}
	case int:
		return int64(n), true
	case int64:
		return n, true
	}
	return 0, false
}
