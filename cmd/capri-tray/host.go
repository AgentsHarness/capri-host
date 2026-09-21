//go:build windows

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/AgentsHarness/capri-host/internal/procattr"
)

// hostProc supervises the Capri-host child process.
//
// This is the whole reason the tray is a separate binary. Owning the child is
// what makes the host restartable (a settings change, a crash, a port that was
// held by something else a minute ago), what keeps a failed start from being a
// silent nothing, and what turns the host's stderr into a log file it knows
// nothing about.
type hostProc struct {
	bin string
	log *rotatingFile

	mu      sync.Mutex
	cmd     *exec.Cmd
	done    chan struct{} // closed when the current child exits
	stopped bool          // true when the exit was requested, not a crash
	code    int
	waitErr error

	// port and token are what this child bound at start. Stop and status
	// talk to that listener, not to whatever config.json says now.
	port  int
	token string
}

func newHostProc(bin string, log *rotatingFile) *hostProc {
	return &hostProc{bin: bin, log: log}
}

// running reports whether a child is currently alive.
func (h *hostProc) running() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.cmd != nil
}

// logPath is where this host's output is being written.
func (h *hostProc) logPath() string {
	if h.log == nil {
		return ""
	}
	return h.log.Path()
}

// lastError describes how the current (or most recent) child ended, empty when
// it is still running or exited on request.
func (h *hostProc) lastError() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cmd != nil || h.stopped {
		return ""
	}
	if h.waitErr != nil {
		return h.waitErr.Error()
	}
	if h.code != 0 {
		return fmt.Sprintf("Capri-host 退出码 %d（详见日志）", h.code)
	}
	return "Capri-host 已退出"
}

// endpoint is the listener this child was started with. ok is false before
// the first successful start.
func (h *hostProc) endpoint() (port int, token string, ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.port <= 0 {
		return 0, "", false
	}
	return h.port, h.token, true
}

// start launches the host. It is an error to call it while one is running.
func (h *hostProc) start() error {
	if _, err := os.Stat(h.bin); err != nil {
		return fmt.Errorf("找不到 Capri-host：%s", h.bin)
	}
	settings, err := loadSettings()
	if err != nil {
		return err
	}
	port := settings.ListenPort()
	token := strings.TrimSpace(settings.FEToken)

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cmd != nil {
		return nil
	}

	cmd := exec.Command(h.bin)
	// The child's working directory is the log directory rather than wherever
	// Explorer happened to launch the tray from, so a relative path it prints
	// is predictable.
	cmd.Dir = filepath.Dir(h.log.Path())
	cmd.Stdout = h.log
	cmd.Stderr = h.log
	// CREATE_NO_WINDOW. The tray is a GUI-subsystem binary with no console, so
	// without this Windows allocates a fresh console for the console-subsystem
	// host and the default terminal flashes it on screen. The host's own
	// children (grok, git) then inherit that hidden console, which is what
	// keeps them from popping windows of their own.
	procattr.HideConsole(cmd)

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("启动 Capri-host 失败: %w", err)
	}

	h.cmd = cmd
	h.done = make(chan struct{})
	h.stopped = false
	h.code = 0
	h.waitErr = nil
	h.port = port
	h.token = token
	done := h.done

	logf("已启动 Capri-host pid=%d bin=%s", cmd.Process.Pid, h.bin)

	go func() {
		err := cmd.Wait()
		h.mu.Lock()
		h.waitErr = err
		h.code = cmd.ProcessState.ExitCode()
		if h.cmd == cmd {
			h.cmd = nil
		}
		h.mu.Unlock()
		close(done)
		logf("Capri-host 已退出 pid=%d code=%d err=%v", cmd.Process.Pid, cmd.ProcessState.ExitCode(), err)
	}()
	return nil
}

// stop asks the host to shut down and waits for it.
//
// It asks over HTTP rather than signalling: Windows does not deliver SIGTERM
// to a console child, and a hard kill would orphan the grok process the host
// owns, because terminating a parent does not terminate its children there.
// Only if the polite request goes unanswered does this escalate to Kill.
func (h *hostProc) stop(wait time.Duration) error {
	h.mu.Lock()
	cmd, done := h.cmd, h.done
	port, token := h.port, h.token
	if cmd != nil {
		h.stopped = true
	}
	h.mu.Unlock()
	if cmd == nil {
		return nil
	}

	go requestQuit(baseURL(port), token)

	select {
	case <-done:
		return nil
	case <-time.After(wait):
	}
	logf("Capri-host 未在 %s 内退出，强制结束 pid=%d", wait, cmd.Process.Pid)
	if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("结束 Capri-host 失败: %w", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		return errors.New("Capri-host 未能结束")
	}
	return nil
}

// restart stops the host if it is up and starts it again.
func (h *hostProc) restart(wait time.Duration) error {
	if err := h.stop(wait); err != nil {
		return err
	}
	return h.start()
}

// requestQuit posts the graceful-shutdown request. Failures are ignored: the
// caller falls back to Kill, and a host that is not listening has nothing to
// hear the request anyway.
func requestQuit(baseURL, token string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/api/host/quit", strings.NewReader("{}"))
	if err != nil {
		return
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	if err == nil {
		_ = res.Body.Close()
	}
}
