// Untagged on purpose: this is os/filepath/sync work with no Windows API in
// it, and rotation is exactly the kind of logic that is wrong in a way nobody
// notices until a log has grown for a month. `go test ./...` reaches it here.

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// rotatingFile is the log sink the supervised host writes into.
//
// The host itself logs to stderr like any console program — it has no idea a
// supervisor exists, and that is the point of the split: the same binary has
// to work under a terminal, launchd and systemd too. Somebody has to turn that
// stream into a file, and the process that already owns the child's pipes is
// the natural owner.
//
// Rotation is by size, because an unattended tray may run for months and the
// host logs every reconnect attempt.
type rotatingFile struct {
	mu   sync.Mutex
	path string
	max  int64
	keep int
	f    *os.File
	size int64
}

const (
	defaultLogMax  = 8 << 20 // 8 MiB
	defaultLogKeep = 3
)

func openRotating(path string, max int64, keep int) (*rotatingFile, error) {
	if max <= 0 {
		max = defaultLogMax
	}
	if keep <= 0 {
		keep = defaultLogKeep
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	w := &rotatingFile{path: path, max: max, keep: keep}
	if err := w.open(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *rotatingFile) open() error {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("打开日志 %s: %w", w.path, err)
	}
	size := int64(0)
	if st, err := f.Stat(); err == nil {
		size = st.Size()
	}
	w.f, w.size = f, size
	return nil
}

// Path is the active log file.
func (w *rotatingFile) Path() string { return w.path }

func (w *rotatingFile) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return len(p), nil // closed: drop rather than fail the logger
	}
	if w.size+int64(len(p)) > w.max {
		w.rotateLocked()
	}
	n, err := w.f.Write(p)
	w.size += int64(n)
	return n, err
}

// rotateLocked shifts host.log → host.log.1 → … and reopens a fresh file.
// Errors are swallowed on purpose: failing to rotate is not a reason to lose
// the line that triggered it.
func (w *rotatingFile) rotateLocked() {
	_ = w.f.Close()
	w.f = nil

	_ = os.Remove(fmt.Sprintf("%s.%d", w.path, w.keep))
	for i := w.keep - 1; i >= 1; i-- {
		_ = os.Rename(fmt.Sprintf("%s.%d", w.path, i), fmt.Sprintf("%s.%d", w.path, i+1))
	}
	_ = os.Rename(w.path, w.path+".1")

	if err := w.open(); err != nil {
		w.f, w.size = nil, 0
	}
}

func (w *rotatingFile) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}
