// Package applog sends the standard logger to a rotating file.
//
// This is not a nicety. A Windows GUI binary is linked with -H=windowsgui so
// double-clicking it does not flash a console, and that subsystem gives the
// process no standard error at all: every log.Printf in the codebase would
// otherwise be written to an invalid handle and lost, taking the hub client's
// pairing and reconnect diagnostics with it.
package applog

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// Options configures the log file. The zero value is usable via Setup's
// defaults.
type Options struct {
	// Dir is the directory to write into; created if missing.
	Dir string
	// Name is the file name, e.g. "host.log".
	Name string
	// MaxBytes rotates the file once it exceeds this size. Default 8 MiB.
	MaxBytes int64
	// Keep is how many rotated generations to retain (host.log.1 …).
	// Default 3.
	Keep int
	// AlsoStderr additionally writes to stderr. Harmless under
	// -H=windowsgui: writes there fail silently rather than breaking the
	// file sink (see bestEffort).
	AlsoStderr bool
}

// File is a size-rotating log sink.
type File struct {
	path     string
	maxBytes int64
	keep     int

	mu   sync.Mutex
	f    *os.File
	size int64
}

// Setup opens the log file and points the standard logger at it. It returns the
// sink so the caller can close it on shutdown.
func Setup(opt Options) (*File, error) {
	if opt.Name == "" {
		opt.Name = "host.log"
	}
	if opt.MaxBytes <= 0 {
		opt.MaxBytes = 8 << 20
	}
	if opt.Keep <= 0 {
		opt.Keep = 3
	}
	if opt.Dir == "" {
		return nil, fmt.Errorf("applog: 缺少目录")
	}
	if err := os.MkdirAll(opt.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("applog: 创建目录 %s: %w", opt.Dir, err)
	}

	lf := &File{
		path:     filepath.Join(opt.Dir, opt.Name),
		maxBytes: opt.MaxBytes,
		keep:     opt.Keep,
	}
	if err := lf.open(); err != nil {
		return nil, err
	}

	var w io.Writer = lf
	if opt.AlsoStderr {
		// File first: io.MultiWriter aborts at the first error, so a dead
		// stderr must never be able to starve the file sink.
		w = io.MultiWriter(lf, bestEffort{os.Stderr})
	}
	log.SetOutput(w)
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	return lf, nil
}

// Path is the active log file's path.
func (lf *File) Path() string { return lf.path }

func (lf *File) open() error {
	f, err := os.OpenFile(lf.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("applog: 打开 %s: %w", lf.path, err)
	}
	size := int64(0)
	if st, err := f.Stat(); err == nil {
		size = st.Size()
	}
	lf.f, lf.size = f, size
	return nil
}

func (lf *File) Write(p []byte) (int, error) {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	if lf.f == nil {
		return len(p), nil // closed: drop rather than fail the logger
	}
	if lf.size+int64(len(p)) > lf.maxBytes {
		lf.rotateLocked()
	}
	n, err := lf.f.Write(p)
	lf.size += int64(n)
	return n, err
}

// rotateLocked shifts host.log → host.log.1 → … and reopens a fresh file.
// Errors are swallowed on purpose: failing to rotate is not a reason to lose
// the log line that triggered it.
func (lf *File) rotateLocked() {
	_ = lf.f.Close()
	lf.f = nil

	oldest := fmt.Sprintf("%s.%d", lf.path, lf.keep)
	_ = os.Remove(oldest)
	for i := lf.keep - 1; i >= 1; i-- {
		_ = os.Rename(fmt.Sprintf("%s.%d", lf.path, i), fmt.Sprintf("%s.%d", lf.path, i+1))
	}
	_ = os.Rename(lf.path, lf.path+".1")

	if err := lf.open(); err != nil {
		// Nothing sensible left to do — Write returns success so callers
		// are not turned into error-handling paths over a log line.
		lf.f, lf.size = nil, 0
	}
}

// Close flushes and closes the file.
func (lf *File) Close() error {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	if lf.f == nil {
		return nil
	}
	err := lf.f.Close()
	lf.f = nil
	return err
}

// bestEffort discards write errors from its target. Under -H=windowsgui
// os.Stderr wraps an invalid handle, and every write to it fails; that must not
// propagate out of a MultiWriter and suppress the sinks after it.
type bestEffort struct{ w io.Writer }

func (b bestEffort) Write(p []byte) (int, error) {
	_, _ = b.w.Write(p)
	return len(p), nil
}
