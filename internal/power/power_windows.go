//go:build windows

package power

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"unsafe"

	"golang.org/x/sys/windows"
)

// The power-request API moved between DLLs: PowerCreateRequest and friends are
// documented against Kernel32.dll on Windows 8 and later, and only live in
// PowrProf.dll on Windows 7. Looking only in powrprof made Supported() report
// false on Windows 10/11 and silently disabled the whole feature, so both are
// tried and the first DLL exporting all three wins.
var (
	kernel32 = windows.NewLazySystemDLL("kernel32.dll")
	powrprof = windows.NewLazySystemDLL("powrprof.dll")
)

type powerProcs struct {
	create *windows.LazyProc
	set    *windows.LazyProc
	clear  *windows.LazyProc
	err    error
}

// procs resolves once, on first use rather than at init, so merely importing
// the package does not load a DLL.
var procs = sync.OnceValue(func() powerProcs {
	for _, dll := range []*windows.LazyDLL{kernel32, powrprof} {
		p := powerProcs{
			create: dll.NewProc("PowerCreateRequest"),
			set:    dll.NewProc("PowerSetRequest"),
			clear:  dll.NewProc("PowerClearRequest"),
		}
		if p.create.Find() == nil && p.set.Find() == nil && p.clear.Find() == nil {
			return p
		}
	}
	return powerProcs{err: errors.New("系统未提供 PowerCreateRequest（需要 Windows 7 以上）")}
})

// POWER_REQUEST_TYPE values.
const (
	powerRequestDisplayRequired = 0
	powerRequestSystemRequired  = 1
)

const (
	powerRequestContextVersion      = 0
	powerRequestContextSimpleString = 0x1
)

// reasonContext mirrors Win32 REASON_CONTEXT with the SIMPLE_STRING flag set.
// The C type's second member is a union whose largest arm is a struct of
// pointers, but with POWER_REQUEST_CONTEXT_SIMPLE_STRING the API reads only the
// leading LPWSTR, so the short form is what it expects to be handed.
type reasonContext struct {
	Version            uint32
	Flags              uint32
	SimpleReasonString *uint16
}

// Inhibitor keeps the machine awake while enabled.
//
// It uses PowerCreateRequest/PowerSetRequest rather than
// SetThreadExecutionState. That choice matters in Go: SetThreadExecutionState
// applies to the CALLING THREAD, and a goroutine is not pinned to one, so the
// state silently belongs to whichever thread happened to run the call and lapses
// when the runtime reuses it. A power request is held by the PROCESS, and it is
// also visible in `powercfg /requests`, which makes the feature auditable
// instead of something the user has to take on faith.
//
// Only SystemRequired is requested, never DisplayRequired: the ask is "do not
// suspend", and pinning the display on as well would stop the screen from
// blanking, which is a different and unwanted promise.
type Inhibitor struct {
	reason string

	mu      sync.Mutex
	handle  windows.Handle
	enabled bool
}

// New returns an inhibitor that is not yet active. reason is shown by
// `powercfg /requests`.
func New(reason string) *Inhibitor {
	if reason == "" {
		reason = "capri-host"
	}
	return &Inhibitor{reason: reason}
}

// Supported reports whether the OS exposes the power-request API.
func Supported() bool {
	return procs().err == nil
}

// Enable starts holding the machine awake. Calling it while already enabled is
// a no-op.
func (i *Inhibitor) Enable() error {
	p := procs()
	if p.err != nil {
		return p.err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.enabled {
		return nil
	}
	if i.handle == 0 {
		h, err := i.createRequest(p)
		if err != nil {
			return err
		}
		i.handle = h
	}
	r, _, err := p.set.Call(uintptr(i.handle), powerRequestSystemRequired)
	if r == 0 {
		return fmt.Errorf("PowerSetRequest 失败: %w", err)
	}
	i.enabled = true
	return nil
}

// Disable releases the request, letting normal idle timers apply again.
func (i *Inhibitor) Disable() error {
	p := procs()
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.enabled || i.handle == 0 || p.err != nil {
		i.enabled = false
		return nil
	}
	r, _, err := p.clear.Call(uintptr(i.handle), powerRequestSystemRequired)
	i.enabled = false
	if r == 0 {
		return fmt.Errorf("PowerClearRequest 失败: %w", err)
	}
	return nil
}

// Enabled reports whether the machine is currently being held awake.
func (i *Inhibitor) Enabled() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.enabled
}

// Toggle flips the inhibitor and reports the state it ended in.
func (i *Inhibitor) Toggle() (bool, error) {
	if i.Enabled() {
		return false, i.Disable()
	}
	return true, i.Enable()
}

// Close releases the request and its handle.
func (i *Inhibitor) Close() error {
	err := i.Disable()
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.handle != 0 {
		_ = windows.CloseHandle(i.handle)
		i.handle = 0
	}
	return err
}

func (i *Inhibitor) createRequest(p powerProcs) (windows.Handle, error) {
	wreason, err := windows.UTF16PtrFromString(i.reason)
	if err != nil {
		return 0, err
	}
	ctx := reasonContext{
		Version:            powerRequestContextVersion,
		Flags:              powerRequestContextSimpleString,
		SimpleReasonString: wreason,
	}
	h, _, callErr := p.create.Call(uintptr(unsafe.Pointer(&ctx)))
	// The struct and the wide string it points at are only reachable through
	// a uintptr across the call boundary, which escape analysis cannot see
	// through — pin both until the API has copied the reason.
	runtime.KeepAlive(&ctx)
	runtime.KeepAlive(wreason)
	if h == 0 || windows.Handle(h) == windows.InvalidHandle {
		return 0, fmt.Errorf("PowerCreateRequest 失败: %w", callErr)
	}
	return windows.Handle(h), nil
}
