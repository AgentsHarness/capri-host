//go:build windows

package main

import (
	"fmt"
	"runtime"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	user32Clip   = windows.NewLazySystemDLL("user32.dll")
	kernel32Clip = windows.NewLazySystemDLL("kernel32.dll")

	procOpenClipboard    = user32Clip.NewProc("OpenClipboard")
	procCloseClipboard   = user32Clip.NewProc("CloseClipboard")
	procEmptyClipboard   = user32Clip.NewProc("EmptyClipboard")
	procSetClipboardData = user32Clip.NewProc("SetClipboardData")

	procGlobalAlloc  = kernel32Clip.NewProc("GlobalAlloc")
	procGlobalLock   = kernel32Clip.NewProc("GlobalLock")
	procGlobalUnlock = kernel32Clip.NewProc("GlobalUnlock")
)

const (
	gmemMoveable  = 0x0002
	cfUnicodeText = 13
)

// writeClipboard copies text to the Windows system clipboard as CF_UNICODETEXT.
func writeClipboard(text string) error {
	utf16, err := windows.UTF16FromString(text)
	if err != nil {
		return err
	}

	r, _, err := procOpenClipboard.Call(0)
	if r == 0 {
		return fmt.Errorf("打开剪贴板失败: %w", err)
	}
	defer procCloseClipboard.Call()

	r, _, err = procEmptyClipboard.Call()
	if r == 0 {
		return fmt.Errorf("清空剪贴板失败: %w", err)
	}

	bytesLen := len(utf16) * 2
	hMem, _, err := procGlobalAlloc.Call(gmemMoveable, uintptr(bytesLen))
	if hMem == 0 {
		return fmt.Errorf("分配内存失败: %w", err)
	}

	pMem, _, err := procGlobalLock.Call(hMem)
	if pMem == 0 {
		return fmt.Errorf("锁定内存失败: %w", err)
	}

	dst := unsafe.Slice((*uint16)(unsafe.Pointer(pMem)), len(utf16))
	copy(dst, utf16)
	runtime.KeepAlive(utf16)

	procGlobalUnlock.Call(hMem)

	r, _, err = procSetClipboardData.Call(cfUnicodeText, hMem)
	if r == 0 {
		return fmt.Errorf("写入剪贴板数据失败: %w", err)
	}
	return nil
}
