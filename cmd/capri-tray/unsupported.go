//go:build !windows

// The tray is a Windows program: it exists to give a double-clicked Windows
// binary a UI, and it drives the Windows registry (autostart), the power
// request API and the shell's URL handler. macOS has Capri.app and the other
// platforms run the host under their own service manager.
//
// This stub exists so `go build ./...` and `go vet ./...` stay meaningful on a
// developer's machine instead of failing with "build constraints exclude all
// Go files" — which would hide real breakage in every other package.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "Capri 只在 Windows 上构建；其他平台请直接用 Capri-host，macOS 用 Capri.app。")
	os.Exit(1)
}
