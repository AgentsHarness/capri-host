//go:build windows

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/AgentsHarness/capri-host/internal/config"
	"github.com/AgentsHarness/capri-host/internal/procattr"
)

// openURL opens a URL in the default browser.
//
// rundll32 rather than `cmd /c start`, because start treats its first quoted
// argument as a window title and mangles any URL containing an ampersand —
// which hub addresses with query strings do.
func openURL(u string) {
	if u == "" {
		return
	}
	cmd := exec.Command("rundll32", "url.dll,FileProtocolHandler", u)
	procattr.HideConsole(cmd)
	if err := cmd.Start(); err != nil {
		logf("打开 %s 失败: %v", u, err)
	}
}

// openPath opens a file or folder with its registered handler.
func openPath(p string) {
	if p == "" {
		return
	}
	cmd := exec.Command("rundll32", "url.dll,FileProtocolHandler", p)
	procattr.HideConsole(cmd)
	if err := cmd.Start(); err != nil {
		logf("打开 %s 失败: %v", p, err)
	}
}

// openSettings opens the shared settings file in a text editor.
//
// An editor, not the shell's default handler: .json has no consistent
// association on Windows, and on plenty of machines ShellExecute hands it to a
// browser — the opposite of what "编辑设置" should do. Notepad is always there;
// CAPRI_EDITOR replaces it for anyone who wants their own.
//
// Note the absence of procattr.HideConsole: the editor's window is the whole
// point. The tray spawning a GUI process from a GUI process raises no console.
func openSettings() error {
	path := config.Path()
	// Create a fully spelled-out file first when there is none. Notepad would
	// otherwise answer with a "cannot find the file, create it?" prompt, and an
	// empty scaffold would leave the user with nothing to fill in and no hint
	// that anything else is settable.
	if created, err := config.WriteStarter(); err != nil {
		return err
	} else if created {
		logf("已创建默认配置 %s", path)
	}

	editor := strings.TrimSpace(os.Getenv("CAPRI_EDITOR"))
	if editor == "" {
		editor = "notepad.exe"
	}
	cmd := exec.Command(editor, path)
	if err := cmd.Start(); err != nil {
		return err
	}
	logf("已用 %s 打开 %s", filepath.Base(editor), path)
	return nil
}
