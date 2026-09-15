package config

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

const defaultGrokName = "grok"

// resolveGrokBin 把用户指定值（或空）收成最终可执行路径。
// 空 / "grok" / "grok.exe" 会走常见安装位置；其它字符串视为用户指定，原样保留。
func resolveGrokBin(explicit string) string {
	explicit = strings.TrimSpace(explicit)
	if explicit != "" && !isDefaultGrokName(explicit) {
		return explicit
	}
	if found := DiscoverGrokBin(); found != "" {
		return found
	}
	if explicit != "" {
		return explicit
	}
	return defaultGrokName
}

func isDefaultGrokName(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	return s == defaultGrokName || s == defaultGrokName+".exe"
}

// DiscoverGrokBin 按常见路径查找 grok，找不到返回空串。
func DiscoverGrokBin() string {
	home, _ := os.UserHomeDir()
	return discoverGrokBin(home, os.Stat, exec.LookPath)
}

func discoverGrokBin(home string, stat func(string) (os.FileInfo, error), lookPath func(string) (string, error)) string {
	for _, p := range grokSearchPaths(home) {
		st, err := stat(p)
		if err == nil && st != nil && !st.IsDir() {
			return p
		}
	}
	if p, err := lookPath(defaultGrokName); err == nil && p != "" {
		return p
	}
	if runtime.GOOS == "windows" {
		if p, err := lookPath(defaultGrokName + ".exe"); err == nil && p != "" {
			return p
		}
	}
	return ""
}

func grokSearchPaths(home string) []string {
	name := defaultGrokName
	if runtime.GOOS == "windows" {
		name = defaultGrokName + ".exe"
	}
	var out []string
	if home != "" {
		out = append(out,
			filepath.Join(home, ".local", "bin", name),
			filepath.Join(home, ".grok", "bin", name),
		)
	}
	switch runtime.GOOS {
	case "darwin", "linux":
		out = append(out,
			filepath.Join("/opt/homebrew/bin", name),
			filepath.Join("/usr/local/bin", name),
		)
	}
	return out
}
