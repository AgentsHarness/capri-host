package server

// http_local_fs.go — 目录浏览与新建（纯 host 本地能力：不经 agent，也不走
// /api/shell）。空状态的「选择工作目录」弹窗用它列宿主机目录、建文件夹。
//
// 为什么另起一套而不是复用 /api/shell：shell 端点硬编码 `sh -c`，Windows
// 上没有 sh/find 就整条路不通；另外 MSYS/Git-Bash 风格的路径（/d/aiwork）
// 也不是 Windows 上 os.ReadDir 能直接吃的形式。这里用 os.ReadDir 原生列目录，
// 并按平台把用户输入归一化成宿主原生路径（见 normalizeLocalPath），因此
// macOS / Linux / Windows 行为一致。

import (
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
)

// registerLocalFSRoutes 注册本域路由（路由与实现同址）。
func (s *Server) registerLocalFSRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/local/dirs", s.handleLocalDirs)
	mux.HandleFunc("POST /api/local/mkdir", s.handleLocalMkdir)
}

type localDirsBody struct {
	// Path 可空：空 → 用户主目录（宿主没有可用主目录时 400）。
	Path string `json:"path"`
}

// handleLocalDirs — POST /api/local/dirs {path?}: 列出一个宿主本地路径的
// 直接子目录。纯 host 能力，不依赖 agent 也不依赖活动会话（空状态还没有
// 会话）。path 缺省为主目录；`~`、`~/x` 展开为主目录；Windows 上接受
// MSYS/Git-Bash 盘符写法（/d/aiwork → D:\aiwork）与正/反斜杠混用。
//
// 返回 dirs 里每项都带宿主原生的绝对路径，前端不做路径拼接。路径不存在 /
// 不是目录 / 没权限都是 400 + 人话 error（这是用户的正常输入状态，不是坏
// 请求），错误响应里同样带 path（归一化后的值），前端据此显示"你输入的
// /d/aiwork 实际是 D:\aiwork"。
func (s *Server) handleLocalDirs(w http.ResponseWriter, r *http.Request) {
	var body localDirsBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	isWindows := runtime.GOOS == "windows"
	home, _ := os.UserHomeDir()
	dir := normalizeLocalPath(body.Path, home, isWindows)
	if dir == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"ok": false, "error": "无法确定目录：宿主没有可用的主目录，请直接输入绝对路径",
		})
		return
	}
	fail := func(msg string) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": msg, "path": dir, "home": home})
	}
	info, err := os.Stat(dir)
	if err != nil {
		fail(localFSError(err, body.Path, dir))
		return
	}
	if !info.IsDir() {
		fail("不是目录：" + dir)
		return
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		fail(localFSError(err, body.Path, dir))
		return
	}
	dirs := make([]map[string]string, 0, len(entries))
	for _, e := range entries {
		if !isLocalDirEntry(e, dir) {
			continue
		}
		dirs = append(dirs, map[string]string{"name": e.Name(), "path": localJoin(dir, e.Name(), isWindows)})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":   true,
		"path": dir,
		"home": home,
		"dirs": dirs,
	})
}

type localMkdirBody struct {
	Parent string `json:"parent"`
	Name   string `json:"name"`
}

// handleLocalMkdir — POST /api/local/mkdir {parent, name}: 在 parent 下新建
// 一层目录。只建一层（name 必须是一个目录名，不能含路径分隔符），已存在
// 报 409。返回宿主原生的新目录绝对路径。
func (s *Server) handleLocalMkdir(w http.ResponseWriter, r *http.Request) {
	var body localMkdirBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	isWindows := runtime.GOOS == "windows"
	name := strings.TrimSpace(body.Name)
	if msg := invalidLocalDirName(name, isWindows); msg != "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": msg})
		return
	}
	home, _ := os.UserHomeDir()
	parent := normalizeLocalPath(body.Parent, home, isWindows)
	if parent == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"ok": false, "error": "无法确定目录：宿主没有可用的主目录，请直接输入绝对路径",
		})
		return
	}
	info, err := os.Stat(parent)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": localFSError(err, body.Parent, parent)})
		return
	}
	if !info.IsDir() {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "不是目录：" + parent})
		return
	}
	target := localJoin(parent, name, isWindows)
	if err := os.Mkdir(target, 0o755); err != nil {
		if errors.Is(err, fs.ErrExist) {
			writeJSON(w, http.StatusConflict, map[string]any{"ok": false, "error": "同名目录或文件已存在：" + target})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"ok": false, "error": "新建文件夹失败：" + target + "（" + err.Error() + "）",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "path": target})
}

// normalizeLocalPath 把用户输入的路径归一化成宿主原生形式的路径。
//
//	"" / "~" / "~/x" / "~\x" → 主目录（展开后交给平台清理）
//	Windows: "/d/aiwork"（MSYS/Git-Bash 盘符写法）→ "D:\aiwork"；
//	         根相对路径（"/aiwork"）补主目录所在盘符；"d:" → "D:\"；
//	         反斜杠与正斜杠都接受，按段折叠 "." / ".."；UNC（//server/share）保留。
//	其他平台: filepath.Clean；相对路径按主目录解析（前端会先把相对草稿拼到
//	         当前目录上，这里只是兜底）。
//
// 输入被归一化过时，调用方展示的是归一化后的路径——Windows 上输入
// /d/aiwork 后看到 D:\aiwork，选中写回的就是后者。
func normalizeLocalPath(raw, home string, isWindows bool) string {
	p := strings.TrimSpace(raw)
	if rest, ok := localHomeRelative(p); ok {
		if home == "" {
			return ""
		}
		p = strings.TrimRight(strings.ReplaceAll(home, `\`, "/"), "/") + "/" + rest
	} else if p == "" {
		if home == "" {
			return ""
		}
		p = home
	}
	if isWindows {
		if !isWindowsRooted(p) {
			p = strings.TrimRight(strings.ReplaceAll(home, `\`, "/"), "/") + "/" + p
		}
		return cleanWindowsLocalPath(p, home)
	}
	if !filepath.IsAbs(p) {
		if home == "" {
			return ""
		}
		p = filepath.Join(home, p)
	}
	return filepath.Clean(p)
}

// localHomeRelative 识别 "~" / "~/x" / "~\x"，返回 "~" 之后的部分。
func localHomeRelative(p string) (string, bool) {
	if p == "~" {
		return "", true
	}
	if strings.HasPrefix(p, "~/") || strings.HasPrefix(p, `~\`) {
		return p[2:], true
	}
	return "", false
}

// cleanWindowsLocalPath 归一化 Windows 路径（纯字符串处理，可跨平台测试）。
func cleanWindowsLocalPath(p, home string) string {
	p = strings.ReplaceAll(p, `\`, "/")
	// MSYS/Git-Bash 盘符写法：/d/aiwork → d:/aiwork（首段恰好一个字母）。
	// /Users/x 这种首段更长的路径不在此列，走下面的"补盘符"分支。
	if len(p) >= 2 && p[0] == '/' && isASCIILetter(p[1]) && (len(p) == 2 || p[2] == '/') {
		p = p[1:2] + ":" + p[2:]
	}
	if strings.HasPrefix(p, "/") && !strings.HasPrefix(p, "//") {
		p = homeDrive(home) + p
	}
	if strings.HasPrefix(p, "//") {
		p = "//" + path.Clean(strings.TrimPrefix(p, "//"))
	} else {
		p = path.Clean(p)
	}
	// 盘根：Clean 会把 "d:/" 收成 "d:"，补回分隔符，否则 "d:" 在 Windows
	// 上是"盘符内当前目录"而不是盘根。
	if len(p) == 2 && p[1] == ':' {
		p += "/"
	}
	if len(p) >= 2 && p[1] == ':' {
		p = strings.ToUpper(p[:1]) + p[1:]
	}
	return strings.ReplaceAll(p, "/", `\`)
}

// homeDrive 取主目录所在盘符（"D:"）；主目录不可用时退到 "C:"。
func homeDrive(home string) string {
	if len(home) >= 2 && isASCIILetter(home[0]) && home[1] == ':' {
		return strings.ToUpper(home[:2])
	}
	return "C:"
}

func isASCIILetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// isWindowsRooted 判断路径是否自带起点：盘符（含 MSYS 的 /d）、根相对（/ 或
// \）或 UNC。相对路径按主目录解析，与 POSIX 分支一致。
func isWindowsRooted(p string) bool {
	if strings.HasPrefix(p, "/") || strings.HasPrefix(p, `\`) {
		return true
	}
	return len(p) >= 2 && isASCIILetter(p[0]) && p[1] == ':'
}

// localJoin 按宿主平台拼接一层目录名。base 已是原生形式（Windows 上可能是
// 盘根 "D:\" 或 UNC），所以不走 filepath.Join 的语义转换。
func localJoin(base, name string, isWindows bool) string {
	if isWindows {
		return strings.TrimRight(base, `\/`) + `\` + name
	}
	return filepath.Join(base, name)
}

// isLocalDirEntry 判断条目是不是目录：真目录，或指向目录的符号链接（macOS
// 上工作区常在 /Volumes 的链接后面；find -type d 不跟链接，这里跟）。
func isLocalDirEntry(e os.DirEntry, dir string) bool {
	if e.IsDir() {
		return true
	}
	if e.Type()&fs.ModeSymlink == 0 {
		return false
	}
	st, err := os.Stat(filepath.Join(dir, e.Name()))
	return err == nil && st.IsDir()
}

// localFSError 把 os 错误翻成人话；raw 与 dir 不同（被归一化过）时一并列出，
// 免得用户对着 D:\aiwork 想不明白自己输入的 /d/aiwork 去哪了。
func localFSError(err error, raw, dir string) string {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		if t := strings.TrimSpace(raw); t != "" && t != dir {
			return "目录不存在：" + dir + "（由 " + t + " 归一化）"
		}
		return "目录不存在：" + dir
	case errors.Is(err, fs.ErrPermission):
		return "没有读取权限：" + dir
	}
	return "无法读取目录：" + dir + "（" + err.Error() + "）"
}

// invalidLocalDirName 返回不能当目录名的原因（"" = 可用）。单层语义：
// 路径分隔符、"." / ".." 一律拒绝；Windows 额外拒绝非法字符、以点/空格结尾
// 与保留设备名。
func invalidLocalDirName(name string, isWindows bool) string {
	switch {
	case name == "":
		return "文件夹名称不能为空"
	case name == "." || name == "..":
		return "文件夹名称无效：" + name
	case strings.ContainsAny(name, `/\`):
		return "文件夹名称不能包含路径分隔符"
	case strings.ContainsRune(name, 0):
		return "文件夹名称包含非法字符"
	}
	if !isWindows {
		return ""
	}
	if strings.ContainsAny(name, `<>:"|?*`) {
		return `文件夹名称包含 Windows 不允许的字符（<>:"|?*）`
	}
	if strings.HasSuffix(name, ".") || strings.HasSuffix(name, " ") {
		return "Windows 上文件夹名称不能以点或空格结尾"
	}
	if isWindowsReservedName(name) {
		return "Windows 保留名称，不能用作文件夹名：" + name
	}
	return ""
}

// isWindowsReservedName 判断 CON / PRN / AUX / NUL / COM1-9 / LPT1-9（含带
// 扩展名的形式，如 "con.txt"；这些在 Windows 上也指向设备）。
func isWindowsReservedName(name string) bool {
	stem := name
	if i := strings.IndexByte(stem, '.'); i >= 0 {
		stem = stem[:i]
	}
	switch strings.ToUpper(stem) {
	case "CON", "PRN", "AUX", "NUL":
		return true
	}
	upper := strings.ToUpper(stem)
	if len(upper) == 4 {
		switch upper[:3] {
		case "COM", "LPT":
			return upper[3] >= '1' && upper[3] <= '9'
		}
	}
	return false
}
