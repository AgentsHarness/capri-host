package server

import (
	"embed"
	"io"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// webDist 是 acp-fe 的生产构建产物（index.html + 内容哈希的 assets），
// 从 ../acp-fe/dist 复制而来。嵌入二进制后 host 一个进程一个端口同时
// 提供 API 与 TUI Web 界面 —— 部署无需 nginx / 静态服务器。
//
// 更新方式：cd acp-fe && npm run build && cp -R dist ../acp-host/internal/server/web/dist
//
//go:embed web/dist
var webDist embed.FS

// webAssets 指向嵌入目录内的 dist 根（文件即 index.html / favicon.svg 等）。
var webAssets fs.FS

func init() {
	sub, err := fs.Sub(webDist, "web/dist")
	if err != nil {
		panic(err)
	}
	webAssets = sub
}

// serveEmbedded serves one file from the embedded dist with content-hash
// aware caching; reports false when the file does not exist.
func serveEmbedded(w http.ResponseWriter, r *http.Request, name string) bool {
	f, err := webAssets.Open(name)
	if err != nil {
		return false
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.IsDir() {
		return false
	}
	rs, ok := f.(io.ReadSeeker)
	if !ok {
		return false
	}
	// 哈希资源不可变：文件名变了内容才变，长缓存避免每次刷新重新
	// 下载整套 JS；入口文档（index.html）每次回源拿最新引用。
	if strings.HasPrefix(r.URL.Path, "/assets/") {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "no-cache")
	}
	http.ServeContent(w, r, st.Name(), st.ModTime(), rs)
	return true
}

// isAPIOrLivePath reports whether p is inside the host's API / SSE
// namespaces — those never fall back to the SPA shell.
func isAPIOrLivePath(p string) bool {
	return strings.HasPrefix(p, "/api/") ||
		p == "/api" ||
		p == "/events" ||
		strings.HasPrefix(p, "/events/")
}

// handleWeb 服务嵌入的 acp-fe SPA：
//   - 存在的文件按原样返回（assets 带长缓存）；
//   - 其余非 API 的 GET 一律回退到 index.html（前端路由）；
//   - 未注册的 /api/*、/events/* 保持 JSON 404，不会落到 SPA 壳。
func (s *Server) handleWeb(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
	if name == "" {
		name = "index.html"
	}
	if serveEmbedded(w, r, name) {
		return
	}
	if isAPIOrLivePath(r.URL.Path) {
		writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "error": "not found"})
		return
	}
	if serveEmbedded(w, r, "index.html") {
		return
	}
	http.NotFound(w, r)
}
