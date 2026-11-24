package server

import (
	"github.com/AgentsHarness/capri-host/internal/acp"
	"net/http"
)

// deployment 是 FE 看到的部署形态：有 hub 地址 → hub 模式（内嵌前端跨源
// 直连 hub 看全部 host）；否则 local（前端锁定本机）。/api/status、
// /api/probe 与免鉴权的 /api/hosts 共用它。
//
// 读 live hubSnapshot，不读启动时的 s.cfg.HubURL：托盘配对改的是
// manager，不重启进程。托盘自己走 GET /api/hub/state，那条已经是对的；
// 这里对的是 FE 探测（局域网打开内嵌页靠 /api/hosts 升 hub）。
func (s *Server) deployment() (mode, hubURL string) {
	if u := s.hubSnapshot().HubURL; u != "" {
		return "hub", u
	}
	return "local", ""
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	snap := s.bridge.Snapshot()
	snap.Mode, snap.HubURL = s.deployment()
	writeJSON(w, http.StatusOK, snap)
}

// handleProbe 是前端「本机近路先探再问」用的最小需鉴权端点：它落在
// withAuth 的常规门禁里（authRequiredForPath 只豁免 /api/hosts），于是
// 200 / 401 本身就构成「这把钥匙对不对」的答案，hostId 再自证应答者身份。
// 不用 /api/status 探路：那份快照序列化整个 bridge（远程实测 1~2s，还带
// cwd / sessionId / roster），塞进启动关键路径既慢又漏信息。
func (s *Server) handleProbe(w http.ResponseWriter, r *http.Request) {
	mode, hubURL := s.deployment()
	out := map[string]any{"ok": true, "hostId": s.bridge.Snapshot().HostID, "mode": mode, "version": acp.EffectiveVersion()}
	if hubURL != "" {
		out["hubUrl"] = hubURL
	}
	writeJSON(w, http.StatusOK, out)
}

// handleHosts 是启动期唯一免鉴权的身份端点（authRequiredForPath 豁免，
// host 在这里永不 401）。除注册表外还回 mode / hubUrl / hostId / port：
// 页面打到 host 时，前端只读这一份应答就能认出「送我这页的就是这台」并
// 升到 hub——不必再问需要钥匙的 /api/status。少了这几个字段，局域网 IP
// 打开的内嵌页（浏览器手里还没有 host 那把）会永远升不了 hub。
func (s *Server) handleHosts(w http.ResponseWriter, r *http.Request) {
	// Local mode: single host (this process). Multi-host comes via Hub.
	snap := s.bridge.Snapshot()
	mode, hubURL := s.deployment()
	out := map[string]any{
		"hosts": []map[string]any{
			{
				"hostId":   snap.HostID,
				"hostName": snap.HostName,
				"online":   true,
				"ready":    snap.Ready,
				"local":    true,
			},
		},
		// authRequired: this host requires the access token on its API (the
		// FE reads it in probeAccess to decide whether to show the gate).
		"authRequired": s.cfg.AccessToken != "",
		"mode":         mode,
		"hostId":       snap.HostID,
		"port":         s.cfg.Port,
	}
	if hubURL != "" {
		out["hubUrl"] = hubURL
	}
	writeJSON(w, http.StatusOK, out)
}
