package server

import (
	"context"
	"net/http"
	"strings"

	"github.com/AgentsHarness/capri-host/internal/config"
)

type hostConfigResponse struct {
	OK      bool        `json:"ok"`
	Error   string      `json:"error,omitempty"`
	Config  config.File `json:"config"`
	Running struct {
		Port     int    `json:"port"`
		Bind     string `json:"bind"`
		HostID   string `json:"hostId"`
		HostName string `json:"hostName"`
	} `json:"running"`
}

type hostConfigUpdate struct {
	Bind              *string `json:"bind"`
	Port              *int    `json:"port"`
	HostID            *string `json:"host_id"`
	HostName          *string `json:"host_name"`
	HubURL            *string `json:"hub_url"`
	FEToken           *string `json:"fe_token"`
	GrokBin           *string `json:"grok_bin"`
	Proxy             *string `json:"proxy"`
	NoProxy           *string `json:"no_proxy"`
	StartHostOnLaunch *bool   `json:"start_host_on_launch"`
	StartAtLogin      *bool   `json:"start_at_login"`
	KeepAwake         *bool   `json:"keep_awake"`
}

func (s *Server) handleGetHostConfig(w http.ResponseWriter, r *http.Request) {
	cfgFile, err := config.LoadFile()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"ok": false, "error": "读取配置文件失败: " + err.Error(),
		})
		return
	}

	var resp hostConfigResponse
	resp.OK = true
	resp.Config = cfgFile
	resp.Running.Port = s.cfg.Port
	resp.Running.Bind = s.cfg.BindAddr
	resp.Running.HostID = s.cfg.HostID
	resp.Running.HostName = s.cfg.HostName

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handlePostHostConfig(w http.ResponseWriter, r *http.Request) {
	var body hostConfigUpdate
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "无效的请求格式"})
		return
	}

	curFile, _ := config.LoadFile()
	targetBind := curFile.Bind
	if body.Bind != nil {
		targetBind = *body.Bind
	}
	targetToken := curFile.FEToken
	if body.FEToken != nil {
		targetToken = *body.FEToken
	}
	targetBind = strings.TrimSpace(targetBind)
	targetToken = strings.TrimSpace(targetToken)
	if targetBind != "" && targetBind != "127.0.0.1" && targetBind != "localhost" && targetBind != "::1" {
		if targetToken == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"ok":    false,
				"error": "局域网访问（" + targetBind + "）必须设置本机钥匙（FE_TOKEN），或改回仅本机访问",
			})
			return
		}
	}

	if body.Port != nil && *body.Port <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "端口必须是正整数"})
		return
	}

	var renameName string
	err := config.UpdateFile(func(f *config.File) {
		if body.Bind != nil {
			f.Bind = strings.TrimSpace(*body.Bind)
		}
		if body.Port != nil {
			f.Port = *body.Port
		}
		if body.HostID != nil && strings.TrimSpace(*body.HostID) != "" {
			f.HostID = strings.TrimSpace(*body.HostID)
		}
		if body.HostName != nil && strings.TrimSpace(*body.HostName) != "" {
			newName := strings.TrimSpace(*body.HostName)
			if newName != f.HostName {
				renameName = newName
			}
			f.HostName = newName
		}
		if body.HubURL != nil {
			f.HubURL = strings.TrimSpace(*body.HubURL)
		}
		if body.FEToken != nil {
			f.FEToken = strings.TrimSpace(*body.FEToken)
		}
		if body.GrokBin != nil {
			f.GrokBin = strings.TrimSpace(*body.GrokBin)
		}
		if body.Proxy != nil {
			f.Proxy = strings.TrimSpace(*body.Proxy)
		}
		if body.NoProxy != nil {
			f.NoProxy = strings.TrimSpace(*body.NoProxy)
		}
		if body.StartHostOnLaunch != nil {
			f.StartHostOnLaunch = body.StartHostOnLaunch
		}
		if body.StartAtLogin != nil {
			f.StartAtLogin = body.StartAtLogin
		}
		if body.KeepAwake != nil {
			f.KeepAwake = body.KeepAwake
		}
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"ok": false, "error": "保存配置文件失败: " + err.Error(),
		})
		return
	}

	if renameName != "" {
		if ctl := s.hubController(); ctl != nil {
			ctx, cancel := context.WithTimeout(context.Background(), hostRenameTimeout)
			_ = ctl.Rename(ctx, renameName)
			cancel()
		}
	}

	saved, _ := config.LoadFile()
	var resp hostConfigResponse
	resp.OK = true
	resp.Config = saved
	resp.Running.Port = s.cfg.Port
	resp.Running.Bind = s.cfg.BindAddr
	resp.Running.HostID = s.cfg.HostID
	resp.Running.HostName = s.cfg.HostName

	writeJSON(w, http.StatusOK, resp)
}
