package config

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

const (
	// DirName 是用户主目录下的配置目录名。
	DirName = ".capri-host"
	// FileName 是 GUI / 手写配置的文件名。
	FileName = "config.json"
	// HomeEnv 覆盖配置目录（测试与便携安装）。未设则用 ~/.capri-host。
	HomeEnv = "CAPRI_HOME"
)

// File 是 ~/.capri-host/config.json 的磁盘形状。环境变量仍然覆盖这里的字段。
// 带 json 标签的 app 专用项 host 的 Load() 会读到但忽略。
type File struct {
	Bind        string `json:"bind,omitempty"`
	Port        int    `json:"port,omitempty"`
	HostID      string `json:"host_id,omitempty"`
	HostName    string `json:"host_name,omitempty"`
	HubURL      string `json:"hub_url,omitempty"`
	HubPairCode string `json:"hub_pair_code,omitempty"`
	FEToken     string `json:"fe_token,omitempty"`
	GrokBin     string `json:"grok_bin,omitempty"`
	HubQUICPin  string `json:"hub_quic_pin,omitempty"`

	// Proxy / NoProxy 是出网代理：host 启动时导出成 HTTPS_PROXY 等标准
	// 变量，hub 中继与本机拉起的 grok agent 都跟着走。留空时 macOS 上
	// 改读系统网络设置里的 HTTP/HTTPS/SOCKS 代理；系统也没开才是直连。
	Proxy   string `json:"proxy,omitempty"`
	NoProxy string `json:"no_proxy,omitempty"`

	// StartHostOnLaunch 由菜单栏应用读取：打开应用时是否自动拉起 host。
	// nil = 缺省（视为 true）。
	StartHostOnLaunch *bool `json:"start_host_on_launch,omitempty"`
	// StartAtLogin 由菜单栏应用读取并同步到系统登录项。
	StartAtLogin *bool `json:"start_at_login,omitempty"`
}

// ShouldStartHostOnLaunch 缺省为 true（第一次装上就想用）。
func (f File) ShouldStartHostOnLaunch() bool {
	if f.StartHostOnLaunch == nil {
		return true
	}
	return *f.StartHostOnLaunch
}

// ShouldStartAtLogin 缺省为 false，要用户显式勾选。
func (f File) ShouldStartAtLogin() bool {
	if f.StartAtLogin == nil {
		return false
	}
	return *f.StartAtLogin
}

// ListenPort 是文件里的端口，0 / 缺省视为 8765。
func (f File) ListenPort() int {
	if f.Port > 0 {
		return f.Port
	}
	return 8765
}

// Dir 是配置目录：CAPRI_HOME 或 ~/.capri-host。
func Dir() string {
	if v := strings.TrimSpace(os.Getenv(HomeEnv)); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return DirName
	}
	return filepath.Join(home, DirName)
}

// Path 是 config.json 的绝对路径。
func Path() string {
	return filepath.Join(Dir(), FileName)
}

// LoadFile 读 config.json。文件不存在返回零值 File、nil error。
func LoadFile() (File, error) {
	b, err := os.ReadFile(Path())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return File{}, nil
		}
		return File{}, err
	}
	var f File
	if err := json.Unmarshal(b, &f); err != nil {
		return File{}, err
	}
	return f, nil
}

// SaveFile 原子写入 config.json（目录 0755，文件 0600：里面可能有 FE_TOKEN）。
func SaveFile(f File) error {
	dir := Dir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	tmp, err := os.CreateTemp(dir, FileName+".tmp.*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, Path()); err != nil {
		return err
	}
	ok = true
	return nil
}
