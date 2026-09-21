//go:build windows

package main

import (
	"context"
	"embed"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"fyne.io/systray"
	"github.com/ncruces/zenity"

	"github.com/AgentsHarness/capri-host/internal/config"
	"github.com/AgentsHarness/capri-host/internal/hubstate"
	"github.com/AgentsHarness/capri-host/internal/netinfo"
)

// Capri.ico 由 packaging/macos/render-menubar-icon.swift 的 drawAppIcon 绘出
// （16/20/24/32/48/64/128/256 PNG）。托盘运行时读它；make-exes.sh 再把它嵌进 Capri.exe。
//go:embed assets/Capri.ico
var assets embed.FS

const (
	refreshInterval    = 2 * time.Second
	netRefreshInterval = 30 * time.Second
	stopGrace          = 8 * time.Second
)

const (
	startTimeout = 20 * time.Second
)

// trayApp owns the menu and everything the menu can reach.
type trayApp struct {
	version string
	log     *rotatingFile
	host    *hostProc

	mu       sync.Mutex
	settings config.File
	state    hubstate.State
	stateErr string
	hostVer  string

	netMu sync.RWMutex
	net   netinfo.Info

	sleep *powerInhibitor

	// 顶部只读状态
	mStatus *systray.MenuItem

	// Web 界面与连接入口
	mConsole *systray.MenuItem
	mCopyLAN *systray.MenuItem
	mHub     *systray.MenuItem

	// 进程管理与休眠控制
	mToggle  *systray.MenuItem
	mRestart *systray.MenuItem
	mPower   *systray.MenuItem

	// 设置与系统工具
	mSettings *systray.MenuItem
	mBoot     *systray.MenuItem
	mInfo     *systray.MenuItem
	mLog      *systray.MenuItem
	mEdit     *systray.MenuItem

	// 退出
	mQuit *systray.MenuItem

	hubShown bool
	quitOnce sync.Once
}

func newTrayApp(version string, log *rotatingFile, host *hostProc) *trayApp {
	return &trayApp{
		version: version,
		log:     log,
		host:    host,
		sleep:   powerNew("Capri-host 正在保持本机唤醒"),
	}
}

func (t *trayApp) api() *apiClient {
	if t.host.running() {
		if port, token, ok := t.host.endpoint(); ok {
			return newAPIClient(port, token)
		}
	}
	s, _ := loadSettings()
	return newAPIClient(s.ListenPort(), strings.TrimSpace(s.FEToken))
}

func (t *trayApp) settingsSnapshot() config.File {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.settings
}

func (t *trayApp) stateSnapshot() (hubstate.State, string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state, t.stateErr
}

func (t *trayApp) onReady() {
	if icon, err := assets.ReadFile("assets/Capri.ico"); err == nil {
		systray.SetIcon(icon)
	} else {
		logf("读取图标失败: %v", err)
	}
	systray.SetTitle("Capri")
	systray.SetTooltip("Capri")

	t.reloadSettings()
	t.refreshNet()

	if fixed, err := autostartSync(); err != nil {
		logf("检查开机自启失败: %v", err)
	} else if fixed {
		logf("开机自启路径已更新为当前程序")
	}

	// 1. 顶部只读状态指示
	t.mStatus = systray.AddMenuItem("Capri 正在启动…", "当前服务运行状态")
	t.mStatus.Disable()

	systray.AddSeparator()
	// 2. 界面与连接
	t.mConsole = systray.AddMenuItem("打开 Capri 控制台", "在浏览器中打开本机页面，未运行时会自动启动")
	t.mCopyLAN = systray.AddMenuItem("复制内网访问地址", "复制同一局域网内其他设备可访问的地址到剪贴板")
	t.mHub = systray.AddMenuItem("打开 Hub 控制台", "在浏览器中打开已连接的 Hub 网页")
	t.mHub.Hide()

	systray.AddSeparator()
	// 3. 进程管理与休眠控制
	t.mToggle = systray.AddMenuItem("启动 Host", "启动或停止 Capri-host 进程")
	t.mRestart = systray.AddMenuItem("重启 Host", "改完设置文件后用它生效")
	t.mPower = systray.AddMenuItemCheckbox("阻止电脑休眠", "按住系统电源请求，屏幕仍可正常息屏", false)
	if !powerSupported() {
		t.mPower.Disable()
	} else if t.settingsSnapshot().ShouldKeepAwake() {
		if err := t.sleep.Enable(); err == nil {
			t.mPower.Check()
			logf("已根据配置恢复阻止电脑休眠")
		}
	}

	systray.AddSeparator()
	// 4. 设置与系统工具
	t.mSettings = systray.AddMenuItem("设置…", "在浏览器中打开 Capri 设置页面 (/settings)")
	t.mBoot = systray.AddMenuItemCheckbox("开机自启", "登录 Windows 时自动启动（默认关闭）", autostartEnabled())
	if !autostartSupported() {
		t.mBoot.Disable()
	}
	t.mInfo = systray.AddMenuItem("连接与诊断信息…", "查看本机名称、端口、内网/Hub 地址及详细网络诊断")
	t.mLog = systray.AddMenuItem("打开日志目录", "打开 Capri 与 Capri-host 的运行日志文件夹")
	t.mEdit = systray.AddMenuItem("编辑原始配置 (config.json)", "用文本编辑器打开 "+config.Path())

	systray.AddSeparator()
	// 6. 退出
	t.mQuit = systray.AddMenuItem("退出 Capri", "停止 Capri-host 并退出托盘")

	go t.watch(t.mConsole.ClickedCh, t.onOpenConsole)
	go t.watch(t.mCopyLAN.ClickedCh, t.onCopyLAN)
	go t.watch(t.mHub.ClickedCh, t.onOpenHub)
	go t.watch(t.mToggle.ClickedCh, t.onToggleHost)
	go t.watch(t.mRestart.ClickedCh, t.onRestartHost)
	go t.watch(t.mPower.ClickedCh, t.onToggleAwake)
	go t.watch(t.mSettings.ClickedCh, t.onOpenSettingsWeb)
	go t.watch(t.mBoot.ClickedCh, t.onToggleAutostart)
	go t.watch(t.mInfo.ClickedCh, t.onInfo)
	go t.watch(t.mLog.ClickedCh, func() { openPath(filepath.Join(config.Dir(), "logs")) })
	go t.watch(t.mEdit.ClickedCh, t.onEditSettings)
	go t.watch(t.mQuit.ClickedCh, t.onQuit)

	if t.settingsSnapshot().ShouldStartHostOnLaunch() {
		if err := t.host.start(); err != nil {
			logf("启动 Capri-host 失败: %v", err)
		}
	}

	t.poll()
	go t.pollLoop()
	go t.netRefreshLoop()

	logf("托盘已就绪（host=%s 端口=%d 休眠控制=%v 开机自启=%v）",
		t.host.bin, t.settingsSnapshot().ListenPort(), powerSupported(), autostartEnabled())
}

func (t *trayApp) watch(ch <-chan struct{}, fn func()) {
	for range ch {
		fn()
	}
}

func (t *trayApp) onExit() {
	_ = t.sleep.Close()
}

func (t *trayApp) pollLoop() {
	tick := time.NewTicker(refreshInterval)
	defer tick.Stop()
	for range tick.C {
		t.poll()
	}
}

func (t *trayApp) netRefreshLoop() {
	tick := time.NewTicker(netRefreshInterval)
	defer tick.Stop()
	for range tick.C {
		t.refreshNet()
	}
}

func (t *trayApp) refreshNet() {
	ni := netinfo.Local()
	t.netMu.Lock()
	t.net = ni
	t.netMu.Unlock()
}

func (t *trayApp) netSnapshot() netinfo.Info {
	t.netMu.RLock()
	defer t.netMu.RUnlock()
	return t.net
}

func (t *trayApp) reloadSettings() {
	s, err := loadSettings()
	if err != nil {
		logf("读取配置失败: %v", err)
	}
	t.mu.Lock()
	t.settings = s
	t.mu.Unlock()
}

func (t *trayApp) poll() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	api := t.api()
	var st hubstate.State
	var stateErr string
	var ver string

	if t.host.running() {
		if got, err := api.hubState(ctx); err == nil {
			st = got
		} else {
			stateErr = err.Error()
		}
		ver = api.hostVersion(ctx)
	}

	t.mu.Lock()
	t.state, t.stateErr, t.hostVer = st, stateErr, ver
	t.mu.Unlock()

	t.refresh()
}

func (t *trayApp) refresh() {
	st, stateErr := t.stateSnapshot()
	running := t.host.running()

	t.reloadSettings()
	port := t.settingsSnapshot().ListenPort()
	if running {
		if p, _, ok := t.host.endpoint(); ok {
			port = p
		}
	}

	status := statusText(running, st, stateErr)
	tip := fmt.Sprintf("Capri %s — %s\n%s", t.version, t.hostName(), status)
	if !running {
		tip = fmt.Sprintf("Capri %s — Host 未运行\n%s", t.version, t.host.lastError())
	}
	systray.SetTooltip(tip)

	// 1. 顶部只读状态行更新
	statusLine := menuStatusLine(running, port, st)
	t.mStatus.SetTitle(statusLine)
	t.mStatus.SetTooltip("状态：" + status)

	// 2. 进程启停与重启
	if running {
		t.mToggle.SetTitle("停止 Host")
		t.mRestart.Enable()
	} else {
		t.mToggle.SetTitle("启动 Host")
		t.mRestart.Disable()
	}

	// 3. 复制内网地址
	if u := lanURL(t.netSnapshot(), port); u != "" {
		ip := netinfo.PreferredIP(t.netSnapshot())
		t.mCopyLAN.SetTitle(fmt.Sprintf("复制内网地址 (%s)", ip))
		t.mCopyLAN.SetTooltip(u + "（点击复制到剪贴板）")
		t.mCopyLAN.Enable()
	} else {
		t.mCopyLAN.SetTitle("复制内网访问地址")
		t.mCopyLAN.SetTooltip("未找到可用的局域网地址")
		t.mCopyLAN.Disable()
	}

	if st.Paired != t.hubShown {
		if st.Paired {
			t.mHub.Show()
		} else {
			t.mHub.Hide()
		}
		t.hubShown = st.Paired
	}
	if st.Paired {
		t.mHub.SetTooltip(st.HubURL)
	}

	// 4. 休眠与自启状态同步
	if t.sleep.Enabled() != t.mPower.Checked() {
		if t.sleep.Enabled() {
			t.mPower.Check()
		} else {
			t.mPower.Uncheck()
		}
	}
	if autostartSupported() {
		if on := autostartEnabled(); on != t.mBoot.Checked() {
			if on {
				t.mBoot.Check()
			} else {
				t.mBoot.Uncheck()
			}
		}
	}
}

func (t *trayApp) hostName() string {
	st, _ := t.stateSnapshot()
	if strings.TrimSpace(st.HostName) != "" {
		return st.HostName
	}
	s := t.settingsSnapshot()
	if strings.TrimSpace(s.HostName) != "" {
		return s.HostName
	}
	return config.DefaultHostName
}

func (t *trayApp) status() string {
	st, stateErr := t.stateSnapshot()
	return statusText(t.host.running(), st, stateErr)
}

// ── menu actions ──────────────────────────────────────────────────────

func (t *trayApp) onOpenConsole() {
	if !t.host.running() {
		if err := t.host.start(); err != nil {
			t.errorDialog("无法启动 Host", err)
			return
		}
	}
	port, token, ok := t.host.endpoint()
	if !ok {
		t.errorDialog("Host 未能就绪", fmt.Errorf("没有记下启动时的端口"))
		return
	}
	if !t.waitListening(port, token, startTimeout) {
		t.hostNotReady(port)
		return
	}
	openURL(localURL(port))
}

func (t *trayApp) onOpenHub() {
	if st, _ := t.stateSnapshot(); st.HubURL != "" {
		openURL(st.HubURL)
	}
}

func (t *trayApp) onCopyLAN() {
	t.refreshNet()
	port := t.settingsSnapshot().ListenPort()
	if t.host.running() {
		if p, _, ok := t.host.endpoint(); ok {
			port = p
		}
	}
	u := lanURL(t.netSnapshot(), port)
	if u == "" {
		_ = zenity.Error("找不到可用的局域网地址。\n\n请确认本机已连接到路由器或 Wi-Fi。",
			zenity.Title("Capri"))
		return
	}
	if err := writeClipboard(u); err != nil {
		logf("复制内网地址到剪贴板失败: %v", err)
		_ = zenity.Error("复制到剪贴板失败：\n\n"+err.Error(), zenity.Title("Capri"))
		return
	}
	logf("已复制内网地址到剪贴板: %s", u)
	notify("Capri", "内网访问地址已复制到剪贴板：\n"+u)
}

func (t *trayApp) onToggleHost() {
	if t.host.running() {
		t.stopHost()
		return
	}
	if err := t.host.start(); err != nil {
		t.errorDialog("无法启动 Host", err)
	}
	t.refresh()
}

func (t *trayApp) onRestartHost() {
	if err := t.host.stop(stopGrace); err != nil {
		t.errorDialog("无法停止 Host", err)
		return
	}
	if err := t.host.start(); err != nil {
		t.errorDialog("无法启动 Host", err)
		return
	}
	port, token, ok := t.host.endpoint()
	if !ok {
		t.errorDialog("Host 未能就绪", fmt.Errorf("没有记下启动时的端口"))
		return
	}
	if !t.waitListening(port, token, startTimeout) {
		t.hostNotReady(port)
		return
	}
	notify("Capri", "Host 已按当前设置重启。")
	t.refresh()
}

func (t *trayApp) stopHost() {
	if err := t.host.stop(stopGrace); err != nil {
		t.errorDialog("无法停止 Host", err)
	}
	t.refresh()
}

func (t *trayApp) onOpenSettingsWeb() {
	if !t.host.running() {
		if err := t.host.start(); err != nil {
			t.errorDialog("无法启动 Host", err)
			return
		}
	}
	port, token, ok := t.host.endpoint()
	if !ok {
		t.errorDialog("Host 未能就绪", fmt.Errorf("没有记下启动时的端口"))
		return
	}
	if !t.waitListening(port, token, startTimeout) {
		t.hostNotReady(port)
		return
	}
	openURL(fmt.Sprintf("http://localhost:%d/settings", port))
}

func (t *trayApp) onEditSettings() {
	if err := openSettings(); err != nil {
		logf("打开设置失败: %v", err)
		_ = zenity.Error("无法打开设置文件：\n\n"+err.Error(), zenity.Title("Capri"))
		return
	}
}

func (t *trayApp) onToggleAutostart() {
	want := !autostartEnabled()
	if err := autostartSet(want); err != nil {
		logf("设置开机自启失败: %v", err)
		_ = zenity.Error("无法修改开机自启：\n\n"+err.Error(), zenity.Title("Capri"))
		t.refresh()
		return
	}
	if want {
		t.mBoot.Check()
		logf("已开启开机自启")
		notify("Capri", "已开启开机自启")
	} else {
		t.mBoot.Uncheck()
		logf("已关闭开机自启")
		notify("Capri", "已关闭开机自启")
	}
}

func (t *trayApp) onToggleAwake() {
	on, err := t.sleep.Toggle()
	if err != nil {
		logf("切换休眠阻止失败: %v", err)
		_ = zenity.Error("无法切换休眠设置：\n\n"+err.Error(), zenity.Title("Capri"))
		return
	}
	if on {
		t.mPower.Check()
		logf("已阻止电脑休眠")
		notify("Capri", "已开启休眠阻止（屏幕仍可正常息屏）")
	} else {
		t.mPower.Uncheck()
		logf("已恢复正常休眠")
		notify("Capri", "已恢复系统休眠设置")
	}
	_ = config.UpdateFile(func(f *config.File) {
		f.KeepAwake = &on
	})
}

func (t *trayApp) onInfo() {
	t.refreshNet()
	t.poll()
	_ = zenity.Info(t.infoText(), zenity.Title("Capri 连接与诊断信息"))
}

func (t *trayApp) onQuit() {
	t.quitOnce.Do(func() {
		logf("用户请求退出")
		t.stopHost()
		systray.Quit()
	})
}

func (t *trayApp) hostNotReady(port int) {
	if !t.host.running() {
		msg := t.host.lastError()
		if msg == "" {
			msg = "已退出"
		}
		t.errorDialog("Host 未能就绪", fmt.Errorf("%s\n\n日志：%s", msg, t.host.logPath()))
		return
	}
	t.errorDialog("Host 未能就绪", fmt.Errorf("端口 %d 未在 %s 内开始监听\n\n日志：%s",
		port, startTimeout, t.host.logPath()))
}

func (t *trayApp) waitListening(port int, token string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	api := newAPIClient(port, token)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		ok := api.alive(ctx)
		cancel()
		if ok {
			return true
		}
		if !t.host.running() {
			return false
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func (t *trayApp) errorDialog(title string, err error) {
	logf("%s: %v", title, err)
	_ = zenity.Error(title+"：\n\n"+err.Error(), zenity.Title("Capri"))
}
