//go:build windows

package tray

import (
	"context"
	"embed"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"sync"
	"time"

	"fyne.io/systray"
	"github.com/ncruces/zenity"

	"github.com/AgentsHarness/capri-host/internal/autostart"
	"github.com/AgentsHarness/capri-host/internal/hub"
	"github.com/AgentsHarness/capri-host/internal/netinfo"
	"github.com/AgentsHarness/capri-host/internal/power"
	"github.com/AgentsHarness/capri-host/internal/procattr"
)

//go:embed assets/capri.ico
var assets embed.FS

// refreshInterval is how often the tooltip and menu labels re-read the hub
// state. Two seconds is frequent enough that the menu is already correct by the
// time it opens, and the read is a few atomic loads.
const refreshInterval = 2 * time.Second

// netRefreshInterval is how often the machine's own addressing is re-read.
// Much slower than refreshInterval because netinfo.Local dials UDP to find the
// default route, which costs up to a few seconds on an offline machine — far
// too expensive to sit in the two-second loop, and addresses change on the
// timescale of joining a network, not of opening a menu.
const netRefreshInterval = 30 * time.Second

// pairDialogTimeout bounds one pairing attempt made from the menu.
const pairDialogTimeout = 25 * time.Second

// Supported reports whether this build has a tray.
func Supported() bool { return true }

// Run shows the tray icon and blocks until the user quits or Stop is called.
//
// It must be called from the main goroutine: systray's Windows backend owns a
// window and its message pump, and a message pump belongs to the thread that
// created the window.
func Run(d Deps) {
	t := &menu{deps: d, sleep: power.New("capri-host 正在保持本机唤醒")}
	systray.Run(t.onReady, t.onExit)
}

// Stop tears the tray down, which unblocks Run.
func Stop() { systray.Quit() }

type menu struct {
	deps  Deps
	sleep *power.Inhibitor

	hubMode bool

	mLAN   *systray.MenuItem
	mHub   *systray.MenuItem
	mPair  *systray.MenuItem
	mAwake *systray.MenuItem
	mBoot  *systray.MenuItem

	// net is the cached addressing snapshot, refreshed on its own slow loop.
	netMu sync.RWMutex
	net   netinfo.Info

	// hubShown tracks the hub item's visibility so Show/Hide is called only on
	// a transition rather than twice a second.
	hubShown bool

	quitOnce sync.Once
}

func (t *menu) onReady() {
	t.hubMode = t.deps.HubURL != "" && t.deps.HubState != nil && t.deps.Pair != nil

	if icon, err := assets.ReadFile("assets/capri.ico"); err == nil {
		systray.SetIcon(icon)
	} else {
		log.Printf("[tray] 读取图标失败: %v", err)
	}
	systray.SetTitle("Capri Host")
	systray.SetTooltip("Capri Host")

	// Read addressing once up front so the LAN item is already correct — and
	// correctly enabled or not — the first time the menu is opened.
	t.refreshNet()

	// A stale registration is repaired before the checkbox is drawn, so the
	// box never claims autostart is on while pointing at an exe that moved.
	if fixed, err := autostart.Sync(); err != nil {
		log.Printf("[tray] 检查开机自启失败: %v", err)
	} else if fixed {
		log.Printf("[tray] 开机自启路径已更新为当前程序")
	}

	mLocal := systray.AddMenuItem("打开本机地址", t.deps.LocalURL())
	t.mLAN = systray.AddMenuItem("打开内网地址", "同一局域网内的手机/平板用这个地址")
	if t.deps.HubURL != "" {
		t.mHub = systray.AddMenuItem("打开 hub 地址", t.deps.HubURL)
		// Starts hidden: until the state is read, "paired" is unknown, and
		// showing it first would flash an item that then disappears.
		t.mHub.Hide()
	}

	systray.AddSeparator()
	if t.hubMode {
		t.mPair = systray.AddMenuItem("配对 hub…", "输入 hub 上显示的 6 位配对码")
	}
	t.mAwake = systray.AddMenuItemCheckbox("阻止电脑休眠", "按住系统电源请求，屏幕仍可正常息屏", false)
	if !power.Supported() {
		t.mAwake.Disable()
	}

	systray.AddSeparator()
	mInfo := systray.AddMenuItem("连接信息…", "本机名称、本机/内网/hub 地址与配对状态")
	t.mBoot = systray.AddMenuItemCheckbox("开机自启", "登录 Windows 时自动启动（默认关闭）", autostart.Enabled())
	if !autostart.Supported() {
		t.mBoot.Disable()
	}
	mLog := systray.AddMenuItem("打开日志", t.deps.LogPath)
	if t.deps.LogPath == "" {
		mLog.Disable()
	}

	systray.AddSeparator()
	mQuit := systray.AddMenuItem("退出", "关闭 Capri Host 与 grok 子进程")

	// Each handler runs in its own goroutine: the dialogs block for as long
	// as the user leaves them open, and doing that on the click-dispatch
	// goroutine would freeze every other menu item behind it.
	go t.watch(mLocal.ClickedCh, func() { OpenURL(t.deps.LocalURL()) })
	go t.watch(t.mLAN.ClickedCh, t.onOpenLAN)
	if t.mHub != nil {
		go t.watch(t.mHub.ClickedCh, func() { OpenURL(t.deps.HubURL) })
	}
	if t.mPair != nil {
		go t.watch(t.mPair.ClickedCh, t.onPair)
	}
	go t.watch(t.mAwake.ClickedCh, t.onToggleAwake)
	go t.watch(mInfo.ClickedCh, t.onInfo)
	go t.watch(t.mBoot.ClickedCh, t.onToggleAutostart)
	go t.watch(mLog.ClickedCh, func() { openPath(t.deps.LogPath) })
	go t.watch(mQuit.ClickedCh, t.onQuit)

	t.refresh()
	go t.refreshLoop()
	go t.netRefreshLoop()
	// Logged last, so its presence in the log proves the icon was created and
	// every handler is attached. Without it the only evidence the tray came up
	// is that the process failed to exit, which is not evidence at all.
	log.Printf("[tray] 托盘已就绪（hub 模式=%v，休眠控制=%v，开机自启=%v）",
		t.hubMode, power.Supported(), autostart.Enabled())
}

// watch runs fn for every click on ch. ClickedCh is closed when the tray shuts
// down, which ends the goroutine.
func (t *menu) watch(ch <-chan struct{}, fn func()) {
	for range ch {
		fn()
	}
}

func (t *menu) onExit() {
	// Release the power request explicitly. Windows drops it when the process
	// dies anyway, but leaving it to process teardown means a crash during
	// shutdown could leave the machine unable to sleep with nothing on screen
	// to explain why.
	_ = t.sleep.Close()
}

func (t *menu) refreshLoop() {
	tick := time.NewTicker(refreshInterval)
	defer tick.Stop()
	for range tick.C {
		t.refresh()
	}
}

func (t *menu) netRefreshLoop() {
	tick := time.NewTicker(netRefreshInterval)
	defer tick.Stop()
	for range tick.C {
		t.refreshNet()
	}
}

func (t *menu) refreshNet() {
	ni := netinfo.Local()
	t.netMu.Lock()
	t.net = ni
	t.netMu.Unlock()
}

func (t *menu) netSnapshot() netinfo.Info {
	t.netMu.RLock()
	defer t.netMu.RUnlock()
	return t.net
}

func (t *menu) refresh() {
	st := t.state()
	tip := fmt.Sprintf("Capri Host %s — %s\n%s", t.deps.Version, t.deps.HostName, statusText(st))
	systray.SetTooltip(tip)

	// LAN item: the address is only knowable at runtime and can vanish when a
	// network is left, so the item carries the address in its tooltip and is
	// disabled rather than silently opening nothing.
	if t.mLAN != nil {
		if u := t.deps.LANURL(t.netSnapshot()); u != "" {
			t.mLAN.SetTooltip(u)
			t.mLAN.Enable()
		} else {
			t.mLAN.SetTooltip("未找到可用的局域网地址")
			t.mLAN.Disable()
		}
	}

	// Hub item appears only once a pairing exists. An unpaired host cannot
	// serve anything through the hub, so the address would open a page that
	// does not know this machine — worse than no menu entry at all.
	if t.mHub != nil {
		if st.Paired != t.hubShown {
			if st.Paired {
				t.mHub.Show()
			} else {
				t.mHub.Hide()
			}
			t.hubShown = st.Paired
		}
	}

	if t.mPair != nil {
		t.mPair.SetTitle("配对 hub…（" + statusText(st) + "）")
	}
	// Keep the checkbox honest even if Enable/Disable failed underneath us.
	if t.mAwake != nil {
		if t.sleep.Enabled() != t.mAwake.Checked() {
			if t.sleep.Enabled() {
				t.mAwake.Check()
			} else {
				t.mAwake.Uncheck()
			}
		}
	}
	// The registration lives in the registry, where anything else can change
	// it — msconfig, Task Manager's Startup tab, another copy of this exe.
	// Re-reading keeps the box showing what Windows will actually do.
	if t.mBoot != nil && autostart.Supported() {
		if on := autostart.Enabled(); on != t.mBoot.Checked() {
			if on {
				t.mBoot.Check()
			} else {
				t.mBoot.Uncheck()
			}
		}
	}
}

func (t *menu) state() hub.State {
	if t.deps.HubState == nil {
		return hub.State{}
	}
	return t.deps.HubState()
}

func (t *menu) onOpenLAN() {
	// Re-read rather than trusting the 30-second cache: a click is a direct
	// request, and being handed a stale address after switching Wi-Fi is
	// exactly the failure this item exists to avoid.
	t.refreshNet()
	u := t.deps.LANURL(t.netSnapshot())
	if u == "" {
		_ = zenity.Error("找不到可用的局域网地址。\n\n请确认本机已连接到路由器或 Wi-Fi。",
			zenity.Title("Capri Host"))
		return
	}
	OpenURL(u)
}

func (t *menu) onPair() {
	st := t.state()
	prompt := strings.Join([]string{
		"输入 hub 上显示的 6 位配对码：",
		"",
		"hub 地址：" + st.HubURL,
		"当前状态：" + statusText(st),
		"",
		"配对码在 hub 的启动日志里（docker logs capri-hub），",
		"也可以直接读 hub 的 GET /api/pairing。15 分钟过期。",
	}, "\n")

	code, err := zenity.Entry(prompt, zenity.Title("配对 hub"))
	if err != nil {
		return // cancelled
	}
	if strings.TrimSpace(code) == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), pairDialogTimeout)
	defer cancel()
	if err := t.deps.Pair(ctx, code); err != nil {
		log.Printf("[tray] 配对失败: %v", err)
		_ = zenity.Error("配对失败：\n\n"+err.Error(), zenity.Title("配对失败"))
		return
	}
	log.Printf("[tray] 配对成功")
	_ = zenity.Info("配对成功。正在用新凭证重新连接 hub。", zenity.Title("配对成功"))
	t.refresh()
}

func (t *menu) onToggleAutostart() {
	// Drive off what the registry says, not off the checkbox: the two can
	// disagree when something outside this process changed the registration.
	want := !autostart.Enabled()
	if err := autostart.Set(want); err != nil {
		log.Printf("[tray] 设置开机自启失败: %v", err)
		_ = zenity.Error("无法修改开机自启：\n\n"+err.Error(), zenity.Title("Capri Host"))
		t.refresh() // put the box back where reality is
		return
	}
	if want {
		t.mBoot.Check()
		log.Printf("[tray] 已开启开机自启")
	} else {
		t.mBoot.Uncheck()
		log.Printf("[tray] 已关闭开机自启")
	}
}

func (t *menu) onToggleAwake() {
	on, err := t.sleep.Toggle()
	if err != nil {
		log.Printf("[tray] 切换休眠阻止失败: %v", err)
		_ = zenity.Error("无法切换休眠设置：\n\n"+err.Error(), zenity.Title("Capri Host"))
	}
	if on {
		t.mAwake.Check()
		log.Printf("[tray] 已阻止电脑休眠")
	} else {
		t.mAwake.Uncheck()
		log.Printf("[tray] 已恢复正常休眠")
	}
}

func (t *menu) onInfo() {
	// A click means the numbers are being looked at, so pay for a fresh read
	// instead of showing a snapshot up to thirty seconds old.
	t.refreshNet()
	_ = zenity.Info(t.infoText(), zenity.Title("Capri Host 连接信息"))
}

func (t *menu) infoText() string {
	ni := t.netSnapshot()
	var b strings.Builder

	fmt.Fprintf(&b, "本机名称：%s\n", t.deps.HostName)
	fmt.Fprintf(&b, "Host ID：%s\n", t.deps.HostID)
	fmt.Fprintf(&b, "版本：%s　端口：%d\n\n", t.deps.Version, t.deps.Port)

	fmt.Fprintf(&b, "本机地址：%s\n", t.deps.LocalURL())
	if u := t.deps.LANURL(ni); u != "" {
		fmt.Fprintf(&b, "内网地址：%s\n", u)
	} else {
		b.WriteString("内网地址：未找到可用的局域网地址\n")
	}

	st := t.state()
	if !st.Configured {
		b.WriteString("hub 地址：未配置（仅本机模式）\n")
	} else {
		fmt.Fprintf(&b, "hub 地址：%s\n", st.HubURL)
	}

	// Everything below is diagnosis: which interface the LAN address came
	// from, where the hub name resolves to, and why the link is down. It is
	// the difference between "it does not work" and knowing which hop broke.
	if len(ni.Ifaces) > 1 || ni.Outbound != "" {
		b.WriteString("\n本机网卡：\n")
		if len(ni.Ifaces) == 0 {
			b.WriteString("  （无）\n")
		}
		for _, ifc := range ni.Ifaces {
			mark := "  "
			if ifc.IP == ni.Outbound {
				mark = "* " // the default route's source address
			}
			fmt.Fprintf(&b, "%s%s  %s\n", mark, ifc.IP, ifc.Name)
		}
	}

	if st.Configured {
		b.WriteString("\n配对状态：")
		if st.Paired {
			b.WriteString("已配对\n")
		} else {
			b.WriteString("未配对（用「配对 hub」输入配对码）\n")
		}
		fmt.Fprintf(&b, "连接状态：%s\n", statusText(st))
		if _, ips, err := netinfo.ResolveHub(st.HubURL); err != nil {
			fmt.Fprintf(&b, "hub 解析：失败（%v）\n", err)
		} else if len(ips) > 0 {
			fmt.Fprintf(&b, "hub 解析：%s\n", strings.Join(ips, ", "))
		}
		if st.Connected && st.UptimeSec > 0 {
			fmt.Fprintf(&b, "已连接：%s\n", humanDuration(st.UptimeSec))
		}
		if !st.Connected && st.LastError != "" {
			fmt.Fprintf(&b, "最近错误：%s\n", st.LastError)
		}
	}

	fmt.Fprintf(&b, "\n配置文件：%s\n日志：%s\n", t.deps.ConfigPath, t.deps.LogPath)
	b.WriteString("\n（按 Ctrl+C 可复制本窗口全部内容）")
	return b.String()
}

func (t *menu) onQuit() {
	t.quitOnce.Do(func() {
		log.Printf("[tray] 用户请求退出")
		if t.deps.Quit != nil {
			t.deps.Quit()
		}
		systray.Quit()
	})
}

// hiddenCmd builds a command that spawns no console window. Without this a
// GUI-subsystem process still gets a console allocated for every console child,
// and the default terminal makes it visible — which is exactly the black box
// this build exists to remove.
func hiddenCmd(name string, args ...string) *exec.Cmd {
	cmd := exec.Command(name, args...)
	procattr.HideConsole(cmd)
	return cmd
}

// Alert shows a modal error. It is the only way a -H=windowsgui binary can
// report a startup failure: there is no stderr attached, so a message written
// to the log alone would leave a user who double-clicked the exe staring at
// nothing happening.
func Alert(title, msg string) {
	_ = zenity.Error(msg, zenity.Title(title))
}

// OpenURL opens a URL in the default browser via the shell's URL handler.
func OpenURL(u string) {
	if u == "" {
		return
	}
	// rundll32 is used rather than `cmd /c start` because start treats the
	// first quoted argument as a window title and mangles URLs containing &.
	if err := hiddenCmd("rundll32", "url.dll,FileProtocolHandler", u).Start(); err != nil {
		log.Printf("[tray] 打开 %s 失败: %v", u, err)
	}
}

// openPath opens a file or folder with its registered handler.
func openPath(p string) {
	if p == "" {
		return
	}
	if err := hiddenCmd("rundll32", "url.dll,FileProtocolHandler", p).Start(); err != nil {
		log.Printf("[tray] 打开 %s 失败: %v", p, err)
	}
}

func humanDuration(sec int64) string {
	d := time.Duration(sec) * time.Second
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%d 秒", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%d 分钟", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%d 小时 %d 分", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%d 天 %d 小时", int(d.Hours())/24, int(d.Hours())%24)
	}
}
