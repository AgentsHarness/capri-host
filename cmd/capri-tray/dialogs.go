//go:build windows

package main

import (
	"fmt"
	"strings"

	"github.com/ncruces/zenity"

	"github.com/AgentsHarness/capri-host/internal/config"
	"github.com/AgentsHarness/capri-host/internal/netinfo"
)

// notify displays a non-blocking toast/balloon notification when supported.
func notify(title, message string) {
	if err := zenity.Notify(message, zenity.Title(title)); err != nil {
		logf("系统通知: [%s] %s (err: %v)", title, message, err)
	}
}

// infoText is the status panel. Everything below the addresses is diagnosis:
// which interface the LAN address came from, where the hub name resolves to,
// and why the link is down. That is the difference between "it does not work"
// and knowing which hop broke.
func (t *trayApp) infoText() string {
	var b strings.Builder
	ni := t.netSnapshot()
	port := t.settingsSnapshot().ListenPort()
	st, stateErr := t.stateSnapshot()
	t.mu.Lock()
	hostVer := t.hostVer
	t.mu.Unlock()

	fmt.Fprintf(&b, "本机名称：%s\n", t.hostName())
	fmt.Fprintf(&b, "Host ID：%s\n", orDash(st.HostID))
	fmt.Fprintf(&b, "托盘版本：%s　Host 版本：%s\n", t.version, orDash(hostVer))
	fmt.Fprintf(&b, "端口：%d\n\n", port)

	if t.host.running() {
		b.WriteString("Host 进程：运行中\n")
	} else {
		fmt.Fprintf(&b, "Host 进程：未运行（%s）\n", t.host.lastError())
	}
	fmt.Fprintf(&b, "本机地址：%s\n", localURL(port))
	if u := lanURL(ni, port); u != "" {
		fmt.Fprintf(&b, "内网地址：%s\n", u)
	} else {
		b.WriteString("内网地址：未找到可用的局域网地址\n")
	}

	if !st.Configured {
		b.WriteString("\nhub 地址：未配置（在「设置」里填写地址并配对）\n")
	} else {
		fmt.Fprintf(&b, "\nhub 地址：%s\n", st.HubURL)
		b.WriteString("配对状态：")
		if st.Paired {
			b.WriteString("已配对\n")
		} else {
			b.WriteString("未配对（在「设置」里输入配对码）\n")
		}
		fmt.Fprintf(&b, "连接状态：%s\n", t.status())
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
	if stateErr != "" {
		fmt.Fprintf(&b, "接口状态：读不到（%s）\n", stateErr)
	}

	if len(ni.Ifaces) > 0 || ni.Outbound != "" {
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

	fmt.Fprintf(&b, "\n设置文件：%s\n", config.Path())
	fmt.Fprintf(&b, "日志：%s\n", t.log.Path())
	fmt.Fprintf(&b, "       %s\n", t.host.logPath())
	b.WriteString("\n（按 Ctrl+C 可复制本窗口全部内容）")
	return b.String()
}

func orDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "—"
	}
	return s
}
