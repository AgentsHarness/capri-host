// Untagged for the same reason as urls.go: rendering a snapshot into a line of
// text involves no platform API, and this is the piece whose wording users
// actually read — it belongs under `go test ./...`.

package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/AgentsHarness/capri-host/internal/hubstate"
)

// statusText is the one-line link summary shown in the tooltip.
//
// The distinction between "未配对" and "连接中…" is the whole reason the host's
// client grew a State method: previously nothing could tell them apart, so a
// host that had never been paired looked identical to one whose hub was simply
// down, and the only remedy offered for either was to restart the process.
//
// hostRunning comes first because a stopped host is not a disconnected hub:
// reporting "未连接" for a process the user just stopped would send them
// looking at the network.
func statusText(hostRunning bool, st hubstate.State, stateErr string) string {
	if !hostRunning {
		return "Host 未运行"
	}
	if stateErr != "" && !st.Configured {
		return "Host 未就绪"
	}
	switch {
	case !st.Configured:
		return "本机模式（未配置 hub）"
	case !st.Paired:
		return "未配对"
	case st.Connected:
		if st.Transport != "" {
			return "已连接（" + strings.ToUpper(st.Transport) + "）"
		}
		return "已连接"
	case st.LastError != "":
		return "未连接"
	default:
		return "连接中…"
	}
}

// menuStatusLine is the disabled first row of the tray menu. It follows the
// same connected / unpaired / down / dialing split as statusText.
func menuStatusLine(running bool, port int, st hubstate.State) string {
	if !running {
		return "Capri 未运行"
	}
	switch {
	case st.Connected:
		if st.Transport != "" {
			return fmt.Sprintf("运行中 · :%d · Hub (%s)", port, strings.ToUpper(st.Transport))
		}
		return fmt.Sprintf("运行中 · :%d · Hub", port)
	case st.Configured && !st.Paired:
		return fmt.Sprintf("运行中 · :%d · Hub 未配对", port)
	case st.Configured && st.LastError != "":
		return fmt.Sprintf("运行中 · :%d · Hub 未连接", port)
	case st.Configured:
		return fmt.Sprintf("运行中 · :%d · Hub 连接中…", port)
	default:
		return fmt.Sprintf("运行中 · :%d", port)
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
