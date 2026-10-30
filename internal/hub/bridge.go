package hub

import "github.com/AgentsHarness/capri-host/internal/acp"

// bridgeSource 是 hub client 对 *acp.Bridge 的消费者侧窄接口（Go 惯例：
// 接口定义在消费者一侧）。中继客户端只做三件事——订阅 bridge 事件流转发
// 上行、取状态快照拼 hb 心跳、问当前事件序号高水位（新 epoch 的
// hello.resume.up 基线）；除此之外不触碰 Bridge。
// *acp.Bridge 隐式满足本接口，main.go 与现有测试无需适配。
type bridgeSource interface {
	Subscribe() (ch chan acp.Event, unsubscribe func())
	Snapshot() acp.Status
	// LastSeq is the highest event seq the bus has assigned. It anchors a new
	// epoch: the hub seeds its up receiver with it, so the first live event is
	// LastSeq+1 and everything below is history the FE rebuilds from the host.
	LastSeq() uint64
}

// 编译期锚定：*acp.Bridge 必须持续满足本接口。
var _ bridgeSource = (*acp.Bridge)(nil)
