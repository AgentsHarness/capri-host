# host↔hub 会话协议（v2）— host 侧实现说明

权威协议定义在 `capri-wire/PROTOCOL-V2.md`（帧模型、Sender/Receiver、关闭原因、
限额与默认值都由 `github.com/AgentsHarness/capri-wire` 提供）。本文件只记录
**capri-host 这一侧**的实现约定，以及阅读代码时容易误解的几处取舍。

三端（host / hub / FE）同版本发布，不保留 v1 兼容：`auth` 帧、`X-Hub-*` 请求头、
`seqStart`、`seq_reset`、`state_ack`、`ping`/`pong`、`host_status`、`subscribers` /
`viewing` 全部删除。

## 1. 上行 seq 就是事件 seq

`wire.Sender` 的日志是**每条事件一个条目**，序号由 bridge 的事件总线
（`acp.eventBus.publish`）分配一次，永不重编号。由此：

- `AppendAt(eventSeq, eventJSON)` 只接受 `next+1`；事件总线在上游丢了事件时，
  中间缺失的序号立刻补成 `wire.TrimmedEvent("", seq)` 占位，**空洞不上线**。
- `events` 帧是日志里一段连续条目的批次：`Frame.Seq` 是首条事件的 seq，
  `Frame.Events` 是原始事件字节，`Frame.Ack` 是下行水位。hub 逐条接收，因此
  它的水位与 FE 的 `EventSequencer` 是同一个坐标。
- 因此 `seq` 是「事件序号」而不是「帧序号」；不要按帧数推算它。

## 2. 上行日志（outbox）是唯一的修复路径

`internal/hub/outbox.go`。进程级（跨会话）持有，`forwardLoop` 生产、会话循环消费：

- 重连时从 hub 的 `ackUp` 处重放，`hello.resume.up` 只提供基线。
- 新 epoch（host 进程重启）的基线是 `bridge.LastSeq()`：第一次握手的
  `resume.up` 就是它，第一条实时事件是 `baseline+1`，更早的事件视为历史，
  由 FE 从 host 侧历史重建。
- 超过字节预算时，可丢的正文事件就地换成 `trimmed` 占位（**序号不变**），
  关键事件保留；关键条目自己就超过硬上限时以 `overflow` 结束会话，让 hub
  重连后按水位重放。
- 超长单条事件在入日志前就换占位，保证任何 `events` 帧都不超过
  `limits.maxFrame`。

## 3. 下行

- `audience`（绝对状态，带 `gen`）：`watchers == 0` 暂停上行（继续发 `hb`），
  `all:true` 关闭会话过滤，否则按 `sessions`（含一层子代理子会话）丢弃未观看
  会话的正文。
- `request`（可靠，占用下行 seq）：按 `reqId` 去重后执行本地 HTTP API。
  同 epoch 重连重投的同一 `reqId` **不会**再次执行：仍在跑的等待并重发结果，
  已完成的若响应还在内存就重发，否则回 `409 duplicate request`。
- `respond` 属于中继面，`seq:0`，带 `ack = 下行水位`；QUIC 上每个请求独占一条
  stream，WS 上与事件共用一条连接。

## 4. 时间尺度

- `hello_ack.liveness` 决定 `hb` 周期与空闲上限；`hb` 同时携带 `ack` 和
  host 状态快照，状态变化或收到新 ack 时立刻补发一帧。
- 超过 `idle` 没有任何下行帧即 `close(liveness)`。
- RTO = `2*keepalive`，收到 ack 有进展时重置；没有重试次数上限，空闲计时器
  才是判死依据。
- 传输自身的心跳（WS ping / QUIC keepalive）只为 NAT 与半开连接存在，不参与
  协议判定。QUIC 的 keepalive 是拨号时的连接级选项（`quicKeepAlive`），
  无法随 `hello_ack` 调整。

## 5. 压缩

`hello.caps.deflate` 提议、`hello_ack.caps.deflate` 回声确认后才启用；载荷
`< 256` 字节或压缩无收益则发裸帧。

- QUIC：4 字节大端长度前缀，最高位置 1 表示 raw deflate（RFC 1951，无 zlib 头）。
- WS：text = 裸 JSON，binary = 整条 raw deflate 流；下行不压缩。

## 6. 关闭与重连策略

关闭原因只有 `wire.CloseReason` 这一个枚举（WS 映射 `(code, reason)`，QUIC 映射
application error code），host 只按枚举决策：

| 判定 | 重连策略 |
| --- | --- |
| `superseded` | 立即重拨，**不重新配对**（token 仍然有效） |
| `auth` | 有配对码时重新配对一次，然后立即重连 |
| `protocol` | 退避并明确报「两端版本不一致」 |
| `liveness` / `shutdown` / `overflow` / `unreadable` / `normal` | 退避 |

握手层的 401 / QUIC auth 码视为 `auth`（没有关闭帧的地方靠握手状态判断）。
