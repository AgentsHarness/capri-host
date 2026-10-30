# PROTOCOL — host 侧传输说明

协议权威定义在 `capri-wire/PROTOCOL-V2.md`，帧模型、Sender/Receiver、关闭原因
与默认限额都由 `github.com/AgentsHarness/capri-wire` 提供。本文只记录
**capri-host 这一侧**收发帧时需要注意的传输细节。

可靠性模型与故障矩阵：`acp-hub/docs/SESSION-V2-RELIABILITY.md`。

## 1. 帧封装

每帧是一个 JSON 对象 `{"v":2,"type":...,"seq":N,"ack":M}`。

- 新生连接的第一帧永远是 `hello`（epoch、token、resume、caps）；hub 回
  `hello_ack` 后，下行 seq、上行水位、重传与空闲计时都交给会话循环。
- 可靠帧带 seq：`events`、`audience`、`request`；不可靠帧必须不带：
  `hello`、`hello_ack`、`hb`、`respond`。
- `ack` 是本端对反方向的水位，随可携带它的帧一起发出。
- `hello_ack.hubBoot` 标识 hub 进程；中继备忘按 `(hubBoot, reqId)` 键控。

## 2. 握手顺序

1. 拨号并完成传输层认证（WS 请求头 bearer token / QUIC 证书固定）。
2. 写 `hello`，在会话建立窗口内等 `hello_ack`；超时按 `liveness` 关闭。
3. 采纳 `ackUp`/`ackDown`、`liveness`、`limits`、`audience`、`hubBoot`，
   `resume.down` 只作为本端声明，权威值是 hub 的 `ackDown`。
4. 首次心跳，然后重发同一 `hubBoot` 下已完成的应答。
5. 之后进入正常收发：先 flush 上行日志，再等下一帧。

不能解码的 `hello_ack` 按 `protocol` 关闭；握手期间无响应按 `liveness`
关闭。

## 3. 传输

- **WebSocket**：text = 裸 JSON 帧；binary = 整条 raw deflate 流。只在
  `hello_ack.caps.deflate` 回声后压缩上行，下行不压缩。
- **QUIC**：一条双向控制流承载 `hello`、`events`、`audience`、`request`、
  `hb`；每帧 4 字节大端长度 + 帧体，长度最高位表示 raw deflate。`respond`
  另开单向请求流（大应答按块分帧），控制流只由 hub 写。
- 传输自身 keepalive（WS ping / QUIC keepalive）只为 NAT 与半开连接存在，
  不参与协议判活；协议判活只有 `hb` 与 `idle`。

## 4. 关闭映射

关闭原因只有 `wire.CloseReason` 一个枚举：`normal`、`shutdown`、
`superseded`、`auth`、`protocol`、`liveness`、`overflow`、`unreadable`。
共享表把它映射成 WS 的 `(code, reason)` 与 QUIC 的 application error code；
host 只按枚举决定重连策略，从不匹配字符串。
