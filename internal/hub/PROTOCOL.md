# PROTOCOL — host 侧传输说明

协议权威定义在 `wire/PROTOCOL-V2.md`，帧模型、Sender/Receiver、关闭原因
与默认限额都由 `github.com/AgentsHarness/capri-hub/wire` 提供。本文只记录
**capri-host 这一侧**收发帧时需要注意的传输细节。

可靠性模型与故障矩阵：`acp-hub/docs/SESSION-V2-RELIABILITY.md`。

## 1. 帧封装

每帧是一个 JSON 对象 `{"v":2,"type":...,"seq":N,"ack":M}`。

- 新生连接的第一帧永远是 `hello`（epoch、token、resume、caps）；hub 回
  `hello_ack` 后，下行 seq、上行水位、重传与空闲计时都交给会话循环。
- 可靠帧带 seq：`events`、`audience`、`request`；不可靠帧必须不带：
  `hello`、`hello_ack`、`hb`、`respond`。
- `ack` 是本端对反方向的水位，随可携带它的帧一起发出。
- `audience` 是绝对状态：hub 可能在未确认期间原地更新同一 seq 的 payload。
  收到 seq 不高于已应用水位的 `audience` 时，只要 `gen` 更新，仍必须应用，
  不能按普通重传丢弃。
- `hello_ack.hubBoot` 标识 hub 进程；中继备忘按 `(hubBoot, reqId)` 键控。

## 2. 握手顺序

1. 拨号并完成传输层认证（WS 请求头 bearer token / QUIC 证书固定）。
2. 写 `hello`（caps 提供 `deflate`、`transportUnits`、`relayAck`），在会话
   建立窗口内等 `hello_ack`；超时按 `liveness` 关闭。
3. 采纳 `ackUp`/`ackDown`、`liveness`、`limits`、`audience`、`hubBoot`，
   `resume.down` 只作为本端声明，权威值是 hub 的 `ackDown`。`hello_ack.caps`
   必须回声 `transportUnits` 与 `relayAck`，否则按 `protocol` 关闭——没有
   前者分不清 transport unit 与 text JSON，没有后者无法确认中继应答。
4. 用 `hello_ack.relayAcks` 核对中继应答日志（见 §5）。
5. 首次心跳，然后发出仍未确认的应答。
6. 之后进入正常收发：先 flush 上行日志，再等下一帧。

不能解码的 `hello_ack` 按 `protocol` 关闭；握手期间无响应按 `liveness`
关闭。

会话循环从不直接写 socket：心跳、上行 `events`、中继 `respond` 都交给
writer goroutine（`writer.go`）。`events` 帧写完才推进 `sentThrough`；
写错误回报给会话，由会话关闭链路。会话结束时先关 writer 再关传输，并等
writer 退出后才允许下一条会话接管应答日志。

## 3. 传输

- **WebSocket**：text = 一整条 JSON 帧（小且未压缩）；binary = 一个
  transport unit。压缩由 writer 决定，单元头里的 compressed 位就是全部
  信息；下行 binary 交给 `wire.Reassembler`，重组后再按标志 inflate。
- **QUIC**：控制流每条记录是一个 transport unit（沿用 4 字节大端长度前缀，
  长度最高位只服务中继流，控制流按单元自身的标志判断压缩）。`respond`
  另开单向请求流，仍按整帧分帧：4 字节长度前缀 + 完整 JSON 帧，最高位表示
  raw deflate；控制流只由 hub 写。
- 大于 `wire.UnitSize`（32KiB）的帧拆成多个 unit，unit 之间可以插入一个
  完整控制帧（`hb`），所以大帧不会拖住判活；同一方向同时只开一个分片帧。
- `SetWriteTimeout` 限定单次单元写入（hello_ack 后取 idle 的 3/4），
  `SetMaxFrame` 限定重组上限。
- 传输自身 keepalive（WS ping / QUIC keepalive）只为 NAT 与半开连接存在，
  不参与协议判活；协议判活是 `hb`（含 `probe`/`probeAck`）与 `idle`。

## 4. 中继应答（`respond`）

- 每个请求的应答是一个**进程级**应答日志（`relay_outbox.go`）中的一项，
  不是会话队列：链路断了、甚至当前没有会话，应答都留在日志里等下一次
  `hello_ack`。
- 应答按 `relayChunkSize`（256KiB 原文）分块；每块一个 `respond`，带
  `chunkSeq`（1 起、连续，内联 `body` 就是第 1 块）。28MB 应答约 112 块，
  远低于 hub 的 1500 帧续传窗口。
- 送达证明是 `hb.relayAcks` / `hello_ack.relayAcks` 里的
  `{reqId, chunk, done, abort}`：
  - `chunk` 是 hub 已连续应用的最高块，日志据此丢弃前缀并从下一块继续；
  - `done` 表示整条应答已应用，`abort` 表示 hub 已放弃该请求；
  - `hello_ack` 的表是权威快照：没列出的应答说明 hub 已不再持有，直接丢弃
    并给 memo 留下墓碑；`hb` 中途只应用列出的条目（缺失不视为丢失）。
- 每项有自己的 `sent` 游标；重连时回到 `acked`，因此从 hub 的水位继续，
  不会重发已确认的块。RTO 只补发“最老的未确认块”，不会整条重放。
- 请求去重（`relayMemo`）只保留墓碑，不缓存响应体：同一 `(hubBoot, reqId)`
  的重复请求不会再执行，也不会产生第二条应答；日志里的应答会自行重发。
- 超过日志字节上限（默认按两个最大答案的 base64 体积，约 75MiB）时，只把
  **当前**请求以小块 502 结束（`chunkSeq=1`），不影响会话与其他应答。
- `hubBoot` 变化（hub 重启）会清空应答日志与 memo：reqId 空间已重置。

## 5. 关闭映射

关闭原因只有 `wire.CloseReason` 一个枚举：`normal`、`shutdown`、
`superseded`、`auth`、`protocol`、`liveness`、`overflow`、`unreadable`。
共享表把它映射成 WS 的 `(code, reason)` 与 QUIC 的 application error code；
host 只按枚举决定重连策略，从不匹配字符串。
