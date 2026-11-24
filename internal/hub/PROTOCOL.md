# PROTOCOL — host 侧传输说明

协议权威定义在 `wire/PROTOCOL-V2.md`（文件名为历史保留，内容为 wire v3），
帧模型、Sender/Receiver、关闭原因与默认限额都由
`github.com/AgentsHarness/capri-hub/wire` 提供。本文只记录 **capri-host
这一侧**收发帧时需要注意的传输细节。

可靠性模型与故障矩阵：见 `wire/PROTOCOL-V2.md` 的可靠投递与存活章节。

## 1. 帧封装

每帧是一个 JSON 对象 `{"v":3,"type":...,"seq":N,"ack":M}`。

- 新生连接的第一帧永远是 `hello`（epoch、token、resume、caps）；hub 回
  `hello_ack` 后，下行 seq、上行水位、重传与空闲计时都交给会话循环。
- 可靠帧带 seq：`events`、`request`；不可靠帧必须不带：
  `hello`、`hello_ack`、`audience`、`hb`、`respond`。
- `ack` 是本端对反方向的水位，随可携带它的帧一起发出。下行 ack 前进后，
  如果 `wire.AckDelay`（20ms）内没有其他出站帧，必须立即补一帧 heartbeat；
  不能等周期心跳，否则确认延迟会等于 keepalive。
- `audience` 是绝对状态，不可靠、按 `gen` 排序、最新值生效：不带 seq 也不
  需要 ack。相同 `gen` 重复应用是无操作；旧 `gen` 忽略。hub 每个周期心跳重发
  最新快照，因此丢帧最多滞后一个心跳周期即可自愈。
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

### 4.1 写请求幂等（`idemKey` / `idemScope`）

`request` 帧可携带 `idemKey`（1..128 可打印 ASCII）和 `idemScope`
（0..64，由 hub 按浏览器身份生成：`root` / `user:<id>` / `guest:<id>`）。
`serveRelay` 把两者原样注入本地 HTTP 请求的 `Idempotency-Key` /
`Idempotency-Scope` 头；host 的 `withIdempotency` 中间件（`withAuth`
内侧）据此做 at-most-once：

- 同一个 `(hostId, scope, key)` 最多执行一次。重复请求（新 `reqId`、相同
  key）直接回放第一次的 status/body（直连模式下还会带
  `Idempotent-Replayed: true`）；
- 同 key 不同请求指纹（`sha256(method + "\n" + requestURI + "\n" + body)`）
  返回 409 `idempotency_key_conflict`，不执行；
- host 重启后只有 `started` 记录时返回 409
  `idempotency_outcome_unknown`，绝不自动再次执行；
- 结果持久化在 `~/.capri-host/relay-idempotency.jsonl`
  （`RELAY_IDEMPOTENCY_FILE` 覆盖），默认 TTL 24h；响应超过 8MiB 时
  标记为不可回放，重复请求返回 409 `idempotency_result_unavailable`。

两者的分工：`relayMemo` 按 `(hubBoot, reqId)` 处理**同一次传输**的重放
（新 reqId 不受它约束）；幂等 store 按**浏览器逻辑操作的 key** 跨 reqId、
跨重连、跨 host 重启合并执行。`promptId` 仍是 prompt 的应用层键，三者
互不冲突。


## 5. 关闭映射

关闭原因只有 `wire.CloseReason` 一个枚举：`normal`、`shutdown`、
`superseded`、`auth`、`protocol`、`liveness`、`overflow`、`unreadable`。
共享表把它映射成 WS 的 `(code, reason)` 与 QUIC 的 application error code；
host 只按枚举决定重连策略，从不匹配字符串。
