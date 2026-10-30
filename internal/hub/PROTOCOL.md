# host↔hub 上行压缩协议（deflate）

状态：**双侧已实现并对齐**（capri-host `internal/hub/client.go`、acp-hub
`cmd/capri-hub/main.go`）。本文件与 acp-hub `internal/hub/PROTOCOL.md` 保持一致。

三端（host / hub / FE）同版本发布，协议不保留旧版本兼容：任何一侧缺少下述
字段或握手项，另一侧直接拒绝会话，而不是退化成裸 JSON 或旧行为。

> 事件语义（seq 分配 / 双路去重 / 分级背压 / 断线恢复）的契约另见
> [docs/EVENT-CONTRACTS.md](../../docs/EVENT-CONTRACTS.md)。

## 0. 帧可解码性（两条传输一致）

Hub 解不开的上行帧一律**结束会话**，不跳过：

- WS：binary 帧 inflate 失败；
- 两条传输：`seqStart` 与首事件 `seq` 不一致。

跳过它会让该帧的序号凭空消失：下一帧更大的号把交付水位推过这个洞，host 按
该水位删掉回放副本，FE 再到 hub 环里补也补不回来。结束会话则水位保持原值，
host 重连后从该点补发，回放环里的副本仍在。WS 侧的关闭原因是
`uplink unreadable`（1011），QUIC 侧是应用错误码 9（见 §5 的码表）。

## 1. 协商

1. host 发出压缩提议：
   - QUIC：auth 帧增加字段 `"deflate":true`
     （`{"v":1,"type":"auth","token":"…","deflate":true}`）。
   - WebSocket：`GET /ws/host` 握手请求头 `X-Hub-Deflate: 1` 与
     `X-Hub-State-Ack: 1`（两者都是必填；hub 缺少任一项即按协议版本拒绝）。
2. hub 在 hello 帧中回声确认：`{"v":1,"type":"hello",…,"deflate":true}`。
   - 回声为 true ⇒ 本次会话内 host 对 **events 帧与 respond 帧**的载荷启用
     flate 压缩（见 §2 wire format）。
   - 字段缺失或为 false ⇒ host 判定 hub 版本过旧，直接结束会话并重连。
3. 压缩开关是每会话状态：host 在每次重连后清除，必须重新协商。

## 2. wire format（字节级）

压缩采用 RFC 1951 raw deflate（Go `compress/flate`，DefaultCompression；与 hub→FE
路径一致），**不带 zlib/gzip 头**。

### 2.1 QUIC（4 字节大端长度前缀帧）

```
bit  31        bits 30..0
┌─────────┬──────────────────┐
│ deflate │   payload 长度    │
└─────────┴──────────────────┘
```

- 长度前缀的最高位（`0x80000000`）为压缩标志：置 1 ⇒ 帧载荷为 raw deflate 流；
  置 0 ⇒ 裸 JSON（现状）。长度字段取**压缩后**字节数。帧上限 32MB（`n &^
  0x80000000 > 32<<20` 即拒），标志位不占用长度空间。

### 2.2 WebSocket

- 裸帧保持现状：**text** 消息，UTF-8 JSON。
- 压缩帧：**binary** 消息，载荷**整体**为 raw deflate 流（无魔数/前缀）。
  hub 按 message type 区分，无需逐帧试探。

## 3. 何时压缩

- 载荷（压缩前 JSON）`< 256` 字节不压缩（`minCompressSize`，与 hub→FE 一致）。
- 压缩后 ≥ 原始大小则放弃压缩、按裸帧发送。
- auth / ping / pong / host_status / seq_reset 等控制帧本就小于阈值，天然不压缩；
  即便被压缩，上述 wire format 也是自描述的，hub 无需知道帧类型即可解压。
- 下行（hub→host）不压缩；hub→FE 的压缩由 hub 侧另行实施。

## 4. 实现要点（host 侧）

- writer / buffer 来自 `sync.Pool`（`flateWriterPool` / `flateBufPool`）。
- 压缩在会话写闭包内做（QUIC 置标志位 / WS 改 binary），队列中始终是未压缩
  JSON，便于重放/补发逻辑复用。

## 5. 关闭原因（两条传输同一张表）

WS 用 `(code, reason)`，QUIC 没有 reason 通道，用 `CloseWithError` 的 application
error code 承载同一个判定。host 的 `classifyClose` 两者都认，语义必须一一对应：

| 语义 | WS code / reason | QUIC code |
| --- | --- | --- |
| 无关本次会话 | — | 0 |
| 内部错误 | 1011 `internal error` | 1 |
| 被新连接取代 | 1000 `superseded` | 2 |
| 凭据失效 | 1008 `auth` | 3 |
| 协议版本不符 | 1002 `protocol version` | 4 |
| 探针超时 | 1008 `liveness probe timeout` | 5 |
| 上行空闲超时 | 1001 `uplink idle timeout` | 6 |
| ping 写失败 | 1001 `ping write failed` | 7 |
| hub 关闭 | 1000 `shutdown` | 8 |
| 帧无法解码 | 1011 `uplink unreadable` | 9 |

`superseded` 的语义是「立即重拨、不重新配对」：证书是好的，重配对会删掉一个
仍然有效的 token。两条传输都必须给出这个判定，否则同一份 token 上的两个进程
会以退避上限（10s）为周期来回抢同一条会话。
