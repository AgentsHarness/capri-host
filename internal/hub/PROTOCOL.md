# host↔hub 上行压缩协议（deflate）

状态：**双侧已实现并对齐**（capri-host `internal/hub/client.go`、acp-hub
`cmd/capri-hub/main.go`）。本文件与 acp-hub `internal/hub/PROTOCOL.md` 保持一致。

## 1. 协商

1. host 发出压缩提议：
   - QUIC：auth 帧增加字段 `"deflate":true`
     （`{"v":1,"type":"auth","token":"…","deflate":true}`）。
   - WebSocket：`GET /ws/host` 握手请求头 `X-Hub-Deflate: 1`（hub 亦接受 `?deflate=1`）。
2. hub 在 hello 帧中回声确认：`{"v":1,"type":"hello",…,"deflate":true}`。
   - 回声为 true ⇒ 本次会话内 host 对 **events 帧与 respond 帧**的载荷启用
     flate 压缩（见 §2 wire format）。
   - 字段缺失或为 false ⇒ 全程裸 JSON（旧 hub 向后兼容）。
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
