# 事件契约：seq / 双路去重 / 分级背压

状态：**双侧已实现**（capri-host `internal/acp/event_bus.go` + `internal/hub/client.go`、
capri-fe 事件门、acp-hub `internal/hub/` 消费侧）。本文件是
`internal/hub/PROTOCOL.md`（deflate 传输帧格式）的姊妹篇：那篇约定
**字节怎么走**，这篇约定 **事件语义怎么算**。hub 侧与前端各自消费本文
的不变量；修改任何一条都必须同步本文与两侧实现。

## 1. 事件 seq 的分配（不变量）

1. host 内只有一个 seq 空间。`eventBus.publish`（经 `Bridge.Broadcast`）
   在持锁状态下递增 `seq` 并写入事件顶层字段 `"seq"`（uint64，从 1 开始）。
2. **所有订阅者看到同一事件携带同一 seq**：本地 SSE 与 hub-client 转发
   不各自编号。hub-client 转发必须原样保留该 seq（`forwardLoop` /
   `seqAndReplay`），否则第 2 节的双路去重失效。
3. seq 严格单调递增，无空洞豁免：可丢事件被丢弃时 seq 照常前进
   （见第 3 节），落后方靠 gap-pull 补洞，**不允许**为填洞重发旧 seq。
4. `enqueueMu` 保证回放帧与直播帧按 seq 严格递增入队：重连后回放
   （低 seq）与直播（高 seq）交错入队会让 hub 的 stale-seq gate 丢弃
   全部后续回放事件，且这些事件不在 hub 的 gap-pull 缓冲里——transcript
   永久丢失。

## 2. 双路去重（前端）

前端可能同时从两条路径收到同一事件：

- 本机模式：`GET /events`（SSE）；
- hub 模式：hub 的 `/ws/fe` 推回。

去重键为 **`(hostId, seq)`**：两路同 host 同 seq 视为同一事件，先到先
渲染。因此：

- host 必须在 hello 与每个事件上都携带 `hostId`；
- hub 转发事件时不得改写 `seq` 与 `hostId`。

## 3. 事件分级与背压

| 级别 | 事件类型 | 慢消费者满缓冲时 |
| --- | --- | --- |
| 可丢（droppable） | `chunk` `user_chunk` `thought` `gen_rate` `log` `session_updates_chunk` `search_fuzzy_status` `search_content_status` | 直接丢弃，seq 照常前进；FE gap-pull 兜底 |
| 关键（critical） | 其余全部（`done`/`turn_completed` 终态、`error`、`client_request`、roster/权限变更等） | 有界阻塞（5s）等待；超时兜底丢弃并告警 |

不允许为填洞合并 chunk——会破坏与 hub 上行一致的双路 seq。

## 4. 断线恢复（host ↔ hub 上行）

- hub-client 维护有界回放环 `replay`；`lastSentSeq` 证明 **入队**，
  `hubAckSeq`（hub 经 hello.seq / ping 附带 seq 回执）证明 **送达**。
- 重连后 hub 在 hello 里带 `hello.seq`（其最后见过的 seq）；host 从该点
  之后重放缓冲事件（`sendReplayAfter`）。
- 订阅代际：hub 推 `{type:"subscribers", count, gen}`，host 按 gen 单调
  应用，防止"刷新页面先 0 后 1"被迟到的 0 暂停上行；0→1 转换触发补发。

## 5. 进程重启与 seq 归零（滚动升级）

host 进程重启后 seq 从 1 重新计数，而 hub 可能仍持有上一进程的高水位。
约定：

- host 每次会话建立时把本进程观测的最大 seq 记入 `nextSeq`；hub 的
  `hello.seq` **大于** 本进程已产出的最大 seq 视为陈旧（上一进程的），
  不得记录为 ack，也不得据此回放；
- `hello.seq == 0` 或缺失：hub 太旧，按"无回放"处理（gen==0 同理）；
- agent 进程（非 host 进程）重启用 `agentStartedAt`（unix ms）检测，
  与事件 seq 无关：前端比对 hello 里的 `agentStartedAt` 变化即重置权限
  徽标等内存态。

## 6. hello / 快照语义

- SSE `hello.busy` 只反映 hello 中 `sessionId`（被聚焦会话）的忙闲，
  **不是** 所有会话的 OR 聚合——后台会话的 done 会被前端按 sessionId
  过滤掉，聚合 busy 会把前台视图永久钉死。聚合值保留在 `GET /api/status`
  的 `Status.Busy`。
- `hello.permissionMode` 是 host 进程对 agent 权限模式的权威视图，前端
  连接时以它恢复徽标（不用浏览器存储）。
- roster：host 侧每个存活会话一行（busy / awaitingInput 分类），由
  `sessions_changed` 事件维护。
- `busy` 是两条腿的投影：本 host 进程发出、尚未应答的 `session/prompt`
  计数，加上从 agent 事件流观察到的回合（首个执行类 `session/update`
  张开，`turn_completed` / `response_completed` 收口）。第一条腿在传输
  失败时（长回合撞上 30 分钟 promptTimeout 是常见情形）会先归零，此时
  第二条腿继续如实上报运行中；观察腿超过 30 分钟无任何 update 则按过期
  处理（等待用户输入的除外），避免丢掉的终态事件把会话永久钉在 running。
  只有执行类 kind 算证据：标题 / 模式 / 配置 / 命令表 / 模型切换等会话元
  数据，以及 `usage_update`、后台任务与 monitor 轨，都不张开回合。
- 回合错误的上报口径（`reportPromptFailure`）：agent 用 JSON-RPC error 拒绝
  回合、以及 host 因浏览器断连主动取消，一律上事件流；纯传输失败要先看
  观察腿——回合仍在输出（最近 `turnLivenessWindow` 内有 update）时，
  promptTimeout 耗尽只是 host 侧的 RPC 预算到期，不是回合失败，只留日志不
  发 `error` 事件。否则前端会收口正在流式的回答、往 scrollback 塞一条带
  「重启 agent」按钮的错误行，而重启会真的杀掉那个健康在跑的回合。

## 7. 修改清单（改契约时勾对）

- [ ] `internal/acp/event_bus.go`（seq 分配、分级表）
- [ ] `internal/hub/client.go`（seq 保留、回放、gen、hubAck）
- [ ] capri-fe 事件门（去重键、gap-pull）
- [ ] acp-hub `internal/hub/`（stale-seq gate、subscribers gen）
- [ ] 本文
