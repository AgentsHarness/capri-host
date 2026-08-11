# acp-host 启动与部署教程

`acp-host` 启动后**一个进程、一个端口同时提供两样东西**：

1. **ACP 后端 API**（`/api/*`、`/events` SSE 事件流）
2. **内置前端**（acp-fe 构建产物嵌入在二进制里，`GET /` 直接返回 Web 界面）

所以部署时不需要 nginx、不需要单独跑静态服务器，把二进制扔到机器上就能用。

```
本地模式：
  浏览器 ──http://host:8765──▶ acp-host（API + 内置前端）──stdio──▶ grok agent

中继模式（多机 / 远程访问）：
  浏览器 ──http://hub:8787──▶ acp-hub ──QUIC(UDP:8788)/WS──▶ acp-host × N ──stdio──▶ grok
```

## 前置条件

| 依赖 | 说明 |
|------|------|
| Go ≥ 1.26 | 构建用（go.mod 要求）；跑现成二进制则不需要 |
| `grok` CLI | 已安装并登录（`grok login`），或设置 `XAI_API_KEY` |
| （中继模式）`acp-hub` | 另一台机器上的 hub 进程（`../acp-hub`） |

## 方式一：本地模式（单机）

```bash
cd acp-host
./start-host.sh
```

构建完成后前台运行，日志直接打终端，Ctrl+C 优雅退出。然后浏览器打开：

```
http://localhost:8765
```

就是完整的 acp-fe 界面，不需要再单独启动 `npm run dev`。

常用变体：

```bash
./start-host.sh --port 9000 --host-name "我的 Mac"          # 换端口 / 展示名
./start-host.sh --grok-bin /opt/grok/bin/grok               # 指定 grok 路径
./start-host.sh -d                                          # 后台运行
./start-host.sh status                                      # 查看状态
./start-host.sh stop                                        # 停止后台实例
```

`-d` 后台模式：日志写 `bin/acp-host.log`，PID 写 `bin/acp-host.pid`；启动后脚本会自动
等待 `/api/status` 就绪（最多 30s）再提示成功。

手动启动（不依赖脚本）：

```bash
PORT=8765 GROK_BIN=grok go run ./cmd/acp-host
```

## 方式二：中继模式（跨机器访问 / 多 Host）

适合：服务器上跑 hub，家里 / 办公室多台机器各跑一个 host，浏览器访问任意一台的对话。

### 1. 在服务器上启动 hub

```bash
cd acp-hub
FE_TOKEN=换成一段长随机密钥 ./acp-hub        # 生产务必设置 FE_TOKEN
```

- HTTP 默认 `:8787`，QUIC UDP 默认 `:8788`（云安全组放行 UDP 8788 可让 host 走
  更稳的 QUIC；不放行会自动回退 WebSocket，功能不受影响）
- 启动日志会打印 **6 位配对码**（15 分钟有效），也可以随时查看/轮换：

```bash
curl http://hub:8787/api/pairing          # 查看当前配对码
curl -X POST http://hub:8787/api/pairing/rotate   # 轮换（旧的立即失效）
```

### 2. 每台机器启动 host 并配对

```bash
cd acp-host
./start-host.sh --hub http://<hub-ip>:8787 --pair-code XXXXXX --host-name "办公室 Mac"
```

- 配对成功后 token 持久化在 `~/.acp-host/hub.json`，**之后重启不用再带配对码**，
  直接 `./start-host.sh --hub http://<hub-ip>:8787` 即可
- 也可以用已保存的 token：`--token <HOST_TOKEN>`
- 每台机器建议用不同的 `--host-id`（默认 `local`）便于在界面上区分

### 3. 浏览器访问

- **内置前端指向 hub**：浏览器打开 `http://<hub-ip>:8787`，页面门禁输入
  `FE_TOKEN`（存在浏览器 localStorage，不写进构建）
- 或本地开发前端连 hub：

```bash
cd acp-fe
VITE_PROXY_TARGET=http://<hub-ip>:8787 npm run dev
```

hub 会把 `/api/*` 请求中转到目标 host（`?host=<hostId>` 选择，默认最近在线的）。

### 版本契约（FE / hub / host 必须同步）

中继路径的事件模型是跨仓库契约，**不要只升其中一端**：

| 能力 | 说明 |
|------|------|
| `(hostId, seq)` 双路去重 | 本地 SSE 与 hub `/ws/fe` 可能各推一份同源事件；FE 必须按二元组去重 |
| chunk **不再合并** | host 上行保留每条 bridge 事件的独立 `seq` 与原文；旧逻辑把 `a`+`b`+`c` 合成一条会破坏与 SSE 的 seq 对齐 |
| `host_status` 控制帧 | `{"v":1,"type":"host_status","ready":bool}`，**无 `seq`、不在 events 空间**；hub 须识别并不推进 per-host 事件序号 |
| seq 空洞 + gap-pull | 慢消费者丢弃时可能看到 `1,3` 跳号；FE 用 `GET /api/events?host=&after=` 补拉，重复 seq 去重即可 |
| relay 帧带 `hostId` | hub 下行 `request` 帧携带目标 `hostId`；host 端校验与自身 `HOST_ID` 一致才执行，不匹配拒绝（404）——防 hub 路由错误/陈旧转发。FE 侧 `/api/shell` 等独立客户端必须经 transport 带 `?host=`（不能裸 fetch 相对路径） |

**升级 / 回滚**：`acp-fe`、`acp-hub`、`acp-host`（含内嵌 `web/dist`）请升到同一代「双路去重 + 控制帧 + 不合并 chunk」的版本；回滚也三者一起回。

**旧 FE + 新 host/hub 的典型症状**：重复 chunk、host 在线/ready 异常、异常 gap-pull 或事件错位。处理：同步升级，或整体回滚到旧契约版本。

## 环境变量速查

### acp-host

| 变量 | 默认 | 说明 |
|------|------|------|
| `PORT` | `8765` | HTTP 端口（API + 内置前端） |
| `GROK_BIN` | `grok` | grok 可执行文件 |
| `HOST_ID` | `local` | Host 标识（多 Host 区分） |
| `HOST_NAME` | `Local Host` | 界面展示名 |
| `HUB_URL` | — | 设置后进入中继模式，连接 acp-hub |
| `HUB_PAIR_CODE` | — | 一次性配对码（hub 日志 / `GET /api/pairing`） |
| `HOST_TOKEN` | — | 已配对 token，优先于配对码和 `~/.acp-host/hub.json` |
| `XAI_API_KEY` | — | 可选；否则用 `grok login` 缓存 |
| `HUB_QUIC_HOST` | — | 强制 QUIC 拨号地址（域名经代理丢 UDP 时用） |

### acp-hub（中继模式的服务端）

| 变量 | 默认 | 说明 |
|------|------|------|
| `PORT` | `8787` | HTTP 端口 |
| `QUIC_PORT` | `8788` | Host 传输 QUIC UDP 端口 |
| `FE_TOKEN` | — | 前端访问 token，设置后浏览器侧必须携带，否则 401 |

## 日常运维

**更新代码 / 前端产物**：脚本会自动增量构建——源码（`*.go`）或嵌入的前端
（`internal/server/web/dist`）比二进制新时，启动前自动 `go build`。前端产物更新：

```bash
cd acp-fe && npm run build
cp -R dist ../acp-host/internal/server/web/dist
```

**查看日志**：前台模式直接看终端；`-d` 模式看 `bin/acp-host.log`。

**停止**：前台 Ctrl+C；后台 `./start-host.sh stop`。

**开机自启（macOS launchd 示例）**：`~/Library/LaunchAgents/com.acp.host.plist`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.acp.host</string>
  <key>ProgramArguments</key>
  <array>
    <string>/Users/benin/ccwork/acp-host/start-host.sh</string>
    <string>-d</string>
    <string>--hub</string><string>http://hub:8787</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/Users/benin/ccwork/acp-host/bin/acp-host.log</string>
  <key>StandardErrorPath</key><string>/Users/benin/ccwork/acp-host/bin/acp-host.log</string>
</dict>
</plist>
```

## 常见问题

| 现象 | 原因 / 处理 |
|------|------------|
| 启动日志一直刷 `配对失败… 重试` | 配对码填错或过期：hub 上 `POST /api/pairing/rotate` 换新码，host 加 `--pair-code` 重启 |
| host 已配对过，重启还要配对 | 检查 `~/.acp-host/hub.json` 是否存在、`HUB_URL` 是否与当初一致（token 绑定 URL） |
| 中继模式浏览器打开没内容 / 事件不更新 | 无浏览器订阅时 host 暂停向 Hub **实时入队**事件（仍可缓冲供 resume；`host_status` 控制帧不停），打开页面后自动恢复 live；不行就刷新页面重新水合，或依赖 hub `hello.seq` / `GET /api/events` 补拉 |
| 重复 chunk / ready 状态乱 / 事件错位 | FE 与 hub/host **版本未对齐**（见上文「版本契约」）：旧 FE 不懂 `(hostId,seq)` 去重或无 seq 的 `host_status`；同步升级或整体回滚 |
| seq 跳号（如 1→3）后短暂缺口 | 慢订阅缓冲满时 drop 属预期；FE 应 gap-pull，不是 host bug |
| QUIC 连不上 | UDP 8788 被防火墙挡：自动回退 WebSocket（`已连接 hub（ws）`），或在云安全组放行 UDP |
| 前端白屏 / 旧版本 | `internal/server/web/dist` 落后于 acp-fe 最新构建，按上文重新复制产物并重启；中继模式还需 FE 支持双路去重 |
| 端口被占用 | `lsof -nP -iTCP:8765 -sTCP:LISTEN` 查占用；换 `--port` |
| `grok` 找不到 | 装好 grok 并 `grok login`，或用 `--grok-bin` 指定路径；服务本身能起，首次提问前解决即可 |
| 浏览器提示需要 token（中继模式） | hub 设置了 `FE_TOKEN`：在页面门禁输入同一个密钥 |
