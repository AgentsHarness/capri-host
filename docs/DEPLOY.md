# Capri Host 部署

`capri-host` 启动后**一个进程、一个端口同时提供**：

1. Web 界面（内嵌的 capri-fe，打开 `GET /` 即可）
2. 后端接口（`/api/*`、`/events`）

不用 nginx，不用单独跑静态服务器。

```
本机：
  浏览器 ──http://host:8765──▶ capri-host ──▶ grok

远程 / 多机：
  浏览器 ──http://hub:8787──▶ capri-hub ──▶ capri-host × N ──▶ grok
```

只想看三句话上手，回 [README](../README.md)。下面是部署时会用到的细节。

## 前置

| 依赖 | 说明 |
|------|------|
| `grok` CLI | 已安装并登录（`grok login`），或设置 `XAI_API_KEY`。见 [x.ai/cli](https://x.ai/cli) |
| Go ≥ 1.26 | 仅从源码构建需要；跑 [Release 二进制](https://github.com/AgentsHarness/capri-host/releases) 不需要 |
| capri-hub | 仅远程 / 多机需要 |

## 本机

```bash
# 二进制（从 Releases 下载后）
./capri-host-darwin-arm64

# 或源码
go run ./cmd/capri-host
```

打开 <http://localhost:8765>。

```bash
PORT=9000 HOST_NAME="我的 Mac" GROK_BIN=/opt/grok/bin/grok go run ./cmd/capri-host
```

后台跑：

```bash
nohup go run ./cmd/capri-host > capri-host.log 2>&1 &
```

## 远程 / 多机

适合：一台机器跑 Hub，家里 / 办公室各跑一个 Host，浏览器从任何地方选机器。

### 1. 起 Hub

```bash
cd capri-hub
FE_TOKEN=$(openssl rand -hex 24) go run ./cmd/capri-hub
```

- HTTP `:8787`，QUIC UDP `:8788`（安全组放行 UDP 更稳；不放行自动回退 WebSocket）
- 日志里有 6 位配对码（15 分钟有效）

```bash
curl http://<hub>:8787/api/pairing
curl -X POST http://<hub>:8787/api/pairing/rotate
```

### 2. 每台机器配对 Host

```bash
HUB_URL=http://<hub>:8787 HUB_PAIR_CODE=XXXXXX \
HOST_ID=macbook HOST_NAME="办公室 Mac" \
  go run ./cmd/capri-host
```

配对成功后 token 在 `~/.capri-host/hub.json`，之后只需带 `HUB_URL`。每台机器用不同的 `HOST_ID`。

### 3. 打开浏览器

打开 `http://<hub>:8787`，门禁输入 `FE_TOKEN`。密钥存在你的浏览器里，不要写进前端构建。

**FE / Hub / Host 请一起升级。** 三端事件契约是一套的，只升一端会出现重复输出或状态错乱。

## 环境变量

### capri-host

| 变量 | 默认 | 说明 |
|------|------|------|
| `PORT` | `8765` | HTTP 端口（界面 + 接口） |
| `GROK_BIN` | `grok` | grok 可执行文件 |
| `HOST_ID` | `local` | 多 Host 区分 |
| `HOST_NAME` | `Local Host` | 界面展示名 |
| `FE_TOKEN` | — | 本机接口访问密钥（`/api/*`、`/events`）。配了之后浏览器首次打开需要输入；与 Hub 的 `FE_TOKEN` 同语义，部署时建议配同一个值 |
| `HUB_URL` | — | 设置后进入中继模式 |
| `HUB_PAIR_CODE` | — | 一次性配对码 |
| `HOST_TOKEN` | — | 已配对 token，优先于配对码 |
| `XAI_API_KEY` | — | 可选；否则用 `grok login` |
| `HUB_QUIC_HOST` | — | 强制 QUIC 拨号地址（域名经代理丢 UDP 时用） |
| `HUB_QUIC_INSECURE` | — | 设为 `1` 跳过 QUIC 证书校验（仅限可信网络上的自签 hub；生产用 `HUB_QUIC_PIN` 代替） |
| `HUB_QUIC_PIN` | — | 自签 hub 证书的 SPKI 指纹（sha256，hex 或 base64）。设置后跳过系统 CA，改为比对证书公钥指纹，不匹配即握手失败。见下方[自签证书指纹校验](#自签证书指纹校验hub_quic_pin) |

### capri-hub

| 变量 | 默认 | 说明 |
|------|------|------|
| `PORT` | `8787` | HTTP 端口 |
| `QUIC_PORT` | `8788` | Host 传输 QUIC UDP 端口 |
| `QUIC_CERT` / `QUIC_KEY` | — | QUIC 传输的 TLS 证书/私钥文件（PEM） |
| `FE_TOKEN` | — | 浏览器访问密钥，生产必设 |
| `REQUIRE_FE_TOKEN` | — | 设为 `1` 时没配 `FE_TOKEN` 会拒绝启动 |

## 自签证书指纹校验（HUB_QUIC_PIN）

自签证书的 hub 没有系统 CA 可验，`HUB_QUIC_INSECURE=1` 又等于信任该 UDP 端口上的任何应答者（能收走 host token 并驱动中继请求在本机执行）。`HUB_QUIC_PIN` 是两者的替代：host 跳过 CA 链，改为校验 hub 证书**公钥**（SPKI，SubjectPublicKeyInfo）的 sha256 指纹完全一致——hub 换证书/私钥后指纹即失效（这是 pin 的本意），需要在 hub 侧重新生成并更新配置。

hub 侧用自签证书启动（`QUIC_CERT`/`QUIC_KEY`），host 侧取证书文件算一次指纹：

```bash
openssl x509 -in hub-cert.pem -pubkey -noout \
  | openssl pkey -pubin -outform DER \
  | openssl dgst -sha256 | awk '{print $NF}'
```

输出 64 位 hex（如 `dfd749…7c91`），配置：

```bash
HUB_URL=https://hub.example.com HUB_QUIC_PIN=dfd749…7c91 \
  ./capri-host
```

指纹也接受 base64（std/URL、带不带 padding 均可）和 `sha256//` 前缀写法。配置格式错误时 QUIC 握手直接失败并回退 WebSocket（错误原因见日志），不会静默降级为不校验。证书或私钥更换后需同步更新 pin。

## 更新内嵌前端

Host 二进制里嵌着 capri-fe 的构建产物。要换新界面：

```bash
cd ../capri-fe && npm run build
cp -R dist ../capri-host/internal/server/web/dist
```

然后重新编译 / 重启 Host。

## 开机自启

macOS `~/Library/LaunchAgents/com.capri.host.plist`：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.capri.host</string>
  <key>ProgramArguments</key>
  <array>
    <string>/绝对路径/capri-host</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>HOST_NAME</key><string>我的 Mac</string>
    <key>HUB_URL</key><string>http://hub:8787</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/绝对路径/capri-host.log</string>
  <key>StandardErrorPath</key><string>/绝对路径/capri-host.log</string>
</dict>
</plist>
```

Linux systemd `/etc/systemd/system/capri-host.service`：

```ini
[Unit]
Description=capri-host
After=network-online.target

[Service]
ExecStart=/绝对路径/capri-host
Restart=always
Environment=HOST_NAME=我的服务器

[Install]
WantedBy=multi-user.target
```

## 常见问题

| 现象 | 处理 |
|------|------|
| 配对一直失败 | 码填错或过期：Hub 上 `POST /api/pairing/rotate`，Host 带新码重启 |
| 重启还要配对 | 看 `~/.capri-host/hub.json` 是否在、`HUB_URL` 是否和当初一致 |
| 中继模式打开没内容 | 没有浏览器订阅时 Host 会暂停实时上报（省流量），打开页面后会恢复；不行就刷新 |
| 重复输出 / 状态乱 | FE、Hub、Host 版本没对齐，三者一起升或一起回 |
| QUIC 连不上 | UDP 8788 被挡，会自动走 WebSocket，功能不受影响 |
| 端口被占 | `lsof -nP -iTCP:8765 -sTCP:LISTEN`，或换 `PORT` |
| 找不到 `grok` | 先 `grok login`，或设 `GROK_BIN` |
| 浏览器要 token | Hub 或 Host 设了 `FE_TOKEN`，在页面门禁输入同一个密钥 |
| 前端白屏 / 旧界面 | 按上文重新拷贝 capri-fe 构建产物并重启 Host |
