<p align="center">
  <img src="docs/brand/banner.png" alt="Capri" width="920" />
</p>

<p align="center">
  <img src="docs/brand/capri.png" alt="Capri mark" width="88" />
</p>

<h1 align="center">Capri Host</h1>

<p align="center">
  <strong>跑在本机的 Agent 节点</strong><br />
  <em>Capricorn · AgentsHarness 的第一颗星座</em>
</p>

<p align="center">
  <a href="https://github.com/AgentsHarness/capri-host/releases"><img src="https://img.shields.io/github/v/release/AgentsHarness/capri-host?style=flat-square&color=002255" alt="release" /></a>
  <a href="https://github.com/AgentsHarness"><img src="https://img.shields.io/badge/AgentsHarness-vision-002255?style=flat-square" alt="AgentsHarness" /></a>
  <img src="https://img.shields.io/badge/for-Grok%20Build-0c0c0e?style=flat-square" alt="Grok Build" />
  <img src="https://img.shields.io/badge/license-MIT-0c0c0e?style=flat-square" alt="MIT" />
</p>

---

[AgentsHarness](https://github.com/AgentsHarness) 想让你在任何时间、任何设备上，掌控任何设备上的 Agents。这件事叫 **slogin**。

**Capri**（Capricorn）是现在的落地。`capri-host` 跑在有 [Grok Build](https://x.ai/cli) 的那台机器上：拉起 `grok`，把会话交给浏览器，或交给 [capri-hub](https://github.com/AgentsHarness/capri-hub) 让你从别处连进来。

一个进程、一个端口，同时提供 **Web 界面** 和接口。不用 nginx，不用再起前端。

```
浏览器  ──本机──▶  capri-host :8765  ──▶  grok
浏览器  ──远程──▶  capri-hub        ──▶  capri-host × N  ──▶  grok
```

读盘、写盘、跑命令都由 grok 自己在这台机器上完成。Host 只负责把 Agent 扶起来、把对话转出去。

## 本机三分钟

需要：已安装并登录的 [`grok`](https://x.ai/cli)（或设置 `XAI_API_KEY`）。从源码构建再加 Go 1.26+。

**最快：下二进制。**

从 [Releases](https://github.com/AgentsHarness/capri-host/releases) 选你的平台，然后：

```bash
chmod +x acp-host-darwin-arm64   # 按实际文件名
./acp-host-darwin-arm64
```

**或者从源码：**

```bash
git clone https://github.com/AgentsHarness/capri-host.git
cd capri-host
go run ./cmd/acp-host
```

浏览器打开 <http://localhost:8765>。

```bash
PORT=9000 HOST_NAME="我的 Mac" go run ./cmd/acp-host
```

## 从别的设备连进来

在一台能被访问的机器上先起 [capri-hub](https://github.com/AgentsHarness/capri-hub)，记下日志里的 6 位配对码。然后在这台有 grok 的机器上：

```bash
HUB_URL=http://<hub>:8787 HUB_PAIR_CODE=XXXXXX HOST_NAME="家里的 Mac" \
  go run ./cmd/acp-host
```

配对成功后 token 写在 `~/.acp-host/hub.json`，之后只需带 `HUB_URL` 重启。浏览器打开 Hub 的地址，选这台 Host 即可。

更完整的部署说明（后台、开机自启、防火墙）见 [docs/DEPLOY.md](docs/DEPLOY.md)。

## 常用变量

| 变量 | 默认 | 说明 |
|------|------|------|
| `PORT` | `8765` | HTTP 端口（界面 + 接口） |
| `GROK_BIN` | `grok` | grok 可执行文件 |
| `HOST_ID` | `local` | 多机时用来区分 |
| `HOST_NAME` | `Local Host` | 界面上的名字 |
| `XAI_API_KEY` | — | 可选；否则用 `grok login` |
| `HUB_URL` | — | 设置后连上 Hub |
| `HUB_PAIR_CODE` | — | 一次性配对码 |

## 一家子

| | |
|---|---|
| [AgentsHarness](https://github.com/AgentsHarness) | 愿景：远程接入，互相调用 |
| [capri-hub](https://github.com/AgentsHarness/capri-hub) | 中继，把多台 Host 收拢到一处 |
| [capri-fe](https://github.com/AgentsHarness/capri-fe) | 浏览器操作台（已嵌在本仓库） |

MIT · [Linux.do](https://linux.do)
