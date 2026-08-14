<p align="center">
  <img src="docs/brand/capri.png" alt="Capri mark" width="88" />
</p>

<h1 align="center">Capri Host</h1>

<p align="center">
  <strong>连接你的 Grok Agent</strong><br />
  <em>Capricorn · AgentsHarness 的第一颗星座</em>
</p>

<p align="center">
  <a href="https://github.com/AgentsHarness/capri-host/releases"><img src="https://img.shields.io/github/v/release/AgentsHarness/capri-host?style=flat-square&color=002255" alt="release" /></a>
  <a href="https://github.com/AgentsHarness"><img src="https://img.shields.io/badge/AgentsHarness-vision-002255?style=flat-square" alt="AgentsHarness" /></a>
  <img src="https://img.shields.io/badge/for-Grok%20Build-0c0c0e?style=flat-square" alt="Grok Build" />
  <img src="https://img.shields.io/badge/license-MIT-0c0c0e?style=flat-square" alt="MIT" />
</p>

---

[AgentsHarness](https://github.com/AgentsHarness) 让你随时随地远程使用 Agents。

**Capri**（Capricorn）是 [Grok Build](https://x.ai/cli) 的具体适配项目，我们基于 ACP 协议，搭配 capri-fe、capri-hub 实现远程 Agent 控制。

一个进程、一个端口，同时提供 **Web 界面** 和接口。

```
浏览器  ──本机──▶  capri-host :8765  ──▶  grok
浏览器  ──远程──▶  capri-hub        ──▶  capri-host × N  ──▶  grok
```

## 快速开始

1、安装并登录 [`Grok Build`](https://x.ai/cli)（或设置 `XAI_API_KEY`）。

2、从 [Releases](https://github.com/AgentsHarness/capri-host/releases) 选你的平台，然后：

```bash
chmod +x capri-host   # 按实际文件名
# 参考环境变量进行自定义设置
./capri-host
```

**或者从源码构建：**

```bash
git clone https://github.com/AgentsHarness/capri-host.git
cd capri-host
go run ./cmd/capri-host
```

浏览器打开 <http://localhost:8765>。

## 连接到 capri-hub

在一台能被访问的服务器上先起 [capri-hub](https://github.com/AgentsHarness/capri-hub)，并部署 capri-fe，通过前端左上角添加 Host 获得配对码，然后在部署 capri-host 的机器上提供环境变量：

```bash
# 自行修改
HUB_URL=http://<hub>:8787
HUB_PAIR_CODE=XXXXXX
HOST_ID=pc
HOST_NAME="家里的 Mac"
FE_TOKEN=XXXXXX
nohup ./capri-host >> capri-host.log 2>&1 & echo $! > capri-host.pid
```

配对成功后 token 写在 `~/.capri-host/hub.json`，之后只需带 `HUB_URL`、`FE_TOKEN` 重启。浏览器打开独立部署的前端地址，选这台 Host 即可。

更完整的部署说明（后台、开机自启、防火墙）见 [docs/DEPLOY.md](docs/DEPLOY.md)。

## 常用变量

| 变量            | 默认         | 说明                      |
| --------------- | ------------ | ------------------------- |
| `PORT`          | `8765`       | HTTP 端口（界面 + 接口）  |
| `GROK_BIN`      | `grok`       | grok 可执行文件           |
| `HOST_ID`       | `local`      | 多机时用来区分            |
| `HOST_NAME`     | `Local Host` | 界面上的名字              |
| `XAI_API_KEY`   | —            | 可选；否则用 `grok login` |
| `HUB_URL`       | —            | 设置后连上 Hub            |
| `HUB_PAIR_CODE` | —            | 一次性配对码              |
| `FE_TOKEN`      | —            | 入口鉴权                  |

## 项目生态

|                                 项目                        |          简介                       |
| ------------------------------------------------------- | ------------------------------- |
| [AgentsHarness](https://github.com/AgentsHarness)       | 总项目                          |
| [capri-hub](https://github.com/AgentsHarness/capri-hub) | 中继节点，转发用户和 Agent 消息 |
| [capri-fe](https://github.com/AgentsHarness/capri-fe)   | WebUI                           |

## 友情链接

[Linux.do](https://linux.do)

MIT
