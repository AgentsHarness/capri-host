<p align="center">
  <img src="docs/brand/banner.png" alt="Capri" />
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

一个进程、一个端口，同时提供 **Web 界面** 和接口。macOS 上还提供一个菜单栏应用，把启停和配置收进图形界面。

```
浏览器  ──本机──▶  Capri-host :8765  ──▶  grok
浏览器  ──远程──▶  capri-hub        ──▶  Capri-host × N  ──▶  grok
```

## 截图

![Capri Host 界面](docs/screenshot.png)

## 快速开始

1、安装并登录 [`Grok Build`](https://x.ai/cli)（或设置 `XAI_API_KEY`）。

2、从 [Releases](https://github.com/AgentsHarness/capri-host/releases) 里选一种装上。

### macOS 应用（推荐，13+）

下载 `Capri-macos.zip`（或 `Capri-macos.dmg`），把 `Capri.app` 拖进「应用程序」后打开。

1、菜单栏会出现摩羯图标。
2、首次使用点击摩羯图标，前往设置，配置好后点击保存并启动。
3、按钮左侧会提示 Host 已在 :8765 启动。
4、点击菜单按钮，点击打开界面即可跳转本地 Web UI。

首次打开若被系统拦下，右键点「打开」，或：

```bash
xattr -dr com.apple.quarantine /Applications/Capri.app
```

### Windows 应用（推荐）

下载 `Capri-windows-amd64.zip`（或 `Capri-windows-arm64.zip`，根据系统架构选择），解压后进入 `Capri`
目录，双击 **`Capri.exe`**。

目录结构：

```
Capri/
├── Capri.exe            双击运行的就是它：托盘图标、对话框、Host 进程的启停
└── bin/
    └── Capri-host.exe   干活的引擎（HTTP + grok），由 Capri.exe 拉起
```

**引擎不要单独双击**——它是个控制台程序，双击只会开一个黑窗口。整个目录一起保留，
`Capri.exe` 按相对位置找 `bin/Capri-host.exe`。

点击托盘菜单的设置进入网页设置界面，配置好即可保存，然后通过托盘菜单启动或重启 Host。
## 命令行二进制

从 Releases 下对应平台的 `Capri-host-*`：

```bash
chmod +x Capri-host   # 按实际文件名
./Capri-host
```

可以只用环境变量配置，适合脚本、launchd、systemd，以及在服务器上跑。

### 从源码构建

```bash
git clone https://github.com/AgentsHarness/capri-host.git
cd capri-host
go run ./cmd/capri-host
```

浏览器打开 <http://localhost:8765>。

要出 Windows 那套目录（`Capri/Capri.exe` + `Capri/bin/Capri-host.exe`，清单按架构生成）：

```bash
./packaging/windows/make-exes.sh amd64 arm64
```

macOS 应用同理：`./packaging/macos/make-app.sh --universal`。

## 连接到 capri-hub

在一台能被访问的服务器上先起 [capri-hub](https://github.com/AgentsHarness/capri-hub)，并部署 capri-fe，通过前端左上角添加 Host 获得配对码。

macOS 应用在设置窗里填 Hub URL 和配对码即可，配对成功后地址会留在 `config.json`。
Windows 托盘在设置界面配置，同样落在 `config.json`。

命令行启动则需要提供环境变量：

```bash
# 自行修改
HUB_URL=http://<hub>:8787
HUB_PAIR_CODE=XXXXXX
HOST_ID=pc
HOST_NAME="家里的 Mac"
FE_TOKEN=XXXXXX
nohup ./Capri-host >> Capri-host.log 2>&1 & echo $! > capri-host.pid
```

配对成功后 token 写在 `~/.capri-host/hub.json`，之后只需带 `HUB_URL`、`FE_TOKEN` 重启。浏览器打开独立部署的前端地址，选这台 Host 即可。

## 配置

两处可配，**环境变量优先于文件**：

1. `~/.capri-host/config.json`——macOS 应用和 Windows 托盘读写的是同一份，也可以手写；
2. 环境变量——命令行、launchd、systemd 用这套，会覆盖文件里的同名字段。

所以装了应用之后依然可以临时 `PORT=9000 ./Capri-host`。

```json
{
  "bind": "127.0.0.1",
  "port": 8765,
  "host_id": "mba",
  "host_name": "MacBook Air",
  "hub_url": "https://agents.example.com",
  "fe_token": "",
  "grok_bin": "",
  "hub_pair_code": "",
  "proxy": "",
  "no_proxy": ""
}
```

测试或便携安装可设 `CAPRI_HOME` 换掉配置目录。

## 常用变量

| 变量            | 默认        | 说明                                                                                                                                 |
| --------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `PORT`          | `8765`      | HTTP 端口（界面 + 接口）                                                                                                             |
| `BIND`          | `127.0.0.1` | 监听地址。默认只听回环，只有本机能够直连；要让手机等同网段设备访问，显式设 `BIND=0.0.0.0`——那**必须**同时设 `FE_TOKEN`，否则拒绝启动 |
| `GROK_BIN`      | `grok`      | grok 可执行文件；留空则按常见路径探测                                                                                                |
| `HOST_ID`       | `local`     | 多机时用来区分                                                                                                                       |
| `HOST_NAME`     | `Local Host`| 界面上的名字                                                                                                                         |
| `XAI_API_KEY`   | —           | 可选；否则用 `grok login`                                                                                                            |
| `HUB_URL`       | —           | 设置后连上 Hub                                                                                                                       |
| `HUB_PAIR_CODE` | —           | 一次性配对码                                                                                                                         |
| `FE_TOKEN`      | —           | 本机接口的访问密钥（`/api/*`、`/events`）。与 Hub 的 `FE_TOKEN` **是两把独立的钥匙**，见下                                            |
| `HUB_QUIC_PIN`  | —           | 自签 hub 的 QUIC 证书指纹（见 `docs/DEPLOY.md`）                                                                                     |
| `PROXY`         | —           | 出网代理，可只写 `host:port`（自动补 `http://`）。留空则 macOS 读取系统网络设置里的代理；系统未开时直连                                 |
| `NO_PROXY`      | —           | 不走代理的地址列表，逗号分隔。留空则沿用系统网络设置里的排除列表                                                                      |
| `CAPRI_HOME`    | `~/.capri-host` | 配置目录（`config.json`、`hub.json` 都在这下面）                                                                                 |

## 两把 `FE_TOKEN`

Hub 和每台 Host 各有一把 `FE_TOKEN`，它们保护的对象不同，**不要求同值**：

- 经 Hub 中继的请求由 Host 进程自己注入凭据，浏览器只出示 Hub 那把；
- 浏览器直连本机端口（`127.0.0.1:8765`，省一跳延迟、Hub 挂了也还能用）时，出示的才是这台 Host 那把。

所以典型配法是：只在 Hub 上设 `FE_TOKEN`，Host 留空——回环 + 无密钥 = 近路是免鉴权的本机请求，
浏览器全程只问一次。给 Host 也设上一把同样可以：页面会先拿 Hub 那把探一次本机
（`GET /api/probe`），两把同值就不再多问，不同值才弹一次「这台 Host 的钥匙」。
Host 的密钥被拒只会让那一台退回 Hub 中继，不影响 Hub 登录。

## 项目生态

|                                 项目                        |          简介                       |
| ------------------------------------------------------- | ------------------------------- |
| [AgentsHarness](https://github.com/AgentsHarness)       | 总项目                          |
| [capri-hub](https://github.com/AgentsHarness/capri-hub) | 中继节点，转发用户和 Agent 消息 |
| [capri-fe](https://github.com/AgentsHarness/capri-fe)   | WebUI                           |

## 友情链接

[Linux.do](https://linux.do)

MIT
