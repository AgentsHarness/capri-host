# macOS 打包

`make-app.sh` 组装 `Capri.app`（SwiftUI 菜单栏 + 内嵌 capri-host），`make-dmg.sh` 把它打成 dmg。

```bash
cd ../..                                    # 仓库根
rm -rf internal/server/web/dist             # 内嵌前端必须先就位
cp -R ../acp-fe/dist internal/server/web/dist
OUT=/tmp/capri-build/Capri.app VERSION=dev bash packaging/macos/make-app.sh
```

需要 macOS 13+ 的 SDK 与 `xcrun swiftc`。输出恒为 **arm64**：双架构（universal）会让
app 从 18 MB 涨到 35 MB，而每个 slice 各带一份内嵌前端（go:embed 不压缩）。

## 签名

脚本按以下顺序挑选签名身份：

1. `CAPRI_CODESIGN_IDENTITY`（名字或 sha1）
2. 名为 `Capri Dev` 的身份（`make-cert.sh` 装的就是它）
3. Keychain 里**唯一**一个可用 codesigning 身份
4. 都没有 → 退回 ad-hoc，并打印一段警告

这一步不是门面功夫，它决定系统权限会不会在每次重建后失配。

系统用「设计要求」（designated requirement）匹配已授权的权限。两种签名方式的要求不同：

| 签名方式 | 设计要求 | 重建后 |
|----------|----------|--------|
| ad-hoc | `cdhash H"..."` | cdhash 变 → **权限失配** |
| 证书（含自签） | `identifier "com.agentsharness.capri" and certificate leaf = H"..."` | **恒定** |

失配的实际表现（agent 调 `screencapture` 截图时最常见）：已经给过「屏幕录制」权限，
却仍然弹提示，且截图仍然失败。tccd 日志里的判据是：

```
Failed to match existing code requirement for subject com.agentsharness.capri
  and service kTCCServiceScreenCapture
    cdhash H"...（库里存的）..."
    cdhash H"...（正在跑的）..."
Service kTCCServiceScreenCapture does not allow prompting; returning denied.
```

屏幕录制**不允许** tccd 主动弹窗，所以用户看到的是提示加失败，而不是正常的授权对话框。

**换了签名身份后必须重置一次权限**，否则 TCC 里那条指向旧身份的记录仍会一直不匹配：

```bash
tccutil reset ScreenCapture com.agentsharness.capri
# 然后到「系统设置 → 隐私与安全性 → 屏幕录制」重新勾选 Capri
```

查看当前 app 的身份与要求：

```bash
codesign -dvvv <app>/Contents/MacOS/Capri | grep -E 'Identifier|CDHash|Signature'
codesign -d -r- <app> | tail -1     # 裸 cdhash = ad-hoc
```

### 生成一个自签身份（本机自用）

```bash
bash packaging/macos/make-cert.sh          # 装进登录钥匙串并设为信任，身份名 "Capri Dev"
bash packaging/macos/make-cert.sh --p12    # 只导出 dist/Capri-Dev.p12（给 CI 用）
```

默认口令 `capri-dev`，可用 `CAPRI_CERT_PASSWORD` 覆盖。身份已存在时脚本直接沿用，
因为重建会换密钥对——指纹一变，已授权的 TCC 权限全部失配，之前签的包也对不上。
确实要重建加 `--force`。

自签证书足够让设计要求稳定；它不会让 app 通过 Gatekeeper 公证，发布给他人仍会被拦。

## CI（release.yml 的 macos-app job）

未配置密钥时 `import-cert.sh` 直接成功退出，`make-app.sh` 退回 ad-hoc，发版流程不中断——
但这样每个版本的用户升级后都要重新授权一次屏幕录制。要根治，给仓库配三个 secret：

| Secret | 内容 |
|--------|------|
| `MACOS_CERT_P12` | `.p12` 的 base64（`base64 -i dist/Capri-Dev.p12 \| tr -d '\n'`） |
| `MACOS_CERT_PASSWORD` | 该 `.p12` 的导出密码 |
| `MACOS_CODESIGN_IDENTITY` | 证书显示名，即 `Capri Dev` |

推送（仓库需有 admin 权限）：

```bash
base64 -i dist/Capri-Dev.p12 | tr -d '\n' | gh secret set MACOS_CERT_P12 -R AgentsHarness/capri-host
gh secret set MACOS_CERT_PASSWORD   -R AgentsHarness/capri-host --body "capri-dev"
gh secret set MACOS_CODESIGN_IDENTITY -R AgentsHarness/capri-host --body "Capri Dev"
```

`import-cert.sh` 把 p12 导入 runner 的临时钥匙串并放进搜索列表；工作流随后把身份名传给
`make-app.sh`。自签证书**不需要**在 runner 上设信任根就能签名，设计要求也照常是
identifier + 证书指纹，所以脚本不碰信任设置（那一步要 sudo）。

验证 CI 出的包用了同一身份：`codesign -d -r- Capri.app` 里应出现
`certificate leaf = H"dbe51657b516e90c24040b01bf8b64b872135833"`，与本地构建一致。
