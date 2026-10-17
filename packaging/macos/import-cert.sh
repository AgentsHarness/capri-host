#!/usr/bin/env bash
# 把 base64 的 .p12 导入 CI 的临时钥匙串，供 make-app.sh 用证书签名。
#
# 为什么需要证书：TCC 按「设计要求」匹配已授权的权限。ad-hoc 签名的设计要求是
# 裸 cdhash，每次发版都变，用户升级后已授权的屏幕录制等权限立即失配——tccd 记
# “Failed to match existing code requirement”，用户看到的是反复的授权提示。
# 证书签名（含自签）的设计要求是 identifier + 证书指纹，跨版本恒定。
#
# 用法（CI）：
#   CERT_P12=<base64 of .p12> CERT_PASSWORD=<p12 口令> bash packaging/macos/import-cert.sh
# 变量都没设时直接成功退出，让未配置密钥的仓库照常发版（退回 ad-hoc 签名）。
set -euo pipefail

if [[ -z "${CERT_P12:-}" ]]; then
  echo "未配置 MACOS_CERT_P12，跳过导入；make-app.sh 将退回 ad-hoc 签名。"
  exit 0
fi

KEYCHAIN="${RUNNER_TEMP:-/tmp}/capri-signing.keychain-db"
KEYCHAIN_PASSWORD="$(openssl rand -hex 24)"
P12="${RUNNER_TEMP:-/tmp}/capri-cert.p12"

security create-keychain -p "$KEYCHAIN_PASSWORD" "$KEYCHAIN"
security set-keychain-settings -lut 3600 "$KEYCHAIN"
security unlock-keychain -p "$KEYCHAIN_PASSWORD" "$KEYCHAIN"

printf '%s' "$CERT_P12" | base64 --decode > "$P12"
security import "$P12" -k "$KEYCHAIN" -P "${CERT_PASSWORD:-}" -T /usr/bin/codesign
rm -f "$P12"

# 不设信任根：自签证书未受信也能签名，设计要求照样是 identifier + 证书指纹
# （实测）。add-trusted-cert 在这里要走 sudo，CI 上没必要冒这个险。
#
# 把临时钥匙串放进搜索列表，codesign 才找得到导入的身份。用 login 钥匙串之外
# 的完整路径拼接，别 `tr -d '"'`——那会破坏带空格的路径。
security list-keychains -d user -s "$KEYCHAIN" "$HOME/Library/Keychains/login.keychain-db"
security set-key-partition-list -S apple-tool:,apple:,codesign: -s -k "$KEYCHAIN_PASSWORD" "$KEYCHAIN" >/dev/null

echo "已导入签名身份："
security find-identity -v -p codesigning "$KEYCHAIN"
