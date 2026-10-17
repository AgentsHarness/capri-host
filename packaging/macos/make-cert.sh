#!/usr/bin/env bash
# 创建本地自签名代码签名证书 "Capri Dev"（仅自用；正式分发请改用 Developer ID）。
#
# 为什么要它：TCC 按「设计要求」匹配已授权的权限。ad-hoc 签名的设计要求是裸 cdhash，
# 每次重建都变，已授权的屏幕录制等权限随即失配，表现为反复弹权限提示。证书签名的
# 设计要求是 identifier + 证书指纹，跨重建恒定。
# 与 aipaste 的 "AIPaste Dev" 是同一套办法（见 ~/ccwork/aipaste/tools/make-cert.sh）。
#
# 用法：
#   bash packaging/macos/make-cert.sh            # 装进登录钥匙串并设为信任
#   bash packaging/macos/make-cert.sh --p12      # 只导出 .p12（给 CI 用），不动钥匙串
#
# 身份名已存在时默认直接沿用（导出 `.p12` 也是从钥匙串里那份导出），因为重新生成
# 会得到新的密钥对——指纹一变，已授权的 TCC 权限全部失配，之前签的包也对不上。
# 确实要重建就加 --force。
#
# 注意：自签证书不改变 Gatekeeper 结论，发布给他人的包仍会被拦。
set -euo pipefail

CN="Capri Dev"
P12_PASSWORD="${CAPRI_CERT_PASSWORD:-capri-dev}"
P12_OUT="${CAPRI_CERT_P12_OUT:-$PWD/dist/Capri-Dev.p12}"
FORCE=0
for arg in "$@"; do
  [[ "$arg" == "--force" ]] && FORCE=1
done
KEYCHAIN="$HOME/Library/Keychains/login.keychain-db"
EXISTS=0
security find-certificate -c "$CN" -Z "$KEYCHAIN" >/dev/null 2>&1 && EXISTS=1

export_p12() {
  mkdir -p "$(dirname "$P12_OUT")"
  chmod 600 "$P12_OUT"
  echo "已生成 $P12_OUT（口令 $P12_PASSWORD）"
}

if [[ "$EXISTS" == 1 && "$FORCE" == 0 ]]; then
  # 沿用已有身份。注意 `security export -t identities` 会导出钥匙串里**全部**身份
  # （实测把 AIPaste Dev 一起打包了），所以这里不做自动导出：.p12 是创建时生成的那份，
  # 丢了就只能从「钥匙串访问」里手工导出这一条。
  echo "身份 \"$CN\" 已存在（加 --force 才会重建；重建会换密钥，已授权的权限全部失配）。"
  if [[ -f "$P12_OUT" ]]; then
    echo "沿用已有 $P12_OUT"
    export_p12
  else
    cat >&2 <<'EOF'
没有现成的 .p12。请手工导出这一条身份：
  钥匙串访问 → 我的证书 → "Capri Dev"（含私钥）→ 右键「导出…」→ p12
再放到 dist/Capri-Dev.p12，或用 CAPRI_CERT_P12_OUT 指定路径。
EOF
    exit 1
  fi
  security find-identity -v -p codesigning | grep "$CN" || true
  exit 0
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

openssl req -x509 -newkey rsa:2048 -keyout "$TMP/dev.key" -out "$TMP/dev.crt" -days 3650 -nodes \
  -subj "/CN=$CN" \
  -addext "keyUsage=digitalSignature,keyEncipherment" \
  -addext "extendedKeyUsage=codeSigning" 2>/dev/null

# 固定友好名，否则 openssl 生成的 p12 在 Keychain 里显示成随机 UUID，
# find-identity 认不出 "Capri Dev" 这个名字。
openssl pkcs12 -export -inkey "$TMP/dev.key" -in "$TMP/dev.crt" -out "$TMP/dev.p12" \
  -name "$CN" -passout "pass:$P12_PASSWORD" 2>/dev/null

mkdir -p "$(dirname "$P12_OUT")"
cp "$TMP/dev.p12" "$P12_OUT"

if [[ "${1:-}" == "--p12" ]]; then
  export_p12
  exit 0
fi

security import "$TMP/dev.p12" -k "$KEYCHAIN" -P "$P12_PASSWORD" -T /usr/bin/codesign
# 设为信任根，否则 find-identity -v 不列它（仍能签名，但脚本挑身份时就看不见了）。
security add-trusted-cert -r trustRoot -k "$KEYCHAIN" "$TMP/dev.crt"

echo "已安装，证书导出在 $P12_OUT"
security find-identity -v -p codesigning | grep "$CN"
