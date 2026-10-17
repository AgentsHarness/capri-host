#!/usr/bin/env bash
# 组装 Capri.app：SwiftUI 原生菜单栏 + 内嵌 Capri-host。
# 只出 arm64：双架构（universal）让 app 从 18 MB 涨到 35 MB，而每个 slice
# 各带一份内嵌前端（go:embed 不压缩），重复的部分占了一半以上。
# 用法：./packaging/macos/make-app.sh
# 环境：VERSION（默认 dev）、OUT（默认 dist/Capri.app）
# 需要先有 internal/server/web/dist（go:embed）。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

VERSION="${VERSION:-dev}"
ARCH=arm64

OUT="${OUT:-$ROOT/dist/Capri.app}"
rm -rf "$OUT"
MACOS="$OUT/Contents/MacOS"
RES="$OUT/Contents/Resources"
mkdir -p "$MACOS" "$RES"

if [[ ! -f internal/server/web/dist/index.html ]]; then
  echo "missing internal/server/web/dist/index.html — copy capri-fe dist first" >&2
  exit 1
fi

LDFLAGS="-s -w -X github.com/AgentsHarness/capri-host/internal/acp.Version=${VERSION}"
SDK="$(xcrun --sdk macosx --show-sdk-path)"
MACOSX_DEPLOYMENT_TARGET=13.0
export MACOSX_DEPLOYMENT_TARGET

build_host() {
  local arch=$1 dest=$2
  CGO_ENABLED=0 GOOS=darwin GOARCH="$arch" go build -trimpath -ldflags "$LDFLAGS" -o "$dest" ./cmd/capri-host
}

build_app() {
  local arch=$1 dest=$2
  local target="arm64-apple-macos13"
  local tmp_src="$TMP/swift-$arch"
  mkdir -p "$tmp_src"
  cp "$ROOT"/packaging/macos/CapriApp/*.swift "$tmp_src/"
  printf 'let capriVersion = "%s"\n' "$VERSION" > "$tmp_src/Version.swift"
  xcrun swiftc -parse-as-library -swift-version 5 -O \
    -target "$target" \
    -sdk "$SDK" \
    -framework SwiftUI \
    -framework AppKit \
    -framework Foundation \
    -framework ServiceManagement \
    -framework IOKit \
    -o "$dest" \
    "$tmp_src"/*.swift
}

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "building Capri.app ($VERSION, darwin/$ARCH)"
build_host "$ARCH" "$MACOS/Capri-host"
build_app "$ARCH" "$MACOS/Capri"
chmod +x "$MACOS/Capri" "$MACOS/Capri-host"

# 图标以 packaging/macos/CapriApp/ 里的 PNG 为准，打包不再覆盖手绘稿。
ICON_SRC="$ROOT/packaging/macos/CapriApp"
for f in MenuBarIcon.png MenuBarIcon@2x.png MenuBarIcon@3x.png AppIcon.png; do
  if [[ ! -f "$ICON_SRC/$f" ]]; then
    echo "missing $ICON_SRC/$f" >&2
    exit 1
  fi
done
cp "$ICON_SRC"/MenuBarIcon*.png "$RES/"
cp "$ICON_SRC/AppIcon.png" "$RES/"

PNG="$ROOT/packaging/macos/CapriApp/AppIcon.png"
ICONSET="$TMP/Capri.iconset"
mkdir -p "$ICONSET"
for s in 16 32 64 128 256 512; do
  sips -z "$s" "$s" "$PNG" --out "$ICONSET/icon_${s}x${s}.png" >/dev/null
  ds=$((s * 2))
  if [[ "$ds" -le 1024 ]]; then
    sips -z "$ds" "$ds" "$PNG" --out "$ICONSET/icon_${s}x${s}@2x.png" >/dev/null
  fi
done
iconutil -c icns -o "$RES/AppIcon.icns" "$ICONSET"

sed "s/CAPRI_VERSION/${VERSION}/g" "$ROOT/packaging/macos/Info.plist" > "$OUT/Contents/Info.plist"

# ── 签名 ──────────────────────────────────────────────────────────────
# TCC 用「设计要求」（designated requirement）匹配已授权的权限。ad-hoc 签名的
# 设计要求就是裸 cdhash，源码或 VERSION 一变 cdhash 就变，已授权的屏幕录制等
# 权限随即失配——tccd 记的是 “Failed to match existing code requirement”，用户
# 看到的是重复的授权提示。换成证书签名后设计要求是 identifier + 证书指纹，
# 跨重建恒定。取不到身份就退回 ad-hoc：照常能跑，但每次重建都要重新授权。
if command -v codesign >/dev/null; then
  IDENTITY="${CAPRI_CODESIGN_IDENTITY:-}"
  if [[ -z "$IDENTITY" ]]; then
    # 优先专用身份 "Capri Dev"（make-cert.sh 装的就是它），其次机上有且只有一个
    # 可用身份时直接用它。两者都没有就不猜：任何证书（含自签）都会给出
    # identifier + 证书指纹 的稳定设计要求，所以不必挑特定名字，但挑错别人的
    # 证书签名也不好。多于一个且都不是 Capri Dev 时，让调用方用
    # CAPRI_CODESIGN_IDENTITY 明确指定。
    IDENTITIES="$(security find-identity -v -p codesigning 2>/dev/null | awk -F'"' '/^ *[0-9]+\)/{print $2}')"
    if printf '%s\n' "$IDENTITIES" | grep -qx "Capri Dev"; then
      IDENTITY="Capri Dev"
    elif [[ "$(printf '%s\n' "$IDENTITIES" | grep -c .)" -eq 1 ]]; then
      IDENTITY="$IDENTITIES"
    fi
  fi
  if [[ -z "$IDENTITY" ]]; then
    echo "warn: 没有唯一可用的签名身份，退回 ad-hoc。" >&2
    echo "      ad-hoc 的设计要求是 cdhash，每次重建都会让已授权权限（如屏幕录制）失配。" >&2
    echo "      用 CAPRI_CODESIGN_IDENTITY=\"<身份名>\" 指定身份（security find-identity -v -p codesigning）可避开。" >&2
    IDENTITY="-"
  fi
  # 嵌套二进制先各自签，再签外层 bundle。不用 --deep：它已废弃，且会给每个组件
  # 派生一个带随机 UUID 的 identifier（每次构建都不同），那本身就是不稳定身份。
  codesign --force --timestamp=none --sign "$IDENTITY" \
    --identifier com.agentsharness.capri-host "$MACOS/Capri-host"
  codesign --force --timestamp=none --sign "$IDENTITY" \
    --identifier com.agentsharness.capri "$MACOS/Capri"
  codesign --force --timestamp=none --sign "$IDENTITY" "$OUT"
  echo "sign   $IDENTITY"
  echo -n "requirement: "
  codesign -d -r- "$OUT" 2>/dev/null | tail -1
fi

ZIP="$ROOT/dist/Capri-macos.zip"
rm -f "$ZIP"
ditto -c -k --keepParent --norsrc --noextattr "$OUT" "$ZIP"
echo "built $OUT"
echo "zip    $ZIP"
