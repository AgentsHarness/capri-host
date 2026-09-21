#!/usr/bin/env bash
# 组装 Capri.app：SwiftUI 原生菜单栏 + 内嵌 Capri-host。
# 用法：
#   ./packaging/macos/make-app.sh              # 当前架构
#   ./packaging/macos/make-app.sh --universal  # arm64+amd64
# 环境：VERSION（默认 dev）、OUT（默认 dist/Capri.app）
# 需要先有 internal/server/web/dist（go:embed）。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

VERSION="${VERSION:-dev}"
UNIVERSAL=0
if [[ "${1:-}" == "--universal" ]]; then
  UNIVERSAL=1
fi

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
  local target
  case "$arch" in
    arm64) target="arm64-apple-macos13" ;;
    amd64|x86_64) target="x86_64-apple-macos13" ;;
    *) echo "unknown arch $arch" >&2; return 1 ;;
  esac
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

if [[ "$UNIVERSAL" == 1 ]]; then
  echo "building universal Capri.app ($VERSION)"
  build_host amd64 "$TMP/host-amd64"
  build_host arm64 "$TMP/host-arm64"
  build_app amd64 "$TMP/app-amd64"
  build_app arm64 "$TMP/app-arm64"
  lipo -create -output "$MACOS/Capri-host" "$TMP/host-amd64" "$TMP/host-arm64"
  lipo -create -output "$MACOS/Capri" "$TMP/app-amd64" "$TMP/app-arm64"
else
  arch="$(uname -m)"
  [[ "$arch" == "x86_64" ]] && arch="amd64"
  echo "building Capri.app ($VERSION, darwin/$arch)"
  build_host "$arch" "$MACOS/Capri-host"
  build_app "$arch" "$MACOS/Capri"
fi
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

if command -v codesign >/dev/null; then
  codesign --force --sign - --deep "$OUT" >/dev/null 2>&1 || true
fi

ZIP="$ROOT/dist/Capri-macos.zip"
rm -f "$ZIP"
ditto -c -k --keepParent --norsrc --noextattr "$OUT" "$ZIP"
echo "built $OUT"
echo "zip    $ZIP"
