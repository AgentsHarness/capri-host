#!/usr/bin/env bash
# 把已有的 Capri.app 打成可拖进「应用程序」的 dmg。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
APP="${1:-$ROOT/dist/Capri.app}"
DMG="${2:-$ROOT/dist/Capri-macos.dmg}"
if [[ ! -d "$APP" ]]; then
  echo "missing $APP — run packaging/macos/make-app.sh first" >&2
  exit 1
fi
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
cp -R "$APP" "$STAGE/Capri.app"
ln -s /Applications "$STAGE/Applications"
rm -f "$DMG"
hdiutil create -volname Capri -srcfolder "$STAGE" -ov -format UDZO "$DMG" >/dev/null
echo "dmg    $DMG"
