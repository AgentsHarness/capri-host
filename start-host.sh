#!/usr/bin/env bash
#
# acp-host 启动脚本 — 本地模式 / Hub 中继（relay）模式
#
#   ./start-host.sh                    本地模式（默认 :8765，API + 内置前端一个端口）
#   ./start-host.sh -d                 后台运行（日志 bin/acp-host.log，PID bin/acp-host.pid）
#   ./start-host.sh --hub URL --pair-code XXXXX   中继模式：配对并连接 acp-hub
#   ./start-host.sh status / stop      查看 / 停止后台实例
#
# 常用参数（也可用同名环境变量）：
#   --port 8765       监听端口（PORT）
#   --grok-bin grok   grok 可执行文件（GROK_BIN）
#   --host-id local   Host 标识，多 Host 时区分（HOST_ID）
#   --host-name "我的 Mac"   展示名（HOST_NAME）
#   --hub URL         Hub 地址，如 http://1.2.3.4:8787（HUB_URL）
#   --pair-code CODE  一次性配对码，Hub 启动日志 / GET /api/pairing 查看（HUB_PAIR_CODE）
#   --token TOKEN     已配对 token，优先于配对码（HOST_TOKEN）
#   --no-build        跳过增量构建，直接跑现有二进制
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$ROOT/bin/acp-host"
PIDFILE="$ROOT/bin/acp-host.pid"
LOGFILE="$ROOT/bin/acp-host.log"

# ── 参数（环境变量为默认值）─────────────────────────────────────────
PORT="${PORT:-8765}"
GROK_BIN="${GROK_BIN:-grok}"
HOST_ID="${HOST_ID:-local}"
HOST_NAME="${HOST_NAME:-Local Host}"
HUB_URL="${HUB_URL:-}"
HUB_PAIR_CODE="${HUB_PAIR_CODE:-}"
HOST_TOKEN="${HOST_TOKEN:-}"
NO_BUILD=0
DAEMON=0

usage() {
  sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}

stop_cmd() {
  if [[ ! -f "$PIDFILE" ]]; then
    echo "没有运行中的后台实例 ($PIDFILE 不存在)"
    exit 1
  fi
  local pid
  pid="$(cat "$PIDFILE")"
  kill "$pid" 2>/dev/null || true
  for _ in $(seq 1 20); do
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$PIDFILE"
      echo "已停止 (pid $pid)"
      exit 0
    fi
    sleep 0.5
  done
  echo "进程未在 10s 内退出，强制结束" >&2
  kill -9 "$pid" 2>/dev/null || true
  rm -f "$PIDFILE"
}

status_cmd() {
  if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    echo "运行中 pid=$(cat "$PIDFILE")"
    if curl -fsS "http://127.0.0.1:$PORT/api/status" 2>/dev/null | head -c 300; then
      echo
    fi
  else
    echo "未运行"
    [[ -f "$PIDFILE" ]] && rm -f "$PIDFILE"
  fi
}

# 子命令：先于参数解析处理
case "${1:-}" in
  stop)   stop_cmd ;;
  status) status_cmd ;;
  -h|--help|help) usage ;;
esac

while [[ $# -gt 0 ]]; do
  case "$1" in
    --hub)        HUB_URL="$2"; shift 2 ;;
    --pair-code)  HUB_PAIR_CODE="$2"; shift 2 ;;
    --token)      HOST_TOKEN="$2"; shift 2 ;;
    --host-id)    HOST_ID="$2"; shift 2 ;;
    --host-name)  HOST_NAME="$2"; shift 2 ;;
    --port)       PORT="$2"; shift 2 ;;
    --grok-bin)   GROK_BIN="$2"; shift 2 ;;
    --no-build)   NO_BUILD=1; shift ;;
    -d|--daemon)  DAEMON=1; shift ;;
    -h|--help|help) usage ;;
    stop|status)  ;; # 已在上面的 case 处理过，这里只是吞掉
    *) echo "未知参数: $1" >&2; usage >&2; exit 1 ;;
  esac
done

# ── 前置检查 ─────────────────────────────────────────────────────────
if [[ $NO_BUILD -eq 0 ]] && ! command -v go >/dev/null 2>&1; then
  echo "错误: 找不到 go (需要构建 $BIN)，可用 --no-build 跑已有二进制" >&2
  exit 1
fi
if ! command -v "$GROK_BIN" >/dev/null 2>&1; then
  echo "警告: 找不到 grok ($GROK_BIN) — 服务能启动，但首次提问前请先 grok login 或设置 XAI_API_KEY" >&2
fi

# ── 增量构建：二进制缺失，或源码 / 嵌入的前端产物比二进制新 ─────────
needs_build=0
if [[ ! -x "$BIN" ]]; then
  needs_build=1
elif find "$ROOT" -name '*.go' -newer "$BIN" -print -quit | grep -q . \
  || find "$ROOT/internal/server/web" -type f -newer "$BIN" -print -quit | grep -q .; then
  needs_build=1
fi
if [[ $NO_BUILD -eq 1 ]]; then
  [[ -x "$BIN" ]] || { echo "错误: --no-build 但 $BIN 不存在" >&2; exit 1; }
  needs_build=0
fi
if [[ $needs_build -eq 1 ]]; then
  echo "==> 构建 $BIN ..."
  (cd "$ROOT" && go build -o "$BIN" ./cmd/acp-host)
fi

# ── 组装环境 ─────────────────────────────────────────────────────────
envs=(PORT="$PORT" GROK_BIN="$GROK_BIN" HOST_ID="$HOST_ID" HOST_NAME="$HOST_NAME")
[[ -n "$HUB_URL" ]]       && envs+=(HUB_URL="$HUB_URL")
[[ -n "$HUB_PAIR_CODE" ]] && envs+=(HUB_PAIR_CODE="$HUB_PAIR_CODE")
[[ -n "$HOST_TOKEN" ]]    && envs+=(HOST_TOKEN="$HOST_TOKEN")

if [[ -n "$HUB_URL" ]]; then
  echo "==> 中继模式：连接 $HUB_URL"
  if [[ -z "$HUB_PAIR_CODE" && -z "$HOST_TOKEN" && ! -f "$HOME/.acp-host/hub.json" ]]; then
    echo "    未提供配对码/token：先在 Hub 上 curl $HUB_URL/api/pairing 拿到配对码再补 --pair-code" >&2
  fi
else
  echo "==> 本地模式：http://localhost:$PORT (API + 内置 acp-fe 前端，一个端口全搞定)"
fi

mkdir -p "$ROOT/bin"
if [[ $DAEMON -eq 1 ]]; then
  nohup env "${envs[@]}" "$BIN" >>"$LOGFILE" 2>&1 &
  echo $! > "$PIDFILE"
  echo "==> 已后台启动 (pid $(cat "$PIDFILE"))，日志: $LOGFILE"
  # 健康检查：最多等 30s，/api/status 通了就报就绪
  for _ in $(seq 1 30); do
    if curl -fsS "http://127.0.0.1:$PORT/api/status" >/dev/null 2>&1; then
      echo "==> 就绪: http://localhost:$PORT"
      exit 0
    fi
    sleep 1
  done
  echo "!! 30s 内未就绪，检查日志 $LOGFILE" >&2
  exit 1
fi

# 前台模式：exec 让 Go 进程直接接管终端与信号（Ctrl+C 优雅退出）
exec env "${envs[@]}" "$BIN"
