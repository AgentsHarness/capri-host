# grok-build 1.0.89 ACP surface delta

Audit of the installed `grok` binary against the 1.0.45 source mirror and the
three Capri ends (`acp-host`, `acp-hub`, `acp-fe`).

- Binary: `~/.grok/downloads/grok-1.0.89-macos-aarch64`
  (`grok 1.0.89 (28be5bb1bf65)`).
- Source mirror: `~/ccwork/grok-build` at `2bdd1d6a` (`xai-grok-shell`
  version `1.0.45`). There is no newer source checkout.
- Live probes: `grok agent stdio` with newline-delimited ACP JSON-RPC.
  Extension methods use the `_x.ai/...` stdio encoding; a bare `x.ai/...`
  is answered with `Method not found`. Results below were confirmed on the
  wire, not inferred from strings alone.
- `strings` concatenates adjacent Rust literals, so method presence was
  confirmed by a live call. `SessionUpdate` variants were confirmed by the
  binary's `struct variant SessionUpdate::<Name> with N elements` metadata
  and by the enum's serde field table.

## Summary

Seven new `SessionUpdate` variants, one removed variant, three new request
methods, two removed request methods, three new client-capability flags, and
one new notification method. Most field changes are additive; the notable
exceptions are `auto_compact_started.reason` and
`task_backgrounded.description`.

The two host changes made in this session are marked **done** below. The rest
are recommendations, ordered by impact.

## Request methods

| Method | Status | Evidence | Host | FE | Action |
|---|---|---|---|---|---|
| `x.ai/session/turns` | **new** | `{sessionId, before: string, turns: nonzero usize}`; wrong cursor → `Invalid params: the cursor is not one this session issued`. Binary also names `BackwardsCursor`, `x.ai/session/turnCount`, `x.ai/earlierTurns`, `x.ai/transcriptStart`, `x.ai/session/turns/list`. | No wrapper; `SessionLoadHistory` still calls the removed `x.ai/session/load_history`. | No caller. `loadMoreHistory` uses `POST /api/session-updates`, so it is unaffected. | Migrate `SessionLoadHistory`/`POST /api/session-load-history` to `x.ai/session/turns`, or retire the dead endpoint. Add a typed wrapper only when the FE implements earlier-turn pagination. |
| `x.ai/models/refresh` | **new** | `{}` → `{"kind":"refreshed","modelCount":6}`. | No wrapper. | No caller. | Optional: add a `ModelsRefresh` wrapper + “refresh models” action. Catalog updates already arrive via `x.ai/models/update`. |
| `x.ai/memory/dream/cancel` | **new** | `{session_id: string}` → `{}` (snake_case; camelCase is rejected). | `MemoryDream` exists, no cancel. | `MemoryModal` has a dream button, no cancel. | Optional: add `MemoryDreamCancel` + `POST /api/memory-dream-cancel` and a cancel button while a dream is running. |
| `x.ai/session/load_history` | **removed** | Absent from the 1.0.89 binary; live call → `-32601 Method not found`. | `SessionLoadHistory` + `POST /api/session-load-history` still call it. | API wrapper exists, no callers. | Retire or migrate to `x.ai/session/turns`. No user-visible break today. |
| `x.ai/subagent/message` | **removed from the stdio agent** | Live call → `-32601` in both leader and `--no-leader` modes. The pager still has the request code, but the 1.0.89 shell route exposes subagent messaging as the `send_subagent_message` tool, gated by `features.active_agent_messages` (default false). | `SubagentMessage` + `POST /api/subagent/message` still call it. | `SubagentComposer` is intentionally not rendered; its comment already documents this exact gate and address problem. | Host endpoint is dead but unused by the FE. Keep the FE composer hidden; if messaging is wanted later, route it through the `send_subagent_message` tool or the daemon route, not this ext method. |

## `SessionUpdate` variants

Confirmed new in 1.0.89 (absent from the 1.0.45 enum):

| Variant | Shape | Meaning / likely fields | Host today | FE today |
|---|---|---|---|---|
| `session_ended` | 1 field | Session is gone (closed by another client, teardown, ended relay). Field name not recovered; the FE accepts `reason`/`message`/`summary`. | Generic `session_notification`; `observeUpdateKind` now closes the observed turn. **done** | `notifCore.ts` writes one “会话已结束” line. **done** |
| `memory_browse` | unit | `/memory` browse signal. | Generic `session_notification`. | Registered in `UNCONSUMED_KINDS`; no UI. |
| `login_refused` | 1 field | Auth failure; the field-name pool contains `remedy`. | Generic `session_notification`. | Registered in `UNCONSUMED_KINDS`; login UI still goes through the hub/FE gate. |
| `backend_responsive` | 1 field, `responsive` | Auth backend recovered. | Generic `session_notification`. | Registered in `UNCONSUMED_KINDS`. |
| `resume_notice` | 1 field | Notice produced while replaying a resumed session (`session/load`). | Generic `session_notification`. | Registered in `UNCONSUMED_KINDS`; history-load state already covers the visible path. |
| `links_left_out` | 1 field, likely `count` | Tool output omitted links. | Generic `session_notification`. | Registered in `UNCONSUMED_KINDS`. |
| `workflow_log` | 4 fields | Workflow log line. | Generic `session_notification`. | Registered in `UNCONSUMED_KINDS`; no workflow-log panel. |

Removed in 1.0.89:

- `memory_capture_activity` — absent from the binary. The host/FE still have
  handlers for older agents; harmless to keep.

Already present in 1.0.45 and unchanged: `DiffReview`, `RetryState`,
`AutoCompact*`, `Memory*` (except the removed one), `Hook*`, `Plugin*`,
`SessionStatus`, `SessionRecap*`, `LastTurnSummary`, `CompactionCheckpoint`,
`RewindMarker`, `TaskCompleted`, `Subagent*`, `TaskBackgrounded`,
`BackgroundTasks`, `ScheduledTask*`, `MonitorEvent`, `Model*`, `ServedModel`,
`ToolCallDeltaChunk`, `Image*`, `MemoryFiles`, `WorkflowUpdated`,
`GoalUpdated`, `PendingInteraction`, `InteractionResolved`, `Plan*`,
`TurnCompleted`, `Response*`, `ReasoningCompleted`, `TurnUsage`, `Unknown`.

## Client capabilities and notifications

The 1.0.89 binary's capability/config name list contains these `x.ai/...`
entries:

- `x.ai/incrementalBashOutput`, `x.ai/bashOutputNoColor`,
  `x.ai/gitHeadChanged`, `x.ai/userMessageEcho`, `x.ai/hunkTracker`,
  `x.ai/statusLine`, `x.ai/sessionEnded`, `x.ai/sessionNotice`,
  `x.ai/planKept`.

`x.ai/statusLine` is confirmed as a client capability by the 1.0.45 source
(the pager's `client_capabilities_meta`). The other three are new in 1.0.89
and follow the same naming convention; their exact gating is inferred, not
proven. Live probe confirmed the `statusLine` behavior:

- with `x.ai/statusLine: true`: `session_status` arrives with
  `context_window.context_window_size`, `context_tokens`, `used_percentage`,
  `auto_compact_threshold_percent`, `cost.total_duration_ms`, `model`, etc.
- without it: no `session_status` at all.

The FE already renders `session_status` (`notifApps.ts` → `parseSessionStatus`
→ `StatusChips`). **Done**: `initCapabilitiesMeta()` now sends
`x.ai/statusLine: true`.

The other three flags are new in 1.0.89. They likely gate the corresponding
events, but this audit could not trigger them:

- `x.ai/sessionEnded` → `session_ended`.
- `x.ai/sessionNotice` → the new `x.ai/session/notice` notification (the
  1.0.89 pager handles it at `session_notification.rs:22`; absent from
  1.0.45).
- `x.ai/planKept` → `plan_kept` (the variant exists in 1.0.45 but has no
  producer there).

Recommendation: do not advertise these until the FE has a consumer; today
they would only produce generic `ext_notification` rows (or be dropped).
`x.ai/session/notice` currently falls through the host default to a dim
“扩展通知: x.ai/session/notice” row.

## Field-level changes

| Carrier | 1.0.45 | 1.0.89 | Impact |
|---|---|---|---|
| `session/load` response `_meta` | — | `x.ai/session/turnCount` (confirmed: 3 for a 3-turn session) | Host stores it in `sessionMeta`; FE does not read it. Additive. |
| `session/new`/`session/load` `_meta` | — | `x.ai/memoryNotesWritable` | Host passes through. Additive. |
| `auto_compact_started` | `tokens_used`, `context_window`, `percentage`, `reason` | adds `no_estimate` (metadata says 5 fields) | FE only reads `percentage`; TUI banner copy may need `no_estimate`. Verify with a real auto-compact sample. |
| `task_backgrounded` | `tool_call_id`, `task_id`, `command`, `cwd`, `output_file`, `monitor_description`, `description` | adds `background_reason`, `truncated`; `description` not in the enum field table | FE falls back from `description` to `command`; row title still works. Use `background_reason` for detail if desired. |
| `image_dropped` | `notes` | adds `side_question` | Additive. |
| `turn_completed` | 6 fields | 7 fields; `message_id` in the field table | Additive; sample did not include it. |
| `subagent_spawned` | 16 fields | 17 fields; adds `spawner_session_id` (optional) | Live sample confirmed `agentAddress`, `effective_context_source`, `model`; `workflow_run_id`/`agentAddress` already existed in 1.0.45. FE already renders the core fields. |
| `subagent_progress` / `subagent_finished` | 12 / 11 | 12 / 11 | Live sample compatible (`tool_calls`, `turns`, `duration_ms`, `tokens_used`, `output`, `will_wake`). |
| `goal_updated` | 32 | 32 | No confirmed delta; the `current_deliverable_title` / `classifier_*` / `live_*` fields already exist in 1.0.45. |
| `workflow_updated` | 22 | 22 | No confirmed delta; `workflow_log` is a separate variant. |

Additional 1.0.89 `session/load` request `_meta` keys: `x.ai/initialTurns` and
`x.ai/session/excludeTurns` (binary line 70083). They control windowed
replay. The host sends neither, so it keeps the full-history replay and does
not need them unless it adopts earlier-turn pagination.

## What changed in this session

Host (`~/ccwork/acp-host`):

- `internal/acp/bridge.go`
  - `initCapabilitiesMeta()` now advertises `x.ai/statusLine: true`.
  - `observeUpdateKind()` closes the observed turn on `session_ended`, so a
    session that dies mid-turn cannot leave the FE busy until
    `turnStaleAfter`.
- `internal/acp/align_full_test.go` and `internal/server/align_full_test.go`
  updated for the new capability key.
- `internal/acp/session_ended_test.go` added: asserts `session_ended` closes
  the observed turn and clears `Busy`.

FE (`~/ccwork/acp-fe`):

- `src/store/chat/events/kinds.ts`
  - `EXTENSION_KINDS` now includes `login_refused`, `backend_responsive`,
    `memory_browse`, `links_left_out`, `session_ended`, `resume_notice`,
    `workflow_log`.
  - `UNCONSUMED_KINDS` documents the six without a UI; `session_ended` is
    consumed instead.
- `src/store/chat/events/notifCore.ts` handles `session_ended` with a
  scrollback line (accepts `reason`/`message`/`summary`).
- `src/store/chat/events/notifCore.test.ts` adds two `session_ended` tests.

Verification:

- `cd ~/ccwork/acp-host && go test ./internal/acp/ ./internal/server/ -count=1`
  → both packages pass.
- `cd ~/ccwork/acp-fe && npx vitest run` → 277 files, 3271 tests pass.
- `cd ~/ccwork/acp-fe && npm run build` → TypeScript + Vite build passes.
- Live 1.0.89 probe: `x.ai/statusLine: true` produces `session_status`;
  absent produces none.
- Browser E2E was not run because `playwright-cli` is not installed on this
  machine. The status-line path was verified through the host capability test,
  the live agent probe, and the existing FE `session_status` tests.

## Remaining recommendations

1. **Host history endpoint**: retire `POST /api/session-load-history` or
   migrate it to `x.ai/session/turns`. No FE caller today, but the endpoint
   now always returns `-32601`.
2. **Host subagent endpoint**: `POST /api/subagent/message` is dead against
   1.0.89. The FE already hides the composer; either remove the endpoint or
   route future messaging through the `send_subagent_message` tool.
3. **Host memory cancel**: add `MemoryDreamCancel` +
   `POST /api/memory-dream-cancel` if the FE wants a cancel button.
4. **Host models refresh**: add a thin `ModelsRefresh` wrapper if a manual
   catalog refresh is wanted; the generic `POST /api/xai-call` already works.
5. **FE `session_ended`**: the current handler writes a line but does not
   change connection state. If the session ended because the agent is gone,
   the next `ready`/`load` re-anchors; no further action unless the product
   wants a stronger “session closed” state.
6. **FE `x.ai/session/notice`**: add a typed handler (likely `statusText`)
   or add it to `SILENT_EXT_NOTIFICATIONS`; today it renders as a dim
   “扩展通知” row if the agent emits it.
7. **Hub**: no mandatory change. New event types are droppable. If
   `session_ended` must never be dropped, add it to `criticalEventTypes` in
   `internal/hub/hub.go` alongside the turn terminals.

## Unverified / follow-ups

- Exact payloads of `session_ended`, `login_refused`, `backend_responsive`,
  `resume_notice`, `links_left_out`, `workflow_log`. Field names were
  inferred from the binary's type metadata and field-name pool; no real wire
  sample was produced.
- `x.ai/session/turns` response shape. A valid `before` cursor is issued by
  the session and was not obtained from a 3-turn test session (the agent
  replayed all 3 turns and returned only `x.ai/session/turnCount`).
- Gating semantics of `x.ai/sessionEnded`, `x.ai/sessionNotice`,
  `x.ai/planKept`. Only the capability list and the corresponding variant
  names are confirmed.
- `auto_compact_started.reason` → `no_estimate`: the field count grew from 4
  to 5, but a real auto-compact sample was not produced.
