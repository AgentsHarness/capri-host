# SPEC — align/ext1（SA2a：fs/git/worktree/search/terminal/code 扩展包装）

## 目标
为 grok 支持的 x.ai 扩展方法补齐 host 侧 typed 包装（bridge 层），全部基于 SA1 提供的 `XaiCall`。只写**新建文件**，不碰任何现有文件。

## 文件所有权（严格）
- **只允许新建**：`internal/acp/bridge_ext.go`（UnwrapExtResult 辅助）、`internal/acp/bridge_ext_fs.go`（fs/git/worktree/search/terminal/code 包装）、`internal/acp/bridge_ext_fs_test.go`（测试，包 acp）
- **禁止触碰**：`internal/acp/bridge.go`、`types.go`、`session_tasks.go`、`ext_methods_test.go`、`session_meta_test.go`、`internal/server/*`、README.md
- `XaiCall` 由 align/core 工作包提供（合并后存在）；你**不要**重新定义它，只调用：
  ```go
  func (b *Bridge) XaiCall(ctx context.Context, method string, params map[string]any) (map[string]any, error)
  ```
  语义：发 `_x.ai/<method>`；params 中 `"sessionId"` 或 `"session_id"` 为 `""` 时自动填活动会话 id（无活动会话返回 HTTPError 404）；键缺失则不带；返回**原始** result（不拆 ExtMethodResult 信封）。
- 可用的同包现有辅助：`pick`、`toInt64`、`resolveSessionID`、`HTTPError`。

## 背景：线级字段契约（已从 grok-build 源码核实，grok 侧一律 `#[serde(rename_all="camelCase")]`，除非注明）
所有请求字段名按下列表**原样**发送（wrapper 参数转成这些键）。

### fs（x.ai/fs/*，全部 camelCase，sessionId 可选）
- `x.ai/fs/list`：`{sessionId?, path, depth, includeHidden, limit, offset, followSymlinks, respectGitIgnore, includeGlobs, excludeGlobs}`
- `x.ai/fs/exists`：`{sessionId?, path}`
- `x.ai/fs/read_file`：`{sessionId?, path, maxBytes, maxLines?, offset?, length?, encoding?}`
- `x.ai/fs/write_file`：`{sessionId?, path, content, createDirs}`
- `x.ai/fs/delete_file`：`{sessionId?, path}`

### git（x.ai/git/*，全部 camelCase，gitRoot 可选）
- `x.ai/git/status`：`{sessionId?, gitRoot?, includeUntracked?, includeStats?, ignoreSubmodules?, includePatches?}`
- `x.ai/git/files`：`{sessionId?, gitRoot?, paths: Vec<String>, version: String}`
- `x.ai/git/diffs`：`{sessionId?, gitRoot?, paths?, from: String, to: String, includePatch: bool}`
- `x.ai/git/stage`：`{sessionId?, gitRoot?, paths?}`
- `x.ai/git/stage/content`：`{sessionId?, gitRoot?, path: String, content: String}`
- `x.ai/git/unstage`：`{sessionId?, gitRoot?, paths?}`
- `x.ai/git/discard`：`{sessionId?, gitRoot?, paths?, includeUntracked: bool}`
- `x.ai/git/commit`：`{sessionId?, gitRoot?, message: String, amend: bool, signoff: bool, push: bool}`
- `x.ai/git/stash`：`{sessionId?, gitRoot?, includeUntracked: bool}`
- `x.ai/git/checkout`：`{sessionId?, gitRoot?, branch: String, create: bool}`
- `x.ai/git/checkout_commit`：`{sessionId?, gitRoot?, commit: String, stashIfDirty: bool}`
- `x.ai/git/checkout_session_head`：`{sessionId}`
- `x.ai/git/branches`：`{sessionId?, gitRoot?}`
- `x.ai/git/current_commit`：`{sessionId?, gitRoot?}`
- `x.ai/git/info`：`{sessionId?, gitRoot?}`（**已有** Bridge.GitInfo 实现，不要重复）
- `x.ai/git/git_repo_root`：先去 grok 源码确认请求形状（`extensions/git.rs` 搜索 git_repo_root 或 repo root handler），按实际实现写。

### worktree（x.ai/git/worktree/*，camelCase；先去 grok 源码 `extensions/worktree.rs` 核实每个请求形状）
- `create`、`create_from_worktree`、`create_from_worktree_sync`、`remove`、`apply`、`list`、`show`、`gc`、`resume_session`、`db/path`、`db/stats`、`db/rebuild` —— 逐个读 worktree.rs 的 Request struct 定义后实现（不要猜字段）。

### search（x.ai/search/*，camelCase）
- `x.ai/search/fuzzy/open`：`{sessionId?, cwd?, root?}`
- `x.ai/search/fuzzy/change`：`{searchId: String, query: String, dirsOnly: bool, limit?}`
- `x.ai/search/fuzzy/close`：`{searchId: String}`
- `x.ai/search/content`：`{sessionId?, cwd?, params: <ContentSearchRequestParams>}` —— params 是**嵌套对象**，先读 `extensions/search.rs` 中 ContentSearchRequestParams 定义，按实际字段写。

### terminal（x.ai/terminal/*，camelCase；先读 `extensions/terminal.rs` 核实）
- `create`、`output`、`wait_for_exit`、`kill`、`release`、`background`、`list`、`pty/create`、`pty/load`、`pty/resize`、`pty/input`、`pty/notification` —— 逐个读 Request struct 定义后实现。

### code（x.ai/code/*，camelCase；先读 `extensions/code_nav.rs` 核实）
- `goto-definition`、`goto-references`、`find-definitions`、`find-references`、`status` —— 按实际 Request struct 实现。

## 任务

### 1. bridge_ext.go：`UnwrapExtResult`
```go
// UnwrapExtResult unwraps the common ExtMethodResult envelope
// {"result": <payload>, "error": ...} → the inner payload map.
// Non-envelope results are returned unchanged. nil-safe.
func UnwrapExtResult(res map[string]any) map[string]any
```

### 2. bridge_ext_fs.go：typed 包装
每个方法一个薄包装，命名规则：`FsList`、`FsExists`、`FsReadFile`、`FsWriteFile`、`FsDeleteFile`；git：`GitStatus(ctx, gitRoot string, includeUntracked *bool) ...` —— 签名你来定，但**必须**：
- 第一个参数 ctx；需要 gitRoot 的都有 `gitRoot string` 参数（空则不带该键）；
- 可选布尔/字符串用指针或 Go 惯例（如 `*bool`、`string` 空即省略）——在函数文档注释里写明省略规则；
- 内部调用 `b.XaiCall(ctx, "x.ai/...", params)`，返回**已 UnwrapExtResult 的**结果（除明确要原始结果的）；
- sessionId 一律传 `""`（依赖 XaiCall 的默认填充规则）或省略（当 grok 侧可选时省略更稳——**约定：凡是 sessionId 可选的，一律省略**，grok 会 fallback；`checkout_session_head` 这种必填 sessionId 的才传 `""` 让 XaiCall 填充）。

### 3. 测试（bridge_ext_fs_test.go，包 acp）
用 `readyBridge()` / `resolveNext` / `recordingStdin`（同包现有，见 ext_methods_test.go）：
- 至少覆盖 8 个代表性方法：`GitStatus`（断言 wire method `_x.ai/git/status`、params 键为 camelCase 如 includeUntracked、无 sessionId 键）、`GitCommit`（message/amend/signoff/push 键）、`FsReadFile`（path/maxBytes）、`FsList`、`SearchFuzzyChange`（searchId/query）、`TerminalList`、`CodeGotoDefinition`、`GitCheckoutSessionHead`（断言 sessionId 被 XaiCall 填成 "s1"）。
- 一个 `UnwrapExtResult` 单测（信封/非信封/nil）。

## 完成标准
- `gofmt -l .` 无输出；`go build ./...`、`go vet ./...`、`go test ./...` 全绿。
- 注意：**此时 worktree 里还没有 XaiCall 的实现**（align/core 分支合并后才存在）——测试里只调你的包装函数，编译期 XaiCall 未定义会报错。**对策**：本 worktree 是 align/ext1 分支，XaiCall 由 SA1 在 align/core 提供；你无法单独编译。请在 SPEC 目录旁再放一个 `internal/acp/bridge_ext_stub_test.go`？不行——不能碰现有文件。
  **正确做法**：合并前先 `git fetch . align/core` 并 `git merge align/core` 到你的分支（把 XaiCall 拉进来），然后开发、测试、提交。若 merge 冲突（不可能：文件不重叠），以 align/core 为准。
- 提交：`git add -A && git commit -m "bridge_ext: fs/git/worktree/search/terminal/code wrappers"`。
- 报告：文件清单、方法清单、测试清单、任何偏离及原因。
