package acp

import "context"

// ── typed x.ai extension wrappers (fs / git / worktree / search / terminal / code) ──
//
// Every wrapper is a thin typed layer over Bridge.XaiCall (align/core):
// it sends `_x.ai/<method>` with camelCase wire keys (the grok extension
// request structs are all #[serde(rename_all = "camelCase")]) and returns
// the XaiCall result with the ExtMethodResult envelope unwrapped via
// UnwrapExtResult.
//
// Session rule (host convention): sessionId is OMITTED whenever grok
// declares it optional (its serde struct has no required session_id field)
// — grok falls back to the session cwd for path resolution. Methods whose
// grok request struct REQUIRES session_id pass `"sessionId": ""` so
// XaiCall fills in the active session id (HTTPError 404 when none).
//
// Per-method omission rules for optional fields (Go zero values):
//   - string "" → key omitted
//   - []string empty → key omitted
//   - *bool / *int nil → key omitted (grok applies its serde default)
//   - plain bool → always sent (false == the grok default, identical wire)

// xaiResult unwraps the XaiCall result envelope (see UnwrapExtResult).
func xaiResult(res map[string]any, err error) (map[string]any, error) {
	return UnwrapExtResult(res), err
}

// ── fs (x.ai/fs/*) ────────────────────────────────────────────────────

// FsListOptions holds the optional x.ai/fs/list params. Nil pointers and
// empty glob slices are omitted so grok applies its defaults (depth=1,
// includeHidden=true, limit=1000, followSymlinks=true,
// respectGitIgnore=true). sessionId is optional on the wire and omitted.
type FsListOptions struct {
	Depth            *int
	IncludeHidden    *bool
	Limit            *int
	Offset           *int64
	FollowSymlinks   *bool
	RespectGitIgnore *bool
	IncludeGlobs     []string
	ExcludeGlobs     []string
}

// FsList calls x.ai/fs/list with the wire keys {path, depth?,
// includeHidden?, limit?, offset?, followSymlinks?, respectGitIgnore?,
// includeGlobs?, excludeGlobs?}. sessionId is omitted (grok falls back to
// the session cwd for relative paths). Omitting an option lets grok apply
// its default; the result is the unwrapped FsListData payload.
func (b *Bridge) FsList(ctx context.Context, path string, opts FsListOptions) (map[string]any, error) {
	params := map[string]any{"path": path}
	if opts.Depth != nil {
		params["depth"] = *opts.Depth
	}
	if opts.IncludeHidden != nil {
		params["includeHidden"] = *opts.IncludeHidden
	}
	if opts.Limit != nil {
		params["limit"] = *opts.Limit
	}
	if opts.Offset != nil {
		params["offset"] = *opts.Offset
	}
	if opts.FollowSymlinks != nil {
		params["followSymlinks"] = *opts.FollowSymlinks
	}
	if opts.RespectGitIgnore != nil {
		params["respectGitIgnore"] = *opts.RespectGitIgnore
	}
	if len(opts.IncludeGlobs) > 0 {
		params["includeGlobs"] = opts.IncludeGlobs
	}
	if len(opts.ExcludeGlobs) > 0 {
		params["excludeGlobs"] = opts.ExcludeGlobs
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/fs/list", params))
}

// FsExists calls x.ai/fs/exists with the wire key {path} (sessionId is
// optional and omitted). Returns the unwrapped FsExistsData payload.
func (b *Bridge) FsExists(ctx context.Context, path string) (map[string]any, error) {
	return xaiResult(b.XaiCall(ctx, "x.ai/fs/exists", map[string]any{"path": path}))
}

// FsReadFile calls x.ai/fs/read_file with the wire keys {path, maxBytes?,
// maxLines?, offset?, length?, encoding?}. sessionId is optional and
// omitted. maxBytes nil → omitted → grok's 1 MiB limit; encoding "" →
// omitted → grok's "utf8" (pass "base64" for binary-safe reads); the
// result is the unwrapped FsReadFileData payload.
func (b *Bridge) FsReadFile(ctx context.Context, path string, maxBytes *int, maxLines *int, offset, length *int64, encoding string) (map[string]any, error) {
	params := map[string]any{"path": path}
	if maxBytes != nil {
		params["maxBytes"] = *maxBytes
	}
	if maxLines != nil {
		params["maxLines"] = *maxLines
	}
	if offset != nil {
		params["offset"] = *offset
	}
	if length != nil {
		params["length"] = *length
	}
	if encoding != "" {
		params["encoding"] = encoding
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/fs/read_file", params))
}

// FsWriteFile calls x.ai/fs/write_file with the wire keys {path, content,
// createDirs?}. sessionId is optional and omitted. createDirs nil → omitted
// → grok's default true. Returns the unwrapped result (empty payload).
func (b *Bridge) FsWriteFile(ctx context.Context, path, content string, createDirs *bool) (map[string]any, error) {
	params := map[string]any{"path": path, "content": content}
	if createDirs != nil {
		params["createDirs"] = *createDirs
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/fs/write_file", params))
}

// FsDeleteFile calls x.ai/fs/delete_file with the wire key {path}
// (sessionId is optional and omitted). Returns the unwrapped result.
func (b *Bridge) FsDeleteFile(ctx context.Context, path string) (map[string]any, error) {
	return xaiResult(b.XaiCall(ctx, "x.ai/fs/delete_file", map[string]any{"path": path}))
}

// ── git (x.ai/git/*) ──────────────────────────────────────────────────
//
// All git wrappers take `gitRoot string` — "" omits the gitRoot key and
// grok resolves the root from the session cwd. sessionId is optional on
// the wire for every method below except checkout_session_head (its
// request struct requires session_id), so it is omitted everywhere except
// there.

// GitStatus calls x.ai/git/status with the wire keys {gitRoot?,
// includeUntracked?, includeStats?, ignoreSubmodules?, includePatches?}.
// sessionId is omitted. Nil *bool → key omitted → grok default
// (includeUntracked=true, others false). Returns the unwrapped
// GitStatusData payload.
func (b *Bridge) GitStatus(ctx context.Context, gitRoot string, includeUntracked, includeStats, ignoreSubmodules, includePatches *bool) (map[string]any, error) {
	params := map[string]any{}
	if gitRoot != "" {
		params["gitRoot"] = gitRoot
	}
	if includeUntracked != nil {
		params["includeUntracked"] = *includeUntracked
	}
	if includeStats != nil {
		params["includeStats"] = *includeStats
	}
	if ignoreSubmodules != nil {
		params["ignoreSubmodules"] = *ignoreSubmodules
	}
	if includePatches != nil {
		params["includePatches"] = *includePatches
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/status", params))
}

// GitFiles calls x.ai/git/files with the wire keys {gitRoot?, paths,
// version?}. sessionId is omitted. paths is required and always sent;
// version "" → omitted → grok's default "HEAD". Returns the unwrapped
// payload.
func (b *Bridge) GitFiles(ctx context.Context, gitRoot string, paths []string, version string) (map[string]any, error) {
	params := map[string]any{"paths": paths}
	if gitRoot != "" {
		params["gitRoot"] = gitRoot
	}
	if version != "" {
		params["version"] = version
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/files", params))
}

// GitDiffs calls x.ai/git/diffs with the wire keys {gitRoot?, paths?,
// from?, to?, includePatch?, includeContent?, maxPatchBytes?,
// maxPatchLines?, mergeBase?}. sessionId is omitted; empty paths omitted;
// from/to "" → omitted → grok defaults "HEAD"/"working". Returns the
// unwrapped GitDiffsData payload.
func (b *Bridge) GitDiffs(ctx context.Context, gitRoot string, paths []string, from, to string, includePatch, includeContent, mergeBase bool, maxPatchBytes, maxPatchLines *int) (map[string]any, error) {
	params := map[string]any{}
	if gitRoot != "" {
		params["gitRoot"] = gitRoot
	}
	if len(paths) > 0 {
		params["paths"] = paths
	}
	if from != "" {
		params["from"] = from
	}
	if to != "" {
		params["to"] = to
	}
	params["includePatch"] = includePatch
	params["includeContent"] = includeContent
	params["mergeBase"] = mergeBase
	if maxPatchBytes != nil {
		params["maxPatchBytes"] = *maxPatchBytes
	}
	if maxPatchLines != nil {
		params["maxPatchLines"] = *maxPatchLines
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/diffs", params))
}

// GitStage calls x.ai/git/stage with the wire keys {gitRoot?, paths?}.
// sessionId is omitted; empty paths → omitted (stages everything).
func (b *Bridge) GitStage(ctx context.Context, gitRoot string, paths []string) (map[string]any, error) {
	params := map[string]any{}
	if gitRoot != "" {
		params["gitRoot"] = gitRoot
	}
	if len(paths) > 0 {
		params["paths"] = paths
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/stage", params))
}

// GitStageContent calls x.ai/git/stage/content with the wire keys
// {gitRoot?, path, content}. sessionId is omitted.
func (b *Bridge) GitStageContent(ctx context.Context, gitRoot, path, content string) (map[string]any, error) {
	params := map[string]any{"path": path, "content": content}
	if gitRoot != "" {
		params["gitRoot"] = gitRoot
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/stage/content", params))
}

// GitUnstage calls x.ai/git/unstage with the wire keys {gitRoot?, paths?}.
// sessionId is omitted; empty paths → omitted (unstages everything).
func (b *Bridge) GitUnstage(ctx context.Context, gitRoot string, paths []string) (map[string]any, error) {
	params := map[string]any{}
	if gitRoot != "" {
		params["gitRoot"] = gitRoot
	}
	if len(paths) > 0 {
		params["paths"] = paths
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/unstage", params))
}

// GitDiscard calls x.ai/git/discard with the wire keys {gitRoot?, paths?,
// includeUntracked?, scope?}. sessionId is omitted; empty paths → omitted;
// scope "" → omitted → grok default "both" (else "working" | "staged" |
// "both", lowercase on the wire).
func (b *Bridge) GitDiscard(ctx context.Context, gitRoot string, paths []string, includeUntracked bool, scope string) (map[string]any, error) {
	params := map[string]any{"includeUntracked": includeUntracked}
	if gitRoot != "" {
		params["gitRoot"] = gitRoot
	}
	if len(paths) > 0 {
		params["paths"] = paths
	}
	if scope != "" {
		params["scope"] = scope
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/discard", params))
}

// GitCommit calls x.ai/git/commit with the wire keys {gitRoot?, message,
// amend?, signoff?, push?, sync?}. sessionId is omitted. message is
// required; the bool flags are always sent (false == grok default).
func (b *Bridge) GitCommit(ctx context.Context, gitRoot, message string, amend, signoff, push, sync bool) (map[string]any, error) {
	params := map[string]any{
		"message": message,
		"amend":   amend,
		"signoff": signoff,
		"push":    push,
		"sync":    sync,
	}
	if gitRoot != "" {
		params["gitRoot"] = gitRoot
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/commit", params))
}

// GitStash calls x.ai/git/stash with the wire keys {gitRoot?,
// includeUntracked?}. sessionId is omitted.
func (b *Bridge) GitStash(ctx context.Context, gitRoot string, includeUntracked bool) (map[string]any, error) {
	params := map[string]any{"includeUntracked": includeUntracked}
	if gitRoot != "" {
		params["gitRoot"] = gitRoot
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/stash", params))
}

// GitCheckout calls x.ai/git/checkout with the wire keys {gitRoot?,
// branch, create?}. sessionId is omitted. branch is required.
func (b *Bridge) GitCheckout(ctx context.Context, gitRoot, branch string, create bool) (map[string]any, error) {
	params := map[string]any{"branch": branch, "create": create}
	if gitRoot != "" {
		params["gitRoot"] = gitRoot
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/checkout", params))
}

// GitCheckoutCommit calls x.ai/git/checkout_commit with the wire keys
// {gitRoot?, commit, stashIfDirty?}. sessionId is omitted. commit is
// required. The agent returns a raw (non-envelope) response, which
// UnwrapExtResult passes through unchanged.
func (b *Bridge) GitCheckoutCommit(ctx context.Context, gitRoot, commit string, stashIfDirty bool) (map[string]any, error) {
	params := map[string]any{"commit": commit, "stashIfDirty": stashIfDirty}
	if gitRoot != "" {
		params["gitRoot"] = gitRoot
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/checkout_commit", params))
}

// GitCheckoutSessionHead calls x.ai/git/checkout_session_head with the
// wire keys {sessionId, gitRoot?, stashIfDirty?}. Unlike the other git
// methods, session_id is REQUIRED by the agent's request struct, so the
// wrapper passes "" and XaiCall fills in the active session id (HTTPError
// 404 when no session is active). The agent returns a raw (non-envelope)
// response, passed through unchanged.
func (b *Bridge) GitCheckoutSessionHead(ctx context.Context, gitRoot string, stashIfDirty bool) (map[string]any, error) {
	params := map[string]any{"sessionId": "", "stashIfDirty": stashIfDirty}
	if gitRoot != "" {
		params["gitRoot"] = gitRoot
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/checkout_session_head", params))
}

// GitBranches calls x.ai/git/branches with the wire keys {gitRoot?}.
// sessionId is omitted.
func (b *Bridge) GitBranches(ctx context.Context, gitRoot string) (map[string]any, error) {
	params := map[string]any{}
	if gitRoot != "" {
		params["gitRoot"] = gitRoot
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/branches", params))
}

// GitCurrentCommit calls x.ai/git/current_commit with the wire keys
// {gitRoot?}. sessionId is omitted.
func (b *Bridge) GitCurrentCommit(ctx context.Context, gitRoot string) (map[string]any, error) {
	params := map[string]any{}
	if gitRoot != "" {
		params["gitRoot"] = gitRoot
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/current_commit", params))
}

// GitRepoRoot calls x.ai/git/git_repo_root with the wire key
// {currentWorkingDirectory} — the agent's GitRepoRequest is camelCase but
// has NO sessionId/gitRoot fields. The response is a raw GitRepoResponse
// (NotGitRepo | {gitRoot}) and passes through UnwrapExtResult unchanged.
func (b *Bridge) GitRepoRoot(ctx context.Context, currentWorkingDirectory string) (map[string]any, error) {
	return xaiResult(b.XaiCall(ctx, "x.ai/git/git_repo_root", map[string]any{
		"currentWorkingDirectory": currentWorkingDirectory,
	}))
}

// GitSerializeChanges calls x.ai/git/serialize_changes with no params.
// The agent's handler unconditionally rejects this method in the current
// build ("git serialize_changes is unavailable in this build"); the
// wrapper is provided for wire parity and forwards the error as-is.
func (b *Bridge) GitSerializeChanges(ctx context.Context) (map[string]any, error) {
	return xaiResult(b.XaiCall(ctx, "x.ai/git/serialize_changes", map[string]any{}))
}

// ── worktree (x.ai/git/worktree/*) ────────────────────────────────────
//
// The worktree request structs (xai-grok-workspace-types rpc/worktree.rs)
// are camelCase. Only create / apply / resume_session require session_id
// on the wire (plain String fields); the rest carry no session at all.

// WorktreeCreateOptions holds the optional x.ai/git/worktree/create
// params. Strings are omitted when "" so grok applies its defaults
// (copyMode → "dirty", worktreeType → agent config default, auto label).
type WorktreeCreateOptions struct {
	WorktreePath            string
	CopyMode                string // "clean" | "dirty"
	GitRef                  string
	CopyIgnoredInBackground bool
	IgnoredSkipPatterns     []string
	WorktreeType            string // "linked" | "standalone" | "git"
	Label                   string
}

// WorktreeCreate calls x.ai/git/worktree/create with the wire keys
// {sessionId, sourcePath, worktreePath?, copyMode?, gitRef?,
// copyIgnoredInBackground?, ignoredSkipPatterns?, worktreeType?, label?}.
// session_id is REQUIRED (plain String in the agent's struct) → "" is
// passed so XaiCall fills the active session id (404 when none).
func (b *Bridge) WorktreeCreate(ctx context.Context, sourcePath string, opts WorktreeCreateOptions) (map[string]any, error) {
	params := map[string]any{"sessionId": "", "sourcePath": sourcePath}
	if opts.WorktreePath != "" {
		params["worktreePath"] = opts.WorktreePath
	}
	if opts.CopyMode != "" {
		params["copyMode"] = opts.CopyMode
	}
	if opts.GitRef != "" {
		params["gitRef"] = opts.GitRef
	}
	if opts.CopyIgnoredInBackground {
		params["copyIgnoredInBackground"] = true
	}
	if len(opts.IgnoredSkipPatterns) > 0 {
		params["ignoredSkipPatterns"] = opts.IgnoredSkipPatterns
	}
	if opts.WorktreeType != "" {
		params["worktreeType"] = opts.WorktreeType
	}
	if opts.Label != "" {
		params["label"] = opts.Label
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/worktree/create", params))
}

// WorktreeForkOptions holds the optional params shared by the
// create_from_worktree variants. Strings are omitted when "" (grok
// defaults: copyMode → "dirty", worktreeType → agent config default).
type WorktreeForkOptions struct {
	CopyMode     string // "clean" | "dirty"
	GitRef       string
	WorktreeType string // "linked" | "standalone" | "git"
	Label        string
}

// WorktreeCreateFromWorktree calls x.ai/git/worktree/create_from_worktree
// with the wire keys {sourceWorktreePath, newSessionId, copyMode?, gitRef?,
// worktreeType?, label?}. The agent's struct has NO session_id field —
// the caller supplies the optimistic newSessionId directly.
func (b *Bridge) WorktreeCreateFromWorktree(ctx context.Context, sourceWorktreePath, newSessionID string, opts WorktreeForkOptions) (map[string]any, error) {
	params := map[string]any{
		"sourceWorktreePath": sourceWorktreePath,
		"newSessionId":       newSessionID,
	}
	if opts.CopyMode != "" {
		params["copyMode"] = opts.CopyMode
	}
	if opts.GitRef != "" {
		params["gitRef"] = opts.GitRef
	}
	if opts.WorktreeType != "" {
		params["worktreeType"] = opts.WorktreeType
	}
	if opts.Label != "" {
		params["label"] = opts.Label
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/worktree/create_from_worktree", params))
}

// WorktreeCreateFromWorktreeSync calls
// x.ai/git/worktree/create_from_worktree_sync with the same wire keys as
// WorktreeCreateFromWorktree {sourceWorktreePath, newSessionId, copyMode?,
// gitRef?, worktreeType?, label?} — the synchronous variant blocks until
// the worktree is fully created. No session_id field on the wire.
func (b *Bridge) WorktreeCreateFromWorktreeSync(ctx context.Context, sourceWorktreePath, newSessionID string, opts WorktreeForkOptions) (map[string]any, error) {
	params := map[string]any{
		"sourceWorktreePath": sourceWorktreePath,
		"newSessionId":       newSessionID,
	}
	if opts.CopyMode != "" {
		params["copyMode"] = opts.CopyMode
	}
	if opts.GitRef != "" {
		params["gitRef"] = opts.GitRef
	}
	if opts.WorktreeType != "" {
		params["worktreeType"] = opts.WorktreeType
	}
	if opts.Label != "" {
		params["label"] = opts.Label
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/worktree/create_from_worktree_sync", params))
}

// WorktreeRemove calls x.ai/git/worktree/remove with the wire keys
// {worktreePath?, idOrPath?, force?, dryRun?}. The agent's struct has no
// session_id; exactly one of worktreePath / idOrPath must be non-empty
// (the agent rejects both-set or neither-set). "" → key omitted.
func (b *Bridge) WorktreeRemove(ctx context.Context, worktreePath, idOrPath string, force, dryRun bool) (map[string]any, error) {
	params := map[string]any{"force": force, "dryRun": dryRun}
	if worktreePath != "" {
		params["worktreePath"] = worktreePath
	}
	if idOrPath != "" {
		params["idOrPath"] = idOrPath
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/worktree/remove", params))
}

// WorktreeApply calls x.ai/git/worktree/apply with the wire keys
// {sessionId, worktreePath, mode?}. session_id is REQUIRED → "" so
// XaiCall fills the active session id (404 when none). mode "" → omitted
// → grok default "overwrite" (else "overwrite" | "merge").
func (b *Bridge) WorktreeApply(ctx context.Context, worktreePath, mode string) (map[string]any, error) {
	params := map[string]any{"sessionId": "", "worktreePath": worktreePath}
	if mode != "" {
		params["mode"] = mode
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/worktree/apply", params))
}

// WorktreeList calls x.ai/git/worktree/list with the wire keys {repo?,
// type?, includeAll?}. The agent's WorktreeListReq renames its vec field
// to `type` (lowercase, a JSON array of strings) — do not send `types`.
// No session_id on the wire. Empty types slice → key omitted.
func (b *Bridge) WorktreeList(ctx context.Context, repo string, types []string, includeAll bool) (map[string]any, error) {
	params := map[string]any{"includeAll": includeAll}
	if repo != "" {
		params["repo"] = repo
	}
	if len(types) > 0 {
		params["type"] = types
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/worktree/list", params))
}

// WorktreeShow calls x.ai/git/worktree/show with the wire key {idOrPath}.
// No session_id on the wire.
func (b *Bridge) WorktreeShow(ctx context.Context, idOrPath string) (map[string]any, error) {
	return xaiResult(b.XaiCall(ctx, "x.ai/git/worktree/show", map[string]any{"idOrPath": idOrPath}))
}

// WorktreeGc calls x.ai/git/worktree/gc with the wire keys {dryRun?,
// maxAge?, force?}. No session_id on the wire. maxAge "" → omitted (a
// duration string like "7d", "24h", "30m", "60s").
func (b *Bridge) WorktreeGc(ctx context.Context, dryRun, force bool, maxAge string) (map[string]any, error) {
	params := map[string]any{"dryRun": dryRun, "force": force}
	if maxAge != "" {
		params["maxAge"] = maxAge
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/worktree/gc", params))
}

// WorktreeResumeOptions holds the optional x.ai/git/worktree/resume_session
// params. Strings are omitted when ""; RestoreCode nil → omitted.
type WorktreeResumeOptions struct {
	CopyMode     string // "clean" | "dirty"
	WorktreeType string // "linked" | "standalone" | "git"
	RestoreCode  *bool
	GitRef       string
}

// WorktreeResumeSession calls x.ai/git/worktree/resume_session with the
// wire keys {sessionId, sourceCwd, copyMode?, worktreeType?, restoreCode?,
// gitRef?}. session_id is REQUIRED → "" so XaiCall fills the active
// session id (404 when none).
func (b *Bridge) WorktreeResumeSession(ctx context.Context, sourceCwd string, opts WorktreeResumeOptions) (map[string]any, error) {
	params := map[string]any{"sessionId": "", "sourceCwd": sourceCwd}
	if opts.CopyMode != "" {
		params["copyMode"] = opts.CopyMode
	}
	if opts.WorktreeType != "" {
		params["worktreeType"] = opts.WorktreeType
	}
	if opts.RestoreCode != nil {
		params["restoreCode"] = *opts.RestoreCode
	}
	if opts.GitRef != "" {
		params["gitRef"] = opts.GitRef
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/git/worktree/resume_session", params))
}

// WorktreeDbPath calls x.ai/git/worktree/db/path with no params (the
// agent's WorktreeDbPathReq is empty). Returns the unwrapped
// WorktreeDbPathResponse payload.
func (b *Bridge) WorktreeDbPath(ctx context.Context) (map[string]any, error) {
	return xaiResult(b.XaiCall(ctx, "x.ai/git/worktree/db/path", map[string]any{}))
}

// WorktreeDbStats calls x.ai/git/worktree/db/stats with no params (empty
// request struct).
func (b *Bridge) WorktreeDbStats(ctx context.Context) (map[string]any, error) {
	return xaiResult(b.XaiCall(ctx, "x.ai/git/worktree/db/stats", map[string]any{}))
}

// WorktreeDbRebuild calls x.ai/git/worktree/db/rebuild with no params
// (empty request struct). Returns the unwrapped rebuild report
// ({discovered, registered, alreadyTracked}).
func (b *Bridge) WorktreeDbRebuild(ctx context.Context) (map[string]any, error) {
	return xaiResult(b.XaiCall(ctx, "x.ai/git/worktree/db/rebuild", map[string]any{}))
}

// ── search (x.ai/search/*) ────────────────────────────────────────────
//
// grok resolves the search root from `cwd` first, then `sessionId` — at
// least one is required. Per the host convention sessionId is omitted
// (optional on the wire), so callers must provide cwd.

// SearchFuzzyOpen calls x.ai/search/fuzzy/open with the wire keys {cwd?,
// root?, requestId?, hidden?}. sessionId is optional and omitted — grok
// requires cwd or sessionId, so pass cwd. "" strings are omitted.
func (b *Bridge) SearchFuzzyOpen(ctx context.Context, cwd, root, requestID string, hidden bool) (map[string]any, error) {
	params := map[string]any{"hidden": hidden}
	if cwd != "" {
		params["cwd"] = cwd
	}
	if root != "" {
		params["root"] = root
	}
	if requestID != "" {
		params["requestId"] = requestID
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/search/fuzzy/open", params))
}

// SearchFuzzyChange calls x.ai/search/fuzzy/change with the wire keys
// {searchId, query, dirsOnly?, limit?}. No sessionId on the wire. searchId
// and query are required; limit nil → omitted.
func (b *Bridge) SearchFuzzyChange(ctx context.Context, searchID, query string, dirsOnly bool, limit *int) (map[string]any, error) {
	params := map[string]any{"searchId": searchID, "query": query, "dirsOnly": dirsOnly}
	if limit != nil {
		params["limit"] = *limit
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/search/fuzzy/change", params))
}

// SearchFuzzyClose calls x.ai/search/fuzzy/close with the wire key
// {searchId}. No sessionId on the wire.
func (b *Bridge) SearchFuzzyClose(ctx context.Context, searchID string) (map[string]any, error) {
	return xaiResult(b.XaiCall(ctx, "x.ai/search/fuzzy/close", map[string]any{"searchId": searchID}))
}

// ContentSearchOptions holds the optional x.ai/search/content params.
// The agent's ContentSearchRequest #[serde(flatten)]s these into the TOP
// level of the request — there is no nested `params` object on the wire.
// Bool flags are always sent (false == grok default); nil pointers and
// empty glob slices are omitted.
type ContentSearchOptions struct {
	CaseInsensitive  bool
	WholeWord        bool
	IsRegex          bool
	IncludeGlobs     []string
	ExcludeGlobs     []string
	MaxFiles         *int
	MaxMatches       *int
	RespectGitignore *bool // nil → omitted → grok default true
}

// ContentSearch calls x.ai/search/content with the FLATTENED wire keys
// {pattern, caseInsensitive?, wholeWord?, isRegex?, includeGlobs?,
// excludeGlobs?, maxFiles?, maxMatches?, respectGitignore?} (the agent
// flattens ContentSearchRequestParams into the request top level — a
// nested `params` object would be ignored). sessionId is optional and
// omitted; grok requires cwd or sessionId, so pass cwd in the opts.
// pattern is required.
func (b *Bridge) ContentSearch(ctx context.Context, pattern string, opts ContentSearchOptions) (map[string]any, error) {
	params := map[string]any{
		"pattern":         pattern,
		"caseInsensitive": opts.CaseInsensitive,
		"wholeWord":       opts.WholeWord,
		"isRegex":         opts.IsRegex,
	}
	if len(opts.IncludeGlobs) > 0 {
		params["includeGlobs"] = opts.IncludeGlobs
	}
	if len(opts.ExcludeGlobs) > 0 {
		params["excludeGlobs"] = opts.ExcludeGlobs
	}
	if opts.MaxFiles != nil {
		params["maxFiles"] = *opts.MaxFiles
	}
	if opts.MaxMatches != nil {
		params["maxMatches"] = *opts.MaxMatches
	}
	if opts.RespectGitignore != nil {
		params["respectGitignore"] = *opts.RespectGitignore
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/search/content", params))
}

// ── terminal (x.ai/terminal/*) ────────────────────────────────────────
//
// The agent's terminal request structs are camelCase. create / output /
// wait_for_exit / release / background declare session_id as a required
// String → those pass "" so XaiCall fills the active session. kill
// declares sessionId optional → omitted. list and the pty/* methods carry
// no session at all (except pty/create's optional one, omitted).

// TerminalCreate calls x.ai/terminal/create with the wire keys {sessionId,
// command, args?, env?, cwd?, outputByteLimit?}. session_id is REQUIRED →
// "" so XaiCall fills the active session id (404 when none). env is a
// map converted to the agent's [{name, value}] array; cwd "" and nil
// outputByteLimit → omitted.
func (b *Bridge) TerminalCreate(ctx context.Context, command string, args []string, env map[string]string, cwd string, outputByteLimit *int) (map[string]any, error) {
	params := map[string]any{"sessionId": "", "command": command, "args": args}
	if env != nil {
		envVars := make([]map[string]any, 0, len(env))
		for name, value := range env {
			envVars = append(envVars, map[string]any{"name": name, "value": value})
		}
		params["env"] = envVars
	}
	if cwd != "" {
		params["cwd"] = cwd
	}
	if outputByteLimit != nil {
		params["outputByteLimit"] = *outputByteLimit
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/terminal/create", params))
}

// TerminalOutput calls x.ai/terminal/output with the wire keys {sessionId,
// terminalId}. session_id is REQUIRED → "" so XaiCall fills the active
// session id (404 when none).
func (b *Bridge) TerminalOutput(ctx context.Context, terminalID string) (map[string]any, error) {
	return xaiResult(b.XaiCall(ctx, "x.ai/terminal/output", map[string]any{
		"sessionId":  "",
		"terminalId": terminalID,
	}))
}

// TerminalWaitForExit calls x.ai/terminal/wait_for_exit with the wire keys
// {sessionId, terminalId}. session_id is REQUIRED → "" so XaiCall fills
// the active session id (404 when none).
func (b *Bridge) TerminalWaitForExit(ctx context.Context, terminalID string) (map[string]any, error) {
	return xaiResult(b.XaiCall(ctx, "x.ai/terminal/wait_for_exit", map[string]any{
		"sessionId":  "",
		"terminalId": terminalID,
	}))
}

// TerminalKill calls x.ai/terminal/kill with the wire key {terminalId}.
// sessionId is optional on the wire (the agent looks up piped terminals by
// id and ignores sessionId for PTYs) and omitted per convention.
func (b *Bridge) TerminalKill(ctx context.Context, terminalID string) (map[string]any, error) {
	return xaiResult(b.XaiCall(ctx, "x.ai/terminal/kill", map[string]any{"terminalId": terminalID}))
}

// TerminalRelease calls x.ai/terminal/release with the wire keys
// {sessionId, terminalId}. session_id is REQUIRED → "" so XaiCall fills
// the active session id (404 when none).
func (b *Bridge) TerminalRelease(ctx context.Context, terminalID string) (map[string]any, error) {
	return xaiResult(b.XaiCall(ctx, "x.ai/terminal/release", map[string]any{
		"sessionId":  "",
		"terminalId": terminalID,
	}))
}

// TerminalBackground calls x.ai/terminal/background with the wire keys
// {sessionId, terminalId}. session_id is REQUIRED → "" so XaiCall fills
// the active session id (404 when none).
func (b *Bridge) TerminalBackground(ctx context.Context, terminalID string) (map[string]any, error) {
	return xaiResult(b.XaiCall(ctx, "x.ai/terminal/background", map[string]any{
		"sessionId":  "",
		"terminalId": terminalID,
	}))
}

// TerminalList calls x.ai/terminal/list with NO params (the agent's list
// handler ignores the request body). Returns the unwrapped
// {terminals: [...]} payload.
func (b *Bridge) TerminalList(ctx context.Context) (map[string]any, error) {
	return xaiResult(b.XaiCall(ctx, "x.ai/terminal/list", map[string]any{}))
}

// TerminalPtyCreate calls x.ai/terminal/pty/create with the wire keys
// {shell?, cwd?, env?, rows?, cols?, name?}. sessionId is optional and
// omitted (the agent falls back to the session cwd only when cwd is
// absent). "" strings and nil rows/cols are omitted (agent defaults
// 24x80).
func (b *Bridge) TerminalPtyCreate(ctx context.Context, shell, cwd, name string, env map[string]string, rows, cols *int) (map[string]any, error) {
	params := map[string]any{}
	if shell != "" {
		params["shell"] = shell
	}
	if cwd != "" {
		params["cwd"] = cwd
	}
	if env != nil {
		envVars := make([]map[string]any, 0, len(env))
		for k, v := range env {
			envVars = append(envVars, map[string]any{"name": k, "value": v})
		}
		params["env"] = envVars
	}
	if rows != nil {
		params["rows"] = *rows
	}
	if cols != nil {
		params["cols"] = *cols
	}
	if name != "" {
		params["name"] = name
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/terminal/pty/create", params))
}

// TerminalPtyLoad calls x.ai/terminal/pty/load with the wire key
// {terminalId}. No sessionId on the wire.
func (b *Bridge) TerminalPtyLoad(ctx context.Context, terminalID string) (map[string]any, error) {
	return xaiResult(b.XaiCall(ctx, "x.ai/terminal/pty/load", map[string]any{"terminalId": terminalID}))
}

// TerminalPtyResize calls x.ai/terminal/pty/resize with the wire keys
// {terminalId, rows, cols} — rows and cols are required (u16) and always
// sent. No sessionId on the wire.
func (b *Bridge) TerminalPtyResize(ctx context.Context, terminalID string, rows, cols int) (map[string]any, error) {
	return xaiResult(b.XaiCall(ctx, "x.ai/terminal/pty/resize", map[string]any{
		"terminalId": terminalID,
		"rows":       rows,
		"cols":       cols,
	}))
}

// TerminalPtyInput sends x.ai/terminal/pty/input: {terminalId, data}
// (data is base64-encoded bytes) as a fire-and-forget NOTIFICATION —
// the agent handles it only in its ext_notification path, so a
// request-style call would fail with -32601 (mirrors TogglePlanMode).
// Returns a bare {"ok": true} without waiting for a reply.
func (b *Bridge) TerminalPtyInput(ctx context.Context, terminalID, data string) (map[string]any, error) {
	if err := b.Boot(ctx); err != nil {
		return nil, err
	}
	if err := b.write(map[string]any{
		"jsonrpc": "2.0",
		"method":  "_x.ai/terminal/pty/input",
		"params":  map[string]any{"terminalId": terminalID, "data": data},
	}); err != nil {
		return nil, err
	}
	return map[string]any{"ok": true}, nil
}

// ── code (x.ai/code/*) ────────────────────────────────────────────────
//
// The agent's code-nav eligibility check rejects requests without a
// session id (reason: sessionRequired) even when cwd is provided, so
// sessionId is treated as REQUIRED for all x.ai/code/* methods: the
// wrappers pass "" and XaiCall fills the active session id (404 when
// none). Wire keys are camelCase; cwd "" → omitted (falls back to the
// session cwd).

// CodeGotoDefinition calls x.ai/code/goto-definition with the wire keys
// {sessionId, cwd?, path, row, column}. sessionId is filled by XaiCall
// (required for code-nav eligibility); row/column are 1-indexed.
func (b *Bridge) CodeGotoDefinition(ctx context.Context, cwd, path string, row, column int) (map[string]any, error) {
	params := map[string]any{"sessionId": "", "path": path, "row": row, "column": column}
	if cwd != "" {
		params["cwd"] = cwd
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/code/goto-definition", params))
}

// CodeGotoReferences calls x.ai/code/goto-references with the wire keys
// {sessionId, cwd?, path, row, column}. sessionId is filled by XaiCall
// (required for code-nav eligibility); row/column are 1-indexed.
func (b *Bridge) CodeGotoReferences(ctx context.Context, cwd, path string, row, column int) (map[string]any, error) {
	params := map[string]any{"sessionId": "", "path": path, "row": row, "column": column}
	if cwd != "" {
		params["cwd"] = cwd
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/code/goto-references", params))
}

// CodeFindDefinitions calls x.ai/code/find-definitions with the wire keys
// {sessionId, cwd?, symbol, contextPath?}. sessionId is filled by XaiCall
// (required for code-nav eligibility); contextPath "" → omitted.
func (b *Bridge) CodeFindDefinitions(ctx context.Context, cwd, symbol, contextPath string) (map[string]any, error) {
	params := map[string]any{"sessionId": "", "symbol": symbol}
	if cwd != "" {
		params["cwd"] = cwd
	}
	if contextPath != "" {
		params["contextPath"] = contextPath
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/code/find-definitions", params))
}

// CodeFindReferences calls x.ai/code/find-references with the wire keys
// {sessionId, cwd?, symbol, contextPath?}. sessionId is filled by XaiCall
// (required for code-nav eligibility); contextPath "" → omitted.
func (b *Bridge) CodeFindReferences(ctx context.Context, cwd, symbol, contextPath string) (map[string]any, error) {
	params := map[string]any{"sessionId": "", "symbol": symbol}
	if cwd != "" {
		params["cwd"] = cwd
	}
	if contextPath != "" {
		params["contextPath"] = contextPath
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/code/find-references", params))
}

// CodeStatus calls x.ai/code/status with the wire keys {sessionId, cwd?}.
// sessionId is filled by XaiCall (required for code-nav eligibility).
func (b *Bridge) CodeStatus(ctx context.Context, cwd string) (map[string]any, error) {
	params := map[string]any{"sessionId": ""}
	if cwd != "" {
		params["cwd"] = cwd
	}
	return xaiResult(b.XaiCall(ctx, "x.ai/code/status", params))
}
