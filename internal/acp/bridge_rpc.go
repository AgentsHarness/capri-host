package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

type rpcResult struct {
	result map[string]any
	// raw carries a JSON-RPC result that is NOT a JSON object (e.g. the
	// bare array workspace_list_recent returns). requestRaw() returns it
	// verbatim; request() keeps the old behavior of coercing it to {}.
	raw any
	err error
}

// RPCError is a JSON-RPC error reply from the agent — the agent process
// is alive and answered (it parsed the request and rejected it), it just
// failed the call itself (e.g. the model API's 400 "Internal Error: …").
// Distinct from transport failures (timeout / write error / boot failure),
// which mean the agent may be wedged: callers keep the process for
// RPCError and only self-heal (kill + restore) on real transport failures.
type RPCError struct {
	Code int
	Msg  string
}

func (e *RPCError) Error() string { return e.Msg }

func (b *Bridge) write(msg map[string]any) error {
	b.mu.Lock()
	stdin := b.stdin
	b.mu.Unlock()
	return writeAgentMessage(stdin, msg)
}

func writeAgentMessage(stdin io.Writer, msg map[string]any) error {
	if stdin == nil {
		return errors.New("grok 进程未运行")
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = stdin.Write(data)
	return err
}

func (b *Bridge) respond(id any, result map[string]any) {
	_ = b.write(map[string]any{
		kJSONRPC: "2.0",
		kID:      id,
		kResult:  result,
	})
}

func (b *Bridge) respondError(id any, message string, code int) {
	_ = b.write(map[string]any{
		kJSONRPC: "2.0",
		kID:      id,
		kError: map[string]any{
			"code":    code,
			"message": message,
		},
	})
}

// request 是标准 JSON-RPC 请求：固定 timeout，不因 agent 活动而顺延。
// （曾有过按会话活跃度顺延 prompt 截止时刻的设计，2026-08-14 取消：
// 默认 agent 可靠，超时到点即报错，不做续命式的自愈。）
// timeout <= 0 表示不设截止（只跟 ctx 走），用于时长由模型调用决定的
// 命令（memory flush / dream，TUI 侧同样没有截止）。
// 返回的 result 按对象处理：非对象 result（如裸数组）保持旧行为被
// 规整为 {}。需要原样拿到任意 JSON 值的调用方用 requestRaw。
func (b *Bridge) request(ctx context.Context, method string, params map[string]any, timeout time.Duration) (map[string]any, error) {
	res, err := b.requestRaw(ctx, method, params, timeout)
	if err != nil {
		return nil, err
	}
	if m, ok := res.(map[string]any); ok {
		return m, nil
	}
	return map[string]any{}, nil
}

// requestRaw 与 request 相同，但 result 原样返回（任意 JSON 值：对象、
// 数组、标量、nil），不做对象规整。x.ai 扩展方法里唯一返回裸数组的
// workspace_list_recent 必须走这里，否则数组会被 request 吞成 {}。
func (b *Bridge) requestRaw(ctx context.Context, method string, params map[string]any, timeout time.Duration) (any, error) {
	id := b.nextAgentID.Add(1)
	key := idKey(id)
	ch := make(chan rpcResult, 1)
	// Agent replies with JSON numbers (float64); idKey normalizes both sides.
	b.pending.Store(key, ch)

	msg := map[string]any{
		kJSONRPC: "2.0",
		kID:      id,
		kMethod:  method,
		kParams:  params,
	}
	if err := b.write(msg); err != nil {
		b.pending.Delete(key)
		return nil, err
	}

	tctx, cancel := context.WithCancel(ctx)
	if timeout > 0 {
		tctx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	select {
	case <-tctx.Done():
		b.pending.Delete(key)
		if err := ctx.Err(); err != nil {
			// The caller's context went away (e.g. the browser disconnected
			// mid-turn). Surface the cause so callers can tell a dead client
			// apart from a dead agent instead of treating every failure as
			// "agent wedged".
			return nil, fmt.Errorf("%s: %w", method, err)
		}
		return nil, fmt.Errorf("%s 超时", method)
	case res := <-ch:
		if res.raw != nil {
			return res.raw, res.err
		}
		return res.result, res.err
	}
}

func (b *Bridge) failAllPending(err error) {
	b.pending.Range(func(key, value any) bool {
		b.pending.Delete(key)
		ch := value.(chan rpcResult)
		select {
		case ch <- rpcResult{err: err}:
		default:
		}
		return true
	})
}
