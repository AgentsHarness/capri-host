package acp

import (
	"context"
	"testing"
	"time"
)

// request(..., timeout<=0) 必须等价于「不设截止」。曾经写成
// context.WithTimeout(ctx, 0)：那会让 deadline 立刻过期，请求在写出后
// 第一轮 select 就报「超时」——memory flush / dream 这类时长由模型调用
// 决定的命令因此必然假失败。这里用一个迟到的回复证明它仍在等。
func TestRequestWithoutDeadlineWaitsForReply(t *testing.T) {
	ctx := context.Background()
	b, w := readyBridge()

	type callResult struct {
		res map[string]any
		err error
	}
	done := make(chan callResult, 1)
	go func() {
		res, err := b.request(ctx, "_x.ai/memory/flush", map[string]any{"session_id": "s1"}, 0)
		done <- callResult{res: res, err: err}
	}()

	// 迟于「零截止」必然失败的窗口，但远短于任何真实预算。
	time.Sleep(50 * time.Millisecond)
	select {
	case r := <-done:
		t.Fatalf("request returned before the reply: err=%v res=%v", r.err, r.res)
	default:
	}

	resolveNext(t, b, w, map[string]any{"flushed": true, "disposition": "flushed"})
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("request without deadline failed: %v", r.err)
		}
		if r.res["disposition"] != "flushed" {
			t.Fatalf("result = %v, want the agent's reply", r.res)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("request never returned after the reply landed")
	}
}

// 正数 timeout 仍然生效（记忆命令去掉截止不能顺带取消其它调用的预算）。
func TestRequestWithDeadlineStillExpires(t *testing.T) {
	ctx := context.Background()
	b, _ := readyBridge()
	start := time.Now()
	if _, err := b.request(ctx, "_x.ai/memory/list", map[string]any{"sessionId": "s1"}, 30*time.Millisecond); err == nil {
		t.Fatal("request with a 30ms deadline returned no error")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("deadline ignored: waited %s", elapsed)
	}
}
