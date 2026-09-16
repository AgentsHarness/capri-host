package acp

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestInitializeCapabilitiesUseACPMetadataKey(t *testing.T) {
	b, w := metaReadyBridge(t)
	b.ready = false
	b.cmd = &exec.Cmd{Process: &os.Process{Pid: 1}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.ensureBooted(ctx) }()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	var message map[string]any
	for message == nil {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("initialize was not written")
		case <-ticker.C:
			message = w.last()
		}
	}
	cancel()
	<-done
	caps := message["params"].(map[string]any)["clientCapabilities"].(map[string]any)
	if _, ok := caps["_meta"].(map[string]any); !ok {
		t.Fatalf("ACP capabilities missing _meta: %#v", caps)
	}
	if _, ok := caps["meta"]; ok {
		t.Fatal("legacy meta capability key remains")
	}
}

func TestAgentRequestMustNotResolveSameIDHostRequest(t *testing.T) {
	b, w := metaReadyBridge(t)
	pending := make(chan rpcResult, 1)
	b.pending.Store("7", pending)
	reqID := pushPermission(t, b, w, permissionParams(allowAlwaysOption()), 7)
	t.Cleanup(func() { _ = b.RespondPermissionWithMeta(reqID, "", true, nil, "") })
	if _, ok := b.pending.Load("7"); !ok {
		t.Fatal("agent permission request consumed unrelated host RPC with same numeric id")
	}
	if _, ok := b.clientReqs.Load(reqID); !ok {
		t.Fatal("permission was not forwarded")
	}
	b.onAgentMessage(map[string]any{"id": float64(7), "result": map[string]any{"stopReason": "end_turn"}})
	select {
	case res := <-pending:
		if res.result["stopReason"] != "end_turn" {
			t.Fatalf("wrong result: %+v", res)
		}
	default:
		t.Fatal("real response did not complete host RPC")
	}
}

func TestUnknownSessionCancelIsIsolated(t *testing.T) {
	b, w := metaReadyBridge(t)
	reqID := pushPermission(t, b, w, permissionParams(allowAlwaysOption()), 7)
	t.Cleanup(func() { _ = b.RespondPermissionWithMeta(reqID, "", true, nil, "") })
	b.CancelWithMeta("missing-session", nil)
	if _, ok := b.clientReqs.Load(reqID); !ok {
		t.Fatal("cancelling an unknown session removed another session's permission request")
	}
}

func TestProcessResetRetiresClientRequests(t *testing.T) {
	b, w := metaReadyBridge(t)
	reqID := pushPermission(t, b, w, permissionParams(allowAlwaysOption()), 7)
	t.Cleanup(func() { _ = b.RespondPermissionWithMeta(reqID, "", true, nil, "") })
	b.resetRoster("test-process-exit")
	if _, ok := b.clientReqs.Load(reqID); ok {
		t.Fatalf("request %s remains actionable after process reset", reqID)
	}
}
