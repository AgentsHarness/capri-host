package hub

import (
	"errors"
	"net/http"
	"testing"

	"github.com/coder/websocket"
	"github.com/quic-go/quic-go"

	"github.com/AgentsHarness/capri-wire"
)

// TestCloseReasonTables: WS and QUIC must describe the same verdict, and both
// directions of each table must round-trip.
func TestCloseReasonTables(t *testing.T) {
	reasons := []wire.CloseReason{
		wire.CloseNormal, wire.CloseShutdown, wire.CloseSuperseded, wire.CloseAuth,
		wire.CloseProtocol, wire.CloseLiveness, wire.CloseOverflow, wire.CloseUnreadable,
	}
	for _, r := range reasons {
		code, reason := r.WS()
		got, ok := wire.CloseReasonFromWS(code, reason)
		if !ok || got != r {
			t.Errorf("WS table does not round-trip %s: (%d,%q) → %s ok=%v", r, code, reason, got, ok)
		}
		qc := r.QUICCode()
		if got, ok := wire.CloseReasonFromQUIC(qc); !ok || got != r {
			t.Errorf("QUIC table does not round-trip %s: %d → %s ok=%v", r, qc, got, ok)
		}
	}
	// The verdicts the reconnect loop switches on must stay distinct.
	seen := map[uint64]wire.CloseReason{}
	for _, r := range reasons {
		if prev, ok := seen[r.QUICCode()]; ok && prev != r {
			t.Errorf("%s and %s share QUIC code %d", prev, r, r.QUICCode())
		}
		seen[r.QUICCode()] = r
	}
}

// TestReconnectPolicy: the enum decides the reconnect path, never a string.
func TestReconnectPolicy(t *testing.T) {
	cases := []struct {
		name   string
		reason wire.CloseReason
		err    error
		want   wire.CloseReason
	}{
		{name: "superseded", reason: wire.CloseSuperseded, want: wire.CloseSuperseded},
		{name: "auth", reason: wire.CloseAuth, want: wire.CloseAuth},
		{name: "protocol", reason: wire.CloseProtocol, want: wire.CloseProtocol},
		{name: "liveness", reason: wire.CloseLiveness, want: wire.CloseLiveness},
		{name: "overflow", reason: wire.CloseOverflow, want: wire.CloseOverflow},
		{name: "peer close carried by the error", reason: wire.CloseShutdown,
			err: &peerClose{reason: wire.CloseShutdown}, want: wire.CloseShutdown},
		{name: "auth without a close frame", err: errNoToken, want: wire.CloseAuth},
		{name: "http 401 during the upgrade",
			err: &wsHandshakeError{status: http.StatusUnauthorized}, want: wire.CloseAuth},
		{name: "plain transport drop", err: errors.New("connection reset"), want: wire.CloseNormal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := reconnectPolicy(tc.reason, tc.err); got != tc.want {
				t.Fatalf("policy = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestTransportCloseMapping: both adapters turn the peer's verdict into the
// shared enum so Run never inspects a reason string or a raw code.
func TestTransportCloseMapping(t *testing.T) {
	for _, r := range []wire.CloseReason{wire.CloseSuperseded, wire.CloseAuth, wire.CloseProtocol, wire.CloseLiveness, wire.CloseOverflow} {
		code, reason := r.WS()
		err := wrapWSClose(websocket.CloseError{Code: websocket.StatusCode(code), Reason: reason})
		got, ok := closeReasonOf(err)
		if !ok || got != r {
			t.Errorf("ws close %s mapped to %s ok=%v", r, got, ok)
		}

		qerr := wrapQUICClose(&quic.ApplicationError{ErrorCode: quic.ApplicationErrorCode(r.QUICCode())})
		got, ok = closeReasonOf(qerr)
		if !ok || got != r {
			t.Errorf("quic close %s mapped to %s ok=%v", r, got, ok)
		}
	}
	if _, ok := closeReasonOf(errors.New("generic")); ok {
		t.Error("a generic error was given a protocol verdict")
	}
}
