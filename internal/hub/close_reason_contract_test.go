package hub

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/quic-go/quic-go"
)

// TestCloseReasonContract pins the host's copy of the hub's WebSocket close
// reasons. The strings MUST match acp-hub cmd/capri-hub/main.go's reason
// constants VERBATIM (reasonAuth / reasonSuperseded / reasonShutdown /
// reasonLiveness / reasonProtocol) — the two modules are separate binaries with no shared
// package, and a drift is silent until a real credential rotation: a typo
// turns "auth" into an ordinary drop and the host never re-pairs.
//
// The host only names the reasons it switches on; a hub reason it does not
// know (e.g. "uplink idle timeout") deliberately falls into closeOther.
//
// The second half scans client.go for closeReason* constants, so ADDING a
// reason here without teaching this test fails too — otherwise the new
// string would sit outside the contract with nothing checking it.
func TestCloseReasonContract(t *testing.T) {
	want := map[string]string{
		"closeReasonAuth":       "auth",
		"closeReasonSuperseded": "superseded",
		"closeReasonShutdown":   "shutdown",
		"closeReasonLiveness":   "liveness probe timeout",
		"closeReasonProtocol":   "protocol version",
	}
	got := map[string]string{
		"closeReasonAuth":       closeReasonAuth,
		"closeReasonSuperseded": closeReasonSuperseded,
		"closeReasonShutdown":   closeReasonShutdown,
		"closeReasonLiveness":   closeReasonLiveness,
		"closeReasonProtocol":   closeReasonProtocol,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("close reason constants drifted from acp-hub's reason table:\n got  %#v\n want %#v", got, want)
	}

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	fset := token.NewFileSet()
	af, err := parser.ParseFile(fset, filepath.Join(filepath.Dir(file), "client.go"), nil, 0)
	if err != nil {
		t.Fatalf("parse client.go: %v", err)
	}
	found := map[string]string{}
	ast.Inspect(af, func(n ast.Node) bool {
		gd, ok := n.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			return true
		}
		for _, spec := range gd.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range vs.Names {
				if !strings.HasPrefix(name.Name, "closeReason") || i >= len(vs.Values) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				s, err := strconv.Unquote(lit.Value)
				if err != nil {
					continue
				}
				found[name.Name] = s
			}
		}
		return true
	})
	if !reflect.DeepEqual(found, want) {
		t.Fatalf("client.go closeReason* constants = %#v, want exactly %#v (update this test together with acp-hub's reason table)", found, want)
	}
}

// TestQUICErrorCodeContract pins the host's copy of the hub's QUIC
// application error codes. QUIC has no close-reason frame, so the code is the
// whole verdict: if "superseded" drifts, the old process backs off and
// re-acquires the same hostId from the new one every 10s.
func TestQUICErrorCodeContract(t *testing.T) {
	want := map[string]uint64{
		"quicCodeSuperseded": 2,
		"quicCodeAuth":       3,
		"quicCodeProtocol":   4,
		"quicCodeLiveness":   5,
		"quicCodeUplinkIdle": 6,
		"quicCodePingWrite":  7,
		"quicCodeShutdown":   8,
		"quicCodeUnreadable": 9,
	}
	got := map[string]uint64{
		"quicCodeSuperseded": uint64(quicCodeSuperseded),
		"quicCodeAuth":       uint64(quicCodeAuth),
		"quicCodeProtocol":   uint64(quicCodeProtocol),
		"quicCodeLiveness":   uint64(quicCodeLiveness),
		"quicCodeUplinkIdle": uint64(quicCodeUplinkIdle),
		"quicCodePingWrite":  uint64(quicCodePingWrite),
		"quicCodeShutdown":   uint64(quicCodeShutdown),
		"quicCodeUnreadable": uint64(quicCodeUnreadable),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("QUIC error codes drifted from acp-hub's errCode table:\n got  %#v\n want %#v", got, want)
	}
}

// A QUIC session end must reach the same verdict a WebSocket close reason
// would: that equivalence is the whole point of the code table.
func TestClassifyCloseQUICCodes(t *testing.T) {
	cases := []struct {
		code quic.ApplicationErrorCode
		want closeKind
	}{
		{quicCodeSuperseded, closeSuperseded},
		{quicCodeAuth, closeAuth},
		{quicCodeProtocol, closeProtocol},
		{quicCodeLiveness, closeLiveness},
		{quicCodeShutdown, closeShutdown},
		// Ordinary drops: same recovery as any other redial.
		{quicCodeUplinkIdle, closeOther},
		{quicCodePingWrite, closeOther},
		{quicCodeUnreadable, closeOther},
		// 0 is a routine close, never an auth signal.
		{0, closeOther},
	}
	for _, c := range cases {
		err := &quic.ApplicationError{ErrorCode: c.code}
		if got := classifyClose(err); got != c.want {
			t.Errorf("code %d classified as %v, want %v", c.code, got, c.want)
		}
		if c.want == closeAuth {
			if !isAuthErr(err) {
				t.Errorf("code %d must be recognised as an auth failure", c.code)
			}
		} else if isAuthErr(err) {
			t.Errorf("code %d must not be treated as auth", c.code)
		}
	}
}
