package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/AgentsHarness/capri-host/internal/hub"
)

// HubController is the slice of the hub client this API needs. Declaring it as
// an interface rather than taking *hub.Manager keeps the handlers testable with
// a stub and documents exactly how much of the hub layer the HTTP layer may
// touch.
type HubController interface {
	// State returns a snapshot of the hub link.
	State() hub.State
	// PairWith points the host at hubURL (empty = keep the current one) and
	// exchanges code for a token, adopting it live.
	PairWith(ctx context.Context, hubURL, code string) error
}

// SetHubController injects the live hub client after New has returned.
//
// Deliberately not a New parameter: six test call sites pass exactly
// (config.Config, *acp.Bridge), and most of the ~30 test files reach the
// server through shared harnesses that call it. Widening the constructor to
// carry something only main can supply would churn all of them for no gain,
// and a nil controller is a meaningful state anyway — it is what local mode
// looks like.
func (s *Server) SetHubController(h HubController) {
	s.hubMu.Lock()
	s.hubCtl = h
	s.hubMu.Unlock()
}

func (s *Server) hubController() HubController {
	s.hubMu.Lock()
	defer s.hubMu.Unlock()
	return s.hubCtl
}

// hubSnapshot reports the hub link, falling back to a configuration-only view
// when no controller is attached. The fallback is not merely the local-mode
// case: it is also the brief window during startup after the server is
// listening but before main has injected the client, and answering "configured
// but not yet paired" there is truthful where an error would not be.
func (s *Server) hubSnapshot() hub.State {
	if ctl := s.hubController(); ctl != nil {
		return ctl.State()
	}
	return hub.State{
		Configured: s.cfg.HubURL != "",
		HubURL:     s.cfg.HubURL,
		HostID:     s.cfg.HostID,
		HostName:   s.cfg.HostName,
	}
}

// handleHubState answers GET /api/hub/state.
func (s *Server) handleHubState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "hub": s.hubSnapshot()})
}

type hubPairBody struct {
	Code string `json:"code"`
	// HubURL is optional. Supplying it points the host at that hub, which is
	// what lets a browser complete first-time setup on a host that was started
	// with no hub configured at all — the same thing the tray dialog does.
	HubURL string `json:"hubUrl"`
}

// hubPairTimeout bounds one pairing attempt. The client's own http.Client
// carries a 50-minute timeout sized for relayed prompts, which would leave a
// pairing request against an unreachable hub hanging effectively forever.
const hubPairTimeout = 20 * time.Second

// handleHubPair answers POST /api/hub/pair.
func (s *Server) handleHubPair(w http.ResponseWriter, r *http.Request) {
	var body hubPairBody
	if err := readJSON(r, &body); err != nil || strings.TrimSpace(body.Code) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "需要 code"})
		return
	}
	ctl := s.hubController()
	if ctl == nil {
		writeJSON(w, http.StatusConflict, map[string]any{
			"ok": false, "error": "本机未启用 hub 客户端",
		})
		return
	}

	// Detached from the request context on purpose. If the browser goes away
	// mid-flight, a cancel could abort us AFTER the hub has already issued a
	// token — leaving the hub holding a pairing this host never recorded. A
	// bounded, uncancellable attempt keeps both sides in agreement.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), hubPairTimeout)
	defer cancel()

	if err := ctl.PairWith(ctx, body.HubURL, body.Code); err != nil {
		status := http.StatusBadGateway
		if errors.Is(err, hub.ErrBadPairCode) {
			status = http.StatusBadRequest
		}
		writeJSON(w, status, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "hub": ctl.State()})
}
