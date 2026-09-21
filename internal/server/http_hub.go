package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/AgentsHarness/capri-host/internal/config"
	"github.com/AgentsHarness/capri-host/internal/hubstate"
)

// HubController is the slice of the hub client this API needs. Declaring it as
// an interface rather than taking *hub.Manager keeps the handlers testable with
// a stub and documents exactly how much of the hub layer the HTTP layer may
// touch.
type HubController interface {
	// State returns a snapshot of the hub link.
	State() hubstate.State
	// PairWith points the host at hubURL (empty = keep the current one) and
	// exchanges code for a token, adopting it live.
	PairWith(ctx context.Context, hubURL, code string) error
	// Reuse points the host at hubURL using the credential already on disk,
	// with no pairing code. It fails when no usable credential exists.
	Reuse(hubURL string) error
	// Disconnect clears the configured hub and returns to local-only mode.
	Disconnect() error
	// Rename changes this host's display name everywhere it appears: the
	// hub registry, the running bridge, and the settings file.
	Rename(ctx context.Context, newName string) error
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
func (s *Server) hubSnapshot() hubstate.State {
	if ctl := s.hubController(); ctl != nil {
		return ctl.State()
	}
	token := hubstate.ReadToken(config.HubStatePath())
	st := hubstate.State{
		Configured: s.cfg.HubURL != "",
		HubURL:     s.cfg.HubURL,
		HostID:     s.cfg.HostID,
		HostName:   s.cfg.HostName,
	}
	if token != nil {
		st.StoredURL = token.URLOrEmpty()
		checkURL := st.HubURL
		if checkURL == "" {
			checkURL = st.StoredURL
		}
		st.CanReuse = token.Usable(checkURL)
	}
	return st
}

// handleHubState answers GET /api/hub/state.
func (s *Server) handleHubState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "hub": s.hubSnapshot()})
}

type hubPairBody struct {
	Code string `json:"code"`
	// HubURL is optional. Supplying it points the host at that hub, which is
	// what lets the tray (and a localhost FE) complete first-time setup on a
	// host that was started with no hub configured. Honoured only from a
	// local origin — see handleHubPair.
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

	// hubUrl retargets this host. Only a local origin (the tray, curl, the
	// embedded FE on localhost) may do that. A hub FE is a trusted origin
	// for re-pairing the current hub, but must not point us at another one.
	hubURL := body.HubURL
	if !isLocalOrigin(r) {
		hubURL = ""
	}

	if err := ctl.PairWith(ctx, hubURL, body.Code); err != nil {
		status := http.StatusBadGateway
		if errors.Is(err, hubstate.ErrBadPairCode) {
			status = http.StatusBadRequest
		}
		writeJSON(w, status, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "hub": ctl.State()})
}

type hostRenameBody struct {
	Name string `json:"name"`
}

// hostRenameTimeout bounds one rename attempt against the hub. Same reasoning
// as hubPairTimeout: the client's http.Client carries a 50-minute timeout
// sized for relayed prompts, which an unreachable hub would sit behind.
const hostRenameTimeout = 15 * time.Second

// handleHostRename answers POST /api/host/rename.
//
// This exists because the display name is not a client-side property: the
// bridge stamps it on every event frame and grok message, and the hub keys
// its registry by it, so only the process that owns all three can change it.
// A supervisor process (the Windows tray, Capri.app) has no other way in.
//
// Manager.Rename applies the name locally — client display name, bridge and
// settings file — and treats the hub half as best-effort, so a disconnected
// host is still renameable. The response is therefore OK even when the hub
// call failed. That failure is logged and does not replace the session error.
func (s *Server) handleHostRename(w http.ResponseWriter, r *http.Request) {
	var body hostRenameBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "需要 name"})
		return
	}
	// Validate and forward the SAME value. Trimming in two places would let a
	// whitespace-only name pass the check here and reach a controller that
	// never trims it.
	name := strings.TrimSpace(body.Name)
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "需要 name"})
		return
	}
	ctl := s.hubController()
	if ctl == nil {
		writeJSON(w, http.StatusConflict, map[string]any{
			"ok": false, "error": "本机未启用 hub 客户端",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), hostRenameTimeout)
	defer cancel()

	if err := ctl.Rename(ctx, name); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "hub": ctl.State()})
}

type hubReuseBody struct {
	HubURL string `json:"hubUrl"`
}

// handleHubReuse answers POST /api/hub/reuse: point the host at a hub it has
// already paired with, using the credential from hub.json.
//
// The counterpart to /api/hub/pair for the case that does not need a code.
// Without it, a supervisor whose user re-selects the hub they already use has
// to send them to the hub for a fresh 6-digit code — which expires in 15
// minutes and proves nothing, since the machine is already trusted there.
//
// hubUrl retargets this host. Only a local origin may do that, same as
// pairing: a page on the current hub must not point us at another one.
func (s *Server) handleHubReuse(w http.ResponseWriter, r *http.Request) {
	var body hubReuseBody
	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "需要 hubUrl"})
		return
	}
	ctl := s.hubController()
	if ctl == nil {
		writeJSON(w, http.StatusConflict, map[string]any{
			"ok": false, "error": "本机未启用 hub 客户端",
		})
		return
	}

	hubURL := body.HubURL
	if !isLocalOrigin(r) {
		hubURL = ""
	}
	if err := ctl.Reuse(hubURL); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "hub": ctl.State()})
}

// handleHubDisconnect answers POST /api/hub/disconnect: switch the host
// to local-only mode, closing any active connection to the hub.
func (s *Server) handleHubDisconnect(w http.ResponseWriter, r *http.Request) {
	ctl := s.hubController()
	if ctl == nil {
		writeJSON(w, http.StatusConflict, map[string]any{
			"ok": false, "error": "本机未启用 hub 客户端",
		})
		return
	}

	if err := ctl.Disconnect(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "hub": ctl.State()})
}

// SetQuitFunc injects the orderly-shutdown trigger, the same way
// SetHubController injects the hub client.
//
// A supervisor process has no other way to stop this host cleanly: Windows
// does not deliver SIGTERM to a console child, so the alternatives are a hard
// kill — which orphans the grok process this host owns, because killing a
// parent does not kill its children there — or a Job Object. Asking over HTTP
// keeps shutdown on the one path this process already implements
// (signal.NotifyContext in main), so Ctrl+C, launchd's SIGTERM and the tray's
// "退出" all unwind through the same code.
func (s *Server) SetQuitFunc(fn func()) {
	s.hubMu.Lock()
	s.quitFn = fn
	s.hubMu.Unlock()
}

// handleHostQuit answers POST /api/host/quit. It answers first and stops
// afterwards, so the caller sees a response instead of a dropped connection.
func (s *Server) handleHostQuit(w http.ResponseWriter, r *http.Request) {
	s.hubMu.Lock()
	fn := s.quitFn
	s.hubMu.Unlock()
	if fn == nil {
		writeJSON(w, http.StatusConflict, map[string]any{
			"ok": false, "error": "本机未启用退出控制",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	go fn()
}
