package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
)

// This file writes the starter config.json.
//
// Capri.app does not need it: it has a form for these settings, so the file is
// an implementation detail there. The Windows tray hands the user a text
// editor, and a file containing only {"port": 8765} answers none of "what else
// can this host be told?" — so the file the tray opens has to teach.

// StarterFile is the content of a brand-new config.json: the values the host
// will actually run with, every key spelled out.
//
// Keys whose correct answer is "let the host decide" are deliberately left
// empty rather than pre-filled. Pinning grok_bin to whatever was discovered
// today would stop discovery from following a later reinstall, and an empty
// value there is an instruction to keep looking — which is the truth.
func StarterFile() File {
	on, off := true, false
	return File{
		// Loopback, spelled out. It is the value already in force, and seeing
		// it is how a user learns that binding wider is something to ask for
		// (along with a FE_TOKEN — see CheckBindPolicy).
		Bind: "127.0.0.1",
		Port: 8765,
		// Both are the compiled defaults, written so the choice is visible. A
		// compact file that omitted them would leave the user guessing whether
		// the feature exists at all.
		StartHostOnLaunch: &on,
		StartAtLogin:      &off,
		KeepAwake:         &off,
	}
}

// WriteStarter creates config.json when it does not exist yet, reporting
// whether it created one. An existing file is never touched: it belongs to the
// user, and possibly to Capri.app.
func WriteStarter() (created bool, err error) {
	if _, statErr := os.Stat(Path()); statErr == nil {
		return false, nil
	} else if !os.IsNotExist(statErr) {
		return false, statErr
	}

	b, err := marshalAllKeys(StarterFile())
	if err != nil {
		return false, err
	}
	if err := os.MkdirAll(Dir(), 0o755); err != nil {
		return false, err
	}
	if err := os.WriteFile(Path(), b, 0o600); err != nil {
		return false, err
	}
	return true, nil
}

// marshalAllKeys renders f as JSON with EVERY tagged field present, including
// the empty ones that omitempty drops everywhere else.
//
// By reflection rather than a hand-written twin struct: a parallel definition
// would silently stop covering a key the day somebody adds one to File, and
// the starter is precisely the place that must not fall behind. It also means
// the field order in the file follows the declaration order in File, which is
// already grouped by concern.
func marshalAllKeys(f File) ([]byte, error) {
	t := reflect.TypeOf(f)
	v := reflect.ValueOf(f)

	var b bytes.Buffer
	b.WriteString("{\n")
	written := 0
	for i := 0; i < t.NumField(); i++ {
		name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ",")
		if name == "" || name == "-" || !v.Field(i).CanInterface() {
			continue
		}
		raw, err := json.Marshal(v.Field(i).Interface())
		if err != nil {
			return nil, fmt.Errorf("序列化 %s: %w", name, err)
		}
		if written > 0 {
			b.WriteString(",\n")
		}
		written++
		fmt.Fprintf(&b, "  %q: %s", name, raw)
	}
	b.WriteString("\n}\n")
	return b.Bytes(), nil
}
