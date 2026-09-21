// This file resolves where the engine lives, relative to the GUI.
//
// Untagged so `go test ./...` can pin the shipped layout on any machine. The
// layout itself is produced by packaging/windows/make-exes.sh, and the two
// drifting apart would ship a folder whose GUI cannot find its engine — a
// failure that only shows up on a user's machine, as "找不到 Capri-host.exe".

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// engineCandidates lists where the engine may be, best first.
//
// bin/ is the shipped place: the folder the user unzips should have one
// obvious thing to run, not two programs to choose between. The flattened
// layout stays supported so anyone who drags the engine out of bin/ beside the
// GUI keeps working rather than getting a "not found".
func engineCandidates(dir string) []string {
	return []string{
		filepath.Join(dir, "bin", "Capri-host.exe"),
		filepath.Join(dir, "Capri-host.exe"),
		filepath.Join(dir, "bin", "Capri-host"),
		filepath.Join(dir, "Capri-host"),
	}
}

// resolveEngine picks the engine. envBin is CAPRI_HOST_BIN, which overrides
// everything — that is the escape hatch for running the GUI against a build
// tree or a differently-placed engine.
//
// stat is injected so the search can be exercised without a real filesystem.
func resolveEngine(envBin, dir string, stat func(string) (os.FileInfo, error)) (string, error) {
	if env := strings.TrimSpace(envBin); env != "" {
		if st, err := stat(env); err == nil && !st.IsDir() {
			return env, nil
		}
		return "", fmt.Errorf("CAPRI_HOST_BIN=%s 不存在", env)
	}
	for _, c := range engineCandidates(dir) {
		if st, err := stat(c); err == nil && !st.IsDir() {
			return c, nil
		}
	}
	return "", fmt.Errorf("找不到 Capri-host.exe（应在 %s 里）", filepath.Join(dir, "bin"))
}
