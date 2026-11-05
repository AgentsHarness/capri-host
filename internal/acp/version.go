package acp

import "runtime/debug"

// EffectiveVersion returns the version this host should report externally.
// Version is stamped at build time via -ldflags; plain local builds leave it
// "dev", so fall back to the VCS metadata Go embeds and report the exact
// revision instead (dev-<shortsha>). Builds without VCS metadata (module zip,
// -buildvcs=false) keep the plain "dev" fallback.
func EffectiveVersion() string {
	info, _ := debug.ReadBuildInfo()
	return resolveVersion(Version, info)
}

func resolveVersion(stamped string, info *debug.BuildInfo) string {
	if stamped != "dev" || info == nil {
		return stamped
	}
	var rev string
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			rev = s.Value
		}
	}
	if rev == "" {
		return stamped
	}
	if len(rev) > 7 {
		rev = rev[:7]
	}
	return "dev-" + rev
}
