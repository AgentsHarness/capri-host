//go:build windows

package acp

// lockCampaignsState is a no-op on Windows: the advisory flock(2) lock
// is unavailable there, and the lock is best-effort anyway (a lock
// failure still proceeds — same semantics as the Unix path).
func lockCampaignsState(path string) (release func()) {
	return func() {}
}
