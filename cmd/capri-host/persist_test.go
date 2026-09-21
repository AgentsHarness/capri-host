package main

import (
	"testing"

	"github.com/AgentsHarness/capri-host/internal/config"
)

func TestPersistHubChoiceDoesNotOverwriteExistingName(t *testing.T) {
	t.Setenv("CAPRI_HOME", t.TempDir())
	if err := config.SaveFile(config.File{HostName: "办公室"}); err != nil {
		t.Fatal(err)
	}

	fn := persistHubChoice(config.Config{HostID: "pc", HostName: "pc"})
	if err := fn("https://hub.example"); err != nil {
		t.Fatal(err)
	}

	f, err := config.LoadFile()
	if err != nil {
		t.Fatal(err)
	}
	if f.HubURL != "https://hub.example" {
		t.Errorf("HubURL = %q", f.HubURL)
	}
	if f.HostName != "办公室" {
		t.Errorf("HostName = %q, want 办公室 (Rename must not be clobbered)", f.HostName)
	}
	if f.HostID != "pc" {
		t.Errorf("HostID = %q, want the adopted id stamped into an empty key", f.HostID)
	}
}

func TestPersistHubChoiceStampsEmptyIdentity(t *testing.T) {
	t.Setenv("CAPRI_HOME", t.TempDir())

	fn := persistHubChoice(config.Config{HostID: "pc", HostName: "pc"})
	if err := fn("https://hub.example"); err != nil {
		t.Fatal(err)
	}

	f, err := config.LoadFile()
	if err != nil {
		t.Fatal(err)
	}
	if f.HostID != "pc" || f.HostName != "pc" || f.HubURL != "https://hub.example" {
		t.Errorf("file = %+v, want adopted identity and hub url", f)
	}
}
