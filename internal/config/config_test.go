package config

import "testing"

// 默认只听回环；BIND / HOST_BIND 显式改写。
func TestLoadBindAddr(t *testing.T) {
	t.Setenv("BIND", "")
	t.Setenv("HOST_BIND", "")
	if got := Load().BindAddr; got != DefaultBindAddr {
		t.Fatalf("default BindAddr = %q, want %q", got, DefaultBindAddr)
	}

	t.Setenv("BIND", "0.0.0.0")
	if got := Load().BindAddr; got != "0.0.0.0" {
		t.Fatalf("BIND=0.0.0.0 → BindAddr = %q", got)
	}

	t.Setenv("BIND", "")
	t.Setenv("HOST_BIND", "::1")
	cfg := Load()
	if cfg.BindAddr != "::1" {
		t.Fatalf("HOST_BIND alias → BindAddr = %q", cfg.BindAddr)
	}
	if !cfg.BindIsLoopback() {
		t.Error("::1 must count as loopback")
	}
}

func TestBindIsLoopback(t *testing.T) {
	cases := []struct {
		addr string
		want bool
	}{
		{"", true},          // 空 = Load() 的默认（回环）
		{"127.0.0.1", true}, //
		{"::1", true},       //
		{"localhost", true}, //
		{"0.0.0.0", false},  //
		{"::", false},       //
		{"192.168.1.3", false},
	}
	for _, tc := range cases {
		if got := (Config{BindAddr: tc.addr}).BindIsLoopback(); got != tc.want {
			t.Errorf("BindIsLoopback(%q) = %v, want %v", tc.addr, got, tc.want)
		}
	}
}

// 非回环 + 没有 FE_TOKEN = 把能驱动 agent 的 API 开到网段上，直接拒启动。
// 这是 host 侧对 hub 的 REQUIRE_FE_TOKEN 的等价物，只在非回环时要求。
func TestCheckBindPolicy(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"loopback open", Config{BindAddr: "127.0.0.1"}, false},
		{"loopback tokened", Config{BindAddr: "127.0.0.1", AccessToken: "k"}, false},
		{"all ifaces open", Config{BindAddr: "0.0.0.0"}, true},
		{"all ifaces tokened", Config{BindAddr: "0.0.0.0", AccessToken: "k"}, false},
		{"lan ip open", Config{BindAddr: "192.168.1.3"}, true},
		{"lan ip tokened", Config{BindAddr: "192.168.1.3", AccessToken: "k"}, false},
		{"whitespace token open", Config{BindAddr: "0.0.0.0", AccessToken: "  "}, true},
	}
	for _, tc := range cases {
		err := CheckBindPolicy(tc.cfg)
		if tc.wantErr && err == nil {
			t.Errorf("%s: want error, got nil", tc.name)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("%s: want nil, got %v", tc.name, err)
		}
	}
}
