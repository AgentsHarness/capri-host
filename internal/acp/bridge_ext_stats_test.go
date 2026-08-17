package acp

import "testing"

func TestStreamGenWindow(t *testing.T) {
	// 单包：末包 − streamStart。
	if g := streamGenWindow(2100, 2100, 2000); g != 100 {
		t.Errorf("single chunk = %d, want 100", g)
	}
	// Grok 2ms 尾巴：末包 − streamStart，避免分母只剩 2ms。
	if g := streamGenWindow(7507, 7509, 5441); g != 7509-5441 {
		t.Errorf("tiny tail = %d, want %d", g, 7509-5441)
	}
	// 真流式也用末包 − streamStart（含首包生成，不再切 500ms）。
	if g := streamGenWindow(1500, 2500, 1000); g != 1500 {
		t.Errorf("streaming = %d, want 1500", g)
	}
	// 缺首包：仍是末包 − streamStart。
	if g := streamGenWindow(0, 2500, 1000); g != 1500 {
		t.Errorf("missing first = %d, want 1500", g)
	}
	// 缺 streamStart：回退首包 → 末包。
	if g := streamGenWindow(1500, 2500, 0); g != 1000 {
		t.Errorf("missing streamStart = %d, want 1000", g)
	}
	// 全缺：0。
	if g := streamGenWindow(0, 0, 1000); g != 0 {
		t.Errorf("empty = %d, want 0", g)
	}
}
