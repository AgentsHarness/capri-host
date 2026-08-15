package acp

import "testing"

func TestStreamGenWindow(t *testing.T) {
	// 单包：只能用末包 − streamStart。
	if g := streamGenWindow(2100, 2100, 2000); g != 100 {
		t.Errorf("single chunk = %d, want 100", g)
	}
	// Grok 2ms 尾巴：仍用末包 − streamStart，避免分母只剩 2ms。
	if g := streamGenWindow(7507, 7509, 5441); g != 7509-5441 {
		t.Errorf("tiny tail = %d, want %d", g, 7509-5441)
	}
	// 真流式（尾巴 ≥500ms）：用首包 → 末包，不含等到首字。
	if g := streamGenWindow(1500, 2500, 1000); g != 1000 {
		t.Errorf("streaming tail = %d, want 1000", g)
	}
	// 缺首包：回退末包 − streamStart。
	if g := streamGenWindow(0, 2500, 1000); g != 1500 {
		t.Errorf("missing first = %d, want 1500", g)
	}
	// 全缺：0。
	if g := streamGenWindow(0, 0, 1000); g != 0 {
		t.Errorf("empty = %d, want 0", g)
	}
}
