package acp

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

// 背压回归：publish 的调用方是 agent stdout 读取协程（单协程），所以
// 「投递」绝不能再阻塞发布者。旧实现持锁阻塞投递关键事件，一个卡死的
// SSE 消费者就能把 agent 输出通路按停。
func TestPublishNeverBlocksOnWedgedSubscriber(t *testing.T) {
	b := NewBridge(GrokConfig{})
	ch, unsub := b.Subscribe()
	defer unsub()

	// 消费通道塞满且不再有人读（半开连接的最坏形态）。
	for i := 0; i < cap(ch); i++ {
		ch <- Event{"type": "filler"}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		// 关键事件 + 可丢事件各一：旧实现下前者会阻塞最多 5s。
		b.Broadcast(Event{"type": "done", "stopReason": "end_turn"})
		b.Broadcast(Event{"type": "chunk", "text": "x"})
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publish blocked on a wedged subscriber (agent stdout path would stall)")
	}
}

// 一个卡死的订阅者不得拖慢其他订阅者——旧实现里 5s/关键事件的阻塞会
// 让健康订阅者一起挨饿。
func TestSlowSubscriberDoesNotStarveOthers(t *testing.T) {
	b := NewBridge(GrokConfig{})
	wedged, unwedge := b.Subscribe()
	defer unwedge()
	for i := 0; i < cap(wedged); i++ {
		wedged <- Event{"type": "filler"}
	}

	healthy, unsub := b.Subscribe()
	defer unsub()

	const n = 200
	go func() {
		for i := 0; i < n; i++ {
			b.Broadcast(Event{"type": "tool_call", "text": strconv.Itoa(i)})
		}
	}()

	deadline := time.After(3 * time.Second)
	seen := 0
	for seen < n {
		select {
		case ev := <-healthy:
			if ev["type"] != "tool_call" {
				continue
			}
			if got, _ := ev["text"].(string); got != strconv.Itoa(seen) {
				t.Fatalf("order broken at %d: %v", seen, ev["text"])
			}
			seen++
		case <-deadline:
			t.Fatalf("healthy subscriber saw %d/%d events while another was wedged", seen, n)
		}
	}
}

// 队列路径必须保持 seq 顺序：通道满时事件转入环形队列，由 pump 按序补投
// （就地投递与队列混用时的乱序是这套背压机制最容易犯的错）。
func TestQueuedDeliveryPreservesOrder(t *testing.T) {
	b := NewBridge(GrokConfig{})
	ch, unsub := b.Subscribe()
	defer unsub()

	for i := 0; i < cap(ch); i++ {
		ch <- Event{"type": "filler"}
	}

	const n = 64
	for i := 0; i < n; i++ {
		b.Broadcast(Event{"type": "tool_call", "text": strconv.Itoa(i)})
	}

	seen := 0
	deadline := time.After(3 * time.Second)
	for seen < n {
		select {
		case ev := <-ch:
			if ev["type"] != "tool_call" {
				continue // filler 排空过程中
			}
			if got, _ := ev["text"].(string); got != strconv.Itoa(seen) {
				t.Fatalf("order broken at %d: %v", seen, ev["text"])
			}
			seen++
		case <-deadline:
			t.Fatalf("only %d/%d queued events delivered in order", seen, n)
		}
	}
}

// 关键事件不能被可丢积压挤掉：消费通道塞满 + 超量 chunk 之后发出的
// done，仍必须在消费者恢复读取时送达。
func TestCriticalEventSurvivesDroppableBacklog(t *testing.T) {
	b := NewBridge(GrokConfig{})
	ch, unsub := b.Subscribe()
	defer unsub()

	for i := 0; i < cap(ch); i++ {
		ch <- Event{"type": "filler"}
	}
	for i := 0; i < subQueueCap+64; i++ {
		b.Broadcast(Event{"type": "chunk", "text": fmt.Sprintf("c%d", i)})
	}
	b.Broadcast(Event{"type": "done", "stopReason": "end_turn"})

	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev := <-ch:
			if ev["type"] == "done" {
				return // 关键事件穿过积压送达
			}
		case <-deadline:
			t.Fatal("critical event never arrived after the droppable backlog")
		}
	}
}
