package acp

import (
	"log"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────
// eventBus — Bridge 的事件发布/订阅内核。
//
// 职责单一：持有订阅者集合与全局事件序号，负责 fan-out 与背压分级。
// Bridge 保留 Broadcast/Subscribe 门面方法（调用方不感知本类型）；
// 订阅者状态与 subscribersMu 的锁纪律由此收敛到一处。
// ─────────────────────────────────────────────────────────────────────

type eventBus struct {
	mu   sync.Mutex
	subs map[chan Event]struct{}

	// seq 是广播事件的全局单调序号（publish 附加到每个事件）。本地 SSE
	// 与 hub 中继（/ws/fe 推回）携带同一 seq，前端选中本机 host 时双路
	// （本地 SSE + hub WS）收到同一事件可按 (hostId, seq) 去重；
	// hub-client 转发时保留该 seq（不再自行分配）。
	seq uint64
}

func (eb *eventBus) init() {
	eb.subs = make(map[chan Event]struct{})
}

// subscribe returns a buffered event channel; call unsubscribe to remove.
func (eb *eventBus) subscribe() (ch chan Event, unsubscribe func()) {
	ch = make(chan Event, 512)
	eb.mu.Lock()
	eb.subs[ch] = struct{}{}
	eb.mu.Unlock()
	return ch, func() {
		eb.mu.Lock()
		delete(eb.subs, ch)
		eb.mu.Unlock()
		// drain
		for {
			select {
			case <-ch:
			default:
				return
			}
		}
	}
}

func (eb *eventBus) publish(ev Event) {
	if ev == nil {
		return
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()
	// 全局事件序号：所有订阅者（本地 SSE、hub-client 转发）看到同一
	// 事件同一 seq。注意 hub-client 转发时必须保留该 seq（见其
	// seqAndReplay），否则双路去重失效。
	eb.seq++
	ev[kSeq] = eb.seq
	critical := !droppableEventType(ev[kType])
	for ch := range eb.subs {
		if !critical {
			// 慢消费者：可丢事件（chunk/thought 等流式噪声）直接丢弃，
			// seq 照常前进，落后方靠 FE gap-pull 补。不要为了填洞而合并
			// chunk——会破坏与 hub 上行一致的双路 seq。
			select {
			case ch <- ev:
			default:
			}
			continue
		}
		// 关键事件（终态 done/turn_completed、error、client_request、
		// roster 变更等）永不主动丢弃：丢掉它们会让 FE 永远等不到终态。
		// 改为有界阻塞等待慢消费者腾出位置；仅在极端超时（消费者已死
		// 而未注销）时兜底丢弃并大声告警。阻塞期间持有 mu，
		// 后续 publish 排队——全局 seq 顺序因此得以保持。
		select {
		case ch <- ev:
		case <-time.After(broadcastCriticalTimeout):
			log.Printf("[bridge] 关键事件 %v 投递超时（订阅者阻塞 %s），被迫丢弃 seq=%v",
				ev[kType], broadcastCriticalTimeout, ev[kSeq])
		}
	}
}

// broadcastCriticalTimeout bounds how long publish blocks on a full
// subscriber channel for a critical (non-droppable) event before giving up.
// SSE 消费者循环读取，正常情况下远不会触及该上限；到达即说明消费者已经
// 死亡（连接半开、进程卡死）而未注销。
const broadcastCriticalTimeout = 5 * time.Second

// droppableEventType reports whether an event type is lossy-tolerable stream
// noise: dropping one (or several) of these under backpressure costs at most
// transient UI jitter that FE gap-pull can repair or ignore. Everything else
// (terminal states like done/turn_completed, errors, client_request, roster
// and permission changes) is critical and is never dropped voluntarily.
func droppableEventType(t any) bool {
	s, ok := t.(string)
	if !ok {
		return false
	}
	switch s {
	case "chunk", "user_chunk", "thought", "gen_rate", "log",
		"session_updates_chunk", "search_fuzzy_status", "search_content_status":
		return true
	}
	return false
}
