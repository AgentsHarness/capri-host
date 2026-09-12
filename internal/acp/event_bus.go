package acp

import (
	"log"
	"sync"
	"sync/atomic"
)

// ─────────────────────────────────────────────────────────────────────
// eventBus — Bridge 的事件发布/订阅内核。
//
// 职责单一：持有订阅者集合与全局事件序号，负责 fan-out 与背压分级。
// Bridge 保留 Broadcast/Subscribe 门面方法（调用方不感知本类型）；
// 订阅者状态与 subscribersMu 的锁纪律由此收敛到一处。
//
// 背压模型（为什么 publish 不直接写订阅者通道）：
// publish 的调用方是 agent stdout 的读取协程（bridge.go readStdout），
// 单协程，且 agent 的 stdout 管道容量有限。旧实现在持锁状态下对关键
// 事件做有界阻塞投递：一个卡死的 SSE 消费者就能把 agent 输出通路按停
// ——连同全部会话、全部客户端。现在 publish 只做 O(1) 入队（绝不阻塞），
// 每个订阅者由自己的 pump 协程按序投递：慢消费者只拖慢它自己，
// 关键事件在它的队列里等到投递成功（unsubscribe 时止损退出）。
// ─────────────────────────────────────────────────────────────────────

type eventBus struct {
	mu   sync.Mutex
	subs map[*eventSub]struct{}

	// seq 是广播事件的全局单调序号（publish 附加到每个事件）。本地 SSE
	// 与 hub 中继（/ws/fe 推回）携带同一 seq，前端选中本机 host 时双路
	// （本地 SSE + hub WS）收到同一事件可按 (hostId, seq) 去重；
	// hub-client 转发时保留该 seq（不再自行分配）。
	seq uint64
}

// eventSub 是一个订阅者的 fan-out 槽：有界环形队列 + 独立 pump 协程。
// publish 只入队（持 eb.mu 做 O(1) 追加），pump 按 FIFO 投递到消费通道，
// 因此投递顺序 = 全局 seq 顺序由构造保证，而生产者永不被消费者拖住。
type eventSub struct {
	ch   chan Event
	mu   sync.Mutex
	ring []Event
	head int
	n    int

	wake     chan struct{} // 1 槽唤醒信号（队列空转后有新事件）
	done     chan struct{} // unsubscribe 关闭：pump 退出
	stopOnce sync.Once

	dropped         atomic.Uint64 // 可丢事件满环丢弃计数
	criticalDropped atomic.Uint64 // 关键事件满环丢弃计数（正常恒为 0）
}

const (
	// subQueueCap 是每个订阅者的可丢事件积压上限。超出即丢（chunk/thought
	// 等流式噪声），落后方靠 FE 补拉 / 历史回放修复。
	subQueueCap = 4096
	// subCriticalSlack 是环形队列为关键事件额外预留的空间：可丢积压永远
	// 挤不掉终态（done/turn_completed/error/client_request…）。
	subCriticalSlack = 1024
)

func (eb *eventBus) init() {
	eb.subs = make(map[*eventSub]struct{})
}

// subscribe returns a buffered event channel; call unsubscribe to remove.
// The channel is fed by this subscriber's own pump goroutine (see eventSub),
// so a slow reader no longer back-pressures the publisher.
func (eb *eventBus) subscribe() (ch chan Event, unsubscribe func()) {
	s := &eventSub{
		ch:   make(chan Event, 512),
		ring: make([]Event, subQueueCap+subCriticalSlack),
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
	eb.mu.Lock()
	eb.subs[s] = struct{}{}
	eb.mu.Unlock()
	go s.pump()
	return s.ch, func() {
		eb.mu.Lock()
		delete(eb.subs, s)
		eb.mu.Unlock()
		s.stopOnce.Do(func() { close(s.done) })
		// drain
		for {
			select {
			case <-s.ch:
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
	for s := range eb.subs {
		// 投递绝不等待消费者：通道有空位且无积压时就地写（保持同步语义，
		// 也是最常见的快路径），否则入队由 pump 按序补投——见 eventBus
		// 头部注释。
		s.enqueueOrDirect(ev, critical)
	}
}

// enqueueOrDirect hands ev to this subscriber: a direct non-blocking write
// while the channel has room and nothing is backlogged, otherwise a queue
// append (drained in order by the pump). Called with eb.mu held; the
// per-subscriber lock is only ever taken in this direction (eb.mu → s.mu),
// never the reverse (the pump holds no other lock).
//
// The n == 0 guard is what keeps ordering: while the pump owns a ring item,
// that item still counts in n (the pump decrements only after the hand-off),
// so new events queue behind it instead of jumping the channel.
func (s *eventSub) enqueueOrDirect(ev Event, critical bool) {
	s.mu.Lock()
	if s.n == 0 {
		select {
		case s.ch <- ev:
			s.mu.Unlock()
			return
		default:
		}
	}
	s.mu.Unlock()
	s.enqueue(ev, critical)
}

// enqueue appends ev to this subscriber's ring. Called with eb.mu held; the
// per-subscriber lock is only ever taken in this direction (eb.mu → s.mu),
// never the reverse (the pump holds no other lock).
func (s *eventSub) enqueue(ev Event, critical bool) {
	limit := subQueueCap
	if critical {
		limit = len(s.ring)
	}
	s.mu.Lock()
	if s.n >= limit {
		s.mu.Unlock()
		if critical {
			// 可丢积压顶满预留区：消费者长时间假死而关键事件仍在产生。
			// 丢弃并告警；订阅者 unsubscribe 后 pump 立即退出。
			if n := s.criticalDropped.Add(1); n == 1 {
				log.Printf("[bridge] 订阅者关键事件积压超过 %d 条，被迫丢弃（消费者假死？）", limit)
			}
		} else {
			s.dropped.Add(1)
		}
		return
	}
	s.ring[(s.head+s.n)%len(s.ring)] = ev
	s.n++
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// pump delivers this subscriber's queue in order. Droppable events are
// dropped the moment the consumer channel is full (transient UI jitter the
// FE repairs); critical events wait for a free slot instead — never dropped
// while the subscriber is live, and never blocking anyone else.
func (s *eventSub) pump() {
	for {
		s.mu.Lock()
		if s.n == 0 {
			s.mu.Unlock()
			select {
			case <-s.wake:
				continue
			case <-s.done:
				return
			}
		}
		ev := s.ring[s.head]
		s.mu.Unlock()

		if droppableEventType(ev[kType]) {
			select {
			case s.ch <- ev:
			case <-s.done:
				return
			default:
				// 慢消费者：可丢事件不占位，seq 照常前进，落后方靠 FE
				// 补拉 / 历史回放修复。
				s.dropped.Add(1)
			}
		} else {
			select {
			case s.ch <- ev:
			case <-s.done:
				return
			}
		}

		s.mu.Lock()
		s.ring[s.head] = nil // 释放事件引用（大 payload 不随环常驻）
		s.head = (s.head + 1) % len(s.ring)
		s.n--
		s.mu.Unlock()
	}
}

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
