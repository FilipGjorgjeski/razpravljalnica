package server

import (
	"sync"

	"github.com/FilipGjorgjeski/razpravljalnica/storage"
)

// hub.go is a delivery mechanism for subs.
// we sort of duplicate some info with the type subscription
// even though we have subscriptionToken in storage,
// this way we separate locks (mutex) and can modify structure more freely
// for the purpose of event delivery.
type subscription struct {
	topics map[int64]struct{}
	ch     chan storage.Event
}

type Hub struct {
	mu     sync.RWMutex
	nextID int64
	subs   map[int64]*subscription
}

func NewHub() *Hub {
	return &Hub{subs: map[int64]*subscription{}}
}

func (h *Hub) Add(topicIDs []int64) (id int64, ch <-chan storage.Event, remove func()) {
	topicSet := make(map[int64]struct{}, len(topicIDs))
	for _, tid := range topicIDs {
		topicSet[tid] = struct{}{}
	}

	h.mu.Lock()
	h.nextID++
	id = h.nextID
	sub := &subscription{topics: topicSet, ch: make(chan storage.Event, 256)}
	h.subs[id] = sub
	h.mu.Unlock()

	remove = func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if s, ok := h.subs[id]; ok {
			delete(h.subs, id)
			close(s.ch)
		}
	}
	return id, sub.ch, remove
}

func (h *Hub) Broadcast(ev storage.Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, sub := range h.subs {
		if _, ok := sub.topics[ev.Message.TopicID]; !ok {
			continue
		}
		select {
		case sub.ch <- ev:
		default:
			// Delivery is best-effort if a subscriber is too slow.
		}
	}
}
