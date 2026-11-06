package telemetry

import (
	"sync"
)

const clientBufferSize = 64

// Broadcaster manages fan-out of telemetry events to multiple SSE client channels.
type Broadcaster struct {
	mu      sync.RWMutex
	clients map[chan DashboardEvent]struct{}
}

// NewBroadcaster creates a new Broadcaster.
func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		clients: make(map[chan DashboardEvent]struct{}),
	}
}

// Subscribe registers a new client channel and returns an unregister function.
func (b *Broadcaster) Subscribe() (<-chan DashboardEvent, func()) {
	ch := make(chan DashboardEvent, clientBufferSize)

	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()

	unsubscribe := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if _, ok := b.clients[ch]; ok {
			delete(b.clients, ch)
			close(ch)
		}
	}

	return ch, unsubscribe
}

// Broadcast sends the event to all registered clients.
// If a client's buffer is full, the event is dropped for that client
// to ensure telemetry never stalls worker goroutines.
func (b *Broadcaster) Broadcast(event DashboardEvent) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for ch := range b.clients {
		select {
		case ch <- event:
		default:
			// Client channel full; drop event to preserve backend throughput
		}
	}
}
