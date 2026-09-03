// Package fanout distributes journal-derived frames to whoever is watching.
//
// Fly's network does not carry IP multicast and the deployment is one machine,
// so what ships is a shared-memory ring. The seam is named anyway: Transport is
// the interface, RingTransport is the implementation, and a MulticastTransport
// is a file that does not exist yet and can be written without touching a
// caller — the day this runs on metal or an AWS multicast domain.
//
// Naming the seam now is the point. Building the second implementation now
// would be building for a network we are not on.
//
// # Channels are not treated alike
//
// Because they are not alike:
//
//	marks      ~4/s, superseded instantly   conflate — keep the newest
//	book       one per change, superseded   conflate
//	fills      every print is history       sequenced — never dropped
//	orders     lifecycle                    sequenced
//	account    balances, halts              sequenced
//
// The channels that carry volume are exactly the ones that conflate, and the
// ones that cannot be conflated carry almost no volume — an account channel is
// bounded by how fast one account trades, not by how busy the venue is. So a
// burst costs no events: ten thousand mark updates in a hot second collapse to
// the one still true, and the client reading it is fully caught up.
package fanout

import (
	"errors"
	"sync"
)

// Channel is a named stream.
type Channel string

// The channels. These exact strings are the contract with the client: a channel
// published under any other name is indistinguishable from one never published
// at all.
const (
	// Marks carries prices. High rate, and every one supersedes the last.
	Marks Channel = "marks"
	// Book carries the current order book state. Superseded on every change.
	Book Channel = "book"
	// Fills carries executions. Every print is history and none may be dropped.
	Fills Channel = "fills"
	// Orders carries lifecycle transitions.
	Orders Channel = "orders"
	// Account carries balances, halts and resumes.
	Account Channel = "account"
)

// Policy is what happens to a frame a slow subscriber has not read yet.
type Policy int

const (
	// Conflate keeps only the newest frame. Correct when a frame is a complete
	// picture of current state, because an older one is not history — it is a
	// picture that is now wrong.
	Conflate Policy = iota
	// Sequenced delivers every frame in order. Correct when each frame is an
	// event that happened, and dropping one loses it.
	Sequenced
)

// policies fixes what each channel does. A map rather than a field on the
// subscription, so a caller cannot subscribe to fills with conflation and
// quietly lose executions.
var policies = map[Channel]Policy{
	Marks:   Conflate,
	Book:    Conflate,
	Fills:   Sequenced,
	Orders:  Sequenced,
	Account: Sequenced,
}

// PolicyFor reports how a channel treats a backlog.
func PolicyFor(ch Channel) Policy {
	if p, ok := policies[ch]; ok {
		return p
	}
	// An unnamed channel is sequenced. Dropping something nobody has thought
	// about is the wrong default.
	return Sequenced
}

// Channels lists every channel, in a fixed order.
func Channels() []Channel {
	return []Channel{Marks, Book, Fills, Orders, Account}
}

// QueueLimit is how far a sequenced subscriber may fall behind before it is
// declared hopeless — roughly forty seconds of tape at a hundred events a
// second, which is far longer than any live connection should ever need.
const QueueLimit = 8192

// ErrResync means a subscriber fell too far behind and must reload.
//
// Told, not merely disconnected. A client that reconnects assuming continuity
// is worse off than one that knows it missed something.
var ErrResync = errors.New("subscriber fell behind; resync required")

// Transport delivers frames to subscribers.
//
// The seam. Everything above this is written against the interface, so the day
// this moves to a network that carries multicast, nothing above changes.
type Transport interface {
	// Publish sends a frame to everyone watching a channel.
	Publish(ch Channel, frame []byte)
	// Subscribe returns a subscription. Close it when done.
	Subscribe(ch Channel) *Subscription
	// Subscribers reports how many are watching a channel.
	Subscribers(ch Channel) int
}

// Subscription is one reader's view of one channel.
type Subscription struct {
	ch     Channel
	policy Policy
	frames chan []byte
	// resync is closed when the subscriber has fallen too far behind.
	resync chan struct{}
	once   sync.Once
	parent *RingTransport
}

// Frames is the stream to read. It is closed when the subscription is closed.
func (s *Subscription) Frames() <-chan []byte { return s.frames }

// Resync is closed when this subscriber has fallen too far behind and must
// reload from scratch.
func (s *Subscription) Resync() <-chan struct{} { return s.resync }

// Channel is what this subscription watches.
func (s *Subscription) Channel() Channel { return s.ch }

// Close detaches the subscriber.
func (s *Subscription) Close() {
	s.once.Do(func() {
		if s.parent != nil {
			s.parent.remove(s)
		}
		close(s.frames)
	})
}

// RingTransport is the in-process implementation.
//
// A shared-memory ring: publishers write, subscribers read, and nobody blocks.
// A publisher that could be blocked by a slow reader would put the writer's
// latency at the mercy of the slowest browser attached to it, which is how a
// display becomes a trading problem.
type RingTransport struct {
	mu          sync.RWMutex
	subscribers map[Channel][]*Subscription
	dropped     map[Channel]uint64
}

// NewRing returns an empty transport.
func NewRing() *RingTransport {
	return &RingTransport{
		subscribers: map[Channel][]*Subscription{},
		dropped:     map[Channel]uint64{},
	}
}

// Publish sends a frame to everyone watching.
//
// Never blocks, whatever the subscribers are doing.
func (r *RingTransport) Publish(ch Channel, frame []byte) {
	r.mu.RLock()
	subscribers := append([]*Subscription(nil), r.subscribers[ch]...)
	r.mu.RUnlock()

	for _, sub := range subscribers {
		switch sub.policy {
		case Conflate:
			// Keep the newest. Drain what is stale first — an older picture of
			// current state is not history, it is a picture that is now wrong.
			for {
				select {
				case <-sub.frames:
					r.countDrop(ch)
					continue
				default:
				}
				break
			}
			select {
			case sub.frames <- frame:
			default:
				// The reader took the slot between the drain and the send.
				// Theirs is newer than nothing; skip.
			}
		case Sequenced:
			select {
			case sub.frames <- frame:
			default:
				// Past the limit this connection is hopeless — but it is told
				// so rather than silently starved.
				sub.markResync()
			}
		}
	}
}

// Subscribe returns a subscription to a channel.
func (r *RingTransport) Subscribe(ch Channel) *Subscription {
	depth := 1
	if PolicyFor(ch) == Sequenced {
		depth = QueueLimit
	}
	sub := &Subscription{
		ch:     ch,
		policy: PolicyFor(ch),
		frames: make(chan []byte, depth),
		resync: make(chan struct{}),
		parent: r,
	}
	r.mu.Lock()
	r.subscribers[ch] = append(r.subscribers[ch], sub)
	r.mu.Unlock()
	return sub
}

// Subscribers reports how many are watching a channel.
func (r *RingTransport) Subscribers(ch Channel) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.subscribers[ch])
}

// Dropped reports how many frames a channel has conflated away.
//
// Worth watching: on a conflating channel it is expected and means the
// transport is doing its job. On a sequenced one it would be zero, because
// those do not drop — they resync.
func (r *RingTransport) Dropped(ch Channel) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.dropped[ch]
}

func (r *RingTransport) countDrop(ch Channel) {
	r.mu.Lock()
	r.dropped[ch]++
	r.mu.Unlock()
}

func (r *RingTransport) remove(target *Subscription) {
	r.mu.Lock()
	defer r.mu.Unlock()
	subscribers := r.subscribers[target.ch]
	for i, sub := range subscribers {
		if sub == target {
			r.subscribers[target.ch] = append(subscribers[:i], subscribers[i+1:]...)
			return
		}
	}
}

func (s *Subscription) markResync() {
	select {
	case <-s.resync:
		// Already told.
	default:
		close(s.resync)
	}
}
