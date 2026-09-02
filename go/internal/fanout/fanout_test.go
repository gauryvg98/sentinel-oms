package fanout

import (
	"fmt"
	"testing"
	"time"
)

func TestTheChannelsThatCarryVolumeAreTheOnesThatConflate(t *testing.T) {
	// The whole reason the policies differ. If this inverts, either a burst
	// costs events or a display costs memory.
	if PolicyFor(Marks) != Conflate {
		t.Error("marks should conflate")
	}
	if PolicyFor(Book) != Conflate {
		t.Error("book should conflate")
	}
	for _, ch := range []Channel{Fills, Orders, Account} {
		if PolicyFor(ch) != Sequenced {
			t.Errorf("%s should be sequenced", ch)
		}
	}
}

func TestAnUnnamedChannelIsSequenced(t *testing.T) {
	// Dropping something nobody has thought about is the wrong default.
	if PolicyFor(Channel("something-new")) != Sequenced {
		t.Error("an unnamed channel should not silently drop")
	}
}

func TestAConflatingChannelKeepsOnlyTheNewest(t *testing.T) {
	r := NewRing()
	sub := r.Subscribe(Marks)
	defer sub.Close()

	// Ten thousand updates in a hot second. The reader has not looked yet.
	for i := range 10_000 {
		r.Publish(Marks, []byte(fmt.Sprintf("%d", i)))
	}

	got := <-sub.Frames()
	if string(got) != "9999" {
		t.Errorf("got %q, want the newest", got)
	}
	// And the reader is fully caught up: there is nothing behind it.
	select {
	case extra := <-sub.Frames():
		t.Errorf("expected to be caught up, found %q", extra)
	default:
	}
	if r.Dropped(Marks) == 0 {
		t.Error("the transport should say it conflated")
	}
}

func TestASequencedChannelDropsNothing(t *testing.T) {
	r := NewRing()
	sub := r.Subscribe(Fills)
	defer sub.Close()

	const count = 500
	for i := range count {
		r.Publish(Fills, []byte(fmt.Sprintf("%d", i)))
	}
	for i := range count {
		select {
		case got := <-sub.Frames():
			if string(got) != fmt.Sprintf("%d", i) {
				t.Fatalf("frame %d = %q, out of order", i, got)
			}
		default:
			t.Fatalf("frame %d was dropped", i)
		}
	}
}

func TestASequencedSubscriberThatFallsTooFarBehindIsTold(t *testing.T) {
	// Told, not merely disconnected. A client that reconnects assuming
	// continuity is worse off than one that knows it missed something.
	r := NewRing()
	sub := r.Subscribe(Fills)
	defer sub.Close()

	for i := range QueueLimit + 10 {
		r.Publish(Fills, []byte(fmt.Sprintf("%d", i)))
	}

	select {
	case <-sub.Resync():
	default:
		t.Fatal("the subscriber was starved without being told")
	}
}

func TestEveryoneWatchingGetsTheFrame(t *testing.T) {
	r := NewRing()
	a := r.Subscribe(Orders)
	b := r.Subscribe(Orders)
	defer a.Close()
	defer b.Close()

	if got := r.Subscribers(Orders); got != 2 {
		t.Fatalf("subscribers = %d", got)
	}
	r.Publish(Orders, []byte("filled"))

	for i, sub := range []*Subscription{a, b} {
		select {
		case got := <-sub.Frames():
			if string(got) != "filled" {
				t.Errorf("subscriber %d got %q", i, got)
			}
		case <-time.After(time.Second):
			t.Errorf("subscriber %d got nothing", i)
		}
	}
}

func TestPublishingNeverBlocksOnASlowReader(t *testing.T) {
	// A publisher that could be blocked by a slow reader would put the
	// writer's latency at the mercy of the slowest browser attached to it.
	r := NewRing()
	sub := r.Subscribe(Fills)
	defer sub.Close()

	done := make(chan struct{})
	go func() {
		for i := range QueueLimit * 2 {
			r.Publish(Fills, []byte(fmt.Sprintf("%d", i)))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("publishing blocked on a reader that never read")
	}
}

func TestClosingDetachesTheSubscriber(t *testing.T) {
	r := NewRing()
	sub := r.Subscribe(Marks)
	if got := r.Subscribers(Marks); got != 1 {
		t.Fatalf("subscribers = %d", got)
	}
	sub.Close()
	if got := r.Subscribers(Marks); got != 0 {
		t.Errorf("subscribers after close = %d", got)
	}
	// Closing twice is a no-op rather than a panic: a connection can end from
	// either side, and both will try.
	sub.Close()
}

func TestPublishingToNobodyIsFine(t *testing.T) {
	r := NewRing()
	r.Publish(Marks, []byte("109432.5"))
	if got := r.Subscribers(Marks); got != 0 {
		t.Errorf("subscribers = %d", got)
	}
}

func TestTheChannelNamesAreTheContract(t *testing.T) {
	// A channel published under any other name is indistinguishable from one
	// never published at all. These exact strings are what the client waits on.
	want := []string{"marks", "book", "fills", "orders", "account"}
	got := Channels()
	if len(got) != len(want) {
		t.Fatalf("got %d channels, want %d", len(got), len(want))
	}
	for i, name := range want {
		if string(got[i]) != name {
			t.Errorf("channel %d = %q, want %q", i, got[i], name)
		}
	}
}

func TestRingTransportSatisfiesTheSeam(t *testing.T) {
	// The point of the interface: everything above is written against this, so
	// a multicast implementation can replace it without touching a caller.
	var _ Transport = NewRing()
}
