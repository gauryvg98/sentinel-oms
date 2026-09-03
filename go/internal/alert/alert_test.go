package alert

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// receiver stands in for a Discord or Slack webhook.
type receiver struct {
	mu   sync.Mutex
	got  []string
	code int
}

func (r *receiver) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		var payload map[string]string
		_ = json.Unmarshal(body, &payload)
		r.mu.Lock()
		r.got = append(r.got, payload["content"])
		r.mu.Unlock()
		if r.code != 0 {
			w.WriteHeader(r.code)
		}
	}
}

func (r *receiver) messages() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.got...)
}

func TestThePayloadIsTheShapeV1Sent(t *testing.T) {
	// An existing webhook must keep working without anyone reconfiguring it.
	rec := &receiver{}
	server := httptest.NewServer(rec.handler())
	defer server.Close()

	n, err := New(server.URL, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	n.Send("", "the account halted")

	got := rec.messages()
	if len(got) != 1 {
		t.Fatalf("got %d messages, want 1", len(got))
	}
	if want := "🛑 Sentinel: the account halted"; got[0] != want {
		t.Errorf("got %q, want %q", got[0], want)
	}
}

func TestAConditionThatStaysTrueDoesNotPageRepeatedly(t *testing.T) {
	rec := &receiver{}
	server := httptest.NewServer(rec.handler())
	defer server.Close()

	n, _ := New(server.URL, time.Hour)
	for i := 0; i < 20; i++ {
		n.Send("stalled", "the writer is wedged")
	}
	if got := len(rec.messages()); got != 1 {
		t.Errorf("sent %d times, want 1 — a stuck condition is checked every "+
			"second and must not page every second", got)
	}
}

func TestAResolvedConditionPagesAgainWhenItReturns(t *testing.T) {
	rec := &receiver{}
	server := httptest.NewServer(rec.handler())
	defer server.Close()

	n, _ := New(server.URL, time.Hour)
	n.Send("halted", "first halt")
	n.Clear("halted")
	n.Send("halted", "second halt")

	if got := len(rec.messages()); got != 2 {
		t.Errorf("sent %d, want 2 — two halts half an hour apart are two "+
			"events, not one long one", got)
	}
}

func TestStateChangesSendUnconditionally(t *testing.T) {
	rec := &receiver{}
	server := httptest.NewServer(rec.handler())
	defer server.Close()

	n, _ := New(server.URL, time.Hour)
	n.Send("", "resumed")
	n.Send("", "resumed")
	if got := len(rec.messages()); got != 2 {
		t.Errorf("sent %d, want 2 — an empty key means every occurrence is news", got)
	}
}

func TestDeliveryFailureIsNotFatal(t *testing.T) {
	// A webhook refusing connections must never take a trading system down.
	n, _ := New("http://127.0.0.1:1/nothing-listens-here", time.Hour)
	n.Send("halted", "the account halted") // must simply return

	rec := &receiver{code: http.StatusInternalServerError}
	server := httptest.NewServer(rec.handler())
	defer server.Close()
	broken, _ := New(server.URL, time.Hour)
	broken.Send("halted", "the account halted") // a 500 is survivable too
}

func TestNoWebhookIsADeploymentChoiceNotAnError(t *testing.T) {
	if _, err := New("", time.Hour); err != ErrNotConfigured {
		t.Errorf("got %v, want ErrNotConfigured", err)
	}
}
