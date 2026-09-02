package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gauryvg98/sentinel-oms/go/internal/fixed"
	"github.com/gauryvg98/sentinel-oms/go/internal/journal"
	"github.com/gauryvg98/sentinel-oms/go/internal/record"
)

func newGateway(token string) *Gateway {
	return New(Config{JournalDir: "/nonexistent", AdminToken: token})
}

// applyDirect skips decoding, since these tests are about the HTTP surface and
// the decoder has its own suite against the golden fixture.
func applyDirect(g *Gateway, records ...record.Record) {
	for _, r := range records {
		seq := g.book.LastSeq() + 1
		g.book.Apply(journal.Header{Seq: seq, Epoch: 1, Nanos: 1_000 + seq, Kind: r.Kind}, r)
	}
}

func TestHealthIsUpEvenWhenHalted(t *testing.T) {
	// The mistake that cost the previous system five days: a health check that
	// failed on a halt would make an orchestrator restart a process whose whole
	// point is that it has stopped, and the restart would not clear a real
	// divergence — it would hide it behind a crash loop.
	g := newGateway("")
	applyDirect(g, record.Record{
		Kind:     journal.KindDecision,
		Decision: &record.Decision{Actor: 6, Kind: record.Halted, Note: "divergence"},
	})

	w := httptest.NewRecorder()
	g.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	var body struct {
		OK     bool `json:"ok"`
		Halted bool `json:"halted"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !body.OK {
		t.Error("ok = false")
	}
	if !body.Halted {
		t.Error("the halt is not reported, which is the point of the endpoint")
	}
}

func TestStateIsACompletePicture(t *testing.T) {
	// A client with nothing gets the whole thing rather than a promise of it.
	// The alternative is a screen that fills in as events happen to arrive,
	// which on a quiet account is a screen that stays blank.
	g := newGateway("")
	applyDirect(g,
		record.Record{
			Kind: journal.KindIntentPersisted,
			Intent: &record.Intent{
				ClientOrderID: "UI-1", Instrument: "BTCUSD", Side: record.Buy,
				Kind: record.Market, Authority: record.Entry,
				Qty: fixed.MustParse("0.003"),
			},
		},
		record.Record{
			Kind:           journal.KindMark,
			MarkInstrument: "BTCUSD",
			MarkPrice:      fixed.MustParse("109432.5"),
		},
		record.Record{
			Kind: journal.KindBalance, Asset: "USD", Amount: fixed.MustParse("5942.65"),
		},
	)

	w := httptest.NewRecorder()
	g.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/state", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var state State
	if err := json.Unmarshal(w.Body.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	if len(state.Orders) != 1 {
		t.Errorf("orders = %d", len(state.Orders))
	}
	if state.Balances["USD"] != fixed.MustParse("5942.65") {
		t.Errorf("balance = %v", state.Balances["USD"])
	}
	if state.LastSeq == 0 {
		t.Error("last_seq should say how far the projection has read")
	}
}

func TestNumbersCrossTheWireAsStrings(t *testing.T) {
	// JavaScript has one numeric type and it is a float64.
	g := newGateway("")
	applyDirect(g, record.Record{
		Kind: journal.KindBalance, Asset: "USD", Amount: fixed.MustParse("5942.65"),
	})

	w := httptest.NewRecorder()
	g.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/state", nil))
	if !strings.Contains(w.Body.String(), `"5942.65"`) {
		t.Errorf("the balance did not cross as a string: %s", w.Body.String())
	}
	if strings.Contains(w.Body.String(), `:5942.65`) {
		t.Errorf("a bare number reached the wire: %s", w.Body.String())
	}
}

func TestWritesAreRefusedWithoutTheToken(t *testing.T) {
	g := newGateway("s3cret")
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/command", strings.NewReader(`{"op":"halt"}`))
	g.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", w.Code)
	}
}

func TestAnUnconfiguredGatewayIsReadOnlyForEveryone(t *testing.T) {
	// The safe configuration is the one you get by not configuring anything.
	g := newGateway("")
	for _, token := range []string{"", "anything", "s3cret"} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/api/command", strings.NewReader(`{"op":"halt"}`))
		r.Header.Set("X-Sentinel-Token", token)
		g.Handler().ServeHTTP(w, r)
		if w.Code != http.StatusForbidden {
			t.Errorf("token %q got status %d, want 403", token, w.Code)
		}
	}
}

func TestMalformedCommandsAreRefusedBeforeTheWriterSeesThem(t *testing.T) {
	g := newGateway("s3cret")
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/command", strings.NewReader("not json"))
	r.Header.Set("X-Sentinel-Token", "s3cret")
	g.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestAWriterThatIsNotListeningIsReportedAsSuch(t *testing.T) {
	// Distinct from a refusal: one means the operator lacks permission, the
	// other means the engine is down. Collapsing them wastes the operator's
	// time at exactly the wrong moment.
	g := newGateway("s3cret")
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/command", strings.NewReader(`{"op":"halt"}`))
	r.Header.Set("X-Sentinel-Token", "s3cret")
	g.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", w.Code)
	}
	if !strings.Contains(w.Body.String(), "not listening") {
		t.Errorf("body = %s", w.Body.String())
	}
}

func TestTheTokenComparisonDoesNotLeakItsAnswer(t *testing.T) {
	if !constantTimeEqual("abc", "abc") {
		t.Error("equal strings compared unequal")
	}
	if constantTimeEqual("abc", "abd") {
		t.Error("different strings compared equal")
	}
	if constantTimeEqual("abc", "abcd") {
		t.Error("different lengths compared equal")
	}
	if constantTimeEqual("", "x") {
		t.Error("empty matched")
	}
}

func TestTheStreamOpensWithACompletePicture(t *testing.T) {
	g := newGateway("")
	applyDirect(g, record.Record{
		Kind: journal.KindBalance, Asset: "USD", Amount: fixed.MustParse("100"),
	})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/stream", nil)
	ctx, cancel := contextWithImmediateCancel(r)
	r = r.WithContext(ctx)
	cancel()
	g.Handler().ServeHTTP(w, r)

	body := w.Body.String()
	if !strings.HasPrefix(body, "event: book\n") {
		t.Errorf("the stream did not open with a snapshot: %q", firstLine(body))
	}
	if !strings.Contains(body, `"100"`) {
		t.Errorf("the snapshot is missing the balance: %s", body)
	}
}

func TestSSEHeadersAreSet(t *testing.T) {
	g := newGateway("")
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/stream", nil)
	ctx, cancel := contextWithImmediateCancel(r)
	r = r.WithContext(ctx)
	cancel()
	g.Handler().ServeHTTP(w, r)

	if got := w.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("content type = %q", got)
	}
	if got := w.Header().Get("Cache-Control"); got != "no-cache" {
		t.Errorf("cache control = %q", got)
	}
}

// contextWithImmediateCancel gives the handler a context the test can end, so
// the stream returns instead of blocking the suite forever.
func contextWithImmediateCancel(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithCancel(r.Context())
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
