// Package alert notifies a person that something needs them.
//
// The whole reason this exists: on 2026-09-03 the account halted on a false
// positive at 05:40 and nobody knew for ten hours. The reason had aged out of
// the gateway's decision ring and the host's logs did not reach back a day. A
// supervisor that stops trading and waits to be noticed is not supervising
// anything.
//
// Delivery never takes anything down. A webhook that is refusing connections is
// a worse thing to crash a trading system for than the event it was trying to
// report.
package alert

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"
)

// ErrNotConfigured means no webhook was named, which is a deployment choice.
var ErrNotConfigured = errors.New("alert: no webhook")

// Notifier posts messages to a webhook.
type Notifier struct {
	url    string
	client *http.Client

	// last is when each key was last sent, so a condition that stays true does
	// not send every time it is checked.
	last map[string]time.Time
	// repeat is how long a key stays quiet after firing.
	repeat time.Duration
}

// New builds a notifier, or reports that none was asked for.
func New(url string, repeat time.Duration) (*Notifier, error) {
	if url == "" {
		return nil, ErrNotConfigured
	}
	if repeat == 0 {
		repeat = time.Hour
	}
	return &Notifier{
		url:    url,
		client: &http.Client{Timeout: 10 * time.Second},
		last:   map[string]time.Time{},
		repeat: repeat,
	}, nil
}

// Send delivers a message, at most once per repeat window for a given key.
//
// The key is the condition, not the text: "halted" fires once and stays quiet
// while it is still halted, rather than paging every sweep. Passing an empty
// key sends unconditionally, which is right for state *changes* — a resume is
// news however recently the halt was.
func (n *Notifier) Send(key, message string) {
	if key != "" {
		if at, seen := n.last[key]; seen && time.Since(at) < n.repeat {
			return
		}
		n.last[key] = time.Now()
	}

	// The shape v1 sent, so an existing Discord or Slack webhook keeps working
	// without anyone reconfiguring anything.
	body, err := json.Marshal(map[string]string{
		"content": fmt.Sprintf("🛑 Sentinel: %s", message),
	})
	if err != nil {
		log.Printf("alertd: could not encode alert: %v", err)
		return
	}

	resp, err := n.client.Post(n.url, "application/json", bytes.NewReader(body))
	if err != nil {
		// Logged, never fatal. Alerting is the thing that tells you something
		// broke; it must not be a thing that breaks.
		log.Printf("alertd: delivery failed: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("alertd: webhook answered %s", resp.Status)
		return
	}
	log.Printf("alertd: sent: %s", message)
}

// Clear forgets a key, so the next occurrence sends immediately.
//
// Called when a condition resolves. Without it, a halt at 09:00 and another at
// 09:30 would look identical to one long halt and the second would be silent.
func (n *Notifier) Clear(key string) { delete(n.last, key) }
