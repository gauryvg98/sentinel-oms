// Package supabase writes the journal's history somewhere it can be queried.
//
// The cold path, and the direct answer to "no Postgres in the hot path".
// Postgres was never slow; it was *load bearing* — the engine could not place
// an order without it. Here nothing reads from Supabase to make a decision, so
// it can be down for an hour and the engine will not notice. What it loses is
// history for a chart, and history is a thing that can arrive late.
//
// Rows are keyed on the journal sequence, and the sequence is the primary key.
// A batch that is sent twice — because the process died between writing and
// committing its cursor — collides and is ignored. That is what makes the
// projector safe to restart at any point: at-least-once delivery with an
// idempotent sink is exactly-once, and the alternative is a two-phase commit
// with a service that does not offer one.
package supabase

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// Config is where to write.
type Config struct {
	// URL is the project's REST endpoint, e.g. https://xyz.supabase.co.
	URL string
	// ServiceKey authorises the write. A service key, not an anon key: this
	// process writes rows nobody else may.
	ServiceKey string
	// Table receives the records.
	Table string
	// Timeout bounds one request.
	Timeout time.Duration
}

// ErrNotConfigured means no destination was given.
//
// Distinct from a failure to reach one: running without a projector is a
// deployment choice, and running with a broken one is a fault.
var ErrNotConfigured = errors.New("supabase is not configured")

// Client writes batches.
type Client struct {
	config Config
	http   *http.Client
}

// New returns a client, or ErrNotConfigured when nothing was set.
func New(config Config) (*Client, error) {
	if config.URL == "" || config.ServiceKey == "" {
		return nil, ErrNotConfigured
	}
	if config.Table == "" {
		config.Table = "journal_records"
	}
	if config.Timeout == 0 {
		config.Timeout = 15 * time.Second
	}
	return &Client{
		config: config,
		http:   &http.Client{Timeout: config.Timeout},
	}, nil
}

// Row is one journal record, flattened for querying.
//
// The payload is kept as decoded JSON rather than raw bytes, because the point
// of this table is that somebody can ask it questions — and nobody can ask a
// question of a bytea column.
type Row struct {
	Seq        uint64          `json:"seq"`
	Epoch      uint64          `json:"epoch"`
	Nanos      uint64          `json:"nanos"`
	Kind       string          `json:"kind"`
	Instrument string          `json:"instrument,omitempty"`
	OrderID    string          `json:"client_order_id,omitempty"`
	Payload    json.RawMessage `json:"payload"`
}

// Insert writes a batch.
//
// Duplicate sequences are ignored rather than refused: this is the property
// that makes a restart safe, because a batch sent twice must not be an error.
func (c *Client) Insert(rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	body, err := json.Marshal(rows)
	if err != nil {
		return fmt.Errorf("encode batch: %w", err)
	}

	url := fmt.Sprintf("%s/rest/v1/%s", c.config.URL, c.config.Table)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("apikey", c.config.ServiceKey)
	req.Header.Set("Authorization", "Bearer "+c.config.ServiceKey)
	req.Header.Set("Content-Type", "application/json")
	// The whole reason a resend is harmless.
	req.Header.Set("Prefer", "resolution=ignore-duplicates,return=minimal")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("supabase returned HTTP %d", resp.StatusCode)
	}
	return nil
}

// Schema is the table this package writes into.
//
// Kept here rather than in a migrations directory, because it is four lines and
// the thing it has to stay in step with is the struct above it.
const Schema = `
create table if not exists journal_records (
  seq             bigint primary key,
  epoch           bigint      not null,
  nanos           bigint      not null,
  kind            text        not null,
  instrument      text,
  client_order_id text,
  payload         jsonb       not null,
  inserted_at     timestamptz not null default now()
);
create index if not exists journal_records_order
  on journal_records (client_order_id) where client_order_id is not null;
create index if not exists journal_records_kind_time
  on journal_records (kind, nanos desc);
`
