// Package pg projects the journal into Postgres.
//
// The cold path, and the reason it exists: the journal is the source of truth
// and it is a binary log. Nothing can run SQL over it, so "what did I trade on
// Tuesday" means replaying a quarter of a million records. This copies the log
// into a table so the question has an answer, without putting a database
// anywhere near the order path.
//
// Nothing reads from here to make a decision. This can be down for an hour and
// the engine will not notice — which is the whole of "no Postgres in the hot
// path". Postgres was never slow; in v1 it was load bearing.
package pg

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq" // registers the "postgres" driver
)

// ErrNotConfigured means no database was named, which is a deployment choice
// rather than a fault.
var ErrNotConfigured = errors.New("pg: no DATABASE_URL")

// Config is how to reach the database.
type Config struct {
	// URL is a libpq connection string or postgres:// URL.
	URL string
	// Table receives the rows.
	Table string
	// Timeout bounds a single batch.
	Timeout time.Duration
}

// Client writes batches of journal records.
type Client struct {
	db    *sql.DB
	table string
}

// Row is one journal record, flattened for querying.
//
// The columns that get asked about — sequence, instrument, order id, kind — are
// real columns; the rest of the record stays as jsonb rather than being
// modelled. A schema that mirrored every record variant would need migrating
// every time the log learns a new one, and the log is allowed to learn.
type Row struct {
	Seq        uint64
	Epoch      uint64
	Nanos      uint64
	Kind       string
	Instrument string
	OrderID    string
	Payload    json.RawMessage
}

// New opens the pool, or reports that none was asked for.
func New(config Config) (*Client, error) {
	if config.URL == "" {
		return nil, ErrNotConfigured
	}
	if config.Table == "" {
		config.Table = "journal_records"
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}

	db, err := sql.Open("postgres", config.URL)
	if err != nil {
		return nil, fmt.Errorf("pg: open: %w", err)
	}
	// One projector, one writer, modest throughput. A large pool here would
	// only mean more ways to be holding a transaction when the network goes.
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(30 * time.Minute)

	return &Client{db: db, table: config.Table}, nil
}

// Close releases the pool.
func (c *Client) Close() error { return c.db.Close() }

// Ping checks the database is actually reachable, so a misconfiguration is
// reported at startup rather than at the first batch an hour later.
func (c *Client) Ping() error { return c.db.Ping() }

// Insert writes a batch, ignoring sequences already present.
//
// Ignoring rather than refusing is the property that makes a restart safe: the
// cursor is committed only after a batch is durable, so a crash re-sends, and
// at-least-once delivery into an idempotent sink is exactly-once. The
// alternative is a two-phase commit with a service that does not offer one.
func (c *Client) Insert(rows []Row) error {
	if len(rows) == 0 {
		return nil
	}

	// One statement per batch. Five hundred round trips would make the linger
	// pointless and the projector the slowest thing in the deployment.
	var (
		values strings.Builder
		args   = make([]any, 0, len(rows)*7)
	)
	for i, r := range rows {
		if i > 0 {
			values.WriteByte(',')
		}
		n := i * 7
		fmt.Fprintf(&values, "($%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			n+1, n+2, n+3, n+4, n+5, n+6, n+7)
		args = append(args,
			int64(r.Seq), int64(r.Epoch), int64(r.Nanos), r.Kind,
			nullable(r.Instrument), nullable(r.OrderID), []byte(r.Payload))
	}

	query := fmt.Sprintf(
		`insert into %s (seq, epoch, nanos, kind, instrument, client_order_id, payload)
		 values %s on conflict (seq) do nothing`,
		c.table, values.String())

	if _, err := c.db.Exec(query, args...); err != nil {
		return fmt.Errorf("pg: insert %d rows: %w", len(rows), err)
	}
	return nil
}

// nullable keeps an absent value out of the column rather than storing "".
// An empty string and "this record has no instrument" are different facts, and
// a query filtering on instrument should not have to know the difference.
func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// Schema is the table this writes into.
//
// Printed rather than applied: creating tables from the process that also
// writes to them is how a bad migration reaches production at three in the
// morning.
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

-- The three questions actually asked of this table: what happened to this
-- order, what happened on this instrument, and what happened in this window.
create index if not exists journal_records_order on journal_records (client_order_id)
  where client_order_id is not null;
create index if not exists journal_records_instrument on journal_records (instrument, seq)
  where instrument is not null;
create index if not exists journal_records_kind on journal_records (kind, seq);
`
