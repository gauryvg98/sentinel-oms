# Bitquery queries — the Phase 0 price tape

Drafts shaped to Bitquery's documented Solana schema. **Run them and adjust before
trusting the field names** — they have not been executed here (no API key), and the
one genuinely uncertain thing is whether Bitquery indexes **Token-2022** transfers.
Both CASH and World's outcome tokens are Token-2022, so if it only indexes classic
SPL, the tape is incomplete and this whole route is dead. `01-probe.graphql` exists
to answer that in one call, before any other work.

Endpoint: `https://streaming.bitquery.io/eap` (`Authorization: Bearer <token>`).
Turn any query into a live stream by replacing `query` with `subscription`.
