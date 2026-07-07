"""MarketHub — ONE Binance combined-stream socket for the whole fleet.

Instead of every bot opening its own websocket (2 per bot: MarketData's
@kline+@bookTicker and BarFeed's @kline — 26 sockets for 13 bots), the hub opens
a SINGLE connection subscribed to every symbol's streams and dispatches each
message to that symbol's feeds by the ``stream`` tag Binance stamps on it.

Why: 26 independent sockets each need their own keepalive pings and reconnect
loop; when the event loop gets busy the pings miss, the sockets drop, and 26
reconnect loops storm at once — starving the loop and freezing the marks. One
socket has one keepalive and one clean reconnect. Same data, ~13x less
connection overhead, and no reconnect storm.

Scope: the market-data layer only. It calls the existing MarketData / BarFeed
ingest methods; it never touches the ledger. If the hub drops, marks briefly go
stale (exactly as a single per-bot socket would) — trading state is untouched.
"""

from __future__ import annotations

import asyncio
import json
import logging

log = logging.getLogger("sentinel.market")


class MarketHub:
    def __init__(self, stream_base: str) -> None:
        self._stream = stream_base
        # symbol(lower) -> (market, bars). market is a MarketData; bars a BarFeed.
        self._feeds: dict[str, tuple] = {}
        self._ws = None
        # set whenever the wanted stream set changes (register / unregister /
        # timeframe switch) so run() reconnects with the new subscription.
        self._dirty = asyncio.Event()

    # -------------------------------------------------------------- registry

    def register(self, market, bars) -> None:
        """Attach a bot's feeds. market.on_change should already be wired to the
        UI bump. The hub drives them via their _ingest_* methods."""
        market._hub = self                       # so set_interval can re-sub
        self._feeds[market.symbol.lower()] = (market, bars)
        self._touch()

    def unregister(self, symbol: str) -> None:
        if self._feeds.pop(symbol.lower(), None) is not None:
            self._touch()

    def _touch(self) -> None:
        """Mark the subscription dirty and drop the live socket so run()
        reconnects with the new stream set (debounced there)."""
        self._dirty.set()
        ws = self._ws
        if ws is not None:
            asyncio.create_task(self._close(ws))

    @staticmethod
    async def _close(ws) -> None:
        try:
            await ws.close()
        except Exception:  # noqa: BLE001
            pass

    def _wanted(self) -> list[str]:
        streams: set[str] = set()
        for sym, (md, bf) in list(self._feeds.items()):
            streams.add(f"{sym}@bookTicker")
            streams.add(f"{sym}@kline_{md.interval}")   # chart candles + mark
            streams.add(f"{sym}@kline_{bf.interval}")   # strategy bars (deduped)
        return sorted(streams)

    # -------------------------------------------------------------- dispatch

    def _dispatch(self, stream: str, data: dict):
        """Route one combined-stream message to its symbol's feeds. Returns the
        MarketData if it was updated (so the caller bumps the UI), else None."""
        sym, _, rest = stream.partition("@")
        feed = self._feeds.get(sym)
        if feed is None:
            return None
        md, bf = feed
        if rest == "bookTicker":
            md._ingest_book(data)
            return md
        if rest.startswith("kline_"):
            interval = rest[len("kline_"):]
            k = data.get("k")
            if k:
                touched = False
                if md.interval == interval:     # chart timeframe kline -> mark
                    md._ingest_kline(k)
                    touched = True
                if bf.interval == interval:     # strategy's fixed-interval bar
                    bf._ingest(k)
                return md if touched else None
        return None

    # -------------------------------------------------------------- lifecycle

    async def run(self) -> None:
        """Supervised task: one socket for the whole fleet, reconnect forever."""
        import websockets

        backoff = 1.0
        while True:
            if not self._feeds:                 # nothing registered yet
                await asyncio.sleep(0.5)
                continue
            self._dirty.clear()
            url = f"{self._stream}/stream?streams=" + "/".join(self._wanted())
            try:
                async with websockets.connect(url) as ws:
                    self._ws = ws
                    backoff = 1.0
                    log.info("market hub: %d streams on one socket",
                             len(self._wanted()))
                    async for raw in ws:
                        if self._dirty.is_set():
                            break               # roster/interval changed -> resub
                        msg = json.loads(raw)
                        stream = msg.get("stream")
                        if not stream:
                            continue            # subscribe ack / control frame
                        md = self._dispatch(stream, msg.get("data", {}))
                        if md is not None:
                            await md._bump_throttled()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                if self._dirty.is_set():
                    pass                        # deliberate close to re-subscribe
                else:
                    log.warning("market hub dropped (%r); reconnecting", e)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)
            finally:
                self._ws = None
            # debounce: let a burst of registrations (e.g. seeding) settle so we
            # reconnect once with the full set rather than once per bot.
            if self._dirty.is_set():
                await asyncio.sleep(0.4)
