# algotrade2025

Async Python trading bots for the **AlgoTrade 2025** competition exchange. Bots speak the venue's WebSocket protocol directly — quoting markets, hedging deltas, and pricing options in real time.

## Bots

| Bot | Strategy |
| --- | --- |
| `bots.market_maker` | Quotes both sides of every instrument around the mid, with inventory-aware skew and a per-second send rate limiter. |
| `bots.hybrid` | Selects a front-month future, fits an implied vol from nearby option mids, and continuously two-sided-quotes the five ATM strikes. |
| `bots.delta_hedge` | Buys an ATM straddle on the first tick, then delta-hedges with the underlying future on every market update. |

All three inherit the shared protocol layer in `bots/base.py` (dataclass message types, async send/await request matching, market-data parsing).

## Layout

```
.
├── bots/
│   ├── base.py          # protocol dataclasses + DemoTradingBot base client
│   ├── market_maker.py  # inventory-skewed market maker
│   ├── hybrid.py        # future + ATM options market maker
│   └── delta_hedge.py   # straddle with continuous delta hedging
├── scripts/
│   └── ping.py          # minimal WS connection sanity check
└── requirements.txt
```

## Quickstart

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Run any bot as a module:

```bash
python -m bots.market_maker
python -m bots.hybrid
python -m bots.delta_hedge
python scripts/ping.py
```

Each entrypoint connects to the exchange URI and team secret defined at the bottom of the file — edit those before running against a different venue.

## Protocol notes

The exchange streams `market_data_update` messages over a single WebSocket; every client request carries a `user_request_id` that the server echoes back on the response. `bots/base.py` matches responses to pending futures by that ID, so callers `await` request results directly.

Instrument IDs follow the pattern `$SYMBOL_<future|call|put>_<strike?>_<expiry-seconds>`, e.g. `$SIMP_call_100_300`. Strategies parse these to discover the universe live rather than hardcoding it.
