# hybridBot.py
import asyncio
import json
import websockets
import math
import time
import logging
from collections import deque

from demoTradingBot import (
    DemoTradingBot,
    MarketDataResponse,
    OrderbookDepth,
    WelcomeMessage,
    AddOrderResponse,
    CancelOrderResponse,
    GetInventoryResponse,
    GetPendingOrdersResponse,
    ErrorResponse
)

# ─── Logging Setup ─────────────────────────────────────────────────────────────
logger = logging.getLogger("HybridBot")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
fmt = logging.Formatter(
    "%(asctime)s %(levelname)-5s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
ch.setFormatter(fmt)
logger.addHandler(ch)
fh = logging.FileHandler("hybrid_bot.log", mode="a")
fh.setLevel(logging.INFO)
fh.setFormatter(fmt)
logger.addHandler(fh)
# ───────────────────────────────────────────────────────────────────────────────

class HybridBot(DemoTradingBot):
    def __init__(self, uri, team_secret):
        super().__init__(uri, team_secret, print_market_data=False)
        logger.info("HybridBot: initializing strategy")

        self.ws = None
        self.server_time = None         # latest server timestamp (ms)
        self.future_instr = None
        self.future_mid = None
        self.option_mids = {}           # instr_id -> deque([mid])
        self.implied_vol = 0.2
        self.T_remain = None
        self.strike_list = []
        self.base_instr = None
        self.future_expiry = None       # seconds to expiry
        self.active_quotes = {}         # instr_id -> (bid_id, ask_id)
        self.net_delta = 0
        self.mid_threshold = 10         # ticks before INFO
        self.closed_instruments = set()

    async def connect(self):
        if self.ws:
            try:
                await self.ws.close()
            except:
                pass
        self.ws = await websockets.connect(
            self.uri, ping_interval=30, ping_timeout=30
        )
        raw = await self.ws.recv()
        w = WelcomeMessage(**json.loads(raw))
        logger.info(f"Connected: {w.message}")
        asyncio.create_task(self._receive_loop())

    async def _receive_loop(self):
        assert self.ws, "Websocket not connected"
        try:
            async for raw in self.ws:
                data = json.loads(raw)
                rid = data.get("user_request_id")
                if rid and rid in self._pending:
                    self._pending[rid].set_result(data)
                    del self._pending[rid]

                if data.get("type") == "market_data_update":
                    depths = {iid: OrderbookDepth(**d)
                              for iid, d in data["orderbook_depths"].items()}
                    md = MarketDataResponse(
                        type=data["type"],
                        time=data["time"],
                        candles=data.get("candles", {}),
                        orderbook_depths=depths,
                        events=data.get("events", []),
                        user_request_id=rid
                    )
                    # record server time
                    self.server_time = md.time
                    logger.debug(f"Market tick @ {md.time}")
                    self._handle_market_data_update(md)
                else:
                    await self._handle_non_md(data)
        except (websockets.exceptions.ConnectionClosedError, TimeoutError) as e:
            logger.error(f"Connection closed: {e}; reconnecting")
            await self.connect()
        except Exception:
            logger.exception("Unexpected error; reconnecting")
            await self.connect()

    async def _handle_non_md(self, data):
        mtype = data.get("type")
        if mtype == "add_order_response":
            resp = AddOrderResponse(**data) if isinstance(data, dict) else data
            status = "OK" if resp.success else "FAIL"
            logger.info(f"add_order_response: {status} (req={resp.user_request_id})")
        elif mtype == "cancel_order_response":
            resp = CancelOrderResponse(**data) if isinstance(data, dict) else data
            status = "OK" if resp.success else "FAIL"
            logger.info(f"cancel_order_response: {status} (req={resp.user_request_id})")
        elif mtype == "error":
            err = ErrorResponse(**data)
            logger.error(f"exchange error: {err.message}")

    def _handle_market_data_update(self, md: MarketDataResponse):
        elapsed = md.time // 1000  # seconds since sim start
        # 1) select front-month future once, skip expired
        if not self.future_instr:
            futs = []
            for instr in md.orderbook_depths:
                if "_future_" not in instr:
                    continue
                try:
                    exp = int(instr.rsplit("_",1)[-1])
                except:
                    continue
                if exp <= elapsed:
                    continue
                futs.append((instr, exp))
            if futs:
                instr, exp = max(futs, key=lambda x: x[1])
                self.future_instr, self.future_expiry = instr, exp
                self.base_instr = instr.split("_")[0]
                logger.info(f"Selected future: {instr}, expires in {exp}s")
        # 2) update mids
        for instr, depth in md.orderbook_depths.items():
            # skip expired
            parts = instr.split("_")
            try:
                inst_exp = int(parts[-1])
            except:
                inst_exp = None
            if inst_exp is not None and inst_exp <= elapsed:
                self.closed_instruments.add(instr)
                continue
            bids = list(map(int, depth.bids))
            asks = list(map(int, depth.asks))
            # future mid
            if instr == self.future_instr and bids and asks:
                new_mid = (max(bids) + min(asks)) // 2
                if self.future_mid is None:
                    self.future_mid = new_mid
                    logger.info(f"future_mid → {new_mid}")
                else:
                    diff = abs(new_mid - self.future_mid)
                    if diff >= self.mid_threshold:
                        logger.info(f"future_mid moved by {diff} → {new_mid}")
                    else:
                        logger.debug(f"future_mid → {new_mid}")
                    self.future_mid = new_mid
            # option mids
            elif self.base_instr and (
                instr.startswith(f"{self.base_instr}_call_") or
                instr.startswith(f"{self.base_instr}_put_")
            ) and bids and asks:
                mid = (max(bids) + min(asks)) // 2
                self.option_mids.setdefault(instr, deque(maxlen=50)).append(mid)
        # 3) start trading once warmed up
        if (
            self.future_mid is not None
            and len(self.option_mids) > 5
            and not getattr(self, '_running', False)
        ):
            logger.info(f"Warm-up complete: {len(self.option_mids)} option series")
            asyncio.create_task(self.main_loop())

    def _get_order_ids(self, raw, instr):
        # parse response, handle closed instruments
        if isinstance(raw, ErrorResponse):
            msg = raw.message.lower()
            if 'closed' in msg or 'expiry' in msg:
                self.closed_instruments.add(instr)
            return False, None
        if hasattr(raw, 'success'):
            return raw.success, getattr(raw.data, 'order_id', None)
        if isinstance(raw, dict):
            return raw.get('success', False), raw.get('data', {}).get('order_id')
        return False, None

    async def safe_buy(self, instr, price):
        # expire 10s after latest server_time
        expiry = (self.server_time or int(time.time()*1000)) + 10000
        return await self.buy(instr, price, expiry)

    async def safe_sell(self, instr, price):
        expiry = (self.server_time or int(time.time()*1000)) + 10000
        return await self.sell(instr, price, expiry)

    async def main_loop(self):
        self._running = True
        while True:
            try:
                # expiry rollover
                now = time.time()
                self.T_remain = self.future_expiry - (now - self.start_time)
                if self.T_remain <= 0:
                    logger.info("Expiry reached; resetting state")
                    self._running = False
                    self.future_instr = None
                    self.future_mid = None
                    self.option_mids.clear()
                    self.closed_instruments.clear()
                    return
                logger.info(f"T_remain={self.T_remain:.1f}s")
                # dynamic ATM strikes
                calls = sorted(
                    [i for i in self.option_mids if i not in self.closed_instruments and "_call_" in i],
                    key=lambda s: abs(int(s.rsplit("_",2)[-2]) - self.future_mid)
                )[:5]
                puts = [c.replace("_call_", "_put_") for c in calls]
                self.strike_list = [s for s in calls + puts if s in self.option_mids]
                logger.info(f"Strikes: {self.strike_list}")
                # implied vol
                vols = []
                for instr in calls:
                    mids = list(self.option_mids.get(instr, []))
                    if mids:
                        K = int(instr.rsplit("_",2)[-2])
                        σ = (mids[-1] * math.sqrt(2*math.pi)) / (self.future_mid * math.sqrt(max(1, self.T_remain)))
                        vols.append(σ)
                if vols:
                    old = self.implied_vol
                    self.implied_vol = sum(vols) / len(vols)
                    logger.info(f"σ {old:.4f}→{self.implied_vol:.4f}")
                # market-making
                spread = max(1, int(0.01 * self.future_mid))
                for instr in list(self.strike_list):
                    if instr in self.closed_instruments: continue
                    _, Ks, _ = instr.rsplit("_",2)
                    K = int(Ks)
                    d1 = (math.log(self.future_mid/K) + 0.5*self.implied_vol**2*self.T_remain) / (self.implied_vol*math.sqrt(self.T_remain))
                    Nd1 = 0.5 * (1 + math.erf(d1/math.sqrt(2)))
                    fair = (self.future_mid*Nd1 - K*(1-Nd1)) if "_call_" in instr else (K*(1-Nd1) - self.future_mid*(1-Nd1))
                    bid_px = max(1, int(fair - spread/2))
                    ask_px = max(bid_px+1, int(fair + spread/2))
                    # cancel old
                    if instr in self.active_quotes:
                        b_id, a_id = self.active_quotes.pop(instr)
                        await self.cancel(instr, b_id)
                        await self.cancel(instr, a_id)
                    # place new
                    b_raw = await self.safe_buy(instr, bid_px)
                    a_raw = await self.safe_sell(instr, ask_px)
                    b_succ, b_id = self._get_order_ids(b_raw, instr)
                    a_succ, a_id = self._get_order_ids(a_raw, instr)
                    if b_succ and a_succ:
                        logger.info(f"quoted {instr}@{bid_px}/{ask_px}")
                    self.active_quotes[instr] = (b_id, a_id)
                # delta hedge
                total_delta = sum((Nd1 if "_call_" in i else -(1-Nd1)) for i in self.strike_list if i not in self.closed_instruments)
                hedge = int(round(-total_delta))
                if hedge != self.net_delta:
                    fut = self.future_instr
                    side = "buy" if hedge > self.net_delta else "sell"
                    qty = abs(hedge - self.net_delta)
                    raw = await getattr(self, f"safe_{side}")(fut, self.future_mid)
                    succ, _ = self._get_order_ids(raw, fut)
                    if succ:
                        logger.info(f"delta hedge {side} {qty}@{self.future_mid}")
                        self.net_delta = hedge
                # parity arb
                for c in calls:
                    p = c.replace("_call_","_put_")
                    if p not in self.option_mids: continue
                    midc = self.option_mids[c][-1]
                    midp = self.option_mids[p][-1]
                    mis = (midc - midp) - (self.future_mid - int(c.split("_")[-2]))
                    if abs(mis) > spread:
                        arb = "sell" if mis > 0 else "buy"
                        logger.info(f"arb mis={mis:.0f}: {arb} {c}/{p}")
                        await getattr(self, f"safe_{arb}")(c, midc)
                        await getattr(self, f"safe_{'buy' if arb=='sell' else 'sell'}")(p, midp)
                        await self.safe_buy(self.future_instr, self.future_mid)
                await asyncio.sleep(0.05)
            except Exception:
                logger.exception("Error in main_loop iteration; continuing")

# Entrypoint
async def main():
    EXCHANGE_URI = "ws://192.168.100.10:9001/trade"
    TEAM_SECRET = "9dd0a684-c786-4500-a04b-91b777385403"
    bot = HybridBot(EXCHANGE_URI, TEAM_SECRET)
    bot.start_time = time.time()
    await bot.connect()
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
