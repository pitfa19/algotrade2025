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
fmt = logging.Formatter("%(asctime)s %(levelname)-5s %(name)s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
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
        self.future_instr   = None     # full instr id
        self.future_mid     = None
        self.option_mids    = {}       # instr_id → deque([mid], maxlen=50)
        self.implied_vol    = 0.2
        self.T_remain       = None
        self.strike_list    = []
        self.base_instr     = None
        self.future_expiry  = None     # seconds to expiry
        self.active_quotes  = {}       # instr_id → (bid_id, ask_id)
        self.net_delta      = 0
        self.mid_threshold  = 10       # ticks before logging at INFO

    async def connect(self):
        # reopen ws with longer ping timeout
        if self.ws:
            try: await self.ws.close()
            except: pass
        self.ws = await websockets.connect(self.uri,
                                           ping_interval=30,
                                           ping_timeout=30)
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

                mtype = data.get("type")
                if mtype == "market_data_update":
                    depths = {iid: OrderbookDepth(**d)
                              for iid, d in data["orderbook_depths"].items()}
                    md = MarketDataResponse(
                        type=mtype,
                        time=data["time"],
                        candles=data.get("candles", {}),
                        orderbook_depths=depths,
                        events=data.get("events", []),
                        user_request_id=rid
                    )
                    logger.debug(f"Market tick @ {md.time}")
                    self._handle_market_data_update(md)
                else:
                    await self._handle_non_md(data, mtype)

        except (websockets.exceptions.ConnectionClosedError, TimeoutError) as e:
            logger.error(f"Receive loop error ({e}); reconnecting")
            await self.connect()
        except Exception:
            logger.exception("Unexpected error in receive loop; reconnecting")
            await self.connect()

    async def _handle_non_md(self, data, mtype):
        if mtype == "add_order_response":
            resp = AddOrderResponse(**data) if isinstance(data, dict) else data
            status = "OK" if getattr(resp, "success", False) else "FAIL"
            logger.info(f"add_order_response: {status} (req={resp.user_request_id})")
        elif mtype == "cancel_order_response":
            resp = CancelOrderResponse(**data) if isinstance(data, dict) else data
            status = "OK" if getattr(resp, "success", False) else "FAIL"
            logger.info(f"cancel_order_response: {status} (req={resp.user_request_id})")
        elif mtype == "get_inventory_response":
            inv = GetInventoryResponse(**data)
            logger.info(f"inventory: {inv.data}")
        elif mtype == "get_pending_orders_response":
            pend = GetPendingOrdersResponse(**data)
            summary = {instr: (len(b), len(a))
                       for instr,(b,a) in pend.data.items()}
            logger.info(f"pending_orders: {summary}")
        elif mtype == "error":
            err = ErrorResponse(**data)
            logger.error(f"exchange error: {err.message}")
        else:
            evt = data.get("event_type", mtype)
            logger.info(f"event {evt}: {data.get('data', {})}")

    def _handle_market_data_update(self, md: MarketDataResponse):
        # 1) pick front-month future once
        if not self.future_instr:
            futs = [(instr, int(instr.rsplit("_",1)[-1]))
                    for instr in md.orderbook_depths
                    if "_future_" in instr]
            if futs:
                instr, exp = max(futs, key=lambda x: x[1])
                self.future_instr  = instr
                self.future_expiry = exp
                self.base_instr    = instr.split("_")[0]
                logger.info(f"Selected future: {instr}, expires in {exp}s")

        # 2) update mids
        for instr, depth in md.orderbook_depths.items():
            bids = list(map(int, depth.bids))
            asks = list(map(int, depth.asks))

            # only track that future
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

            # update option mids
            elif self.base_instr and (
                instr.startswith(f"{self.base_instr}_call_") or
                instr.startswith(f"{self.base_instr}_put_")
            ) and bids and asks:
                mid = (max(bids) + min(asks)) // 2
                dq = self.option_mids.setdefault(instr, deque(maxlen=50))
                dq.append(mid)

        # 3) start trading once warmed up
        if (self.future_mid is not None
            and len(self.option_mids) > 5
            and not getattr(self, "_running", False)):
            logger.info(f"Warm-up complete: {len(self.option_mids)} option series")
            asyncio.create_task(self.main_loop())

    async def safe_buy(self, instr, price):
        try:
            return await self.buy(instr, price)
        except (websockets.exceptions.ConnectionClosedError, TimeoutError) as e:
            logger.error(f"Buy failed ({e}); reconnecting")
            await self.connect()
            return {}

    async def safe_sell(self, instr, price):
        try:
            return await self.sell(instr, price)
        except (websockets.exceptions.ConnectionClosedError, TimeoutError) as e:
            logger.error(f"Sell failed ({e}); reconnecting")
            await self.connect()
            return {}

    async def main_loop(self):
        self._running = True
        now = time.time()
        self.T_remain = max(1, self.future_expiry - (now - self.start_time))
        logger.info(f"Trading loop start; T_remain={self.T_remain:.1f}s")

        # select ATM calls & puts
        calls = sorted(
            [i for i in self.option_mids if "_call_" in i],
            key=lambda s: abs(int(s.split("_")[-2]) - self.future_mid)
        )[:5]
        puts = [c.replace("_call_","_put_") for c in calls]
        self.strike_list = calls + puts
        logger.info(f"Using strikes: {self.strike_list}")

        try:
            while True:
                # 1) re-estimate implied vol
                vols = []
                for instr in calls:
                    K = int(instr.split("_")[-2])
                    mids = list(self.option_mids[instr])
                    if mids:
                        σ = mids[-1] * math.sqrt(2*math.pi) / (
                            self.future_mid * math.sqrt(self.T_remain)
                        )
                        vols.append(σ)
                if vols:
                    old = self.implied_vol
                    self.implied_vol = sum(vols) / len(vols)
                    logger.info(f"σ → {self.implied_vol:.4f}")

                # 2) quote each strike
                for instr in self.strike_list:
                    _, K_s, _ = instr.rsplit("_", 2)
                    K = int(K_s)
                    d1 = (
                        math.log(self.future_mid / K)
                        + 0.5 * self.implied_vol**2 * self.T_remain
                    ) / (self.implied_vol * math.sqrt(self.T_remain))
                    N_d1 = 0.5*(1+math.erf(d1/math.sqrt(2)))
                    fair = (
                        self.future_mid * N_d1 - K*(1-N_d1)
                    ) if "_call_" in instr else (
                        K*(1-N_d1) - self.future_mid*(1-N_d1)
                    )
                    spread = max(1, int(0.01 * self.future_mid))
                    skew   = int(self.net_delta * 0.1)

                    # clamp prices to ≥1, ensure ask > bid
                    bid_px = max(1, int(fair - spread/2 + skew))
                    ask_px = max(bid_px+1, int(fair + spread/2 + skew))

                    # cancel old quotes
                    if instr in self.active_quotes:
                        b_id, a_id = self.active_quotes.pop(instr)
                        await self.cancel(instr, b_id)
                        await self.cancel(instr, a_id)

                    # post via safe methods
                    b_raw = await self.safe_buy(instr, bid_px)
                    a_raw = await self.safe_sell(instr, ask_px)

                    # extract success & order_id
                    if isinstance(b_raw, dict):
                        b_succ = b_raw.get("success", False)
                        b_id   = b_raw.get("data", {}).get("order_id")
                    else:
                        b_succ = getattr(b_raw, "success", False)
                        b_id   = getattr(b_raw.data, "order_id", None)

                    if isinstance(a_raw, dict):
                        a_succ = a_raw.get("success", False)
                        a_id   = a_raw.get("data", {}).get("order_id")
                    else:
                        a_succ = getattr(a_raw, "success", False)
                        a_id   = getattr(a_raw.data, "order_id", None)

                    if b_succ and a_succ:
                        logger.info(f"quoted {instr} @ {bid_px}/{ask_px}")
                    self.active_quotes[instr] = (b_id, a_id)

                # 3) delta-hedge
                total_delta = sum(
                    (N_d1 if "_call_" in inst else -(1-N_d1))
                    for inst in self.strike_list
                )
                hedge = int(round(-total_delta))
                if hedge != self.net_delta:
                    fut  = self.future_instr
                    side = "buy" if hedge > self.net_delta else "sell"
                    qty  = abs(hedge - self.net_delta)
                    h_raw = await getattr(self, f"safe_{side}")(fut, self.future_mid)
                    h_succ = (h_raw.get("success", False)
                              if isinstance(h_raw, dict)
                              else getattr(h_raw, "success", False))
                    if h_succ:
                        logger.info(f"delta hedge {side} {qty} @ {self.future_mid}")
                        self.net_delta = hedge

                # 4) parity arb
                for c in calls:
                    p    = c.replace("_call_","_put_")
                    midc = self.option_mids[c][-1]
                    midp = self.option_mids[p][-1]
                    mis  = (midc - midp) - (self.future_mid - int(c.split("_")[-2]))
                    if abs(mis) > spread:
                        arb = "sell" if mis>0 else "buy"
                        logger.info(f"arb mis={mis:.0f}: {arb} {c}/{p}")
                        if mis>0:
                            await self.safe_sell(c, midc)
                            await self.safe_buy(p, midp)
                        else:
                            await self.safe_buy(c, midc)
                            await self.safe_sell(p, midp)
                        # quick hedge
                        await self.safe_buy(self.future_instr, self.future_mid)

                await asyncio.sleep(0.05)

        except Exception:
            logger.exception("main_loop crashed")

# Entrypoint
async def main():
    EXCHANGE_URI = "ws://192.168.100.10:9001/trade"
    TEAM_SECRET  = "9dd0a684-c786-4500-a04b-91b777385403"
    bot = HybridBot(EXCHANGE_URI, TEAM_SECRET)
    bot.start_time = time.time()
    await bot.connect()
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
