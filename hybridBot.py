# hybridBot.py

import asyncio
import json
import websockets
import math
import time
import logging
from collections import deque
from dataclasses import dataclass, asdict, field
from typing import Optional, List, Dict, Tuple, Any

InstrumentID_t = str
Price_t = int
Time_t = int
Quantity_t = int
OrderID_t = str
TeamID_t = str

# ─── Message & Data Classes ────────────────────────────────────────────────────

@dataclass
class BaseMessage:
    type: str

@dataclass
class AddOrderRequest(BaseMessage):
    type: str = field(default="add_order", init=False)
    user_request_id: str
    instrument_id: InstrumentID_t
    price: Price_t
    expiry: Time_t
    side: str
    quantity: Quantity_t

@dataclass
class CancelOrderRequest(BaseMessage):
    type: str = field(default="cancel_order", init=False)
    user_request_id: str
    order_id: OrderID_t
    instrument_id: InstrumentID_t

@dataclass
class WelcomeMessage(BaseMessage):
    type: str
    message: str

@dataclass
class AddOrderResponseData:
    order_id: Optional[OrderID_t] = None
    message: Optional[str] = None
    immediate_inventory_change: Optional[Quantity_t] = None
    immediate_balance_change: Optional[Quantity_t] = None

@dataclass
class AddOrderResponse(BaseMessage):
    type: str
    user_request_id: str
    success: bool
    data: AddOrderResponseData

@dataclass
class CancelOrderResponse(BaseMessage):
    type: str
    user_request_id: str
    success: bool
    message: Optional[str] = None

@dataclass
class ErrorResponse(BaseMessage):
    type: str
    user_request_id: str
    message: str

@dataclass
class OrderbookDepth:
    bids: Dict[Price_t, Quantity_t]
    asks: Dict[Price_t, Quantity_t]

@dataclass
class MarketDataResponse(BaseMessage):
    type: str
    time: Time_t
    candles: Dict[str, Any]
    orderbook_depths: Dict[InstrumentID_t, OrderbookDepth]
    events: List[Dict[str, Any]]
    user_request_id: Optional[str] = None

@dataclass
class GetInventoryRequest(BaseMessage):
    type: str = field(default="get_inventory", init=False)
    user_request_id: str

@dataclass
class GetPendingOrdersRequest(BaseMessage):
    type: str = field(default="get_pending_orders", init=False)
    user_request_id: str

@dataclass
class GetInventoryResponse(BaseMessage):
    type: str
    user_request_id: str
    data: Dict[InstrumentID_t, Tuple[Quantity_t, Quantity_t]]

@dataclass
class OrderJSON:
    orderID: OrderID_t
    teamID: TeamID_t
    price: Price_t
    time: Time_t
    expiry: Time_t
    side: str
    unfilled_quantity: Quantity_t
    total_quantity: Quantity_t
    live: bool

@dataclass
class GetPendingOrdersResponse:
    type: str
    user_request_id: str
    data: Dict[InstrumentID_t, Tuple[List[OrderJSON], List[OrderJSON]]]

@dataclass
class CandleDataResponse:
    tradeable: Dict[InstrumentID_t, List[Dict[str, Any]]]
    untradeable: Dict[InstrumentID_t, List[Dict[str, Any]]]

Trade_t = Dict[str, Any]
Settlement_t = Dict[str, Any]
Cancel_t = Dict[str, Any]

global_user_request_id = 0

# ─── DemoTradingBot Base ───────────────────────────────────────────────────────

class DemoTradingBot:
    def __init__(self, uri: str, team_secret: str, print_market_data: bool = True):
        self.uri = f"{uri}?team_secret={team_secret}"
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._pending: Dict[str, asyncio.Future] = {}
        self.print_market_data = print_market_data

    async def send(self, payload: BaseMessage, timeout: float = 3.0):
        global global_user_request_id
        rid = str(global_user_request_id).zfill(10)
        global_user_request_id += 1
        payload.user_request_id = rid
        fut = asyncio.get_event_loop().create_future()
        self._pending[rid] = fut

        await self.ws.send(json.dumps(asdict(payload)))
        try:
            resp = await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            del self._pending[rid]
            return {"type":"error","user_request_id":rid,"message":"timeout"}

        if resp.get("type") == "add_order_response":
            resp["data"] = AddOrderResponseData(**resp["data"])
            return AddOrderResponse(**resp)
        if resp.get("type") == "cancel_order_response":
            return CancelOrderResponse(**resp)
        if resp.get("type") == "error":
            return ErrorResponse(**resp)
        return resp

# ─── HybridBot Implementation ─────────────────────────────────────────────────

logger = logging.getLogger("HybridBot")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s %(levelname)-5s %(name)s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
ch.setFormatter(fmt)
logger.addHandler(ch)
fh = logging.FileHandler("hybrid_bot.log")
fh.setLevel(logging.INFO)
fh.setFormatter(fmt)
logger.addHandler(fh)

class HybridBot(DemoTradingBot):
    def __init__(self, uri: str, team_secret: str):
        super().__init__(uri, team_secret, print_market_data=False)
        logger.info("HybridBot: initializing strategy")

        self.server_time: Optional[int] = None     # last md.time in ms
        self.future_instr: Optional[str] = None
        self.future_mid: Optional[int] = None
        self.option_mids: Dict[str, deque] = {}
        self.implied_vol: float = 0.2
        self.future_expiry: Optional[int] = None   # seconds to expiry
        self.active_quotes: Dict[str, Tuple[Any,Any]] = {}
        self.net_delta: int = 0
        self.mid_threshold: int = 10
        self.closed_instruments: set = set()

    def _get_server_time(self) -> int:
        return self.server_time or int(time.time() * 1000)

    async def connect(self):
        if self.ws:
            try: await self.ws.close()
            except: pass
        self.ws = await websockets.connect(self.uri, ping_interval=30, ping_timeout=30)
        raw = await self.ws.recv()
        w = WelcomeMessage(**json.loads(raw))
        logger.info(f"Connected: {w.message}")
        asyncio.create_task(self._receive_loop())

    async def _receive_loop(self):
        try:
            async for raw in self.ws:
                data = json.loads(raw)
                rid = data.get("user_request_id")
                if rid in self._pending:
                    self._pending[rid].set_result(data)
                    del self._pending[rid]

                if data.get("type") == "market_data_update":
                    md = MarketDataResponse(
                        type=data["type"],
                        time=data["time"],
                        candles=data.get("candles", {}),
                        orderbook_depths={iid: OrderbookDepth(**d) for iid,d in data["orderbook_depths"].items()},
                        events=data.get("events", []),
                        user_request_id=rid
                    )
                    self.server_time = md.time
                    logger.debug(f"Market tick @ {md.time}")
                    self._handle_market_data_update(md)
                else:
                    await self._handle_non_md(data)
        except Exception as e:
            logger.error(f"Receive loop error: {e}; reconnecting")
            await self.connect()

    async def _handle_non_md(self, data):
        mtype = data.get("type")
        if mtype == "add_order_response":
            resp = AddOrderResponse(**data)
            status = "OK" if resp.success else "FAIL"
            logger.info(f"add_order_response: {status} (req={resp.user_request_id})")
        elif mtype == "cancel_order_response":
            resp = CancelOrderResponse(**data)
            status = "OK" if resp.success else "FAIL"
            logger.info(f"cancel_order_response: {status} (req={resp.user_request_id})")
        elif mtype == "error":
            err = ErrorResponse(**data)
            logger.error(f"exchange error: {err.message}")

    def _handle_market_data_update(self, md: MarketDataResponse):
        elapsed_s = md.time // 1000

        # pick front‐month future once
        if not self.future_instr:
            candidates = []
            for instr in md.orderbook_depths:
                if "_future_" not in instr: continue
                try:
                    exp = int(instr.rsplit("_",1)[1])
                except:
                    continue
                if exp <= elapsed_s: continue
                candidates.append((instr,exp))
            if candidates:
                instr,exp = max(candidates,key=lambda x:x[1])
                self.future_instr, self.future_expiry = instr, exp
                logger.info(f"Selected future: {instr}, expires in {exp}s")

        # update mids
        for instr,depth in md.orderbook_depths.items():
            # mark expired
            try:
                exp = int(instr.rsplit("_",1)[1])
            except:
                exp = None
            if exp is not None and exp <= elapsed_s:
                self.closed_instruments.add(instr)
                continue

            bids = list(map(int, depth.bids))
            asks = list(map(int, depth.asks))

            if instr==self.future_instr and bids and asks:
                new_mid = (max(bids)+min(asks))//2
                if self.future_mid is None:
                    self.future_mid = new_mid
                    logger.info(f"future_mid → {new_mid}")
                else:
                    diff = abs(new_mid-self.future_mid)
                    log = logger.info if diff>=self.mid_threshold else logger.debug
                    log(f"future_mid → {new_mid}")
                    self.future_mid=new_mid

            elif self.future_instr and instr.startswith(self.future_instr.split("_")[0]):
                if bids and asks:
                    mid=(max(bids)+min(asks))//2
                    self.option_mids.setdefault(instr,deque(maxlen=50)).append(mid)

        # start trading
        if self.future_mid is not None and len(self.option_mids)>5:
            asyncio.create_task(self.main_loop())

    def _get_order_ids(self, raw, instr):
        if isinstance(raw, ErrorResponse):
            if "expiry" in raw.message.lower() or "closed" in raw.message.lower():
                self.closed_instruments.add(instr)
            return False,None
        if isinstance(raw, AddOrderResponse):
            return raw.success, raw.data.order_id
        if isinstance(raw, dict):
            return raw.get("success",False), raw.get("data",{}).get("order_id")
        return False,None

    async def safe_buy(self, instr:str, price:int):
        expiry = self._get_server_time() + 10000
        req = AddOrderRequest(
            user_request_id="",
            instrument_id=instr,
            price=price,
            expiry=expiry,
            side="bid",
            quantity=1
        )
        return await self.send(req)

    async def safe_sell(self, instr:str, price:int):
        expiry = self._get_server_time() + 10000
        req = AddOrderRequest(
            user_request_id="",
            instrument_id=instr,
            price=price,
            expiry=expiry,
            side="ask",
            quantity=1
        )
        return await self.send(req)

    async def cancel(self, instr:str, order_id:str):
        req = CancelOrderRequest(
            user_request_id="",
            order_id=order_id,
            instrument_id=instr
        )
        return await self.send(req)

    async def main_loop(self):
        self._running=True
        while True:
            try:
                # rollover on expiry
                now=time.time()
                if now - self.start_time >= self.future_expiry:
                    logger.info("Expiry passed, resetting")
                    self.future_instr=None
                    self.future_mid=None
                    self.option_mids.clear()
                    self.closed_instruments.clear()
                    return

                # select ATM strikes
                calls = sorted(
                    [i for i in self.option_mids if "_call_" in i and i not in self.closed_instruments],
                    key=lambda s: abs(int(s.rsplit("_",2)[1]) - self.future_mid)
                )[:5]
                puts = [c.replace("_call_","_put_") for c in calls]
                strikes = [s for s in calls+puts if s in self.option_mids]

                # estimate vol
                vols=[]
                for c in calls:
                    mids = self.option_mids[c]
                    if mids:
                        K=int(c.rsplit("_",2)[1])
                        vols.append(mids[-1]*math.sqrt(2*math.pi)/(self.future_mid*math.sqrt(max(1,self.future_expiry-(time.time()-self.start_time)))))
                if vols:
                    self.implied_vol=sum(vols)/len(vols)

                # quote
                spread = max(1,int(0.01*self.future_mid))
                for instr in strikes:
                    _,Ks,_=instr.rsplit("_",2)
                    K=int(Ks)
                    d1=(math.log(self.future_mid/K)+0.5*self.implied_vol**2*self.future_expiry)/(self.implied_vol*math.sqrt(self.future_expiry))
                    Nd1=0.5*(1+math.erf(d1/math.sqrt(2)))
                    fair=(self.future_mid*Nd1-K*(1-Nd1)) if "_call_" in instr else (K*(1-Nd1)-self.future_mid*(1-Nd1))
                    bid_px=max(1,int(fair-spread/2))
                    ask_px=max(bid_px+1,int(fair+spread/2))

                    # cancel
                    if instr in self.active_quotes:
                        b,a=self.active_quotes.pop(instr)
                        await self.cancel(instr,b); await self.cancel(instr,a)

                    b_raw=await self.safe_buy(instr,bid_px)
                    a_raw=await self.safe_sell(instr,ask_px)
                    b_succ,b_id=self._get_order_ids(b_raw,instr)
                    a_succ,a_id=self._get_order_ids(a_raw,instr)
                    if b_succ and a_succ:
                        logger.info(f"quoted {instr}@{bid_px}/{ask_px}")
                    self.active_quotes[instr]=(b_id,a_id)

                await asyncio.sleep(0.05)

            except Exception:
                logger.exception("Error in main_loop; continuing")

# Entrypoint
async def main():
    bot = HybridBot("ws://192.168.100.10:9001/trade","9dd0a684-c786-4500-a04b-91b777385403")
    bot.start_time = time.time()
    await bot.connect()
    await asyncio.Future()

if __name__=="__main__":
    asyncio.run(main())
