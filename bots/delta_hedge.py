import asyncio
import json
import time
import websockets
import logging
from datetime import datetime
from dataclasses import dataclass, asdict, field
from typing import Optional, List, Dict, Tuple, Any

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trading.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

InstrumentID_t = str
Price_t = int
Time_t = int
Quantity_t = int
OrderID_t = str
TeamID_t = str

# --- Message Definitions ---
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
class GetInventoryRequest(BaseMessage):
    type: str = field(default="get_inventory", init=False)
    user_request_id: str

@dataclass
class GetPendingOrdersRequest(BaseMessage):
    type: str = field(default="get_pending_orders", init=False)
    user_request_id: str

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
class GetInventoryResponse(BaseMessage):
    type: str
    user_request_id: str
    data: Dict[InstrumentID_t, Tuple[int,int]]

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
class GetPendingOrdersResponse(BaseMessage):
    type: str
    user_request_id: str
    data: Dict[InstrumentID_t, Tuple[List[OrderJSON], List[OrderJSON]]]

@dataclass
class OrderbookDepth:
    bids: Dict[Price_t, Quantity_t]
    asks: Dict[Price_t, Quantity_t]

@dataclass
class CandleDataResponse:
    tradeable: Dict[InstrumentID_t, List[Dict[str, Any]]]
    untradeable: Dict[InstrumentID_t, List[Dict[str, Any]]]

@dataclass
class MarketDataResponse(BaseMessage):
    type: str
    time: Time_t
    candles: CandleDataResponse
    orderbook_depths: Dict[InstrumentID_t, OrderbookDepth]
    events: List[Dict[str, Any]]
    user_request_id: Optional[str] = None

# --- Bot Implementation ---
global_user_request_id = 0

class DemoTradingBot:
    def __init__(self, uri: str, team_secret: str):
        self.uri = f"{uri}?team_secret={team_secret}"
        self.ws = None
        self._pending: Dict[str, asyncio.Future] = {}
        self._instruments_selected = False
        # To store instrument ids and strike
        self.future_instr: Optional[str] = None
        self.call_instr: Optional[str] = None
        self.put_instr: Optional[str] = None
        self.strike: Optional[int] = None
        logger.info("Trading bot initialized")

    async def connect(self):
        try:
            self.ws = await websockets.connect(self.uri)
            welcome = json.loads(await self.ws.recv())
            logger.info(f"Connected to exchange: {welcome.get('message')}")
            asyncio.create_task(self._receive_loop())
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise

    async def _receive_loop(self):
        try:
            async for msg in self.ws:
                data = json.loads(msg)
                if data.get("type") == "market_data_update":
                    try:
                        md = MarketDataResponse(
                            type=data["type"], time=data["time"],
                            candles=CandleDataResponse(**data.get("candles", {})),
                            orderbook_depths={iid: OrderbookDepth(**d) for iid, d in data.get("orderbook_depths", {}).items()},
                            events=data.get("events", []),
                            user_request_id=data.get("user_request_id")
                        )
                        await self._handle_market_data(md)
                    except Exception as e:
                        logger.error(f"MarketData parse error: {e}")
                else:
                    rid = data.get("user_request_id")
                    if rid and rid in self._pending:
                        self._pending[rid].set_result(data)
                        del self._pending[rid]
                    logger.debug(f"Received message: {data}")
        except Exception as e:
            logger.error(f"Error in receive loop: {e}")
            raise

    async def _handle_market_data(self, md: MarketDataResponse):
        # On first update, pick ATM strike and instruments
        if not self._instruments_selected:
            # Find all calls and puts for the latest timeframe
            instruments = md.orderbook_depths.keys()
            timeframes = {instr.split('_')[-1] for instr in instruments if '_' in instr}
            latest_timeframe = max(timeframes)
            
            calls = [iid for iid in instruments if iid.startswith("$SIMP_call_") and iid.endswith(f"_{latest_timeframe}")]
            puts = [iid for iid in instruments if iid.startswith("$SIMP_put_") and iid.endswith(f"_{latest_timeframe}")]
            
            if not calls or not puts:
                logger.error(f"No options found for timeframe {latest_timeframe}")
                return
                
            # Get the strike prices from the first call and put
            call_strike = int(calls[0].split('_')[2])
            put_strike = int(puts[0].split('_')[2])
            
            # Use the strike price that exists in both calls and puts
            self.strike = call_strike if call_strike == put_strike else None
            
            if self.strike is None:
                logger.error("No matching strike prices found between calls and puts")
                return
                
            # Set the instrument IDs
            self.call_instr = f"$SIMP_call_{self.strike}_{latest_timeframe}"
            self.put_instr = f"$SIMP_put_{self.strike}_{latest_timeframe}"
            
            # Find the corresponding future
            futures = [iid for iid in instruments if iid.startswith("$SIMP_future_") and iid.endswith(f"_{latest_timeframe}")]
            if not futures:
                logger.error(f"No future found for timeframe {latest_timeframe}")
                return
            self.future_instr = futures[0]
            
            logger.info(f"Selected instruments - Future: {self.future_instr}, Strike: {self.strike}")
            
            try:
                # Get the mid price from the future
                future_depth = md.orderbook_depths[self.future_instr]
                best_bid = max(future_depth.bids.keys()) if future_depth.bids else 0
                best_ask = min(future_depth.asks.keys()) if future_depth.asks else 0
                mid = (int(best_bid) + int(best_ask)) // 2
                
                # buy call & put
                call_resp = await self.buy(self.call_instr, mid)
                if isinstance(call_resp, AddOrderResponse) and call_resp.success:
                    logger.info(f"Call order placed - OrderID: {call_resp.data.order_id}")
                
                put_resp = await self.buy(self.put_instr, mid)
                if isinstance(put_resp, AddOrderResponse) and put_resp.success:
                    logger.info(f"Put order placed - OrderID: {put_resp.data.order_id}")
                
                self._instruments_selected = True
            except Exception as e:
                logger.error(f"Error placing orders: {e}")
                return

        # After entry, maintain delta hedge each update
        depth = md.orderbook_depths.get(self.future_instr)
        if not depth or not depth.bids or not depth.asks:
            return
            
        bid, ask = max(depth.bids), min(depth.asks)
        mid = (bid + ask)//2
        
        # Decide hedge: if mid > strike -> want short future; if mid < strike -> long future
        desired_pos = 0
        if mid > self.strike: 
            desired_pos = -1
        elif mid < self.strike: 
            desired_pos = 1
            
        # Get current inventory
        inv = await self.get_inventory()
        curr = 0
        if isinstance(inv, GetInventoryResponse):
            curr = inv.data.get(self.future_instr, (0,0))[1]
        
        # Adjust position if needed
        if curr < desired_pos:
            resp = await self.buy(self.future_instr, mid)
            if isinstance(resp, AddOrderResponse) and resp.success:
                logger.info(f"Hedge: Bought future @ {mid}")
        elif curr > desired_pos:
            resp = await self.sell(self.future_instr, mid)
            if isinstance(resp, AddOrderResponse) and resp.success:
                logger.info(f"Hedge: Sold future @ {mid}")

    async def send(self, payload: BaseMessage, timeout: int = 3):
        global global_user_request_id
        rid = str(global_user_request_id).zfill(8)
        global_user_request_id += 1
        payload.user_request_id = rid
        fut = asyncio.get_event_loop().create_future()
        self._pending[rid] = fut
        logger.debug(f"Sending request: {asdict(payload)}")
        await self.ws.send(json.dumps(asdict(payload)))
        try:
            data = await asyncio.wait_for(fut, timeout)
            logger.debug(f"Received response: {data}")
            return data
        except asyncio.TimeoutError:
            logger.error(f"Request timed out after {timeout} seconds")
            raise

    async def buy(self, instr: str, price: int):
        logger.info(f"Placing buy order for {instr} at {price}")
        req = AddOrderRequest(user_request_id="", instrument_id=instr, price=price, quantity=1, side="bid", expiry=int(time.time()*1000)+60000)
        return await self.send(req)

    async def sell(self, instr: str, price: int):
        logger.info(f"Placing sell order for {instr} at {price}")
        req = AddOrderRequest(user_request_id="", instrument_id=instr, price=price, quantity=1, side="ask", expiry=int(time.time()*1000)+60000)
        return await self.send(req)

    async def get_inventory(self):
        logger.debug("Requesting inventory")
        req = GetInventoryRequest(user_request_id="")
        data = await self.send(req)
        try:
            return GetInventoryResponse(**data)
        except Exception as e:
            logger.error(f"Failed to parse inventory response: {e}")
            return data

async def main():
    try:
        logger.info("Starting trading bot")
        bot = DemoTradingBot("ws://192.168.100.10:9001/trade", "9dd0a684-c786-4500-a04b-91b777385403")
        await bot.connect()
        await asyncio.Future()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == '__main__':
    asyncio.run(main())
 