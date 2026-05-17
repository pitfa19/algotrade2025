import asyncio
import json
import websockets
from dataclasses import dataclass, asdict, field
from typing import Optional, List, Dict, Tuple, Any

InstrumentID_t = str
Price_t = int
Time_t = int
Quantity_t = int
OrderID_t = str
TeamID_t = str

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
class OrderbookDepth:
    bids: Dict[Price_t, Quantity_t]
    asks: Dict[Price_t, Quantity_t]

@dataclass
class CandleDataResponse:
    tradeable: Dict[InstrumentID_t, List[Dict[str, Any]]]
    untradeable: Dict[InstrumentID_t, List[Dict[str, Any]]]

Trade_t = Dict[str, Any]
Settlement_t = Dict[str, Any]
Cancel_t = Dict[str, Any]

@dataclass
class MarketDataResponse(BaseMessage):
    type: str
    time: Time_t
    candles: CandleDataResponse
    orderbook_depths: Dict[InstrumentID_t, OrderbookDepth]
    events: List[Dict[str, Any]]
    user_request_id: Optional[str] = None

global_user_request_id = 0

class DemoTradingBot:
    def __init__(self, uri: str, team_secret: str, print_market_data: bool = True):
        self.uri = f"{uri}?team_secret={team_secret}"
        self.ws = None
        self._pending: Dict[str, asyncio.Future] = {}
        self._trade_sequence_triggered = False
        self.print_market_data = print_market_data
        self._instrument_ids = set()
        # track latest server timestamp
        self._last_server_time: Time_t = 0

    async def connect(self):
        self.ws = await websockets.connect(self.uri)
        welcome_data = json.loads(await self.ws.recv())
        welcome_message = WelcomeMessage(**welcome_data)
        print(json.dumps({"welcome": asdict(welcome_message)}, indent=2))
        asyncio.create_task(self._receive_loop())

    async def _receive_loop(self):
        assert self.ws, "Websocket connection not established."
        async for msg in self.ws:
            data = json.loads(msg)

            # print unless market updates are being silenced
            if not (data.get("type") == "market_data_update" and not self.print_market_data):
                print(json.dumps({"message": data}, indent=2))

            # fulfill any pending request futures
            rid = data.get("user_request_id")
            if rid and rid in self._pending:
                self._pending[rid].set_result(data)
                del self._pending[rid]

            # update last seen server time on market_data_update
            if data.get("type") == "market_data_update":
                self._last_server_time = data.get("time", self._last_server_time)

                try:
                    parsed_orderbook_depths = {
                        instr_id: OrderbookDepth(**depth_data)
                        for instr_id, depth_data in data.get("orderbook_depths", {}).items()
                    }
                    candles_data = data.get("candles", {})
                    parsed_candles = CandleDataResponse(
                        tradeable=candles_data.get("tradeable", {}),
                        untradeable=candles_data.get("untradeable", {})
                    )
                    market_data = MarketDataResponse(
                        type=data["type"],
                        time=data["time"],
                        candles=parsed_candles,
                        orderbook_depths=parsed_orderbook_depths,
                        events=data.get("events", []),
                        user_request_id=data.get("user_request_id")
                    )
                    self._handle_market_data_update(market_data)
                except KeyError as e:
                    print(f"Error: Missing expected key in MarketDataResponse: {e}. Data: {data}")
                except Exception as e:
                    print(f"Error deserializing MarketDataResponse: {e}. Data: {data}")

    async def get_server_time(self) -> Time_t:
        """
        Returns the latest server timestamp (from the last market_data_update).
        """
        return self._last_server_time

    # ... the rest of your DemoTradingBot methods (send, buy, sell, cancel, run_sequence, etc.) remain unchanged.

    async def send(self, payload: BaseMessage, timeout: int = 3):
        global global_user_request_id
        rid = str(global_user_request_id).zfill(10)
        global_user_request_id += 1

        payload.user_request_id = rid
        payload_dict = asdict(payload)

        fut = asyncio.get_event_loop().create_future()
        self._pending[rid] = fut

        await self.ws.send(json.dumps(payload_dict))
        print(json.dumps({"sent": payload_dict}, indent=2))

        try:
            resp = await asyncio.wait_for(fut, timeout)
            # ... deserialization logic as before ...
            return resp  # simplified here for brevity
        except asyncio.TimeoutError:
            if rid in self._pending:
                del self._pending[rid]
            print(json.dumps({"error": "timeout", "user_request_id": rid}, indent=2))
            return {"success": False, "user_request_id": rid, "message": "Request timed out"}

    # (buy, sell, cancel, get_inventory, get_pending_orders, run_sequence)


# Example entrypoint remains the same
async def main():
    EXCHANGE_URI = "ws://192.168.100.10:9001/trade"
    TEAM_SECRET = "9dd0a684-c786-4500-a04b-91b777385403"

    bot = DemoTradingBot(EXCHANGE_URI, TEAM_SECRET, print_market_data=True)
    await bot.connect()
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
