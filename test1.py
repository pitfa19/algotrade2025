import asyncio
import json
import websockets
from demoTradingBot import (
    BaseMessage, AddOrderRequest, GetInventoryRequest, GetPendingOrdersRequest,
    WelcomeMessage, AddOrderResponseData, AddOrderResponse, GetInventoryResponse,
    OrderJSON, GetPendingOrdersResponse, InstrumentID_t, Price_t, Time_t,
    Quantity_t, OrderID_t, TeamID_t, global_user_request_id
)

class SimpleTradingBot:
    def __init__(self, uri: str, team_secret: str):
        self.uri = f"{uri}?team_secret={team_secret}"
        self.ws = None
        self._pending: Dict[str, asyncio.Future] = {}

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
            print(json.dumps({"message": data}, indent=2))

            rid = data.get("user_request_id")
            if rid and rid in self._pending:
                self._pending[rid].set_result(data)
                del self._pending[rid]

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
            if resp.get("type") == "add_order_response":
                resp['data'] = AddOrderResponseData(**resp.get('data', {}))
                return AddOrderResponse(**resp)
            elif resp.get("type") == "get_inventory_response":
                return GetInventoryResponse(**resp)
            elif resp.get("type") == "get_pending_orders_response":
                parsed_data = {}
                for instr_id, (bids_raw, asks_raw) in resp.get('data', {}).items():
                    parsed_bids = [OrderJSON(**order_data) for order_data in bids_raw]
                    parsed_asks = [OrderJSON(**order_data) for order_data in asks_raw]
                    parsed_data[instr_id] = (parsed_bids, parsed_asks)
                resp['data'] = parsed_data
                return GetPendingOrdersResponse(**resp)
            else:
                return resp
        except asyncio.TimeoutError:
            if rid in self._pending:
                del self._pending[rid]
            print(json.dumps({"error": "timeout", "user_request_id": rid}, indent=2))
            return {"success": False, "user_request_id": rid, "message": "Request timed out"}

    async def buy(self, instr: InstrumentID_t, price: Price_t, quantity: Quantity_t):
        expiry = int(instr.split("_")[-1]) + 10
        buy_request = AddOrderRequest(
            user_request_id="",
            instrument_id=instr,
            price=price,
            quantity=quantity,
            side="bid",
            expiry=expiry * 1000
        )
        return await self.send(buy_request)

    async def get_pending_orders(self):
        get_pending_request = GetPendingOrdersRequest(user_request_id="")
        return await self.send(get_pending_request)

    async def get_inventory(self):
        get_inventory_request = GetInventoryRequest(user_request_id="")
        return await self.send(get_inventory_request)

    async def run_trading_sequence(self):
        try:
            print("\n--- Starting Trading Sequence ---")
            
            # 1. Get initial inventory
            print("1) Getting initial inventory...")
            initial_inventory = await self.get_inventory()
            if isinstance(initial_inventory, GetInventoryResponse):
                print("Initial Inventory:")
                for instr, (reserved, owned) in initial_inventory.data.items():
                    print(f"  {instr}: {owned} owned, {reserved} reserved")
            else:
                print("Failed to get initial inventory")
                return

            # 2. Place buy order for CARD future
            target_instrument = "$CARD_20240315"  # Example instrument ID
            target_price = 50
            target_quantity = 1
            
            print(f"\n2) Placing buy order for {target_quantity} {target_instrument} at {target_price}...")
            buy_resp = await self.buy(target_instrument, target_price, target_quantity)
            
            if isinstance(buy_resp, AddOrderResponse) and buy_resp.success:
                print(f"Buy Order SUCCESS. OrderID: {buy_resp.data.order_id}")
                if buy_resp.data.immediate_inventory_change:
                    print(f"Immediate inventory change: {buy_resp.data.immediate_inventory_change}")
                if buy_resp.data.immediate_balance_change:
                    print(f"Immediate balance change: {buy_resp.data.immediate_balance_change}")
            else:
                print("Buy Order FAILED")
                return

            # 3. Get updated inventory
            print("\n3) Getting updated inventory...")
            updated_inventory = await self.get_inventory()
            if isinstance(updated_inventory, GetInventoryResponse):
                print("Updated Inventory:")
                for instr, (reserved, owned) in updated_inventory.data.items():
                    print(f"  {instr}: {owned} owned, {reserved} reserved")
            else:
                print("Failed to get updated inventory")

            # 4. Get pending orders
            print("\n4) Getting pending orders...")
            pending_orders = await self.get_pending_orders()
            if isinstance(pending_orders, GetPendingOrdersResponse):
                print("Pending Orders:")
                for instr, (bids, asks) in pending_orders.data.items():
                    if bids:
                        print(f"  Bids for {instr}:")
                        for bid in bids:
                            print(f"    OrderID: {bid.orderID}, Price: {bid.price}, Quantity: {bid.unfilled_quantity}")
                    if asks:
                        print(f"  Asks for {instr}:")
                        for ask in asks:
                            print(f"    OrderID: {ask.orderID}, Price: {ask.price}, Quantity: {ask.unfilled_quantity}")
            else:
                print("Failed to get pending orders")

            print("\n--- Trading Sequence Complete ---")

        except Exception as e:
            print(f"An unexpected error occurred: {e}")

async def main():
    EXCHANGE_URI = "ws://192.168.100.10:9001/trade"
    TEAM_SECRET = "9dd0a684-c786-4500-a04b-91b777385403"

    bot = SimpleTradingBot(
        EXCHANGE_URI,
        TEAM_SECRET
    )

    await bot.connect()
    await bot.run_trading_sequence()
    await asyncio.Future()  # Keep the connection alive

if __name__ == '__main__':
    asyncio.run(main()) 