import asyncio
import json
from datetime import datetime
import websockets
from dataclasses import asdict
from typing import Dict

from demoTradingBot import (
    BaseMessage, GetInventoryRequest, GetPendingOrdersRequest,
    WelcomeMessage, GetInventoryResponse, OrderJSON, GetPendingOrdersResponse,
    InstrumentID_t, Price_t, Time_t, Quantity_t, OrderID_t, TeamID_t, global_user_request_id
)

class InfoPoller:
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

        try:
            resp = await asyncio.wait_for(fut, timeout)
            if resp.get("type") == "get_inventory_response":
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
            return {"success": False, "user_request_id": rid, "message": "Request timed out"}

    async def get_pending_orders(self):
        get_pending_request = GetPendingOrdersRequest(user_request_id="")
        return await self.send(get_pending_request)

    async def get_inventory(self):
        get_inventory_request = GetInventoryRequest(user_request_id="")
        return await self.send(get_inventory_request)

    def print_inventory(self, inventory: GetInventoryResponse):
        print("\n=== Current Inventory ===")
        for instr, (reserved, owned) in inventory.data.items():
            print(f"{instr}:")
            print(f"  Owned: {owned}")
            print(f"  Reserved: {reserved}")

    def print_pending_orders(self, orders: GetPendingOrdersResponse):
        print("\n=== Pending Orders ===")
        for instr, (bids, asks) in orders.data.items():
            if bids or asks:
                print(f"\n{instr}:")
                if bids:
                    print("  Bids:")
                    for bid in bids:
                        print(f"    OrderID: {bid.orderID}")
                        print(f"    Price: {bid.price}")
                        print(f"    Quantity: {bid.unfilled_quantity}/{bid.total_quantity}")
                        print(f"    Expiry: {bid.expiry}")
                if asks:
                    print("  Asks:")
                    for ask in asks:
                        print(f"    OrderID: {ask.orderID}")
                        print(f"    Price: {ask.price}")
                        print(f"    Quantity: {ask.unfilled_quantity}/{ask.total_quantity}")
                        print(f"    Expiry: {ask.expiry}")

    async def poll_info(self):
        while True:
            try:
                print(f"\n=== Polling at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
                
                # Get inventory
                inventory = await self.get_inventory()
                if isinstance(inventory, GetInventoryResponse):
                    self.print_inventory(inventory)
                else:
                    print("Failed to get inventory")

                # Get pending orders
                orders = await self.get_pending_orders()
                if isinstance(orders, GetPendingOrdersResponse):
                    self.print_pending_orders(orders)
                else:
                    print("Failed to get pending orders")

                # Wait for 1 second before next poll
                await asyncio.sleep(1)

            except Exception as e:
                print(f"Error during polling: {e}")
                await asyncio.sleep(1)  # Still wait 1 second even if there's an error

async def main():
    EXCHANGE_URI = "ws://192.168.100.10:9001/trade"
    TEAM_SECRET = "9dd0a684-c786-4500-a04b-91b777385403"

    poller = InfoPoller(
        EXCHANGE_URI,
        TEAM_SECRET
    )

    await poller.connect()
    await poller.poll_info()

if __name__ == '__main__':
    asyncio.run(main()) 