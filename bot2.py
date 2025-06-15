import asyncio
import json
import logging
import uuid
import time

import websockets
from websockets.exceptions import ConnectionClosedError

from fixedDemoTradingBot import (
    DemoTradingBot,
    AddOrderRequest,
    CancelOrderRequest,
    GetInventoryRequest,
    GetPendingOrdersRequest,
)

# ───── Data-Driven Market Maker Bot ─────────────────────────────────────────

class MarketMakerBot(DemoTradingBot):
    def __init__(
        self,
        uri: str,
        team_secret: str,
        spread: int = 1,
        size: int = 1,
        max_pos: int = 5,
        rate_limit_per_sec: int = 200,
    ):
        """
        A market maker that dynamically quotes every instrument seen in data:
        - Fetches instrument IDs from each market_data_update
        - Uses half-spread, size, and max_pos parameters
        - Rate-limits messages to the exchange
        """
        super().__init__(uri, team_secret, print_market_data=False)
        self.spread = spread
        self.size = size
        self.max_pos = max_pos
        self._pending_requests: dict[str, asyncio.Future] = {}
        self._last_call_ts = 0.0
        self._calls_in_window = 0

    async def connect(self):
        # Connect with team_secret in query string
        self.ws = await websockets.connect(f"{self.uri}?team_secret={self.team_secret}")
        logging.info("Connected to %s", self.uri)
        await self._receive_loop()

    def _next_id(self) -> str:
        return uuid.uuid4().hex

    async def _send_request(self, req: dict) -> dict:
        # simple per-second rate limiter
        now = time.time()
        if now - self._last_call_ts >= 1:
            self._last_call_ts = now
            self._calls_in_window = 0
        if self._calls_in_window >= self.rate_limit_per_sec:
            await asyncio.sleep(1 - (now - self._last_call_ts))
            self._last_call_ts = time.time()
            self._calls_in_window = 0
        self._calls_in_window += 1

        # attach unique ID
        uid = self._next_id()
        req['user_request_id'] = uid
        fut = asyncio.get_event_loop().create_future()
        self._pending_requests[uid] = fut
        await self.ws.send(json.dumps(req))
        return await fut

    async def _receive_loop(self):
        while True:
            try:
                async for raw in self.ws:
                    data = json.loads(raw)
                    # match up pending requests
                    rid = data.get('user_request_id')
                    if rid and rid in self._pending_requests:
                        self._pending_requests.pop(rid).set_result(data)
                    # process market data
                    elif data.get('type') == 'market_data_update':
                        asyncio.create_task(self._handle_market_data_update(data))
            except ConnectionClosedError:
                logging.warning("Connection lost, reconnecting...")
                await asyncio.sleep(1)
                return await self.connect()

    async def _handle_market_data_update(self, data: dict):
        depths = data.get('orderbook_depths', {})
        # dynamically iterate all instruments available
        for instr, book in depths.items():
            bids = {int(p): v for p, v in book.get('bids', {}).items()}
            asks = {int(p): v for p, v in book.get('asks', {}).items()}
            if not bids or not asks:
                continue

            best_bid = max(bids)
            best_ask = min(asks)
            mid = (best_bid + best_ask) // 2

            pos = await self.get_position(instr)
            skew = 0
            if pos > self.max_pos:
                skew = self.spread
            elif pos < -self.max_pos:
                skew = -self.spread

            buy_price = mid - self.spread + skew
            sell_price = mid + self.spread + skew

            # cancel existing
            await self.cancel_all(instr)

            expiry = data['time'] + 5000
            # post bid
            await self._send_request({
                'type': 'add_order',
                'instrument_id': instr,
                'price': buy_price,
                'expiry': expiry,
                'side': 'bid',
                'quantity': self.size,
            })
            # post ask
            await self._send_request({
                'type': 'add_order',
                'instrument_id': instr,
                'price': sell_price,
                'expiry': expiry,
                'side': 'ask',
                'quantity': self.size,
            })

    async def cancel_all(self, instrument: str):
        resp = await self._send_request({'type': 'get_pending_orders'})
        data = resp.get('data', {})
        for side_orders in data.get(instrument, [[], []]):
            for o in side_orders:
                await self._send_request({
                    'type': 'cancel_order',
                    'instrument_id': instrument,
                    'order_id': o['orderID'],
                })

    async def get_position(self, instrument: str) -> int:
        resp = await self._send_request({'type': 'get_inventory'})
        data = resp.get('data', {})
        if instrument in data:
            reserved, total = data[instrument]
            return total - reserved
        return 0

async def main():
    EXCHANGE_URI = "ws://192.168.100.10:9001/trade"
    TEAM_SECRET = "9dd0a684-c786-4500-a04b-91b777385403"

    bot = MarketMakerBot(
        uri=EXCHANGE_URI,
        team_secret=TEAM_SECRET,
        spread=1,
        size=1,
        max_pos=5,
    )
    await bot.connect()
    await asyncio.Future()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
