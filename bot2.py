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

# ───── Data-Driven Market Maker Bot with Enhanced Logging ────────────────
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
        # Ensure team_secret and uri are available on this instance
        self.uri = uri
        self.team_secret = team_secret

        self.spread = spread
        self.size = size
        self.max_pos = max_pos
        self.rate_limit_per_sec = rate_limit_per_sec
        self._pending_requests: dict[str, asyncio.Future] = {}
        self._last_call_ts = 0.0
        self._calls_in_window = 0

    async def connect(self):
        # Connect using the raw uri and secret query parameter
        self.ws = await websockets.connect(f"{self.uri}?team_secret={self.team_secret}")
        logging.info("Connected to exchange at %s", self.uri)
        await self._receive_loop()

    def _next_id(self) -> str:
        return uuid.uuid4().hex

    async def _send_request(self, req: dict) -> dict:
        # Simple per-second rate limiter
        now = time.time()
        if now - self._last_call_ts >= 1:
            self._last_call_ts = now
            self._calls_in_window = 0
        if self._calls_in_window >= self.rate_limit_per_sec:
            await asyncio.sleep(1 - (now - self._last_call_ts))
            self._last_call_ts = time.time()
            self._calls_in_window = 0
        self._calls_in_window += 1

        # Attach a unique ID and track the future
        uid = self._next_id()
        req['user_request_id'] = uid
        fut = asyncio.get_event_loop().create_future()
        self._pending_requests[uid] = fut
        await self.ws.send(json.dumps(req))
        logging.debug("Sent request %s", req)
        return await fut

    async def _receive_loop(self):
        while True:
            try:
                async for raw in self.ws:
                    data = json.loads(raw)
                    rid = data.get('user_request_id')
                    if rid and rid in self._pending_requests:
                        self._pending_requests.pop(rid).set_result(data)
                        logging.debug("Received response for %s: %s", rid, data)
                    elif data.get('type') == 'market_data_update':
                        logging.debug("Market data update received")
                        asyncio.create_task(self._handle_market_data_update(data))
            except ConnectionClosedError:
                logging.warning("Connection lost, reconnecting...")
                await asyncio.sleep(1)
                return await self.connect()

    async def _handle_market_data_update(self, data: dict):
        depths = data.get('orderbook_depths', {})
        for instr, book in depths.items():
            bids = {int(p): v for p, v in book.get('bids', {}).items()}
            asks = {int(p): v for p, v in book.get('asks', {}).items()}
            if not bids or not asks:
                continue

            best_bid = max(bids)
            best_ask = min(asks)
            mid = (best_bid + best_ask) // 2
            logging.info("%s midpoint calculated: mid=%d (bid=%d, ask=%d)", instr, mid, best_bid, best_ask)

            pos = await self.get_position(instr)
            logging.info("Current position for %s: %d", instr, pos)

            skew = 0
            if pos > self.max_pos:
                skew = self.spread
            elif pos < -self.max_pos:
                skew = -self.spread
            if skew:
                logging.info("Applying inventory skew %d for %s", skew, instr)

            buy_price = mid - self.spread + skew
            sell_price = mid + self.spread + skew
            logging.info("%s spread orders -> bid: %d, ask: %d", instr, buy_price, sell_price)

            # Cancel existing orders before placing new ones
            await self.cancel_all(instr)

            expiry = data['time'] + 5000
            bid_req = {
                'type': 'add_order',
                'instrument_id': instr,
                'price': buy_price,
                'expiry': expiry,
                'side': 'bid',
                'quantity': self.size,
            }
            ask_req = {
                'type': 'add_order',
                'instrument_id': instr,
                'price': sell_price,
                'expiry': expiry,
                'side': 'ask',
                'quantity': self.size,
            }
            logging.info("Placing bid on %s: %s", instr, bid_req)
            await self._send_request(bid_req)
            logging.info("Placing ask on %s: %s", instr, ask_req)
            await self._send_request(ask_req)

    async def cancel_all(self, instrument: str):
        logging.info("Cancelling all existing orders for %s", instrument)
        resp = await self._send_request({'type': 'get_pending_orders'})
        data = resp.get('data', {})
        for side_orders in data.get(instrument, [[], []]):
            for o in side_orders:
                cancel_req = {
                    'type': 'cancel_order',
                    'instrument_id': instrument,
                    'order_id': o['orderID'],
                }
                logging.info("Cancelling order %s on %s", o['orderID'], instrument)
                await self._send_request(cancel_req)

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

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

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
    asyncio.run(main())
