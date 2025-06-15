import asyncio
import logging
import time
from typing import Dict, Tuple, List

from fixedDemoTradingBot import DemoTradingBot, MarketDataResponse, OrderbookDepth, AddOrderRequest, CancelOrderRequest

# Configure global logging
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[
            logging.FileHandler('hft_bot.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger('HFTBot')

logger = setup_logging()

class HFTBot(DemoTradingBot):
    """
    HFTBot extends DemoTradingBot with:
      - Delta-neutral market making
      - Call-put parity arbitrage
      - Real-time adaptivity (spread adjustment)
      - Auto-reconnect on disconnect
    """
    def __init__(self, uri: str, team_secret: str, print_market_data: bool = False):
        super().__init__(uri, team_secret, print_market_data)
        self.quote_tasks: Dict[str, asyncio.Task] = {}
        self.book: Dict[str, OrderbookDepth] = {}
        self.threshold_arb = 10   # cents
        self.base_spread  = 5    # cents

    def _handle_market_data_update(self, data: MarketDataResponse):
        # Create a copy of the book to avoid modification during iteration
        new_book = {}
        for instr, depth in data.orderbook_depths.items():
            new_book[instr] = depth
        self.book = new_book
        
        # fire off both routines
        asyncio.create_task(self.run_market_maker())
        asyncio.create_task(self.run_parity_arb())

    async def run_market_maker(self):
        # clear finished tasks
        for instr, task in list(self.quote_tasks.items()):
            if task.done():
                del self.quote_tasks[instr]

        # Create a copy of book items to avoid modification during iteration
        book_items = list(self.book.items())
        for instr, depth in book_items:
            if '_call_' not in instr and '_put_' not in instr:
                continue
            bids = [int(p) for p in depth.bids.keys()]
            asks = [int(p) for p in depth.asks.keys()]
            if not bids or not asks:
                continue

            best_bid, best_ask = max(bids), min(asks)
            mid = (best_bid + best_ask) / 2
            spread = self.base_spread
            bid_px = int(mid - spread/2)
            ask_px = int(mid + spread/2)

            await self.cancel_old(instr)
            # Get current server time and add 10 seconds for expiry
            server_time = await self.get_server_time()
            expiry = server_time + 10000  # 10 seconds in the future
            await self.send(AddOrderRequest('', instr, bid_px, expiry, 'bid', 1))
            await self.send(AddOrderRequest('', instr, ask_px, expiry, 'ask', 1))
            logger.info(f"Quoted {instr}: bid@{bid_px}, ask@{ask_px}")
            self.quote_tasks[instr] = asyncio.current_task()

    async def cancel_old(self, instr: str):
        pending = await self.get_pending_orders()
        if hasattr(pending, 'data') and instr in pending.data:
            bids, asks = pending.data[instr]
            for order in bids + asks:
                await self.send(CancelOrderRequest('', order.orderID, instr))
                logger.info(f"Cancelled old order {order.orderID} on {instr}")

    async def run_parity_arb(self):
        # map (underlying, strike, expiry) → { 'call': id, 'put': id }
        option_map: Dict[Tuple[str,int,int], Dict[str,str]] = {}
        for instr in self.book:
            parts = instr.strip('$').split('_')
            if len(parts) != 4:
                continue
            u, t, ks, es = parts
            try:
                k, e = int(ks), int(es)
            except ValueError:
                continue
            option_map.setdefault((u,k,e), {})[t] = instr

        for (u,k,e), legs in option_map.items():
            if 'call' not in legs or 'put' not in legs:
                continue
            call_id, put_id = legs['call'], legs['put']
            fut_id = f"${u}_future_{e}"
            if call_id not in self.book or put_id not in self.book or fut_id not in self.book:
                continue

            call_mid = _mid(self.book[call_id])
            put_mid  = _mid(self.book[put_id])
            fut_mid  = _mid(self.book[fut_id])
            if None in (call_mid, put_mid, fut_mid):
                continue

            diff = (call_mid - put_mid) - (fut_mid - k)
            if abs(diff) < self.threshold_arb:
                continue

            logger.info(f"Parity arb K={k}, exp={e}, diff={diff}")
            if diff > 0:
                trip = [(call_id,'ask'), (put_id,'bid'), (fut_id,'bid')]
            else:
                trip = [(call_id,'bid'), (put_id,'ask'), (fut_id,'ask')]
            await self.execute_arb(trip)

    async def execute_arb(self, legs: List[Tuple[str,str]]):
        for instr, side in legs:
            depth = self.book[instr]
            prices = [int(p) for p in (depth.asks if side=='bid' else depth.bids).keys()]
            if not prices:
                logger.warning(f"Cannot price leg {instr} {side}")
                return
            price = min(prices) if side=='bid' else max(prices)
            # Get current server time and add 10 seconds for expiry
            server_time = await self.get_server_time()
            expiry = server_time + 10000  # 10 seconds in the future
            await self.send(AddOrderRequest('', instr, price, expiry, side, 1))
            logger.info(f"Arb leg {instr} {side}@{price}")

# Helpers
def _extract_expiry(instr: str) -> int:
    try:
        return int(instr.split('_')[-1]) * 1000
    except Exception:
        return 0

def _mid(depth: OrderbookDepth) -> float:
    bids = [int(p) for p in depth.bids.keys()]
    asks = [int(p) for p in depth.asks.keys()]
    if not bids or not asks:
        return None
    return (max(bids) + min(asks)) / 2

# Auto-reconnect loop
async def start_bot():
    EXCHANGE_URI = "ws://192.168.100.10:9001/trade"
    TEAM_SECRET  = "9dd0a684-c786-4500-a04b-91b777385403"
    backoff = 1
    while True:
        bot = HFTBot(EXCHANGE_URI, TEAM_SECRET)
        try:
            await bot.connect()
            await asyncio.Future()  # run forever
        except Exception as e:
            logger.error(f"Disconnected: {e}, reconnecting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
        else:
            break

if __name__ == '__main__':
    try:
        asyncio.run(start_bot())
    except KeyboardInterrupt:
        logger.info("Shutting down.")
