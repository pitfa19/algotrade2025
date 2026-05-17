import asyncio
import websockets
import json

url = "ws://192.168.100.10/trade?team_secret=9dd0a684-c786-4500-a04b-91b777385403"

async def connect():
    async with websockets.connect(url) as websocket:
        print("✅ Connected to AlgoTrade 2025!")

        while True:
            message = await websocket.recv()
            data = json.loads(message)
            print("📩 Received:", data)

asyncio.run(connect())
