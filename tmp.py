import asyncio
import websockets
import json
import base64
import os

URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Subscription Payload
SUBSCRIBE_PAYLOAD = {
    "type": "market",
    "assets_ids": [
        "7988364699307342268260495915445731509602502693363579435248273813409237524599",
        "27120152309306977992209464424197509161348402683341807871062277866907268946377"
    ]
}

# Function to create the WebSocket connection with custom headers
async def connect():
    # WebSocket key for the handshake (generated randomly)
    sec_websocket_key = base64.b64encode(os.urandom(16)).decode('utf-8')

    # Custom headers to replicate the Chrome network request
    headers = {
        "Host": "ws-live-data.polymarket.com",
        "Connection": "Upgrade",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "Upgrade": "websocket",
        "Origin": "https://polymarket.com",
        "Sec-WebSocket-Version": "13",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
        "Sec-WebSocket-Key": sec_websocket_key,
        "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits"
    }

    # Establish the WebSocket connection
    async with websockets.connect(URL, extra_headers=headers) as websocket:
        print(f"Connection established to {URL}")

        # Send the subscription payload to the server
        print(f"Sending subscription payload: {json.dumps(SUBSCRIBE_PAYLOAD)}")
        await websocket.send(json.dumps(SUBSCRIBE_PAYLOAD))

        # Continuously receive messages from the WebSocket
        while True:
            try:
                message = await websocket.recv()
                print("Received message:", message)
            except Exception as e:
                print(f"Error receiving message: {e}")
                break

# Run the WebSocket connection asynchronously
asyncio.run(connect())