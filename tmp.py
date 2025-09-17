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
        "56678887488658711042665014393296062331022428641924974006054058569968887051344",
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
        await websocket.send(json.dumps({
            "type": "market",
            "assets_ids": [
                "23920013258819752881197525832650639378445397094039587501841819155943480602942",
                "60487116984468020978247225474488676749601001829886755968952521846780452448915"
            ]
        }))
        
        try:
            message = await websocket.recv()
            print("Received message:", message)
        except Exception as e:
            print(f"Error receiving message: {e}")
        
        
        print("Sending another subscription payload")
        
        await websocket.send(json.dumps({
            "type": "market",
            "assets_ids": [
                "60487116984468020978247225474488676749601001829886755968952521846780452448915"
            ]
        }))
        
        

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