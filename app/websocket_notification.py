import asyncio
import websockets

status_updates = {}

async def notify_ingestion_status(websocket, path):
    while True:
        message = await websocket.recv()
        if message in status_updates:
            await websocket.send(status_updates[message])

async def update_status(key, status):
    status_updates[key] = status
    await asyncio.sleep(1)

# Start the WebSocket server
async def start_server():
    server = await websockets.serve(notify_ingestion_status, "localhost", 8765)
    await server.wait_closed()
