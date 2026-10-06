from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
import json

app = FastAPI(title="Secure Web Chat")

# Active connected clients
clients = []


@app.get("/")
async def home():
    return FileResponse("static/index.html")


async def send_to_others(sender, payload):
    """
    Send message to all connected clients except sender.
    """
    for client in clients[:]:
        if client is not sender:
            try:
                await client["ws"].send_json(payload)
            except Exception:
                pass


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    # Mini project supports two peers only
    if len(clients) >= 2:
        await websocket.send_json({
            "type": "error",
            "message": "Room full. This demo supports two peers only."
        })
        await websocket.close()
        return

    client = {
        "ws": websocket,
        "name": None,
        "pub": None
    }

    clients.append(client)

    try:
        await websocket.send_json({
            "type": "status",
            "message": "Connected to signaling/relay server. Waiting for peer..."
        })

        while True:
            raw = await websocket.receive_text()

            try:
                msg = json.loads(raw)
            except Exception:
                continue

            mtype = msg.get("type")

            # User joins chat and sends public key
            if mtype == "join":
                client["name"] = str(msg.get("name", "Peer"))[:50]
                client["pub"] = msg.get("pub")

                # Send already connected peers to this new client
                for other in clients:
                    if other is not client and other.get("pub") is not None:
                        await websocket.send_json({
                            "type": "peer",
                            "name": other["name"],
                            "pub": other["pub"]
                        })

                # Inform other peers about this new client
                await send_to_others(client, {
                    "type": "peer",
                    "name": client["name"],
                    "pub": client["pub"]
                })

            # Encrypted chat message relay
            elif mtype == "chat":
                data = msg.get("data")

                if isinstance(data, str) and client["name"]:
                    await send_to_others(client, {
                        "type": "chat",
                        "from": client["name"],
                        "data": data
                    })

    except WebSocketDisconnect:
        pass

    except Exception:
        pass

    finally:
        if client in clients:
            clients.remove(client)

        if client["name"]:
            await send_to_others(client, {
                "type": "peer_left",
                "name": client["name"]
            })
        else:
            await send_to_others(client, {
                "type": "status",
                "message": "A peer disconnected."
            })
