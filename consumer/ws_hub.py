"""WebSocket broadcast hub.

Accepts client connections on :8765 and broadcasts each snapshot to every
connected client. A slow client cannot stall the hot path: every send is
wrapped in `asyncio.wait_for`, and a timeout (or any other error) drops the
client.
"""

from __future__ import annotations

import asyncio
import logging

import orjson
import websockets
from websockets.legacy.server import WebSocketServerProtocol

from aggregator import Aggregate
from config import Settings

log = logging.getLogger("consumer.ws")


class WebSocketHub:
    def __init__(self, send_timeout: float) -> None:
        self._clients: set[WebSocketServerProtocol] = set()
        self._send_timeout = send_timeout

    @property
    def client_count(self) -> int:
        return len(self._clients)

    async def register(self, ws: WebSocketServerProtocol) -> None:
        self._clients.add(ws)
        log.info("ws client connected (total=%d)", len(self._clients))
        # Send a friendly hello so the frontend's "waiting for data" placeholder
        # can clear immediately rather than waiting for the next tick.
        try:
            await asyncio.wait_for(
                ws.send(orjson.dumps({"type": "hello"}).decode()),
                timeout=self._send_timeout,
            )
        except Exception:  # noqa: BLE001
            pass
        try:
            await ws.wait_closed()
        finally:
            self._clients.discard(ws)
            log.info("ws client disconnected (total=%d)", len(self._clients))

    async def broadcast(self, snapshot: list[Aggregate]) -> None:
        if not self._clients:
            return
        payload = orjson.dumps(
            {
                "type": "snapshot",
                "items": [
                    {
                        "ts": a.ts,
                        "symbol": a.symbol,
                        "avg_price": a.avg_price,
                        "sample_count": a.sample_count,
                    }
                    for a in snapshot
                ],
            }
        ).decode()

        # Snapshot the set so disconnects during iteration don't mutate it.
        dropouts: list[WebSocketServerProtocol] = []
        coros = [self._send_one(ws, payload, dropouts) for ws in list(self._clients)]
        await asyncio.gather(*coros, return_exceptions=False)
        for ws in dropouts:
            self._clients.discard(ws)

    async def _send_one(
        self,
        ws: WebSocketServerProtocol,
        payload: str,
        dropouts: list[WebSocketServerProtocol],
    ) -> None:
        try:
            await asyncio.wait_for(ws.send(payload), timeout=self._send_timeout)
        except (asyncio.TimeoutError, websockets.ConnectionClosed):
            dropouts.append(ws)
            try:
                await ws.close(code=1011, reason="slow consumer")
            except Exception:  # noqa: BLE001
                pass
        except Exception:  # noqa: BLE001
            dropouts.append(ws)


async def ws_server_loop(
    settings: Settings,
    hub: WebSocketHub,
    stop: asyncio.Event,
) -> None:
    async def handler(ws: WebSocketServerProtocol) -> None:
        await hub.register(ws)

    server = await websockets.serve(
        handler,
        host=settings.ws_host,
        port=settings.ws_port,
        ping_interval=20,
        ping_timeout=20,
        max_size=2**20,
    )
    log.info("ws server listening on %s:%d", settings.ws_host, settings.ws_port)
    try:
        await stop.wait()
    finally:
        server.close()
        await server.wait_closed()
        log.info("ws server stopped")
