"""
ws.py – WebSocket hub: broadcast + per-client queues + heartbeat.

Per-client bounded queue drops old vehicle_positions first on overflow.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any

from fastapi import WebSocket


@dataclass
class _Client:
    ws: WebSocket
    queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=1000))
    last_heartbeat: float = field(default_factory=time.monotonic)


class WSHub:
    def __init__(self, heartbeat_interval: float = 15.0, queue_max: int = 1000) -> None:
        self._clients: list[_Client] = []
        self._heartbeat_interval = heartbeat_interval
        self._queue_max = queue_max
        self._heartbeat_task: asyncio.Task | None = None
        
    async def start(self) -> None:
        """Start the heartbeat task."""
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
    
    async def stop(self) -> None:
        """Stop the heartbeat task."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
    
    async def connect(self, ws: WebSocket, snapshot: dict[str, Any]) -> None:
        """Accept a new WebSocket connection and send initial snapshot."""
        await ws.accept()
        client = _Client(ws=ws)
        self._clients.append(client)
        
        # Send snapshot
        try:
            await ws.send_json({"type": "snapshot", **snapshot})
        except Exception:
            self._clients.remove(client)
            raise
    
    def disconnect(self, ws: WebSocket) -> None:
        """Remove a disconnected client."""
        self._clients = [c for c in self._clients if c.ws != ws]
    
    def broadcast(self, msg: dict[str, Any]) -> None:
        """Broadcast a message to all clients. Drops old vehicle_positions on overflow."""
        for client in self._clients:
            if client.queue.full():
                # Drop the oldest message if it's a vehicle_positions
                try:
                    old = client.queue.get_nowait()
                    if old.get("type") != "vehicle_positions":
                        # Put it back if not vehicle_positions
                        client.queue.put_nowait(old)
                except asyncio.QueueEmpty:
                    pass
            
            try:
                client.queue.put_nowait(msg)
            except asyncio.QueueFull:
                pass  # Still full; drop this message
    
    async def send_loop(self, ws: WebSocket) -> None:
        """Send queued messages to a specific client."""
        client = next((c for c in self._clients if c.ws == ws), None)
        if not client:
            return
        
        try:
            while True:
                msg = await client.queue.get()
                await ws.send_text(json.dumps(msg))
        except Exception:
            pass
        finally:
            self.disconnect(ws)
    
    async def _heartbeat_loop(self) -> None:
        """Send periodic pings to all clients."""
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            dead: list[_Client] = []
            for client in self._clients:
                try:
                    await client.ws.send_json({"type": "ping", "ts": int(time.time() * 1000)})
                    client.last_heartbeat = time.monotonic()
                except Exception:
                    dead.append(client)
            
            for c in dead:
                self.disconnect(c.ws)


# Global hub instance
hub = WSHub()
