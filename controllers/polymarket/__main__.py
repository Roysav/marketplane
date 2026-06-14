import asyncio
import json
import logging
import math
import os
from typing import Any

import grpc
import grpc.aio
import websockets
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value
from marketplane.apiserver.v1 import apiserver_pb2, apiserver_pb2_grpc

from controllers.base import Controller, run_controllers

logger = logging.getLogger(__name__)

GRPC_ADDR   = os.environ["MARKETPLANE_GRPC_ADDR"]
TRADESPACE  = os.environ["POLYMARKET_TRADESPACE"]
RECORD_TYPE = "polymarket/v1beta/Ticker"
WS_URL      = "wss://ws-live-data.polymarket.com"
PING_INTERVAL = 5.0


class PolymarketController(Controller):
    resync_interval = 30.0

    def __init__(self, stub: apiserver_pb2_grpc.ApiserverServiceStub) -> None:
        self._stub = stub
        self._stream_task: asyncio.Task | None = None
        self._current_symbols: frozenset[str] = frozenset()

    async def list_records(self) -> dict[str, str]:
        resp = await self._stub.ListRecords(apiserver_pb2.ListRecordsRequest(
            type=RECORD_TYPE, tradespace=TRADESPACE,
        ))
        result = {}
        for r in resp.records:
            spec = json_format.MessageToDict(r.spec)
            result[r.metadata.name] = spec["ticker"]
        return result

    async def watch(self, changed: asyncio.Event) -> None:
        async for _ in self._stub.WatchRecords(apiserver_pb2.WatchRecordsRequest(
            type=RECORD_TYPE, tradespace=TRADESPACE,
        )):
            changed.set()

    async def reconcile(self, records: dict[str, Any]) -> None:
        if (
            self._stream_task
            and self._stream_task.done()
            and not self._stream_task.cancelled()
        ):
            self._stream_task.result()  # re-raise if stream failed

        new_symbols = frozenset(records.values())
        if new_symbols == self._current_symbols and self._stream_task and not self._stream_task.done():
            return

        if self._stream_task and not self._stream_task.done():
            self._stream_task.cancel()

        self._current_symbols = new_symbols
        self._stream_task = asyncio.create_task(self._stream_prices(sorted(new_symbols)))
        logger.info("reconciled: subscribed to %d symbols %s", len(new_symbols), sorted(new_symbols))

    async def _stream_prices(self, symbols: list[str]) -> None:
        if not symbols:
            await asyncio.sleep(math.inf)
            return

        symbol_set = set(symbols)
        async with websockets.connect(WS_URL, ping_interval=PING_INTERVAL) as ws:
            await ws.send(json.dumps({
                "action": "subscribe",
                "subscriptions": [{"topic": "crypto_prices", "type": "update"}],
            }))
            logger.info("ws connected, tracking %s", symbols)
            async for raw in ws:
                if not raw:
                    continue
                msg = json.loads(raw)
                if msg.get("topic") != "crypto_prices" or msg.get("type") != "update":
                    raise RuntimeError(f"unexpected ws message: {msg!r}")
                payload = msg["payload"]
                symbol = payload["symbol"]
                if symbol not in symbol_set:
                    continue
                await self._stub.PublishTick(apiserver_pb2.PublishTickRequest(
                    name=f"polymarket/{symbol}",
                    value=json_format.ParseDict(payload, Value()),
                ))


async def main() -> None:
    async with grpc.aio.insecure_channel(GRPC_ADDR) as channel:
        stub = apiserver_pb2_grpc.ApiserverServiceStub(channel)
        await run_controllers(PolymarketController(stub))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(main())
