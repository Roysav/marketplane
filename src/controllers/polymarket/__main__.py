import asyncio
import os

import grpc.aio
from marketplane.apiserver.v1 import apiserver_pb2_grpc

from controller import Controller
from controllers.polymarket import MARKET_CHANNEL_URL, MarketChannel, Polymarket
from sdk import MarketplaneClient


async def _run(*, address: str, reconnect_backoff: float, resync_interval: float) -> None:
    async with grpc.aio.insecure_channel(address) as grpc_channel:
        client = MarketplaneClient(apiserver_pb2_grpc.ApiserverServiceStub(grpc_channel))
        controller = Controller(client, reconnect_backoff=reconnect_backoff, resync_interval=resync_interval)
        await Polymarket(controller, MarketChannel(MARKET_CHANNEL_URL)).run()


def main() -> None:
    asyncio.run(_run(
        address=os.environ["MARKETPLANE_ADDRESS"],
        reconnect_backoff=float(os.environ["POLYMARKET_RECONNECT_BACKOFF"]),
        resync_interval=float(os.environ["POLYMARKET_RESYNC_INTERVAL"]),
    ))


if __name__ == "__main__":
    main()
