import asyncio

import grpc.aio
import httpx
from marketplane.apiserver.v1 import apiserver_pb2_grpc

from controller import Controller
from .api import PolymarketAPI
from .channel import MarketChannel
from .config import Settings
from .reconcilers import AssetSubscriber, EventImporter, EventReconciler, MarketReconciler
from sdk import MarketplaneClient


async def _run(settings: Settings) -> None:
    async with (
        grpc.aio.insecure_channel(settings.marketplane.address) as grpc_channel,
        httpx.AsyncClient(base_url=settings.polymarket.gamma_api_url, headers={"User-Agent": "marketplane-polymarket/0"}) as http_client,
    ):
        client = MarketplaneClient(apiserver_pb2_grpc.ApiserverServiceStub(grpc_channel))
        controller = Controller(
            client,
            reconnect_backoff=settings.controller.reconnect_backoff,
            resync_interval=settings.controller.resync_interval,
        )
        channel = MarketChannel(settings.polymarket.market_channel_url)
        api = PolymarketAPI(http_client, page_size=settings.polymarket.page_size)

        AssetSubscriber(controller, channel)
        MarketReconciler(controller, client)
        EventReconciler(controller, client, api)
        EventImporter(controller, client, api, interval=settings.polymarket.cron_interval)

        await asyncio.gather(channel.run(), controller.run())


def main() -> None:
    asyncio.run(_run(Settings()))


if __name__ == "__main__":
    main()
