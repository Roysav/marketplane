import asyncio
import logging

import grpc.aio
import httpx
from marketplane.apiserver.v1 import apiserver_pb2_grpc

from controller import Controller
from .api import PolymarketAPI
from .channel import MarketChannel
from .config import Settings
from .reconcilers import AssetSubscriber, EventImporter, EventReconciler, MarketReconciler
from .trades import TradePublisher
from sdk import MarketplaneClient

logger = logging.getLogger(__name__)


async def _run(settings: Settings) -> None:
    logger.info("polymarket controller starting")
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
        channel = MarketChannel(settings.polymarket.market_channel_url, TradePublisher(client).on_message, max_assets=settings.polymarket.max_assets)
        api = PolymarketAPI(http_client, page_size=settings.polymarket.page_size, max_concurrency=settings.polymarket.max_concurrency)

        AssetSubscriber(controller, channel)
        MarketReconciler(controller, client)
        EventReconciler(controller, client, api)
        EventImporter(controller, client, api, interval=settings.polymarket.cron_interval)

        await asyncio.gather(channel.run(), controller.run())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(_run(Settings()))


if __name__ == "__main__":
    main()
