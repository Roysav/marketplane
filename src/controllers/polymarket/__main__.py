import asyncio
import logging.config

import grpc.aio
import httpx
from marketplane.apiserver.v1 import apiserver_pb2_grpc

from controller import Controller
from controllers.polymarket.api import PolymarketAPI
from controllers.polymarket.channel import MarketChannel
from controllers.polymarket.config import Settings
from controllers.polymarket.prices import PricePublisher
from controllers.polymarket.reconcilers import AssetSubscriber, EventImporter, EventReconciler, MarketReconciler
from sdk import MarketplaneClient

logger = logging.getLogger("controllers.polymarket")


async def _run(settings: Settings) -> None:
    logger.info("polymarket controller starting")
    async with (
        grpc.aio.insecure_channel(settings.marketplane.address, options=[
            ("grpc.max_send_message_length", settings.marketplane.max_message_bytes),
            ("grpc.max_receive_message_length", settings.marketplane.max_message_bytes),
        ]) as grpc_channel,
        httpx.AsyncClient(base_url=settings.polymarket.gamma_api_url) as http_client,
    ):
        client = MarketplaneClient(apiserver_pb2_grpc.ApiserverServiceStub(grpc_channel))
        controller = Controller(
            client,
            reconnect_backoff=settings.controller.reconnect_backoff,
            resync_interval=settings.controller.resync_interval,
        )
        channel = MarketChannel(settings.polymarket.market_channel_url, PricePublisher(client).on_message, max_assets=settings.polymarket.max_assets, ping_timeout=settings.polymarket.ping_timeout)
        api = PolymarketAPI(http_client, page_size=settings.polymarket.page_size, max_concurrency=settings.polymarket.max_concurrency)

        AssetSubscriber(controller, channel)
        MarketReconciler(controller, client)
        EventReconciler(controller, client, api)
        EventImporter(controller, client, api, interval=settings.polymarket.cron_interval)

        await asyncio.gather(channel.run(), controller.run())


def main() -> None:
    settings = Settings()
    logging.config.dictConfig(settings.logging)
    asyncio.run(_run(settings))


if __name__ == "__main__":
    main()
