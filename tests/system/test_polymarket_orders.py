import asyncio
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from controller import Controller
from controllers.polymarket.config import PolymarketConfig
from controllers.polymarket.orders import OrderReconciler
from controllers.polymarket.signer import build_clob_client
from controllers.polymarket.types import (
    ORDER_TYPE,
    OrderPhase,
    OrderSide,
    OrderSpec,
    OrderType,
    PolymarketOrderRecord,
)
from sdk import MarketplaneClient, Record
from utils.config import deep_merge

_DEFAULT = Path("src/controllers/polymarket/default.config.yaml")
_SECRETS = Path("secrets/polymarket-controller.yaml")

# "Will Jesus Christ return before 2027?" -> NO outcome (clobTokenIds index 1).
# A resting BUY of NO at 1c can never fill (NO trades near 99c), so it's a safe live probe.
NO_TOKEN = "51797157743046504218541616681751597845468055908324407922581755135522797852101"


def _read(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f) or {}


@pytest.fixture(scope="module")
def poly() -> PolymarketConfig:
    if not _SECRETS.is_file():
        pytest.skip("secrets/polymarket-controller.yaml not configured")
    config = PolymarketConfig(**deep_merge(_read(_DEFAULT), _read(_SECRETS))["polymarket"])
    if not config.signer.private_key:
        pytest.skip("polymarket signer key not set")
    return config


@pytest.fixture(scope="module")
def clob(poly):
    return build_clob_client(poly.clob_api_url, poly.chain_id, poly.signer)


async def _await_placed(client: MarketplaneClient, tradespace: str, name: str, *, timeout: float = 20.0) -> PolymarketOrderRecord:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        record = await client.get_record(ORDER_TYPE, tradespace, name)
        order = PolymarketOrderRecord.model_validate(record.spec)
        if order.status.phase is OrderPhase.placed:
            return order
        await asyncio.sleep(0.3)
    raise AssertionError(f"controller did not place {name} within {timeout}s")


@pytest.mark.system
async def test_order_record_places_order_on_polymarket(stub, poly, clob):
    client = MarketplaneClient(stub)
    controller = Controller(client, reconnect_backoff=0.1, resync_interval=0.5, idle_timeout=60.0)
    OrderReconciler(controller, client, clob, tradespace=poly.tradespace, lease=poly.order_lease)
    run = asyncio.create_task(controller.run())
    order_id: str | None = None
    try:
        spec = OrderSpec(
            type=OrderType.gtc,
            token=NO_TOKEN,
            side=OrderSide.buy,
            size=Decimal(10),
            price=Decimal("0.01"),
            active=True,
        )
        await client.create_record(Record(
            type=ORDER_TYPE,
            tradespace=poly.tradespace,
            name="jesus-no",
            spec=PolymarketOrderRecord(spec=spec).model_dump(by_alias=True, mode="json"),
        ))
        order = await _await_placed(client, poly.tradespace, "jesus-no")
        order_id = order.status.polymarket_order_id
        assert order_id, "order placed but no polymarket order id recorded"
        assert order.spec.signature, "signature was not written ahead of placement"
    finally:
        run.cancel()
        await asyncio.gather(run, return_exceptions=True)
        if order_id:
            clob.cancel_orders([order_id])
