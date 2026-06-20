import asyncio
import logging
import time
from dataclasses import replace
from decimal import Decimal
from uuid import uuid4

from grpc.aio import AioRpcError
from py_clob_client_v2 import ClobClient, OrderArgs

from controller import Controller, RecordNotification
from sdk import LEASE_LABEL, OWNER_LABEL, MarketplaneClient, Record

from .types import ORDER_TYPE, OrderPhase, PolymarketOrderRecord
from .user_channel import UserChannel

logger = logging.getLogger(__name__)

SIGNATURE_LABEL = "polymarket.io/signature"
ORDER_ID_LABEL = "polymarket.io/order-id"


class OrderReconciler:
    def __init__(self, controller: Controller, client: MarketplaneClient, clob: ClobClient, *, tradespace: str, lease: float):
        self._client = client
        self._clob = clob
        self._lease = lease
        self._owner = uuid4().hex
        controller.on_existing(ORDER_TYPE, tradespace=tradespace)(self._reconcile)

    async def _reconcile(self, n: RecordNotification) -> None:
        record = await self._client.get_record(ORDER_TYPE, n.record.tradespace, n.record.name)
        order = PolymarketOrderRecord.model_validate(record.spec)
        if not order.spec.active or order.status.phase is not OrderPhase.pending:
            return
        if order.spec.signature is not None:
            logger.warning("order %s/%s already signed but unconfirmed; leaving for recovery", record.tradespace, record.name)
            return
        owner = record.labels.get(OWNER_LABEL)
        if owner is not None and owner != self._owner and time.time() < float(record.labels.get(LEASE_LABEL, "0")):
            return
        async with self._client.ownership(record, owner=self._owner, until=time.time() + self._lease) as owned:
            args = OrderArgs(
                token_id=order.spec.token,
                price=float(order.spec.price),
                size=float(order.spec.size),
                side=order.spec.side.value.upper(),
            )
            signed = await asyncio.to_thread(self._clob.create_order, args)
            signed_spec = order.spec.model_copy(update={"signature": signed.signature})
            owned = replace(
                owned,
                revision=owned.revision + 1,
                labels={**owned.labels, SIGNATURE_LABEL: signed.signature},
                spec=PolymarketOrderRecord(spec=signed_spec, status=order.status).model_dump(by_alias=True, mode="json"),
            )
            await self._client.update_record(owned)
            response = await asyncio.to_thread(self._clob.post_order, signed, order.spec.type.value)
            status = order.status.model_copy(update={
                "approved": True,
                "phase": OrderPhase.placed,
                "polymarket_order_id": response["orderID"],
            })
            owned = replace(
                owned,
                revision=owned.revision + 1,
                labels={**owned.labels, ORDER_ID_LABEL: response["orderID"]},
                spec=PolymarketOrderRecord(spec=signed_spec, status=status).model_dump(by_alias=True, mode="json"),
            )
            await self._client.update_record(owned)
        logger.info("placed polymarket order %s for %s/%s", response["orderID"], record.tradespace, record.name)


class StatusReconciler:
    def __init__(self, controller: Controller, client: MarketplaneClient, clob: ClobClient, channel: UserChannel, *, tradespace: str, interval: float):
        self._client = client
        self._clob = clob
        self._channel = channel
        self._tradespace = tradespace
        controller.on_schedule(interval)(self._resync)

    async def run(self) -> None:
        async for event in self._channel.events():
            if event.get("event_type") == "order":
                await self._track(event["id"], Decimal(event["size_matched"]), event["type"])

    async def _resync(self) -> None:
        for record in await self._client.list_records(ORDER_TYPE, self._tradespace):
            order = PolymarketOrderRecord.model_validate(record.spec)
            if order.status.polymarket_order_id is None:
                continue
            remote = await asyncio.to_thread(self._clob.get_order, order.status.polymarket_order_id)
            await self._track(order.status.polymarket_order_id, Decimal(remote["size_matched"]), remote["status"])

    async def _track(self, order_id: str, filled: Decimal, api_status: str) -> None:
        for record in await self._client.list_records(ORDER_TYPE, self._tradespace, labels={ORDER_ID_LABEL: order_id}):
            order = PolymarketOrderRecord.model_validate(record.spec)
            if order.status.filled == filled and order.status.api_status == api_status:
                continue
            status = order.status.model_copy(update={"filled": filled, "api_status": api_status})
            updated = replace(record, revision=record.revision + 1, spec=PolymarketOrderRecord(spec=order.spec, status=status).model_dump(by_alias=True, mode="json"))
            try:
                await self._client.update_record(updated)
            except AioRpcError as err:
                logger.warning("status update for %s conflicted; retrying on next event", order_id, exc_info=err)
