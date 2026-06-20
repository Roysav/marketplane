import asyncio
import logging
from dataclasses import replace
from uuid import uuid4

from py_clob_client_v2 import ClobClient, OrderArgs

from controller import Controller, RecordNotification
from sdk import MarketplaneClient, Record

from .types import ORDER_TYPE, OrderPhase, PolymarketOrderRecord

logger = logging.getLogger(__name__)

SIGNATURE_LABEL = "polymarket.io/signature"


class OrderReconciler:
    def __init__(self, controller: Controller, client: MarketplaneClient, clob: ClobClient, *, tradespace: str):
        self._client = client
        self._clob = clob
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
        async with self._client.ownership(record, self._owner) as owned:
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
                spec=PolymarketOrderRecord(spec=signed_spec, status=status).model_dump(by_alias=True, mode="json"),
            )
            await self._client.update_record(owned)
        logger.info("placed polymarket order %s for %s/%s", response["orderID"], record.tradespace, record.name)
