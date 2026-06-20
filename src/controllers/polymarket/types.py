import enum
from decimal import Decimal

import pydantic
from pydantic.alias_generators import to_camel

ORDER_TYPE = "alphav1/polymarket/Order"

USDC = "USDC"


def asset_currency(token: str) -> str:
    return f"polymarket/assets/{token}"


class OrderSide(str, enum.Enum):
    buy = "buy"
    sell = "sell"


class OrderType(str, enum.Enum):
    gtc = "GTC"
    gtd = "GTD"
    fok = "FOK"
    fak = "FAK"


class OrderPhase(str, enum.Enum):
    pending = "pending"
    placed = "placed"
    filled = "filled"
    cancelled = "cancelled"
    failed = "failed"


class _Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(alias_generator=to_camel, populate_by_name=True)


class OrderSpec(_Model):
    type: OrderType
    token: str
    side: OrderSide
    size: Decimal
    price: Decimal
    active: bool
    signature: str | None = None

    def reserve_currency(self) -> str:
        return USDC if self.side is OrderSide.buy else asset_currency(self.token)

    def reserve_amount(self) -> Decimal:
        return self.size * self.price if self.side is OrderSide.buy else self.size


class OrderStatus(_Model):
    approved: bool = False
    phase: OrderPhase = OrderPhase.pending
    polymarket_order_id: str | None = None
    last_error: str | None = None


class PolymarketOrderRecord(_Model):
    spec: OrderSpec
    status: OrderStatus = pydantic.Field(default_factory=OrderStatus)
