from decimal import Decimal
from typing import Protocol

import pydantic

from apiserver.types import Subject


class InsufficientBalanceError(Exception):
    pass


class LedgerEntry(pydantic.BaseModel):
    from_principal: str
    to_principal: str
    currency: str
    amount: Decimal
    subject: str


class LedgerStorage(Protocol):
    async def allocate(self, from_principal: str, to_principal: str, currency: str, amount: Decimal, subject: str, *, from_balance_inf: bool = False) -> None: ...
    async def balance(self, principal: str, currency: str) -> Decimal: ...


class LedgerClient:
    def __init__(self, backend: LedgerStorage) -> None:
        self._backend = backend

    async def allocate(self, from_principal: str, to_principal: str, currency: str, amount: Decimal, subject: Subject) -> None:
        await self._backend.allocate(from_principal, to_principal, currency, amount, subject.key())

    async def grant(self, from_principal: str, to_principal: str, currency: str, amount: Decimal, subject: Subject) -> None:
        await self._backend.allocate(from_principal, to_principal, currency, amount, subject.key(), from_balance_inf=True)

    async def balance(self, principal: str, currency: str) -> Decimal:
        return await self._backend.balance(principal, currency)
