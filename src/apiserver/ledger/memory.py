import asyncio
from decimal import Decimal

from apiserver.ledger import InsufficientBalanceError, LedgerEntry


class MemoryLedgerStorage:
    def __init__(self) -> None:
        self._entries: list[LedgerEntry] = []
        self._lock = asyncio.Lock()

    def _compute_balance(self, principal: str, currency: str) -> Decimal:
        total = Decimal(0)
        for entry in self._entries:
            if entry.currency != currency:
                continue
            if entry.to_principal == principal:
                total += entry.amount
            if entry.from_principal == principal:
                total -= entry.amount
        return total

    async def allocate(self, from_principal: str, to_principal: str, currency: str, amount: Decimal, subject: str, *, from_balance_inf: bool = False) -> None:
        async with self._lock:
            if not from_balance_inf and self._compute_balance(from_principal, currency) < amount:
                raise InsufficientBalanceError(from_principal, currency, amount)
            self._entries.append(LedgerEntry(from_principal=from_principal, to_principal=to_principal, currency=currency, amount=amount, subject=subject))

    async def balance(self, principal: str, currency: str) -> Decimal:
        async with self._lock:
            return self._compute_balance(principal, currency)
