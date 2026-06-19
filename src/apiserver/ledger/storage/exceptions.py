from decimal import Decimal


class InsufficientBalanceError(Exception):
    def __init__(self, principal: str, currency: str, amount: Decimal) -> None:
        self.principal = principal
        self.currency = currency
        self.amount = amount
        super().__init__({"principal": principal, "currency": currency, "amount": str(amount)})
