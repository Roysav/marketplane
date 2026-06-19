from decimal import Decimal

import grpc

from .types import Subject


class ServiceError(Exception):
    grpc_code: grpc.StatusCode

    def __init__(self, **data: object) -> None:
        self.data = data
        super().__init__(data)

    def as_grpc(self) -> tuple[grpc.StatusCode, str]:
        return self.grpc_code, str(self.data)


class RecordNotFound(ServiceError):
    grpc_code = grpc.StatusCode.NOT_FOUND

    def __init__(self, subject: Subject) -> None:
        self.subject = subject
        super().__init__(subject=subject.key())


class RecordAlreadyExists(ServiceError):
    grpc_code = grpc.StatusCode.ALREADY_EXISTS

    def __init__(self, subject: Subject) -> None:
        self.subject = subject
        super().__init__(subject=subject.key())


class RecordRevisionConflict(ServiceError):
    grpc_code = grpc.StatusCode.ABORTED

    def __init__(self, subject: Subject) -> None:
        self.subject = subject
        super().__init__(subject=subject.key())


class TickNotFound(ServiceError):
    grpc_code = grpc.StatusCode.NOT_FOUND

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(name=name)


class InsufficientBalance(ServiceError):
    grpc_code = grpc.StatusCode.FAILED_PRECONDITION

    def __init__(self, principal: str, currency: str, amount: Decimal) -> None:
        self.principal = principal
        self.currency = currency
        self.amount = amount
        super().__init__(principal=principal, currency=currency, amount=str(amount))
