import grpc


class ServiceError(Exception):
    grpc_code: grpc.StatusCode

    def as_grpc(self) -> tuple[grpc.StatusCode, str]:
        return self.grpc_code, str(self)


class RecordNotFound(ServiceError):
    grpc_code = grpc.StatusCode.NOT_FOUND

    def __init__(self, type_: str, tradespace: str, name: str) -> None:
        self.type = type_
        self.tradespace = tradespace
        self.name = name
        super().__init__(f"record {type_}/{tradespace}/{name} not found")


class RecordAlreadyExists(ServiceError):
    grpc_code = grpc.StatusCode.ALREADY_EXISTS

    def __init__(self, type_: str, tradespace: str, name: str) -> None:
        self.type = type_
        self.tradespace = tradespace
        self.name = name
        super().__init__(f"record {type_}/{tradespace}/{name} already exists")


class RecordRevisionConflict(ServiceError):
    grpc_code = grpc.StatusCode.ABORTED

    def __init__(self, type_: str, tradespace: str, name: str) -> None:
        self.type = type_
        self.tradespace = tradespace
        self.name = name
        super().__init__(f"record {type_}/{tradespace}/{name} revision conflict")


class TickNotFound(ServiceError):
    grpc_code = grpc.StatusCode.NOT_FOUND

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"tick {name!r} not found")


class InsufficientBalance(ServiceError):
    grpc_code = grpc.StatusCode.FAILED_PRECONDITION

    def __init__(self, principal: str, currency: str, amount) -> None:
        self.principal = principal
        self.currency = currency
        self.amount = amount
        super().__init__(f"principal {principal!r} has insufficient {currency} balance for {amount}")
