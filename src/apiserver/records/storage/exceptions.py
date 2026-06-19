class KeyNotFound(Exception):
    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__({"key": key})


class KeyAlreadyExists(Exception):
    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__({"key": key})


class RevisionMismatch(Exception):
    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__({"key": key})
