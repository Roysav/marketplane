class MemoryRecordStorage:
    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}
        self._indexes: dict[str, set[str]] = {}

    async def set(self, key: str, value: bytes, indexes: list[str]) -> None:
        self._store[key] = value
        for index in indexes:
            self._indexes.setdefault(index, set()).add(key)

    async def get(self, key: str) -> bytes:
        return self._store[key]

    async def list(self, prefix: str, indices: list[str] | None) -> list[bytes]:
        if indices is None:
            keys = {k for k in self._store if k.startswith(prefix)}
        else:
            keys: set[str] = set()
            for n, i in enumerate(indices):
                s = self._indexes.get(i, set())
                keys = s if n == 0 else keys & s
            keys = {k for k in keys if k.startswith(prefix)}
        return [self._store[k] for k in keys]
