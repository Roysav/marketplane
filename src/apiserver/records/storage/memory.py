from apiserver.records.storage.exceptions import KeyAlreadyExists, KeyNotFound, RevisionMismatch


class MemoryRecordStorage:
    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}
        self._indexes: dict[str, set[str]] = {}
        self._revisions: dict[str, int] = {}

    async def create(self, key: str, value: bytes, indexes: list[str]) -> None:
        if key in self._store:
            raise KeyAlreadyExists(key)
        self._store[key] = value
        self._revisions[key] = 0
        for index in indexes:
            self._indexes.setdefault(index, set()).add(key)

    async def update(self, key: str, value: bytes, indexes: list[str], expected_revision: int) -> None:
        if key not in self._store or self._revisions[key] != expected_revision - 1:
            raise RevisionMismatch(key)
        for idx_set in self._indexes.values():
            idx_set.discard(key)
        self._store[key] = value
        self._revisions[key] = expected_revision
        for index in indexes:
            self._indexes.setdefault(index, set()).add(key)

    async def set(self, key: str, value: bytes, indexes: list[str]) -> None:
        for idx_set in self._indexes.values():
            idx_set.discard(key)
        self._store[key] = value
        self._revisions[key] = self._revisions.get(key, -1) + 1
        for index in indexes:
            self._indexes.setdefault(index, set()).add(key)

    async def get(self, key: str) -> bytes:
        if key not in self._store:
            raise KeyNotFound(key)
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
