from .exceptions import KeyAlreadyExists, KeyNotFound, RevisionMismatch


class MemoryRecordStorage:
    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}
        self._indices: dict[str, set[str]] = {}
        self._revisions: dict[str, int] = {}

    async def create(self, key: str, value: bytes, indices: list[str]) -> None:
        if key in self._store:
            raise KeyAlreadyExists(key)
        self._store[key] = value
        self._revisions[key] = 0
        for index in indices:
            self._indices.setdefault(index, set()).add(key)

    async def update(self, key: str, value: bytes, indices: list[str], target_revision: int) -> None:
        if key not in self._store or self._revisions[key] != target_revision - 1:
            raise RevisionMismatch(key)
        for idx_set in self._indices.values():
            idx_set.discard(key)
        self._store[key] = value
        self._revisions[key] = target_revision
        for index in indices:
            self._indices.setdefault(index, set()).add(key)

    async def set(self, key: str, value: bytes, indices: list[str]) -> None:
        for idx_set in self._indices.values():
            idx_set.discard(key)
        self._store[key] = value
        self._revisions[key] = self._revisions.get(key, -1) + 1
        for index in indices:
            self._indices.setdefault(index, set()).add(key)

    async def get(self, key: str) -> tuple[bytes, int]:
        if key not in self._store:
            raise KeyNotFound(key)
        return self._store[key], self._revisions[key]

    async def list(self, prefix: str, indices: list[str] | None) -> list[tuple[bytes, int]]:
        if indices is None:
            keys = {k for k in self._store if k.startswith(prefix)}
        else:
            keys: set[str] = set()
            for n, i in enumerate(indices):
                s = self._indices.get(i, set())
                keys = s if n == 0 else keys & s
            keys = {k for k in keys if k.startswith(prefix)}
        return [(self._store[k], self._revisions[k]) for k in keys]
