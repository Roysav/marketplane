from ...types import Subject
from .exceptions import KeyAlreadyExists, KeyNotFound, RevisionMismatch


class MemoryRecordStorage:
    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}
        self._indices: dict[str, set[str]] = {}
        self._revisions: dict[str, int] = {}

    async def create(self, subject: Subject, value: bytes, indices: list[str]) -> None:
        key = subject.key()
        if key in self._store:
            raise KeyAlreadyExists(subject)
        self._store[key] = value
        self._revisions[key] = 0
        for index in indices:
            self._indices.setdefault(index, set()).add(key)

    async def update(self, subject: Subject, value: bytes, indices: list[str], target_revision: int) -> None:
        key = subject.key()
        if key not in self._store or self._revisions[key] != target_revision - 1:
            raise RevisionMismatch(subject)
        for idx_set in self._indices.values():
            idx_set.discard(key)
        self._store[key] = value
        self._revisions[key] = target_revision
        for index in indices:
            self._indices.setdefault(index, set()).add(key)

    async def set(self, subject: Subject, value: bytes, indices: list[str]) -> None:
        key = subject.key()
        for idx_set in self._indices.values():
            idx_set.discard(key)
        self._store[key] = value
        self._revisions[key] = self._revisions.get(key, -1) + 1
        for index in indices:
            self._indices.setdefault(index, set()).add(key)

    async def get(self, subject: Subject) -> bytes:
        key = subject.key()
        if key not in self._store:
            raise KeyNotFound(subject)
        return self._store[key]

    async def list(self, prefix: str, indices: list[str] | None) -> list[bytes]:
        if indices is None:
            keys = {k for k in self._store if k.startswith(prefix)}
        else:
            keys: set[str] = set()
            for n, i in enumerate(indices):
                s = self._indices.get(i, set())
                keys = s if n == 0 else keys & s
            keys = {k for k in keys if k.startswith(prefix)}
        return [self._store[k] for k in keys]
