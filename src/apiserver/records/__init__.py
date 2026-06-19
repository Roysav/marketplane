import json
from typing import Any, Protocol

import pydantic

from apiserver.errors import RecordAlreadyExists, RecordNotFound, RecordRevisionConflict
from apiserver.records.storage.exceptions import KeyAlreadyExists, KeyNotFound, RevisionMismatch
from apiserver.types import Subject


class RecordStorage(Protocol):
    async def create(self, key: str, value: bytes, indexes: list[str]) -> None: ...
    async def update(self, key: str, value: bytes, indexes: list[str], expected_revision: int) -> None: ...
    async def set(self, key: str, value: bytes, indexes: list[str]) -> None: ...
    async def get(self, key: str) -> bytes: ...
    async def list(self, prefix: str, indices: list[str] | None) -> list[bytes]: ...


class RecordMetadata(pydantic.BaseModel):
    name: str
    labels: dict[str, str] = {}
    revision: int = 0


class Record(pydantic.BaseModel):
    type: str
    tradespace: str
    metadata: RecordMetadata
    spec: dict[str, Any] = {}

    @classmethod
    def from_bytes(cls, value: bytes) -> "Record":
        return cls(**json.loads(value))

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()


def _labels_to_indices(labels: dict[str, str] | None) -> list[str] | None:
    if labels is None:
        return None
    return [f"{k}={v}" for k, v in labels.items()]


class RecordsClient:
    def __init__(self, backend: RecordStorage):
        self._backend = backend

    async def get_record(self, subject: Subject) -> Record:
        try:
            data = await self._backend.get(subject.key())
        except KeyNotFound:
            raise RecordNotFound(subject.type, subject.tradespace, subject.name)
        return Record.from_bytes(data)

    async def create_record(self, record: Record) -> None:
        record = record.model_copy(update={"metadata": record.metadata.model_copy(update={"revision": 0})})
        key = f"{record.type}/{record.tradespace}/{record.metadata.name}"
        try:
            await self._backend.create(key, record.to_bytes(), _labels_to_indices(record.metadata.labels) or [])
        except KeyAlreadyExists:
            raise RecordAlreadyExists(record.type, record.tradespace, record.metadata.name)

    async def update_record(self, record: Record) -> None:
        key = f"{record.type}/{record.tradespace}/{record.metadata.name}"
        try:
            await self._backend.update(key, record.to_bytes(), _labels_to_indices(record.metadata.labels) or [], record.metadata.revision)
        except RevisionMismatch:
            raise RecordRevisionConflict(record.type, record.tradespace, record.metadata.name)

    async def apply_record(self, record: Record) -> None:
        key = f"{record.type}/{record.tradespace}/{record.metadata.name}"
        value = record.model_dump_json(exclude={"metadata": {"revision"}}).encode()
        await self._backend.set(key, value, _labels_to_indices(record.metadata.labels) or [])

    async def list_records(self, type_: str, tradespace: str = None, labels: dict[str, str] = None, *, all_tradespaces=False) -> list[Record]:
        prefix = type_

        if not all_tradespaces:
            if tradespace is None:
                raise ValueError("`tradespace` not set when `all_tradespaces=False`")
            prefix += "/" + tradespace

        return [Record.from_bytes(item) for item in await self._backend.list(prefix, _labels_to_indices(labels))]
