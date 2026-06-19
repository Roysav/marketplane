import json
from typing import Any, Protocol

import pydantic

from ..errors import RecordAlreadyExists, RecordNotFound, RecordRevisionConflict
from ..types import Subject
from .storage.exceptions import KeyAlreadyExists, KeyNotFound, RevisionMismatch


class RecordStorage(Protocol):
    async def create(self, subject: Subject, value: bytes, indices: list[str]) -> None: ...
    async def update(self, subject: Subject, value: bytes, indices: list[str], target_revision: int) -> None: ...
    async def set(self, subject: Subject, value: bytes, indices: list[str]) -> None: ...
    async def get(self, subject: Subject) -> bytes: ...
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

    def subject(self) -> Subject:
        return Subject(type=self.type, tradespace=self.tradespace, name=self.metadata.name)

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
            data = await self._backend.get(subject)
        except KeyNotFound as err:
            raise RecordNotFound(subject) from err
        return Record.from_bytes(data)

    async def create_record(self, record: Record) -> None:
        record = record.model_copy(update={"metadata": record.metadata.model_copy(update={"revision": 0})})
        subject = record.subject()
        try:
            await self._backend.create(subject, record.to_bytes(), _labels_to_indices(record.metadata.labels) or [])
        except KeyAlreadyExists as err:
            raise RecordAlreadyExists(subject) from err

    async def update_record(self, record: Record) -> None:
        subject = record.subject()
        try:
            await self._backend.update(subject, record.to_bytes(), _labels_to_indices(record.metadata.labels) or [], record.metadata.revision)
        except RevisionMismatch as err:
            raise RecordRevisionConflict(subject) from err

    async def apply_record(self, record: Record) -> None:
        subject = record.subject()
        value = record.model_dump_json(exclude={"metadata": {"revision"}}).encode()
        await self._backend.set(subject, value, _labels_to_indices(record.metadata.labels) or [])

    async def list_records(self, type_: str, tradespace: str | None = None, labels: dict[str, str] | None = None, *, all_tradespaces: bool = False) -> list[Record]:
        prefix = type_
        if not all_tradespaces:
            if tradespace is None:
                raise ValueError("`tradespace` not set when `all_tradespaces=False`")
            prefix += "/" + tradespace
        return [Record.from_bytes(item) for item in await self._backend.list(prefix, _labels_to_indices(labels))]
