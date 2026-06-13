import json
from typing import Any, Protocol

import pydantic

from apiserver.types import Subject


class RecordStorage(Protocol):
    async def set(self, key: str, value: bytes, indexes: list[str]): ...
    async def get(self, key: str) -> bytes: ...
    async def list(self, prefix: str, indices: list[str] | None) -> list[bytes]: ...


class RecordMetadata(pydantic.BaseModel):
    name: str
    labels: dict[str, str] = {}

class Record(pydantic.BaseModel):
    type: str
    tradespace: str
    metadata: RecordMetadata
    spec: dict[str, Any] = {}

    @classmethod
    def from_bytes(cls, value: bytes) -> Record:
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
        return Record.from_bytes(await self._backend.get(subject.key()))

    async def set_record(self, record: Record) -> None:
        key = f"{record.type}/{record.tradespace}/{record.metadata.name}"
        await self._backend.set(key, record.to_bytes(), _labels_to_indices(record.metadata.labels) or [])

    async def list_records(self, type_: str, tradespace: str = None, labels: dict[str, str] = None, *, all_tradespaces=False) -> list[Record]:
        prefix = type_

        if not all_tradespaces:
            if tradespace is None:
                raise ValueError("`tradespace` not set when `all_tradespaces=False`")
            prefix += "/" + tradespace

        return [Record.from_bytes(item) for item in await self._backend.list(prefix, _labels_to_indices(labels))]