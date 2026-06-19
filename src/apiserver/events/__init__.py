from collections.abc import AsyncIterator
from typing import Literal, Protocol

import pydantic

from ..types import Subject


class Event(pydantic.BaseModel):
    action: Literal["created", "deleted", "updated"]
    subject: Subject


class EventStorage(Protocol):
    async def publish(self, stream: str, event: Event) -> None: ...
    def watch(self, prefix: str) -> AsyncIterator[Event]: ...


class EventsClient:
    def __init__(self, backend: EventStorage) -> None:
        self._backend = backend

    async def publish(self, action: Literal["created", "deleted", "updated"], subject: Subject) -> None:
        stream = subject.key()
        await self._backend.publish(stream, Event(action=action, subject=subject))

    def watch(
        self,
        type_: str,
        tradespace: str | None = None,
        *,
        all_tradespaces: bool = False,
    ) -> AsyncIterator[Event]:
        prefix = type_
        if not all_tradespaces:
            if tradespace is None:
                raise ValueError("`tradespace` not set when `all_tradespaces=False`")
            prefix += f"/{tradespace}"
        return self._backend.watch(prefix)
