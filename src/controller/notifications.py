import dataclasses
import enum
from collections.abc import Hashable
from typing import Any

from sdk import Record, RecordEvent


class NotificationType(enum.Enum):
    RECORD_CREATED = "RECORD_CREATED"
    RECORD_UPDATED = "RECORD_UPDATED"
    RECORD_DELETED = "RECORD_DELETED"
    TICK_CHANGED = "TICK_CHANGED"


_ACTION_TYPES = {
    "created": NotificationType.RECORD_CREATED,
    "updated": NotificationType.RECORD_UPDATED,
    "deleted": NotificationType.RECORD_DELETED,
}


@dataclasses.dataclass(frozen=True)
class BaseNotification:
    type: NotificationType

    @property
    def key(self) -> Hashable:
        raise NotImplementedError


@dataclasses.dataclass(frozen=True)
class RecordNotification(BaseNotification):
    record: Record

    @classmethod
    def from_event(cls, event: RecordEvent) -> "RecordNotification":
        return cls(_ACTION_TYPES[event.action], event.record)

    @property
    def key(self) -> Hashable:
        return ("record", self.record.type, self.record.tradespace, self.record.name)


@dataclasses.dataclass(frozen=True)
class TickNotification(BaseNotification):
    name: str
    value: Any

    @classmethod
    def from_tick(cls, name: str, value: Any) -> "TickNotification":
        return cls(NotificationType.TICK_CHANGED, name, value)

    @property
    def key(self) -> Hashable:
        return ("tick", self.name)
