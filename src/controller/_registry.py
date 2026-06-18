import dataclasses
from collections.abc import Awaitable, Callable, Iterator

from controller.notifications import BaseNotification, NotificationType, RecordNotification, TickNotification

Handler = Callable[[BaseNotification], Awaitable[None]]


@dataclasses.dataclass(frozen=True)
class Selector:
    types: frozenset[NotificationType]
    record_type: str | None = None
    tradespace: str | None = None
    labels: tuple[tuple[str, str], ...] = ()
    tick_name: str | None = None

    def matches(self, notification: BaseNotification) -> bool:
        if notification.type not in self.types:
            return False
        if isinstance(notification, RecordNotification):
            record = notification.record
            if self.record_type is not None and record.type != self.record_type:
                return False
            if self.tradespace is not None and record.tradespace != self.tradespace:
                return False
            return all(record.labels.get(k) == v for k, v in self.labels)
        if isinstance(notification, TickNotification):
            return self.tick_name is None or notification.name == self.tick_name
        return True


class HandlerRegistry:
    def __init__(self) -> None:
        self._entries: list[tuple[Selector, Handler]] = []

    def register(self, handler: Handler, selector: Selector) -> None:
        self._entries.append((selector, handler))

    def iter_handlers(self, notification: BaseNotification) -> Iterator[Handler]:
        for selector, handler in self._entries:
            if selector.matches(notification):
                yield handler
