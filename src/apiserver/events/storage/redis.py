import re
from collections.abc import AsyncIterator

import redis.asyncio as redis

from .. import Event

_GLOB_META = re.compile(r"([\\*?\[\]])")


def _escape_glob(value: str) -> str:
    return _GLOB_META.sub(r"\\\1", value)


class RedisEventStorage:
    def __init__(self, client: redis.Redis) -> None:
        self._client = client

    async def publish(self, stream: str, event: Event) -> None:
        await self._client.publish(stream, event.model_dump_json())

    async def watch(self, prefix: str) -> AsyncIterator[Event]:
        async with self._client.pubsub() as pubsub:
            await pubsub.psubscribe(_escape_glob(prefix) + "*")
            async for message in pubsub.listen():
                if message["type"] == "pmessage":
                    yield Event.model_validate_json(message["data"])
