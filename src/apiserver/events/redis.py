from collections.abc import AsyncIterator

import redis.asyncio as redis

from apiserver.events import Event


class RedisEventStorage:
    def __init__(self, client: redis.Redis) -> None:
        self._client = client

    async def publish(self, stream: str, event: Event) -> None:
        await self._client.publish(stream, event.model_dump_json())

    async def watch(self, prefix: str) -> AsyncIterator[Event]:
        async with self._client.pubsub() as pubsub:
            await pubsub.psubscribe(f"{prefix}*")
            async for message in pubsub.listen():
                if message["type"] == "pmessage":
                    yield Event.model_validate_json(message["data"])
