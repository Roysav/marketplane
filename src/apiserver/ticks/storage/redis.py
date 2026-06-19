from collections.abc import AsyncIterator

import redis.asyncio as redis

from .exceptions import KeyNotFound


class RedisTickStorage:
    def __init__(self, client: redis.Redis) -> None:
        self._client = client

    async def publish(self, key: str, value: str) -> None:
        await self._client.set(key, value)
        await self._client.publish(key, value)

    async def get(self, key: str) -> str:
        result = await self._client.get(key)
        if result is None:
            raise KeyNotFound(key)
        return result.decode()

    async def subscribe(self, key: str) -> AsyncIterator[str]:
        async with self._client.pubsub() as pubsub:
            await pubsub.subscribe(key)
            async for message in pubsub.listen():
                if message["type"] == "message":
                    yield message["data"].decode()
