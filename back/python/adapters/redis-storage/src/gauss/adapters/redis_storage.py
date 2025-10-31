from typing import Dict, Any

import redis.asyncio as aioredis

from gauss.core.ports.kv_storage import BaseKVStorage


class RedisStorage(BaseKVStorage):
    def __init__(self, url: str = "redis://localhost"):
        self._client = aioredis.from_url(url, decode_responses=True)

    async def get(self, key: str) -> Any:
        return await self._client.get(key)

    async def set(self, key: str, value: Any) -> None:
        await self._client.set(key, value)

    async def delete(self, key: str) -> None:
        await self._client.delete(key)

    async def get_all(self, prefix: str) -> Dict[str, Any]:
        keys = await self._client.keys(f"{prefix}*")
        values = await self._client.mget(keys)
        return dict(zip(keys, values))

    async def delete_by_prefix(self, prefix: str) -> None:
        keys = await self._client.keys(f"{prefix}*")
        if keys:
            await self._client.delete(*keys)

    async def close(self):
        await self._client.close()

    def __repr__(self):
        return "<RedisStorage>"
        # return f"<RedisStorage host={self._r.connection_pool.connection_kwargs.get('host')} db={self._r.connection_pool.connection_kwargs.get('db')}>"
