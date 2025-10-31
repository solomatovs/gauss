from typing import Dict, Any

from gauss.core.ports.kv_storage import BaseKVStorage


class MemoryKVStorage(BaseKVStorage):
    def __init__(self):
        self._data: Dict[str, Any] = {}

    async def get(self, key: str) -> Any:
        return self._data.get(key)

    async def set(self, key: str, value: Any) -> None:
        self._data[key] = value

    async def delete(self, key: str) -> None:
        if key in self._data:
            del self._data[key]

    async def get_all(self, prefix: str) -> Dict[str, Any]:
        return {k: v for k, v in self._data.items() if k.startswith(prefix)}

    async def delete_by_prefix(self, prefix: str) -> None:
        keys_to_delete = [k for k in self._data.keys() if k.startswith(prefix)]
        for key in keys_to_delete:
            del self._data[key]

    def __repr__(self):
        return f"MemoryStorage({self._data})"

    async def close(self):
        pass
