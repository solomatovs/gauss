from typing import Dict, Any


class BaseKVStorage:
    async def get(self, key: str) -> Any:
        raise NotImplementedError

    async def set(self, key: str, value: Any) -> None:
        raise NotImplementedError

    async def delete(self, key: str) -> None:
        raise NotImplementedError

    async def get_all(self, prefix: str) -> Dict[str, Any]:
        raise NotImplementedError

    async def delete_by_prefix(self, prefix: str) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError
