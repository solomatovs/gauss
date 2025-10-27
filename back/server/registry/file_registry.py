import json
import asyncio
from typing import List, Optional
from pathlib import Path

from server.core.worker_context import WorkerContext
from server.core.worker_registry import WorkerRegistry


# class FileWorkerRegistry(WorkerRegistry):
#     """Хранит метаинформацию о воркерах в локальном JSON-файле."""

#     def __init__(self, path: str):
#         self._path = Path(path)
#         self._lock = asyncio.Lock()

#     async def _read_file(self) -> dict:
#         if not self._path.exists():
#             return {}
#         async with self._lock:
#             data = json.loads(self._path.read_text())
#         return data

#     async def _write_file(self, data: dict):
#         async with self._lock:
#             self._path.write_text(json.dumps(data, indent=2, ensure_ascii=False))

#     async def list_workers(self) -> List[WorkerContext]:
#         data = await self._read_file()
#         return [
#             WorkerContext(id=k, **v)
#             for k, v in data.items()
#         ]

#     async def get_worker(self, worker_id: str) -> Optional[WorkerContext]:
#         data = await self._read_file()
#         if worker_id not in data:
#             return None
#         return WorkerContext(id=worker_id, **data[worker_id])

#     async def register_worker(self, worker: WorkerContext) -> None:
#         data = await self._read_file()
#         data[worker.id] = {
#             "host": worker.host,
#             "port": worker.port,
#             "last_heartbeat": worker.last_heartbeat.isoformat(),
#         }
#         await self._write_file(data)

#     async def unregister_worker(self, worker_id: str) -> None:
#         data = await self._read_file()
#         data.pop(worker_id, None)
#         await self._write_file(data)