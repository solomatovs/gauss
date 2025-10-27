from abc import ABC, abstractmethod
from typing import List, Optional

from server.core.worker_context import WorkerContext


class WorkerRegistry(ABC):
    """
    Абстрактный интерфейс для работы с информацией о воркерах
    """

    @abstractmethod
    async def list_workers(self) -> List[WorkerContext]:
        """Вернуть список всех известных воркеров."""
        pass

    @abstractmethod
    async def get_worker(self, worker_id: str) -> Optional[WorkerContext]:
        """Получить метаинформацию конкретного воркера по ID."""
        pass

    @abstractmethod
    async def register_worker(self, worker: WorkerContext) -> None:
        """Зарегистрировать или обновить информацию о воркере."""
        pass

    @abstractmethod
    async def unregister_worker(self, worker_id: str) -> None:
        """Удалить воркера из реестра."""
        pass
