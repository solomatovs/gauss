from abc import ABC, abstractmethod

from gauss.core.ports.worker_context import WorkerContext


class WorkerRegistry(ABC):
    """
    Абстрактный интерфейс для работы с информацией о воркерах
    """

    @abstractmethod
    async def list_workers(self) -> list[WorkerContext]:
        """Вернуть список всех известных воркеров."""
        pass

    @abstractmethod
    async def get_worker(self, worker_id: str) -> WorkerContext | None:
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
