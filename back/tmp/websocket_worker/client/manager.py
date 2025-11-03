import asyncio
from typing import Dict, Optional, Set, Any, List, Iterator
from websockets.asyncio.server import ServerConnection

from server.helper.logging import LoggingHelper
from server.websocket_worker.client.client import Client


class ClientCollection:
    """
    Коллекция клиентов с быстрым поиском по ID.
    ID хранится только в объекте Client.
    """

    def __init__(self):
        self._clients: List[Client] = []
        self._index: dict[str, int] = {}  # client_id -> позиция в списке
        self._lock = asyncio.Lock()

    async def add(self, client: Client) -> None:
        """Добавить клиента в коллекцию"""
        async with self._lock:
            if client.ctx.id in self._index:
                raise ValueError(f"Client {client.ctx.id} already exists")

            position = len(self._clients)
            self._clients.append(client)
            self._index[client.ctx.id] = position

    async def remove(self, client_id: str) -> Optional[Client]:
        """Удалить клиента по ID"""
        async with self._lock:
            if client_id not in self._index:
                return None

            position = self._index[client_id]
            client = self._clients[position]

            # Удаляем клиента, заменяя его последним элементом (O(1))
            last_client = self._clients[-1]
            self._clients[position] = last_client
            self._clients.pop()

            # Обновляем индекс
            if position < len(self._clients):
                self._index[last_client.ctx.id] = position
            del self._index[client_id]

            return client

    async def get(self, client_id: str) -> Optional[Client]:
        """Получить клиента по ID"""
        async with self._lock:
            position = self._index.get(client_id)
            if position is not None:
                return self._clients[position]
            return None

    async def get_all(self) -> List[Client]:
        """Получить всех клиентов"""
        async with self._lock:
            return self._clients.copy()

    def __len__(self) -> int:
        """Количество клиентов"""
        return len(self._clients)

    def __iter__(self) -> Iterator[Client]:
        """Итерация по клиентам"""
        return iter(self._clients)

    async def clear(self) -> None:
        """Удалить всех клиентов"""
        async with self._lock:
            self._clients.clear()
            self._index.clear()

    @property
    def ids(self) -> Set[str]:
        """Множество всех ID клиентов"""
        return set(self._index.keys())


class ClientManager:
    """
    Менеджер клиентских WebSocket-соединений.

    Ответственность:
    - Регистрация и управление жизненным циклом клиентов
    - Параллельная обработка клиентских подключений через TaskGroup
    - Graceful shutdown всех клиентов
    - Предоставление API для работы с клиентами
    """

    def __init__(self):
        self._clients = ClientCollection()
        self._shutdown_event = asyncio.Event()
        self._logger = LoggingHelper.getLogger("client_manager")

    async def process(self, websocket: ServerConnection):
        """
        Основной метод для обработки нового клиентского подключения.
        Создает клиента и управляет его жизненным циклом.
        """
        async with Client(websocket) as client:
            try:
                await self._clients.add(client)
                await client.run(self._shutdown_event)
            except Exception as _e:
                pass
            finally:
                await self._clients.remove(client.ctx.id)

    def shutdown_signal(self):
        """
        Инициация graceful shutdown всех клиентов.
        Можно вызывать безопасно несколько раз.
        """
        self._logger.info("shutdown...")

        # Устанавливаем событие shutdown для всех клиентов
        self._shutdown_event.set()

    async def shutdown_wait(self):
        while len(self._clients) > 0:
            await asyncio.sleep(0.5)

    @property
    def is_shutdown(self):
        return self._shutdown_event.is_set()

    async def health_check(self) -> Dict[str, Any]:
        """
        Проверка здоровья менеджера.

        Returns:
            Статус менеджера и его клиентов
        """
        return {
            "shutdown_initiated": self.is_shutdown,
        }
