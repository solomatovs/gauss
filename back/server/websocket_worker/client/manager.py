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
        # self._clients: Dict[str, Client] = {}
        self._clients = ClientCollection()
        self._client_tasks: Dict[str, asyncio.Task] = {}
        self._task_group: Optional[asyncio.TaskGroup] = None
        self._stop_event = asyncio.Event()
        self._shutdown_event = asyncio.Event()
        self._logger = LoggingHelper.getLogger("client_manager")
        
        # Для отслеживания статуса
        self._is_running = False
        self._shutdown_initiated = False
        
        # Lock для thread-safe операций с клиентами
        self._clients_lock = asyncio.Lock()

    async def __aenter__(self):
        """Инициализация менеджера при входе в контекст"""
        await self._start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Graceful shutdown при выходе из контекста"""
        await self._stop()
        # Не подавляем исключения
        return False

    async def _start(self):
        """Запуск менеджера"""
        if self._is_running:
            raise RuntimeError("ClientManager is already running")
        
        self._is_running = True
        self._logger.info("starting client manager...")
        
        # Сбрасываем события
        self._stop_event.clear()
        self._shutdown_event.clear()
        self._shutdown_initiated = False

    async def _stop(self):
        """Остановка менеджера с graceful shutdown всех клиентов"""
        if not self._is_running:
            return
        
        self._logger.info("stopping client manager...")
        
        # Инициируем shutdown если еще не начат
        if not self._shutdown_initiated:
            await self.shutdown()
        
        # Ждем завершения всех клиентов
        await self._wait_all_clients_closed()
        
        self._is_running = False
        self._logger.info("client manager stopped")

    async def process(self, websocket: ServerConnection):
        """
        Основной метод для обработки нового клиентского подключения.
        Создает клиента и управляет его жизненным циклом.
        """
        if not self._is_running:
            self._logger.warning("cannot process client: manager is not running")
            await websocket.close(code=1001, reason="server shutting down")
            return
        
        client = None
        client_id = None
        
        try:
            # Создаем и регистрируем клиента
            client = await self._create_client(websocket)
            client_id = client.ctx.id
            
            # Обрабатываем клиента с использованием TaskGroup
            async with asyncio.TaskGroup() as tg:
                # Сохраняем task group для клиента
                client._task_group = tg
                
                # Запускаем основную обработку клиента
                main_task = tg.create_task(client.run())
                
                # Запускаем мониторинг shutdown события
                tg.create_task(self._monitor_client_shutdown(client))
                
                # Ожидаем завершения
                await main_task
                
        except* asyncio.CancelledError:
            self._logger.info(f"client {client_id} processing cancelled")
        except* Exception as eg:
            for exc in eg.exceptions:
                self._logger.exception(f"error processing client {client_id}", exc_info=exc)
        finally:
            # Удаляем клиента из реестра
            if client_id:
                await self._remove_client(client_id)

    async def _create_client(self, websocket: ServerConnection) -> Client:
        """Создание и регистрация нового клиента"""
        async with self._clients_lock:
            # Проверяем, не в процессе ли shutdown
            if self._shutdown_initiated:
                raise RuntimeError("Cannot add client during shutdown")
            
            # Создаем клиента
            client = Client(websocket)
            
            # Регистрируем клиента
            await self._clients.add(client)
            
            self._logger.info(f"client {client.ctx.id} registered (total: {len(self._clients)})")
            
            return client

    async def _remove_client(self, client_id: str):
        """Удаление клиента из реестра"""
        async with self._clients_lock:
            client = await self._clients.remove(client_id)
            
            if client:
                # Гарантируем закрытие клиента
                await client.close()
                self._logger.info(f"client {client_id} removed (remaining: {len(self._clients)})")

    async def _monitor_client_shutdown(self, client: Client):
        """Мониторинг события shutdown для конкретного клиента"""
        await self._shutdown_event.wait()
        
        # Инициируем shutdown клиента
        self._logger.info(f"initiating shutdown for client {client.ctx.id}")
        await client.shutdown()

    async def shutdown(self):
        """
        Инициация graceful shutdown всех клиентов.
        Можно вызывать безопасно несколько раз.
        """
        if self._shutdown_initiated:
            return
        
        self._shutdown_initiated = True
        self._logger.info("initiating shutdown for all clients...")
        
        # Устанавливаем событие shutdown для всех клиентов
        self._shutdown_event.set()
        
        # Получаем копию списка клиентов для итерации
        async with self._clients_lock:
            client_ids = list(self._clients.ids)
        
        self._logger.info(f"shutting down {len(client_ids)} clients...")
        
        # Инициируем shutdown для каждого клиента параллельно
        shutdown_tasks = []
        for client_id in client_ids:
            if client := await self._clients.get(client_id):
                task = asyncio.create_task(client.shutdown())
                shutdown_tasks.append(task)
        
        # Ждем завершения shutdown всех клиентов
        if shutdown_tasks:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)

    async def _wait_all_clients_closed(self):
        """Ожидание закрытия всех клиентов"""
        while True:
            async with self._clients_lock:
                if not self._clients:
                    break
            
            self._logger.info(f"waiting for {len(self._clients)} clients to close...")
            await asyncio.sleep(0.5)

    async def broadcast(self, message: str, exclude: Optional[Set[str]] = None):
        """
        Отправка сообщения всем клиентам (кроме исключенных).
        
        Args:
            message: Сообщение для отправки
            exclude: Множество ID клиентов для исключения
        """
        exclude = exclude or set()
        
        async with self._clients_lock:
            clients_to_send = [
                client for client in self._clients
                if client.ctx.id not in exclude
            ]
        
        # Отправляем параллельно
        send_tasks = [
            asyncio.create_task(client.send(message))
            for client in clients_to_send
        ]
        
        if send_tasks:
            results = await asyncio.gather(*send_tasks, return_exceptions=True)
            
            # Логируем ошибки
            for client, result in zip(clients_to_send, results):
                if isinstance(result, Exception):
                    self._logger.error(
                        f"failed to send to client {client.ctx.id}: {result}"
                    )

    async def get_client(self, client_id: str) -> Optional[Client]:
        """Получение клиента по ID"""
        async with self._clients_lock:
            return await self._clients.get(client_id)

    async def disconnect_client(self, client_id: str, code: int = 1000, reason: str = ""):
        """
        Принудительное отключение конкретного клиента.
        
        Args:
            client_id: ID клиента для отключения
            code: WebSocket close код
            reason: Причина отключения
        """
        async with self._clients_lock:
            client = await self._clients.get(client_id)
        
        if client:
            self._logger.info(f"disconnecting client {client_id}: {reason}")
            await client.disconnect(code, reason)

    @property
    def client_count(self) -> int:
        """Количество активных клиентов"""
        return len(self._clients)

    @property
    def client_ids(self) -> Set[str]:
        """ID всех активных клиентов"""
        return self._clients.ids

    async def get_clients_info(self) -> Dict[str, dict]:
        """
        Получение информации обо всех клиентах.
        
        Returns:
            Словарь с информацией о клиентах
        """
        async with self._clients_lock:
            return {
                client.ctx.id: {
                    "id": client.ctx.id,
                    "remote_addr": client.ctx.remote_addr,
                    "connected_at": client.ctx.connected_at.isoformat(),
                    "last_message_at": client.ctx.last_message_at.isoformat(),
                    "is_active": client.ctx.is_active,
                }
                for client in self._clients
            }

    async def health_check(self) -> Dict[str, Any]:
        """
        Проверка здоровья менеджера.
        
        Returns:
            Статус менеджера и его клиентов
        """
        return {
            "is_running": self._is_running,
            "shutdown_initiated": self._shutdown_initiated,
            "client_count": self.client_count,
            "client_ids": list(self.client_ids),
        }