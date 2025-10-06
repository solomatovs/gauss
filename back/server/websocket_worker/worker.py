import websockets
import asyncio
import socket
import signal
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings
from websockets.asyncio.server import ServerConnection, Server
from contextlib import asynccontextmanager

from server.helper.socket import SocketHelper
from server.helper.logging import LoggingHelper
from server.websocket_worker.client.manager import ClientManager


class WebsocketConfig(BaseSettings):
    """Конфигурация WebSocket воркера (неизменяемые настройки)"""
    addr: str = Field(default="localhost", description="Адрес для привязки сервера")
    port: int = Field(default=9000, description="Порт для биндинга", ge=1, le=65535)
    ping_interval: int = Field(default=20, description="Интервал ping в секундах", gt=0)
    ping_timeout: int = Field(default=10, description="Таймаут ping в секундах", gt=0)
    log_level: str = Field(default="info", description="log level")


class WebSocketWorker:
    """
    WebSocket сервер с правильным управлением жизненным циклом.
    
    Ответственность:
    - Создание и управление WebSocket сервером
    - Координация между сервером и менеджером клиентов
    - Graceful shutdown при получении сигналов
    """
    
    def __init__(self, config: WebsocketConfig, socket_obj: socket.socket):
        self._config = config
        self._socket = socket_obj
        self._server: Optional[Server] = None
        self._manager: Optional[ClientManager] = None
        self._stop_event = asyncio.Event()
        self._task_group: Optional[asyncio.TaskGroup] = None
        self._logger = LoggingHelper.getLogger(f"worker:{SocketHelper.socket_addr(socket_obj)}")
        
        # Для отслеживания статуса
        self._is_running = False
        self._shutdown_initiated = False

    @classmethod
    @asynccontextmanager
    async def create(cls, config: WebsocketConfig):
        """
        Асинхронная фабрика с контекстным менеджером.
        Гарантирует правильную инициализацию и очистку ресурсов.
        """
        socket_obj = SocketHelper.make_socket_with_reuse_port(config.addr, config.port)
        worker = cls(config, socket_obj)
        
        try:
            await worker._initialize()
            yield worker
        finally:
            await worker._cleanup()

    async def _initialize(self):
        """Инициализация всех компонентов воркера"""
        self._logger.info("initializing worker...")
        
        # Создаем менеджер клиентов
        self._manager = ClientManager()
        
        # Создаем WebSocket сервер
        async def handle_client(websocket: ServerConnection):
            """Обработчик новых подключений"""
            if not self._manager:
                raise RuntimeError("manager is not running")
            
            await self._manager.process(websocket)
        
        self._server = await websockets.serve(
            handle_client,
            host=None,
            port=None,
            sock=self._socket,
            ping_interval=self._config.ping_interval,
            ping_timeout=self._config.ping_timeout,
            start_serving=False,  # Не начинаем сразу
        )
        
        self._logger.info("worker initialized successfully")

    async def _cleanup(self):
        """Очистка всех ресурсов"""
        self._logger.info("cleaning up worker resources...")
        
        if self._server:
            self._server.close(close_connections=True)
            await self._server.wait_closed()
            self._logger.info("server closed")
        
        self._logger.info("worker cleanup completed")

    async def start(self):
        """
        Запуск воркера с TaskGroup для управления параллельными задачами.
        Этот метод блокирующий - завершается только при shutdown.
        """
        if self._is_running:
            raise RuntimeError("Worker is already running")
        
        self._is_running = True
        self._logger.info("starting worker...")
        
        # Настраиваем обработчики сигналов
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(self.shutdown()))
        loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(self.shutdown()))
        
        try:
            # Запускаем менеджер клиентов и держим его активным
            if not self._manager:
                raise RuntimeError("ClientManager is not build")
            
            async with self._manager:
                # Используем TaskGroup для управления всеми задачами воркера
                async with asyncio.TaskGroup() as tg:
                    self._task_group = tg
                    
                    # Запускаем основные задачи воркера
                    tg.create_task(self._serve_forever())
                    tg.create_task(self._monitor_stop_event())
                    
                    pass
                    # Здесь можно добавлять дополнительные задачи
                    # tg.create_task(self._health_check())
                    # tg.create_task(self._metrics_collector())
                    
                    # TaskGroup будет ждать завершения всех задач
                    # Менеджер остается активным пока работают задачи
                    
        except* asyncio.CancelledError:
            self._logger.info("worker tasks cancelled")
        except* Exception as eg:
            for exc in eg.exceptions:
                self._logger.exception("error in worker task", exc_info=exc)
            raise
        finally:
            self._is_running = False
            self._task_group = None
            
            # Удаляем обработчики сигналов
            loop.remove_signal_handler(signal.SIGINT)
            loop.remove_signal_handler(signal.SIGTERM)

    async def _serve_forever(self):
        """Основная задача - обслуживание WebSocket соединений"""
        try:
            if self._server:
                self._logger.info("starting to serve connections...")
                await self._server.serve_forever()
        except asyncio.CancelledError:
            self._logger.info("serve task cancelled")
            raise
        finally:
            self._logger.info("stopped serving connections")

    async def _monitor_stop_event(self):
        """Мониторинг события остановки для graceful shutdown"""
        await self._stop_event.wait()
        self._logger.info("stop event received, initiating shutdown...")
        
        # Отменяем все задачи в группе
        if self._task_group:
            # TaskGroup сам отменит все задачи при выходе из контекста
            pass

    async def shutdown(self):
        """
        Инициирует graceful shutdown воркера.
        Можно вызывать из любого места.
        """
        if self._shutdown_initiated:
            return
        
        self._shutdown_initiated = True
        self._logger.info("shutdown initiated")
        
        # Устанавливаем событие остановки
        self._stop_event.set()
        
        # Закрываем сервер для новых подключений
        if self._server:
            self._server.close()
        
        # Инициируем shutdown менеджера клиентов
        if self._manager:
            await self._manager.shutdown()

    # async def wait_stop(self):
    #     """
    #     Ожидание завершения работы воркера.
    #     Используется для совместимости со старым API.
    #     """
    #     await self.start()

    def add_task(self, coro):
        """
        Добавление новой задачи в TaskGroup воркера.
        Можно вызывать только когда воркер запущен.
        """
        if not self._is_running or not self._task_group:
            raise RuntimeError("Cannot add task: worker is not running")
        
        return self._task_group.create_task(coro)

    @property
    def is_running(self) -> bool:
        """Проверка статуса воркера"""
        return self._is_running

    @property
    def client_count(self) -> int:
        """Количество подключенных клиентов"""
        return self._manager.client_count if self._manager else 0