import websockets
import asyncio
import signal
from pydantic import Field
from pydantic_settings import BaseSettings
from websockets.asyncio.server import ServerConnection
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

    def __init__(
        self, config: WebsocketConfig, server: websockets.Server, manager: ClientManager
    ):
        self._config = config
        self._manager = manager
        self._server = server
        self._shutdown_event = asyncio.Event()
        self._logger = LoggingHelper.getLogger(
            f"worker:{SocketHelper.sockets_addr(self._server.sockets)}"
        )

    @classmethod
    @asynccontextmanager
    async def create(cls, config: WebsocketConfig):
        """
        Фабрика для создания WebsocketWorker
        Гарантирует правильную инициализацию и очистку ресурсов.
        """
        # создаем менеджер клиентских подключений
        manager = ClientManager()

        async def handle_client(websocket: ServerConnection):
            """Обработчик новых клиентских подключений"""
            await manager.process(websocket)

        # создаем серверный слушающий socket согласно конфигурации
        server_socket = SocketHelper.make_socket_with_reuse_port(
            config.addr, config.port
        )
        # создаем websocket сервер и передаем ему socket
        server = await websockets.serve(
            handle_client,
            host=None,
            port=None,
            sock=server_socket,
            ping_interval=config.ping_interval,
            ping_timeout=config.ping_timeout,
            start_serving=False,  # Не начинаем сразу
        )

        worker = cls(config, server, manager)

        try:
            yield worker
        finally:
            await worker._cleanup()

    async def _cleanup(self):
        """Очистка всех ресурсов"""
        self._logger.info("cleaning up...")

        # Закрываем прием новых соединений
        self._server.close()

        # завершаем менеджер клиентов и соответственно все клиентские подключения
        self._manager.shutdown_signal()
        await self._manager.shutdown_wait()

        # ждем завершения сервера
        await self._server.wait_closed()

        self._logger.info("cleanup completed")

    async def run(self):
        """
        Этот метод блокирующий - завершается только при shutdown.
        """
        self._logger.info("starting...")

        # Настраиваем обработчики сигналов
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(
            signal.SIGINT, lambda: asyncio.create_task(self.shutdown_signal())
        )
        loop.add_signal_handler(
            signal.SIGTERM, lambda: asyncio.create_task(self.shutdown_signal())
        )

        try:
            await self._server.start_serving()
            self._logger.info("started")

            await self._shutdown_event.wait()
        except* Exception as eg:
            for exc in eg.exceptions:
                self._logger.exception("error in worker task", exc_info=exc)
            raise
        finally:
            # Удаляем обработчики сигналов
            loop.remove_signal_handler(signal.SIGINT)
            loop.remove_signal_handler(signal.SIGTERM)

    async def shutdown_signal(self):
        """
        Инициирует graceful shutdown воркера.
        """
        self._logger.info("shutdown...")
        self._shutdown_event.set()
