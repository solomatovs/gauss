import logging
import asyncio
import socket
import time

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings
from asyncio.locks import Lock
from typing import Dict

from server.helper.socket import SocketHelper, SocketOpts
from server.helper.os import OsHelper

# настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BalancerConfig(BaseSettings):
    port: int = Field(8000, description="Порт, на котором слушает балансировщик")
    host: str = Field("0.0.0.0", description="Адрес для биндинга сокета")
    sock: SocketOpts = Field(default_factory=SocketOpts, description="Параметры сокета")
    backlog: int = Field(512, description="Размер очереди listen()")

    @field_validator("port")
    def validate_ports(cls, port):
        if not port:
            raise ValueError("Список портов не может быть пустым")

        if not isinstance(port, int):
            raise ValueError("Порт должен быть int")

        if not (1 <= port <= 65535):
            raise ValueError(f"Некорректный порт: {port}")

        return port


class BalancerContext(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    worker_id: str = Field(
        default_factory=lambda: OsHelper.generate_identifier(),
        description="Уникальный идентификатор воркера",
    )
    connections: Dict[int, Dict] = Field(default={})
    is_shutting_down: bool = Field(
        default=False,
        description="Флаг процесса завершения работы",
    )
    lock: Lock = Field(default=asyncio.Lock())
    sock: socket.socket = Field(..., description="socket")

    def register_connection(self, client_sock: socket.socket, addr, backend: int):
        self.connections[client_sock.fileno()] = {
            "addr": addr,
            "backend": backend,
            "started": time.time(),
        }

    def unregister_connection(self, client_sock: socket.socket, addr):
        self.connections.pop(client_sock.fileno(), None)
        client_sock.close()
        logger.info(f"unregister connection from {addr}")


class LoadBalancer:
    def __init__(self, config: BalancerConfig):
        self.config = config
        self.cx = self._cx_bootstrap(self.config)

        # Настройка обработчиков сигналов
        OsHelper.setup_signals(self._signal_handler)

        logger.info(
            f"L4 LoadBalancer {self.cx.worker_id} initialized (PID: {OsHelper.getpid()})"
        )

    def _signal_handler(self, signum: int, frame) -> None:
        """Обработчик системных сигналов"""
        logger.info(f"Worker {self.cx.worker_id}: Received signal {signum}")
        self.cx.is_shutting_down = True

    @staticmethod
    def _create_sock(config: BalancerConfig):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        SocketHelper.setsockopt(sock, opts=config.sock)
        sock.bind((config.host, config.port))
        sock.listen(config.backlog)
        sock.setblocking(False)

        return sock

    @staticmethod
    def _cx_bootstrap(config: BalancerConfig):
        return BalancerContext(
            sock=LoadBalancer._create_sock(config),
        )

    async def start_serving(self) -> None:
        """Главный цикл accept + передача соединения"""
        try:
            # Создаем event loop если его нет
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        self._serve_task = loop.create_task(
            self._serve_loop(),
        )

        self._serving = True

    async def wait_closed(self) -> None:
        """Ожидание завершения работы"""
        if self._serve_task:
            await self._serve_task
            logger.info(f"LoadBalancer {self.cx.worker_id}: Shutting down")
        else:
            logger.warning(f"LoadBalancer {self.cx.worker_id}: No server to wait for")

    async def shutdown(self) -> None:
        """Корректное завершение работы сервера"""
        logger.info("Shutting down load balancer...")
        self._serving = False
        if self._serve_task:
            self._serve_task.cancel()

            try:
                await self._serve_task
            except asyncio.CancelledError:
                pass

        self.cx.sock.close()

    async def _serve_loop(self):
        """Асинхронный accept"""
        loop = asyncio.get_running_loop()
        sock = self.cx.sock

        while self._serving:
            try:
                client_sock, addr = await loop.sock_accept(sock)
            except Exception as e:
                logger.error(f"Accept error: {e}")
            else:
                # регистрируем соединение
                self.cx.register_connection(client_sock, addr, backend=-1)

                # передаем дескриптор воркеру
                await self._dispatch_connection(client_sock, addr)

                # удаляем соединение
                self.cx.unregister_connection(client_sock, addr)

    async def _dispatch_connection(self, client_sock: socket.socket, addr):
        """
        Передача сокета воркеру (через SCM_RIGHTS или через IPC).
        Здесь пока просто закрываем, чтобы не висело.
        """
        logger.info(f"Dispatching connection from {addr}")

    @property
    def host(self):
        return self.config.host

    @property
    def port(self):
        return self.cx.sock.getsockname()[1]
