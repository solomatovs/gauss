import asyncio
import socket
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from gauss.core.domain.plugin import load_storage
from gauss.core.helper.logging import LoggingHelper
from gauss.core.helper.os import OsHelper
from gauss.core.helper.socket import SocketHelper
from gauss.server.fast_api import (
    FastAPI,
    FastApiConfig,
    get_fast_api,
)
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class ConnectionRequest(BaseModel):
    """Модель запроса на создание подключения"""

    client_info: str = Field(default="web_client", max_length=100)

    class Config:
        json_schema_extra = {"example": {"client_info": "React WebSocket Client v1.0"}}


class ConnectionResponse(BaseModel):
    """Модель ответа с данными для подключения"""

    worker_id: str = Field(..., description="Unique worker identifier")
    addr: str = Field(..., description="Server host")
    port: int = Field(..., description="Server port", ge=1, le=65535)
    websocket_url: str = Field(..., description="WebSocket connection URL")
    created_at: str = Field(..., description="Creation timestamp")

    class Config:
        json_schema_extra = {
            "example": {
                "worker_id": "12345678-1234-1234-1234-123456789012",
                "host": "localhost",
                "port": 8001,
                "websocket_url": "ws://localhost:8001",
                "created_at": "2024-01-01T12:00:00Z",
            }
        }


class WorkerStats(BaseModel):
    """Статистика воркера"""

    worker_id: str = Field(..., description="Worker identifier")
    pid: int = Field(..., description="Process ID")
    port: int = Field(..., description="Server port")
    uptime_seconds: float = Field(..., description="Uptime in seconds")
    start_time: str = Field(..., description="Start time ISO format")
    requests_count: int = Field(..., description="Total requests processed")
    memory_usage_mb: float | None = Field(None, description="Memory usage in MB")


class HealthResponse(BaseModel):
    """Ответ проверки здоровья"""

    status: str = Field(..., description="Health status")
    timestamp: float = Field(..., description="Current timestamp")
    worker_id: str = Field(..., description="Worker ID")
    uptime_seconds: float = Field(..., description="Uptime in seconds")


class WorkerRootResponse(BaseModel):
    """Ответ корневого эндпоинта воркера"""

    message: str = Field(..., description="Welcome message")
    status: str = Field(..., description="Worker status")
    worker_id: str = Field(..., description="Worker identifier")
    pid: int = Field(..., description="Process ID")
    port: int = Field(..., description="Server port")
    uptime_seconds: float = Field(..., description="Uptime in seconds")
    requests_count: int = Field(..., description="Total requests")
    start_time: str = Field(..., description="Start time ISO format")


class ValidationErrorResponse(BaseModel):
    """Ответ на ошибку валидации"""

    detail: str = Field(..., description="Error description")
    errors: list[Any] = Field(..., description="Validation errors")
    worker_id: str = Field(..., description="Worker identifier")


class HttpErrorResponse(BaseModel):
    """Ответ на HTTP ошибку"""

    detail: str = Field(..., description="Error description")
    worker_id: str = Field(..., description="Worker identifier")
    timestamp: str = Field(..., description="timestamp")


class ShutdownResponse(BaseModel):
    """Ответ при завершении работы сервера"""

    detail: str = Field(default="Server is shutting down")


class UvicornConfig(BaseModel):
    addr: str = Field(default="0.0.0.0", description="ip address to binding")
    port: int = Field(default=8000, description="port to binding")
    log_level: str = Field(default="info", description="log_level")
    access_log: bool = Field(default=True, description="access_log")
    server_header: bool = Field(default=False, description="server_header")
    date_header: bool = Field(default=True, description="date_header")
    timeout_keep_alive: int = Field(default=5, description="timeout_keep_alive")
    timeout_notify: int = Field(default=30, description="timeout_notify")
    limit_max_requests: int = Field(default=10000, description="limit_max_requests")
    limit_concurrency: int = Field(default=1000, description="limit_concurrency")


class HttpConfig(BaseSettings):
    fastapi: FastApiConfig = Field(
        default_factory=FastApiConfig, description="fast api"
    )
    uvicorn: UvicornConfig = Field(default_factory=UvicornConfig, description="uvicorn")
    log_level: str = Field(default="info", description="log level")

    def to_uncorn_config(self, app):
        return uvicorn.Config(
            app=app,
            host=self.uvicorn.addr,
            port=self.uvicorn.port,
            log_level=self.uvicorn.log_level,
            access_log=self.uvicorn.access_log,
            server_header=self.uvicorn.server_header,
            date_header=self.uvicorn.server_header,
            timeout_keep_alive=self.uvicorn.timeout_keep_alive,
            timeout_notify=self.uvicorn.timeout_notify,
            limit_max_requests=self.uvicorn.limit_max_requests,
            limit_concurrency=self.uvicorn.limit_concurrency,
        )


class HttpWorker:
    """HTTP Worker с синхронным созданием в конструкторе"""

    def __init__(self, config: HttpConfig):
        self.config = config
        self._serve_task: asyncio.Task | None = None
        self._logger = LoggingHelper.getLogger("http")

        self.server, self._socket = self._create_server_sync(self.config, self._logger)

        # подписываемся на сигналы ос
        OsHelper.setup_shutdown_signal(self.shutdown)

        self._logger.info("initialized")

    async def start_serving(self):
        if self._serve_task is not None:
            self._logger.warning("already running")
            return

        try:
            # Создаем event loop если его нет
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        self._serve_task = loop.create_task(self.server.serve(sockets=[self._socket]))

    async def wait_closed(self):
        if self._serve_task:
            await self._serve_task
        else:
            self._logger.warning("no server to wait for")

    @staticmethod
    def _create_server_sync(
        config: HttpConfig, logger
    ) -> tuple[uvicorn.Server, socket.socket]:
        """Синхронное создание WebSocket сервера"""

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # === Инициализация зависимостей ===

            # Загружаем storage-плагин
            # например, имя можно взять из конфига FastAPI или переменной окружения
            import os

            storage_name = os.getenv("STORAGE_PLUGIN", "memory")

            storage = load_storage(storage_name)

            # Сохраняем в app.state
            app.state.storage = storage

            yield

            # === Завершение работы ===
            await storage.close()

        # Пробуем создать сервер на каждом порту в диапазоне
        try:

            # Создаем FastAPI приложение на указанном порту
            app = get_fast_api(config.fastapi, lifespan)

            """Запустить созданный сервер"""
            uvconfig = config.to_uncorn_config(app)
            reusesocket = SocketHelper.make_socket_with_reuse_port(
                config.uvicorn.addr, config.uvicorn.port
            )
            uvconfig.fd = reusesocket.fileno()

            socket = uvconfig.bind_socket()

            server = uvicorn.Server(uvconfig)

            return server, socket

        except OSError as _:
            logger.exception("failed to create server on port %s", config.uvicorn.port)
            raise
        except Exception as _:
            logger.exception(
                "unexpected error creating server on port %s", config.uvicorn.port
            )
            raise

    async def shutdown(self):
        """Асинхронное корректное завершение работы сервера"""
        self.server.should_exit = True

        if self._serve_task:
            try:
                # корректное завершение uvicorn
                await self.server.shutdown()
            except Exception as e:
                self._logger.warning(f"error during server shutdown: {e}")

            try:
                self._socket.close()
            except Exception as e:
                self._logger.warning(f"error during socket close: {e}")

    # def get_connection_info(self) -> ConnectionResponse:
    #     """Получить информацию о подключении"""
    #     return ConnectionResponse(
    #         worker_id=self.cx.worker_id,
    #         port=self.config.uvicorn.port,
    #         addr=self.config.uvicorn.addr,
    #         websocket_url=f"http://{self.config.uvicorn.addr}:{self.config.uvicorn.port}",
    #         created_at=self.cx.start_time.isoformat(),
    #     )

    # def get_worker_info(self) -> WorkerRootResponse:
    #     """Получить информацию о воркере"""
    #     uptime = (datetime.now() - self.cx.start_time).total_seconds()
    #     return WorkerRootResponse(
    #         message=f"HTTP Worker {self.cx.worker_id}",
    #         status="created",
    #         worker_id=self.cx.worker_id,
    #         pid=OsHelper.getpid(),
    #         port=self.config.uvicorn.port,
    #         uptime_seconds=uptime,
    #         requests_count=self.cx.requests_count,
    #         start_time=self.cx.start_time.isoformat()
    #     )
