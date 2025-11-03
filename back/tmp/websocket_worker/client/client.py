import json
import asyncio
from datetime import datetime
from typing import Optional, Any, Dict
from enum import Enum

from pydantic import BaseModel, Field
from websockets.asyncio.server import ServerConnection
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK,
)

from server.helper.os import OsHelper
from server.helper.socket import SocketHelper
from server.helper.logging import LoggingHelper
from server.helper.asyncio import AsyncioHelper


class ClientState(Enum):
    """Состояния жизненного цикла клиента"""

    CREATED = "created"
    RUNNING = "running"
    SHUTTING_DOWN = "shutting_down"
    CLOSED = "closed"


class ClientContext(BaseModel):
    """Контекст отдельного клиента"""

    model_config = {"arbitrary_types_allowed": True}

    id: str = Field(..., description="Уникальный ID клиента")
    remote_addr: str = Field(..., description="Адрес клиента")
    connected_at: datetime = Field(
        default_factory=datetime.now, description="Время подключения"
    )
    last_message_at: datetime = Field(
        default_factory=datetime.now, description="Время последнего сообщения"
    )
    # is_active: bool = Field(default=True, description="Флаг активности клиента")
    connection_count: int = Field(
        default=0, description="Счетчик для демонстрации работы", ge=0
    )
    start_time: datetime = Field(
        default_factory=datetime.now, description="Время запуска воркера"
    )

    def uptime_seconds(self) -> float:
        """Время работы воркера в секундах"""
        return (datetime.now() - self.start_time).total_seconds()

    def increment_connection_count(self, count: int = 1) -> None:
        """Увеличить счетчик подключений"""
        if count < 0:
            raise ValueError("increment count must be >= 0")
        self.connection_count += count

    def touch(self) -> None:
        """Обновить время последней активности"""
        self.last_message_at = datetime.now()


class ClientMessage(BaseModel):
    """Сообщение от клиента к воркеру с командой для выполнения"""

    action: str = Field(..., description="Действие для выполнения", min_length=1)

    class Config:
        extra = "allow"  # Разрешить дополнительные поля


class StateUpdateMessage(BaseModel):
    """Сообщение с обновлением состояния счетчика"""

    type: str = Field(default="state_update", description="Тип сообщения")
    count: int = Field(..., description="Текущее значение счетчика", ge=0)
    client_id: str = Field(..., description="Идентификатор воркера")
    timestamp: str = Field(..., description="Время обновления в ISO формате")


class ClientInfoMessage(BaseModel):
    """Информационное сообщение о состоянии воркера"""

    type: str = Field(default="worker_info", description="Тип сообщения")
    client_id: str = Field(..., description="Идентификатор воркера")
    port: int = Field(..., description="Порт сервера", ge=1, le=65535)
    pid: int = Field(..., description="Идентификатор процесса", gt=0)
    uptime_seconds: float = Field(..., description="Время работы в секундах", ge=0)
    count: int = Field(..., description="Текущее значение счетчика", ge=0)


class ErrorMessage(BaseModel):
    """Сообщение об ошибке"""

    type: str = Field(default="error", description="Тип сообщения")
    message: str = Field(..., description="Текст ошибки", min_length=1)
    client_id: str = Field(..., description="Идентификатор воркера")


class Client:
    """
    Клиент WebSocket соединения.

    Ответственность:
    - Управление одним WebSocket соединением
    - Обработка входящих сообщений
    - Отправка исходящих сообщений
    - Graceful shutdown при отключении
    """

    def __init__(self, websocket: ServerConnection):
        self._websocket = websocket
        self.ctx = self._context_create(websocket)

        # Очередь для исходящих сообщений (если нужна буферизация)
        self._send_queue = asyncio.Queue()

        # Логирование
        self._logger = LoggingHelper.getLogger(f"client: {self.ctx.id}")

    @staticmethod
    def _context_create(websocket: ServerConnection) -> ClientContext:
        """Инициализация контекста клиента"""
        return ClientContext(
            id=OsHelper.generate_identifier(),
            remote_addr=SocketHelper.websocket_remote_host(websocket),
        )

    async def __aenter__(self):
        """Инициализация менеджера при входе в контекст"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Graceful shutdown при выходе из контекста"""
        await self._cleanup()
        # Не подавляем исключения
        return False

    async def _cleanup(self):
        """Очистка ресурсов клиента"""
        self._logger.info("cleaning up...")

        # Закрываем соединение если еще не закрыто
        if self._socket_still_alive():
            await self._close_connection(1000, "cleanup")

        # Очищаем очередь
        while not self._send_queue.empty():
            try:
                self._send_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

        self._logger.info("cleanup")

    def _socket_still_alive(self):
        # CONNECTING или OPEN
        return self._websocket.state.value <= 1

    async def _close_connection(self, code: int, reason: str):
        """Закрытие WebSocket соединения"""
        try:
            await self._websocket.close(code=code, reason=reason)
        except Exception as _:
            self._logger.exception("error closing connection")

    async def run(self, shutdown_event: asyncio.Event):
        """
        Основной метод запуска клиента.
        Управляет всеми параллельными задачами через TaskGroup.
        """
        try:
            # Отправляем начальное состояние
            await self._send_initial_state()

            # Запускаем параллельные задачи через TaskGroup
            async with AsyncioHelper.cancellable_task_group(
                shutdown_event, asyncio.FIRST_COMPLETED
            ) as tg:
                tg.create_task(self._receive_messages())
                tg.create_task(self._send_message())

        except* asyncio.CancelledError:
            self._logger.debug("receive task cancelled")
        except* ConnectionClosedOK as eg:
            # когда соединение закрыто нормальным способом (code=1000)
            for exc in eg.exceptions:
                self._logger.info(f"WebSocket connection closed ok: {exc}")
        except* ConnectionClosedError as eg:
            # когда соединение закрывается не по протоколу WebSocket
            # то есть без корректного “close frame” (специального пакета, уведомляющего о завершении соединения).
            for exc in eg.exceptions:
                self._logger.warning(f"WebSocket connection closed unexpectedly: {exc}")
        except* ConnectionClosed as eg:
            # Базовый exception закрытия соединения. если перехвачен err, ok варианты, то сюда не попадет
            #  └── websockets.exceptions.WebSocketException
            #       └── websockets.exceptions.ConnectionClosed
            #            ├── ConnectionClosedOK
            #            └── ConnectionClosedError
            for exc in eg.exceptions:
                self._logger.warning(f"WebSocket connection closed unexpectedly: {exc}")
        except* Exception as eg:
            for exc in eg.exceptions:
                self._logger.exception("error in task", exc_info=exc)
            raise
        finally:
            pass

    async def _receive_messages(self):
        """Прием и обработка входящих сообщений"""
        async for message in self._websocket:
            await self._handle_message(message)

    async def _send_message(self):
        """
        Обработка очереди исходящих сообщений.
        Позволяет буферизировать сообщения и отправлять их асинхронно.
        """
        while True:
            # Ждем сообщение из очереди
            message = await self._send_queue.get()
            await self._websocket.send(message)

    async def _handle_message(self, message: str | bytes):
        """Обработка одного входящего сообщения"""
        try:
            # Парсинг JSON
            data = json.loads(message)
            client_message = ClientMessage(**data)

            self._logger.debug(f"received action: {client_message.action}")

            # Обработка действий
            match client_message.action:
                case "increment":
                    await self._handle_increment()
                case "get_info":
                    await self._handle_get_info()
                case "reset":
                    await self._handle_reset()
                case "ping":
                    await self._handle_ping()
                case _:
                    await self._send_error(f"unknown action: {client_message.action}")

        except json.JSONDecodeError as e:
            self._logger.error(f"invalid json: {e}")
            await self._send_error("invalid json format")
        except Exception as e:
            self._logger.error(f"error handling message: {e}")
            await self._send_error(f"Error processing message: {str(e)}")
        finally:
            pass

    async def _handle_increment(self):
        """Обработка команды increment"""
        self.ctx.increment_connection_count()
        self._logger.info(f"count incremented to {self.ctx.connection_count}")
        await self._send_state_update()

    async def _handle_get_info(self):
        """Обработка команды get_info"""
        await self._send_info()

    async def _handle_reset(self):
        """Обработка команды reset"""
        self.ctx.connection_count = 0
        self._logger.info("count reset")
        await self._send_state_update()

    async def _handle_ping(self):
        """Обработка команды ping"""
        await self.send_json({"type": "pong", "client_id": self.ctx.id})

    async def _send_initial_state(self):
        """Отправка начального состояния клиенту"""
        await self._send_state_update()

    async def _send_state_update(self):
        """Отправка обновления состояния"""
        message = StateUpdateMessage(
            count=self.ctx.connection_count,
            client_id=self.ctx.id,
            timestamp=datetime.now().isoformat(),
        )
        await self.send_json(message.model_dump())

    async def _send_info(self):
        """Отправка информации о клиенте"""
        message = ClientInfoMessage(
            client_id=self.ctx.id,
            port=9000,  # TODO: получать из конфигурации
            pid=OsHelper.getpid(),
            uptime_seconds=self.ctx.uptime_seconds(),
            count=self.ctx.connection_count,
        )
        await self.send_json(message.model_dump())

    async def _send_error(self, error_text: str):
        """Отправка сообщения об ошибке"""
        message = ErrorMessage(message=error_text, client_id=self.ctx.id)
        await self.send_json(message.model_dump())

    async def send(self, message: str | bytes):
        """
        Отправка сообщения клиенту (публичный метод).
        Использует очередь для буферизации.
        """
        await self._send_queue.put(message)

    async def send_json(self, data: Dict[str, Any]):
        """Отправка JSON сообщения"""
        await self.send(json.dumps(data))

    @property
    def is_active(self) -> bool:
        """Проверка активности клиента"""
        return not self._socket_still_alive()
