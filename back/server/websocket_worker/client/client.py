import json
import asyncio
from datetime import datetime
from typing import Optional, Any, Dict
from enum import Enum

from pydantic import BaseModel, Field
from websockets.asyncio.server import ServerConnection
from websockets.exceptions import ConnectionClosed

from server.helper.os import OsHelper
from server.helper.socket import SocketHelper
from server.helper.logging import LoggingHelper


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
    connected_at: datetime = Field(default_factory=datetime.now, description="Время подключения")
    last_message_at: datetime = Field(default_factory=datetime.now, description="Время последнего сообщения")
    is_active: bool = Field(default=True, description="Флаг активности клиента")
    connection_count: int = Field(default=0, description="Счетчик для демонстрации работы", ge=0)
    start_time: datetime = Field(default_factory=datetime.now, description="Время запуска воркера")

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
        self.ctx = self._init_context(websocket)
        
        # Управление состоянием
        self._state = ClientState.CREATED
        self._shutdown_event = asyncio.Event()
        self._stop_event = asyncio.Event()
        
        # TaskGroup для параллельных задач клиента
        self._task_group: Optional[asyncio.TaskGroup] = None
        
        # Очередь для исходящих сообщений (если нужна буферизация)
        self._send_queue: asyncio.Queue = asyncio.Queue()
        
        # Логирование
        self._logger = LoggingHelper.getLogger(f"client: {self.ctx.id}")

    @staticmethod
    def _init_context(websocket: ServerConnection) -> ClientContext:
        """Инициализация контекста клиента"""
        client_id = OsHelper.generate_identifier()
        return ClientContext(
            id=client_id,
            remote_addr=SocketHelper.websocket_remote_host(websocket),
        )

    async def run(self):
        """
        Основной метод запуска клиента.
        Управляет всеми параллельными задачами через TaskGroup.
        """
        if self._state != ClientState.CREATED:
            raise RuntimeError(f"cannot run client in state {self._state}")
        
        self._state = ClientState.RUNNING
        self._logger.info("started")
        
        try:
            # Отправляем начальное состояние
            await self._send_initial_state()
            
            # Запускаем параллельные задачи через TaskGroup
            async with asyncio.TaskGroup() as tg:
                self._task_group = tg
                
                # Основные задачи клиента
                tg.create_task(self._receive_messages())
                tg.create_task(self._process_send_queue())
                tg.create_task(self._monitor_shutdown())
                
                # Дополнительные задачи можно добавить здесь
                # tg.create_task(self._heartbeat())
                # tg.create_task(self._monitor_activity())
                
        except* asyncio.CancelledError:
            self._logger.info("tasks cancelled")
        except* ConnectionClosed as eg:
            for exc in eg.exceptions:
                self._logger.info(f"connection closed: {exc}")
        except* Exception as eg:
            for exc in eg.exceptions:
                self._logger.exception("error in task", exc_info=exc)
            raise
        finally:
            self._state = ClientState.CLOSED
            self._task_group = None
            await self._cleanup()

    async def _receive_messages(self):
        """Прием и обработка входящих сообщений"""
        try:
            async for message in self._websocket:
                self.ctx.touch()  # Обновляем время последней активности
                await self._handle_message(message)
                
        except asyncio.CancelledError:
            self._logger.debug("receive task cancelled")
            raise
        except ConnectionClosed as e:
            self._logger.info(f"connection closed during receive: {e.code} {e.reason}")
            raise
        except Exception as e:
            self._logger.exception("error in receive loop")
            await self._send_error(f"internal error: {str(e)}")
            raise

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

    async def _process_send_queue(self):
        """
        Обработка очереди исходящих сообщений.
        Позволяет буферизировать сообщения и отправлять их асинхронно.
        """
        try:
            while True:
                # Ждем сообщение из очереди
                message = await self._send_queue.get()
                
                try:
                    await self._websocket.send(message)
                except ConnectionClosed:
                    self._logger.warning("cannot send: connection closed")
                    break
                except Exception as e:
                    self._logger.error(f"error sending message: {e}")
        
        except asyncio.CancelledError:
            # Отправляем оставшиеся сообщения перед завершением
            while not self._send_queue.empty():
                try:
                    message = self._send_queue.get_nowait()
                    await self._websocket.send(message)
                except Exception:
                    break
            raise

    async def _monitor_shutdown(self):
        """Мониторинг события shutdown"""
        await self._shutdown_event.wait()
        self._logger.info("shutdown event received")
        
        # Отправляем сообщение о закрытии
        await self.send_json({
            "type": "closing",
            "client_id": self.ctx.id,
            "reason": "server shutdown"
        })
        
        # Закрываем соединение
        await self._close_connection(1001, "server shutdown")
        
        # Останавливаем другие задачи
        self._stop_event.set()

    async def _send_initial_state(self):
        """Отправка начального состояния клиенту"""
        await self._send_state_update()

    async def _send_state_update(self):
        """Отправка обновления состояния"""
        message = StateUpdateMessage(
            count=self.ctx.connection_count,
            client_id=self.ctx.id,
            timestamp=datetime.now().isoformat()
        )
        await self.send_json(message.model_dump())

    async def _send_info(self):
        """Отправка информации о клиенте"""
        message = ClientInfoMessage(
            client_id=self.ctx.id,
            port=9000,  # TODO: получать из конфигурации
            pid=OsHelper.getpid(),
            uptime_seconds=self.ctx.uptime_seconds(),
            count=self.ctx.connection_count
        )
        await self.send_json(message.model_dump())

    async def _send_error(self, error_text: str):
        """Отправка сообщения об ошибке"""
        message = ErrorMessage(
            message=error_text,
            client_id=self.ctx.id
        )
        await self.send_json(message.model_dump())

    async def send(self, message: str | bytes):
        """
        Отправка сообщения клиенту (публичный метод).
        Использует очередь для буферизации.
        """
        if self._state == ClientState.CLOSED:
            raise RuntimeError("Cannot send to closed client")
        
        await self._send_queue.put(message)

    async def send_json(self, data: Dict[str, Any]):
        """Отправка JSON сообщения"""
        await self.send(json.dumps(data))

    async def shutdown(self):
        """
        Инициация graceful shutdown клиента.
        Можно вызывать из любого места.
        """
        if self._state != ClientState.RUNNING:
            return
        
        self._state = ClientState.SHUTTING_DOWN
        self._logger.info("initiating client shutdown")
        
        # Устанавливаем событие shutdown
        self._shutdown_event.set()

    async def disconnect(self, code: int = 1000, reason: str = ""):
        """
        Принудительное отключение клиента.
        
        Args:
            code: WebSocket close код
            reason: Причина отключения
        """
        self._logger.info(f"disconnecting: {reason}")
        await self._close_connection(code, reason)
        self._stop_event.set()

    async def _close_connection(self, code: int = 1000, reason: str = ""):
        """Закрытие WebSocket соединения"""
        try:
            await self._websocket.close(code=code, reason=reason)
        except Exception as e:
            self._logger.warning(f"error closing connection: {e}")

    async def _cleanup(self):
        """Очистка ресурсов клиента"""
        self._logger.info("cleaning up...")
        
        # Очищаем очередь
        while not self._send_queue.empty():
            try:
                self._send_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        
        # Закрываем соединение если еще не закрыто
        if self._websocket.state.value <= 1:  # CONNECTING или OPEN
            await self._close_connection(1000, "cleanup")
        
        self.ctx.is_active = False
        self._logger.info("cleanup")

    async def close(self):
        """Полное закрытие клиента (вызывается менеджером)"""
        await self.shutdown()
        await self._cleanup()

    def add_task(self, coro):
        """
        Добавление новой задачи в TaskGroup клиента.
        
        Args:
            coro: Корутина для выполнения
            
        Returns:
            asyncio.Task: Созданная задача
        """
        if self._state != ClientState.RUNNING or not self._task_group:
            raise RuntimeError("cannot add task: client is not running")
        
        return self._task_group.create_task(coro)

    @property
    def is_active(self) -> bool:
        """Проверка активности клиента"""
        return self.ctx.is_active and self._state == ClientState.RUNNING

    @property
    def state(self) -> ClientState:
        """Текущее состояние клиента"""
        return self._state
