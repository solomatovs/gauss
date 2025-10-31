from abc import ABC, abstractmethod
from datetime import datetime
from pydantic import BaseModel, Field, field_validator, model_validator

from gauss.core.helper.os import OsHelper


class WorkerContext(ABC):
    """
    Абстрактный интерфейс для хранения контекста воркера.

    Не содержит реализации хранения данных - только контракт методов.
    """

    @property
    @abstractmethod
    def id(self) -> str:
        """Уникальный идентификатор воркера."""
        pass

    @property
    @abstractmethod
    def host(self) -> str:
        """Хост воркера."""
        pass

    @property
    @abstractmethod
    def port(self) -> int:
        """Порт воркера."""
        pass

    @property
    @abstractmethod
    def start_time(self) -> datetime:
        """Время запуска воркера."""
        pass

    @property
    @abstractmethod
    def requests_count(self) -> int:
        """Количество обработанных запросов."""
        pass

    @requests_count.setter
    @abstractmethod
    def requests_count(self, value: int) -> None:
        """Установить количество обработанных запросов."""
        pass

    @property
    @abstractmethod
    def last_heartbeat(self) -> datetime:
        """Время последнего heartbeat."""
        pass

    @abstractmethod
    def url(self) -> str:
        """Возвращает URL воркера."""
        pass

    @abstractmethod
    def uptime_seconds(self) -> int:
        """Возвращает аптайм воркера в секундах."""
        pass

    @abstractmethod
    def increment_requests(self, count: int = 1) -> None:
        """Увеличить счетчик обработанных запросов."""
        pass


class PydenticWorkerContext(BaseModel):
    """Внутренняя модель данных с Pydantic валидацией."""

    id: str = Field(
        default_factory=lambda: OsHelper.generate_identifier(),
        min_length=1,
        description="Уникальный идентификатор воркера",
    )
    host: str = Field(..., min_length=1, description="Хост воркера")
    port: int = Field(..., ge=1, le=65535, description="Порт воркера")
    start_time: datetime = Field(
        default_factory=datetime.now, description="Время запуска воркера"
    )
    requests_count: int = Field(
        default=0, ge=0, description="Количество обработанных запросов"
    )
    last_heartbeat: datetime = Field(
        default_factory=datetime.now, description="Время последнего heartbeat"
    )

    @field_validator("host")
    @classmethod
    def validate_host(cls, v: str) -> str:
        """Валидация хоста."""
        v = v.strip()
        if not v:
            raise ValueError("Host cannot be empty")
        return v

    @model_validator(mode="after")
    def validate_heartbeat(self):
        """Проверка, что last_heartbeat не раньше start_time."""
        if self.last_heartbeat < self.start_time:
            raise ValueError("last_heartbeat cannot be before start_time")
        return self
