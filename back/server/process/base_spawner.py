from abc import ABC, abstractmethod
import typing

class DaemonProcess:
    def __init__(self, pid: int | None, tty: str | None, extra: dict | None = None):
        self.pid = pid
        self.tty = tty
        self.extra = extra or {}

    def __repr__(self):
        return f"<DaemonProcess pid={self.pid} tty={self.tty} extra={self.extra}>"
    


class BaseSpawner(ABC):
    """Базовый класс для запуска процессов"""
    @abstractmethod
    def spawn(self, cmd: typing.List[str]) -> DaemonProcess:
        """
        Запустить процесс как автономный демон.
        Возвращает DaemonProcess (pid + tty или идентификатор).
        """
        pass
