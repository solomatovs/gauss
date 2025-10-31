import sys
import os
import signal
import uuid
import asyncio

from typing import Any, Callable


class OsHelper:
    @staticmethod
    def platform_short() -> str:
        if sys.platform.startswith("linux"):
            return "linux"
        elif sys.platform == "darwin":
            return "macos"
        elif sys.platform == "win32":
            return "windows"
        else:
            return "unknown"

    @staticmethod
    def generate_identifier():
        return str(uuid.uuid4())

    @staticmethod
    def validate_identifier(v: str):
        #  проверка что это валидный identifier
        uuid.UUID(v)

    @staticmethod
    def get_memory_usage_mb() -> float:
        """Возвращает использование памяти процессом (MB), без psutil"""
        if sys.platform.startswith("linux"):
            with open("/proc/self/statm") as f:
                pages = int(f.readline().split()[1])
                rss = pages * os.sysconf("SC_PAGE_SIZE")
                return rss / 1024 / 1024
        elif sys.platform == "darwin":
            import resource

            usage = resource.getrusage(resource.RUSAGE_SELF)
            return usage.ru_maxrss / 1024  # в KB
        elif sys.platform == "win32":
            import ctypes
            import ctypes.wintypes

            class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                _fields_ = [
                    ("cb", ctypes.wintypes.DWORD),
                    ("PageFaultCount", ctypes.wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.wintypes.SIZE_T),
                    ("WorkingSetSize", ctypes.wintypes.SIZE_T),
                    ("QuotaPeakPagedPoolUsage", ctypes.wintypes.SIZE_T),
                    ("QuotaPagedPoolUsage", ctypes.wintypes.SIZE_T),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.wintypes.SIZE_T),
                    ("QuotaNonPagedPoolUsage", ctypes.wintypes.SIZE_T),
                    ("PagefileUsage", ctypes.wintypes.SIZE_T),
                    ("PeakPagefileUsage", ctypes.wintypes.SIZE_T),
                ]

            counters = PROCESS_MEMORY_COUNTERS()
            ctypes.windll.psapi.GetProcessMemoryInfo(
                ctypes.windll.kernel32.GetCurrentProcess(),
                ctypes.byref(counters),
                ctypes.sizeof(counters),
            )
            return float(counters.WorkingSetSize / 1024 / 1024)

        return float("nan")

    @staticmethod
    def getpid():
        return os.getpid()

    @staticmethod
    def setup_signals(signal_handler: Callable[[Any, Any], None]):
        """Настройка обработчиков сигналов для graceful shutdown"""
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    @staticmethod
    def setup_shutdown_signal(shutdown):
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(shutdown()))
        loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(shutdown()))

    @staticmethod
    def executable():
        return sys.executable

    @staticmethod
    def exit(exit_code: int):
        sys.exit(exit_code)

    @staticmethod
    def fork():
        return os.fork()
