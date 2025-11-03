import os
import pty
import subprocess
import logging
import typing

from server.process.base_spawner import BaseSpawner, DaemonProcess

# настройка логирования
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class LocalSpawner(BaseSpawner):
    """Локальный spawner для запуска процессов"""

    def spawn(self, cmd: typing.List[str]) -> DaemonProcess:
        """Запустить процесс локально как демон"""
        try:
            # Создаем PTY для процесса
            master_fd, slave_fd = pty.openpty()
            slave_name = os.ttyname(slave_fd)

            # Запускаем процесс как демон
            proc = subprocess.Popen(
                cmd,
                start_new_session=True,  # Создаем новую сессию
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                close_fds=True,
            )

            # Закрываем file descriptors в родительском процессе
            os.close(master_fd)
            os.close(slave_fd)

            logger.info(f"Spawned process PID {proc.pid} with TTY {slave_name}")
            return DaemonProcess(proc.pid, slave_name)

        except Exception as e:
            logger.error(f"Failed to spawn process: {e}")
            raise RuntimeError(f"Failed to spawn process: {e}")


# class DetachedProcess:
#     def __init__(self, pid, master_fd, slave_name):
#         self.pid = pid
#         self.master_fd = master_fd
#         self.slave_name = slave_name

#     def read(self, size=1024):
#         """Читать вывод процесса"""
#         return os.read(self.master_fd, size).decode(errors="ignore")

#     def write(self, data: str):
#         """Отправить данные в stdin процесса"""
#         if not data.endswith("\n"):
#             data += "\n"
#         os.write(self.master_fd, data.encode())

#     def fileno(self):
#         """Для select/poll"""
#         return self.master_fd

#     def __repr__(self):
#         return f"<DetachedProcess pid={self.pid} tty={self.slave_name}>"


# def spawn_detached_with_pty(cmd):
#     """
#     Запускает процесс в новом сеансе с собственным PTY.
#     Возвращает DetachedProcess.
#     """
#     master_fd, slave_fd = pty.openpty()
#     slave_name = os.ttyname(slave_fd)

#     proc = subprocess.Popen(
#         cmd,
#         start_new_session=True,   # делает процесс лидером сеанса
#         stdin=slave_fd,
#         stdout=slave_fd,
#         stderr=slave_fd,
#         close_fds=True,
#     )

#     os.close(slave_fd)  # в родителе slave не нужен

#     return DetachedProcess(proc.pid, master_fd, slave_name)
