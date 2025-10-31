import socket
import struct
import array
from typing import List, Tuple, Optional, Iterable
from pydantic import BaseModel, Field, field_validator


class SoOpt(BaseModel):
    # --- COMMON ---
    # Можно повторно биндиться на тот же адрес/порт после закрытия сокета
    SO_REUSEADDR: Optional[int] = Field(
        default=None,
        alias="reuseaddr",
        description="Allow reuse of local addresses after socket close",
        json_schema_extra={"level": socket.SOL_SOCKET, "opt": socket.SO_REUSEADDR},
    )
    # Несколько процессов/потоков могут слушать один и тот же порт (Linux-specific)
    SO_REUSEPORT: Optional[int] = Field(
        default=None,
        alias="reuseport",
        description="Allow multiple sockets to bind to the same port",
        json_schema_extra={"level": socket.SOL_SOCKET, "opt": socket.SO_REUSEPORT},
    )
    # Включить TCP keep-alive для проверки активности соединения
    SO_KEEPALIVE: Optional[int] = Field(
        default=None,
        alias="keepalive",
        description="Enable sending of TCP keepalive probes",
        json_schema_extra={"level": socket.SOL_SOCKET, "opt": socket.SO_KEEPALIVE},
    )
    # Управление поведением close(): (onoff, linger_time)
    SO_LINGER: Optional[tuple[int, int]] = Field(
        default=None,
        alias="linger",
        description="Control socket close behavior (on/off, linger time)",
        json_schema_extra={"level": socket.SOL_SOCKET, "opt": socket.SO_LINGER},
    )
    # Размер буфера приёма
    SO_RCVBUF: Optional[int] = Field(
        default=None,
        alias="rcvbuf",
        description="Size of the receive buffer (bytes)",
        json_schema_extra={"level": socket.SOL_SOCKET, "opt": socket.SO_RCVBUF},
    )
    # Размер буфера отправки
    SO_SNDBUF: Optional[int] = Field(
        default=None,
        alias="sndbuf",
        description="Size of the send buffer (bytes)",
        json_schema_extra={"level": socket.SOL_SOCKET, "opt": socket.SO_SNDBUF},
    )
    # Разрешить отправку broadcast-пакетов
    SO_BROADCAST: Optional[int] = Field(
        default=None,
        alias="broadcast",
        description="Allow transmission of broadcast messages (UDP)",
        json_schema_extra={"level": socket.SOL_SOCKET, "opt": socket.SO_BROADCAST},
    )
    # Встраивать out-of-band данные в основной поток
    SO_OOBINLINE: Optional[int] = Field(
        default=None,
        alias="oobinline",
        description="Receive out-of-band data in the normal data stream",
        json_schema_extra={"level": socket.SOL_SOCKET, "opt": socket.SO_OOBINLINE},
    )
    # Минимальное количество данных для срабатывания read()
    SO_RCVLOWAT: Optional[int] = Field(
        default=None,
        alias="rcvlowat",
        description="Minimum number of bytes in the input queue before read()",
        json_schema_extra={"level": socket.SOL_SOCKET, "opt": socket.SO_RCVLOWAT},
    )
    # Минимальное количество данных для срабатывания write()
    SO_SNDLOWAT: Optional[int] = Field(
        default=None,
        alias="sndlowat",
        description="Minimum number of bytes in the output queue before write()",
        json_schema_extra={"level": socket.SOL_SOCKET, "opt": socket.SO_SNDLOWAT},
    )
    # Таймаут операций чтения (в секундах)
    SO_RCVTIMEO: Optional[float] = Field(
        default=None,
        alias="rcvtimeo",
        description="Timeout value for input operations (seconds)",
        json_schema_extra={"level": socket.SOL_SOCKET, "opt": socket.SO_RCVTIMEO},
    )
    # Таймаут операций записи (в секундах)
    SO_SNDTIMEO: Optional[float] = Field(
        default=None,
        alias="sndtimeo",
        description="Timeout value for output operations (seconds)",
        json_schema_extra={"level": socket.SOL_SOCKET, "opt": socket.SO_SNDTIMEO},
    )

    # --- Validators ---
    @field_validator(
        "SO_REUSEADDR", "SO_REUSEPORT", "SO_KEEPALIVE", "SO_BROADCAST", "SO_OOBINLINE"
    )
    def validate_boolean_opts(cls, v):
        if v not in (0, 1, None):
            raise ValueError("This socket option must be 0, 1, or None")
        return v

    @field_validator("SO_RCVBUF", "SO_SNDBUF", "SO_RCVLOWAT", "SO_SNDLOWAT")
    def validate_positive_int(cls, v):
        if v is not None and v <= 0:
            raise ValueError("Buffer sizes and low watermarks must be > 0")
        return v

    @field_validator("SO_RCVTIMEO", "SO_SNDTIMEO")
    def validate_timeout(cls, v):
        if v is not None and v < 0:
            raise ValueError("Timeout values must be >= 0")
        return v


class TcpOpts(BaseModel):
    # --- TCP OPTS ---
    # Отключить алгоритм Нейгла (ускоряет маленькие пакеты)
    TCP_NODELAY: Optional[int] = Field(
        default=None,
        alias="nodelay",
        description="Disable Nagle's algorithm (send small packets immediately)",
        json_schema_extra={"level": socket.IPPROTO_TCP, "opt": socket.TCP_NODELAY},
    )
    # Буферизовать данные до flush (Linux-specific)
    TCP_CORK: Optional[int] = Field(
        default=None,
        alias="cork",
        description="Control packet coalescing before transmission (Linux only)",
        json_schema_extra={"level": socket.IPPROTO_TCP, "opt": socket.TCP_CORK},
    )
    # Быстрый ACK (уменьшает задержку ACK-пакетов)
    TCP_QUICKACK: Optional[int] = Field(
        default=None,
        alias="quickack",
        description="Enable quick ACKs to reduce ACK delay",
        json_schema_extra={"level": socket.IPPROTO_TCP, "opt": socket.TCP_QUICKACK},
    )
    # Время простоя до начала keepalive (секунды)
    TCP_KEEPIDLE: Optional[int] = Field(
        default=None,
        alias="keepidle",
        description="Idle time before sending TCP keepalive probes (seconds)",
        json_schema_extra={"level": socket.IPPROTO_TCP, "opt": socket.TCP_KEEPIDLE},
    )
    # Интервал между keepalive-пакетами (секунды)
    TCP_KEEPINTVL: Optional[int] = Field(
        default=None,
        alias="keepintvl",
        description="Interval between TCP keepalive probes (seconds)",
        json_schema_extra={"level": socket.IPPROTO_TCP, "opt": socket.TCP_KEEPINTVL},
    )
    # Количество неудачных keepalive до разрыва
    TCP_KEEPCNT: Optional[int] = Field(
        default=None,
        alias="keepcnt",
        description="Number of failed keepalive probes before connection drop",
        json_schema_extra={"level": socket.IPPROTO_TCP, "opt": socket.TCP_KEEPCNT},
    )

    # --- Validators ---
    @field_validator(
        "TCP_NODELAY",
        "TCP_CORK",
        "TCP_QUICKACK",
    )
    def validate_boolean_opts(cls, v):
        if v not in (0, 1, None):
            raise ValueError("This socket option must be 0, 1, or None")
        return v

    @field_validator("TCP_KEEPIDLE", "TCP_KEEPINTVL", "TCP_KEEPCNT")
    def validate_keepalive(cls, v):
        if v is not None and v <= 0:
            raise ValueError("TCP keepalive parameters must be > 0")
        return v


class IpOpts(BaseModel):
    # --- IP ---
    # TTL для исходящих IP-пакетов
    IP_TTL: Optional[int] = Field(
        default=None,
        alias="ttl",
        description="Set Time-To-Live (TTL) for outgoing IP packets",
        json_schema_extra={"level": socket.IPPROTO_IP, "opt": socket.IP_TTL},
    )

    # ToS/DSCP метка для QoS
    IP_TOS: Optional[int] = Field(
        default=None,
        alias="tos",
        description="Set Type of Service (ToS) / DSCP for outgoing packets",
        json_schema_extra={"level": socket.IPPROTO_IP, "opt": socket.IP_TOS},
    )

    # --- Validators ---
    @field_validator("IP_TTL")
    def validate_ttl(cls, v):
        if v is not None and not (0 <= v <= 255):
            raise ValueError("IP_TTL must be between 0 and 255")
        return v

    @field_validator("IP_TOS")
    def validate_tos(cls, v):
        if v is not None and not (0 <= v <= 255):
            raise ValueError("IP_TOS must be between 0 and 255")
        return v


class Ipv6Opts(BaseModel):
    # --- IPv6 only ---
    # Только IPv6 (без IPv4-mapped адресов)
    IPV6_V6ONLY: Optional[int] = Field(
        default=None,
        alias="v6only",
        description="Restrict socket to IPv6 only (disable IPv4-mapped addresses)",
        json_schema_extra={"level": socket.IPPROTO_IPV6, "opt": socket.IPV6_V6ONLY},
    )

    # --- Validators ---
    @field_validator("IPV6_V6ONLY")
    def validate_boolean_opts(cls, v):
        if v not in (0, 1, None):
            raise ValueError("This socket option must be 0, 1, or None")
        return v


class SocketOpts(BaseModel):
    so: SoOpt = Field(default_factory=SoOpt)
    tcp: TcpOpts = Field(default_factory=TcpOpts)
    ip: IpOpts = Field(default_factory=IpOpts)
    ipv6: Ipv6Opts = Field(default_factory=Ipv6Opts)


class SocketHelper:
    @staticmethod
    def setsockopt(sock: socket.socket, opts: SocketOpts) -> socket.socket:
        """Apply socket options defined in SocketOpts including nested models"""

        def apply_options(
            model: BaseModel, model_class: type[BaseModel], prefix: str = ""
        ):
            """Рекурсивно применяет опции из модели и вложенных моделей"""
            for name, field in model_class.model_fields.items():
                value = getattr(model, name)

                # Пропускаем None значения
                if value is None:
                    continue

                # Если значение является вложенной моделью Pydantic
                if isinstance(value, BaseModel):
                    # Рекурсивно обрабатываем вложенную модель
                    apply_options(value, type(value), prefix=f"{prefix}{name}.")
                    continue

                # Получаем метаданные из json_schema_extra
                meta = field.json_schema_extra
                if not isinstance(meta, dict):
                    continue

                level = meta.get("level")
                opt = meta.get("opt")

                if level is None or opt is None:
                    continue

                # Специальный случай для SO_LINGER
                if name.upper() == "SO_LINGER" or opt == socket.SO_LINGER:
                    if not isinstance(value, tuple) or len(value) != 2:
                        raise ValueError(
                            f"SO_LINGER must be tuple (onoff, linger), got {type(value)}"
                        )
                    value = struct.pack("ii", *value)

                # Применяем опцию к сокету
                if isinstance(level, int) and isinstance(opt, int):
                    sock.setsockopt(level, opt, value)

        # Начинаем применение опций с корневой модели
        apply_options(opts, type(opts))

        return sock

    @staticmethod
    def make_socket_with_reuse_port(host: str, port: int):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        SocketHelper.setsockopt(
            sock,
            opts=SocketOpts(
                so=SoOpt(
                    reuseaddr=1,
                    reuseport=1,
                )
            ),
        )
        sock.bind((host, port))
        sock.listen(100)
        sock.set_inheritable(True)  # важно для передачи fd

        return sock

    @staticmethod
    def find_free_port(host: str, port_range_start: int, port_range_end: int) -> int:
        """Найти свободный порт из пула"""
        for port in range(port_range_start, port_range_end + 1):
            # дополнительно проверяем, что порт действительно свободен
            if SocketHelper.is_port_free(host, port):
                return port

        raise ValueError(
            f"No free ports in range {port_range_start} - {port_range_end}"
        )

    @staticmethod
    def is_port_free(host: str, port: int) -> bool:
        """Проверить, свободен ли порт"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((host, port))
                return True
        except OSError:
            return False

    @staticmethod
    def send_fds(sock: socket.socket, fds: List[int], message: bytes = b"FD") -> None:
        """
        Отправить файловые дескрипторы через Unix domain socket.

        Args:
            sock: Unix domain socket (AF_UNIX, SOCK_STREAM или SOCK_DGRAM)
            fds: Список файловых дескрипторов (int) для отправки
            message: Сообщение-маркер (минимум 1 байт, иначе sendmsg не работает)

        Raises:
            ValueError: Если message пустое или sock не Unix socket
            OSError: Если отправка не удалась
        """
        if not message:
            raise ValueError("Message cannot be empty")

        if not fds:
            raise ValueError("Must provide at least one file descriptor")

        # Создаем массив int из списка дескрипторов
        fds_array = array.array("i", fds)

        # Формируем ancillary data с типом SCM_RIGHTS
        ancdata = [
            (
                socket.SOL_SOCKET,  # Уровень протокола
                socket.SCM_RIGHTS,  # Тип - передача прав
                fds_array.tobytes(),  # Массив дескрипторов как bytes
            )
        ]

        # Отправляем через sendmsg
        # message - обычные данные (должны быть непустыми)
        # ancdata - специальные данные (наши дескрипторы)
        sent = sock.sendmsg(
            [message],  # buffers - список байтовых буферов
            ancdata,  # ancdata - ancillary data
            0,  # flags
            None,  # address (для connected sockets не нужен)
        )

        if sent == 0:
            raise OSError("Failed to send file descriptors")

    @staticmethod
    def recv_fds(
        sock: socket.socket, maxfds: int = 10, bufsize: int = 1024
    ) -> Tuple[bytes, List[int]]:
        """
        Получить файловые дескрипторы через Unix domain socket.

        Args:
            sock: Unix domain socket
            maxfds: Максимальное количество ожидаемых дескрипторов
            bufsize: Размер буфера для обычного сообщения

        Returns:
            Tuple[bytes, List[int]]: (полученное сообщение, список дескрипторов)

        Raises:
            OSError: Если получение не удалось
        """
        # Размер буфера для ancillary data
        # Нужно достаточно места для всех дескрипторов
        fds_size = maxfds * array.array("i").itemsize

        # recvmsg возвращает: (data, ancdata, msg_flags, address)
        msg, ancdata, msg_flags, addr = sock.recvmsg(
            bufsize,  # Размер буфера для обычных данных
            socket.CMSG_LEN(fds_size),  # Размер буфера для ancillary data
        )

        # Извлекаем дескрипторы из ancillary data
        received_fds = []

        for cmsg_level, cmsg_type, cmsg_data in ancdata:
            # Проверяем что это именно SCM_RIGHTS
            if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
                # Конвертируем bytes обратно в массив int
                fds_array = array.array("i")
                fds_array.frombytes(cmsg_data)
                received_fds.extend(fds_array.tolist())

        return msg, received_fds

    @staticmethod
    def websocket_remote_addr(websocket):
        host, port = getattr(websocket, "remote_address", ("unknown", ""))
        return f"{host}:{port}"

    @staticmethod
    def websocket_remote_host(websocket):
        host, _ = getattr(websocket, "remote_address", ("unknown", ""))
        return host

    @staticmethod
    def socket_addr(socket: socket.socket):
        addr, port = socket.getsockname()
        return f"{addr}:{port}"

    @staticmethod
    def sockets_addr(socket: Iterable[socket.socket]):
        res = map(lambda x: SocketHelper.socket_addr(x), socket)
        res = ", ".join(res)

        return res
