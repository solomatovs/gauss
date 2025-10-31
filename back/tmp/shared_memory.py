import struct
import json
from typing import TypeVar, Generic, Optional, Type, Any, Dict, Callable
from multiprocessing import shared_memory, Lock as ProcessLock, RLock as ProcessRLock
from threading import RLock as ThreadRLock
from abc import ABC, abstractmethod
from enum import Enum

from pydantic import Field
from pydantic_settings import BaseSettings


class LockMode(Enum):
    """Режимы блокировки."""
    READ = "read"
    WRITE = "write"


class SharedMemoryConfig(BaseSettings):
    """
    Конфигурация для SharedMemoryStorage.
    
    Может быть загружена из переменных окружения с префиксом SHM_
    """
    
    # ========== Lock настройки ==========
    
    lock_timeout: float = Field(
        default=5.0,
        ge=0.1,
        description="Таймаут ожидания блокировки в секундах"
    )
    
    class Config:
        env_prefix = 'SHM_'
        case_sensitive = False
        env_file = '.env'
        env_file_encoding = 'utf-8'


class Serializable(ABC):
    """
    Абстрактный интерфейс для объектов, которые могут быть сохранены в SharedMemory.
    """
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """
        Преобразует объект в словарь для сериализации.
        
        Returns:
            Словарь с данными объекта
        """
        pass
    
    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Serializable':
        """
        Создает объект из словаря.
        
        Args:
            data: Словарь с данными
            
        Returns:
            Новый экземпляр объекта
        """
        pass


T = TypeVar('T', bound=Serializable)


class LockContext:
    """
    Context manager для управления блокировками.
    
    Поддерживает два режима:
    - READ: Разделяемая блокировка (несколько читателей одновременно)
    - WRITE: Эксклюзивная блокировка (один писатель)
    """
    
    def __init__(
        self,
        read_lock: ProcessRLock,
        write_lock: ProcessLock,
        thread_lock: ThreadRLock,
        mode: LockMode,
        timeout: float
    ):
        self.read_lock = read_lock
        self.write_lock = write_lock
        self.thread_lock = thread_lock
        self.mode = mode
        self.timeout = timeout
        self.acquired = False
        self._thread_acquired = False
    
    def __enter__(self):
        # Сначала захватываем thread lock
        self._thread_acquired = self.thread_lock.acquire(timeout=self.timeout)
        if not self._thread_acquired:
            raise TimeoutError(f"Failed to acquire thread lock within {self.timeout} seconds")
        
        try:
            if self.mode == LockMode.READ:
                # Разделяемая блокировка для чтения
                # Несколько процессов могут одновременно читать
                self.acquired = self.read_lock.acquire(timeout=self.timeout)
            else:
                # Эксклюзивная блокировка для записи
                # Только один процесс может писать
                self.acquired = self.write_lock.acquire(timeout=self.timeout)
            
            if not self.acquired:
                self.thread_lock.release()
                self._thread_acquired = False
                raise TimeoutError(f"Failed to acquire {self.mode.value} lock within {self.timeout} seconds")
            
            return self
            
        except Exception:
            if self._thread_acquired:
                self.thread_lock.release()
                self._thread_acquired = False
            raise
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.acquired:
            if self.mode == LockMode.READ:
                self.read_lock.release()
            else:
                self.write_lock.release()
            self.acquired = False
        
        if self._thread_acquired:
            self.thread_lock.release()
            self._thread_acquired = False
        
        return False


class SharedMemoryStorage(Generic[T]):
    """
    Универсальное хранилище для объектов в SharedMemory.
    
    Особенности:
    - Generic - может хранить любой тип данных, реализующий Serializable
    - Многопроцессная синхронизация через блокировки
    - Поддержка read/write блокировок (несколько читателей, один писатель)
    - Автоматический счетчик ссылок (reference counting)
    - Автоматическое удаление SharedMemory когда refcount достигает 0
    - Thread-safe и process-safe операции
    
    Формат SharedMemory:
    [HEADER: 16 байт]
    - 4 байта: magic number (для проверки валидности)
    - 4 байта: версия формата
    - 4 байта: reference count
    - 4 байта: длина данных
    [DATA: переменная длина]
    - N байт: JSON данные объекта
    """
    
    # Магическое число для проверки валидности SharedMemory
    MAGIC_NUMBER = 0x53484D53  # "SHMS" в hex (SharedMemory Storage)
    FORMAT_VERSION = 1
    
    # Размер заголовка
    HEADER_SIZE = 16
    
    # Дефолтная конфигурация для класса
    _default_config: Optional[SharedMemoryConfig] = None
    
    @classmethod
    def set_default_config(cls, config: SharedMemoryConfig) -> None:
        """Устанавливает конфигурацию по умолчанию для всех новых экземпляров."""
        cls._default_config = config
    
    @classmethod
    def get_default_config(cls) -> SharedMemoryConfig:
        """Возвращает конфигурацию по умолчанию."""
        if cls._default_config is None:
            cls._default_config = SharedMemoryConfig()
        return cls._default_config
    
    @classmethod
    def create(
        cls,
        name: str,
        data_type: Type[T],
        initial_data: Optional[T] = None,
        config: Optional[SharedMemoryConfig] = None
    ) -> 'SharedMemoryStorage[T]':
        """
        Создает или подключается к существующему SharedMemoryStorage.
        
        Args:
            name: Уникальное имя для SharedMemory
            data_type: Класс типа данных (должен реализовывать Serializable)
            initial_data: Начальные данные (если создается новое хранилище)
            config: Конфигурация поведения
            
        Returns:
            Экземпляр SharedMemoryStorage
            
        Raises:
            ValueError: Если SharedMemory не существует и не предоставлены initial_data
        """
        config = config or cls.get_default_config()
        shm_name = f"shm_{name}"
        
        # Пробуем подключиться к существующему SharedMemory
        existing_data = cls._try_load_existing(shm_name, data_type, config)
        
        if existing_data:
            # Нашли существующий SharedMemory
            data = data_type.from_dict(existing_data)  # type: ignore[assignment]
            is_new = False
            shm = shared_memory.SharedMemory(name=shm_name)
        elif initial_data:
            # Создаем новое хранилище с начальными данными
            data = initial_data
            is_new = True
            
            # Вычисляем необходимый размер
            serialized = cls._serialize_data(data, refcount=0)
            size = len(serialized) + 1024  # Добавляем запас
            
            shm = shared_memory.SharedMemory(
                create=True,
                size=size,
                name=shm_name
            )
            
            # Записываем начальные данные
            shm.buf[:len(serialized)] = serialized
        else:
            raise ValueError(
                f"SharedMemory '{name}' does not exist and no initial_data provided"
            )
        
        # Создаем экземпляр с уже инициализированными данными
        return cls(
            name=name,
            data_type=data_type,
            data=data,
            shm=shm,
            is_new=is_new,
            config=config
        )
    
    def __init__(
        self,
        name: str,
        data_type: Type[T],
        data: T,
        shm: shared_memory.SharedMemory,
        is_new: bool,
        config: SharedMemoryConfig
    ):
        """
        Приватный конструктор. Используйте create() для создания экземпляра.
        
        Args:
            name: Уникальное имя
            data_type: Класс типа данных
            data: Инициализированные данные
            shm: Инициализированный SharedMemory
            is_new: Флаг нового хранилища
            config: Конфигурация
        """
        self._name = name
        self._data_type = data_type
        self._data = data
        self._shm = shm
        self._is_new = is_new
        self._config = config
        
        # Блокировки
        self._read_lock = ProcessRLock()  # Reentrant lock для множественного чтения
        self._write_lock = ProcessLock()  # Exclusive lock для записи
        self._thread_lock = ThreadRLock()  # Thread-level reentrant lock
        
        # Состояние
        self._closed = False
        
        # Увеличиваем счетчик ссылок
        self._increment_refcount()

    @classmethod
    def _try_load_existing(
        cls,
        shm_name: str,
        data_type: Type[T],
        config: SharedMemoryConfig
    ) -> Optional[Dict[str, Any]]:
        """
        Пытается загрузить данные из существующего SharedMemory.
        
        Args:
            shm_name: Полное имя SharedMemory
            data_type: Тип данных
            config: Конфигурация
            
        Returns:
            Словарь с данными или None, если SharedMemory не существует
        """
        try:
            # Пробуем подключиться к существующему SharedMemory
            shm = shared_memory.SharedMemory(name=shm_name)
            lock = ProcessLock()
            
            try:
                # Пробуем захватить lock с таймаутом
                if lock.acquire(timeout=config.lock_timeout):
                    try:
                        # Проверяем валидность данных
                        if not cls._validate_header(bytes(shm.buf)):
                            return None
                        
                        # Читаем данные
                        data = cls._deserialize_data(bytes(shm.buf))
                        return data
                    finally:
                        lock.release()
                else:
                    return None
            finally:
                shm.close()
                
        except FileNotFoundError:
            # SharedMemory не существует - это нормально
            return None
        except Exception:
            return None

    @staticmethod
    def _validate_header(data: bytes) -> bool:
        """
        Проверяет валидность заголовка SharedMemory.
        
        Args:
            data: Данные из SharedMemory
            
        Returns:
            True если заголовок валиден
        """
        if len(data) < SharedMemoryStorage.HEADER_SIZE:
            return False
        
        magic, version, refcount, data_len = struct.unpack(
            'IIII',
            data[:SharedMemoryStorage.HEADER_SIZE]
        )
        
        return (
            magic == SharedMemoryStorage.MAGIC_NUMBER and 
            version == SharedMemoryStorage.FORMAT_VERSION
        )

    def _acquire_lock(self, mode: LockMode, timeout: Optional[float] = None) -> LockContext:
        """
        Создает context manager для захвата блокировки.
        
        Args:
            mode: Режим блокировки (READ или WRITE)
            timeout: Таймаут ожидания
            
        Returns:
            LockContext для использования в with statement
        """
        timeout = timeout or self._config.lock_timeout
        return LockContext(
            self._read_lock,
            self._write_lock,
            self._thread_lock,
            mode,
            timeout
        )

    @staticmethod
    def _serialize_data(data: Serializable, refcount: int) -> bytes:
        """
        Сериализует данные в байты для записи в SharedMemory.
        
        Args:
            data: Данные для сериализации
            refcount: Счетчик ссылок
            
        Returns:
            Байты для записи в SharedMemory
        """
        # Сериализуем объект через его метод to_dict
        data_dict = data.to_dict()
        json_bytes = json.dumps(data_dict, default=str).encode('utf-8')
        data_length = len(json_bytes)
        
        # Создаем заголовок
        header = struct.pack(
            'IIII',
            SharedMemoryStorage.MAGIC_NUMBER,
            SharedMemoryStorage.FORMAT_VERSION,
            refcount,
            data_length
        )
        
        return header + json_bytes

    @staticmethod
    def _deserialize_data(data: bytes) -> Dict[str, Any]:
        """
        Десериализует байты из SharedMemory в словарь.
        
        Args:
            data: Байты из SharedMemory
            
        Returns:
            Словарь с данными
        """
        # Читаем заголовок
        magic, version, refcount, data_length = struct.unpack(
            'IIII',
            data[:SharedMemoryStorage.HEADER_SIZE]
        )
        
        # Проверяем magic number
        if magic != SharedMemoryStorage.MAGIC_NUMBER:
            raise ValueError(f"Invalid magic number: {magic:08x}")
        
        if version != SharedMemoryStorage.FORMAT_VERSION:
            raise ValueError(f"Unsupported format version: {version}")
        
        # Читаем JSON данные
        json_start = SharedMemoryStorage.HEADER_SIZE
        json_end = json_start + data_length
        json_bytes = data[json_start:json_end]
        result = json.loads(json_bytes.decode('utf-8'))
        
        return result

    def _get_refcount(self) -> int:
        """Читает текущий refcount из SharedMemory (БЕЗ блокировки)."""
        refcount_bytes = bytes(self._shm.buf[8:12])
        refcount, = struct.unpack('I', refcount_bytes)
        return refcount

    def _set_refcount(self, value: int) -> None:
        """Устанавливает refcount в SharedMemory (БЕЗ блокировки)."""
        self._shm.buf[8:12] = struct.pack('I', value)

    def _increment_refcount(self) -> int:
        """Увеличивает счетчик ссылок на 1."""
        with self._acquire_lock(LockMode.WRITE):
            refcount = self._get_refcount()
            new_refcount = refcount + 1
            self._set_refcount(new_refcount)
            return new_refcount

    def _decrement_refcount(self) -> int:
        """Уменьшает счетчик ссылок на 1."""
        with self._acquire_lock(LockMode.WRITE):
            refcount = self._get_refcount()
            new_refcount = max(0, refcount - 1)
            self._set_refcount(new_refcount)
            
            # Если refcount достиг 0, удаляем SharedMemory
            if new_refcount == 0:
                try:
                    self._shm.unlink()
                except Exception:
                    pass
            
            return new_refcount

    def _load_from_shared_memory(self) -> None:
        """Загружает данные из SharedMemory (БЕЗ блокировки)."""
        data_dict = self._deserialize_data(bytes(self._shm.buf))
        self._data = self._data_type.from_dict(data_dict)  # type: ignore[assignment]

    def _save_to_shared_memory(self) -> None:
        """Сохраняет текущее состояние в SharedMemory (БЕЗ блокировки)."""
        # Сохраняем текущий refcount
        current_refcount = self._get_refcount()
        
        # Сериализуем
        serialized = self._serialize_data(self._data, current_refcount)
        
        if len(serialized) > self._shm.size:
            raise ValueError(
                f"Serialized data ({len(serialized)} bytes) exceeds "
                f"SharedMemory size ({self._shm.size} bytes)"
            )
        
        self._shm.buf[:len(serialized)] = serialized

    # ========== PUBLIC API ==========
    
    @property
    def name(self) -> str:
        """Возвращает имя хранилища."""
        return self._name
    
    @property
    def config(self) -> SharedMemoryConfig:
        """Возвращает конфигурацию (read-only)."""
        return self._config
    
    @property
    def refcount(self) -> int:
        """Возвращает текущий счетчик ссылок."""
        with self._acquire_lock(LockMode.READ):
            return self._get_refcount()

    def get(self) -> T:
        """
        Получает текущие данные из SharedMemory (read lock).
        
        Returns:
            Актуальная копия данных
        """
        with self._acquire_lock(LockMode.READ):
            self._load_from_shared_memory()
            return self._data

    def set(self, data: T) -> None:
        """
        Устанавливает новые данные в SharedMemory (write lock).
        
        Args:
            data: Новые данные
        """
        with self._acquire_lock(LockMode.WRITE):
            self._data = data
            self._save_to_shared_memory()

    def update(self, updater: Callable[[T], T]) -> T:
        """
        Атомарно обновляет данные через функцию (write lock).
        
        Args:
            updater: Функция, которая принимает текущие данные и возвращает новые
                    updater(old_data: T) -> T
        
        Returns:
            Обновленные данные
        
        Example:
            def increment_count(worker):
                worker.requests_count += 1
                return worker
            
            storage.update(increment_count)
        """
        with self._acquire_lock(LockMode.WRITE):
            # Загружаем актуальные данные
            self._load_from_shared_memory()
            
            # Применяем функцию обновления
            self._data = updater(self._data)
            
            # Сохраняем
            self._save_to_shared_memory()
            
            return self._data

    def reload(self) -> T:
        """
        Перезагружает данные из SharedMemory (read lock).
        
        Returns:
            Обновленные данные
        """
        with self._acquire_lock(LockMode.READ):
            self._load_from_shared_memory()
            return self._data

    def close(self) -> None:
        """
        Закрывает хранилище.
        
        Уменьшает refcount. Если refcount достигает 0, SharedMemory удаляется автоматически.
        Безопасно вызывать многократно.
        """
        if self._closed:
            return
        
        self._closed = True
        
        try:
            # Уменьшаем refcount (внутри происходит автоудаление при refcount=0)
            self._decrement_refcount()
            
            # Закрываем дескриптор
            self._shm.close()
        except Exception:
            pass

    def __del__(self):
        """Автоматически закрывает при удалении объекта."""
        self.close()

    def __repr__(self) -> str:
        try:
            refcount = self.refcount
        except Exception:
            refcount = "?"
        
        return (
            f"SharedMemoryStorage(name={self._name!r}, "
            f"type={self._data_type.__name__}, "
            f"refcount={refcount})"
        )

    def __enter__(self):
        """Context manager support."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager support."""
        self.close()
        return False
