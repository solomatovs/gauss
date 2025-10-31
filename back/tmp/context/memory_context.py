import struct
import json
import ctypes
import ctypes.util
import time
import os
import threading
from typing import TypeVar, Generic, Optional, Type, Any, Dict, List, Callable
from multiprocessing import shared_memory
from abc import ABC, abstractmethod
from enum import Enum
from dataclasses import dataclass

from pydantic import Field
from pydantic_settings import BaseSettings


# ========== Конфигурация ==========

class SharedMemoryConfig(BaseSettings):
    """
    Конфигурация для SharedMemory системы.
    
    Может быть загружена из переменных окружения с префиксом SHM_
    """
    
    lock_timeout: float = Field(
        default=5.0,
        ge=0.1,
        description="Таймаут ожидания блокировки в секундах"
    )
    
    heartbeat_interval: float = Field(
        default=10.0,
        ge=1.0,
        description="Интервал обновления heartbeat в секундах"
    )
    
    stale_lock_timeout: float = Field(
        default=60.0,
        ge=10.0,
        description="Время после которого блокировка считается зависшей"
    )
    
    max_locks: int = Field(
        default=16,
        ge=1,
        le=256,
        description="Максимальное количество одновременных блокировок"
    )
    
    class Config:
        env_prefix = 'SHM_'
        case_sensitive = False
        env_file = '.env'
        env_file_encoding = 'utf-8'


# ========== Загрузка pthread библиотеки ==========

libpthread = ctypes.CDLL(ctypes.util.find_library('pthread'), use_errno=True)

# Размеры структур на x86_64 Linux
PTHREAD_RWLOCK_SIZE = 56

# Константы
PTHREAD_PROCESS_SHARED = 1
ETIMEDOUT = 110

# pthread_rwlockattr API
pthread_rwlockattr_init = libpthread.pthread_rwlockattr_init
pthread_rwlockattr_init.argtypes = [ctypes.c_void_p]
pthread_rwlockattr_init.restype = ctypes.c_int

pthread_rwlockattr_setpshared = libpthread.pthread_rwlockattr_setpshared
pthread_rwlockattr_setpshared.argtypes = [ctypes.c_void_p, ctypes.c_int]
pthread_rwlockattr_setpshared.restype = ctypes.c_int

pthread_rwlockattr_destroy = libpthread.pthread_rwlockattr_destroy
pthread_rwlockattr_destroy.argtypes = [ctypes.c_void_p]
pthread_rwlockattr_destroy.restype = ctypes.c_int

# pthread_rwlock API
pthread_rwlock_init = libpthread.pthread_rwlock_init
pthread_rwlock_init.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
pthread_rwlock_init.restype = ctypes.c_int

pthread_rwlock_rdlock = libpthread.pthread_rwlock_rdlock
pthread_rwlock_rdlock.argtypes = [ctypes.c_void_p]
pthread_rwlock_rdlock.restype = ctypes.c_int

pthread_rwlock_wrlock = libpthread.pthread_rwlock_wrlock
pthread_rwlock_wrlock.argtypes = [ctypes.c_void_p]
pthread_rwlock_wrlock.restype = ctypes.c_int

pthread_rwlock_unlock = libpthread.pthread_rwlock_unlock
pthread_rwlock_unlock.argtypes = [ctypes.c_void_p]
pthread_rwlock_unlock.restype = ctypes.c_int

pthread_rwlock_destroy = libpthread.pthread_rwlock_destroy
pthread_rwlock_destroy.argtypes = [ctypes.c_void_p]
pthread_rwlock_destroy.restype = ctypes.c_int

pthread_rwlock_timedrdlock = libpthread.pthread_rwlock_timedrdlock
pthread_rwlock_timedrdlock.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
pthread_rwlock_timedrdlock.restype = ctypes.c_int

pthread_rwlock_timedwrlock = libpthread.pthread_rwlock_timedwrlock
pthread_rwlock_timedwrlock.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
pthread_rwlock_timedwrlock.restype = ctypes.c_int


# ========== Pthread RWLock Wrapper ==========

class PthreadRWLock:
    """Wrapper для pthread_rwlock_t в SharedMemory."""
    
    def __init__(self, memory_buffer: memoryview, offset: int):
        self.buffer = memory_buffer
        self.offset = offset
        self._rwlock_array = (ctypes.c_byte * PTHREAD_RWLOCK_SIZE).from_buffer(
            memory_buffer, offset
        )
        self.rwlock_ptr = ctypes.cast(
            ctypes.pointer(self._rwlock_array),
            ctypes.c_void_p
        )
    
    def init(self) -> None:
        """Инициализирует rwlock для межпроцессного использования."""
        rwlockattr = (ctypes.c_byte * 8)()
        attr_ptr = ctypes.cast(ctypes.pointer(rwlockattr), ctypes.c_void_p)
        
        ret = pthread_rwlockattr_init(attr_ptr)
        if ret != 0:
            raise OSError(f"pthread_rwlockattr_init failed: {ret}")
        
        try:
            ret = pthread_rwlockattr_setpshared(attr_ptr, PTHREAD_PROCESS_SHARED)
            if ret != 0:
                raise OSError(f"pthread_rwlockattr_setpshared failed: {ret}")
            
            ret = pthread_rwlock_init(self.rwlock_ptr, attr_ptr)
            if ret != 0:
                raise OSError(f"pthread_rwlock_init failed: {ret}")
        finally:
            pthread_rwlockattr_destroy(attr_ptr)
    
    def read_lock(self, timeout: Optional[float] = None) -> None:
        """Захватывает read lock."""
        if timeout is None:
            ret = pthread_rwlock_rdlock(self.rwlock_ptr)
        else:
            current_time = time.time()
            abs_timeout = current_time + timeout
            tv_sec = int(abs_timeout)
            tv_nsec = int((abs_timeout - tv_sec) * 1_000_000_000)
            
            timespec = struct.pack('ll', tv_sec, tv_nsec)
            timespec_array = (ctypes.c_byte * len(timespec)).from_buffer_copy(timespec)
            timespec_ptr = ctypes.cast(ctypes.pointer(timespec_array), ctypes.c_void_p)
            
            ret = pthread_rwlock_timedrdlock(self.rwlock_ptr, timespec_ptr)
        
        if ret != 0:
            if ret == ETIMEDOUT:
                raise TimeoutError(f"Failed to acquire read lock within {timeout} seconds")
            raise OSError(f"pthread_rwlock_rdlock failed: {ret}")
    
    def write_lock(self, timeout: Optional[float] = None) -> None:
        """Захватывает write lock."""
        if timeout is None:
            ret = pthread_rwlock_wrlock(self.rwlock_ptr)
        else:
            current_time = time.time()
            abs_timeout = current_time + timeout
            tv_sec = int(abs_timeout)
            tv_nsec = int((abs_timeout - tv_sec) * 1_000_000_000)
            
            timespec = struct.pack('ll', tv_sec, tv_nsec)
            timespec_array = (ctypes.c_byte * len(timespec)).from_buffer_copy(timespec)
            timespec_ptr = ctypes.cast(ctypes.pointer(timespec_array), ctypes.c_void_p)
            
            ret = pthread_rwlock_timedwrlock(self.rwlock_ptr, timespec_ptr)
        
        if ret != 0:
            if ret == ETIMEDOUT:
                raise TimeoutError(f"Failed to acquire write lock within {timeout} seconds")
            raise OSError(f"pthread_rwlock_wrlock failed: {ret}")
    
    def unlock(self) -> None:
        """Освобождает блокировку."""
        ret = pthread_rwlock_unlock(self.rwlock_ptr)
        if ret != 0:
            raise OSError(f"pthread_rwlock_unlock failed: {ret}")
    
    def destroy(self) -> None:
        """Уничтожает rwlock."""
        pthread_rwlock_destroy(self.rwlock_ptr)


# ========== Lock Types ==========

class LockType(Enum):
    """Типы блокировок."""
    SHARED = 1
    EXCLUSIVE = 2


@dataclass
class LockEntry:
    """Запись в таблице блокировок."""
    start_offset: int       # Начало диапазона в DATA SECTION
    length: int             # Длина диапазона
    owner_pid: int          # PID владельца
    owner_tid: int          # Thread ID владельца
    lock_type: LockType     # SHARED или EXCLUSIVE
    active: bool            # Активна ли блокировка
    timestamp: float        # Время последнего обновления (heartbeat)


# ========== Lock Handle ==========

class LockHandle:
    """
    Handle для активной блокировки.
    Автоматически обновляет timestamp (heartbeat).
    """
    
    def __init__(
        self,
        block: 'SharedMemoryBlock',
        lock_id: int,
        heartbeat_interval: float
    ):
        self.block = block
        self.lock_id = lock_id
        self.heartbeat_interval = heartbeat_interval
        self._closed = False
        self._heartbeat_thread = None
        
        # Запускаем heartbeat поток
        if heartbeat_interval > 0:
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop,
                daemon=True
            )
            self._heartbeat_thread.start()
    
    def _heartbeat_loop(self):
        """Обновляет timestamp периодически."""
        while not self._closed:
            time.sleep(self.heartbeat_interval)
            if not self._closed:
                try:
                    self.block._update_lock_timestamp(self.lock_id)
                except Exception:
                    # Игнорируем ошибки в heartbeat
                    pass
    
    def release(self):
        """Освобождает блокировку."""
        if self._closed:
            return
        
        self._closed = True
        self.block.release_lock(self.lock_id)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False
    
    def __del__(self):
        self.release()


# ========== Shared Memory Block ==========

class SharedMemoryBlock:
    """
    Низкоуровневый примитив для управления блокировками диапазонов
    в разделяемой памяти.
    
    Структура памяти:
    [CONTROL SECTION: 72 bytes]
      - Magic (4) + Version (4) + Control RWLock (56) + Total Size (8)
    [LOCK TABLE: max_locks * 88 bytes]
      - Каждая запись: RWLock (56) + metadata (32)
    [DATA SECTION: variable]
    """
    
    MAGIC_NUMBER = 0x534D424C  # "SMBL"
    VERSION = 1
    
    CONTROL_SIZE = 72  # Magic(4) + Version(4) + RWLock(56) + TotalSize(8)
    LOCK_ENTRY_SIZE = 88  # RWLock(56) + StartOffset(8) + Length(8) + 
                          # OwnerPID(4) + OwnerTID(4) + LockType(1) + 
                          # Active(1) + Timestamp(8) + padding(2)
    
    def __init__(
        self,
        name: str,
        shm: shared_memory.SharedMemory,
        config: SharedMemoryConfig,
        is_creator: bool
    ):
        """
        Приватный конструктор. Используйте create() или attach().
        """
        self.name = name
        self._shm = shm
        self._config = config
        self._is_creator = is_creator
        
        # Вычисляем размеры
        self._max_locks = config.max_locks
        self._lock_table_size = self._max_locks * self.LOCK_ENTRY_SIZE
        self._header_size = self.CONTROL_SIZE + self._lock_table_size
        self._data_section_offset = self._header_size
        
        # Control RWLock (для защиты метаданных)
        self._control_lock = PthreadRWLock(self._shm.buf, offset=8)  # После magic+version
        
        # Если создатель - инициализируем control lock
        if is_creator:
            self._control_lock.init()
    
    @classmethod
    def create(
        cls,
        name: str,
        data_size: int,
        config: Optional[SharedMemoryConfig] = None
    ) -> 'SharedMemoryBlock':
        """
        Создает новый блок разделяемой памяти.
        
        Args:
            name: Уникальное имя блока
            data_size: Размер DATA SECTION в байтах
            config: Конфигурация (опционально)
        
        Returns:
            Новый экземпляр SharedMemoryBlock
        """
        config = config or SharedMemoryConfig()
        
        # Вычисляем общий размер
        lock_table_size = config.max_locks * cls.LOCK_ENTRY_SIZE
        total_size = cls.CONTROL_SIZE + lock_table_size + data_size
        
        # Создаем SharedMemory
        shm = shared_memory.SharedMemory(
            create=True,
            size=total_size,
            name=f"smb_{name}"
        )
        
        # Создаем экземпляр
        block = cls(name, shm, config, is_creator=True)
        
        # Инициализируем заголовок
        block._init_header(total_size)
        
        # Инициализируем таблицу блокировок
        block._init_lock_table()
        
        return block
    
    @classmethod
    def attach(
        cls,
        name: str,
        config: Optional[SharedMemoryConfig] = None
    ) -> 'SharedMemoryBlock':
        """
        Подключается к существующему блоку.
        
        Args:
            name: Имя блока
            config: Конфигурация (опционально)
            
        Returns:
            Экземпляр SharedMemoryBlock
        """
        config = config or SharedMemoryConfig()
        
        # Подключаемся к SharedMemory
        shm = shared_memory.SharedMemory(name=f"smb_{name}")
        
        # Проверяем magic number
        magic, version = struct.unpack('II', bytes(shm.buf[0:8]))
        if magic != cls.MAGIC_NUMBER:
            shm.close()
            raise ValueError(f"Invalid magic number: {magic:08x}")
        if version != cls.VERSION:
            shm.close()
            raise ValueError(f"Unsupported version: {version}")
        
        return cls(name, shm, config, is_creator=False)
    
    def _init_header(self, total_size: int):
        """Инициализирует CONTROL SECTION."""
        # Magic + Version
        self._shm.buf[0:4] = struct.pack('I', self.MAGIC_NUMBER)
        self._shm.buf[4:8] = struct.pack('I', self.VERSION)
        
        # Control RWLock уже инициализирован в __init__
        
        # Total Size
        self._shm.buf[64:72] = struct.pack('Q', total_size)
    
    def _init_lock_table(self):
        """Инициализирует таблицу блокировок."""
        offset = self.CONTROL_SIZE
        
        for i in range(self._max_locks):
            entry_offset = offset + (i * self.LOCK_ENTRY_SIZE)
            
            # Инициализируем RWLock для этой записи
            lock = PthreadRWLock(self._shm.buf, entry_offset)
            lock.init()
            
            # Помечаем как неактивную
            active_offset = entry_offset + 56 + 8 + 8 + 4 + 4 + 1  # После всех полей до active
            self._shm.buf[active_offset] = 0
    
    def acquire_lock(
        self,
        offset: int,
        length: int,
        lock_type: LockType,
        timeout: Optional[float] = None
    ) -> LockHandle:
        """
        Захватывает блокировку на диапазон DATA SECTION.
        
        Алгоритм:
        1. Очищает упавшие блокировки (cleanup)
        2. Захватывает control lock (write)
        3. Находит свободный слот
        4. Проверяет конфликты, ждет если нужно
        5. Регистрирует блокировку
        6. Захватывает соответствующий RWLock
        7. Освобождает control lock
        8. Возвращает LockHandle с heartbeat
        
        Args:
            offset: Начало диапазона (относительно DATA SECTION)
            length: Длина диапазона
            lock_type: SHARED или EXCLUSIVE
            timeout: Таймаут ожидания
            
        Returns:
            LockHandle для управления блокировкой
        """
        timeout = timeout or self._config.lock_timeout
        start_time = time.time()
        
        # 1. Cleanup упавших блокировок
        self.cleanup_dead_locks()
        
        while True:
            # Проверяем timeout
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                raise TimeoutError(
                    f"Failed to acquire lock on [{offset}:{offset+length}) "
                    f"within {timeout} seconds"
                )
            
            remaining_timeout = timeout - elapsed
            
            # 2. Захватываем control lock
            self._control_lock.write_lock(timeout=remaining_timeout)
            
            try:
                # 3. Находим свободный слот
                lock_id = self._find_free_lock_slot()
                if lock_id is None:
                    raise ValueError(
                        f"No free lock slots available (max {self._max_locks})"
                    )
                
                # 4. Проверяем конфликты
                has_conflict = self._check_lock_conflicts(offset, length, lock_type)
                
                if not has_conflict:
                    # Нет конфликтов - можем захватить
                    
                    # 5. Регистрируем блокировку
                    entry = LockEntry(
                        start_offset=offset,
                        length=length,
                        owner_pid=os.getpid(),
                        owner_tid=threading.get_ident(),
                        lock_type=lock_type,
                        active=True,
                        timestamp=time.time()
                    )
                    self._set_lock_entry(lock_id, entry)
                    
                    # 6. Захватываем RWLock для этого диапазона
                    entry_rwlock = self._get_entry_rwlock(lock_id)
                    if lock_type == LockType.SHARED:
                        entry_rwlock.read_lock(timeout=remaining_timeout)
                    else:
                        entry_rwlock.write_lock(timeout=remaining_timeout)
                    
                    # 7. Освобождаем control lock
                    self._control_lock.unlock()
                    
                    # 8. Возвращаем handle с heartbeat
                    return LockHandle(
                        self,
                        lock_id,
                        self._config.heartbeat_interval
                    )
                else:
                    # Есть конфликты - освобождаем control lock и ждем
                    self._control_lock.unlock()
                    
                    # Ждем немного перед повторной попыткой
                    time.sleep(0.1)
                    
            except Exception:
                # При любой ошибке освобождаем control lock
                try:
                    self._control_lock.unlock()
                except Exception:
                    pass
                raise
    
    def release_lock(self, lock_id: int):
        """
        Освобождает блокировку.
        
        Args:
            lock_id: ID блокировки (индекс в таблице)
        """
        # Захватываем control lock
        self._control_lock.write_lock(timeout=self._config.lock_timeout)
        
        try:
            # Получаем entry
            entry = self._get_lock_entry(lock_id)
            
            if not entry.active:
                return  # Уже освобождена
            
            # Освобождаем RWLock
            entry_rwlock = self._get_entry_rwlock(lock_id)
            entry_rwlock.unlock()
            
            # Помечаем как неактивную
            entry.active = False
            self._set_lock_entry(lock_id, entry)
            
        finally:
            self._control_lock.unlock()
    
    def read(self, offset: int, length: int) -> bytes:
        """
        Читает данные из DATA SECTION.
        
        ВАЖНО: Должна быть активна блокировка на этот диапазон!
        """
        data_offset = self._data_section_offset + offset
        return bytes(self._shm.buf[data_offset:data_offset + length])
    
    def write(self, offset: int, data: bytes):
        """
        Записывает данные в DATA SECTION.
        
        ВАЖНО: Должна быть активна EXCLUSIVE блокировка на этот диапазон!
        """
        data_offset = self._data_section_offset + offset
        self._shm.buf[data_offset:data_offset + len(data)] = data
    
    def cleanup_dead_locks(self):
        """
        Очищает блокировки от упавших/зависших процессов.
        
        Вызывается автоматически при acquire_lock().
        """
        self._control_lock.write_lock(timeout=self._config.lock_timeout)
        
        try:
            current_time = time.time()
            
            for lock_id in range(self._max_locks):
                entry = self._get_lock_entry(lock_id)
                
                if not entry.active:
                    continue
                
                # Проверка 1: Жив ли процесс?
                process_alive = True
                try:
                    os.kill(entry.owner_pid, 0)
                except OSError:
                    process_alive = False
                
                # Проверка 2: Не истек ли timeout?
                lock_age = current_time - entry.timestamp
                lock_stale = lock_age > self._config.stale_lock_timeout
                
                # Если процесс мертв или блокировка зависла - очищаем
                if not process_alive or lock_stale:
                    # Освобождаем RWLock
                    entry_rwlock = self._get_entry_rwlock(lock_id)
                    try:
                        entry_rwlock.unlock()
                    except Exception:
                        pass
                    
                    # Помечаем как неактивную
                    entry.active = False
                    self._set_lock_entry(lock_id, entry)
        finally:
            self._control_lock.unlock()
    
    def close(self):
        """Закрывает доступ к блоку."""
        if self._shm:
            self._shm.close()
            self._shm = None
    
    def unlink(self):
        """Удаляет блок из системы. Только для создателя!"""
        if self._shm and self._is_creator:
            # Уничтожаем все RWLocks
            self._control_lock.destroy()
            
            for lock_id in range(self._max_locks):
                entry_rwlock = self._get_entry_rwlock(lock_id)
                entry_rwlock.destroy()
            
            self._shm.unlink()
    
    # ========== Внутренние методы ==========
    
    def _check_lock_conflicts(
        self,
        start: int,
        length: int,
        lock_type: LockType
    ) -> bool:
        """
        Проверяет конфликты с существующими блокировками.
        
        Правила:
        - SHARED + SHARED на пересечении = OK
        - SHARED + EXCLUSIVE на пересечении = CONFLICT
        - EXCLUSIVE + любой на пересечении = CONFLICT
        - Непересекающиеся = OK
        
        Returns:
            True если есть конфликт (нужно ждать)
        """
        end = start + length
        
        for lock_id in range(self._max_locks):
            entry = self._get_lock_entry(lock_id)
            
            if not entry.active:
                continue
            
            # Проверяем пересечение диапазонов
            entry_end = entry.start_offset + entry.length
            
            # Диапазоны НЕ пересекаются
            if end <= entry.start_offset or start >= entry_end:
                continue
            
            # Диапазоны пересекаются - проверяем типы
            if lock_type == LockType.SHARED and entry.lock_type == LockType.SHARED:
                continue  # OK - оба на чтение
            
            # Любая другая комбинация на пересечении = конфликт
            return True
        
        return False
    
    def _find_free_lock_slot(self) -> Optional[int]:
        """Находит свободный слот в таблице блокировок."""
        for lock_id in range(self._max_locks):
            entry = self._get_lock_entry(lock_id)
            if not entry.active:
                return lock_id
        return None
    
    def _get_lock_entry(self, lock_id: int) -> LockEntry:
        """Читает lock entry из памяти."""
        entry_offset = self.CONTROL_SIZE + (lock_id * self.LOCK_ENTRY_SIZE)
        
        # Пропускаем RWLock (56 байт)
        data_offset = entry_offset + 56
        
        # Читаем поля
        start_offset, length, owner_pid, owner_tid = struct.unpack(
            'QQIi',
            bytes(self._shm.buf[data_offset:data_offset + 24])
        )
        
        lock_type_byte, active_byte = struct.unpack(
            'BB',
            bytes(self._shm.buf[data_offset + 24:data_offset + 26])
        )
        
        timestamp, = struct.unpack(
            'd',
            bytes(self._shm.buf[data_offset + 26:data_offset + 34])
        )
        
        return LockEntry(
            start_offset=start_offset,
            length=length,
            owner_pid=owner_pid,
            owner_tid=owner_tid,
            lock_type=LockType(lock_type_byte) if lock_type_byte > 0 else LockType.SHARED,
            active=bool(active_byte),
            timestamp=timestamp
        )
    
    def _set_lock_entry(self, lock_id: int, entry: LockEntry):
        """Записывает lock entry в память."""
        entry_offset = self.CONTROL_SIZE + (lock_id * self.LOCK_ENTRY_SIZE)
        data_offset = entry_offset + 56
        
        # Записываем поля
        self._shm.buf[data_offset:data_offset + 24] = struct.pack(
            'QQIi',
            entry.start_offset,
            entry.length,
            entry.owner_pid,
            entry.owner_tid
        )
        
        self._shm.buf[data_offset + 24:data_offset + 26] = struct.pack(
            'BB',
            entry.lock_type.value,
            1 if entry.active else 0
        )
        
        self._shm.buf[data_offset + 26:data_offset + 34] = struct.pack(
            'd',
            entry.timestamp
        )
    
    def _update_lock_timestamp(self, lock_id: int):
        """Обновляет timestamp блокировки (heartbeat)."""
        self._control_lock.write_lock(timeout=self._config.lock_timeout)
        
        try:
            entry = self._get_lock_entry(lock_id)
            if entry.active:
                entry.timestamp = time.time()
                self._set_lock_entry(lock_id, entry)
        finally:
            self._control_lock.unlock()
    
    def _get_entry_rwlock(self, lock_id: int) -> PthreadRWLock:
        """Возвращает RWLock для записи в таблице."""
        entry_offset = self.CONTROL_SIZE + (lock_id * self.LOCK_ENTRY_SIZE)
        return PthreadRWLock(self._shm.buf, entry_offset)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    
    def __repr__(self) -> str:
        return f"SharedMemoryBlock(name={self.name!r}, max_locks={self._max_locks})"