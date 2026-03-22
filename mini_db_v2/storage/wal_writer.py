# START_MODULE_CONTRACT
# Module: mini_db_v2.storage.wal_writer
# Intent: WAL Writer с буферизацией и контролем fsync.
# Dependencies: threading, dataclasses, typing
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: WALWriter, WALWriteOptions
# END_MODULE_MAP

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, BinaryIO
import threading
import os

from mini_db_v2.storage.wal import (
    WALRecord,
    WALRecordType,
    WALError,
    WALWriteError,
)


# =============================================================================
# START_BLOCK_WRITE_OPTIONS
# =============================================================================

@dataclass
class WALWriteOptions:
    """
    [START_CONTRACT_WAL_WRITE_OPTIONS]
    Intent: Опции записи WAL.
    Input: sync - выполнить fsync; flush - сбросить буфер.
    Output: Конфигурация для операции записи.
    [END_CONTRACT_WAL_WRITE_OPTIONS]
    """
    sync: bool = False       # fsync after write
    flush: bool = True       # flush buffer after write
    buffer: bool = True      # use buffering


# END_BLOCK_WRITE_OPTIONS


# =============================================================================
# START_BLOCK_WAL_WRITER
# =============================================================================

class WALWriter:
    """
    [START_CONTRACT_WAL_WRITER]
    Intent: Писатель WAL записей с буферизацией.
    Input: WALRecord для записи.
    Output: LSN записанной записи.
    Note: Буферизация для производительности, fsync для durability.
    [END_CONTRACT_WAL_WRITER]
    """
    
    DEFAULT_BUFFER_SIZE = 64 * 1024  # 64KB
    
    def __init__(
        self,
        file: BinaryIO,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        sync_on_write: bool = True
    ):
        """
        [START_CONTRACT_WW_INIT]
        Intent: Инициализация WAL Writer.
        Input: file - открытый файл; buffer_size - размер буфера.
        Output: Готовый к работе writer.
        [END_CONTRACT_WW_INIT]
        """
        self._file = file
        self._buffer_size = buffer_size
        self._sync_on_write = sync_on_write
        self._buffer = bytearray()
        self._lock = threading.RLock()
        self._bytes_written = 0
        self._records_written = 0
    
    @property
    def bytes_written(self) -> int:
        """Возвращает количество записанных байт."""
        with self._lock:
            return self._bytes_written
    
    @property
    def records_written(self) -> int:
        """Возвращает количество записанных записей."""
        with self._lock:
            return self._records_written
    
    def write(
        self,
        record: WALRecord,
        options: Optional[WALWriteOptions] = None
    ) -> int:
        """
        [START_CONTRACT_WW_WRITE]
        Intent: Записать WAL record.
        Input: record - запись; options - опции записи.
        Output: LSN записанной записи.
        Note: Возвращает LSN из record.
        [END_CONTRACT_WW_WRITE]
        """
        if options is None:
            options = WALWriteOptions()
        
        with self._lock:
            data = record.to_bytes()
            
            if options.buffer:
                # Add to buffer
                self._buffer.extend(data)
                
                # Flush if buffer is full
                if len(self._buffer) >= self._buffer_size:
                    self._flush_buffer()
            else:
                # Write directly
                self._file.write(data)
            
            self._bytes_written += len(data)
            self._records_written += 1
            
            # Handle flush and sync options
            if options.flush and options.buffer:
                self._flush_buffer()
            
            if options.sync or self._sync_on_write:
                if record.type == WALRecordType.COMMIT:
                    self.sync()
            
            return record.lsn
    
    def write_batch(
        self,
        records: list[WALRecord],
        options: Optional[WALWriteOptions] = None
    ) -> list[int]:
        """
        [START_CONTRACT_WW_WRITE_BATCH]
        Intent: Записать несколько WAL records.
        Input: records - список записей; options - опции записи.
        Output: Список LSN записанных записей.
        [END_CONTRACT_WW_WRITE_BATCH]
        """
        if options is None:
            options = WALWriteOptions(flush=True, sync=False)
        
        lsns = []
        with self._lock:
            for record in records:
                lsn = self.write(record, WALWriteOptions(flush=False, sync=False))
                lsns.append(lsn)
            
            # Final flush and sync
            if options.flush:
                self.flush()
            
            if options.sync:
                self.sync()
        
        return lsns
    
    def flush(self) -> None:
        """
        [START_CONTRACT_WW_FLUSH]
        Intent: Сбросить буфер на диск.
        Input: Нет.
        Output: Буфер записан в файл.
        [END_CONTRACT_WW_FLUSH]
        """
        with self._lock:
            self._flush_buffer()
            self._file.flush()
    
    def sync(self) -> None:
        """
        [START_CONTRACT_WW_SYNC]
        Intent: fsync файл для гарантии durability.
        Input: Нет.
        Output: Данные гарантированно на диске.
        [END_CONTRACT_WW_SYNC]
        """
        with self._lock:
            self.flush()
            os.fsync(self._file.fileno())
    
    def _flush_buffer(self) -> None:
        """Сбрасывает внутренний буфер в файл."""
        if self._buffer:
            self._file.write(self._buffer)
            self._buffer.clear()
    
    def close(self) -> None:
        """Закрыть writer (flush + sync)."""
        with self._lock:
            self.flush()
            self.sync()


# END_BLOCK_WAL_WRITER


# =============================================================================
# START_BLOCK_HELPERS
# =============================================================================

def create_wal_writer(
    file: BinaryIO,
    buffer_size: int = WALWriter.DEFAULT_BUFFER_SIZE,
    sync_on_write: bool = True
) -> WALWriter:
    """
    [START_CONTRACT_CREATE_WAL_WRITER]
    Intent: Фабрика для создания WALWriter.
    Input: file - файл; buffer_size - размер буфера; sync_on_write - fsync.
    Output: Готовый к работе WALWriter.
    [END_CONTRACT_CREATE_WAL_WRITER]
    """
    return WALWriter(file, buffer_size, sync_on_write)


# END_BLOCK_HELPERS