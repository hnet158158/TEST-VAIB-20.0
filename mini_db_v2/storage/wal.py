# START_MODULE_CONTRACT
# Module: mini_db_v2.storage.wal
# Intent: Write-Ahead Logging (WAL) для durability и crash recovery.
# Dependencies: dataclasses, enum, struct, threading, json, os, zlib
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: WALManager, WALRecord, WALRecordType, WALError, CheckpointData
# END_MODULE_MAP

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional, List, Dict, BinaryIO
from enum import IntEnum
from datetime import datetime
import struct
import threading
import os
import zlib
import json


# =============================================================================
# START_BLOCK_ENUMS
# =============================================================================

class WALRecordType(IntEnum):
    """
    Типы WAL records.
    
    Каждому типу соответствует числовой код для бинарной сериализации.
    """
    BEGIN = 1       # Начало транзакции
    COMMIT = 2      # Коммит транзакции
    ABORT = 3       # Откат транзакции
    INSERT = 4      # Вставка строки
    UPDATE = 5      # Обновление строки
    DELETE = 6      # Удаление строки
    CHECKPOINT = 7  # Checkpoint record


# END_BLOCK_ENUMS


# =============================================================================
# START_BLOCK_ERRORS
# =============================================================================

class WALError(Exception):
    """Базовая ошибка WAL."""
    pass


class WALWriteError(WALError):
    """Ошибка записи в WAL."""
    pass


class WALReadError(WALError):
    """Ошибка чтения WAL."""
    pass


class WALCorruptionError(WALError):
    """Ошибка целостности WAL (CRC mismatch)."""
    pass


class WALRecoveryError(WALError):
    """Ошибка восстановления из WAL."""
    pass


# END_BLOCK_ERRORS


# =============================================================================
# START_BLOCK_WAL_RECORD
# =============================================================================

@dataclass
class WALRecord:
    """
    [START_CONTRACT_WAL_RECORD]
    Intent: Запись WAL для логирования операций БД.
    Input: lsn - Log Sequence Number; xid - ID транзакции; type - тип записи.
    Output: Структура для хранения одной WAL записи.
    Note: LSN монотонно возрастает. CRC32 для integrity.
    [END_CONTRACT_WAL_RECORD]
    """
    lsn: int                      # Log Sequence Number (monotonic)
    xid: int                      # Transaction ID
    type: WALRecordType           # Type of record
    table_name: str = ""          # Table name (for INSERT/UPDATE/DELETE)
    row_id: int = -1              # Row ID (for INSERT/UPDATE/DELETE)
    old_data: Optional[Dict[str, Any]] = None  # For UNDO (UPDATE/DELETE)
    new_data: Optional[Dict[str, Any]] = None  # For REDO (INSERT/UPDATE)
    checkpoint_data: Optional[Dict[str, Any]] = None  # For CHECKPOINT
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_bytes(self) -> bytes:
        """
        [START_CONTRACT_RECORD_TO_BYTES]
        Intent: Сериализовать запись в бинарный формат.
        Input: Нет (использует поля записи).
        Output: Байтовое представление записи с CRC32.
        
        Format:
        | LSN (8) | XID (8) | Type (1) | TableLen (2) | Table (var) |
        | RowID (8) | DataLen (4) | Data (var) | CRC32 (4) |
        [END_CONTRACT_RECORD_TO_BYTES]
        """
        # Serialize data payload
        payload = {
            'old_data': self.old_data,
            'new_data': self.new_data,
            'checkpoint_data': self.checkpoint_data,
            'timestamp': self.timestamp.isoformat()
        }
        payload_bytes = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        table_bytes = self.table_name.encode('utf-8')
        
        # Build record without CRC
        # Format: LSN(8) + XID(8) + Type(1) + TableLen(2) + Table(var) +
        #         RowID(8) + PayloadLen(4) + Payload(var)
        header = struct.pack(
            '>QqBHqI',
            self.lsn,           # 8 bytes, unsigned long long
            self.xid,           # 8 bytes, signed long long (supports negative XID)
            self.type.value,    # 1 byte, unsigned char
            len(table_bytes),   # 2 bytes, unsigned short
            self.row_id,        # 8 bytes, signed long long
            len(payload_bytes)  # 4 bytes, unsigned int
        )
        
        record_without_crc = header + table_bytes + payload_bytes
        
        # Calculate CRC32
        crc = zlib.crc32(record_without_crc) & 0xFFFFFFFF
        
        # Return full record with CRC
        return record_without_crc + struct.pack('>I', crc)
    
    @classmethod
    def from_bytes(cls, data: bytes, offset: int = 0) -> tuple['WALRecord', int]:
        """
        [START_CONTRACT_RECORD_FROM_BYTES]
        Intent: Десериализовать запись из бинарного формата.
        Input: data - байтовый буфер; offset - смещение в буфере.
        Output: Кортеж (запись, новое смещение).
        Raises: WALCorruptionError - если CRC не совпадает.
        [END_CONTRACT_RECORD_FROM_BYTES]
        """
        # Minimum record size: header(31) + CRC(4) = 35 bytes
        if len(data) < offset + 35:
            raise WALReadError("Insufficient data for WAL record")
        
        # Parse header
        header_size = 31  # 8 + 8 + 1 + 2 + 8 + 4
        header = data[offset:offset + header_size]
        lsn, xid, type_val, table_len, row_id, payload_len = struct.unpack(
            '>QqBHqI', header
        )
        
        # Calculate total record size
        record_size = header_size + table_len + payload_len + 4  # +4 for CRC
        if len(data) < offset + record_size:
            raise WALReadError("Incomplete WAL record")
        
        # Extract table name and payload
        table_start = offset + header_size
        table_name = data[table_start:table_start + table_len].decode('utf-8')
        
        payload_start = table_start + table_len
        payload_bytes = data[payload_start:payload_start + payload_len]
        
        # Extract and verify CRC
        crc_offset = payload_start + payload_len
        stored_crc = struct.unpack('>I', data[crc_offset:crc_offset + 4])[0]
        calculated_crc = zlib.crc32(data[offset:crc_offset]) & 0xFFFFFFFF
        
        if stored_crc != calculated_crc:
            raise WALCorruptionError(
                f"CRC mismatch at LSN {lsn}: expected {calculated_crc:08x}, "
                f"got {stored_crc:08x}"
            )
        
        # Parse payload
        payload = json.loads(payload_bytes.decode('utf-8'))
        
        # Create record
        record = cls(
            lsn=lsn,
            xid=xid,
            type=WALRecordType(type_val),
            table_name=table_name,
            row_id=row_id,
            old_data=payload.get('old_data'),
            new_data=payload.get('new_data'),
            checkpoint_data=payload.get('checkpoint_data'),
            timestamp=datetime.fromisoformat(payload['timestamp'])
        )
        
        return record, offset + record_size


# END_BLOCK_WAL_RECORD


# =============================================================================
# START_BLOCK_CHECKPOINT_DATA
# =============================================================================

@dataclass
class CheckpointData:
    """
    [START_CONTRACT_CHECKPOINT_DATA]
    Intent: Данные checkpoint для recovery.
    Input: active_xids - активные транзакции; last_lsn - последний LSN.
    Output: Структура для хранения состояния checkpoint.
    [END_CONTRACT_CHECKPOINT_CHECKPOINT_DATA]
    """
    active_xids: List[int]           # Active transaction IDs
    last_lsn: int                    # Last LSN before checkpoint
    dirty_pages: List[Dict[str, Any]]  # Dirty page info (table, row_id)
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Конвертирует в словарь для сериализации."""
        return {
            'active_xids': self.active_xids,
            'last_lsn': self.last_lsn,
            'dirty_pages': self.dirty_pages,
            'timestamp': self.timestamp.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CheckpointData':
        """Создаёт из словаря."""
        return cls(
            active_xids=data['active_xids'],
            last_lsn=data['last_lsn'],
            dirty_pages=data['dirty_pages'],
            timestamp=datetime.fromisoformat(data['timestamp'])
        )


# END_BLOCK_CHECKPOINT_DATA


# =============================================================================
# START_BLOCK_WAL_MANAGER
# =============================================================================

class WALManager:
    """
    [START_CONTRACT_WAL_MANAGER]
    Intent: Управление Write-Ahead Log для durability.
    Input: Операции транзакций (BEGIN, COMMIT, ABORT, INSERT, UPDATE, DELETE).
    Output: LSN для каждой записи, гарантия durable commit.
    Note: fsync после каждого commit (configurable). LSN монотонно возрастает.
    [END_CONTRACT_WAL_MANAGER]
    """
    
    # WAL file magic number for identification
    WAL_MAGIC = b'WAL1'
    WAL_HEADER_SIZE = 8  # Magic (4) + Version (4)
    
    def __init__(self, wal_dir: str, sync_on_write: bool = True):
        """
        [START_CONTRACT_WM_INIT]
        Intent: Инициализация WAL менеджера.
        Input: wal_dir - директория для WAL файлов; sync_on_write - fsync при commit.
        Output: Готовый к работе WAL менеджер.
        [END_CONTRACT_WM_INIT]
        """
        self.wal_dir = wal_dir
        self.sync_on_write = sync_on_write
        self._next_lsn = 1
        self._current_file: Optional[BinaryIO] = None
        self._current_file_path: Optional[str] = None
        self._lock = threading.RLock()
        self._buffer: List[WALRecord] = []
        self._buffer_size = 0
        self._max_buffer_size = 64 * 1024  # 64KB buffer
        
        # Ensure directory exists
        os.makedirs(wal_dir, exist_ok=True)
        
        # Initialize or open WAL file
        self._init_wal_file()
    
    def _init_wal_file(self) -> None:
        """Инициализирует WAL файл."""
        self._current_file_path = os.path.join(self.wal_dir, 'wal.log')
        
        if os.path.exists(self._current_file_path):
            # Read existing WAL to get next LSN
            self._next_lsn = self._get_last_lsn() + 1
            self._current_file = open(self._current_file_path, 'ab')
        else:
            # Create new WAL file with header
            self._current_file = open(self._current_file_path, 'wb')
            self._write_header()
    
    def _write_header(self) -> None:
        """Записывает заголовок WAL файла."""
        header = self.WAL_MAGIC + struct.pack('>I', 1)  # Version 1
        self._current_file.write(header)
        self._current_file.flush()
    
    def _get_last_lsn(self) -> int:
        """
        [START_CONTRACT_GET_LAST_LSN]
        Intent: Получить последний LSN из существующего WAL файла.
        Input: Нет.
        Output: Последний LSN или 0 если файл пуст.
        [END_CONTRACT_GET_LAST_LSN]
        """
        try:
            from mini_db_v2.storage.wal_reader import WALReader
            with open(self._current_file_path, 'rb') as f:
                reader = WALReader(f, self.WAL_HEADER_SIZE)
                return reader.get_last_lsn()
        except FileNotFoundError:
            return 0
    
    @property
    def next_lsn(self) -> int:
        """Возвращает следующий LSN."""
        with self._lock:
            return self._next_lsn
    
    @property
    def current_file_path(self) -> Optional[str]:
        """Возвращает путь к текущему WAL файлу."""
        return self._current_file_path
    
    def _allocate_lsn(self) -> int:
        """Выделяет новый LSN."""
        lsn = self._next_lsn
        self._next_lsn += 1
        return lsn
    
    def _write_record(self, record: WALRecord) -> int:
        """
        [START_CONTRACT_WRITE_RECORD]
        Intent: Записать WAL record в буфер/файл.
        Input: record - запись для записи.
        Output: LSN записи.
        [END_CONTRACT_WRITE_RECORD]
        """
        with self._lock:
            data = record.to_bytes()
            self._buffer.append(record)
            self._buffer_size += len(data)
            
            # Flush if buffer is full
            if self._buffer_size >= self._max_buffer_size:
                self.flush()
            
            return record.lsn
    
    def begin_transaction(self, xid: int) -> int:
        """
        [START_CONTRACT_BEGIN_TRANSACTION]
        Intent: Записать BEGIN record для транзакции.
        Input: xid - ID транзакции.
        Output: LSN записи BEGIN.
        [END_CONTRACT_BEGIN_TRANSACTION]
        """
        with self._lock:
            record = WALRecord(
                lsn=self._allocate_lsn(),
                xid=xid,
                type=WALRecordType.BEGIN
            )
            return self._write_record(record)
    
    def commit_transaction(self, xid: int) -> int:
        """
        [START_CONTRACT_COMMIT_TRANSACTION]
        Intent: Записать COMMIT record и гарантировать durability.
        Input: xid - ID транзакции.
        Output: LSN записи COMMIT.
        Note: Выполняет fsync для гарантии durability.
        [END_CONTRACT_COMMIT_TRANSACTION]
        """
        with self._lock:
            record = WALRecord(
                lsn=self._allocate_lsn(),
                xid=xid,
                type=WALRecordType.COMMIT
            )
            lsn = self._write_record(record)
            
            # Force flush and sync for durability
            self.flush()
            if self.sync_on_write:
                self.sync()
            
            return lsn
    
    def abort_transaction(self, xid: int) -> int:
        """
        [START_CONTRACT_ABORT_TRANSACTION]
        Intent: Записать ABORT record для транзакции.
        Input: xid - ID транзакции.
        Output: LSN записи ABORT.
        [END_CONTRACT_ABORT_TRANSACTION]
        """
        with self._lock:
            record = WALRecord(
                lsn=self._allocate_lsn(),
                xid=xid,
                type=WALRecordType.ABORT
            )
            return self._write_record(record)
    
    def log_insert(self, xid: int, table: str, row: Dict[str, Any], 
                   row_id: int = -1) -> int:
        """
        [START_CONTRACT_LOG_INSERT]
        Intent: Записать INSERT record.
        Input: xid - ID транзакции; table - имя таблицы; row - данные строки.
        Output: LSN записи INSERT.
        [END_CONTRACT_LOG_INSERT]
        """
        with self._lock:
            record = WALRecord(
                lsn=self._allocate_lsn(),
                xid=xid,
                type=WALRecordType.INSERT,
                table_name=table,
                row_id=row_id,
                new_data=row
            )
            return self._write_record(record)
    
    def log_update(self, xid: int, table: str, old_row: Dict[str, Any],
                   new_row: Dict[str, Any], row_id: int = -1) -> int:
        """
        [START_CONTRACT_LOG_UPDATE]
        Intent: Записать UPDATE record.
        Input: xid - ID транзакции; table - имя таблицы; old_row/new_row - данные.
        Output: LSN записи UPDATE.
        [END_CONTRACT_LOG_UPDATE]
        """
        with self._lock:
            record = WALRecord(
                lsn=self._allocate_lsn(),
                xid=xid,
                type=WALRecordType.UPDATE,
                table_name=table,
                row_id=row_id,
                old_data=old_row,
                new_data=new_row
            )
            return self._write_record(record)
    
    def log_delete(self, xid: int, table: str, row: Dict[str, Any],
                   row_id: int = -1) -> int:
        """
        [START_CONTRACT_LOG_DELETE]
        Intent: Записать DELETE record.
        Input: xid - ID транзакции; table - имя таблицы; row - удалённые данные.
        Output: LSN записи DELETE.
        [END_CONTRACT_LOG_DELETE]
        """
        with self._lock:
            record = WALRecord(
                lsn=self._allocate_lsn(),
                xid=xid,
                type=WALRecordType.DELETE,
                table_name=table,
                row_id=row_id,
                old_data=row
            )
            return self._write_record(record)
    
    def checkpoint(self, active_xids: List[int], dirty_pages: List[Dict[str, Any]]) -> int:
        """
        [START_CONTRACT_CHECKPOINT]
        Intent: Записать CHECKPOINT record.
        Input: active_xids - активные транзакции; dirty_pages - грязные страницы.
        Output: LSN записи CHECKPOINT.
        Note: Checkpoint используется для recovery point.
        [END_CONTRACT_CHECKPOINT]
        """
        with self._lock:
            checkpoint_data = CheckpointData(
                active_xids=active_xids,
                last_lsn=self._next_lsn - 1,
                dirty_pages=dirty_pages
            )
            
            record = WALRecord(
                lsn=self._allocate_lsn(),
                xid=0,  # Checkpoint не принадлежит транзакции
                type=WALRecordType.CHECKPOINT,
                checkpoint_data=checkpoint_data.to_dict()
            )
            lsn = self._write_record(record)
            
            # Force flush and sync checkpoint
            self.flush()
            if self.sync_on_write:
                self.sync()
            
            return lsn
    
    def flush(self) -> None:
        """
        [START_CONTRACT_FLUSH]
        Intent: Сбросить буфер WAL на диск.
        Input: Нет.
        Output: Буфер записан в файл.
        [END_CONTRACT_FLUSH]
        """
        with self._lock:
            if not self._buffer:
                return
            
            for record in self._buffer:
                data = record.to_bytes()
                self._current_file.write(data)
            
            self._current_file.flush()
            self._buffer.clear()
            self._buffer_size = 0
    
    def sync(self) -> None:
        """
        [START_CONTRACT_SYNC]
        Intent: fsync WAL файл для гарантии durability.
        Input: Нет.
        Output: Данные гарантированно на диске.
        [END_CONTRACT_SYNC]
        """
        with self._lock:
            if self._current_file:
                os.fsync(self._current_file.fileno())
    
    def recover(self) -> List[WALRecord]:
        """
        [START_CONTRACT_RECOVER]
        Intent: Прочитать все WAL records для recovery.
        Input: Нет.
        Output: Список всех записей WAL в порядке LSN.
        Note: Используется для crash recovery (Phase 10).
        [END_CONTRACT_RECOVER]
        """
        with self._lock:
            # Flush any buffered data first
            self.flush()
            
            if not self._current_file_path or not os.path.exists(self._current_file_path):
                return []
            
            from mini_db_v2.storage.wal_reader import WALReader
            with open(self._current_file_path, 'rb') as f:
                reader = WALReader(f, self.WAL_HEADER_SIZE)
                return reader.read_all()
    
    def get_uncommitted_transactions(self) -> Dict[int, List[WALRecord]]:
        """
        [START_CONTRACT_GET_UNCOMMITTED]
        Intent: Получить незакоммиченные транзакции из WAL.
        Input: Нет.
        Output: Словарь {xid: [records]} для незакоммиченных транзакций.
        [END_CONTRACT_GET_UNCOMMITTED]
        """
        records = self.recover()
        
        # Group by transaction
        tx_records: Dict[int, List[WALRecord]] = {}
        committed: set = set()
        aborted: set = set()
        
        for record in records:
            xid = record.xid
            if xid == 0:  # Checkpoint
                continue
            
            if xid not in tx_records:
                tx_records[xid] = []
            tx_records[xid].append(record)
            
            if record.type == WALRecordType.COMMIT:
                committed.add(xid)
            elif record.type == WALRecordType.ABORT:
                aborted.add(xid)
        
        # Return only uncommitted and non-aborted
        return {
            xid: recs 
            for xid, recs in tx_records.items() 
            if xid not in committed and xid not in aborted
        }
    
    def close(self) -> None:
        """Закрывает WAL файл."""
        with self._lock:
            self.flush()
            if self._current_file:
                self._current_file.close()
                self._current_file = None
    
    def rotate(self) -> str:
        """
        [START_CONTRACT_ROTATE]
        Intent: Rotate WAL file (создать новый, старый переименовать).
        Input: Нет.
        Output: Путь к старому WAL файлу.
        Note: Используется после checkpoint для архивации.
        [END_CONTRACT_ROTATE]
        """
        with self._lock:
            self.flush()
            
            if self._current_file:
                self._current_file.close()
            
            # Rename current file with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            old_path = self._current_file_path
            new_path = os.path.join(self.wal_dir, f'wal_{timestamp}.log')
            
            if old_path and os.path.exists(old_path):
                os.rename(old_path, new_path)
            
            # Create new WAL file
            self._current_file_path = old_path
            self._current_file = open(self._current_file_path, 'wb')
            self._write_header()
            
            return new_path


# END_BLOCK_WAL_MANAGER


# =============================================================================
# START_BLOCK_HELPERS
# =============================================================================

def create_wal_manager(wal_dir: str, sync_on_write: bool = True) -> WALManager:
    """
    [START_CONTRACT_CREATE_WAL_MANAGER]
    Intent: Фабрика для создания WALManager.
    Input: wal_dir - директория для WAL; sync_on_write - fsync при commit.
    Output: Готовый к работе WALManager.
    [END_CONTRACT_CREATE_WAL_MANAGER]
    """
    return WALManager(wal_dir, sync_on_write)


# END_BLOCK_HELPERS