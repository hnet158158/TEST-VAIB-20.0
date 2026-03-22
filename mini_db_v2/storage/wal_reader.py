# START_MODULE_CONTRACT
# Module: mini_db_v2.storage.wal_reader
# Intent: WAL Reader для чтения и парсинга WAL записей.
# Dependencies: threading, typing, dataclasses
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: WALReader, WALReadOptions, WALIterator
# END_MODULE_MAP

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List, BinaryIO, Iterator, Callable
import threading
import os

from mini_db_v2.storage.wal import (
    WALRecord,
    WALRecordType,
    WALError,
    WALReadError,
    WALCorruptionError,
    CheckpointData,
)


# =============================================================================
# START_BLOCK_READ_OPTIONS
# =============================================================================

@dataclass
class WALReadOptions:
    """
    [START_CONTRACT_WAL_READ_OPTIONS]
    Intent: Опции чтения WAL.
    Input: skip_corrupted - пропускать повреждённые записи; verify_crc - проверять CRC.
    Output: Конфигурация для операции чтения.
    [END_CONTRACT_WAL_READ_OPTIONS]
    """
    skip_corrupted: bool = True    # Skip corrupted records
    verify_crc: bool = True        # Verify CRC32 checksum
    stop_on_checkpoint: bool = False  # Stop at checkpoint record


# END_BLOCK_READ_OPTIONS


# =============================================================================
# START_BLOCK_WAL_READER
# =============================================================================

class WALReader:
    """
    [START_CONTRACT_WAL_READER]
    Intent: Читатель WAL записей с поддержкой recovery.
    Input: WAL файл для чтения.
    Output: Список WALRecord или итератор.
    Note: Поддерживает чтение с определённого LSN, фильтрацию по XID.
    [END_CONTRACT_WAL_READER]
    """
    
    READ_CHUNK_SIZE = 8192  # 8KB chunks
    
    def __init__(self, file: BinaryIO, header_size: int = 8):
        """
        [START_CONTRACT_WR_INIT]
        Intent: Инициализация WAL Reader.
        Input: file - открытый файл; header_size - размер заголовка файла.
        Output: Готовый к работе reader.
        [END_CONTRACT_WR_INIT]
        """
        self._file = file
        self._header_size = header_size
        self._lock = threading.RLock()
        self._position = header_size
        self._records_read = 0
    
    @property
    def records_read(self) -> int:
        """Возвращает количество прочитанных записей."""
        with self._lock:
            return self._records_read
    
    @property
    def position(self) -> int:
        """Возвращает текущую позицию в файле."""
        with self._lock:
            return self._position
    
    def read_all(
        self,
        options: Optional[WALReadOptions] = None
    ) -> List[WALRecord]:
        """
        [START_CONTRACT_WR_READ_ALL]
        Intent: Прочитать все WAL records из файла.
        Input: options - опции чтения.
        Output: Список всех записей WAL.
        [END_CONTRACT_WR_READ_ALL]
        """
        if options is None:
            options = WALReadOptions()
        
        records = []
        
        with self._lock:
            # Seek to start (after header)
            self._file.seek(self._header_size)
            
            # Read all records
            for record in self._iterate_records(options):
                records.append(record)
                
                if options.stop_on_checkpoint and record.type == WALRecordType.CHECKPOINT:
                    break
        
        return records
    
    def read_from(
        self,
        lsn: int,
        options: Optional[WALReadOptions] = None
    ) -> List[WALRecord]:
        """
        [START_CONTRACT_WR_READ_FROM]
        Intent: Прочитать WAL records с указанного LSN.
        Input: lsn - начальный LSN; options - опции чтения.
        Output: Список записей с LSN >= указанного.
        [END_CONTRACT_WR_READ_FROM]
        """
        if options is None:
            options = WALReadOptions()
        
        records = []
        
        with self._lock:
            # Read all and filter by LSN
            self._file.seek(self._header_size)
            
            for record in self._iterate_records(options):
                if record.lsn >= lsn:
                    records.append(record)
        
        return records
    
    def read_for_transaction(
        self,
        xid: int,
        options: Optional[WALReadOptions] = None
    ) -> List[WALRecord]:
        """
        [START_CONTRACT_WR_READ_FOR_TX]
        Intent: Прочитать WAL records для конкретной транзакции.
        Input: xid - ID транзакции; options - опции чтения.
        Output: Список записей для указанной транзакции.
        [END_CONTRACT_WR_READ_FOR_TX]
        """
        if options is None:
            options = WALReadOptions()
        
        records = []
        
        with self._lock:
            self._file.seek(self._header_size)
            
            for record in self._iterate_records(options):
                if record.xid == xid:
                    records.append(record)
        
        return records
    
    def read_by_type(
        self,
        record_type: WALRecordType,
        options: Optional[WALReadOptions] = None
    ) -> List[WALRecord]:
        """
        [START_CONTRACT_WR_READ_BY_TYPE]
        Intent: Прочитать WAL records определённого типа.
        Input: record_type - тип записи; options - опции чтения.
        Output: Список записей указанного типа.
        [END_CONTRACT_WR_READ_BY_TYPE]
        """
        if options is None:
            options = WALReadOptions()
        
        records = []
        
        with self._lock:
            self._file.seek(self._header_size)
            
            for record in self._iterate_records(options):
                if record.type == record_type:
                    records.append(record)
        
        return records
    
    def find_last_checkpoint(self) -> Optional[WALRecord]:
        """
        [START_CONTRACT_WR_FIND_LAST_CHECKPOINT]
        Intent: Найти последнюю checkpoint запись.
        Input: Нет.
        Output: Последняя checkpoint запись или None.
        [END_CONTRACT_WR_FIND_LAST_CHECKPOINT]
        """
        checkpoint = None
        
        with self._lock:
            self._file.seek(self._header_size)
            
            for record in self._iterate_records(WALReadOptions()):
                if record.type == WALRecordType.CHECKPOINT:
                    checkpoint = record
        
        return checkpoint
    
    def get_last_lsn(self) -> int:
        """
        [START_CONTRACT_WR_GET_LAST_LSN]
        Intent: Получить последний LSN из WAL.
        Input: Нет.
        Output: Последний LSN или 0 если WAL пуст.
        [END_CONTRACT_WR_GET_LAST_LSN]
        """
        last_lsn = 0
        
        with self._lock:
            self._file.seek(self._header_size)
            
            for record in self._iterate_records(WALReadOptions()):
                last_lsn = max(last_lsn, record.lsn)
        
        return last_lsn
    
    def iterate(
        self,
        options: Optional[WALReadOptions] = None
    ) -> Iterator[WALRecord]:
        """
        [START_CONTRACT_WR_ITERATE]
        Intent: Итератор по WAL records.
        Input: options - опции чтения.
        Output: Итератор по записям WAL.
        [END_CONTRACT_WR_ITERATE]
        """
        if options is None:
            options = WALReadOptions()
        
        with self._lock:
            self._file.seek(self._header_size)
            
            for record in self._iterate_records(options):
                yield record
    
    def _iterate_records(
        self,
        options: WALReadOptions
    ) -> Iterator[WALRecord]:
        """
        Внутренний итератор по записям.
        """
        buffer = b''  # Buffer for incomplete records at chunk boundaries
        
        while True:
            # Read chunk and combine with remaining buffer
            chunk = self._file.read(self.READ_CHUNK_SIZE * 10)
            if not chunk:
                # Process any remaining data in buffer
                if buffer:
                    offset = 0
                    while offset < len(buffer):
                        try:
                            record, new_offset = WALRecord.from_bytes(buffer, offset)
                            self._records_read += 1
                            self._position = self._file.tell() - len(buffer) + new_offset
                            yield record
                            offset = new_offset
                        except (WALCorruptionError, WALReadError):
                            break
                break
            
            # Combine buffer with new chunk
            data = buffer + chunk
            buffer = b''
            
            # Parse records from data
            offset = 0
            while offset < len(data):
                try:
                    record, new_offset = WALRecord.from_bytes(data, offset)
                    self._records_read += 1
                    self._position = self._file.tell() - len(data) + new_offset
                    yield record
                    offset = new_offset
                except WALCorruptionError as e:
                    if options.skip_corrupted:
                        # Try to find next valid record
                        offset += 1
                        continue
                    raise
                except WALReadError:
                    # Incomplete record at chunk boundary - save remainder for next iteration
                    buffer = data[offset:]
                    break
    
    def seek_to_lsn(self, lsn: int) -> bool:
        """
        [START_CONTRACT_WR_SEEK_TO_LSN]
        Intent: Найти позицию записи с указанным LSN.
        Input: lsn - искомый LSN.
        Output: True если запись найдена.
        [END_CONTRACT_WR_SEEK_TO_LSN]
        """
        with self._lock:
            self._file.seek(self._header_size)
            
            for record in self._iterate_records(WALReadOptions()):
                if record.lsn == lsn:
                    return True
                if record.lsn > lsn:
                    # Passed the target LSN
                    return False
            
            return False
    
    def get_transaction_summary(self) -> dict:
        """
        [START_CONTRACT_WR_GET_TX_SUMMARY]
        Intent: Получить сводку по транзакциям в WAL.
        Input: Нет.
        Output: Словарь со статистикой транзакций.
        [END_CONTRACT_WR_GET_TX_SUMMARY]
        """
        summary = {
            'total_records': 0,
            'transactions': set(),
            'committed': set(),
            'aborted': set(),
            'active': set(),
            'by_type': {t.name: 0 for t in WALRecordType}
        }
        
        with self._lock:
            self._file.seek(self._header_size)
            
            for record in self._iterate_records(WALReadOptions()):
                summary['total_records'] += 1
                summary['by_type'][record.type.name] += 1
                
                if record.xid > 0:  # Skip checkpoint (xid=0)
                    summary['transactions'].add(record.xid)
                    
                    if record.type == WALRecordType.COMMIT:
                        summary['committed'].add(record.xid)
                    elif record.type == WALRecordType.ABORT:
                        summary['aborted'].add(record.xid)
        
        # Calculate active (not committed, not aborted)
        summary['active'] = summary['transactions'] - summary['committed'] - summary['aborted']
        
        # Convert sets to lists for JSON serialization
        summary['transactions'] = sorted(summary['transactions'])
        summary['committed'] = sorted(summary['committed'])
        summary['aborted'] = sorted(summary['aborted'])
        summary['active'] = sorted(summary['active'])
        
        return summary


# END_BLOCK_WAL_READER


# =============================================================================
# START_BLOCK_WAL_ITERATOR
# =============================================================================

class WALIterator:
    """
    [START_CONTRACT_WAL_ITERATOR]
    Intent: Итератор для последовательного чтения WAL.
    Input: Функция для чтения следующей записи.
    Output: Последовательный доступ к записям.
    [END_CONTRACT_WAL_ITERATOR]
    """
    
    def __init__(
        self,
        reader: WALReader,
        options: Optional[WALReadOptions] = None
    ):
        self._reader = reader
        self._options = options or WALReadOptions()
        self._current_lsn = 0
    
    def __iter__(self) -> 'WALIterator':
        return self
    
    def __next__(self) -> WALRecord:
        for record in self._reader.iterate(self._options):
            if record.lsn > self._current_lsn:
                self._current_lsn = record.lsn
                return record
        raise StopIteration


# END_BLOCK_WAL_ITERATOR


# =============================================================================
# START_BLOCK_HELPERS
# =============================================================================

def create_wal_reader(
    file: BinaryIO,
    header_size: int = 8
) -> WALReader:
    """
    [START_CONTRACT_CREATE_WAL_READER]
    Intent: Фабрика для создания WALReader.
    Input: file - файл; header_size - размер заголовка.
    Output: Готовый к работе WALReader.
    [END_CONTRACT_CREATE_WAL_READER]
    """
    return WALReader(file, header_size)


def read_wal_file(
    file_path: str,
    options: Optional[WALReadOptions] = None
) -> List[WALRecord]:
    """
    [START_CONTRACT_READ_WAL_FILE]
    Intent: Прочитать все записи из WAL файла.
    Input: file_path - путь к файлу; options - опции чтения.
    Output: Список всех записей.
    [END_CONTRACT_READ_WAL_FILE]
    """
    with open(file_path, 'rb') as f:
        reader = WALReader(f)
        return reader.read_all(options)


# END_BLOCK_HELPERS