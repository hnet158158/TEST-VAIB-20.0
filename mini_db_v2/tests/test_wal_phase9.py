# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_wal_phase9
# Intent: Comprehensive test suite для Phase 9 - WAL (Write-Ahead Logging).
# Dependencies: pytest, tempfile, threading
# END_MODULE_CONTRACT

"""
Phase 9: WAL (Write-Ahead Logging) Test Suite

Тестирует:
- WAL Manager: begin_transaction, commit_transaction, abort_transaction
- WAL Manager: log_insert, log_update, log_delete, checkpoint
- WAL Writer: write, flush, sync, buffering
- WAL Reader: read_all, read_from, read_for_transaction, find_last_checkpoint
- WAL Record: serialization, CRC32 integrity
- LSN monotonic increasing
- Recovery functionality
"""

from __future__ import annotations
import pytest
import tempfile
import os
import threading
import time
import struct
import zlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

# WAL imports
from mini_db_v2.storage.wal import (
    WALManager,
    WALRecord,
    WALRecordType,
    WALError,
    WALWriteError,
    WALReadError,
    WALCorruptionError,
    WALRecoveryError,
    CheckpointData,
    create_wal_manager,
)

from mini_db_v2.storage.wal_writer import (
    WALWriter,
    WALWriteOptions,
    create_wal_writer,
)

from mini_db_v2.storage.wal_reader import (
    WALReader,
    WALReadOptions,
    WALIterator,
    create_wal_reader,
    read_wal_file,
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def temp_wal_dir():
    """Создаёт временную директорию для WAL файлов."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def wal_manager(temp_wal_dir):
    """Создаёт WALManager для тестов."""
    manager = WALManager(temp_wal_dir, sync_on_write=False)
    yield manager
    manager.close()


@pytest.fixture
def wal_manager_sync(temp_wal_dir):
    """Создаёт WALManager с sync_on_write=True."""
    manager = WALManager(temp_wal_dir, sync_on_write=True)
    yield manager
    manager.close()


# =============================================================================
# WAL RECORD TESTS
# =============================================================================

class TestWALRecord:
    """Тесты для WALRecord."""
    
    def test_wal_record_creation(self):
        """Тест создания WALRecord."""
        record = WALRecord(
            lsn=1,
            xid=100,
            type=WALRecordType.BEGIN
        )
        
        assert record.lsn == 1
        assert record.xid == 100
        assert record.type == WALRecordType.BEGIN
        assert record.table_name == ""
        assert record.row_id == -1
        assert record.old_data is None
        assert record.new_data is None
    
    def test_wal_record_with_data(self):
        """Тест создания WALRecord с данными."""
        record = WALRecord(
            lsn=2,
            xid=100,
            type=WALRecordType.INSERT,
            table_name="users",
            row_id=1,
            new_data={"id": 1, "name": "Alice"}
        )
        
        assert record.lsn == 2
        assert record.table_name == "users"
        assert record.new_data == {"id": 1, "name": "Alice"}
    
    def test_wal_record_serialization(self):
        """Тест сериализации/десериализации WALRecord."""
        original = WALRecord(
            lsn=1,
            xid=100,
            type=WALRecordType.INSERT,
            table_name="users",
            row_id=1,
            new_data={"id": 1, "name": "Alice"}
        )
        
        # Serialize
        data = original.to_bytes()
        assert isinstance(data, bytes)
        assert len(data) > 0
        
        # Deserialize
        record, offset = WALRecord.from_bytes(data)
        
        assert record.lsn == original.lsn
        assert record.xid == original.xid
        assert record.type == original.type
        assert record.table_name == original.table_name
        assert record.row_id == original.row_id
        assert record.new_data == original.new_data
    
    def test_wal_record_crc_integrity(self):
        """Тест CRC32 integrity check."""
        record = WALRecord(
            lsn=1,
            xid=100,
            type=WALRecordType.INSERT,
            table_name="users",
            new_data={"id": 1, "name": "Alice"}
        )
        
        data = record.to_bytes()
        
        # Verify CRC is correct
        record2, _ = WALRecord.from_bytes(data)
        assert record2.lsn == record.lsn
        
        # Corrupt the data
        corrupted = bytearray(data)
        corrupted[-10] ^= 0xFF  # Flip some bits
        
        # Should raise WALCorruptionError
        with pytest.raises(WALCorruptionError):
            WALRecord.from_bytes(bytes(corrupted))
    
    def test_wal_record_all_types(self):
        """Тест сериализации всех типов записей."""
        types_to_test = [
            WALRecordType.BEGIN,
            WALRecordType.COMMIT,
            WALRecordType.ABORT,
            WALRecordType.INSERT,
            WALRecordType.UPDATE,
            WALRecordType.DELETE,
            WALRecordType.CHECKPOINT,
        ]
        
        for record_type in types_to_test:
            record = WALRecord(
                lsn=1,
                xid=100,
                type=record_type,
                table_name="test" if record_type in [WALRecordType.INSERT, WALRecordType.UPDATE, WALRecordType.DELETE] else "",
            )
            
            data = record.to_bytes()
            record2, _ = WALRecord.from_bytes(data)
            
            assert record2.type == record_type
    
    def test_wal_record_with_unicode(self):
        """Тест записи с Unicode данными."""
        record = WALRecord(
            lsn=1,
            xid=100,
            type=WALRecordType.INSERT,
            table_name="пользователи",
            new_data={"имя": "Алиса", "город": "Москва"}
        )
        
        data = record.to_bytes()
        record2, _ = WALRecord.from_bytes(data)
        
        assert record2.table_name == "пользователи"
        assert record2.new_data["имя"] == "Алиса"
    
    def test_wal_record_with_null_values(self):
        """Тест записи с NULL значениями."""
        record = WALRecord(
            lsn=1,
            xid=100,
            type=WALRecordType.INSERT,
            table_name="users",
            new_data={"id": 1, "name": None, "email": None}
        )
        
        data = record.to_bytes()
        record2, _ = WALRecord.from_bytes(data)
        
        assert record2.new_data["name"] is None
        assert record2.new_data["email"] is None
    
    def test_wal_record_with_large_data(self):
        """Тест записи с большими данными."""
        large_data = {"data": "x" * 10000}
        record = WALRecord(
            lsn=1,
            xid=100,
            type=WALRecordType.INSERT,
            table_name="large_table",
            new_data=large_data
        )
        
        data = record.to_bytes()
        record2, _ = WALRecord.from_bytes(data)
        
        assert record2.new_data["data"] == "x" * 10000


# =============================================================================
# WAL MANAGER TESTS
# =============================================================================

class TestWALManager:
    """Тесты для WALManager."""
    
    def test_wal_manager_creation(self, temp_wal_dir):
        """Тест создания WALManager."""
        manager = WALManager(temp_wal_dir)
        
        assert manager.wal_dir == temp_wal_dir
        assert manager.next_lsn == 1
        assert manager.current_file_path is not None
        
        manager.close()
    
    def test_begin_transaction(self, wal_manager):
        """Тест begin_transaction."""
        lsn = wal_manager.begin_transaction(xid=100)
        
        assert lsn == 1
        assert wal_manager.next_lsn == 2
    
    def test_commit_transaction(self, wal_manager):
        """Тест commit_transaction."""
        wal_manager.begin_transaction(xid=100)
        lsn = wal_manager.commit_transaction(xid=100)
        
        assert lsn == 2
        assert wal_manager.next_lsn == 3
    
    def test_abort_transaction(self, wal_manager):
        """Тест abort_transaction."""
        wal_manager.begin_transaction(xid=100)
        lsn = wal_manager.abort_transaction(xid=100)
        
        assert lsn == 2
    
    def test_log_insert(self, wal_manager):
        """Тест log_insert."""
        wal_manager.begin_transaction(xid=100)
        lsn = wal_manager.log_insert(
            xid=100,
            table="users",
            row={"id": 1, "name": "Alice"}
        )
        
        assert lsn == 2
        
        # Verify record was written
        records = wal_manager.recover()
        assert len(records) == 2  # BEGIN + INSERT
    
    def test_log_update(self, wal_manager):
        """Тест log_update."""
        wal_manager.begin_transaction(xid=100)
        lsn = wal_manager.log_update(
            xid=100,
            table="users",
            old_row={"id": 1, "name": "Alice"},
            new_row={"id": 1, "name": "Bob"}
        )
        
        assert lsn == 2
        
        records = wal_manager.recover()
        update_record = records[1]
        assert update_record.type == WALRecordType.UPDATE
        assert update_record.old_data == {"id": 1, "name": "Alice"}
        assert update_record.new_data == {"id": 1, "name": "Bob"}
    
    def test_log_delete(self, wal_manager):
        """Тест log_delete."""
        wal_manager.begin_transaction(xid=100)
        lsn = wal_manager.log_delete(
            xid=100,
            table="users",
            row={"id": 1, "name": "Alice"}
        )
        
        assert lsn == 2
        
        records = wal_manager.recover()
        delete_record = records[1]
        assert delete_record.type == WALRecordType.DELETE
        assert delete_record.old_data == {"id": 1, "name": "Alice"}
    
    def test_checkpoint(self, wal_manager):
        """Тест checkpoint."""
        wal_manager.begin_transaction(xid=100)
        wal_manager.log_insert(xid=100, table="users", row={"id": 1})
        
        lsn = wal_manager.checkpoint(
            active_xids=[100],
            dirty_pages=[{"table": "users", "row_id": 1}]
        )
        
        assert lsn == 3
        
        records = wal_manager.recover()
        checkpoint_record = records[2]
        assert checkpoint_record.type == WALRecordType.CHECKPOINT
        assert checkpoint_record.checkpoint_data is not None
    
    def test_lsn_monotonic_increasing(self, wal_manager):
        """Тест монотонного возрастания LSN."""
        lsns = []
        
        for i in range(100):
            lsn = wal_manager.begin_transaction(xid=i)
            lsns.append(lsn)
        
        # Verify LSNs are strictly increasing
        for i in range(1, len(lsns)):
            assert lsns[i] > lsns[i-1], f"LSN {lsns[i]} not > {lsns[i-1]}"
    
    def test_recover(self, wal_manager):
        """Тест recovery."""
        # Write some records
        wal_manager.begin_transaction(xid=100)
        wal_manager.log_insert(xid=100, table="users", row={"id": 1})
        wal_manager.log_update(xid=100, table="users", old_row={"id": 1}, new_row={"id": 2})
        wal_manager.log_delete(xid=100, table="users", row={"id": 2})
        wal_manager.commit_transaction(xid=100)
        
        # Flush to ensure data is on disk
        wal_manager.flush()
        
        # Recover
        records = wal_manager.recover()
        
        assert len(records) == 5  # BEGIN + INSERT + UPDATE + DELETE + COMMIT
        assert records[0].type == WALRecordType.BEGIN
        assert records[1].type == WALRecordType.INSERT
        assert records[2].type == WALRecordType.UPDATE
        assert records[3].type == WALRecordType.DELETE
        assert records[4].type == WALRecordType.COMMIT
    
    def test_get_uncommitted_transactions(self, wal_manager):
        """Тест get_uncommitted_transactions."""
        # Transaction 100 - committed
        wal_manager.begin_transaction(xid=100)
        wal_manager.log_insert(xid=100, table="users", row={"id": 1})
        wal_manager.commit_transaction(xid=100)
        
        # Transaction 101 - not committed
        wal_manager.begin_transaction(xid=101)
        wal_manager.log_insert(xid=101, table="users", row={"id": 2})
        
        wal_manager.flush()
        
        uncommitted = wal_manager.get_uncommitted_transactions()
        
        assert 100 not in uncommitted
        assert 101 in uncommitted
    
    def test_flush(self, wal_manager):
        """Тест flush."""
        wal_manager.begin_transaction(xid=100)
        wal_manager.flush()
        
        # Verify data is on disk
        records = wal_manager.recover()
        assert len(records) == 1
    
    def test_sync(self, wal_manager_sync):
        """Тест sync (fsync)."""
        wal_manager_sync.begin_transaction(xid=100)
        wal_manager_sync.commit_transaction(xid=100)
        
        # sync() is called automatically on commit with sync_on_write=True
        # Verify data is durable
        records = wal_manager_sync.recover()
        assert len(records) == 2
    
    def test_rotate(self, wal_manager):
        """Тест rotate."""
        wal_manager.begin_transaction(xid=100)
        wal_manager.commit_transaction(xid=100)
        wal_manager.flush()
        
        old_path = wal_manager.rotate()
        
        assert os.path.exists(old_path)
        assert wal_manager.next_lsn == 3  # LSN counter preserved
    
    def test_multiple_transactions(self, wal_manager):
        """Тест нескольких транзакций."""
        # Transaction 100
        wal_manager.begin_transaction(xid=100)
        wal_manager.log_insert(xid=100, table="users", row={"id": 1})
        wal_manager.commit_transaction(xid=100)
        
        # Transaction 101
        wal_manager.begin_transaction(xid=101)
        wal_manager.log_insert(xid=101, table="users", row={"id": 2})
        wal_manager.commit_transaction(xid=101)
        
        wal_manager.flush()
        
        records = wal_manager.recover()
        
        # Should have 6 records: BEGIN, INSERT, COMMIT for each transaction
        assert len(records) == 6
    
    def test_persistence_across_reopen(self, temp_wal_dir):
        """Тест персистентности при повторном открытии."""
        # Write data
        manager1 = WALManager(temp_wal_dir)
        manager1.begin_transaction(xid=100)
        manager1.log_insert(xid=100, table="users", row={"id": 1})
        manager1.commit_transaction(xid=100)
        manager1.close()
        
        # Reopen and verify
        manager2 = WALManager(temp_wal_dir)
        records = manager2.recover()
        
        assert len(records) == 3  # BEGIN + INSERT + COMMIT
        assert manager2.next_lsn == 4
        
        manager2.close()


# =============================================================================
# WAL WRITER TESTS
# =============================================================================

class TestWALWriter:
    """Тесты для WALWriter."""
    
    def test_wal_writer_creation(self, temp_wal_dir):
        """Тест создания WALWriter."""
        file_path = os.path.join(temp_wal_dir, "test.wal")
        with open(file_path, 'wb') as f:
            writer = WALWriter(f)
            assert writer.bytes_written == 0
            assert writer.records_written == 0
    
    def test_write_single_record(self, temp_wal_dir):
        """Тест записи одной записи."""
        file_path = os.path.join(temp_wal_dir, "test.wal")
        
        with open(file_path, 'wb') as f:
            writer = WALWriter(f, sync_on_write=False)
            
            record = WALRecord(
                lsn=1,
                xid=100,
                type=WALRecordType.BEGIN
            )
            
            lsn = writer.write(record)
            
            assert lsn == 1
            assert writer.records_written == 1
            assert writer.bytes_written > 0
    
    def test_write_batch(self, temp_wal_dir):
        """Тест записи пакета записей."""
        file_path = os.path.join(temp_wal_dir, "test.wal")
        
        with open(file_path, 'wb') as f:
            writer = WALWriter(f, sync_on_write=False)
            
            records = [
                WALRecord(lsn=i, xid=100, type=WALRecordType.BEGIN)
                for i in range(1, 11)
            ]
            
            lsns = writer.write_batch(records)
            
            assert len(lsns) == 10
            assert writer.records_written == 10
    
    def test_flush(self, temp_wal_dir):
        """Тест flush."""
        file_path = os.path.join(temp_wal_dir, "test.wal")
        
        with open(file_path, 'wb') as f:
            writer = WALWriter(f, sync_on_write=False)
            
            record = WALRecord(lsn=1, xid=100, type=WALRecordType.BEGIN)
            writer.write(record, WALWriteOptions(flush=False))
            
            writer.flush()
        
        # Verify file has data
        assert os.path.getsize(file_path) > 0
    
    def test_sync(self, temp_wal_dir):
        """Тест sync."""
        file_path = os.path.join(temp_wal_dir, "test.wal")
        
        with open(file_path, 'wb') as f:
            writer = WALWriter(f, sync_on_write=True)
            
            record = WALRecord(lsn=1, xid=100, type=WALRecordType.COMMIT)
            writer.write(record, WALWriteOptions(sync=True))
        
        # File should exist and have data
        assert os.path.exists(file_path)
    
    def test_buffering(self, temp_wal_dir):
        """Тест буферизации."""
        file_path = os.path.join(temp_wal_dir, "test.wal")
        
        with open(file_path, 'wb') as f:
            writer = WALWriter(f, buffer_size=1024, sync_on_write=False)
            
            # Write small records without flush
            for i in range(10):
                record = WALRecord(lsn=i+1, xid=100, type=WALRecordType.BEGIN)
                writer.write(record, WALWriteOptions(flush=False))
            
            # Records should be in buffer
            assert writer.records_written == 10
            
            writer.flush()
        
        # Verify all records were written
        assert os.path.getsize(file_path) > 0
    
    def test_write_options(self, temp_wal_dir):
        """Тест опций записи."""
        file_path = os.path.join(temp_wal_dir, "test.wal")
        
        with open(file_path, 'wb') as f:
            writer = WALWriter(f, sync_on_write=False)
            
            # No buffer
            record = WALRecord(lsn=1, xid=100, type=WALRecordType.BEGIN)
            writer.write(record, WALWriteOptions(buffer=False, flush=True))
            
            # With buffer
            record2 = WALRecord(lsn=2, xid=100, type=WALRecordType.COMMIT)
            writer.write(record2, WALWriteOptions(buffer=True, flush=True))
            
            assert writer.records_written == 2


# =============================================================================
# WAL READER TESTS
# =============================================================================

class TestWALReader:
    """Тесты для WALReader."""
    
    @pytest.fixture
    def wal_file_with_records(self, temp_wal_dir):
        """Создаёт WAL файл с записями."""
        file_path = os.path.join(temp_wal_dir, "test.wal")
        
        # Write header
        with open(file_path, 'wb') as f:
            f.write(b'WAL1' + struct.pack('>I', 1))  # Magic + version
            
            # Write records
            records = [
                WALRecord(lsn=1, xid=100, type=WALRecordType.BEGIN),
                WALRecord(lsn=2, xid=100, type=WALRecordType.INSERT, table_name="users", new_data={"id": 1}),
                WALRecord(lsn=3, xid=100, type=WALRecordType.COMMIT),
                WALRecord(lsn=4, xid=101, type=WALRecordType.BEGIN),
                WALRecord(lsn=5, xid=101, type=WALRecordType.INSERT, table_name="users", new_data={"id": 2}),
                WALRecord(lsn=6, xid=101, type=WALRecordType.CHECKPOINT, checkpoint_data={"active_xids": [101]}),
            ]
            
            for record in records:
                f.write(record.to_bytes())
        
        return file_path
    
    def test_read_all(self, wal_file_with_records):
        """Тест read_all."""
        with open(wal_file_with_records, 'rb') as f:
            reader = WALReader(f)
            records = reader.read_all()
            
            assert len(records) == 6
            assert records[0].type == WALRecordType.BEGIN
            assert records[2].type == WALRecordType.COMMIT
    
    def test_read_from(self, wal_file_with_records):
        """Тест read_from."""
        with open(wal_file_with_records, 'rb') as f:
            reader = WALReader(f)
            records = reader.read_from(lsn=4)
            
            assert len(records) == 3  # LSN 4, 5, 6
            assert records[0].lsn == 4
    
    def test_read_for_transaction(self, wal_file_with_records):
        """Тест read_for_transaction."""
        with open(wal_file_with_records, 'rb') as f:
            reader = WALReader(f)
            
            # Transaction 100
            records_100 = reader.read_for_transaction(xid=100)
            assert len(records_100) == 3  # BEGIN, INSERT, COMMIT
            
            # Transaction 101
            records_101 = reader.read_for_transaction(xid=101)
            assert len(records_101) == 3  # BEGIN, INSERT, CHECKPOINT
    
    def test_read_by_type(self, wal_file_with_records):
        """Тест read_by_type."""
        with open(wal_file_with_records, 'rb') as f:
            reader = WALReader(f)
            
            begins = reader.read_by_type(WALRecordType.BEGIN)
            assert len(begins) == 2
            
            inserts = reader.read_by_type(WALRecordType.INSERT)
            assert len(inserts) == 2
    
    def test_find_last_checkpoint(self, wal_file_with_records):
        """Тест find_last_checkpoint."""
        with open(wal_file_with_records, 'rb') as f:
            reader = WALReader(f)
            checkpoint = reader.find_last_checkpoint()
            
            assert checkpoint is not None
            assert checkpoint.type == WALRecordType.CHECKPOINT
            assert checkpoint.lsn == 6
    
    def test_get_last_lsn(self, wal_file_with_records):
        """Тест get_last_lsn."""
        with open(wal_file_with_records, 'rb') as f:
            reader = WALReader(f)
            last_lsn = reader.get_last_lsn()
            
            assert last_lsn == 6
    
    def test_iterate(self, wal_file_with_records):
        """Тест iterate."""
        with open(wal_file_with_records, 'rb') as f:
            reader = WALReader(f)
            
            records = list(reader.iterate())
            assert len(records) == 6
    
    def test_seek_to_lsn(self, wal_file_with_records):
        """Тест seek_to_lsn."""
        with open(wal_file_with_records, 'rb') as f:
            reader = WALReader(f)
            
            found = reader.seek_to_lsn(3)
            assert found is True
            
            found = reader.seek_to_lsn(999)
            assert found is False
    
    def test_get_transaction_summary(self, wal_file_with_records):
        """Тест get_transaction_summary."""
        with open(wal_file_with_records, 'rb') as f:
            reader = WALReader(f)
            summary = reader.get_transaction_summary()
            
            assert summary['total_records'] == 6
            assert 100 in summary['committed']
            assert 101 in summary['transactions']
    
    def test_read_options_skip_corrupted(self, temp_wal_dir):
        """Тест skip_corrupted опции."""
        file_path = os.path.join(temp_wal_dir, "test.wal")
        
        with open(file_path, 'wb') as f:
            f.write(b'WAL1' + struct.pack('>I', 1))
            
            # Write valid record
            record = WALRecord(lsn=1, xid=100, type=WALRecordType.BEGIN)
            f.write(record.to_bytes())
            
            # Write corrupted data
            f.write(b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')
            
            # Write another valid record
            record2 = WALRecord(lsn=2, xid=100, type=WALRecordType.COMMIT)
            f.write(record2.to_bytes())
        
        with open(file_path, 'rb') as f:
            reader = WALReader(f)
            
            # With skip_corrupted=True
            records = reader.read_all(WALReadOptions(skip_corrupted=True))
            assert len(records) >= 1  # At least the first valid record


# =============================================================================
# WAL ITERATOR TESTS
# =============================================================================

class TestWALIterator:
    """Тесты для WALIterator."""
    
    def test_iterator_basic(self, temp_wal_dir):
        """Тест базовой работы итератора."""
        file_path = os.path.join(temp_wal_dir, "test.wal")
        
        with open(file_path, 'wb') as f:
            f.write(b'WAL1' + struct.pack('>I', 1))
            
            for i in range(5):
                record = WALRecord(lsn=i+1, xid=100, type=WALRecordType.BEGIN)
                f.write(record.to_bytes())
        
        with open(file_path, 'rb') as f:
            reader = WALReader(f)
            iterator = WALIterator(reader)
            
            records = list(iterator)
            assert len(records) == 5


# =============================================================================
# CHECKPOINT DATA TESTS
# =============================================================================

class TestCheckpointData:
    """Тесты для CheckpointData."""
    
    def test_checkpoint_data_creation(self):
        """Тест создания CheckpointData."""
        checkpoint = CheckpointData(
            active_xids=[100, 101],
            last_lsn=50,
            dirty_pages=[{"table": "users", "row_id": 1}]
        )
        
        assert checkpoint.active_xids == [100, 101]
        assert checkpoint.last_lsn == 50
        assert len(checkpoint.dirty_pages) == 1
    
    def test_checkpoint_data_serialization(self):
        """Тест сериализации CheckpointData."""
        original = CheckpointData(
            active_xids=[100, 101],
            last_lsn=50,
            dirty_pages=[{"table": "users", "row_id": 1}]
        )
        
        data = original.to_dict()
        restored = CheckpointData.from_dict(data)
        
        assert restored.active_xids == original.active_xids
        assert restored.last_lsn == original.last_lsn
        assert restored.dirty_pages == original.dirty_pages


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

class TestWALIntegration:
    """Интеграционные тесты WAL."""
    
    def test_full_transaction_lifecycle(self, wal_manager):
        """Тест полного жизненного цикла транзакции."""
        xid = 100
        
        # BEGIN
        lsn1 = wal_manager.begin_transaction(xid)
        assert lsn1 == 1
        
        # INSERT
        lsn2 = wal_manager.log_insert(
            xid=xid,
            table="users",
            row={"id": 1, "name": "Alice"}
        )
        assert lsn2 == 2
        
        # UPDATE
        lsn3 = wal_manager.log_update(
            xid=xid,
            table="users",
            old_row={"id": 1, "name": "Alice"},
            new_row={"id": 1, "name": "Bob"}
        )
        assert lsn3 == 3
        
        # DELETE
        lsn4 = wal_manager.log_delete(
            xid=xid,
            table="users",
            row={"id": 1, "name": "Bob"}
        )
        assert lsn4 == 4
        
        # COMMIT
        lsn5 = wal_manager.commit_transaction(xid)
        assert lsn5 == 5
        
        # Verify all records
        wal_manager.flush()
        records = wal_manager.recover()
        
        assert len(records) == 5
        assert all(r.xid == xid for r in records)
    
    def test_concurrent_transactions(self, wal_manager):
        """Тест конкурентных транзакций."""
        results = []
        
        def write_transaction(xid):
            wal_manager.begin_transaction(xid)
            wal_manager.log_insert(xid=xid, table="users", row={"id": xid})
            wal_manager.commit_transaction(xid)
            results.append(xid)
        
        threads = [
            threading.Thread(target=write_transaction, args=(i,))
            for i in range(100, 110)
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(results) == 10
        
        wal_manager.flush()
        records = wal_manager.recover()
        
        # Should have 30 records: BEGIN + INSERT + COMMIT for each transaction
        assert len(records) == 30
    
    def test_crash_recovery_simulation(self, temp_wal_dir):
        """Тест симуляции crash recovery."""
        # Write some data
        manager1 = WALManager(temp_wal_dir, sync_on_write=True)
        
        # Committed transaction
        manager1.begin_transaction(xid=100)
        manager1.log_insert(xid=100, table="users", row={"id": 1})
        manager1.commit_transaction(xid=100)
        
        # Uncommitted transaction (simulating crash)
        manager1.begin_transaction(xid=101)
        manager1.log_insert(xid=101, table="users", row={"id": 2})
        manager1.flush()
        # No commit for xid=101
        
        manager1.close()
        
        # Reopen and recover
        manager2 = WALManager(temp_wal_dir)
        uncommitted = manager2.get_uncommitted_transactions()
        
        assert 100 not in uncommitted  # Committed
        assert 101 in uncommitted  # Not committed
        
        manager2.close()


# =============================================================================
# ADVERSARIAL TESTS
# =============================================================================

class TestWALAdversarial:
    """Адверсарные тесты WAL."""
    
    def test_empty_wal_file(self, temp_wal_dir):
        """Тест пустого WAL файла."""
        manager = WALManager(temp_wal_dir)
        records = manager.recover()
        
        assert len(records) == 0
        manager.close()
    
    def test_very_large_record(self, wal_manager):
        """Тест очень большой записи."""
        large_data = {"data": "x" * 100000}
        
        wal_manager.begin_transaction(xid=100)
        lsn = wal_manager.log_insert(xid=100, table="large", row=large_data)
        wal_manager.commit_transaction(xid=100)
        
        wal_manager.flush()
        records = wal_manager.recover()
        
        assert len(records) == 3
        assert records[1].new_data["data"] == "x" * 100000
    
    def test_special_characters_in_table_name(self, wal_manager):
        """Тест специальных символов в имени таблицы."""
        table_name = "table_with_special-chars_123"
        
        wal_manager.begin_transaction(xid=100)
        wal_manager.log_insert(xid=100, table=table_name, row={"id": 1})
        wal_manager.commit_transaction(xid=100)
        
        wal_manager.flush()
        records = wal_manager.recover()
        
        assert records[1].table_name == table_name
    
    def test_unicode_data(self, wal_manager):
        """Тест Unicode данных."""
        data = {
            "name": "Алиса",
            "city": "Москва",
            "emoji": "😀🎉"
        }
        
        wal_manager.begin_transaction(xid=100)
        wal_manager.log_insert(xid=100, table="users", row=data)
        wal_manager.commit_transaction(xid=100)
        
        wal_manager.flush()
        records = wal_manager.recover()
        
        assert records[1].new_data == data
    
    def test_null_and_empty_values(self, wal_manager):
        """Тест NULL и пустых значений."""
        data = {
            "null_value": None,
            "empty_string": "",
            "zero": 0,
            "false": False
        }
        
        wal_manager.begin_transaction(xid=100)
        wal_manager.log_insert(xid=100, table="users", row=data)
        wal_manager.commit_transaction(xid=100)
        
        wal_manager.flush()
        records = wal_manager.recover()
        
        assert records[1].new_data["null_value"] is None
        assert records[1].new_data["empty_string"] == ""
        assert records[1].new_data["zero"] == 0
        assert records[1].new_data["false"] is False
    
    def test_negative_xid(self, wal_manager):
        """Тест отрицательного XID."""
        # Should work (XID is just an identifier)
        wal_manager.begin_transaction(xid=-1)
        wal_manager.commit_transaction(xid=-1)
        
        wal_manager.flush()
        records = wal_manager.recover()
        
        assert len(records) == 2
        assert records[0].xid == -1
    
    def test_large_xid(self, wal_manager):
        """Тест большого XID."""
        large_xid = 2**63 - 1
        
        wal_manager.begin_transaction(xid=large_xid)
        wal_manager.commit_transaction(xid=large_xid)
        
        wal_manager.flush()
        records = wal_manager.recover()
        
        assert records[0].xid == large_xid
    
    def test_zero_xid(self, wal_manager):
        """Тест нулевого XID."""
        # XID=0 is reserved for checkpoint, but should work for transactions
        wal_manager.begin_transaction(xid=0)
        wal_manager.commit_transaction(xid=0)
        
        wal_manager.flush()
        records = wal_manager.recover()
        
        assert len(records) == 2
    
    def test_many_small_transactions(self, wal_manager):
        """Тест множества маленьких транзакций."""
        for i in range(100):
            wal_manager.begin_transaction(xid=i)
            wal_manager.log_insert(xid=i, table="users", row={"id": i})
            wal_manager.commit_transaction(xid=i)
        
        wal_manager.flush()
        records = wal_manager.recover()
        
        # 3 records per transaction: BEGIN + INSERT + COMMIT
        assert len(records) == 300
    
    def test_corrupted_wal_file(self, temp_wal_dir):
        """Тест повреждённого WAL файла."""
        file_path = os.path.join(temp_wal_dir, "wal.log")
        
        # Write corrupted data
        with open(file_path, 'wb') as f:
            f.write(b'WAL1' + struct.pack('>I', 1))
            f.write(b'\x00' * 100)  # Garbage
        
        manager = WALManager(temp_wal_dir)
        
        # Should handle corruption gracefully
        records = manager.recover()
        # May be empty or partial
        assert isinstance(records, list)
        
        manager.close()


# =============================================================================
# CHECKPOINT TESTS (FOR PHASE 10 PREPARATION)
# =============================================================================

class TestWALCheckpoint:
    """Тесты checkpoint для подготовки к Phase 10."""
    
    def test_checkpoint_with_active_transactions(self, wal_manager):
        """Тест checkpoint с активными транзакциями."""
        # Transaction 100 - active
        wal_manager.begin_transaction(xid=100)
        wal_manager.log_insert(xid=100, table="users", row={"id": 1})
        
        # Transaction 101 - active
        wal_manager.begin_transaction(xid=101)
        wal_manager.log_insert(xid=101, table="users", row={"id": 2})
        
        # Checkpoint
        lsn = wal_manager.checkpoint(
            active_xids=[100, 101],
            dirty_pages=[
                {"table": "users", "row_id": 1},
                {"table": "users", "row_id": 2}
            ]
        )
        
        wal_manager.flush()
        records = wal_manager.recover()
        
        checkpoint_record = [r for r in records if r.type == WALRecordType.CHECKPOINT][0]
        assert checkpoint_record.checkpoint_data["active_xids"] == [100, 101]
    
    def test_multiple_checkpoints(self, wal_manager):
        """Тест множественных checkpoint'ов."""
        for i in range(3):
            wal_manager.begin_transaction(xid=100+i)
            wal_manager.checkpoint(active_xids=[100+i], dirty_pages=[])
            wal_manager.commit_transaction(xid=100+i)
        
        wal_manager.flush()
        
        checkpoints = [
            r for r in wal_manager.recover() 
            if r.type == WALRecordType.CHECKPOINT
        ]
        
        assert len(checkpoints) == 3


# =============================================================================
# PERFORMANCE TESTS
# =============================================================================

class TestWALPerformance:
    """Тесты производительности WAL."""
    
    def test_write_throughput(self, wal_manager):
        """Тест пропускной способности записи."""
        import time
        
        start = time.time()
        
        for i in range(1000):
            wal_manager.begin_transaction(xid=i)
            wal_manager.log_insert(xid=i, table="users", row={"id": i})
            wal_manager.commit_transaction(xid=i)
        
        wal_manager.flush()
        
        elapsed = time.time() - start
        
        # Should complete 1000 transactions in reasonable time
        assert elapsed < 10.0, f"Write throughput too slow: {elapsed:.2f}s for 1000 transactions"
    
    def test_read_throughput(self, wal_manager):
        """Тест пропускной способности чтения."""
        # Write 1000 records
        for i in range(1000):
            wal_manager.begin_transaction(xid=i)
            wal_manager.log_insert(xid=i, table="users", row={"id": i})
            wal_manager.commit_transaction(xid=i)
        
        wal_manager.flush()
        
        import time
        start = time.time()
        
        records = wal_manager.recover()
        
        elapsed = time.time() - start
        
        assert len(records) == 3000  # 3 records per transaction
        assert elapsed < 5.0, f"Read throughput too slow: {elapsed:.2f}s for 3000 records"


# =============================================================================
# HELPER FUNCTION TESTS
# =============================================================================

class TestHelperFunctions:
    """Тесты вспомогательных функций."""
    
    def test_create_wal_manager(self, temp_wal_dir):
        """Тест create_wal_manager."""
        manager = create_wal_manager(temp_wal_dir)
        
        assert isinstance(manager, WALManager)
        manager.close()
    
    def test_create_wal_writer(self, temp_wal_dir):
        """Тест create_wal_writer."""
        file_path = os.path.join(temp_wal_dir, "test.wal")
        
        with open(file_path, 'wb') as f:
            writer = create_wal_writer(f)
            assert isinstance(writer, WALWriter)
    
    def test_create_wal_reader(self, temp_wal_dir):
        """Тест create_wal_reader."""
        file_path = os.path.join(temp_wal_dir, "test.wal")
        
        # Create file with header
        with open(file_path, 'wb') as f:
            f.write(b'WAL1' + struct.pack('>I', 1))
        
        with open(file_path, 'rb') as f:
            reader = create_wal_reader(f)
            assert isinstance(reader, WALReader)
    
    def test_read_wal_file(self, temp_wal_dir):
        """Тест read_wal_file."""
        file_path = os.path.join(temp_wal_dir, "test.wal")
        
        # Create file with records
        with open(file_path, 'wb') as f:
            f.write(b'WAL1' + struct.pack('>I', 1))
            
            record = WALRecord(lsn=1, xid=100, type=WALRecordType.BEGIN)
            f.write(record.to_bytes())
        
        records = read_wal_file(file_path)
        
        assert len(records) == 1


# =============================================================================
# RUN TESTS
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-p", "no:asyncio"])