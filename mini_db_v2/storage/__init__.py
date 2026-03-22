# START_MODULE_CONTRACT
# Module: mini_db_v2.storage
# Intent: Storage module - хранение данных, таблицы, индексы, MVCC, WAL.
# Dependencies: dataclasses, typing, threading
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: Database, Table, ColumnDef, Row, BTree, BTreeNode, MVCC classes, WAL classes
# END_MODULE_MAP

from mini_db_v2.storage.database import (
    Database,
    DatabaseError
)

from mini_db_v2.storage.table import (
    Table,
    ColumnDef,
    Row,
    TableError,
    DataType,
    DuplicateKeyError,
    ValidationError,
    ColumnNotFoundError,
    IndexNotFoundError,
    IndexInfo
)

from mini_db_v2.storage.btree import (
    BTree,
    BTreeNode,
    BTreeError,
    DuplicateKeyError as BTreeDuplicateKeyError,
    KeyNotFoundError,
    create_btree_index
)

from mini_db_v2.storage.mvcc import (
    RowVersion,
    Snapshot,
    VersionChain,
    VisibilityChecker,
    TransactionInfo as MVCCTransactionInfo,
    TransactionState,
    IsolationLevel as MVCCIsolationLevel,
    MVCCError,
    TransactionNotFoundError,
    TransactionAbortedError,
    SnapshotError
)

from mini_db_v2.storage.wal import (
    WALManager,
    WALRecord,
    WALRecordType,
    CheckpointData,
    WALError,
    WALWriteError,
    WALReadError,
    WALCorruptionError,
    WALRecoveryError,
    create_wal_manager
)

from mini_db_v2.storage.wal_writer import (
    WALWriter,
    WALWriteOptions,
    create_wal_writer
)

from mini_db_v2.storage.wal_reader import (
    WALReader,
    WALReadOptions,
    WALIterator,
    create_wal_reader,
    read_wal_file
)

from mini_db_v2.storage.recovery import (
    RecoveryManager,
    RecoveryState,
    RecoveryPhase,
    RecoveryResult,
    DirtyPage,
    TransactionState as RecoveryTransactionState,
    RecoveryError,
    RecoveryAnalysisError,
    RecoveryRedoError,
    RecoveryUndoError,
    RecoveryCheckpointError,
    create_recovery_manager,
    simulate_crash_and_recover
)

__all__ = [
    # Database
    "Database",
    "DatabaseError",
    
    # Table
    "Table",
    "ColumnDef",
    "Row",
    "TableError",
    "DataType",
    "DuplicateKeyError",
    "ValidationError",
    "ColumnNotFoundError",
    "IndexNotFoundError",
    "IndexInfo",
    
    # BTree
    "BTree",
    "BTreeNode",
    "BTreeError",
    "BTreeDuplicateKeyError",
    "KeyNotFoundError",
    "create_btree_index",
    
    # MVCC
    "RowVersion",
    "Snapshot",
    "VersionChain",
    "VisibilityChecker",
    "MVCCTransactionInfo",
    "TransactionState",
    "MVCCIsolationLevel",
    "MVCCError",
    "TransactionNotFoundError",
    "TransactionAbortedError",
    "SnapshotError",
    
    # WAL
    "WALManager",
    "WALRecord",
    "WALRecordType",
    "CheckpointData",
    "WALError",
    "WALWriteError",
    "WALReadError",
    "WALCorruptionError",
    "WALRecoveryError",
    "create_wal_manager",
    
    # WAL Writer
    "WALWriter",
    "WALWriteOptions",
    "create_wal_writer",
    
    # WAL Reader
    "WALReader",
    "WALReadOptions",
    "WALIterator",
    "create_wal_reader",
    "read_wal_file",
    
    # Recovery
    "RecoveryManager",
    "RecoveryState",
    "RecoveryPhase",
    "RecoveryResult",
    "DirtyPage",
    "RecoveryTransactionState",
    "RecoveryError",
    "RecoveryAnalysisError",
    "RecoveryRedoError",
    "RecoveryUndoError",
    "RecoveryCheckpointError",
    "create_recovery_manager",
    "simulate_crash_and_recover"
]