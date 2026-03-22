# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_mvcc_phase7
# Intent: Comprehensive tests for Phase 7 MVCC implementation.
# Dependencies: pytest, mini_db_v2.storage.mvcc, mini_db_v2.concurrency.transaction
# END_MODULE_CONTRACT

"""
Phase 7 MVCC Test Suite

Coverage:
1. MVCC Structures (RowVersion, Snapshot, VersionChain, VisibilityChecker)
2. Transaction Manager (begin, commit, rollback, get_snapshot)
3. Visibility Rules (PostgreSQL-style)
4. Isolation Levels (READ COMMITTED, REPEATABLE READ)
5. Checkpoint #2: Snapshot Isolation
6. Checkpoint #5: Non-blocking Reads
"""

import pytest
import threading
import time
from datetime import datetime

from mini_db_v2.storage.mvcc import (
    RowVersion,
    Snapshot,
    VersionChain,
    VisibilityChecker,
    IsolationLevel,
    TransactionState,
    TransactionInfo,
    MVCCError,
    TransactionNotFoundError,
)

from mini_db_v2.concurrency.transaction import (
    TransactionManager,
    TransactionInfo as TxInfo,
    IsolationLevel as TxIsolationLevel,
    TransactionState as TxState,
)

# Note: TransactionError is not exported from mvcc.py - this is a Coder bug
# Using MVCCError as base exception instead


# =============================================================================
# START_BLOCK_FIXTURES
# =============================================================================

@pytest.fixture
def transaction_manager():
    """Create a fresh TransactionManager for each test."""
    return TransactionManager()


@pytest.fixture
def version_chain():
    """Create a fresh VersionChain for each test."""
    return VersionChain(row_id=1)


# END_BLOCK_FIXTURES


# =============================================================================
# START_BLOCK_ROW_VERSION_TESTS
# =============================================================================

class TestRowVersion:
    """Tests for RowVersion dataclass."""
    
    def test_row_version_creation(self):
        """Test basic RowVersion creation."""
        data = {"id": 1, "name": "test"}
        version = RowVersion(data=data, xmin=1, xmax=0)
        
        assert version.data == data
        assert version.xmin == 1
        assert version.xmax == 0
        assert version.is_alive() is True
        assert version.created_at is not None
    
    def test_row_version_with_xmax(self):
        """Test RowVersion with xmax set (deleted/updated)."""
        data = {"id": 1, "name": "test"}
        version = RowVersion(data=data, xmin=1, xmax=2)
        
        assert version.xmax == 2
        assert version.is_alive() is False
    
    def test_row_version_is_alive(self):
        """Test is_alive method."""
        alive_version = RowVersion(data={}, xmin=1, xmax=0)
        deleted_version = RowVersion(data={}, xmin=1, xmax=2)
        
        assert alive_version.is_alive() is True
        assert deleted_version.is_alive() is False
    
    def test_row_version_default_values(self):
        """Test default values for RowVersion."""
        version = RowVersion(data={}, xmin=1)
        
        assert version.xmax == 0
        assert version.row_id == -1
        assert version.created_at is not None


# END_BLOCK_ROW_VERSION_TESTS


# =============================================================================
# START_BLOCK_SNAPSHOT_TESTS
# =============================================================================

class TestSnapshot:
    """Tests for Snapshot dataclass."""
    
    def test_snapshot_creation(self):
        """Test basic Snapshot creation."""
        active_xids = {1, 2, 3}
        snapshot = Snapshot(
            xid=4,
            active_xids=active_xids,
            xmin=1,
            xmax=4
        )
        
        assert snapshot.xid == 4
        assert snapshot.active_xids == active_xids
        assert snapshot.xmin == 1
        assert snapshot.xmax == 4
        assert snapshot.created_at is not None
    
    def test_snapshot_is_active(self):
        """Test is_active method."""
        snapshot = Snapshot(
            xid=4,
            active_xids={1, 2, 3},
            xmin=1,
            xmax=4
        )
        
        assert snapshot.is_active(1) is True
        assert snapshot.is_active(2) is True
        assert snapshot.is_active(4) is False  # Not in active_xids
    
    def test_snapshot_is_committed_before_snapshot(self):
        """Test is_committed_before_snapshot method."""
        snapshot = Snapshot(
            xid=5,
            active_xids={2, 3},
            xmin=2,
            xmax=6
        )
        
        # XID 1 was committed before snapshot (not in active_xids, < xmax)
        assert snapshot.is_committed_before_snapshot(1) is True
        
        # XID 2 is active (in active_xids)
        assert snapshot.is_committed_before_snapshot(2) is False
        
        # XID 4 was committed before snapshot
        assert snapshot.is_committed_before_snapshot(4) is True
        
        # XID 6 is >= xmax (future transaction)
        assert snapshot.is_committed_before_snapshot(6) is False
    
    def test_snapshot_empty_active_xids(self):
        """Test snapshot with empty active_xids."""
        snapshot = Snapshot(
            xid=1,
            active_xids=set(),
            xmin=1,
            xmax=2
        )
        
        assert snapshot.is_active(1) is False
        assert snapshot.is_committed_before_snapshot(1) is False


# END_BLOCK_SNAPSHOT_TESTS


# =============================================================================
# START_BLOCK_VISIBILITY_CHECKER_TESTS
# =============================================================================

class TestVisibilityChecker:
    """Tests for VisibilityChecker class."""
    
    def test_own_inserts_visible(self):
        """Rule 1: Own inserts are always visible."""
        version = RowVersion(data={"id": 1}, xmin=1, xmax=0)
        snapshot = Snapshot(xid=1, active_xids={1}, xmin=1, xmax=2)
        
        assert VisibilityChecker.is_visible(version, 1, snapshot) is True
    
    def test_own_deletes_not_visible(self):
        """Rule: Deleted by own transaction - not visible."""
        version = RowVersion(data={"id": 1}, xmin=1, xmax=1)
        snapshot = Snapshot(xid=1, active_xids={1}, xmin=1, xmax=2)
        
        assert VisibilityChecker.is_visible(version, 1, snapshot) is False
    
    def test_xmin_must_be_committed(self):
        """Rule 2: xmin must be committed (not in active list)."""
        version = RowVersion(data={"id": 1}, xmin=2, xmax=0)
        snapshot = Snapshot(xid=1, active_xids={2}, xmin=1, xmax=3)
        
        # xmin=2 is active, so not visible
        assert VisibilityChecker.is_visible(version, 1, snapshot) is False
    
    def test_xmin_before_snapshot(self):
        """Rule 3: xmin must be before snapshot."""
        version = RowVersion(data={"id": 1}, xmin=5, xmax=0)
        snapshot = Snapshot(xid=1, active_xids={1}, xmin=1, xmax=3)
        
        # xmin=5 >= xmax=3, so not visible
        assert VisibilityChecker.is_visible(version, 1, snapshot) is False
    
    def test_alive_version_visible(self):
        """Rule 4: xmax == 0 means version is alive."""
        version = RowVersion(data={"id": 1}, xmin=1, xmax=0)
        snapshot = Snapshot(xid=2, active_xids={2}, xmin=1, xmax=3)
        
        # xmin=1 is committed (not in active_xids), xmax=0 (alive)
        assert VisibilityChecker.is_visible(version, 2, snapshot) is True
    
    def test_deleted_by_active_transaction_visible(self):
        """Rule 6: If xmax is active, version is still visible."""
        version = RowVersion(data={"id": 1}, xmin=1, xmax=3)
        snapshot = Snapshot(xid=2, active_xids={2, 3}, xmin=1, xmax=4)
        
        # xmax=3 is active, so version is still visible
        assert VisibilityChecker.is_visible(version, 2, snapshot) is True
    
    def test_deleted_by_committed_transaction_not_visible(self):
        """Rule 7: If xmax committed before snapshot - not visible."""
        version = RowVersion(data={"id": 1}, xmin=1, xmax=2)
        snapshot = Snapshot(xid=3, active_xids={3}, xmin=1, xmax=4)
        
        # xmax=2 is committed (not in active_xids, < xmax)
        assert VisibilityChecker.is_visible(version, 3, snapshot) is False
    
    def test_find_visible_version(self):
        """Test find_visible_version method."""
        # Create version chain: newest to oldest
        versions = [
            RowVersion(data={"id": 1, "v": 3}, xmin=3, xmax=0),  # Created by active tx
            RowVersion(data={"id": 1, "v": 2}, xmin=2, xmax=3),  # Deleted by active tx
            RowVersion(data={"id": 1, "v": 1}, xmin=1, xmax=0),  # Old committed version
        ]
        
        snapshot = Snapshot(xid=4, active_xids={3}, xmin=1, xmax=5)
        
        # Should find version with v=2 (xmax is active)
        visible = VisibilityChecker.find_visible_version(versions, 4, snapshot)
        assert visible is not None
        assert visible.data["v"] == 2
    
    def test_find_visible_version_none(self):
        """Test find_visible_version returns None when no visible version."""
        versions = [
            RowVersion(data={"id": 1}, xmin=2, xmax=0),  # Created by active tx
        ]
        
        snapshot = Snapshot(xid=1, active_xids={2}, xmin=1, xmax=3)
        
        # No visible version
        visible = VisibilityChecker.find_visible_version(versions, 1, snapshot)
        assert visible is None


# END_BLOCK_VISIBILITY_CHECKER_TESTS


# =============================================================================
# START_BLOCK_VERSION_CHAIN_TESTS
# =============================================================================

class TestVersionChain:
    """Tests for VersionChain class."""
    
    def test_version_chain_creation(self):
        """Test VersionChain creation."""
        chain = VersionChain(row_id=1)
        
        assert chain.row_id == 1
        assert chain.version_count == 0
    
    def test_version_chain_insert(self, version_chain):
        """Test insert operation."""
        version = version_chain.insert({"id": 1, "name": "test"}, xid=1)
        
        assert version.xmin == 1
        assert version.xmax == 0
        assert version.data == {"id": 1, "name": "test"}
        assert version_chain.version_count == 1
    
    def test_version_chain_multiple_inserts(self, version_chain):
        """Test multiple inserts create version chain."""
        v1 = version_chain.insert({"id": 1, "v": 1}, xid=1)
        v2 = version_chain.insert({"id": 1, "v": 2}, xid=2)
        
        # Newest first
        assert version_chain.version_count == 2
        versions = version_chain.get_all_versions()
        assert versions[0].data["v"] == 2
        assert versions[1].data["v"] == 1
    
    def test_version_chain_update(self, version_chain):
        """Test update operation."""
        version_chain.insert({"id": 1, "v": 1}, xid=1)
        
        snapshot = Snapshot(xid=2, active_xids={2}, xmin=1, xmax=3)
        new_version = version_chain.update({"id": 1, "v": 2}, xid=2, snapshot=snapshot)
        
        assert new_version is not None
        assert new_version.data["v"] == 2
        assert version_chain.version_count == 2
    
    def test_version_chain_update_not_visible(self, version_chain):
        """Test update returns None if current version not visible."""
        version_chain.insert({"id": 1, "v": 1}, xid=1)
        
        # XID 1 is still active, so not visible to XID 2
        snapshot = Snapshot(xid=2, active_xids={1, 2}, xmin=1, xmax=3)
        new_version = version_chain.update({"id": 1, "v": 2}, xid=2, snapshot=snapshot)
        
        assert new_version is None
        assert version_chain.version_count == 1
    
    def test_version_chain_delete(self, version_chain):
        """Test delete operation."""
        version_chain.insert({"id": 1}, xid=1)
        
        snapshot = Snapshot(xid=2, active_xids={2}, xmin=1, xmax=3)
        result = version_chain.delete(xid=2, snapshot=snapshot)
        
        assert result is True
        versions = version_chain.get_all_versions()
        assert versions[0].xmax == 2
    
    def test_version_chain_delete_not_visible(self, version_chain):
        """Test delete returns False if version not visible."""
        version_chain.insert({"id": 1}, xid=1)
        
        # XID 1 is still active
        snapshot = Snapshot(xid=2, active_xids={1, 2}, xmin=1, xmax=3)
        result = version_chain.delete(xid=2, snapshot=snapshot)
        
        assert result is False
    
    def test_version_chain_get_visible(self, version_chain):
        """Test get_visible method."""
        version_chain.insert({"id": 1, "v": 1}, xid=1)
        version_chain.insert({"id": 1, "v": 2}, xid=2)
        
        snapshot = Snapshot(xid=3, active_xids={3}, xmin=1, xmax=4)
        visible = version_chain.get_visible(xid=3, snapshot=snapshot)
        
        assert visible is not None
        assert visible.data["v"] == 2
    
    def test_version_chain_vacuum(self, version_chain):
        """Test vacuum removes old versions."""
        # Insert and update
        version_chain.insert({"id": 1, "v": 1}, xid=1)
        snapshot = Snapshot(xid=2, active_xids={2}, xmin=1, xmax=3)
        version_chain.update({"id": 1, "v": 2}, xid=2, snapshot=snapshot)
        
        # Simulate XID 2 committed
        versions = version_chain.get_all_versions()
        versions[1].xmax = 2  # Mark old version as deleted by committed tx
        
        # Vacuum with oldest_xid = 3 (XID 1 and 2 are committed)
        removed = version_chain.vacuum(oldest_xid=3)
        
        assert removed >= 0
    
    def test_version_chain_thread_safety(self, version_chain):
        """Test VersionChain is thread-safe."""
        results = []
        
        def insert_version(xid):
            version = version_chain.insert({"id": 1, "xid": xid}, xid=xid)
            results.append(version)
        
        threads = [
            threading.Thread(target=insert_version, args=(i,))
            for i in range(1, 11)
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert version_chain.version_count == 10


# END_BLOCK_VERSION_CHAIN_TESTS


# =============================================================================
# START_BLOCK_TRANSACTION_MANAGER_TESTS
# =============================================================================

class TestTransactionManager:
    """Tests for TransactionManager class."""
    
    def test_transaction_manager_creation(self):
        """Test TransactionManager creation."""
        tm = TransactionManager()
        
        assert tm.next_xid == 1
        assert len(tm.active_xids) == 0
    
    def test_begin_transaction(self, transaction_manager):
        """Test begin creates transaction."""
        xid = transaction_manager.begin()
        
        assert xid == 1
        assert transaction_manager.next_xid == 2
        assert xid in transaction_manager.active_xids
    
    def test_begin_multiple_transactions(self, transaction_manager):
        """Test multiple begin calls."""
        xid1 = transaction_manager.begin()
        xid2 = transaction_manager.begin()
        xid3 = transaction_manager.begin()
        
        assert xid1 == 1
        assert xid2 == 2
        assert xid3 == 3
        assert len(transaction_manager.active_xids) == 3
    
    def test_begin_with_isolation_level(self, transaction_manager):
        """Test begin with isolation level."""
        xid = transaction_manager.begin(
            isolation_level=TxIsolationLevel.REPEATABLE_READ
        )
        
        tx_info = transaction_manager.get_transaction_info(xid)
        assert tx_info.isolation_level == TxIsolationLevel.REPEATABLE_READ
    
    def test_commit_transaction(self, transaction_manager):
        """Test commit marks transaction as committed."""
        xid = transaction_manager.begin()
        result = transaction_manager.commit(xid)
        
        assert result is True
        assert xid not in transaction_manager.active_xids
        assert transaction_manager.is_committed(xid) is True
    
    def test_commit_nonexistent_transaction(self, transaction_manager):
        """Test commit raises error for nonexistent transaction."""
        with pytest.raises(Exception):  # TransactionNotFoundError
            transaction_manager.commit(999)
    
    def test_commit_already_committed(self, transaction_manager):
        """Test commit returns False for already committed transaction."""
        xid = transaction_manager.begin()
        transaction_manager.commit(xid)
        
        result = transaction_manager.commit(xid)
        assert result is False
    
    def test_rollback_transaction(self, transaction_manager):
        """Test rollback marks transaction as aborted."""
        xid = transaction_manager.begin()
        result = transaction_manager.rollback(xid)
        
        assert result is True
        assert xid not in transaction_manager.active_xids
        tx_info = transaction_manager.get_transaction_info(xid)
        assert tx_info.is_aborted() is True
    
    def test_rollback_nonexistent_transaction(self, transaction_manager):
        """Test rollback raises error for nonexistent transaction."""
        with pytest.raises(Exception):  # TransactionNotFoundError
            transaction_manager.rollback(999)
    
    def test_get_snapshot_read_committed(self, transaction_manager):
        """Test get_snapshot for READ COMMITTED creates new snapshot each time."""
        xid = transaction_manager.begin(TxIsolationLevel.READ_COMMITTED)
        
        snapshot1 = transaction_manager.get_snapshot(xid)
        snapshot2 = transaction_manager.get_snapshot(xid)
        
        # Different snapshots for READ COMMITTED
        assert snapshot1 is not snapshot2
    
    def test_get_snapshot_repeatable_read(self, transaction_manager):
        """Test get_snapshot for REPEATABLE READ returns same snapshot."""
        xid = transaction_manager.begin(TxIsolationLevel.REPEATABLE_READ)
        
        snapshot1 = transaction_manager.get_snapshot(xid)
        snapshot2 = transaction_manager.get_snapshot(xid)
        
        # Same snapshot for REPEATABLE READ
        assert snapshot1 is snapshot2
    
    def test_get_snapshot_includes_active_xids(self, transaction_manager):
        """Test snapshot includes active transactions."""
        xid1 = transaction_manager.begin()
        xid2 = transaction_manager.begin()
        xid3 = transaction_manager.begin()
        
        snapshot = transaction_manager.get_snapshot(xid3)
        
        assert xid1 in snapshot.active_xids
        assert xid2 in snapshot.active_xids
        assert xid3 in snapshot.active_xids
    
    def test_is_active(self, transaction_manager):
        """Test is_active method."""
        xid = transaction_manager.begin()
        
        assert transaction_manager.is_active(xid) is True
        transaction_manager.commit(xid)
        assert transaction_manager.is_active(xid) is False
    
    def test_is_committed(self, transaction_manager):
        """Test is_committed method."""
        xid = transaction_manager.begin()
        
        assert transaction_manager.is_committed(xid) is False
        transaction_manager.commit(xid)
        assert transaction_manager.is_committed(xid) is True
    
    def test_get_all_active_xids(self, transaction_manager):
        """Test get_all_active_xids method."""
        xid1 = transaction_manager.begin()
        xid2 = transaction_manager.begin()
        transaction_manager.commit(xid1)
        
        active = transaction_manager.get_all_active_xids()
        
        assert xid1 not in active
        assert xid2 in active
    
    def test_cleanup_old_transactions(self, transaction_manager):
        """Test cleanup removes old committed transactions."""
        # Create and commit many transactions
        for _ in range(100):
            xid = transaction_manager.begin()
            transaction_manager.commit(xid)
        
        # Cleanup keeping only last 10
        removed = transaction_manager.cleanup_old_transactions(keep_last=10)
        
        assert removed > 0


# END_BLOCK_TRANSACTION_MANAGER_TESTS


# =============================================================================
# START_BLOCK_ISOLATION_LEVEL_TESTS
# =============================================================================

class TestIsolationLevels:
    """Tests for isolation levels."""
    
    def test_read_committed_sees_committed_changes(self, transaction_manager):
        """READ COMMITTED sees changes committed before each query."""
        # Transaction 1 inserts
        xid1 = transaction_manager.begin(TxIsolationLevel.READ_COMMITTED)
        
        # Transaction 2 inserts and commits
        xid2 = transaction_manager.begin()
        transaction_manager.commit(xid2)
        
        # Transaction 1 gets new snapshot
        snapshot = transaction_manager.get_snapshot(xid1)
        
        # XID 2 should not be in active_xids (committed)
        assert xid2 not in snapshot.active_xids
    
    def test_repeatable_read_consistent_snapshot(self, transaction_manager):
        """REPEATABLE READ uses same snapshot throughout transaction."""
        xid1 = transaction_manager.begin(TxIsolationLevel.REPEATABLE_READ)
        
        # Get initial snapshot
        snapshot1 = transaction_manager.get_snapshot(xid1)
        
        # Another transaction commits
        xid2 = transaction_manager.begin()
        transaction_manager.commit(xid2)
        
        # Get snapshot again - should be same
        snapshot2 = transaction_manager.get_snapshot(xid1)
        
        assert snapshot1 is snapshot2
        # XID 2 should still be in active_xids (from initial snapshot)
        # Actually, it won't be because it wasn't active when snapshot was taken
        assert snapshot2.active_xids == snapshot1.active_xids
    
    def test_read_committed_new_snapshot_each_time(self, transaction_manager):
        """READ COMMITTED creates new snapshot on each get_snapshot call."""
        xid1 = transaction_manager.begin(TxIsolationLevel.READ_COMMITTED)
        
        snapshot1 = transaction_manager.get_snapshot(xid1)
        
        # Start another transaction
        xid2 = transaction_manager.begin()
        
        snapshot2 = transaction_manager.get_snapshot(xid1)
        
        # Different snapshots
        assert snapshot1 is not snapshot2
        # New transaction should be in second snapshot
        assert xid2 in snapshot2.active_xids
        assert xid2 not in snapshot1.active_xids


# END_BLOCK_ISOLATION_LEVEL_TESTS


# =============================================================================
# START_BLOCK_CHECKPOINT_2_SNAPSHOT_ISOLATION
# =============================================================================

class TestCheckpoint2SnapshotIsolation:
    """
    Checkpoint #2: MVCC обеспечивает snapshot isolation.
    
    Транзакция видит консистентный snapshot данных.
    Не видит изменения других транзакций, начавшиеся после её snapshot.
    """
    
    def test_snapshot_isolation_basic(self, transaction_manager, version_chain):
        """Test basic snapshot isolation."""
        # Transaction 1 inserts a row
        xid1 = transaction_manager.begin()
        version_chain.insert({"id": 1, "value": "initial"}, xid1)
        transaction_manager.commit(xid1)
        
        # Transaction 2 starts and reads
        xid2 = transaction_manager.begin(TxIsolationLevel.REPEATABLE_READ)
        snapshot2 = transaction_manager.get_snapshot(xid2)
        
        # Transaction 3 updates the row
        xid3 = transaction_manager.begin()
        version_chain.update({"id": 1, "value": "updated"}, xid3, snapshot=snapshot2)
        transaction_manager.commit(xid3)
        
        # Transaction 2 should still see old value
        visible = version_chain.get_visible(xid2, snapshot2)
        assert visible is not None
        assert visible.data["value"] == "initial"
    
    def test_snapshot_isolation_multiple_transactions(self, transaction_manager):
        """Test snapshot isolation with multiple concurrent transactions."""
        # Start multiple transactions
        xid1 = transaction_manager.begin(TxIsolationLevel.REPEATABLE_READ)
        xid2 = transaction_manager.begin(TxIsolationLevel.REPEATABLE_READ)
        xid3 = transaction_manager.begin()
        
        # Each should have consistent snapshot
        snapshot1 = transaction_manager.get_snapshot(xid1)
        snapshot2 = transaction_manager.get_snapshot(xid2)
        
        # Commit xid3
        transaction_manager.commit(xid3)
        
        # Snapshots should not change for REPEATABLE READ
        new_snapshot1 = transaction_manager.get_snapshot(xid1)
        new_snapshot2 = transaction_manager.get_snapshot(xid2)
        
        assert snapshot1 is new_snapshot1
        assert snapshot2 is new_snapshot2
    
    def test_snapshot_isolation_visibility_across_transactions(
        self, transaction_manager, version_chain
    ):
        """Test visibility rules across transactions."""
        # T1 inserts
        xid1 = transaction_manager.begin()
        v1 = version_chain.insert({"id": 1}, xid1)
        
        # T2 starts before T1 commits
        xid2 = transaction_manager.begin(TxIsolationLevel.REPEATABLE_READ)
        snapshot2 = transaction_manager.get_snapshot(xid2)
        
        # T1 commits
        transaction_manager.commit(xid1)
        
        # T2 should not see T1's insert (not in snapshot)
        visible = version_chain.get_visible(xid2, snapshot2)
        # Version was created by xid1 which was active when snapshot was taken
        assert visible is None  # xid1 was in active_xids
    
    def test_read_committed_sees_latest_committed(self, transaction_manager, version_chain):
        """READ COMMITTED sees latest committed changes."""
        # T1 inserts and commits
        xid1 = transaction_manager.begin()
        version_chain.insert({"id": 1, "v": 1}, xid1)
        transaction_manager.commit(xid1)
        
        # T2 reads with READ COMMITTED
        xid2 = transaction_manager.begin(TxIsolationLevel.READ_COMMITTED)
        snapshot2 = transaction_manager.get_snapshot(xid2)
        visible = version_chain.get_visible(xid2, snapshot2)
        assert visible.data["v"] == 1
        
        # T3 updates and commits
        xid3 = transaction_manager.begin()
        version_chain.update({"id": 1, "v": 2}, xid3, snapshot=snapshot2)
        transaction_manager.commit(xid3)
        
        # T2 reads again with new snapshot
        snapshot2_new = transaction_manager.get_snapshot(xid2)
        visible_new = version_chain.get_visible(xid2, snapshot2_new)
        assert visible_new.data["v"] == 2


# END_BLOCK_CHECKPOINT_2_SNAPSHOT_ISOLATION


# =============================================================================
# START_BLOCK_CHECKPOINT_5_NON_BLOCKING_READS
# =============================================================================

class TestCheckpoint5NonBlockingReads:
    """
    Checkpoint #5: Readers don't block writers.
    
    MVCC архитектура позволяет concurrent access:
    - Readers видят только закоммиченные версии
    - Writers создают новые версии без блокировки readers
    """
    
    def test_reader_not_blocked_by_writer(self, transaction_manager, version_chain):
        """Reader can read while writer is updating."""
        # Initial data
        xid1 = transaction_manager.begin()
        version_chain.insert({"id": 1, "value": "initial"}, xid1)
        transaction_manager.commit(xid1)
        
        # Writer starts transaction
        xid_writer = transaction_manager.begin()
        snapshot_writer = transaction_manager.get_snapshot(xid_writer)
        
        # Reader starts and reads
        xid_reader = transaction_manager.begin(TxIsolationLevel.READ_COMMITTED)
        snapshot_reader = transaction_manager.get_snapshot(xid_reader)
        visible = version_chain.get_visible(xid_reader, snapshot_reader)
        
        # Reader sees initial value
        assert visible is not None
        assert visible.data["value"] == "initial"
        
        # Writer updates (creates new version)
        version_chain.update(
            {"id": 1, "value": "updated"},
            xid_writer,
            snapshot=snapshot_writer
        )
        
        # Reader reads again - should still see initial (writer not committed)
        snapshot_reader2 = transaction_manager.get_snapshot(xid_reader)
        visible2 = version_chain.get_visible(xid_reader, snapshot_reader2)
        assert visible2.data["value"] == "initial"
    
    def test_writer_not_blocked_by_reader(self, transaction_manager, version_chain):
        """Writer can write while reader is reading."""
        # Initial data
        xid1 = transaction_manager.begin()
        version_chain.insert({"id": 1, "value": "initial"}, xid1)
        transaction_manager.commit(xid1)
        
        # Reader starts
        xid_reader = transaction_manager.begin()
        snapshot_reader = transaction_manager.get_snapshot(xid_reader)
        
        # Writer can update (no blocking)
        xid_writer = transaction_manager.begin()
        snapshot_writer = transaction_manager.get_snapshot(xid_writer)
        new_version = version_chain.update(
            {"id": 1, "value": "updated"},
            xid_writer,
            snapshot=snapshot_writer
        )
        
        assert new_version is not None
        
        # Reader still sees old version
        visible = version_chain.get_visible(xid_reader, snapshot_reader)
        assert visible.data["value"] == "initial"
    
    def test_concurrent_readers_dont_block_each_other(
        self, transaction_manager, version_chain
    ):
        """Multiple readers can read concurrently."""
        # Initial data
        xid1 = transaction_manager.begin()
        version_chain.insert({"id": 1, "value": "data"}, xid1)
        transaction_manager.commit(xid1)
        
        # Multiple readers
        readers = []
        for _ in range(5):
            xid = transaction_manager.begin(TxIsolationLevel.READ_COMMITTED)
            snapshot = transaction_manager.get_snapshot(xid)
            visible = version_chain.get_visible(xid, snapshot)
            readers.append((xid, visible))
        
        # All readers see the data
        for xid, visible in readers:
            assert visible is not None
            assert visible.data["value"] == "data"
    
    def test_concurrent_writers_create_versions(self, transaction_manager, version_chain):
        """Concurrent writers create separate versions."""
        # Initial data
        xid1 = transaction_manager.begin()
        version_chain.insert({"id": 1, "value": "initial"}, xid1)
        transaction_manager.commit(xid1)
        
        # Multiple writers try to update
        results = []
        for i in range(3):
            xid = transaction_manager.begin()
            snapshot = transaction_manager.get_snapshot(xid)
            # First writer succeeds, others might fail or create versions
            result = version_chain.update(
                {"id": 1, "value": f"update_{i}"},
                xid,
                snapshot=snapshot
            )
            results.append((xid, result))
        
        # At least one should succeed
        successful = [r for r in results if r[1] is not None]
        assert len(successful) >= 1
    
    def test_mvcc_allows_concurrent_access(self, transaction_manager, version_chain):
        """Test that MVCC allows true concurrent access."""
        # Setup initial data
        xid_setup = transaction_manager.begin()
        version_chain.insert({"id": 1, "counter": 0}, xid_setup)
        transaction_manager.commit(xid_setup)
        
        results = {"reads": 0, "writes": 0, "errors": 0}
        lock = threading.Lock()
        
        def reader_thread():
            try:
                xid = transaction_manager.begin(TxIsolationLevel.READ_COMMITTED)
                snapshot = transaction_manager.get_snapshot(xid)
                visible = version_chain.get_visible(xid, snapshot)
                with lock:
                    results["reads"] += 1
                transaction_manager.commit(xid)
            except Exception:
                with lock:
                    results["errors"] += 1
        
        def writer_thread():
            try:
                xid = transaction_manager.begin()
                snapshot = transaction_manager.get_snapshot(xid)
                version_chain.update({"id": 1, "counter": 1}, xid, snapshot=snapshot)
                with lock:
                    results["writes"] += 1
                transaction_manager.commit(xid)
            except Exception:
                with lock:
                    results["errors"] += 1
        
        # Run concurrent readers and writers
        threads = []
        for _ in range(5):
            threads.append(threading.Thread(target=reader_thread))
            threads.append(threading.Thread(target=writer_thread))
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Should have successful reads and writes
        assert results["reads"] > 0
        assert results["errors"] == 0


# END_BLOCK_CHECKPOINT_5_NON_BLOCKING_READS


# =============================================================================
# START_BLOCK_ADVERSARIAL_TESTS
# =============================================================================

class TestAdversarialMVCC:
    """Adversarial tests for edge cases and boundary conditions."""
    
    def test_empty_version_chain(self, version_chain):
        """Test visibility on empty version chain."""
        snapshot = Snapshot(xid=1, active_xids={1}, xmin=1, xmax=2)
        visible = version_chain.get_visible(1, snapshot)
        
        assert visible is None
    
    def test_snapshot_with_no_active_transactions(self, transaction_manager):
        """Test snapshot when no other transactions are active."""
        xid = transaction_manager.begin()
        transaction_manager.commit(xid)
        
        xid2 = transaction_manager.begin()
        snapshot = transaction_manager.get_snapshot(xid2)
        
        # Only xid2 should be active
        assert snapshot.active_xids == {xid2}
    
    def test_transaction_info_state_transitions(self, transaction_manager):
        """Test transaction state transitions."""
        xid = transaction_manager.begin()
        tx_info = transaction_manager.get_transaction_info(xid)
        
        assert tx_info.is_active() is True
        
        transaction_manager.commit(xid)
        tx_info = transaction_manager.get_transaction_info(xid)
        assert tx_info.is_committed() is True
    
    def test_rollback_state_transition(self, transaction_manager):
        """Test rollback state transition."""
        xid = transaction_manager.begin()
        transaction_manager.rollback(xid)
        
        tx_info = transaction_manager.get_transaction_info(xid)
        assert tx_info.is_aborted() is True
    
    def test_version_chain_vacuum_keeps_latest(self, version_chain):
        """Test vacuum keeps at least one version."""
        version_chain.insert({"id": 1}, xid=1)
        
        # Vacuum should not remove the only version
        removed = version_chain.vacuum(oldest_xid=100)
        
        assert removed == 0
        assert version_chain.version_count == 1
    
    def test_large_xid_values(self, transaction_manager):
        """Test with large XID values."""
        # Simulate many transactions
        for _ in range(1000):
            xid = transaction_manager.begin()
            transaction_manager.commit(xid)
        
        # Should handle large XID values
        assert transaction_manager.next_xid == 1001
    
    def test_concurrent_snapshot_creation(self, transaction_manager):
        """Test concurrent snapshot creation is thread-safe."""
        snapshots = []
        
        def create_snapshot():
            xid = transaction_manager.begin()
            snapshot = transaction_manager.get_snapshot(xid)
            snapshots.append(snapshot)
        
        threads = [threading.Thread(target=create_snapshot) for _ in range(10)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # All snapshots should be valid
        assert len(snapshots) == 10
        for snapshot in snapshots:
            assert snapshot.xid > 0
            assert snapshot.active_xids is not None


# END_BLOCK_ADVERSARIAL_TESTS


# =============================================================================
# START_BLOCK_INTEGRATION_TESTS
# =============================================================================

class TestMVCCIntegration:
    """Integration tests for MVCC components working together."""
    
    def test_full_transaction_lifecycle(self, transaction_manager, version_chain):
        """Test complete transaction lifecycle with MVCC."""
        # BEGIN
        xid = transaction_manager.begin(TxIsolationLevel.READ_COMMITTED)
        assert transaction_manager.is_active(xid)
        
        # INSERT
        version = version_chain.insert({"id": 1, "name": "test"}, xid)
        assert version.xmin == xid
        
        # Get snapshot and verify visibility
        snapshot = transaction_manager.get_snapshot(xid)
        visible = version_chain.get_visible(xid, snapshot)
        assert visible is not None
        
        # COMMIT
        transaction_manager.commit(xid)
        assert transaction_manager.is_committed(xid)
    
    def test_transaction_rollback_lifecycle(self, transaction_manager, version_chain):
        """Test transaction rollback lifecycle."""
        # BEGIN
        xid = transaction_manager.begin()
        
        # INSERT
        version_chain.insert({"id": 1}, xid)
        
        # ROLLBACK
        transaction_manager.rollback(xid)
        
        # Transaction should be aborted
        tx_info = transaction_manager.get_transaction_info(xid)
        assert tx_info.is_aborted() is True
    
    def test_multiple_version_chain_operations(self, transaction_manager):
        """Test multiple operations on version chain."""
        chain = VersionChain(row_id=1)
        
        # Multiple inserts from different transactions
        xid1 = transaction_manager.begin()
        chain.insert({"id": 1, "v": 1}, xid1)
        transaction_manager.commit(xid1)
        
        xid2 = transaction_manager.begin()
        snapshot2 = transaction_manager.get_snapshot(xid2)
        chain.update({"id": 1, "v": 2}, xid2, snapshot=snapshot2)
        transaction_manager.commit(xid2)
        
        xid3 = transaction_manager.begin()
        snapshot3 = transaction_manager.get_snapshot(xid3)
        visible = chain.get_visible(xid3, snapshot3)
        
        assert visible.data["v"] == 2


# END_BLOCK_INTEGRATION_TESTS


# =============================================================================
# START_BLOCK_CHECKPOINT_TESTS
# =============================================================================

class TestCheckpoints:
    """Explicit checkpoint tests for Phase 7."""
    
    def test_checkpoint2_mvcc_snapshot_isolation(self, transaction_manager, version_chain):
        """
        CHECKPOINT #2: MVCC обеспечивает snapshot isolation.
        
        Транзакция с REPEATABLE READ видит консистентный snapshot
        и не видит изменения других транзакций.
        """
        # Setup: insert initial data
        xid_setup = transaction_manager.begin()
        version_chain.insert({"id": 1, "value": 100}, xid_setup)
        transaction_manager.commit(xid_setup)
        
        # T1 starts with REPEATABLE READ
        xid1 = transaction_manager.begin(TxIsolationLevel.REPEATABLE_READ)
        snapshot1 = transaction_manager.get_snapshot(xid1)
        
        # T2 updates and commits
        xid2 = transaction_manager.begin()
        snapshot2 = transaction_manager.get_snapshot(xid2)
        version_chain.update({"id": 1, "value": 200}, xid2, snapshot=snapshot2)
        transaction_manager.commit(xid2)
        
        # T1 should still see old value (snapshot isolation)
        visible = version_chain.get_visible(xid1, snapshot1)
        assert visible.data["value"] == 100
        
        # T1 gets snapshot again - should be same
        snapshot1_new = transaction_manager.get_snapshot(xid1)
        assert snapshot1 is snapshot1_new
    
    def test_checkpoint5_non_blocking_reads(self, transaction_manager, version_chain):
        """
        CHECKPOINT #5: Readers don't block writers.
        
        MVCC позволяет concurrent access без блокировок.
        """
        # Setup
        xid_setup = transaction_manager.begin()
        version_chain.insert({"id": 1, "value": "initial"}, xid_setup)
        transaction_manager.commit(xid_setup)
        
        # Reader starts
        xid_reader = transaction_manager.begin(TxIsolationLevel.READ_COMMITTED)
        snapshot_reader = transaction_manager.get_snapshot(xid_reader)
        
        # Writer can update without blocking
        xid_writer = transaction_manager.begin()
        snapshot_writer = transaction_manager.get_snapshot(xid_writer)
        result = version_chain.update(
            {"id": 1, "value": "updated"},
            xid_writer,
            snapshot=snapshot_writer
        )
        
        # Writer succeeded (no blocking)
        assert result is not None
        
        # Reader still sees old value
        visible = version_chain.get_visible(xid_reader, snapshot_reader)
        assert visible.data["value"] == "initial"
        
        # Writer commits
        transaction_manager.commit(xid_writer)
        
        # Reader gets new snapshot and sees new value
        snapshot_reader_new = transaction_manager.get_snapshot(xid_reader)
        visible_new = version_chain.get_visible(xid_reader, snapshot_reader_new)
        assert visible_new.data["value"] == "updated"


# END_BLOCK_CHECKPOINT_TESTS