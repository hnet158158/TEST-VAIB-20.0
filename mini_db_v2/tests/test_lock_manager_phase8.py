# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_lock_manager_phase8
# Intent: Comprehensive tests for Lock Manager (Phase 8)
# Dependencies: pytest, threading, time
# END_MODULE_CONTRACT

"""
Phase 8: Lock Manager Tests

Test Categories:
1. Lock Acquire/Release - базовые операции
2. Lock Compatibility Matrix - совместимость блокировок
3. Lock Upgrade/Downgrade - изменение типа блокировки
4. Lock Timeout - таймаут ожидания
5. Intent Locks - намеренные блокировки
6. Wait-for Graph - построение графа ожидания
7. Concurrent Access - многопоточные тесты
8. Adversarial Tests - граничные случаи
"""

import pytest
import threading
import time
from datetime import datetime, timedelta

from mini_db_v2.concurrency.lock_manager import (
    LockManager,
    LockType,
    LockMode,
    LockError,
    LockTimeoutError,
    LockConflictError,
    DeadlockError,
    LockCompatibility,
    LockEntry,
    WaitEntry,
    create_lock_manager,
    resource_key,
)


# =============================================================================
# START_BLOCK_FIXTURES
# =============================================================================

@pytest.fixture
def lock_manager():
    """Create a fresh LockManager for each test."""
    return LockManager()


@pytest.fixture
def lock_manager_with_detector():
    """Create a LockManager with deadlock detector."""
    from mini_db_v2.concurrency.deadlock import DeadlockDetector
    detector = DeadlockDetector()
    return LockManager(deadlock_detector=detector)


# END_BLOCK_FIXTURES


# =============================================================================
# START_BLOCK_BASIC_OPERATIONS
# =============================================================================

class TestLockAcquireRelease:
    """Tests for basic lock acquire and release operations."""

    def test_acquire_share_lock_success(self, lock_manager):
        """Test acquiring a share lock succeeds."""
        resource = "table:users:1"
        xid = 1
        
        result = lock_manager.acquire_lock(
            resource, LockType.SHARE, xid, timeout=1.0
        )
        
        assert result is True
        assert lock_manager.is_locked(resource)
        assert xid in lock_manager.get_lock_holders(resource)

    def test_acquire_exclusive_lock_success(self, lock_manager):
        """Test acquiring an exclusive lock succeeds."""
        resource = "table:users:1"
        xid = 1
        
        result = lock_manager.acquire_lock(
            resource, LockType.EXCLUSIVE, xid, timeout=1.0
        )
        
        assert result is True
        assert lock_manager.is_locked(resource)
        assert xid in lock_manager.get_lock_holders(resource)

    def test_release_lock_success(self, lock_manager):
        """Test releasing a lock succeeds."""
        resource = "table:users:1"
        xid = 1
        
        lock_manager.acquire_lock(resource, LockType.SHARE, xid)
        result = lock_manager.release_lock(resource, xid)
        
        assert result is True
        assert not lock_manager.is_locked(resource)

    def test_release_nonexistent_lock(self, lock_manager):
        """Test releasing a non-existent lock returns False."""
        result = lock_manager.release_lock("nonexistent", 1)
        assert result is False

    def test_release_lock_wrong_transaction(self, lock_manager):
        """Test releasing another transaction's lock fails."""
        resource = "table:users:1"
        
        lock_manager.acquire_lock(resource, LockType.SHARE, 1)
        result = lock_manager.release_lock(resource, 2)
        
        assert result is False
        assert lock_manager.is_locked(resource)

    def test_release_all_locks(self, lock_manager):
        """Test releasing all locks for a transaction."""
        xid = 1
        
        lock_manager.acquire_lock("table:users:1", LockType.SHARE, xid)
        lock_manager.acquire_lock("table:users:2", LockType.EXCLUSIVE, xid)
        lock_manager.acquire_lock("table:orders:1", LockType.SHARE, xid)
        
        count = lock_manager.release_all_locks(xid)
        
        assert count == 3
        assert not lock_manager.is_locked("table:users:1")
        assert not lock_manager.is_locked("table:users:2")
        assert not lock_manager.is_locked("table:orders:1")

    def test_acquire_same_lock_twice_same_transaction(self, lock_manager):
        """Test acquiring the same lock twice from same transaction."""
        resource = "table:users:1"
        xid = 1
        
        result1 = lock_manager.acquire_lock(resource, LockType.SHARE, xid)
        result2 = lock_manager.acquire_lock(resource, LockType.SHARE, xid)
        
        assert result1 is True
        assert result2 is True  # Idempotent

    def test_get_locks_held_by(self, lock_manager):
        """Test getting all locks held by a transaction."""
        xid = 1
        
        lock_manager.acquire_lock("table:users:1", LockType.SHARE, xid)
        lock_manager.acquire_lock("table:users:2", LockType.EXCLUSIVE, xid)
        lock_manager.acquire_lock("table:orders:1", LockType.SHARE, xid)
        
        locks = lock_manager.get_locks_held_by(xid)
        
        assert len(locks) == 3
        assert "table:users:1" in locks
        assert "table:users:2" in locks
        assert "table:orders:1" in locks


# END_BLOCK_BASIC_OPERATIONS


# =============================================================================
# START_BLOCK_COMPATIBILITY_MATRIX
# =============================================================================

class TestLockCompatibility:
    """Tests for lock compatibility matrix."""

    def test_share_share_compatible(self, lock_manager):
        """Test S + S locks are compatible."""
        resource = "table:users:1"
        
        result1 = lock_manager.acquire_lock(resource, LockType.SHARE, 1)
        result2 = lock_manager.acquire_lock(resource, LockType.SHARE, 2)
        
        assert result1 is True
        assert result2 is True
        assert len(lock_manager.get_lock_holders(resource)) == 2

    def test_share_exclusive_incompatible(self, lock_manager):
        """Test S + X locks are incompatible."""
        resource = "table:users:1"
        
        result1 = lock_manager.acquire_lock(resource, LockType.SHARE, 1)
        
        # X lock should wait (use NOWAIT mode to test incompatibility)
        with pytest.raises(LockConflictError):
            lock_manager.acquire_lock(
                resource, LockType.EXCLUSIVE, 2, mode=LockMode.NOWAIT
            )

    def test_exclusive_share_incompatible(self, lock_manager):
        """Test X + S locks are incompatible."""
        resource = "table:users:1"
        
        result1 = lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, 1)
        
        # S lock should wait (use NOWAIT mode)
        with pytest.raises(LockConflictError):
            lock_manager.acquire_lock(
                resource, LockType.SHARE, 2, mode=LockMode.NOWAIT
            )

    def test_exclusive_exclusive_incompatible(self, lock_manager):
        """Test X + X locks are incompatible."""
        resource = "table:users:1"
        
        result1 = lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, 1)
        
        with pytest.raises(LockConflictError):
            lock_manager.acquire_lock(
                resource, LockType.EXCLUSIVE, 2, mode=LockMode.NOWAIT
            )

    def test_intent_share_share_compatible(self, lock_manager):
        """Test IS + S locks are compatible."""
        resource = "table:users"
        
        result1 = lock_manager.acquire_lock(resource, LockType.INTENT_SHARE, 1)
        result2 = lock_manager.acquire_lock(resource, LockType.SHARE, 2)
        
        assert result1 is True
        assert result2 is True

    def test_intent_share_exclusive_incompatible(self, lock_manager):
        """Test IS + X locks are incompatible."""
        resource = "table:users"
        
        lock_manager.acquire_lock(resource, LockType.INTENT_SHARE, 1)
        
        with pytest.raises(LockConflictError):
            lock_manager.acquire_lock(
                resource, LockType.EXCLUSIVE, 2, mode=LockMode.NOWAIT
            )

    def test_intent_exclusive_share_incompatible(self, lock_manager):
        """Test IX + S locks are incompatible."""
        resource = "table:users"
        
        lock_manager.acquire_lock(resource, LockType.INTENT_EXCLUSIVE, 1)
        
        with pytest.raises(LockConflictError):
            lock_manager.acquire_lock(
                resource, LockType.SHARE, 2, mode=LockMode.NOWAIT
            )

    def test_intent_share_intent_exclusive_compatible(self, lock_manager):
        """Test IS + IX locks are compatible."""
        resource = "table:users"
        
        result1 = lock_manager.acquire_lock(resource, LockType.INTENT_SHARE, 1)
        result2 = lock_manager.acquire_lock(resource, LockType.INTENT_EXCLUSIVE, 2)
        
        assert result1 is True
        assert result2 is True

    def test_intent_exclusive_intent_exclusive_compatible(self, lock_manager):
        """Test IX + IX locks are compatible."""
        resource = "table:users"
        
        result1 = lock_manager.acquire_lock(resource, LockType.INTENT_EXCLUSIVE, 1)
        result2 = lock_manager.acquire_lock(resource, LockType.INTENT_EXCLUSIVE, 2)
        
        assert result1 is True
        assert result2 is True


class TestLockCompatibilityClass:
    """Tests for LockCompatibility class methods."""

    def test_is_compatible_share_share(self):
        """Test S and S are compatible."""
        assert LockCompatibility.is_compatible(LockType.SHARE, LockType.SHARE)

    def test_is_compatible_share_exclusive(self):
        """Test S and X are not compatible."""
        assert not LockCompatibility.is_compatible(LockType.SHARE, LockType.EXCLUSIVE)

    def test_is_compatible_exclusive_any(self):
        """Test X is not compatible with anything."""
        assert not LockCompatibility.is_compatible(LockType.EXCLUSIVE, LockType.SHARE)
        assert not LockCompatibility.is_compatible(LockType.EXCLUSIVE, LockType.EXCLUSIVE)
        assert not LockCompatibility.is_compatible(LockType.EXCLUSIVE, LockType.INTENT_SHARE)
        assert not LockCompatibility.is_compatible(LockType.EXCLUSIVE, LockType.INTENT_EXCLUSIVE)

    def test_can_grant_empty_held(self):
        """Test can grant when no locks held."""
        assert LockCompatibility.can_grant(set(), LockType.SHARE)
        assert LockCompatibility.can_grant(set(), LockType.EXCLUSIVE)

    def test_can_grant_multiple_share(self):
        """Test can grant S when multiple S locks held."""
        held = {LockType.SHARE, LockType.SHARE}
        assert LockCompatibility.can_grant(held, LockType.SHARE)

    def test_can_grant_conflict(self):
        """Test cannot grant when conflict exists."""
        held = {LockType.EXCLUSIVE}
        assert not LockCompatibility.can_grant(held, LockType.SHARE)


# END_BLOCK_COMPATIBILITY_MATRIX


# =============================================================================
# START_BLOCK_UPGRADE_DOWNGRADE
# =============================================================================

class TestLockUpgradeDowngrade:
    """Tests for lock upgrade and downgrade operations."""

    def test_upgrade_share_to_exclusive_success(self, lock_manager):
        """Test upgrading S to X when no other holders."""
        resource = "table:users:1"
        xid = 1
        
        lock_manager.acquire_lock(resource, LockType.SHARE, xid)
        result = lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, xid)
        
        assert result is True
        # Verify it's now exclusive

    def test_upgrade_share_to_exclusive_with_other_holders(self, lock_manager):
        """Test upgrading S to X fails when other S holders exist."""
        resource = "table:users:1"
        
        lock_manager.acquire_lock(resource, LockType.SHARE, 1)
        lock_manager.acquire_lock(resource, LockType.SHARE, 2)
        
        # T1 tries to upgrade to X - should raise LockConflictError with NOWAIT
        with pytest.raises(LockConflictError):
            lock_manager.acquire_lock(
                resource, LockType.EXCLUSIVE, 1, mode=LockMode.NOWAIT
            )

    def test_downgrade_exclusive_to_share(self, lock_manager):
        """Test downgrading X to S."""
        resource = "table:users:1"
        xid = 1
        
        lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, xid)
        result = lock_manager.acquire_lock(resource, LockType.SHARE, xid)
        
        assert result is True

    def test_multiple_upgrades_different_transactions(self, lock_manager):
        """Test multiple transactions trying to upgrade."""
        resource = "table:users:1"
        
        # T1 and T2 both have S locks
        lock_manager.acquire_lock(resource, LockType.SHARE, 1)
        lock_manager.acquire_lock(resource, LockType.SHARE, 2)
        
        # T1 tries to upgrade - should raise LockConflictError with NOWAIT
        with pytest.raises(LockConflictError):
            lock_manager.acquire_lock(
                resource, LockType.EXCLUSIVE, 1, mode=LockMode.NOWAIT
            )
        
        # T2 releases
        lock_manager.release_lock(resource, 2)
        
        # Now T1 can upgrade
        result2 = lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, 1)
        
        assert result2 is True


# END_BLOCK_UPGRADE_DOWNGRADE


# =============================================================================
# START_BLOCK_TIMEOUT
# =============================================================================

class TestLockTimeout:
    """Tests for lock timeout functionality."""

    def test_timeout_expired(self, lock_manager):
        """Test lock acquisition times out."""
        resource = "table:users:1"
        
        lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, 1)
        
        start_time = time.time()
        
        with pytest.raises(LockTimeoutError):
            lock_manager.acquire_lock(
                resource, LockType.SHARE, 2, timeout=0.5
            )
        
        elapsed = time.time() - start_time
        assert elapsed >= 0.5
        assert elapsed < 1.0  # Should not wait too long

    def test_timeout_default_value(self, lock_manager):
        """Test default timeout is 30 seconds."""
        assert LockManager.DEFAULT_TIMEOUT == 30.0

    def test_timeout_custom_value(self, lock_manager):
        """Test custom timeout value."""
        resource = "table:users:1"
        
        lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, 1)
        
        start_time = time.time()
        
        with pytest.raises(LockTimeoutError):
            lock_manager.acquire_lock(
                resource, LockType.SHARE, 2, timeout=0.1
            )
        
        elapsed = time.time() - start_time
        assert elapsed >= 0.1
        assert elapsed < 0.5

    def test_no_timeout_with_release(self, lock_manager):
        """Test lock acquired before timeout when released."""
        resource = "table:users:1"
        
        lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, 1)
        
        # Release immediately in same thread to test notification
        lock_manager.release_lock(resource, 1)
        
        # Should succeed immediately now
        result = lock_manager.acquire_lock(
            resource, LockType.SHARE, 2, timeout=1.0
        )
        
        assert result is True


# END_BLOCK_TIMEOUT


# =============================================================================
# START_BLOCK_LOCK_MODES
# =============================================================================

class TestLockModes:
    """Tests for different lock modes (WAIT, NOWAIT, SKIP)."""

    def test_mode_nowait_raises_on_conflict(self, lock_manager):
        """Test NOWAIT mode raises LockConflictError on conflict."""
        resource = "table:users:1"
        
        lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, 1)
        
        with pytest.raises(LockConflictError):
            lock_manager.acquire_lock(
                resource, LockType.SHARE, 2, mode=LockMode.NOWAIT
            )

    def test_mode_skip_returns_false_on_conflict(self, lock_manager):
        """Test SKIP mode returns False on conflict."""
        resource = "table:users:1"
        
        lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, 1)
        
        result = lock_manager.acquire_lock(
            resource, LockType.SHARE, 2, mode=LockMode.SKIP
        )
        
        assert result is False

    def test_mode_wait_blocks_until_available(self, lock_manager):
        """Test WAIT mode can acquire lock after release."""
        resource = "table:users:1"
        
        # T1 acquires exclusive lock
        lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, 1)
        
        # T1 releases
        lock_manager.release_lock(resource, 1)
        
        # Now T2 can acquire
        result = lock_manager.acquire_lock(
            resource, LockType.SHARE, 2, timeout=1.0
        )
        
        assert result is True


# END_BLOCK_LOCK_MODES


# =============================================================================
# START_BLOCK_INTENT_LOCKS
# =============================================================================

class TestIntentLocks:
    """Tests for intent locks (IS, IX)."""

    def test_intent_share_lock(self, lock_manager):
        """Test acquiring intent share lock."""
        resource = "table:users"
        
        result = lock_manager.acquire_lock(
            resource, LockType.INTENT_SHARE, 1
        )
        
        assert result is True
        assert lock_manager.is_locked(resource)

    def test_intent_exclusive_lock(self, lock_manager):
        """Test acquiring intent exclusive lock."""
        resource = "table:users"
        
        result = lock_manager.acquire_lock(
            resource, LockType.INTENT_EXCLUSIVE, 1
        )
        
        assert result is True
        assert lock_manager.is_locked(resource)

    def test_multiple_intent_share(self, lock_manager):
        """Test multiple IS locks on same table."""
        resource = "table:users"
        
        result1 = lock_manager.acquire_lock(resource, LockType.INTENT_SHARE, 1)
        result2 = lock_manager.acquire_lock(resource, LockType.INTENT_SHARE, 2)
        result3 = lock_manager.acquire_lock(resource, LockType.INTENT_SHARE, 3)
        
        assert result1 is True
        assert result2 is True
        assert result3 is True
        assert len(lock_manager.get_lock_holders(resource)) == 3

    def test_intent_exclusive_blocks_share(self, lock_manager):
        """Test IX blocks S lock."""
        resource = "table:users"
        
        lock_manager.acquire_lock(resource, LockType.INTENT_EXCLUSIVE, 1)
        
        with pytest.raises(LockConflictError):
            lock_manager.acquire_lock(
                resource, LockType.SHARE, 2, mode=LockMode.NOWAIT
            )

    def test_intent_lock_hierarchy(self, lock_manager):
        """Test intent lock hierarchy (table -> row)."""
        table_resource = "table:users"
        row_resource = "row:users:1"
        
        # Acquire IX on table
        result1 = lock_manager.acquire_lock(
            table_resource, LockType.INTENT_EXCLUSIVE, 1
        )
        
        # Acquire X on row
        result2 = lock_manager.acquire_lock(
            row_resource, LockType.EXCLUSIVE, 1
        )
        
        assert result1 is True
        assert result2 is True


# END_BLOCK_INTENT_LOCKS


# =============================================================================
# START_BLOCK_WAIT_FOR_GRAPH
# =============================================================================

class TestWaitForGraph:
    """Tests for wait-for graph generation."""

    def test_empty_wait_for_graph(self, lock_manager):
        """Test empty wait-for graph when no waiting."""
        wfg = lock_manager.get_wait_for_graph()
        assert wfg == {}

    def test_get_waiting_transactions_empty(self, lock_manager):
        """Test get_waiting_transactions returns empty set."""
        waiting = lock_manager.get_waiting_transactions()
        assert waiting == set()

    def test_get_lock_holders_empty(self, lock_manager):
        """Test get_lock_holders returns empty set for non-existent resource."""
        holders = lock_manager.get_lock_holders("nonexistent")
        assert holders == set()

    def test_get_lock_holders_with_locks(self, lock_manager):
        """Test get_lock_holders returns correct holders."""
        resource = "table:users:1"
        
        lock_manager.acquire_lock(resource, LockType.SHARE, 1)
        lock_manager.acquire_lock(resource, LockType.SHARE, 2)
        
        holders = lock_manager.get_lock_holders(resource)
        
        assert holders == {1, 2}


# END_BLOCK_WAIT_FOR_GRAPH


# =============================================================================
# START_BLOCK_CONCURRENT_ACCESS
# =============================================================================

class TestConcurrentAccess:
    """Tests for concurrent lock access."""

    def test_concurrent_share_locks(self, lock_manager):
        """Test multiple transactions acquiring share locks concurrently."""
        resource = "table:users:1"
        results = []
        lock = threading.Lock()
        
        def acquire_share(xid):
            result = lock_manager.acquire_lock(
                resource, LockType.SHARE, xid, timeout=1.0
            )
            with lock:
                results.append((xid, result))
        
        threads = [
            threading.Thread(target=acquire_share, args=(i,))
            for i in range(1, 6)
        ]
        
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        
        # All should succeed
        assert len(results) == 5
        for xid, result in results:
            assert result is True

    def test_sequential_exclusive_locks(self, lock_manager):
        """Test exclusive locks acquired sequentially."""
        resource = "table:users:1"
        
        # T1 acquires and releases
        lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, 1)
        lock_manager.release_lock(resource, 1)
        
        # T2 acquires and releases
        lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, 2)
        lock_manager.release_lock(resource, 2)
        
        # T3 acquires and releases
        lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, 3)
        lock_manager.release_lock(resource, 3)
        
        # All should succeed
        assert not lock_manager.is_locked(resource)

    def test_sequential_mixed_locks(self, lock_manager):
        """Test sequential share and exclusive locks."""
        resource = "table:users:1"
        
        # 3 share locks
        for i in range(1, 4):
            lock_manager.acquire_lock(resource, LockType.SHARE, i)
        
        # Release all share locks
        for i in range(1, 4):
            lock_manager.release_lock(resource, i)
        
        # Now exclusive lock should succeed
        result = lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, 4)
        assert result is True
        
        lock_manager.release_lock(resource, 4)
        assert not lock_manager.is_locked(resource)


# END_BLOCK_CONCURRENT_ACCESS


# =============================================================================
# START_BLOCK_ADVERSARIAL
# =============================================================================

class TestAdversarialCases:
    """Adversarial tests for edge cases."""

    def test_empty_resource_name(self, lock_manager):
        """Test acquiring lock with empty resource name."""
        result = lock_manager.acquire_lock("", LockType.SHARE, 1)
        assert result is True

    def test_very_long_resource_name(self, lock_manager):
        """Test acquiring lock with very long resource name."""
        resource = "table:" + "a" * 10000
        
        result = lock_manager.acquire_lock(resource, LockType.SHARE, 1)
        
        assert result is True
        assert lock_manager.is_locked(resource)

    def test_special_characters_in_resource(self, lock_manager):
        """Test acquiring lock with special characters in resource name."""
        resource = "table:users:row:1:col:name"
        
        result = lock_manager.acquire_lock(resource, LockType.SHARE, 1)
        
        assert result is True

    def test_large_xid_values(self, lock_manager):
        """Test acquiring lock with large XID values."""
        resource = "table:users:1"
        xid = 2**63 - 1  # Max int64
        
        result = lock_manager.acquire_lock(resource, LockType.SHARE, xid)
        
        assert result is True
        assert xid in lock_manager.get_lock_holders(resource)

    def test_negative_xid(self, lock_manager):
        """Test acquiring lock with negative XID."""
        resource = "table:users:1"
        xid = -1
        
        result = lock_manager.acquire_lock(resource, LockType.SHARE, xid)
        
        assert result is True

    def test_zero_timeout(self, lock_manager):
        """Test acquiring lock with zero timeout."""
        resource = "table:users:1"
        
        lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, 1)
        
        with pytest.raises((LockTimeoutError, LockConflictError)):
            lock_manager.acquire_lock(
                resource, LockType.SHARE, 2, timeout=0.0
            )

    def test_very_short_timeout(self, lock_manager):
        """Test acquiring lock with very short timeout."""
        resource = "table:users:1"
        
        lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, 1)
        
        start = time.time()
        
        with pytest.raises(LockTimeoutError):
            lock_manager.acquire_lock(
                resource, LockType.SHARE, 2, timeout=0.001
            )
        
        elapsed = time.time() - start
        assert elapsed < 0.1

    def test_many_locks_same_transaction(self, lock_manager):
        """Test acquiring many locks for same transaction."""
        xid = 1
        
        for i in range(1000):
            result = lock_manager.acquire_lock(
                f"table:users:{i}", LockType.SHARE, xid
            )
            assert result is True
        
        locks = lock_manager.get_locks_held_by(xid)
        assert len(locks) == 1000

    def test_release_all_locks_empty(self, lock_manager):
        """Test release_all_locks when no locks held."""
        count = lock_manager.release_all_locks(1)
        assert count == 0

    def test_multiple_release_same_lock(self, lock_manager):
        """Test releasing same lock multiple times."""
        resource = "table:users:1"
        
        lock_manager.acquire_lock(resource, LockType.SHARE, 1)
        
        result1 = lock_manager.release_lock(resource, 1)
        result2 = lock_manager.release_lock(resource, 1)
        result3 = lock_manager.release_lock(resource, 1)
        
        assert result1 is True
        assert result2 is False
        assert result3 is False


# END_BLOCK_ADVERSARIAL


# =============================================================================
# START_BLOCK_HELPERS
# =============================================================================

class TestHelperFunctions:
    """Tests for helper functions."""

    def test_resource_key_table_only(self):
        """Test resource_key with table only."""
        key = resource_key("users")
        assert key == "table:users"

    def test_resource_key_with_row(self):
        """Test resource_key with table and row."""
        key = resource_key("users", 1)
        assert key == "row:users:1"

    def test_resource_key_large_row_id(self):
        """Test resource_key with large row ID."""
        key = resource_key("users", 2**63 - 1)
        assert key == f"row:users:{2**63 - 1}"

    def test_create_lock_manager(self):
        """Test create_lock_manager factory function."""
        lm = create_lock_manager()
        assert isinstance(lm, LockManager)

    def test_create_lock_manager_with_detector(self):
        """Test create_lock_manager with deadlock detector."""
        from mini_db_v2.concurrency.deadlock import DeadlockDetector
        
        detector = DeadlockDetector()
        lm = create_lock_manager(deadlock_detector=detector)
        
        assert isinstance(lm, LockManager)


# END_BLOCK_HELPERS


# =============================================================================
# START_BLOCK_DATA_STRUCTURES
# =============================================================================

class TestDataStructures:
    """Tests for data structures (LockEntry, WaitEntry)."""

    def test_lock_entry_creation(self):
        """Test LockEntry creation."""
        entry = LockEntry(
            resource="table:users:1",
            lock_type=LockType.SHARE,
            xid=1
        )
        
        assert entry.resource == "table:users:1"
        assert entry.lock_type == LockType.SHARE
        assert entry.xid == 1
        assert entry.acquired_at is not None
        assert entry.waiting_since is None

    def test_lock_entry_is_waiting(self):
        """Test LockEntry.is_waiting method."""
        entry1 = LockEntry(
            resource="table:users:1",
            lock_type=LockType.SHARE,
            xid=1
        )
        
        entry2 = LockEntry(
            resource="table:users:1",
            lock_type=LockType.SHARE,
            xid=2,
            waiting_since=datetime.now()
        )
        
        assert entry1.is_waiting() is False
        assert entry2.is_waiting() is True

    def test_wait_entry_creation(self):
        """Test WaitEntry creation."""
        entry = WaitEntry(
            resource="table:users:1",
            lock_type=LockType.SHARE,
            xid=2,
            blocked_by={1}
        )
        
        assert entry.resource == "table:users:1"
        assert entry.lock_type == LockType.SHARE
        assert entry.xid == 2
        assert entry.blocked_by == {1}
        assert entry.timeout == 30.0

    def test_wait_entry_is_timeout_expired(self):
        """Test WaitEntry.is_timeout_expired method."""
        entry1 = WaitEntry(
            resource="table:users:1",
            lock_type=LockType.SHARE,
            xid=2,
            blocked_by={1},
            timeout=10.0
        )
        
        entry2 = WaitEntry(
            resource="table:users:1",
            lock_type=LockType.SHARE,
            xid=3,
            blocked_by={1},
            waiting_since=datetime.now() - timedelta(seconds=60),
            timeout=30.0
        )
        
        assert entry1.is_timeout_expired() is False
        assert entry2.is_timeout_expired() is True


# END_BLOCK_DATA_STRUCTURES


# =============================================================================
# START_BLOCK_INTEGRATION
# =============================================================================

class TestLockManagerIntegration:
    """Integration tests for Lock Manager."""

    def test_full_transaction_lifecycle(self, lock_manager):
        """Test full transaction lifecycle with locks."""
        xid = 1
        
        # Acquire multiple locks
        lock_manager.acquire_lock("table:users:1", LockType.SHARE, xid)
        lock_manager.acquire_lock("table:users:2", LockType.EXCLUSIVE, xid)
        lock_manager.acquire_lock("table:orders", LockType.INTENT_EXCLUSIVE, xid)
        
        # Verify all held
        assert len(lock_manager.get_locks_held_by(xid)) == 3
        
        # Release all
        count = lock_manager.release_all_locks(xid)
        
        assert count == 3
        assert len(lock_manager.get_locks_held_by(xid)) == 0

    def test_lock_manager_with_deadlock_detector(self, lock_manager_with_detector):
        """Test LockManager with deadlock detector integration."""
        lm = lock_manager_with_detector
        resource = "table:users:1"
        
        # Acquire lock
        lm.acquire_lock(resource, LockType.EXCLUSIVE, 1)
        
        # Try to acquire conflicting lock (should timeout, not deadlock)
        with pytest.raises(LockTimeoutError):
            lm.acquire_lock(resource, LockType.EXCLUSIVE, 2, timeout=0.5)


# END_BLOCK_INTEGRATION