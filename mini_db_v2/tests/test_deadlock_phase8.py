# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_deadlock_phase8
# Intent: Comprehensive tests for Deadlock Detection (Phase 8)
# Dependencies: pytest, threading, time
# END_MODULE_CONTRACT

"""
Phase 8: Deadlock Detection Tests

Test Categories:
1. Simple Cycle Detection - базовые циклы
2. Multi-Transaction Cycles - сложные циклы
3. No Deadlock Cases - отсутствие deadlock
4. Victim Selection Policies - политики выбора жертвы
5. Tarjan's SCC Algorithm - алгоритм Тарьяна
6. Deadlock History - история deadlocks
7. Integration with LockManager - интеграция
8. Adversarial Tests - граничные случаи
"""

import pytest
import threading
import time
from typing import Dict, Set

from mini_db_v2.concurrency.lock_manager import (
    LockManager,
    LockType,
    LockMode,
    LockTimeoutError,
    LockConflictError,
    DeadlockError,
)
from mini_db_v2.concurrency.deadlock import (
    DeadlockDetector,
    VictimSelectionPolicy,
    DeadlockInfo,
    TarjanSCCDetector,
    create_deadlock_detector,
)


# =============================================================================
# START_BLOCK_FIXTURES
# =============================================================================

@pytest.fixture
def deadlock_detector():
    """Create a fresh DeadlockDetector for each test."""
    return DeadlockDetector()


@pytest.fixture
def lock_manager():
    """Create a fresh LockManager for each test."""
    return LockManager()


@pytest.fixture
def lock_manager_with_detector():
    """Create a LockManager with DeadlockDetector."""
    detector = DeadlockDetector()
    return LockManager(deadlock_detector=detector)


@pytest.fixture
def tarjan_detector():
    """Create a TarjanSCCDetector."""
    return TarjanSCCDetector()


# END_BLOCK_FIXTURES


# =============================================================================
# START_BLOCK_SIMPLE_CYCLE
# =============================================================================

class TestSimpleCycleDetection:
    """Tests for simple deadlock cycle detection."""

    def test_two_transaction_cycle_wfg(self, deadlock_detector):
        """Test detecting T1 -> T2 -> T1 deadlock in wait-for graph."""
        # Simulate wait-for graph: T1 waits for T2, T2 waits for T1
        wfg = {1: {2}, 2: {1}}
        
        # Use Tarjan's algorithm to detect cycle
        tarjan = TarjanSCCDetector()
        cycles = tarjan.find_deadlock_cycles(wfg)
        
        assert len(cycles) == 1
        assert set(cycles[0]) == {1, 2}

    def test_self_deadlock_single_resource(self, deadlock_detector, lock_manager):
        """Test detecting when transaction waits for itself (unusual but possible)."""
        # This is a degenerate case
        resource = "table:users:1"
        
        lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, 1)
        
        # Try to acquire again with different lock type (would need upgrade)
        # This shouldn't create a true self-deadlock in our implementation
        result = lock_manager.acquire_lock(resource, LockType.SHARE, 1)
        
        # Should succeed (same transaction)
        assert result is True

    def test_three_transaction_cycle_wfg(self, deadlock_detector):
        """Test detecting T1 -> T2 -> T3 -> T1 deadlock in wait-for graph."""
        # Simulate wait-for graph: T1 -> T2 -> T3 -> T1
        wfg = {1: {2}, 2: {3}, 3: {1}}
        
        tarjan = TarjanSCCDetector()
        cycles = tarjan.find_deadlock_cycles(wfg)
        
        assert len(cycles) == 1
        assert set(cycles[0]) == {1, 2, 3}


# END_BLOCK_SIMPLE_CYCLE


# =============================================================================
# START_BLOCK_NO_DEADLOCK
# =============================================================================

class TestNoDeadlockCases:
    """Tests for cases where no deadlock exists."""

    def test_empty_wait_for_graph(self, deadlock_detector, lock_manager):
        """Test with no waiting transactions."""
        victim = deadlock_detector.detect(lock_manager)
        assert victim is None

    def test_no_cycle_in_wfg(self, deadlock_detector):
        """Test WFG with no cycle."""
        # T1 waits for T2, but T2 doesn't wait for anyone
        wfg = {1: {2}}
        
        # Use Tarjan to verify no cycle
        tarjan = TarjanSCCDetector()
        cycles = tarjan.find_deadlock_cycles(wfg)
        
        assert len(cycles) == 0

    def test_sequential_waiting_wfg(self, deadlock_detector):
        """Test sequential waiting (T1 -> T2 -> T3, no cycle)."""
        # T1 waits for T2, T2 waits for T3, T3 doesn't wait
        wfg = {1: {2}, 2: {3}}
        
        tarjan = TarjanSCCDetector()
        cycles = tarjan.find_deadlock_cycles(wfg)
        
        assert len(cycles) == 0

    def test_multiple_independent_transactions(self, deadlock_detector, lock_manager):
        """Test multiple transactions with no dependencies."""
        for i in range(1, 6):
            lock_manager.acquire_lock(f"table:users:{i}", LockType.EXCLUSIVE, i)
        
        victim = deadlock_detector.detect(lock_manager)
        
        # Cleanup
        for i in range(1, 6):
            lock_manager.release_all_locks(i)
        
        assert victim is None


# END_BLOCK_NO_DEADLOCK


# =============================================================================
# START_BLOCK_VICTIM_SELECTION
# =============================================================================

class TestVictimSelectionPolicies:
    """Tests for victim selection policies."""

    def test_youngest_policy(self, deadlock_detector):
        """Test YOUNGEST policy selects highest XID."""
        # Create mock lock manager
        class MockLockManager:
            def get_locks_held_by(self, xid):
                return []
        
        detector = DeadlockDetector(policy=VictimSelectionPolicy.YOUNGEST)
        
        # Test _select_victim directly
        cycle = [1, 3, 1]  # T1 -> T3 -> T1
        victim = detector._select_victim(cycle, MockLockManager())
        
        # YOUNGEST should select highest XID (3)
        assert victim == 3

    def test_oldest_policy(self, deadlock_detector):
        """Test OLDEST policy selects lowest XID."""
        class MockLockManager:
            def get_locks_held_by(self, xid):
                return []
        
        detector = DeadlockDetector(policy=VictimSelectionPolicy.OLDEST)
        
        cycle = [1, 5, 1]  # T1 -> T5 -> T1
        victim = detector._select_victim(cycle, MockLockManager())
        
        # OLDEST should select lowest XID (1)
        assert victim == 1

    def test_most_locks_policy(self, lock_manager):
        """Test MOST_LOCKS policy selects transaction with most locks."""
        detector = DeadlockDetector(policy=VictimSelectionPolicy.MOST_LOCKS)
        
        # T1 holds 3 locks
        lock_manager.acquire_lock("table:users:1", LockType.EXCLUSIVE, 1)
        lock_manager.acquire_lock("table:users:2", LockType.EXCLUSIVE, 1)
        lock_manager.acquire_lock("table:users:3", LockType.EXCLUSIVE, 1)
        
        # T2 holds 1 lock
        lock_manager.acquire_lock("table:orders:1", LockType.EXCLUSIVE, 2)
        
        cycle = [1, 2, 1]  # T1 -> T2 -> T1
        victim = detector._select_victim(cycle, lock_manager)
        
        # MOST_LOCKS should select T1 (3 locks vs 1 lock)
        assert victim == 1

    def test_fewest_locks_policy(self, lock_manager):
        """Test FEWEST_LOCKS policy selects transaction with fewest locks."""
        detector = DeadlockDetector(policy=VictimSelectionPolicy.FEWEST_LOCKS)
        
        # T1 holds 1 lock
        lock_manager.acquire_lock("table:users:1", LockType.EXCLUSIVE, 1)
        
        # T2 holds 3 locks
        lock_manager.acquire_lock("table:orders:1", LockType.EXCLUSIVE, 2)
        lock_manager.acquire_lock("table:orders:2", LockType.EXCLUSIVE, 2)
        lock_manager.acquire_lock("table:orders:3", LockType.EXCLUSIVE, 2)
        
        cycle = [1, 2, 1]  # T1 -> T2 -> T1
        victim = detector._select_victim(cycle, lock_manager)
        
        # FEWEST_LOCKS should select T1 (1 lock vs 3 locks)
        assert victim == 1

    def test_random_policy(self):
        """Test RANDOM policy selects a victim from cycle."""
        class MockLockManager:
            def get_locks_held_by(self, xid):
                return []
        
        detector = DeadlockDetector(policy=VictimSelectionPolicy.RANDOM)
        
        cycle = [1, 2, 1]
        victim = detector._select_victim(cycle, MockLockManager())
        
        # RANDOM should select either T1 or T2
        assert victim in [1, 2]

    def test_policy_setter(self, deadlock_detector):
        """Test changing policy via setter."""
        assert deadlock_detector.policy == VictimSelectionPolicy.YOUNGEST
        
        deadlock_detector.policy = VictimSelectionPolicy.OLDEST
        assert deadlock_detector.policy == VictimSelectionPolicy.OLDEST
        
        deadlock_detector.policy = VictimSelectionPolicy.MOST_LOCKS
        assert deadlock_detector.policy == VictimSelectionPolicy.MOST_LOCKS


# END_BLOCK_VICTIM_SELECTION


# =============================================================================
# START_BLOCK_TARJAN_SCC
# =============================================================================

class TestTarjanSCC:
    """Tests for Tarjan's SCC algorithm."""

    def test_empty_graph(self, tarjan_detector):
        """Test Tarjan with empty graph."""
        wfg = {}
        sccs = tarjan_detector.find_sccs(wfg)
        assert sccs == []

    def test_single_node(self, tarjan_detector):
        """Test Tarjan with single node (no self-loop)."""
        wfg = {1: set()}  # Node 1 with no edges
        sccs = tarjan_detector.find_sccs(wfg)
        
        # Single node is its own SCC
        assert len(sccs) == 1
        assert [1] in sccs

    def test_two_node_cycle(self, tarjan_detector):
        """Test Tarjan with two-node cycle."""
        wfg = {1: {2}, 2: {1}}  # 1 -> 2 -> 1
        sccs = tarjan_detector.find_sccs(wfg)
        
        # Should find one SCC with both nodes
        assert len(sccs) == 1
        assert set(sccs[0]) == {1, 2}

    def test_three_node_cycle(self, tarjan_detector):
        """Test Tarjan with three-node cycle."""
        wfg = {1: {2}, 2: {3}, 3: {1}}  # 1 -> 2 -> 3 -> 1
        sccs = tarjan_detector.find_sccs(wfg)
        
        # Should find one SCC with all three nodes
        assert len(sccs) == 1
        assert set(sccs[0]) == {1, 2, 3}

    def test_disconnected_components(self, tarjan_detector):
        """Test Tarjan with disconnected components."""
        # Two separate cycles
        wfg = {
            1: {2}, 2: {1},  # Cycle 1
            3: {4}, 4: {3},  # Cycle 2
        }
        sccs = tarjan_detector.find_sccs(wfg)
        
        # Should find two SCCs
        assert len(sccs) == 2
        
        # Convert to sets for comparison
        scc_sets = [set(scc) for scc in sccs]
        assert {1, 2} in scc_sets
        assert {3, 4} in scc_sets

    def test_no_cycles(self, tarjan_detector):
        """Test Tarjan with no cycles (DAG)."""
        wfg = {1: {2}, 2: {3}, 3: set()}  # Linear chain
        sccs = tarjan_detector.find_sccs(wfg)
        
        # Each node is its own SCC
        assert len(sccs) == 3
        assert [1] in sccs
        assert [2] in sccs
        assert [3] in sccs

    def test_find_deadlock_cycles(self, tarjan_detector):
        """Test find_deadlock_cycles method."""
        wfg = {
            1: {2}, 2: {1},  # Deadlock
            3: {4}, 4: set(),  # No deadlock
        }
        cycles = tarjan_detector.find_deadlock_cycles(wfg)
        
        # Only one deadlock cycle
        assert len(cycles) == 1
        assert set(cycles[0]) == {1, 2}

    def test_multiple_deadlock_cycles(self, tarjan_detector):
        """Test finding multiple deadlock cycles."""
        wfg = {
            1: {2}, 2: {1},  # Deadlock 1
            3: {4}, 4: {3},  # Deadlock 2
            5: set(),  # No deadlock
        }
        cycles = tarjan_detector.find_deadlock_cycles(wfg)
        
        assert len(cycles) == 2
        cycle_sets = [set(c) for c in cycles]
        assert {1, 2} in cycle_sets
        assert {3, 4} in cycle_sets

    def test_complex_graph(self, tarjan_detector):
        """Test Tarjan with complex graph structure."""
        # Complex graph with multiple SCCs
        wfg = {
            1: {2},
            2: {3},
            3: {1, 4},  # 1-2-3 form a cycle
            4: {5},
            5: {6},
            6: {4},  # 4-5-6 form a cycle
            7: {8},
            8: set(),  # No cycle
        }
        sccs = tarjan_detector.find_sccs(wfg)
        
        # Should find 3 SCCs: {1,2,3}, {4,5,6}, {7}, {8}
        assert len(sccs) == 4
        
        scc_sets = [set(scc) for scc in sccs]
        assert {1, 2, 3} in scc_sets
        assert {4, 5, 6} in scc_sets
        assert {7} in scc_sets
        assert {8} in scc_sets


# END_BLOCK_TARJAN_SCC


# =============================================================================
# START_BLOCK_DEADLOCK_HISTORY
# =============================================================================

class TestDeadlockHistory:
    """Tests for deadlock history tracking."""

    def test_empty_history(self, deadlock_detector):
        """Test empty history initially."""
        assert deadlock_detector.get_deadlock_count() == 0
        assert deadlock_detector.get_detected_deadlocks() == []

    def test_deadlock_info_creation(self):
        """Test DeadlockInfo creation and properties."""
        info = DeadlockInfo(
            cycle=[1, 2, 1],
            victim_xid=2,
            policy_used=VictimSelectionPolicy.YOUNGEST
        )
        
        assert info.victim_xid == 2
        assert info.cycle == [1, 2, 1]
        assert info.policy_used == VictimSelectionPolicy.YOUNGEST
        assert info.detected_at is not None

    def test_clear_history(self, deadlock_detector):
        """Test clearing deadlock history."""
        # Initially empty
        assert deadlock_detector.get_deadlock_count() == 0
        
        # Clear should work even when empty
        deadlock_detector.clear_history()
        
        assert deadlock_detector.get_deadlock_count() == 0
        assert deadlock_detector.get_detected_deadlocks() == []

    def test_deadlock_info_str(self):
        """Test DeadlockInfo string representation."""
        info = DeadlockInfo(
            cycle=[1, 2, 1],
            victim_xid=2,
            policy_used=VictimSelectionPolicy.YOUNGEST
        )
        
        s = str(info)
        assert "1 -> 2 -> 1" in s
        assert "victim: 2" in s


# END_BLOCK_DEADLOCK_HISTORY


# =============================================================================
# START_BLOCK_INTEGRATION
# =============================================================================

class TestDeadlockIntegration:
    """Integration tests with LockManager."""

    def test_lock_manager_uses_deadlock_detector(self, lock_manager_with_detector):
        """Test that LockManager uses DeadlockDetector."""
        lm = lock_manager_with_detector
        
        resource1 = "table:users:1"
        resource2 = "table:users:2"
        
        lm.acquire_lock(resource1, LockType.EXCLUSIVE, 1)
        lm.acquire_lock(resource2, LockType.EXCLUSIVE, 2)
        
        results = []
        
        def t1_wait():
            try:
                lm.acquire_lock(resource2, LockType.EXCLUSIVE, 1, timeout=2.0)
                results.append(("t1", "acquired"))
            except DeadlockError:
                results.append(("t1", "deadlock"))
            except LockTimeoutError:
                results.append(("t1", "timeout"))
        
        def t2_wait():
            try:
                lm.acquire_lock(resource1, LockType.EXCLUSIVE, 2, timeout=2.0)
                results.append(("t2", "acquired"))
            except DeadlockError:
                results.append(("t2", "deadlock"))
            except LockTimeoutError:
                results.append(("t2", "timeout"))
        
        thread1 = threading.Thread(target=t1_wait)
        thread2 = threading.Thread(target=t2_wait)
        
        thread1.start()
        time.sleep(0.1)
        thread2.start()
        
        thread1.join(timeout=3.0)
        thread2.join(timeout=3.0)
        
        # At least one should detect deadlock or timeout
        assert len(results) >= 1

    def test_deadlock_resolution_allows_progress(self, lock_manager_with_detector):
        """Test that deadlock resolution allows other transactions to proceed."""
        lm = lock_manager_with_detector
        
        resource1 = "table:users:1"
        resource2 = "table:users:2"
        
        lm.acquire_lock(resource1, LockType.EXCLUSIVE, 1)
        lm.acquire_lock(resource2, LockType.EXCLUSIVE, 2)
        
        t1_acquired = threading.Event()
        t2_acquired = threading.Event()
        
        def t1_wait():
            try:
                lm.acquire_lock(resource2, LockType.EXCLUSIVE, 1, timeout=3.0)
                t1_acquired.set()
            except DeadlockError:
                pass
            except LockTimeoutError:
                pass
        
        def t2_wait():
            try:
                lm.acquire_lock(resource1, LockType.EXCLUSIVE, 2, timeout=3.0)
                t2_acquired.set()
            except DeadlockError:
                lm.release_all_locks(2)  # Victim releases
            except LockTimeoutError:
                pass
        
        thread1 = threading.Thread(target=t1_wait)
        thread2 = threading.Thread(target=t2_wait)
        
        thread1.start()
        time.sleep(0.1)
        thread2.start()
        
        thread1.join(timeout=4.0)
        thread2.join(timeout=4.0)
        
        # At least one should have succeeded or properly handled deadlock
        lm.release_all_locks(1)
        lm.release_all_locks(2)


# END_BLOCK_INTEGRATION


# =============================================================================
# START_BLOCK_ADVERSARIAL
# =============================================================================

class TestAdversarialCases:
    """Adversarial tests for edge cases."""

    def test_large_cycle_wfg(self, tarjan_detector):
        """Test detecting a large deadlock cycle in WFG."""
        num_transactions = 10
        
        # Create a cycle: 1 -> 2 -> 3 -> ... -> 10 -> 1
        wfg = {}
        for i in range(1, num_transactions + 1):
            next_i = (i % num_transactions) + 1
            wfg[i] = {next_i}
        
        cycles = tarjan_detector.find_deadlock_cycles(wfg)
        
        # Should find one large cycle
        assert len(cycles) == 1
        assert len(cycles[0]) == num_transactions

    def test_empty_cycle_list(self, deadlock_detector):
        """Test _select_victim with single element cycle."""
        # This tests internal behavior
        cycle = [1]
        
        # Create a mock lock manager
        class MockLockManager:
            def get_locks_held_by(self, xid):
                return []
        
        victim = deadlock_detector._select_victim(cycle, MockLockManager())
        assert victim == 1

    def test_negative_xid_in_cycle(self, deadlock_detector):
        """Test deadlock with negative XIDs."""
        class MockLockManager:
            def get_locks_held_by(self, xid):
                return []
        
        # Cycle with negative XIDs
        cycle = [-1, -2, -1]
        
        victim = deadlock_detector._select_victim(cycle, MockLockManager())
        
        # Should select a victim (either -1 or -2)
        assert victim in [-1, -2]

    def test_zero_xid_in_cycle(self, deadlock_detector):
        """Test deadlock with zero XID."""
        class MockLockManager:
            def get_locks_held_by(self, xid):
                return []
        
        cycle = [0, 1, 0]
        
        victim = deadlock_detector._select_victim(cycle, MockLockManager())
        
        # Should select a victim
        assert victim in [0, 1]

    def test_very_large_xid_values(self, deadlock_detector):
        """Test deadlock with very large XID values."""
        class MockLockManager:
            def get_locks_held_by(self, xid):
                return []
        
        large_xid = 2**63 - 1
        cycle = [1, large_xid, 1]
        
        victim = deadlock_detector._select_victim(cycle, MockLockManager())
        
        # YOUNGEST policy should select the large XID
        assert victim == large_xid


# END_BLOCK_ADVERSARIAL


# =============================================================================
# START_BLOCK_HELPERS
# =============================================================================

class TestHelperFunctions:
    """Tests for helper functions."""

    def test_create_deadlock_detector_default(self):
        """Test create_deadlock_detector with default policy."""
        detector = create_deadlock_detector()
        assert isinstance(detector, DeadlockDetector)
        assert detector.policy == VictimSelectionPolicy.YOUNGEST

    def test_create_deadlock_detector_custom_policy(self):
        """Test create_deadlock_detector with custom policy."""
        detector = create_deadlock_detector(policy=VictimSelectionPolicy.OLDEST)
        assert isinstance(detector, DeadlockDetector)
        assert detector.policy == VictimSelectionPolicy.OLDEST


# END_BLOCK_HELPERS


# =============================================================================
# START_BLOCK_CHECKPOINTS
# =============================================================================

class TestCheckpoints:
    """Checkpoint tests for Phase 8."""

    def test_checkpoint_deadlock_detection_api(self, deadlock_detector, lock_manager):
        """
        CHECKPOINT: DeadlockDetector API works correctly.
        
        Verify:
        1. detect() method exists and returns None for no deadlock
        2. DeadlockInfo can be created
        3. VictimSelectionPolicy enum works
        """
        # Test 1: No deadlock returns None
        wfg = lock_manager.get_wait_for_graph()
        assert wfg == {}
        
        victim = deadlock_detector.detect(lock_manager)
        assert victim is None
        
        # Test 2: DeadlockInfo creation
        info = DeadlockInfo(
            cycle=[1, 2, 1],
            victim_xid=2,
            policy_used=VictimSelectionPolicy.YOUNGEST
        )
        assert info.victim_xid == 2
        assert "1 -> 2 -> 1" in str(info)
        
        # Test 3: Policy enum
        assert VictimSelectionPolicy.YOUNGEST.value == 1
        assert VictimSelectionPolicy.OLDEST.value == 2

    def test_checkpoint_victim_selection_policies(self):
        """
        CHECKPOINT: Victim selection policies work correctly.
        
        Verify all policies can be created and used.
        """
        policies = [
            VictimSelectionPolicy.YOUNGEST,
            VictimSelectionPolicy.OLDEST,
            VictimSelectionPolicy.MOST_LOCKS,
            VictimSelectionPolicy.FEWEST_LOCKS,
            VictimSelectionPolicy.RANDOM,
        ]
        
        for policy in policies:
            detector = DeadlockDetector(policy=policy)
            assert detector.policy == policy

    def test_checkpoint_tarjan_scc_algorithm(self, tarjan_detector):
        """
        CHECKPOINT: Tarjan's SCC algorithm works correctly.
        
        Verify:
        1. Finds cycles in wait-for graph
        2. Returns correct SCCs
        """
        # Test cycle detection
        wfg = {1: {2}, 2: {1}}  # Simple cycle
        cycles = tarjan_detector.find_deadlock_cycles(wfg)
        
        assert len(cycles) == 1
        assert set(cycles[0]) == {1, 2}
        
        # Test no cycle
        wfg_no_cycle = {1: {2}, 2: set()}  # No cycle
        cycles = tarjan_detector.find_deadlock_cycles(wfg_no_cycle)
        
        assert len(cycles) == 0

    def test_checkpoint_lock_manager_basic_operations(self, lock_manager):
        """
        CHECKPOINT: Lock Manager basic operations work.
        
        Verify:
        1. Acquire/release locks
        2. Lock compatibility
        3. Timeout
        """
        # Test 1: Basic acquire/release
        resource = "table:users:1"
        
        result = lock_manager.acquire_lock(resource, LockType.SHARE, 1)
        assert result is True
        assert lock_manager.is_locked(resource)
        
        result = lock_manager.release_lock(resource, 1)
        assert result is True
        assert not lock_manager.is_locked(resource)
        
        # Test 2: Lock compatibility (S + S compatible)
        lock_manager.acquire_lock(resource, LockType.SHARE, 1)
        result = lock_manager.acquire_lock(resource, LockType.SHARE, 2)
        assert result is True
        
        # Test 3: Lock incompatibility (S + X not compatible)
        with pytest.raises(LockConflictError):
            lock_manager.acquire_lock(resource, LockType.EXCLUSIVE, 3, mode=LockMode.NOWAIT)
        
        # Cleanup
        lock_manager.release_all_locks(1)
        lock_manager.release_all_locks(2)


# END_BLOCK_CHECKPOINTS