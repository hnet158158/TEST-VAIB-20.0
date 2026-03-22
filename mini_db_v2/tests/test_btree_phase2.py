# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_btree_phase2
# Intent: Comprehensive tests for B+tree Phase 2 - split, merge, range scans.
# Dependencies: pytest
# END_MODULE_CONTRACT

"""
Phase 2 B-Tree Index - Comprehensive Tests

Tests cover:
- Insert with automatic split
- Delete with merge/redistribute
- Range scans (>, <, >=, <=, BETWEEN)
- Thread safety
- Edge cases and adversarial tests
"""

import pytest
import sys
import os
import threading
import random

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from mini_db_v2.storage.btree import (
    BTree, BTreeNode, BTreeError, DuplicateKeyError, KeyNotFoundError,
    create_btree_index
)


# =============================================================================
# B+Tree Split Tests
# =============================================================================

class TestBTreeSplit:
    """Tests for B+tree split operations."""

    def test_insert_triggers_leaf_split(self):
        """Insert that triggers leaf node split."""
        btree = BTree(order=4)  # Small order to trigger split quickly
        
        # Insert keys to fill a leaf (order=4, so split at 4 keys)
        for i in range(5):
            btree.insert(i, i)
        
        assert btree.size == 5
        # After split, height should increase
        assert btree.height >= 1

    def test_insert_causes_multiple_splits(self):
        """Insert causing multiple splits up the tree."""
        btree = BTree(order=4)
        
        # Insert many keys to cause multiple splits
        for i in range(20):
            btree.insert(i, i)
        
        assert btree.size == 20
        # Verify all keys are findable
        for i in range(20):
            assert i in btree

    def test_split_preserves_order(self):
        """Split preserves key ordering."""
        btree = BTree(order=4)
        
        keys = list(range(20))
        random.shuffle(keys)
        
        for k in keys:
            btree.insert(k, k)
        
        # Get all keys in order
        all_pairs = btree.get_all()
        sorted_keys = [k for k, v in all_pairs]
        
        assert sorted_keys == sorted(sorted_keys)

    def test_split_with_duplicate_keys_non_unique(self):
        """Split with duplicate keys in non-unique tree."""
        btree = BTree(order=4, unique=False)
        
        # Insert duplicates
        for i in range(10):
            btree.insert(i % 5, i)  # Keys: 0,1,2,3,4,0,1,2,3,4
        
        assert btree.size == 10
        
        # Search for key with duplicates
        result = btree.search(0)
        assert len(result) == 2

    def test_split_maintains_leaf_links(self):
        """Split maintains next_leaf pointers for range scan."""
        btree = BTree(order=4)
        
        for i in range(20):
            btree.insert(i, i)
        
        # Range scan should work correctly
        result = btree.range_scan(0, 19)
        assert len(result) == 20
        
        keys = [k for k, v in result]
        assert keys == list(range(20))


# =============================================================================
# B+Tree Delete Tests
# =============================================================================

class TestBTreeDelete:
    """Tests for B+tree delete with rebalancing."""

    def test_delete_single_key(self):
        """Delete single key from tree."""
        btree = BTree()
        btree.insert(42, 0)
        
        count = btree.delete(42)
        assert count == 1
        assert btree.size == 0
        assert 42 not in btree

    def test_delete_non_existent_key(self):
        """Delete key that doesn't exist."""
        btree = BTree()
        btree.insert(1, 0)
        
        count = btree.delete(999)
        assert count == 0
        assert btree.size == 1

    def test_delete_triggers_borrow_from_left(self):
        """Delete that triggers borrow from left sibling."""
        btree = BTree(order=8)  # Small order
        
        # Insert keys to create multiple nodes
        for i in range(20):
            btree.insert(i, i)
        
        # Delete keys to trigger underflow
        for i in range(10):
            btree.delete(i)
        
        assert btree.size == 10
        # Verify remaining keys
        for i in range(10, 20):
            assert i in btree

    def test_delete_triggers_borrow_from_right(self):
        """Delete that triggers borrow from right sibling."""
        btree = BTree(order=8)
        
        for i in range(20):
            btree.insert(i, i)
        
        # Delete from the right side
        for i in range(19, 9, -1):
            btree.delete(i)
        
        assert btree.size == 10

    def test_delete_triggers_merge(self):
        """Delete that triggers node merge."""
        btree = BTree(order=8)
        
        for i in range(15):
            btree.insert(i, i)
        
        # Delete many keys to trigger merge
        for i in range(5, 15):
            btree.delete(i)
        
        assert btree.size == 5

    def test_delete_all_keys(self):
        """Delete all keys from tree."""
        btree = BTree(order=4)
        
        for i in range(20):
            btree.insert(i, i)
        
        for i in range(20):
            btree.delete(i)
        
        assert btree.size == 0
        assert btree.is_empty

    def test_delete_reduces_height(self):
        """Delete that reduces tree height."""
        btree = BTree(order=4)
        
        # Insert to create height > 1
        for i in range(20):
            btree.insert(i, i)
        
        initial_height = btree.height
        
        # Delete most keys
        for i in range(18):
            btree.delete(i)
        
        # Height should decrease or stay same
        assert btree.height <= initial_height


# =============================================================================
# B+Tree Range Scan Tests
# =============================================================================

class TestBTreeRangeScan:
    """Tests for B+tree range scan operations."""

    def test_range_scan_full_range(self):
        """Range scan covering all keys."""
        btree = BTree()
        for i in range(100):
            btree.insert(i, i)
        
        result = btree.range_scan(0, 99)
        assert len(result) == 100

    def test_range_scan_partial(self):
        """Range scan for partial range."""
        btree = BTree()
        for i in range(100):
            btree.insert(i, i)
        
        result = btree.range_scan(25, 75)
        assert len(result) == 51  # 25 to 75 inclusive

    def test_range_scan_exclusive_low(self):
        """Range scan with exclusive low bound."""
        btree = BTree()
        for i in range(10):
            btree.insert(i, i)
        
        result = btree.range_scan(3, 7, low_inclusive=False)
        keys = [k for k, v in result]
        assert 3 not in keys
        assert 4 in keys
        assert 7 in keys

    def test_range_scan_exclusive_high(self):
        """Range scan with exclusive high bound."""
        btree = BTree()
        for i in range(10):
            btree.insert(i, i)
        
        result = btree.range_scan(3, 7, high_inclusive=False)
        keys = [k for k, v in result]
        assert 3 in keys
        assert 7 not in keys
        assert 6 in keys

    def test_range_scan_both_exclusive(self):
        """Range scan with both bounds exclusive."""
        btree = BTree()
        for i in range(10):
            btree.insert(i, i)
        
        result = btree.range_scan(3, 7, low_inclusive=False, high_inclusive=False)
        keys = [k for k, v in result]
        assert keys == [4, 5, 6]

    def test_range_scan_no_results(self):
        """Range scan with no matching keys."""
        btree = BTree()
        for i in range(10):
            btree.insert(i, i)
        
        result = btree.range_scan(100, 200)
        assert result == []

    def test_range_scan_empty_tree(self):
        """Range scan on empty tree."""
        btree = BTree()
        result = btree.range_scan(0, 100)
        assert result == []

    def test_range_scan_single_key(self):
        """Range scan returning single key."""
        btree = BTree()
        for i in range(10):
            btree.insert(i, i)
        
        result = btree.range_scan(5, 5)
        assert len(result) == 1
        assert result[0] == (5, 5)

    def test_range_scan_iter(self):
        """Range scan using iterator."""
        btree = BTree()
        for i in range(100):
            btree.insert(i, i)
        
        result = list(btree.range_scan_iter(10, 20))
        assert len(result) == 11

    def test_range_scan_with_strings(self):
        """Range scan with string keys."""
        btree = BTree()
        words = ["apple", "banana", "cherry", "date", "elderberry"]
        for i, word in enumerate(words):
            btree.insert(word, i)
        
        result = btree.range_scan("banana", "date")
        keys = [k for k, v in result]
        assert "banana" in keys
        assert "cherry" in keys
        assert "date" in keys
        assert "apple" not in keys


# =============================================================================
# B+Tree Min/Max Tests
# =============================================================================

class TestBTreeMinMax:
    """Tests for min/max key operations."""

    def test_min_key_empty_tree(self):
        """Min key on empty tree."""
        btree = BTree()
        assert btree.min_key() is None

    def test_max_key_empty_tree(self):
        """Max key on empty tree."""
        btree = BTree()
        assert btree.max_key() is None

    def test_min_key_single(self):
        """Min key with single entry."""
        btree = BTree()
        btree.insert(42, 0)
        assert btree.min_key() == 42

    def test_max_key_single(self):
        """Max key with single entry."""
        btree = BTree()
        btree.insert(42, 0)
        assert btree.max_key() == 42

    def test_min_key_multiple(self):
        """Min key with multiple entries."""
        btree = BTree()
        for i in [50, 25, 75, 10, 90]:
            btree.insert(i, i)
        assert btree.min_key() == 10

    def test_max_key_multiple(self):
        """Max key with multiple entries."""
        btree = BTree()
        for i in [50, 25, 75, 10, 90]:
            btree.insert(i, i)
        assert btree.max_key() == 90

    def test_min_max_after_delete(self):
        """Min/max after deletion."""
        btree = BTree()
        for i in range(10):
            btree.insert(i, i)
        
        btree.delete(0)
        assert btree.min_key() == 1
        
        btree.delete(9)
        assert btree.max_key() == 8


# =============================================================================
# B+Tree Unique Constraint Tests
# =============================================================================

class TestBTreeUnique:
    """Tests for unique constraint."""

    def test_unique_insert_different_keys(self):
        """Insert different keys in unique tree."""
        btree = BTree(unique=True)
        btree.insert(1, 0)
        btree.insert(2, 1)
        btree.insert(3, 2)
        
        assert btree.size == 3

    def test_unique_insert_duplicate_error(self):
        """Insert duplicate key in unique tree raises error."""
        btree = BTree(unique=True)
        btree.insert(1, 0)
        
        with pytest.raises(DuplicateKeyError):
            btree.insert(1, 1)

    def test_unique_delete_and_reinsert(self):
        """Delete and reinsert same key in unique tree."""
        btree = BTree(unique=True)
        btree.insert(1, 0)
        btree.delete(1)
        
        # Should not raise
        btree.insert(1, 1)
        assert btree.size == 1


# =============================================================================
# B+Tree Thread Safety Tests
# =============================================================================

class TestBTreeThreadSafety:
    """Tests for thread safety."""

    def test_concurrent_inserts(self):
        """Concurrent inserts from multiple threads."""
        btree = BTree()
        errors = []
        
        def insert_keys(start, count):
            try:
                for i in range(start, start + count):
                    btree.insert(i, i)
            except Exception as e:
                errors.append(e)
        
        threads = [
            threading.Thread(target=insert_keys, args=(0, 100)),
            threading.Thread(target=insert_keys, args=(100, 100)),
            threading.Thread(target=insert_keys, args=(200, 100)),
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        assert btree.size == 300

    def test_concurrent_reads_and_writes(self):
        """Concurrent reads and writes."""
        btree = BTree()
        errors = []
        
        # Pre-populate
        for i in range(100):
            btree.insert(i, i)
        
        def insert_more():
            try:
                for i in range(100, 200):
                    btree.insert(i, i)
            except Exception as e:
                errors.append(e)
        
        def read_keys():
            try:
                for i in range(100):
                    _ = btree.search(i)
            except Exception as e:
                errors.append(e)
        
        threads = [
            threading.Thread(target=insert_more),
            threading.Thread(target=read_keys),
            threading.Thread(target=read_keys),
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0

    def test_concurrent_range_scans(self):
        """Concurrent range scans."""
        btree = BTree()
        errors = []
        
        for i in range(1000):
            btree.insert(i, i)
        
        def scan_range(low, high):
            try:
                result = btree.range_scan(low, high)
                assert len(result) == high - low + 1
            except Exception as e:
                errors.append(e)
        
        threads = [
            threading.Thread(target=scan_range, args=(0, 99)),
            threading.Thread(target=scan_range, args=(100, 199)),
            threading.Thread(target=scan_range, args=(200, 299)),
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0


# =============================================================================
# B+Tree Edge Cases Tests
# =============================================================================

class TestBTreeEdgeCases:
    """Tests for edge cases and adversarial inputs."""

    def test_insert_negative_keys(self):
        """Insert negative keys."""
        btree = BTree()
        for i in range(-10, 10):
            btree.insert(i, i)
        
        assert btree.size == 20
        assert btree.min_key() == -10
        assert btree.max_key() == 9

    def test_insert_large_keys(self):
        """Insert very large keys."""
        btree = BTree()
        large_keys = [10**10, 10**15, 10**20]
        for k in large_keys:
            btree.insert(k, k)
        
        assert btree.size == 3

    def test_insert_string_keys(self):
        """Insert string keys."""
        btree = BTree()
        btree.insert("hello", 1)
        btree.insert("world", 2)
        btree.insert("abc", 3)
        
        assert btree.size == 3
        assert "hello" in btree

    def test_insert_float_keys(self):
        """Insert float keys."""
        btree = BTree()
        btree.insert(3.14, 1)
        btree.insert(2.71, 2)
        btree.insert(1.41, 3)
        
        assert btree.size == 3

    def test_insert_reverse_order(self):
        """Insert keys in reverse order."""
        btree = BTree(order=4)
        for i in range(100, 0, -1):
            btree.insert(i, i)
        
        assert btree.size == 100
        assert btree.min_key() == 1
        assert btree.max_key() == 100

    def test_insert_same_key_multiple_values_non_unique(self):
        """Insert same key with multiple values in non-unique tree."""
        btree = BTree(unique=False)
        btree.insert(42, 1)
        btree.insert(42, 2)
        btree.insert(42, 3)
        
        result = btree.search(42)
        assert len(result) == 3
        assert set(result) == {1, 2, 3}

    def test_delete_duplicate_keys(self):
        """Delete key with multiple values."""
        btree = BTree(unique=False)
        btree.insert(42, 1)
        btree.insert(42, 2)
        btree.insert(42, 3)
        
        count = btree.delete(42)
        assert count == 3
        assert btree.size == 0

    def test_range_scan_with_duplicates(self):
        """Range scan with duplicate keys."""
        btree = BTree(unique=False)
        for i in range(5):
            btree.insert(10, i)
        for i in range(5):
            btree.insert(20, i)
        
        result = btree.range_scan(5, 25)
        assert len(result) == 10


# =============================================================================
# B+Tree Stress Tests
# =============================================================================

class TestBTreeStress:
    """Stress tests for B+tree."""

    def test_insert_1000_keys(self):
        """Insert 1000 keys."""
        btree = BTree()
        for i in range(1000):
            btree.insert(i, i)
        
        assert btree.size == 1000
        
        # Verify all keys
        for i in range(1000):
            assert i in btree

    def test_insert_10000_keys(self):
        """Insert 10000 keys."""
        btree = BTree()
        for i in range(10000):
            btree.insert(i, i)
        
        assert btree.size == 10000

    def test_insert_random_keys(self):
        """Insert random keys."""
        btree = BTree()
        keys = list(range(1000))
        random.shuffle(keys)
        
        for k in keys:
            btree.insert(k, k)
        
        assert btree.size == 1000
        
        # Verify ordering
        all_pairs = btree.get_all()
        sorted_keys = [k for k, v in all_pairs]
        assert sorted_keys == sorted(sorted_keys)

    def test_insert_delete_cycle(self):
        """Insert and delete cycle."""
        btree = BTree(order=8)
        
        # Insert
        for i in range(100):
            btree.insert(i, i)
        
        # Delete half
        for i in range(0, 50):
            btree.delete(i)
        
        assert btree.size == 50
        
        # Insert new keys
        for i in range(100, 150):
            btree.insert(i, i)
        
        assert btree.size == 100


# =============================================================================
# Run Tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])