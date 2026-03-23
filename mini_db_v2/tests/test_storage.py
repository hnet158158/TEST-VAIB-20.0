# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_storage
# Intent: Comprehensive tests for Storage (Database, Table, BTree) - Phase 1 Foundation.
# Dependencies: pytest
# END_MODULE_CONTRACT

"""
Phase 1 Foundation - Storage Tests

Tests cover:
- Database: create_table, drop_table, get_table, table_exists
- Table: insert, select, update, delete, unique constraints, validation
- BTree: insert, search, range_scan, delete (skeleton)
"""

import pytest
import sys
import os
import threading
import time

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from mini_db_v2.storage.database import (
    Database, DatabaseError, TableExistsError, TableNotFoundError
)
from mini_db_v2.storage.table import (
    Table, ColumnDef, Row, DataType, ConstraintType,
    TableError, DuplicateKeyError, ValidationError, ColumnNotFoundError
)
from mini_db_v2.storage.btree import (
    BTree, BTreeNode, BTreeError, create_btree_index
)


# =============================================================================
# Database Tests
# =============================================================================

class TestDatabase:
    """Tests for Database class."""

    def test_database_init(self):
        """Database initialization."""
        db = Database("test_db")
        assert db.name == "test_db"
        assert db.tables == []

    def test_database_default_name(self):
        """Database with default name."""
        db = Database()
        assert db.name == "default"

    def test_create_table(self):
        """Create table."""
        db = Database()
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT, primary_key=True),
            "name": ColumnDef(name="name", data_type=DataType.TEXT),
        }
        table = db.create_table("users", columns)
        assert table.name == "users"
        assert "users" in db.tables

    def test_create_table_duplicate_error(self):
        """Create duplicate table raises error."""
        db = Database()
        columns = {"id": ColumnDef(name="id", data_type=DataType.INT)}
        db.create_table("users", columns)
        
        with pytest.raises(TableExistsError):
            db.create_table("users", columns)

    def test_create_table_if_not_exists(self):
        """Create table with IF NOT EXISTS."""
        db = Database()
        columns = {"id": ColumnDef(name="id", data_type=DataType.INT)}
        
        table1 = db.create_table("users", columns)
        table2 = db.create_table("users", columns, if_not_exists=True)
        
        assert table1 == table2

    def test_drop_table(self):
        """Drop table."""
        db = Database()
        columns = {"id": ColumnDef(name="id", data_type=DataType.INT)}
        db.create_table("users", columns)
        
        db.drop_table("users")
        assert "users" not in db.tables

    def test_drop_table_not_found_error(self):
        """Drop non-existent table raises error."""
        db = Database()
        
        with pytest.raises(TableNotFoundError):
            db.drop_table("nonexistent")

    def test_drop_table_if_exists(self):
        """Drop table with IF EXISTS."""
        db = Database()
        
        # Should not raise
        db.drop_table("nonexistent", if_exists=True)

    def test_get_table(self):
        """Get table by name."""
        db = Database()
        columns = {"id": ColumnDef(name="id", data_type=DataType.INT)}
        created = db.create_table("users", columns)
        
        retrieved = db.get_table("users")
        assert retrieved == created

    def test_get_table_not_found(self):
        """Get non-existent table returns None."""
        db = Database()
        
        result = db.get_table("nonexistent")
        assert result is None

    def test_table_exists(self):
        """Check table existence."""
        db = Database()
        columns = {"id": ColumnDef(name="id", data_type=DataType.INT)}
        
        assert db.table_exists("users") is False
        db.create_table("users", columns)
        assert db.table_exists("users") is True

    def test_clear(self):
        """Clear all tables."""
        db = Database()
        columns = {"id": ColumnDef(name="id", data_type=DataType.INT)}
        db.create_table("t1", columns)
        db.create_table("t2", columns)
        
        db.clear()
        assert db.tables == []


# =============================================================================
# Table Tests
# =============================================================================

class TestTable:
    """Tests for Table class."""

    @pytest.fixture
    def sample_table(self):
        """Create sample table for tests."""
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT, primary_key=True),
            "name": ColumnDef(name="name", data_type=DataType.TEXT, nullable=False),
            "active": ColumnDef(name="active", data_type=DataType.BOOL, default=True),
        }
        return Table("users", columns)

    def test_table_init(self, sample_table):
        """Table initialization."""
        assert sample_table.name == "users"
        assert len(sample_table.columns) == 3
        assert sample_table.row_count == 0

    def test_column_names(self, sample_table):
        """Get column names."""
        assert "id" in sample_table.column_names
        assert "name" in sample_table.column_names
        assert "active" in sample_table.column_names

    def test_insert_basic(self, sample_table):
        """Basic insert."""
        row = sample_table.insert({"id": 1, "name": "John"})
        assert row.row_id == 0
        assert row.data["id"] == 1
        assert row.data["name"] == "John"
        assert sample_table.row_count == 1

    def test_insert_with_default(self, sample_table):
        """Insert with default value."""
        row = sample_table.insert({"id": 1, "name": "John"})
        # active has default=True
        assert row.data["active"] is True

    def test_insert_many(self, sample_table):
        """Insert multiple rows."""
        rows = sample_table.insert_many([
            {"id": 1, "name": "John"},
            {"id": 2, "name": "Jane"},
        ])
        assert len(rows) == 2
        assert sample_table.row_count == 2

    def test_insert_preserves_order(self, sample_table):
        """Insert preserves insertion order."""
        sample_table.insert({"id": 1, "name": "First"})
        sample_table.insert({"id": 2, "name": "Second"})
        sample_table.insert({"id": 3, "name": "Third"})
        
        rows = sample_table.select()
        assert rows[0].data["name"] == "First"
        assert rows[1].data["name"] == "Second"
        assert rows[2].data["name"] == "Third"

    def test_select_all(self, sample_table):
        """Select all rows."""
        sample_table.insert({"id": 1, "name": "John"})
        sample_table.insert({"id": 2, "name": "Jane"})
        
        rows = sample_table.select()
        assert len(rows) == 2

    def test_select_with_columns(self, sample_table):
        """Select specific columns."""
        sample_table.insert({"id": 1, "name": "John", "active": True})
        
        rows = sample_table.select(columns=["id", "name"])
        assert "id" in rows[0].data
        assert "name" in rows[0].data
        assert "active" not in rows[0].data

    def test_select_with_where(self, sample_table):
        """Select with WHERE predicate."""
        sample_table.insert({"id": 1, "name": "John", "active": True})
        sample_table.insert({"id": 2, "name": "Jane", "active": False})
        
        rows = sample_table.select(where=lambda r: r["active"] is True)
        assert len(rows) == 1
        assert rows[0].data["name"] == "John"

    def test_update_all(self, sample_table):
        """Update all rows."""
        sample_table.insert({"id": 1, "name": "John", "active": True})
        sample_table.insert({"id": 2, "name": "Jane", "active": True})
        
        count = sample_table.update({"active": False})
        assert count == 2
        
        rows = sample_table.select()
        assert all(r.data["active"] is False for r in rows)

    def test_update_with_where(self, sample_table):
        """Update with WHERE predicate."""
        sample_table.insert({"id": 1, "name": "John", "active": True})
        sample_table.insert({"id": 2, "name": "Jane", "active": True})
        
        count = sample_table.update(
            {"active": False},
            where=lambda r: r["id"] == 1
        )
        assert count == 1
        
        rows = sample_table.select()
        john = [r for r in rows if r.data["id"] == 1][0]
        jane = [r for r in rows if r.data["id"] == 2][0]
        assert john.data["active"] is False
        assert jane.data["active"] is True

    def test_delete_all(self, sample_table):
        """Delete all rows."""
        sample_table.insert({"id": 1, "name": "John"})
        sample_table.insert({"id": 2, "name": "Jane"})
        
        count = sample_table.delete()
        assert count == 2
        assert sample_table.row_count == 0

    def test_delete_with_where(self, sample_table):
        """Delete with WHERE predicate."""
        sample_table.insert({"id": 1, "name": "John"})
        sample_table.insert({"id": 2, "name": "Jane"})
        
        count = sample_table.delete(where=lambda r: r["id"] == 1)
        assert count == 1
        assert sample_table.row_count == 1
        assert sample_table.select()[0].data["name"] == "Jane"

    def test_get_row_by_id(self, sample_table):
        """Get row by row_id."""
        sample_table.insert({"id": 1, "name": "John"})
        sample_table.insert({"id": 2, "name": "Jane"})
        
        row = sample_table.get_row_by_id(0)
        assert row.data["id"] == 1
        
        row = sample_table.get_row_by_id(1)
        assert row.data["id"] == 2

    def test_get_row_by_id_not_found(self, sample_table):
        """Get row by non-existent row_id."""
        row = sample_table.get_row_by_id(999)
        assert row is None

    def test_clear(self, sample_table):
        """Clear table."""
        sample_table.insert({"id": 1, "name": "John"})
        sample_table.insert({"id": 2, "name": "Jane"})
        
        sample_table.clear()
        assert sample_table.row_count == 0

    def test_len(self, sample_table):
        """Table length."""
        assert len(sample_table) == 0
        sample_table.insert({"id": 1, "name": "John"})
        assert len(sample_table) == 1

    def test_iter(self, sample_table):
        """Iterate over table."""
        sample_table.insert({"id": 1, "name": "John"})
        sample_table.insert({"id": 2, "name": "Jane"})
        
        rows = list(sample_table)
        assert len(rows) == 2

    def test_repr(self, sample_table):
        """Table repr."""
        repr_str = repr(sample_table)
        assert "users" in repr_str
        assert "columns=3" in repr_str


# =============================================================================
# Column Validation Tests
# =============================================================================

class TestColumnValidation:
    """Tests for column validation."""

    def test_validate_int(self):
        """Validate INT type."""
        col = ColumnDef(name="id", data_type=DataType.INT)
        assert col.validate_value(42) is True
        assert col.validate_value(-42) is True
        assert col.validate_value(0) is True
        assert col.validate_value(42.0) is False  # float
        assert col.validate_value("42") is False  # string
        assert col.validate_value(True) is False  # bool

    def test_validate_text(self):
        """Validate TEXT type."""
        col = ColumnDef(name="name", data_type=DataType.TEXT)
        assert col.validate_value("hello") is True
        assert col.validate_value("") is True
        assert col.validate_value("привет") is True
        assert col.validate_value(42) is False
        assert col.validate_value(None) is True  # nullable by default

    def test_validate_real(self):
        """Validate REAL type."""
        col = ColumnDef(name="price", data_type=DataType.REAL)
        assert col.validate_value(3.14) is True
        assert col.validate_value(42) is True  # int is acceptable for REAL
        assert col.validate_value(-3.14) is True
        assert col.validate_value("3.14") is False
        assert col.validate_value(True) is False

    def test_validate_bool(self):
        """Validate BOOL type."""
        col = ColumnDef(name="active", data_type=DataType.BOOL)
        assert col.validate_value(True) is True
        assert col.validate_value(False) is True
        assert col.validate_value(1) is False
        assert col.validate_value("true") is False
        assert col.validate_value(None) is True  # nullable by default

    def test_validate_null(self):
        """Validate NULL values."""
        col_nullable = ColumnDef(name="val", data_type=DataType.INT, nullable=True)
        col_not_null = ColumnDef(name="val", data_type=DataType.INT, nullable=False)
        
        assert col_nullable.validate_value(None) is True
        assert col_not_null.validate_value(None) is False


# =============================================================================
# Unique Constraint Tests
# =============================================================================

class TestUniqueConstraint:
    """Tests for UNIQUE constraint."""

    @pytest.fixture
    def unique_table(self):
        """Create table with unique column."""
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT, unique=True),
            "name": ColumnDef(name="name", data_type=DataType.TEXT),
        }
        return Table("users", columns)

    def test_unique_insert_different(self, unique_table):
        """Insert different values in unique column."""
        unique_table.insert({"id": 1, "name": "John"})
        unique_table.insert({"id": 2, "name": "Jane"})
        assert unique_table.row_count == 2

    def test_unique_insert_duplicate_error(self, unique_table):
        """Insert duplicate value raises error."""
        unique_table.insert({"id": 1, "name": "John"})
        
        with pytest.raises(DuplicateKeyError):
            unique_table.insert({"id": 1, "name": "Jane"})

    def test_unique_update_duplicate_error(self, unique_table):
        """Update to duplicate value raises error."""
        unique_table.insert({"id": 1, "name": "John"})
        unique_table.insert({"id": 2, "name": "Jane"})
        
        with pytest.raises(DuplicateKeyError):
            unique_table.update({"id": 1}, where=lambda r: r["id"] == 2)

    def test_unique_delete_frees_value(self, unique_table):
        """Delete frees unique value for reuse."""
        unique_table.insert({"id": 1, "name": "John"})
        unique_table.delete(where=lambda r: r["id"] == 1)
        
        # Should not raise - value 1 is now free
        unique_table.insert({"id": 1, "name": "Jane"})

    def test_unique_null_allowed(self, unique_table):
        """NULL values are allowed in unique column (multiple NULLs)."""
        unique_table.insert({"id": None, "name": "John"})
        unique_table.insert({"id": None, "name": "Jane"})
        assert unique_table.row_count == 2


# =============================================================================
# Error Handling Tests
# =============================================================================

class TestErrorHandling:
    """Tests for error handling."""

    @pytest.fixture
    def strict_table(self):
        """Create table with strict constraints."""
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT, nullable=False),
            "name": ColumnDef(name="name", data_type=DataType.TEXT, nullable=False),
        }
        return Table("users", columns)

    def test_insert_null_in_not_null(self, strict_table):
        """Insert NULL in NOT NULL column raises error."""
        with pytest.raises(ValidationError):
            strict_table.insert({"id": None, "name": "John"})

    def test_insert_wrong_type_int(self, strict_table):
        """Insert wrong type for INT raises error."""
        with pytest.raises(ValidationError):
            strict_table.insert({"id": "not_an_int", "name": "John"})

    def test_insert_wrong_type_text(self, strict_table):
        """Insert wrong type for TEXT raises error."""
        with pytest.raises(ValidationError):
            strict_table.insert({"id": 1, "name": 123})

    def test_insert_missing_column(self, strict_table):
        """Insert with missing NOT NULL column raises error."""
        with pytest.raises(ValidationError):
            strict_table.insert({"id": 1})  # missing name


# =============================================================================
# BTree Tests
# =============================================================================

class TestBTree:
    """Tests for B+tree skeleton."""

    def test_btree_init(self):
        """BTree initialization."""
        btree = BTree()
        assert btree.order == 64
        assert btree.unique is False
        assert btree.size == 0
        assert btree.is_empty is True

    def test_btree_custom_order(self):
        """BTree with custom order."""
        btree = BTree(order=128, unique=True)
        assert btree.order == 128
        assert btree.unique is True

    def test_btree_insert_single(self):
        """Insert single key."""
        btree = BTree()
        btree.insert(42, 0)  # key, value
        assert btree.size == 1
        assert btree.is_empty is False

    def test_btree_insert_multiple(self):
        """Insert multiple keys."""
        btree = BTree()
        for i in range(10):
            btree.insert(i, i)
        assert btree.size == 10

    def test_btree_search_found(self):
        """Search for existing key."""
        btree = BTree()
        btree.insert(42, 0)
        btree.insert(43, 1)
        
        result = btree.search(42)
        assert result == [0]

    def test_btree_search_not_found(self):
        """Search for non-existing key."""
        btree = BTree()
        btree.insert(42, 0)
        
        result = btree.search(999)
        assert result == []

    def test_btree_search_multiple_values(self):
        """Search in non-unique tree returns multiple values."""
        btree = BTree(unique=False)
        btree.insert(42, 0)
        btree.insert(42, 1)
        btree.insert(42, 2)
        
        result = btree.search(42)
        assert len(result) == 3
        assert 0 in result
        assert 1 in result
        assert 2 in result

    def test_btree_unique_duplicate_error(self):
        """Insert duplicate in unique tree raises error."""
        btree = BTree(unique=True)
        btree.insert(42, 0)
        
        with pytest.raises(Exception):  # DuplicateKeyError from btree module
            btree.insert(42, 1)

    def test_btree_range_scan(self):
        """Range scan."""
        btree = BTree()
        for i in range(100):
            btree.insert(i, i)
        
        result = btree.range_scan(10, 20)
        assert len(result) == 11  # 10 to 20 inclusive
        keys = [k for k, v in result]
        assert min(keys) == 10
        assert max(keys) == 20

    def test_btree_range_scan_empty(self):
        """Range scan on empty tree."""
        btree = BTree()
        result = btree.range_scan(0, 100)
        assert result == []

    def test_btree_range_scan_no_match(self):
        """Range scan with no matching keys."""
        btree = BTree()
        for i in range(10):
            btree.insert(i, i)
        
        result = btree.range_scan(100, 200)
        assert result == []

    def test_btree_delete(self):
        """Delete key."""
        btree = BTree()
        btree.insert(42, 0)
        
        count = btree.delete(42)
        assert count == 1
        assert btree.size == 0
        assert btree.search(42) == []

    def test_btree_delete_not_found(self):
        """Delete non-existing key."""
        btree = BTree()
        btree.insert(42, 0)
        
        count = btree.delete(999)
        assert count == 0
        assert btree.size == 1

    def test_btree_contains(self):
        """Check key existence with 'in' operator."""
        btree = BTree()
        btree.insert(42, 0)
        
        assert 42 in btree
        assert 999 not in btree

    def test_btree_len(self):
        """BTree length."""
        btree = BTree()
        assert len(btree) == 0
        
        btree.insert(1, 0)
        btree.insert(2, 1)
        assert len(btree) == 2

    def test_btree_get_all(self):
        """Get all key-value pairs."""
        btree = BTree()
        for i in range(5):
            btree.insert(i, i)
        
        all_pairs = btree.get_all()
        assert len(all_pairs) == 5

    def test_btree_repr(self):
        """BTree repr."""
        btree = BTree(order=64, unique=True)
        repr_str = repr(btree)
        assert "order=64" in repr_str
        assert "unique=True" in repr_str

    def test_create_btree_index_factory(self):
        """create_btree_index factory function."""
        btree = create_btree_index(order=128, unique=True)
        assert btree.order == 128
        assert btree.unique is True


# =============================================================================
# BTreeNode Tests
# =============================================================================

class TestBTreeNode:
    """Tests for BTreeNode class."""

    def test_btree_node_init(self):
        """BTreeNode initialization."""
        node = BTreeNode(is_leaf=True, order=64)
        assert node.is_leaf is True
        assert node.order == 64
        assert node.keys == []
        assert node.values == []
        assert node.children == []
        assert node.next_leaf is None
        assert node.parent is None

    def test_btree_node_is_full(self):
        """BTreeNode is_full check."""
        node = BTreeNode(is_leaf=True, order=4)
        assert node.is_full() is False
        
        node.keys = [1, 2, 3]
        assert node.is_full() is False
        
        node.keys = [1, 2, 3, 4]
        assert node.is_full() is True

    def test_btree_node_is_underflow(self):
        """BTreeNode is_underflow check."""
        parent = BTreeNode(is_leaf=False, order=10)
        node = BTreeNode(is_leaf=True, order=10, parent=parent)
        # min_keys = 10 // 2 = 5
        node.keys = [1, 2, 3, 4, 5]
        assert node.is_underflow() is False
        
        node.keys = [1, 2, 3, 4]
        assert node.is_underflow() is True


# =============================================================================
# Thread Safety Tests
# =============================================================================

class TestThreadSafety:
    """Tests for thread safety."""

    def test_database_thread_safety(self):
        """Database is thread-safe."""
        db = Database()
        columns = {"id": ColumnDef(name="id", data_type=DataType.INT)}
        
        errors = []
        
        def create_tables(start, count):
            try:
                for i in range(start, start + count):
                    db.create_table(f"table_{i}", columns)
            except Exception as e:
                errors.append(e)
        
        threads = [
            threading.Thread(target=create_tables, args=(0, 10)),
            threading.Thread(target=create_tables, args=(10, 10)),
            threading.Thread(target=create_tables, args=(20, 10)),
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        assert len(db.tables) == 30

    def test_table_thread_safety(self):
        """Table is thread-safe."""
        columns = {"id": ColumnDef(name="id", data_type=DataType.INT)}
        table = Table("users", columns)
        
        errors = []
        
        def insert_rows(start, count):
            try:
                for i in range(start, start + count):
                    table.insert({"id": i})
            except Exception as e:
                errors.append(e)
        
        threads = [
            threading.Thread(target=insert_rows, args=(0, 100)),
            threading.Thread(target=insert_rows, args=(100, 100)),
            threading.Thread(target=insert_rows, args=(200, 100)),
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        assert table.row_count == 300

    def test_btree_thread_safety(self):
        """BTree is thread-safe."""
        btree = BTree()
        
        errors = []
        
        def insert_keys(start, count):
            try:
                for i in range(start, start + count):
                    btree.insert(i, i)  # key, value
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


# =============================================================================
# Run Tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])