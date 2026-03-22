# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_integration_phase2
# Intent: Integration tests for Phase 2 - end-to-end SQL workflow.
# Dependencies: pytest
# END_MODULE_CONTRACT

"""
Phase 2 B-Tree Index - Integration Tests

Tests cover:
- Full SQL workflow: CREATE TABLE → INSERT → CREATE INDEX → SELECT
- Index population and updates
- Complex queries with indexes
- Error recovery
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from mini_db_v2.storage.database import Database
from mini_db_v2.storage.table import ColumnDef, DataType
from mini_db_v2.executor.executor import Executor, ExecutionResult
from mini_db_v2.parser.parser import parse_sql


# =============================================================================
# Full Workflow Integration Tests
# =============================================================================

class TestFullWorkflow:
    """End-to-end SQL workflow tests."""

    def test_create_insert_select_workflow(self):
        """Complete workflow: CREATE TABLE → INSERT → SELECT."""
        db = Database()
        executor = Executor(db)
        
        # Create table
        result = executor.execute(parse_sql(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)"
        ))
        assert result.success is True
        
        # Insert data
        for i in range(1, 11):
            result = executor.execute(parse_sql(
                f"INSERT INTO users (id, name, age) VALUES ({i}, 'User{i}', {20 + i})"
            ))
            assert result.success is True
        
        # Select all
        result = executor.execute(parse_sql("SELECT * FROM users"))
        assert result.success is True
        assert len(result.rows) == 10
        
        # Select with WHERE
        result = executor.execute(parse_sql("SELECT * FROM users WHERE age > 25"))
        assert result.success is True
        assert len(result.rows) == 5

    def test_create_index_workflow(self):
        """Complete workflow with index creation."""
        db = Database()
        executor = Executor(db)
        
        # Create table and insert data
        executor.execute(parse_sql(
            "CREATE TABLE products (id INT, name TEXT, price REAL)"
        ))
        
        for i in range(100):
            executor.execute(parse_sql(
                f"INSERT INTO products VALUES ({i}, 'Product{i}', {i * 10.0})"
            ))
        
        # Create index
        result = executor.execute(parse_sql(
            "CREATE INDEX idx_price ON products (price)"
        ))
        assert result.success is True
        
        # Query with index
        result = executor.execute(parse_sql(
            "SELECT * FROM products WHERE price > 500"
        ))
        assert result.success is True
        assert len(result.rows) == 49  # prices 510, 520, ..., 990

    def test_crud_workflow(self):
        """Complete CRUD workflow."""
        db = Database()
        executor = Executor(db)
        
        # Create
        executor.execute(parse_sql(
            "CREATE TABLE items (id INT PRIMARY KEY, name TEXT, qty INT)"
        ))
        
        # Insert
        executor.execute(parse_sql("INSERT INTO items VALUES (1, 'Item1', 10)"))
        executor.execute(parse_sql("INSERT INTO items VALUES (2, 'Item2', 20)"))
        executor.execute(parse_sql("INSERT INTO items VALUES (3, 'Item3', 30)"))
        
        # Read
        result = executor.execute(parse_sql("SELECT * FROM items"))
        assert len(result.rows) == 3
        
        # Update
        result = executor.execute(parse_sql(
            "UPDATE items SET qty = 100 WHERE id = 1"
        ))
        assert result.row_count == 1
        
        # Verify update
        result = executor.execute(parse_sql("SELECT qty FROM items WHERE id = 1"))
        assert result.rows[0]["qty"] == 100
        
        # Delete
        result = executor.execute(parse_sql("DELETE FROM items WHERE id = 2"))
        assert result.row_count == 1
        
        # Verify delete
        result = executor.execute(parse_sql("SELECT * FROM items"))
        assert len(result.rows) == 2


# =============================================================================
# Index Population Tests
# =============================================================================

class TestIndexPopulation:
    """Tests for index population and updates."""

    def test_index_populated_on_create(self):
        """Index is populated when created on existing data."""
        db = Database()
        executor = Executor(db)
        
        # Create table and insert data
        executor.execute(parse_sql(
            "CREATE TABLE data (id INT, value INT)"
        ))
        
        for i in range(100):
            executor.execute(parse_sql(f"INSERT INTO data VALUES ({i}, {i * 2})"))
        
        # Create index
        executor.execute(parse_sql("CREATE INDEX idx_value ON data (value)"))
        
        # Query should use index
        result = executor.execute(parse_sql(
            "SELECT * FROM data WHERE value > 100"
        ))
        
        assert result.success is True
        assert len(result.rows) == 49  # values 102, 104, ..., 198

    def test_index_updated_on_insert(self):
        """Index is updated when new rows are inserted."""
        db = Database()
        executor = Executor(db)
        
        # Create table and index
        executor.execute(parse_sql("CREATE TABLE data (id INT, value INT)"))
        executor.execute(parse_sql("CREATE INDEX idx_value ON data (value)"))
        
        # Insert after index creation
        for i in range(50):
            executor.execute(parse_sql(f"INSERT INTO data VALUES ({i}, {i * 2})"))
        
        # Query should find all
        result = executor.execute(parse_sql(
            "SELECT * FROM data WHERE value >= 0"
        ))
        assert len(result.rows) == 50

    def test_index_rebuilt_on_delete(self):
        """Index is rebuilt when rows are deleted."""
        db = Database()
        executor = Executor(db)
        
        # Create table, insert data, create index
        executor.execute(parse_sql("CREATE TABLE data (id INT, value INT)"))
        for i in range(100):
            executor.execute(parse_sql(f"INSERT INTO data VALUES ({i}, {i})"))
        executor.execute(parse_sql("CREATE INDEX idx_value ON data (value)"))
        
        # Delete some rows
        executor.execute(parse_sql("DELETE FROM data WHERE id < 50"))
        
        # Query should reflect deletions
        result = executor.execute(parse_sql(
            "SELECT * FROM data WHERE value >= 0"
        ))
        assert len(result.rows) == 50


# =============================================================================
# Complex Query Tests
# =============================================================================

class TestComplexQueries:
    """Tests for complex queries."""

    def test_multiple_conditions(self):
        """Query with multiple conditions."""
        db = Database()
        executor = Executor(db)
        
        executor.execute(parse_sql(
            "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL)"
        ))
        
        for i in range(100):
            active = "TRUE" if i % 2 == 0 else "FALSE"
            executor.execute(parse_sql(
                f"INSERT INTO users VALUES ({i}, 'User{i}', {20 + (i % 30)}, {active})"
            ))
        
        result = executor.execute(parse_sql(
            "SELECT * FROM users WHERE age >= 30 AND age <= 40 AND active = TRUE"
        ))
        
        assert result.success is True
        for row in result.rows:
            assert 30 <= row["age"] <= 40
            assert row["active"] is True

    def test_order_by_with_limit(self):
        """Query with ORDER BY and LIMIT."""
        db = Database()
        executor = Executor(db)
        
        executor.execute(parse_sql("CREATE TABLE scores (id INT, score INT)"))
        
        scores = [85, 92, 78, 95, 88, 76, 90, 82]
        for i, score in enumerate(scores):
            executor.execute(parse_sql(f"INSERT INTO scores VALUES ({i}, {score})"))
        
        result = executor.execute(parse_sql(
            "SELECT * FROM scores ORDER BY score DESC LIMIT 3"
        ))
        
        assert len(result.rows) == 3
        assert result.rows[0]["score"] == 95
        assert result.rows[1]["score"] == 92
        assert result.rows[2]["score"] == 90

    def test_aggregate_functions(self):
        """Query with aggregate functions."""
        db = Database()
        executor = Executor(db)
        
        executor.execute(parse_sql("CREATE TABLE sales (id INT, amount INT)"))
        
        amounts = [100, 200, 150, 300, 250]
        for i, amount in enumerate(amounts):
            executor.execute(parse_sql(f"INSERT INTO sales VALUES ({i}, {amount})"))
        
        # SUM
        result = executor.execute(parse_sql("SELECT SUM(amount) FROM sales"))
        # Note: Aggregation returns None in current implementation (Phase 6 will implement)
        
        # COUNT
        result = executor.execute(parse_sql("SELECT COUNT(*) FROM sales"))
        # Note: Returns None until Phase 6

    def test_distinct_query(self):
        """Query with DISTINCT."""
        db = Database()
        executor = Executor(db)
        
        executor.execute(parse_sql("CREATE TABLE tags (id INT, tag TEXT)"))
        
        tags = ["python", "sql", "python", "database", "sql", "python"]
        for i, tag in enumerate(tags):
            executor.execute(parse_sql(f"INSERT INTO tags VALUES ({i}, '{tag}')"))
        
        result = executor.execute(parse_sql("SELECT DISTINCT tag FROM tags"))
        
        assert result.success is True
        tag_values = [row["tag"] for row in result.rows]
        assert len(tag_values) == len(set(tag_values))


# =============================================================================
# Error Recovery Tests
# =============================================================================

class TestErrorRecovery:
    """Tests for error recovery."""

    def test_invalid_query_does_not_corrupt_data(self):
        """Invalid query does not corrupt existing data."""
        db = Database()
        executor = Executor(db)
        
        executor.execute(parse_sql("CREATE TABLE data (id INT PRIMARY KEY, value INT)"))
        executor.execute(parse_sql("INSERT INTO data VALUES (1, 100)"))
        
        # Try invalid insert (duplicate primary key)
        try:
            executor.execute(parse_sql("INSERT INTO data VALUES (1, 200)"))
        except:
            pass
        
        # Data should be intact
        result = executor.execute(parse_sql("SELECT * FROM data"))
        assert len(result.rows) == 1
        assert result.rows[0]["value"] == 100

    def test_drop_table_cleans_indexes(self):
        """DROP TABLE cleans up associated indexes."""
        db = Database()
        executor = Executor(db)
        
        executor.execute(parse_sql("CREATE TABLE data (id INT, value INT)"))
        executor.execute(parse_sql("CREATE INDEX idx_value ON data (value)"))
        
        # Drop table
        executor.execute(parse_sql("DROP TABLE data"))
        
        # Indexes should be cleaned
        assert "data" not in executor._indexes

    def test_multiple_tables_with_indexes(self):
        """Multiple tables with separate indexes."""
        db = Database()
        executor = Executor(db)
        
        # Create multiple tables
        executor.execute(parse_sql("CREATE TABLE t1 (id INT, val INT)"))
        executor.execute(parse_sql("CREATE TABLE t2 (id INT, val INT)"))
        
        # Create indexes
        executor.execute(parse_sql("CREATE INDEX idx_t1_val ON t1 (val)"))
        executor.execute(parse_sql("CREATE INDEX idx_t2_val ON t2 (val)"))
        
        # Insert data
        for i in range(10):
            executor.execute(parse_sql(f"INSERT INTO t1 VALUES ({i}, {i})"))
            executor.execute(parse_sql(f"INSERT INTO t2 VALUES ({i}, {i * 10})"))
        
        # Query each table
        result1 = executor.execute(parse_sql("SELECT * FROM t1 WHERE val > 5"))
        result2 = executor.execute(parse_sql("SELECT * FROM t2 WHERE val > 50"))
        
        assert len(result1.rows) == 4
        assert len(result2.rows) == 4


# =============================================================================
# Range Query Tests
# =============================================================================

class TestRangeQueries:
    """Tests for range queries with indexes."""

    def test_range_query_gt(self):
        """Range query with > operator."""
        db = Database()
        executor = Executor(db)
        
        executor.execute(parse_sql("CREATE TABLE data (id INT, value INT)"))
        executor.execute(parse_sql("CREATE INDEX idx_value ON data (value)"))
        
        for i in range(100):
            executor.execute(parse_sql(f"INSERT INTO data VALUES ({i}, {i})"))
        
        result = executor.execute(parse_sql(
            "SELECT * FROM data WHERE value > 50"
        ))
        
        assert len(result.rows) == 49

    def test_range_query_lt(self):
        """Range query with < operator."""
        db = Database()
        executor = Executor(db)
        
        executor.execute(parse_sql("CREATE TABLE data (id INT, value INT)"))
        executor.execute(parse_sql("CREATE INDEX idx_value ON data (value)"))
        
        for i in range(100):
            executor.execute(parse_sql(f"INSERT INTO data VALUES ({i}, {i})"))
        
        result = executor.execute(parse_sql(
            "SELECT * FROM data WHERE value < 50"
        ))
        
        assert len(result.rows) == 50

    def test_range_query_between(self):
        """Range query with BETWEEN."""
        db = Database()
        executor = Executor(db)
        
        executor.execute(parse_sql("CREATE TABLE data (id INT, value INT)"))
        
        for i in range(100):
            executor.execute(parse_sql(f"INSERT INTO data VALUES ({i}, {i})"))
        
        result = executor.execute(parse_sql(
            "SELECT * FROM data WHERE value BETWEEN 25 AND 75"
        ))
        
        assert len(result.rows) == 51

    def test_range_query_inclusive(self):
        """Range query with >= and <=."""
        db = Database()
        executor = Executor(db)
        
        executor.execute(parse_sql("CREATE TABLE data (id INT, value INT)"))
        executor.execute(parse_sql("CREATE INDEX idx_value ON data (value)"))
        
        for i in range(100):
            executor.execute(parse_sql(f"INSERT INTO data VALUES ({i}, {i})"))
        
        result = executor.execute(parse_sql(
            "SELECT * FROM data WHERE value >= 25 AND value <= 75"
        ))
        
        assert len(result.rows) == 51


# =============================================================================
# Run Tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])