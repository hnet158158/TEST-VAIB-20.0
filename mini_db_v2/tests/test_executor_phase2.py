# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_executor_phase2
# Intent: Comprehensive tests for SQL Executor Phase 2 - indexes, CRUD, expressions.
# Dependencies: pytest
# END_MODULE_CONTRACT

"""
Phase 2 B-Tree Index - Executor Tests

Tests cover:
- CREATE INDEX execution
- INSERT/UPDATE/DELETE execution
- SELECT with index optimization
- Expression evaluation
- NULL handling
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from mini_db_v2.storage.database import Database
from mini_db_v2.storage.table import ColumnDef, DataType
from mini_db_v2.executor.executor import (
    Executor, ExecutionResult, ExecutorError, TableNotFoundError,
    ColumnNotFoundError, DuplicateIndexError
)
from mini_db_v2.parser.parser import parse_sql


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def database():
    """Create a fresh database for each test."""
    return Database("test_db")


@pytest.fixture
def executor(database):
    """Create an executor with database."""
    return Executor(database)


@pytest.fixture
def populated_db(database, executor):
    """Create a database with a table and data."""
    # Create table
    columns = {
        "id": ColumnDef(name="id", data_type=DataType.INT, primary_key=True),
        "name": ColumnDef(name="name", data_type=DataType.TEXT, nullable=False),
        "age": ColumnDef(name="age", data_type=DataType.INT),
        "active": ColumnDef(name="active", data_type=DataType.BOOL, default=True),
    }
    database.create_table("users", columns)
    
    # Insert test data
    for i in range(1, 11):
        executor.execute(parse_sql(
            f"INSERT INTO users (id, name, age) VALUES ({i}, 'User{i}', {20 + i})"
        ))
    
    return database


# =============================================================================
# CREATE TABLE Execution Tests
# =============================================================================

class TestExecuteCreateTable:
    """Tests for CREATE TABLE execution."""

    def test_create_table_basic(self, executor):
        """Execute basic CREATE TABLE."""
        ast = parse_sql("CREATE TABLE users (id INT, name TEXT)")
        result = executor.execute(ast)
        
        assert result.success is True
        assert "users" in executor.database.tables

    def test_create_table_with_constraints(self, executor):
        """Execute CREATE TABLE with constraints."""
        ast = parse_sql("""
            CREATE TABLE users (
                id INT PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE
            )
        """)
        result = executor.execute(ast)
        
        assert result.success is True
        table = executor.database.get_table("users")
        assert table.columns["id"].primary_key is True
        assert table.columns["name"].nullable is False
        assert table.columns["email"].unique is True

    def test_create_table_if_not_exists(self, executor):
        """Execute CREATE TABLE IF NOT EXISTS."""
        ast1 = parse_sql("CREATE TABLE users (id INT)")
        executor.execute(ast1)
        
        ast2 = parse_sql("CREATE TABLE IF NOT EXISTS users (id INT)")
        result = executor.execute(ast2)
        
        assert result.success is True

    def test_create_table_duplicate_error(self, executor):
        """CREATE TABLE duplicate raises error."""
        ast = parse_sql("CREATE TABLE users (id INT)")
        executor.execute(ast)
        
        with pytest.raises(ExecutorError):
            executor.execute(ast)


# =============================================================================
# CREATE INDEX Execution Tests
# =============================================================================

class TestExecuteCreateIndex:
    """Tests for CREATE INDEX execution."""

    def test_create_index_basic(self, populated_db, executor):
        """Execute basic CREATE INDEX."""
        ast = parse_sql("CREATE INDEX idx_age ON users (age)")
        result = executor.execute(ast)
        
        assert result.success is True
        assert "idx_age" in executor._indexes.get("users", {})

    def test_create_unique_index(self, populated_db, executor):
        """Execute CREATE UNIQUE INDEX."""
        ast = parse_sql("CREATE UNIQUE INDEX idx_id ON users (id)")
        result = executor.execute(ast)
        
        assert result.success is True

    def test_create_index_nonexistent_table(self, executor):
        """CREATE INDEX on nonexistent table raises error."""
        ast = parse_sql("CREATE INDEX idx ON nonexistent (col)")
        
        with pytest.raises(TableNotFoundError):
            executor.execute(ast)

    def test_create_index_nonexistent_column(self, populated_db, executor):
        """CREATE INDEX on nonexistent column raises error."""
        ast = parse_sql("CREATE INDEX idx ON users (nonexistent)")
        
        with pytest.raises(ColumnNotFoundError):
            executor.execute(ast)

    def test_create_index_duplicate(self, populated_db, executor):
        """CREATE INDEX duplicate raises error."""
        ast = parse_sql("CREATE INDEX idx_age ON users (age)")
        executor.execute(ast)
        
        with pytest.raises(DuplicateIndexError):
            executor.execute(ast)


# =============================================================================
# INSERT Execution Tests
# =============================================================================

class TestExecuteInsert:
    """Tests for INSERT execution."""

    def test_insert_basic(self, database, executor):
        """Execute basic INSERT."""
        columns = {"id": ColumnDef(name="id", data_type=DataType.INT)}
        database.create_table("users", columns)
        
        ast = parse_sql("INSERT INTO users VALUES (1)")
        result = executor.execute(ast)
        
        assert result.success is True
        assert result.row_count == 1

    def test_insert_with_columns(self, populated_db, executor):
        """Execute INSERT with column names."""
        ast = parse_sql("INSERT INTO users (id, name, age) VALUES (100, 'NewUser', 30)")
        result = executor.execute(ast)
        
        assert result.success is True
        assert result.row_count == 1

    def test_insert_multiple_rows(self, populated_db, executor):
        """Execute INSERT with multiple rows."""
        ast = parse_sql("INSERT INTO users VALUES (100, 'A', 25), (101, 'B', 26)")
        result = executor.execute(ast)
        
        assert result.success is True
        assert result.row_count == 2

    def test_insert_nonexistent_table(self, executor):
        """INSERT into nonexistent table raises error."""
        ast = parse_sql("INSERT INTO nonexistent VALUES (1)")
        
        with pytest.raises(TableNotFoundError):
            executor.execute(ast)


# =============================================================================
# SELECT Execution Tests
# =============================================================================

class TestExecuteSelect:
    """Tests for SELECT execution."""

    def test_select_all(self, populated_db, executor):
        """Execute SELECT *."""
        ast = parse_sql("SELECT * FROM users")
        result = executor.execute(ast)
        
        assert result.success is True
        assert len(result.rows) == 10

    def test_select_columns(self, populated_db, executor):
        """Execute SELECT with specific columns."""
        ast = parse_sql("SELECT id, name FROM users")
        result = executor.execute(ast)
        
        assert result.success is True
        assert "id" in result.columns
        assert "name" in result.columns

    def test_select_with_where(self, populated_db, executor):
        """Execute SELECT with WHERE clause."""
        ast = parse_sql("SELECT * FROM users WHERE id = 1")
        result = executor.execute(ast)
        
        assert result.success is True
        assert len(result.rows) == 1
        assert result.rows[0]["id"] == 1

    def test_select_with_where_gt(self, populated_db, executor):
        """Execute SELECT with WHERE > clause."""
        ast = parse_sql("SELECT * FROM users WHERE age > 25")
        result = executor.execute(ast)
        
        assert result.success is True
        for row in result.rows:
            assert row["age"] > 25

    def test_select_with_where_lt(self, populated_db, executor):
        """Execute SELECT with WHERE < clause."""
        ast = parse_sql("SELECT * FROM users WHERE age < 25")
        result = executor.execute(ast)
        
        assert result.success is True
        for row in result.rows:
            assert row["age"] < 25

    def test_select_with_where_and(self, populated_db, executor):
        """Execute SELECT with WHERE AND clause."""
        ast = parse_sql("SELECT * FROM users WHERE age >= 22 AND age <= 27")
        result = executor.execute(ast)
        
        assert result.success is True
        for row in result.rows:
            assert 22 <= row["age"] <= 27

    def test_select_with_order_by(self, populated_db, executor):
        """Execute SELECT with ORDER BY."""
        ast = parse_sql("SELECT * FROM users ORDER BY age DESC")
        result = executor.execute(ast)
        
        assert result.success is True
        ages = [row["age"] for row in result.rows]
        assert ages == sorted(ages, reverse=True)

    def test_select_with_limit(self, populated_db, executor):
        """Execute SELECT with LIMIT."""
        ast = parse_sql("SELECT * FROM users LIMIT 5")
        result = executor.execute(ast)
        
        assert result.success is True
        assert len(result.rows) == 5

    def test_select_with_offset(self, populated_db, executor):
        """Execute SELECT with OFFSET."""
        ast = parse_sql("SELECT * FROM users LIMIT 5 OFFSET 3")
        result = executor.execute(ast)
        
        assert result.success is True
        assert len(result.rows) == 5

    def test_select_distinct(self, populated_db, executor):
        """Execute SELECT DISTINCT."""
        # Insert duplicate ages
        executor.execute(parse_sql("INSERT INTO users VALUES (100, 'A', 25)"))
        executor.execute(parse_sql("INSERT INTO users VALUES (101, 'B', 25)"))
        
        ast = parse_sql("SELECT DISTINCT age FROM users")
        result = executor.execute(ast)
        
        assert result.success is True
        # Check no duplicates
        ages = [row["age"] for row in result.rows]
        assert len(ages) == len(set(ages))


# =============================================================================
# SELECT with Index Tests
# =============================================================================

class TestSelectWithIndex:
    """Tests for SELECT with index optimization."""

    def test_select_uses_index_for_eq(self, populated_db, executor):
        """SELECT with = uses index."""
        # Create index
        executor.execute(parse_sql("CREATE INDEX idx_age ON users (age)"))
        
        # Query should use index
        ast = parse_sql("SELECT * FROM users WHERE age = 25")
        result = executor.execute(ast)
        
        assert result.success is True
        assert len(result.rows) == 1

    def test_select_uses_index_for_gt(self, populated_db, executor):
        """SELECT with > uses index for range scan."""
        executor.execute(parse_sql("CREATE INDEX idx_age ON users (age)"))
        
        ast = parse_sql("SELECT * FROM users WHERE age > 25")
        result = executor.execute(ast)
        
        assert result.success is True
        for row in result.rows:
            assert row["age"] > 25

    def test_select_uses_index_for_lt(self, populated_db, executor):
        """SELECT with < uses index for range scan."""
        executor.execute(parse_sql("CREATE INDEX idx_age ON users (age)"))
        
        ast = parse_sql("SELECT * FROM users WHERE age < 25")
        result = executor.execute(ast)
        
        assert result.success is True
        for row in result.rows:
            assert row["age"] < 25

    def test_select_uses_index_for_range(self, populated_db, executor):
        """SELECT with range uses index."""
        executor.execute(parse_sql("CREATE INDEX idx_age ON users (age)"))
        
        ast = parse_sql("SELECT * FROM users WHERE age >= 22 AND age <= 27")
        result = executor.execute(ast)
        
        assert result.success is True
        for row in result.rows:
            assert 22 <= row["age"] <= 27


# =============================================================================
# UPDATE Execution Tests
# =============================================================================

class TestExecuteUpdate:
    """Tests for UPDATE execution."""

    def test_update_all(self, populated_db, executor):
        """Execute UPDATE all rows."""
        ast = parse_sql("UPDATE users SET active = FALSE")
        result = executor.execute(ast)
        
        assert result.success is True
        assert result.row_count == 10

    def test_update_with_where(self, populated_db, executor):
        """Execute UPDATE with WHERE."""
        ast = parse_sql("UPDATE users SET active = FALSE WHERE id = 1")
        result = executor.execute(ast)
        
        assert result.success is True
        assert result.row_count == 1

    def test_update_multiple_columns(self, populated_db, executor):
        """Execute UPDATE multiple columns."""
        ast = parse_sql("UPDATE users SET name = 'Updated', age = 99 WHERE id = 1")
        result = executor.execute(ast)
        
        assert result.success is True
        
        # Verify update
        select_result = executor.execute(parse_sql("SELECT * FROM users WHERE id = 1"))
        assert select_result.rows[0]["name"] == "Updated"
        assert select_result.rows[0]["age"] == 99


# =============================================================================
# DELETE Execution Tests
# =============================================================================

class TestExecuteDelete:
    """Tests for DELETE execution."""

    def test_delete_all(self, populated_db, executor):
        """Execute DELETE all rows."""
        ast = parse_sql("DELETE FROM users")
        result = executor.execute(ast)
        
        assert result.success is True
        assert result.row_count == 10

    def test_delete_with_where(self, populated_db, executor):
        """Execute DELETE with WHERE."""
        ast = parse_sql("DELETE FROM users WHERE id = 1")
        result = executor.execute(ast)
        
        assert result.success is True
        assert result.row_count == 1
        
        # Verify deletion
        select_result = executor.execute(parse_sql("SELECT * FROM users"))
        assert len(select_result.rows) == 9

    def test_delete_with_complex_where(self, populated_db, executor):
        """Execute DELETE with complex WHERE."""
        ast = parse_sql("DELETE FROM users WHERE age > 25 AND active = TRUE")
        result = executor.execute(ast)
        
        assert result.success is True


# =============================================================================
# DROP TABLE Tests
# =============================================================================

class TestExecuteDropTable:
    """Tests for DROP TABLE execution."""

    def test_drop_table(self, populated_db, executor):
        """Execute DROP TABLE."""
        ast = parse_sql("DROP TABLE users")
        result = executor.execute(ast)
        
        assert result.success is True
        assert "users" not in populated_db.tables

    def test_drop_table_if_exists(self, executor):
        """Execute DROP TABLE IF EXISTS."""
        ast = parse_sql("DROP TABLE IF EXISTS nonexistent")
        result = executor.execute(ast)
        
        assert result.success is True

    def test_drop_table_nonexistent_error(self, executor):
        """DROP TABLE nonexistent raises error."""
        ast = parse_sql("DROP TABLE nonexistent")
        
        with pytest.raises(TableNotFoundError):
            executor.execute(ast)


# =============================================================================
# DROP INDEX Tests
# =============================================================================

class TestExecuteDropIndex:
    """Tests for DROP INDEX execution."""

    def test_drop_index(self, populated_db, executor):
        """Execute DROP INDEX."""
        executor.execute(parse_sql("CREATE INDEX idx_age ON users (age)"))
        
        ast = parse_sql("DROP INDEX idx_age")
        result = executor.execute(ast)
        
        assert result.success is True
        assert "idx_age" not in executor._indexes.get("users", {})

    def test_drop_index_if_exists(self, executor):
        """Execute DROP INDEX IF EXISTS."""
        ast = parse_sql("DROP INDEX IF EXISTS nonexistent")
        result = executor.execute(ast)
        
        assert result.success is True


# =============================================================================
# Expression Evaluation Tests
# =============================================================================

class TestExpressionEvaluation:
    """Tests for expression evaluation."""

    def test_arithmetic_expression(self, populated_db, executor):
        """Evaluate arithmetic expression."""
        ast = parse_sql("SELECT id + 1 AS next_id FROM users WHERE id = 1")
        result = executor.execute(ast)
        
        assert result.rows[0]["next_id"] == 2

    def test_comparison_expression(self, populated_db, executor):
        """Evaluate comparison expression."""
        ast = parse_sql("SELECT * FROM users WHERE age >= 25")
        result = executor.execute(ast)
        
        assert all(row["age"] >= 25 for row in result.rows)

    def test_logical_and(self, populated_db, executor):
        """Evaluate logical AND."""
        ast = parse_sql("SELECT * FROM users WHERE id >= 1 AND id <= 5")
        result = executor.execute(ast)
        
        assert len(result.rows) == 5

    def test_logical_or(self, populated_db, executor):
        """Evaluate logical OR."""
        ast = parse_sql("SELECT * FROM users WHERE id = 1 OR id = 10")
        result = executor.execute(ast)
        
        assert len(result.rows) == 2

    def test_between_expression(self, populated_db, executor):
        """Evaluate BETWEEN expression."""
        ast = parse_sql("SELECT * FROM users WHERE age BETWEEN 22 AND 27")
        result = executor.execute(ast)
        
        assert all(22 <= row["age"] <= 27 for row in result.rows)

    def test_in_list_expression(self, populated_db, executor):
        """Evaluate IN list expression."""
        ast = parse_sql("SELECT * FROM users WHERE id IN (1, 3, 5)")
        result = executor.execute(ast)
        
        assert len(result.rows) == 3

    def test_like_expression(self, populated_db, executor):
        """Evaluate LIKE expression."""
        ast = parse_sql("SELECT * FROM users WHERE name LIKE 'User%'")
        result = executor.execute(ast)
        
        assert len(result.rows) == 10

    def test_is_null(self, database, executor):
        """Evaluate IS NULL."""
        columns = {"id": ColumnDef(name="id", data_type=DataType.INT),
                   "name": ColumnDef(name="name", data_type=DataType.TEXT)}
        database.create_table("users", columns)
        
        executor.execute(parse_sql("INSERT INTO users (id) VALUES (1)"))
        executor.execute(parse_sql("INSERT INTO users (id, name) VALUES (2, 'John')"))
        
        ast = parse_sql("SELECT * FROM users WHERE name IS NULL")
        result = executor.execute(ast)
        
        assert len(result.rows) == 1
        assert result.rows[0]["id"] == 1

    def test_is_not_null(self, database, executor):
        """Evaluate IS NOT NULL."""
        columns = {"id": ColumnDef(name="id", data_type=DataType.INT),
                   "name": ColumnDef(name="name", data_type=DataType.TEXT)}
        database.create_table("users", columns)
        
        executor.execute(parse_sql("INSERT INTO users (id) VALUES (1)"))
        executor.execute(parse_sql("INSERT INTO users (id, name) VALUES (2, 'John')"))
        
        ast = parse_sql("SELECT * FROM users WHERE name IS NOT NULL")
        result = executor.execute(ast)
        
        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "John"


# =============================================================================
# NULL Handling Tests
# =============================================================================

class TestNullHandling:
    """Tests for NULL handling."""

    def test_null_comparison_returns_null(self, database, executor):
        """NULL comparison returns NULL (unknown)."""
        columns = {"id": ColumnDef(name="id", data_type=DataType.INT),
                   "value": ColumnDef(name="value", data_type=DataType.INT)}
        database.create_table("test", columns)
        
        executor.execute(parse_sql("INSERT INTO test (id) VALUES (1)"))
        
        # NULL = 5 should not match
        ast = parse_sql("SELECT * FROM test WHERE value = 5")
        result = executor.execute(ast)
        
        assert len(result.rows) == 0

    def test_null_arithmetic(self, database, executor):
        """NULL arithmetic returns NULL."""
        columns = {"id": ColumnDef(name="id", data_type=DataType.INT),
                   "value": ColumnDef(name="value", data_type=DataType.INT)}
        database.create_table("test", columns)
        
        executor.execute(parse_sql("INSERT INTO test (id) VALUES (1)"))
        
        ast = parse_sql("SELECT id + value AS result FROM test")
        result = executor.execute(ast)
        
        assert result.rows[0]["result"] is None


# =============================================================================
# Run Tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])