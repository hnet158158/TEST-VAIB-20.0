# START_MODULE_CONTRACT
# Module: tests.test_insert_unique
# Intent: Unit tests для UNIQUE constraint при INSERT.
# END_MODULE_CONTRACT

import pytest

from mini_db.ast.nodes import ColumnDef
from mini_db.executor import Executor, ExecutionResult
from mini_db.parser import Parser
from mini_db.storage import Database, Table


class TestUniqueConstraint:
    """Tests for UNIQUE constraint enforcement."""
    
    @pytest.fixture
    def db(self):
        """Create a fresh database for each test."""
        return Database()
    
    @pytest.fixture
    def executor(self):
        """Create an executor instance."""
        return Executor()
    
    @pytest.fixture
    def parser(self):
        """Create a parser instance."""
        return Parser()
    
    def test_insert_unique_int_success(self, db, executor, parser):
        """Insert unique INT value succeeds."""
        # Create table with UNIQUE INT column
        ast = parser.parse("CREATE TABLE users (id INT UNIQUE, name TEXT)")
        executor.execute(ast, db)
        
        # Insert first row
        ast = parser.parse("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        result = executor.execute(ast, db)
        
        assert result.success is True
        assert "1 row inserted" in result.message
    
    def test_insert_unique_int_duplicate_fails(self, db, executor, parser):
        """Insert duplicate INT value fails."""
        # Create table with UNIQUE INT column
        ast = parser.parse("CREATE TABLE users (id INT UNIQUE, name TEXT)")
        executor.execute(ast, db)
        
        # Insert first row
        ast = parser.parse("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        executor.execute(ast, db)
        
        # Insert duplicate id
        ast = parser.parse("INSERT INTO users (id, name) VALUES (1, 'Bob')")
        result = executor.execute(ast, db)
        
        assert result.success is False
        assert "UNIQUE constraint violated" in result.error
    
    def test_insert_unique_text_duplicate_fails(self, db, executor, parser):
        """Insert duplicate TEXT value fails."""
        # Create table with UNIQUE TEXT column
        ast = parser.parse("CREATE TABLE users (id INT, email TEXT UNIQUE)")
        executor.execute(ast, db)
        
        # Insert first row
        ast = parser.parse("INSERT INTO users (id, email) VALUES (1, 'alice@example.com')")
        executor.execute(ast, db)
        
        # Insert duplicate email
        ast = parser.parse("INSERT INTO users (id, email) VALUES (2, 'alice@example.com')")
        result = executor.execute(ast, db)
        
        assert result.success is False
        assert "UNIQUE constraint violated" in result.error
    
    def test_insert_unique_different_values_succeed(self, db, executor, parser):
        """Insert different values to UNIQUE column succeeds."""
        # Create table with UNIQUE column
        ast = parser.parse("CREATE TABLE users (id INT UNIQUE)")
        executor.execute(ast, db)
        
        # Insert multiple rows with different ids
        for i in range(5):
            ast = parser.parse(f"INSERT INTO users (id) VALUES ({i})")
            result = executor.execute(ast, db)
            assert result.success is True
        
        # Verify all rows inserted
        table = db.get_table("users")
        assert len(table.rows) == 5
    
    def test_insert_multiple_unique_columns(self, db, executor, parser):
        """Table can have multiple UNIQUE columns."""
        # Create table with two UNIQUE columns
        ast = parser.parse(
            "CREATE TABLE users (id INT UNIQUE, email TEXT UNIQUE, name TEXT)"
        )
        executor.execute(ast, db)
        
        # Insert first row
        ast = parser.parse(
            "INSERT INTO users (id, email, name) VALUES (1, 'alice@example.com', 'Alice')"
        )
        result = executor.execute(ast, db)
        assert result.success is True
        
        # Insert with duplicate id
        ast = parser.parse(
            "INSERT INTO users (id, email, name) VALUES (1, 'bob@example.com', 'Bob')"
        )
        result = executor.execute(ast, db)
        assert result.success is False
        assert "UNIQUE constraint violated" in result.error
        
        # Insert with duplicate email
        ast = parser.parse(
            "INSERT INTO users (id, email, name) VALUES (2, 'alice@example.com', 'Bob')"
        )
        result = executor.execute(ast, db)
        assert result.success is False
        assert "UNIQUE constraint violated" in result.error
        
        # Insert with all unique values
        ast = parser.parse(
            "INSERT INTO users (id, email, name) VALUES (2, 'bob@example.com', 'Bob')"
        )
        result = executor.execute(ast, db)
        assert result.success is True
    
    def test_insert_null_to_unique_succeeds(self, db, executor, parser):
        """NULL values can be inserted to UNIQUE column (multiple NULLs allowed)."""
        # Create table with UNIQUE column
        ast = parser.parse("CREATE TABLE users (id INT UNIQUE)")
        executor.execute(ast, db)
        
        # Insert NULL
        ast = parser.parse("INSERT INTO users (id) VALUES (null)")
        result = executor.execute(ast, db)
        
        # Note: In our implementation, NULL is not tracked in unique indexes
        # So multiple NULLs should be allowed
        assert result.success is True
    
    def test_insert_without_unique_column(self, db, executor, parser):
        """Insert to non-UNIQUE column allows duplicates."""
        # Create table without UNIQUE
        ast = parser.parse("CREATE TABLE users (id INT, name TEXT)")
        executor.execute(ast, db)
        
        # Insert same id multiple times
        for name in ["Alice", "Bob", "Charlie"]:
            ast = parser.parse(f"INSERT INTO users (id, name) VALUES (1, '{name}')")
            result = executor.execute(ast, db)
            assert result.success is True
        
        # Verify all rows inserted
        table = db.get_table("users")
        assert len(table.rows) == 3


class TestStrictTyping:
    """Tests for strict type checking during INSERT."""
    
    @pytest.fixture
    def db(self):
        return Database()
    
    @pytest.fixture
    def executor(self):
        return Executor()
    
    @pytest.fixture
    def parser(self):
        return Parser()
    
    def test_insert_int_type_correct(self, db, executor, parser):
        """Insert correct INT type succeeds."""
        ast = parser.parse("CREATE TABLE t (id INT)")
        executor.execute(ast, db)
        
        ast = parser.parse("INSERT INTO t (id) VALUES (42)")
        result = executor.execute(ast, db)
        
        assert result.success is True
    
    def test_insert_int_type_string_fails(self, db, executor, parser):
        """Insert string into INT column fails."""
        ast = parser.parse("CREATE TABLE t (id INT)")
        executor.execute(ast, db)
        
        ast = parser.parse("INSERT INTO t (id) VALUES ('not a number')")
        result = executor.execute(ast, db)
        
        assert result.success is False
        assert "Type mismatch" in result.error
    
    def test_insert_text_type_correct(self, db, executor, parser):
        """Insert correct TEXT type succeeds."""
        ast = parser.parse("CREATE TABLE t (name TEXT)")
        executor.execute(ast, db)
        
        ast = parser.parse("INSERT INTO t (name) VALUES ('Alice')")
        result = executor.execute(ast, db)
        
        assert result.success is True
    
    def test_insert_text_type_int_fails(self, db, executor, parser):
        """Insert int into TEXT column fails (strict typing)."""
        ast = parser.parse("CREATE TABLE t (name TEXT)")
        executor.execute(ast, db)
        
        ast = parser.parse("INSERT INTO t (name) VALUES (123)")
        result = executor.execute(ast, db)
        
        assert result.success is False
        assert "Type mismatch" in result.error
    
    def test_insert_bool_type_correct(self, db, executor, parser):
        """Insert correct BOOL type succeeds."""
        ast = parser.parse("CREATE TABLE t (active BOOL)")
        executor.execute(ast, db)
        
        ast = parser.parse("INSERT INTO t (active) VALUES (true)")
        result = executor.execute(ast, db)
        assert result.success is True
        
        ast = parser.parse("INSERT INTO t (active) VALUES (false)")
        result = executor.execute(ast, db)
        assert result.success is True
    
    def test_insert_bool_type_int_fails(self, db, executor, parser):
        """Insert int into BOOL column fails (strict typing)."""
        ast = parser.parse("CREATE TABLE t (active BOOL)")
        executor.execute(ast, db)
        
        ast = parser.parse("INSERT INTO t (active) VALUES (1)")
        result = executor.execute(ast, db)
        
        assert result.success is False
        assert "Type mismatch" in result.error
    
    def test_insert_bool_type_string_fails(self, db, executor, parser):
        """Insert string into BOOL column fails."""
        ast = parser.parse("CREATE TABLE t (active BOOL)")
        executor.execute(ast, db)
        
        ast = parser.parse("INSERT INTO t (active) VALUES ('true')")
        result = executor.execute(ast, db)
        
        assert result.success is False
        assert "Type mismatch" in result.error


class TestHashIndex:
    """Tests for HashIndex functionality."""
    
    def test_index_add_and_lookup(self):
        """Add value to index and lookup."""
        from mini_db.storage.index import HashIndex
        
        index = HashIndex("id")
        index.add(1, 0)
        index.add(2, 1)
        index.add(1, 2)  # Same value, different row
        
        assert index.lookup(1) == {0, 2}
        assert index.lookup(2) == {1}
        assert index.lookup(3) == set()
    
    def test_index_remove(self):
        """Remove value from index."""
        from mini_db.storage.index import HashIndex
        
        index = HashIndex("id")
        index.add(1, 0)
        index.add(1, 1)
        
        index.remove(1, 0)
        
        assert index.lookup(1) == {1}
    
    def test_index_contains(self):
        """Check if value exists in index."""
        from mini_db.storage.index import HashIndex
        
        index = HashIndex("id")
        index.add(1, 0)
        
        assert index.contains(1) is True
        assert index.contains(2) is False
    
    def test_index_clear(self):
        """Clear index."""
        from mini_db.storage.index import HashIndex
        
        index = HashIndex("id")
        index.add(1, 0)
        index.add(2, 1)
        
        index.clear()
        
        assert index.contains(1) is False
        assert index.contains(2) is False
    
    def test_index_rebuild(self):
        """Rebuild index from rows."""
        from mini_db.storage.index import HashIndex
        
        index = HashIndex("id")
        rows = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
        
        index.rebuild(rows, "id")
        
        assert index.lookup(1) == {0}
        assert index.lookup(2) == {1}
        assert index.lookup(3) == {2}


class TestExecutorIntegration:
    """Integration tests for Executor with Parser and Database."""
    
    @pytest.fixture
    def db(self):
        return Database()
    
    @pytest.fixture
    def executor(self):
        return Executor()
    
    @pytest.fixture
    def parser(self):
        return Parser()
    
    def test_full_workflow(self, db, executor, parser):
        """Full workflow: CREATE TABLE, INSERT, verify data."""
        # Create table
        ast = parser.parse(
            "CREATE TABLE users (id INT UNIQUE, name TEXT, active BOOL)"
        )
        result = executor.execute(ast, db)
        assert result.success is True
        
        # Insert rows
        ast = parser.parse(
            "INSERT INTO users (id, name, active) VALUES (1, 'Alice', true)"
        )
        result = executor.execute(ast, db)
        assert result.success is True
        
        ast = parser.parse(
            "INSERT INTO users (id, name, active) VALUES (2, 'Bob', false)"
        )
        result = executor.execute(ast, db)
        assert result.success is True
        
        # Verify data
        table = db.get_table("users")
        assert len(table.rows) == 2
        
        assert table.rows[0]["id"] == 1
        assert table.rows[0]["name"] == "Alice"
        assert table.rows[0]["active"] is True
        
        assert table.rows[1]["id"] == 2
        assert table.rows[1]["name"] == "Bob"
        assert table.rows[1]["active"] is False
    
    def test_error_format(self, db, executor, parser):
        """Errors are returned in proper format."""
        # Try to insert into non-existent table
        ast = parser.parse("INSERT INTO nonexistent (id) VALUES (1)")
        result = executor.execute(ast, db)
        
        assert result.success is False
        assert result.error is not None
        assert "does not exist" in result.error
    
    def test_create_table_already_exists(self, db, executor, parser):
        """Cannot create table that already exists."""
        ast = parser.parse("CREATE TABLE users (id INT)")
        executor.execute(ast, db)
        
        ast = parser.parse("CREATE TABLE users (id INT)")
        result = executor.execute(ast, db)
        
        assert result.success is False
        assert "already exists" in result.error