# Tests for NULL semantics
# Phase 3: DQL + WHERE
# NULL semantics: col = NULL -> False, any comparison with NULL returns False

import pytest

from mini_db.parser.parser import Parser
from mini_db.executor.executor import Executor, ExecutionResult
from mini_db.storage.database import Database
from mini_db.ast.nodes import ColumnDef


# START_BLOCK_NULL_PARSING
class TestNullParsing:
    """Tests for parsing NULL literals in WHERE clauses."""
    
    def test_parse_null_literal(self):
        """WHERE col = null"""
        parser = Parser()
        ast = parser.parse("SELECT * FROM t WHERE col = null")
        
        from mini_db.ast.nodes import LiteralNode, ComparisonNode
        assert isinstance(ast.where, ComparisonNode)
        assert isinstance(ast.where.right, LiteralNode)
        assert ast.where.right.value is None
    
    def test_parse_null_in_values(self):
        """INSERT with NULL value"""
        parser = Parser()
        from mini_db.ast.nodes import InsertNode
        ast = parser.parse("INSERT INTO t (col) VALUES (null)")
        
        assert isinstance(ast, InsertNode)
        assert ast.values == [None]
# END_BLOCK_NULL_PARSING


# START_BLOCK_NULL_COMPARISON
class TestNullComparisonSemantics:
    """
    NULL semantics: any comparison with NULL returns False.
    This is standard SQL behavior.
    """
    
    @pytest.fixture
    def setup_db(self):
        """Create test database with NULL values."""
        db = Database()
        db.create_table("users", [
            ColumnDef(name="id", data_type="INT"),
            ColumnDef(name="name", data_type="TEXT"),
            ColumnDef(name="phone", data_type="TEXT"),  # Can be NULL
            ColumnDef(name="age", data_type="INT"),     # Can be NULL
        ])
        
        table = db.get_table("users")
        table.insert({"id": 1, "name": "Alice", "phone": "123-456", "age": 25})
        table.insert({"id": 2, "name": "Bob", "phone": None, "age": 30})
        table.insert({"id": 3, "name": "Charlie", "phone": "789-012", "age": None})
        table.insert({"id": 4, "name": "Diana", "phone": None, "age": None})
        
        return db
    
    def test_equals_null_returns_false(self, setup_db):
        """
        col = NULL always returns False (no rows match).
        This is correct SQL NULL semantics.
        """
        db = setup_db
        parser = Parser()
        executor = Executor()
        
        ast = parser.parse("SELECT * FROM users WHERE phone = null")
        result = executor.execute(ast, db)
        
        assert result.success
        assert len(result.data) == 0  # No rows match NULL = NULL
    
    def test_not_equals_null_returns_false(self, setup_db):
        """
        col != NULL also returns False.
        This is correct SQL NULL semantics.
        """
        db = setup_db
        parser = Parser()
        executor = Executor()
        
        ast = parser.parse("SELECT * FROM users WHERE phone != null")
        result = executor.execute(ast, db)
        
        assert result.success
        assert len(result.data) == 0  # No rows match
    
    def test_less_than_null_returns_false(self, setup_db):
        """col < NULL returns False."""
        db = setup_db
        parser = Parser()
        executor = Executor()
        
        ast = parser.parse("SELECT * FROM users WHERE age < null")
        result = executor.execute(ast, db)
        
        assert result.success
        assert len(result.data) == 0
    
    def test_greater_than_null_returns_false(self, setup_db):
        """col > NULL returns False."""
        db = setup_db
        parser = Parser()
        executor = Executor()
        
        ast = parser.parse("SELECT * FROM users WHERE age > null")
        result = executor.execute(ast, db)
        
        assert result.success
        assert len(result.data) == 0
    
    def test_null_in_logical_and(self, setup_db):
        """
        NULL in AND expression: True AND (col = NULL) = False
        """
        db = setup_db
        parser = Parser()
        executor = Executor()
        
        ast = parser.parse("SELECT * FROM users WHERE id = 1 AND phone = null")
        result = executor.execute(ast, db)
        
        assert result.success
        assert len(result.data) == 0  # Alice has phone, but phone = null is False
    
    def test_null_in_logical_or(self, setup_db):
        """
        NULL in OR expression: False OR (col = NULL) = False
        But: True OR (col = NULL) = True (short-circuit)
        """
        db = setup_db
        parser = Parser()
        executor = Executor()
        
        # id = 1 is True for Alice, so OR should return True
        ast = parser.parse("SELECT * FROM users WHERE id = 1 OR phone = null")
        result = executor.execute(ast, db)
        
        assert result.success
        assert len(result.data) == 1  # Alice matches id = 1
        assert result.data[0]["name"] == "Alice"
    
    def test_select_non_null_values(self, setup_db):
        """
        Select rows where column is NOT NULL by checking col != null won't work.
        Instead, we need to check for actual values.
        Note: IS NULL / IS NOT NULL is not implemented (Out of Scope).
        """
        db = setup_db
        parser = Parser()
        executor = Executor()
        
        # Select all rows and filter in application
        ast = parser.parse("SELECT * FROM users")
        result = executor.execute(ast, db)
        
        assert result.success
        assert len(result.data) == 4
        
        # Filter non-null phones manually
        non_null_phones = [r for r in result.data if r["phone"] is not None]
        assert len(non_null_phones) == 2
# END_BLOCK_NULL_COMPARISON


# START_BLOCK_NULL_INSERT_SELECT
class TestNullInsertSelect:
    """Tests for inserting and selecting NULL values."""
    
    def test_insert_null_value(self):
        """Insert row with NULL value."""
        db = Database()
        db.create_table("items", [
            ColumnDef(name="id", data_type="INT"),
            ColumnDef(name="description", data_type="TEXT"),
        ])
        
        parser = Parser()
        executor = Executor()
        
        # Insert with NULL
        ast = parser.parse("INSERT INTO items (id, description) VALUES (1, null)")
        result = executor.execute(ast, db)
        
        assert result.success
        
        # Verify NULL was stored
        table = db.get_table("items")
        row = table.rows[0]
        assert row["description"] is None
    
    def test_select_returns_null(self):
        """SELECT returns NULL values correctly."""
        db = Database()
        db.create_table("items", [
            ColumnDef(name="id", data_type="INT"),
            ColumnDef(name="value", data_type="INT"),
        ])
        
        table = db.get_table("items")
        table.insert({"id": 1, "value": None})
        table.insert({"id": 2, "value": 100})
        
        parser = Parser()
        executor = Executor()
        
        ast = parser.parse("SELECT * FROM items")
        result = executor.execute(ast, db)
        
        assert result.success
        assert len(result.data) == 2
        assert result.data[0]["value"] is None
        assert result.data[1]["value"] == 100
    
    def test_null_in_unique_column(self):
        """NULL values in UNIQUE column - multiple NULLs allowed."""
        db = Database()
        db.create_table("users", [
            ColumnDef(name="id", data_type="INT", unique=True),
            ColumnDef(name="email", data_type="TEXT", unique=True),
        ])
        
        table = db.get_table("users")
        
        # Multiple NULL values should be allowed in UNIQUE column
        result1 = table.insert({"id": 1, "email": None})
        result2 = table.insert({"id": 2, "email": None})
        
        # Both inserts should succeed (NULL != NULL in UNIQUE context)
        assert result1.success
        assert result2.success
# END_BLOCK_NULL_INSERT_SELECT


# START_BLOCK_NULL_EDGE_CASES
class TestNullEdgeCases:
    """Edge cases for NULL handling."""
    
    def test_all_null_row(self):
        """Row with all NULL values."""
        db = Database()
        db.create_table("empty", [
            ColumnDef(name="a", data_type="INT"),
            ColumnDef(name="b", data_type="TEXT"),
        ])
        
        table = db.get_table("empty")
        table.insert({"a": None, "b": None})
        
        parser = Parser()
        executor = Executor()
        
        # Any comparison should return False
        ast = parser.parse("SELECT * FROM empty WHERE a = null OR b = null")
        result = executor.execute(ast, db)
        
        assert result.success
        assert len(result.data) == 0
    
    def test_null_comparison_with_actual_value(self):
        """Compare actual value with NULL literal."""
        db = Database()
        db.create_table("test", [
            ColumnDef(name="val", data_type="INT"),
        ])
        
        table = db.get_table("test")
        table.insert({"val": 10})
        table.insert({"val": None})
        
        parser = Parser()
        executor = Executor()
        
        # val = 10 should match only first row
        ast = parser.parse("SELECT * FROM test WHERE val = 10")
        result = executor.execute(ast, db)
        
        assert result.success
        assert len(result.data) == 1
        assert result.data[0]["val"] == 10
    
    def test_null_in_complex_expression(self):
        """NULL in complex WHERE expression."""
        db = Database()
        db.create_table("data", [
            ColumnDef(name="a", data_type="INT"),
            ColumnDef(name="b", data_type="INT"),
        ])
        
        table = db.get_table("data")
        table.insert({"a": 1, "b": 2})
        table.insert({"a": None, "b": 2})
        table.insert({"a": 1, "b": None})
        
        parser = Parser()
        executor = Executor()
        
        # a = 1 AND b = 2 should match only first row
        ast = parser.parse("SELECT * FROM data WHERE a = 1 AND b = 2")
        result = executor.execute(ast, db)
        
        assert result.success
        assert len(result.data) == 1
# END_BLOCK_NULL_EDGE_CASES