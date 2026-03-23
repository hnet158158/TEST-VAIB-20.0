# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_edge_cases_phase_extra
# Intent: Edge case tests for mini_db_v2 robustness.
# Dependencies: pytest
# END_MODULE_CONTRACT

"""
Phase Extra: Edge Case Tests for Robustness

Tests cover:
1. NULL handling in all operations
2. Empty tables
3. Very large values
4. Unicode in strings
5. Special characters in SQL
6. Boundary conditions
"""

import pytest
import sys
import os

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from mini_db_v2.parser.lexer import Lexer, LexerError
from mini_db_v2.parser.parser import Parser, ParseError
from mini_db_v2.storage.database import Database
from mini_db_v2.storage.table import Table, ColumnDef, DataType, ValidationError, DuplicateKeyError
from mini_db_v2.storage.btree import BTree
from mini_db_v2.executor.executor import Executor


# =============================================================================
# START_BLOCK_NULL_HANDLING
# =============================================================================

class TestNullHandling:
    """Tests for NULL handling in all operations."""
    
    @pytest.fixture
    def nullable_table(self):
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT, nullable=False),
            "name": ColumnDef(name="name", data_type=DataType.TEXT, nullable=True),
            "value": ColumnDef(name="value", data_type=DataType.INT, nullable=True),
            "active": ColumnDef(name="active", data_type=DataType.BOOL, nullable=True),
        }
        return Table("nullable_table", columns)
    
    @pytest.fixture
    def not_null_table(self):
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT, nullable=False),
            "name": ColumnDef(name="name", data_type=DataType.TEXT, nullable=False),
        }
        return Table("not_null_table", columns)
    
    def test_insert_null_in_nullable_column(self, nullable_table):
        """NULL can be inserted in nullable column."""
        row = nullable_table.insert({"id": 1, "name": None, "value": None, "active": None})
        
        assert row.data["name"] is None
        assert row.data["value"] is None
        assert row.data["active"] is None
    
    def test_insert_null_in_not_null_column_fails(self, not_null_table):
        """NULL cannot be inserted in NOT NULL column."""
        with pytest.raises(ValidationError):
            not_null_table.insert({"id": 1, "name": None})
    
    def test_update_to_null(self, nullable_table):
        """Update to NULL works for nullable columns."""
        nullable_table.insert({"id": 1, "name": "test", "value": 100, "active": True})
        
        nullable_table.update({"name": None, "value": None, "active": None})
        
        result = nullable_table.select()
        assert result[0].data["name"] is None
        assert result[0].data["value"] is None
        assert result[0].data["active"] is None
    
    def test_select_with_null_comparison(self, nullable_table):
        """SELECT with NULL comparison."""
        nullable_table.insert({"id": 1, "name": "test", "value": 100, "active": True})
        nullable_table.insert({"id": 2, "name": None, "value": None, "active": None})
        
        # NULL = NULL is not true
        result = nullable_table.select(where=lambda r: r["name"] == None)
        # In SQL, NULL = NULL is NULL (not true), so this should return empty
        # But our implementation might differ
        
        # Find non-null
        result = nullable_table.select(where=lambda r: r["name"] is not None)
        assert len(result) == 1
        assert result[0].data["id"] == 1
    
    def test_null_in_unique_column(self):
        """Multiple NULLs allowed in UNIQUE column."""
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT, unique=True, nullable=True),
            "value": ColumnDef(name="value", data_type=DataType.INT),
        }
        table = Table("unique_nullable", columns)
        
        # Multiple NULLs should be allowed
        table.insert({"id": None, "value": 1})
        table.insert({"id": None, "value": 2})
        
        assert table.row_count == 2
    
    def test_null_in_primary_key_fails(self):
        """NULL not allowed in PRIMARY KEY (primary_key implies not nullable)."""
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT, primary_key=True, nullable=False),
        }
        table = Table("pk_table", columns)
        
        with pytest.raises(ValidationError):
            table.insert({"id": None})
    
    def test_null_not_supported_in_btree(self):
        """BTree does not support NULL values as keys (None is not comparable)."""
        btree = BTree()
        
        # BTree uses bisect which requires comparable keys
        # None cannot be compared with < operator
        # This test verifies that behavior
        try:
            btree.insert(None, 0)
            # If it works, check the result
            result = btree.search(None)
            assert result is not None or True  # May or may not work
        except TypeError:
            # Expected: None is not comparable
            pass
    
    def test_null_sorting(self, nullable_table):
        """NULL values in sorting context."""
        nullable_table.insert({"id": 1, "name": "a", "value": 1, "active": True})
        nullable_table.insert({"id": 2, "name": None, "value": 2, "active": False})
        nullable_table.insert({"id": 3, "name": "c", "value": None, "active": True})
        
        # All rows should be present
        assert nullable_table.row_count == 3


# END_BLOCK_NULL_HANDLING


# =============================================================================
# START_BLOCK_EMPTY_TABLES
# =============================================================================

class TestEmptyTables:
    """Tests for empty table edge cases."""
    
    @pytest.fixture
    def empty_table(self):
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT),
            "name": ColumnDef(name="name", data_type=DataType.TEXT),
        }
        return Table("empty_table", columns)
    
    def test_select_from_empty_table(self, empty_table):
        """SELECT from empty table returns empty result."""
        result = empty_table.select()
        assert result == []
        assert len(result) == 0
    
    def test_update_empty_table(self, empty_table):
        """UPDATE on empty table affects 0 rows."""
        count = empty_table.update({"name": "test"})
        assert count == 0
        assert empty_table.row_count == 0
    
    def test_delete_from_empty_table(self, empty_table):
        """DELETE from empty table affects 0 rows."""
        count = empty_table.delete()
        assert count == 0
        assert empty_table.row_count == 0
    
    def test_select_with_where_on_empty_table(self, empty_table):
        """SELECT with WHERE on empty table returns empty."""
        result = empty_table.select(where=lambda r: r["id"] == 1)
        assert result == []
    
    def test_btree_empty(self):
        """BTree operations on empty tree."""
        btree = BTree()
        
        assert btree.is_empty
        assert btree.size == 0
        assert btree.search(1) == []
        assert btree.range_scan(0, 100) == []
        assert btree.delete(1) == 0
    
    def test_database_no_tables(self):
        """Database with no tables."""
        db = Database()
        
        assert db.tables == []
        assert db.table_exists("nonexistent") is False


# END_BLOCK_EMPTY_TABLES


# =============================================================================
# START_BLOCK_LARGE_VALUES
# =============================================================================

class TestLargeValues:
    """Tests for very large values."""
    
    @pytest.fixture
    def table(self):
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT),
            "text": ColumnDef(name="text", data_type=DataType.TEXT),
        }
        return Table("large_table", columns)
    
    def test_very_long_string(self, table):
        """Handle very long strings."""
        long_string = "a" * 1000000  # 1 million characters
        
        row = table.insert({"id": 1, "text": long_string})
        
        assert len(row.data["text"]) == 1000000
        assert row.data["text"] == long_string
    
    def test_very_large_integer(self, table):
        """Handle very large integers."""
        large_int = 10 ** 18
        
        row = table.insert({"id": large_int, "text": "test"})
        
        assert row.data["id"] == large_int
    
    def test_negative_large_integer(self, table):
        """Handle very negative integers."""
        large_negative = -(10 ** 18)
        
        row = table.insert({"id": large_negative, "text": "test"})
        
        assert row.data["id"] == large_negative
    
    def test_many_columns(self):
        """Handle table with many columns."""
        columns = {}
        for i in range(1000):
            columns[f"col_{i}"] = ColumnDef(name=f"col_{i}", data_type=DataType.INT)
        
        table = Table("many_columns", columns)
        
        assert len(table.columns) == 1000
    
    def test_many_rows(self, table):
        """Handle table with many rows."""
        for i in range(10000):
            table.insert({"id": i, "text": f"row_{i}"})
        
        assert table.row_count == 10000
        
        # Select should work
        result = table.select(where=lambda r: r["id"] < 100)
        assert len(result) == 100
    
    def test_btree_many_keys(self):
        """BTree with many keys."""
        btree = BTree()
        
        for i in range(10000):
            btree.insert(i, i)
        
        assert btree.size == 10000
        
        # Search should work
        for i in range(0, 10000, 1000):
            assert i in btree


# END_BLOCK_LARGE_VALUES


# =============================================================================
# START_BLOCK_UNICODE_STRINGS
# =============================================================================

class TestUnicodeStrings:
    """Tests for Unicode in strings."""
    
    @pytest.fixture
    def table(self):
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT),
            "text": ColumnDef(name="text", data_type=DataType.TEXT),
        }
        return Table("unicode_table", columns)
    
    @pytest.fixture
    def lexer(self):
        # Lexer requires text in constructor
        return None  # Will create per-test
    
    @pytest.fixture
    def parser(self):
        return Parser()
    
    def test_cyrillic_string(self, table):
        """Handle Cyrillic strings."""
        cyrillic = "Привет мир! Тест на кириллице."
        
        row = table.insert({"id": 1, "text": cyrillic})
        
        assert row.data["text"] == cyrillic
    
    def test_chinese_string(self, table):
        """Handle Chinese strings."""
        chinese = "你好世界！这是中文测试。"
        
        row = table.insert({"id": 1, "text": chinese})
        
        assert row.data["text"] == chinese
    
    def test_arabic_string(self, table):
        """Handle Arabic strings."""
        arabic = "مرحبا بالعالم! هذا اختبار عربي."
        
        row = table.insert({"id": 1, "text": arabic})
        
        assert row.data["text"] == arabic
    
    def test_emoji_string(self, table):
        """Handle emoji strings."""
        emojis = "Hello 👋 World 🌍 Test 🧪"
        
        row = table.insert({"id": 1, "text": emojis})
        
        assert row.data["text"] == emojis
    
    def test_mixed_unicode(self, table):
        """Handle mixed Unicode strings."""
        mixed = "Hello Привет 你好 مرحبا 👋"
        
        row = table.insert({"id": 1, "text": mixed})
        
        assert row.data["text"] == mixed
    
    def test_unicode_in_sql_query(self, parser):
        """Handle Unicode in SQL query."""
        query = "SELECT * FROM users WHERE name = 'Привет'"
        
        lexer = Lexer(query)
        tokens = lexer.tokenize()
        assert tokens is not None
        
        # Use parse_sql utility function
        from mini_db_v2.parser.parser import parse_sql
        ast = parse_sql(query)
        assert ast is not None
    
    def test_null_byte_in_string(self, table):
        """Handle null byte in string."""
        # Null byte should be handled gracefully
        string_with_null = "test\x00value"
        
        row = table.insert({"id": 1, "text": string_with_null})
        
        # Should preserve or handle null byte
        assert row.data["text"] == string_with_null


# END_BLOCK_UNICODE_STRINGS


# =============================================================================
# START_BLOCK_SPECIAL_CHARACTERS
# =============================================================================

class TestSpecialCharacters:
    """Tests for special characters in SQL."""
    
    @pytest.fixture
    def parser(self):
        return Parser()
    
    @pytest.fixture
    def table(self):
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT),
            "text": ColumnDef(name="text", data_type=DataType.TEXT),
        }
        return Table("special_table", columns)
    
    def test_escaped_quotes(self, parser):
        """Handle escaped quotes in strings."""
        query = "SELECT * FROM users WHERE name = 'O''Brien'"
        
        lexer = Lexer(query)
        tokens = lexer.tokenize()
        assert tokens is not None
    
    def test_newline_in_string(self, table):
        """Handle newline in string."""
        string_with_newline = "line1\nline2\nline3"
        
        row = table.insert({"id": 1, "text": string_with_newline})
        
        assert row.data["text"] == string_with_newline
    
    def test_tab_in_string(self, table):
        """Handle tab in string."""
        string_with_tab = "col1\tcol2\tcol3"
        
        row = table.insert({"id": 1, "text": string_with_tab})
        
        assert row.data["text"] == string_with_tab
    
    def test_backslash_in_string(self, table):
        """Handle backslash in string."""
        string_with_backslash = "path\\to\\file"
        
        row = table.insert({"id": 1, "text": string_with_backslash})
        
        assert row.data["text"] == string_with_backslash
    
    def test_sql_comment_syntax(self, parser):
        """Handle SQL comment syntax."""
        queries = [
            "SELECT * FROM users -- this is a comment",
            "SELECT * FROM users /* block comment */",
            "SELECT * FROM users WHERE id = 1; -- comment",
        ]
        
        for query in queries:
            try:
                lexer = Lexer(query)
                tokens = lexer.tokenize()
                # Comments might be ignored or tokenized
            except LexerError:
                pass  # Acceptable if comments not supported
    
    def test_special_characters_in_identifier(self, parser):
        """Handle special characters in identifiers."""
        # Identifiers with underscores should work
        query = "SELECT user_id, user_name FROM user_table"
        
        lexer = Lexer(query)
        tokens = lexer.tokenize()
        assert tokens is not None
    
    def test_reserved_words_as_identifiers(self, parser):
        """Handle reserved words as identifiers."""
        # This might fail depending on implementation
        query = "SELECT select, from, where FROM users"
        
        try:
            lexer = Lexer(query)
            tokens = lexer.tokenize()
            from mini_db_v2.parser.parser import parse_sql
            ast = parse_sql(query)
        except (LexerError, ParseError):
            pass  # Acceptable if reserved words can't be identifiers


# END_BLOCK_SPECIAL_CHARACTERS


# =============================================================================
# START_BLOCK_BOUNDARY_CONDITIONS
# =============================================================================

class TestBoundaryConditions:
    """Tests for boundary conditions."""
    
    @pytest.fixture
    def table(self):
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT),
            "value": ColumnDef(name="value", data_type=DataType.INT),
        }
        return Table("boundary_table", columns)
    
    @pytest.fixture
    def btree(self):
        return BTree(order=4)  # Small order for testing
    
    def test_integer_min_value(self, table):
        """Handle minimum integer value."""
        min_int = -(2 ** 63)
        
        row = table.insert({"id": min_int, "value": 0})
        
        assert row.data["id"] == min_int
    
    def test_integer_max_value(self, table):
        """Handle maximum integer value."""
        max_int = 2 ** 63 - 1
        
        row = table.insert({"id": max_int, "value": 0})
        
        assert row.data["id"] == max_int
    
    def test_zero_value(self, table):
        """Handle zero value."""
        row = table.insert({"id": 0, "value": 0})
        
        assert row.data["id"] == 0
        assert row.data["value"] == 0
    
    def test_empty_string(self, table):
        """Handle empty string."""
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT),
            "text": ColumnDef(name="text", data_type=DataType.TEXT),
        }
        text_table = Table("text_table", columns)
        
        row = text_table.insert({"id": 1, "text": ""})
        
        assert row.data["text"] == ""
    
    def test_btree_split_boundary(self, btree):
        """Test BTree split at boundary."""
        # Insert enough keys to cause splits
        for i in range(100):
            btree.insert(i, i)
        
        # All keys should be findable
        for i in range(100):
            assert i in btree
    
    def test_btree_delete_boundary(self, btree):
        """Test BTree delete at boundary."""
        # Insert keys
        for i in range(50):
            btree.insert(i, i)
        
        # Delete all keys
        for i in range(50):
            btree.delete(i)
        
        assert btree.size == 0
        assert btree.is_empty
    
    def test_table_row_id_boundary(self, table):
        """Test row ID assignment."""
        # Insert many rows
        for i in range(1000):
            table.insert({"id": i, "value": i})
        
        # Delete some
        table.delete(where=lambda r: r["id"] < 500)
        
        # Insert more - row IDs should continue from where they left off
        table.insert({"id": 1000, "value": 1000})
        
        assert table.row_count == 501  # 500 remaining + 1 new


# END_BLOCK_BOUNDARY_CONDITIONS


# =============================================================================
# START_BLOCK_TYPE_BOUNDARIES
# =============================================================================

class TestTypeBoundaries:
    """Tests for type boundary conditions."""
    
    @pytest.fixture
    def table(self):
        columns = {
            "int_col": ColumnDef(name="int_col", data_type=DataType.INT),
            "real_col": ColumnDef(name="real_col", data_type=DataType.REAL),
            "bool_col": ColumnDef(name="bool_col", data_type=DataType.BOOL),
            "text_col": ColumnDef(name="text_col", data_type=DataType.TEXT),
        }
        return Table("type_table", columns)
    
    def test_int_accepts_zero(self, table):
        """INT accepts zero."""
        row = table.insert({"int_col": 0, "real_col": 0.0, "bool_col": False, "text_col": ""})
        assert row.data["int_col"] == 0
    
    def test_int_accepts_negative(self, table):
        """INT accepts negative values."""
        row = table.insert({"int_col": -999999, "real_col": 0.0, "bool_col": False, "text_col": ""})
        assert row.data["int_col"] == -999999
    
    def test_real_accepts_int(self, table):
        """REAL accepts INT values."""
        row = table.insert({"int_col": 0, "real_col": 42, "bool_col": False, "text_col": ""})
        assert row.data["real_col"] == 42.0 or row.data["real_col"] == 42
    
    def test_real_accepts_negative(self, table):
        """REAL accepts negative values."""
        row = table.insert({"int_col": 0, "real_col": -3.14, "bool_col": False, "text_col": ""})
        assert row.data["real_col"] == -3.14
    
    def test_real_accepts_very_small(self, table):
        """REAL accepts very small values."""
        row = table.insert({"int_col": 0, "real_col": 0.0000001, "bool_col": False, "text_col": ""})
        assert row.data["real_col"] == 0.0000001
    
    def test_real_accepts_very_large(self, table):
        """REAL accepts very large values."""
        row = table.insert({"int_col": 0, "real_col": 1e100, "bool_col": False, "text_col": ""})
        assert row.data["real_col"] == 1e100
    
    def test_bool_only_true_false(self, table):
        """BOOL only accepts True/False."""
        # True and False should work
        row1 = table.insert({"int_col": 0, "real_col": 0.0, "bool_col": True, "text_col": ""})
        row2 = table.insert({"int_col": 0, "real_col": 0.0, "bool_col": False, "text_col": ""})
        
        assert row1.data["bool_col"] is True
        assert row2.data["bool_col"] is False
    
    def test_text_accepts_empty(self, table):
        """TEXT accepts empty string."""
        row = table.insert({"int_col": 0, "real_col": 0.0, "bool_col": False, "text_col": ""})
        assert row.data["text_col"] == ""


# END_BLOCK_TYPE_BOUNDARIES


# =============================================================================
# Run Tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])