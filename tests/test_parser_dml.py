# START_MODULE_CONTRACT
# Module: tests.test_parser_dml
# Intent: Unit tests для парсинга INSERT.
# END_MODULE_CONTRACT

import pytest

from mini_db.ast.nodes import InsertNode
from mini_db.parser import Parser, ParseError


class TestParseInsert:
    """Tests for INSERT parsing."""
    
    def test_simple_insert(self):
        """Parse simple INSERT with columns and values."""
        parser = Parser()
        ast = parser.parse("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        
        assert isinstance(ast, InsertNode)
        assert ast.table == "users"
        assert ast.columns == ["id", "name"]
        assert ast.values == [1, "Alice"]
    
    def test_insert_with_bool(self):
        """Parse INSERT with boolean values."""
        parser = Parser()
        ast = parser.parse(
            "INSERT INTO users (id, active) VALUES (1, true)"
        )
        
        assert isinstance(ast, InsertNode)
        assert ast.values == [1, True]
    
    def test_insert_with_false(self):
        """Parse INSERT with false value."""
        parser = Parser()
        ast = parser.parse(
            "INSERT INTO users (id, active) VALUES (1, false)"
        )
        
        assert isinstance(ast, InsertNode)
        assert ast.values == [1, False]
    
    def test_insert_with_null(self):
        """Parse INSERT with NULL value."""
        parser = Parser()
        ast = parser.parse(
            "INSERT INTO users (id, name) VALUES (1, null)"
        )
        
        assert isinstance(ast, InsertNode)
        assert ast.values == [1, None]
    
    def test_insert_multiple_values(self):
        """Parse INSERT with multiple values."""
        parser = Parser()
        ast = parser.parse(
            "INSERT INTO users (id, name, age, active) VALUES (1, 'Bob', 25, true)"
        )
        
        assert isinstance(ast, InsertNode)
        assert len(ast.columns) == 4
        assert len(ast.values) == 4
        assert ast.values == [1, "Bob", 25, True]
    
    def test_insert_negative_number(self):
        """Parse INSERT with negative number."""
        parser = Parser()
        ast = parser.parse(
            "INSERT INTO users (id, balance) VALUES (1, -100)"
        )
        
        assert isinstance(ast, InsertNode)
        assert ast.values == [1, -100]
    
    def test_insert_string_with_quotes(self):
        """Parse INSERT with single-quoted string."""
        parser = Parser()
        ast = parser.parse(
            "INSERT INTO users (id, name) VALUES (1, 'O''Brien')"
        )
        
        assert isinstance(ast, InsertNode)
        assert ast.values == [1, "O'Brien"]
    
    def test_insert_double_quoted_string(self):
        """Parse INSERT with double-quoted string."""
        parser = Parser()
        ast = parser.parse(
            'INSERT INTO users (id, name) VALUES (1, "Alice")'
        )
        
        assert isinstance(ast, InsertNode)
        assert ast.values == [1, "Alice"]
    
    def test_insert_without_columns(self):
        """Parse INSERT without column list."""
        parser = Parser()
        ast = parser.parse(
            "INSERT INTO users VALUES (1, 'Alice', 25, true)"
        )
        
        assert isinstance(ast, InsertNode)
        assert ast.table == "users"
        assert ast.columns == []
        assert len(ast.values) == 4
    
    def test_insert_with_semicolon(self):
        """Parse INSERT with trailing semicolon."""
        parser = Parser()
        ast = parser.parse(
            "INSERT INTO users (id) VALUES (1);"
        )
        
        assert isinstance(ast, InsertNode)
    
    def test_insert_case_insensitive(self):
        """Parse INSERT with mixed case keywords."""
        parser = Parser()
        ast = parser.parse(
            "insert into Users (Id) values (1)"
        )
        
        assert isinstance(ast, InsertNode)
        assert ast.table == "Users"
        assert ast.columns == ["Id"]
    
    def test_insert_missing_table(self):
        """Error when table name is missing."""
        parser = Parser()
        
        with pytest.raises(ParseError) as exc_info:
            parser.parse("INSERT INTO VALUES (1)")
        
        assert "Expected table name" in str(exc_info.value)
    
    def test_insert_missing_values_keyword(self):
        """Error when VALUES keyword is missing."""
        parser = Parser()
        
        with pytest.raises(ParseError) as exc_info:
            parser.parse("INSERT INTO users (id) (1)")
        
        assert "Expected VALUES" in str(exc_info.value)
    
    def test_insert_missing_values(self):
        """Error when values are missing."""
        parser = Parser()
        
        with pytest.raises(ParseError) as exc_info:
            parser.parse("INSERT INTO users (id) VALUES")
        
        assert "Expected '('" in str(exc_info.value)
    
    def test_insert_missing_closing_paren_values(self):
        """Error when closing parenthesis in values is missing."""
        parser = Parser()
        
        with pytest.raises(ParseError) as exc_info:
            parser.parse("INSERT INTO users (id) VALUES (1")
        
        assert "Expected ')'" in str(exc_info.value)
    
    def test_insert_missing_into(self):
        """Error when INTO keyword is missing."""
        parser = Parser()
        
        with pytest.raises(ParseError) as exc_info:
            parser.parse("INSERT users (id) VALUES (1)")
        
        assert "Expected INTO" in str(exc_info.value)
    
    def test_insert_empty_values(self):
        """Error when value list is empty."""
        parser = Parser()
        
        with pytest.raises(ParseError) as exc_info:
            parser.parse("INSERT INTO users (id) VALUES ()")
        
        assert "Expected literal value" in str(exc_info.value)
    
    def test_insert_trailing_comma_values(self):
        """Error when trailing comma in values."""
        parser = Parser()
        
        with pytest.raises(ParseError) as exc_info:
            parser.parse("INSERT INTO users (id) VALUES (1,)")
        
        assert "Expected literal value" in str(exc_info.value)


class TestInsertNode:
    """Tests for InsertNode dataclass."""
    
    def test_insert_node_structure(self):
        """InsertNode has correct structure."""
        node = InsertNode(
            table="users",
            columns=["id", "name"],
            values=[1, "Alice"]
        )
        
        assert node.table == "users"
        assert node.columns == ["id", "name"]
        assert node.values == [1, "Alice"]
    
    def test_insert_node_empty_columns(self):
        """InsertNode can have empty columns list."""
        node = InsertNode(
            table="users",
            columns=[],
            values=[1, "Alice", True]
        )
        
        assert node.columns == []
        assert len(node.values) == 3


class TestInsertValueTypes:
    """Tests for different value types in INSERT."""
    
    def test_all_types_together(self):
        """Parse INSERT with all supported types."""
        parser = Parser()
        ast = parser.parse(
            "INSERT INTO t (c_int, c_text, c_bool, c_null) VALUES (42, 'text', true, null)"
        )
        
        assert ast.values[0] == 42
        assert isinstance(ast.values[0], int)
        
        assert ast.values[1] == "text"
        assert isinstance(ast.values[1], str)
        
        assert ast.values[2] is True
        assert isinstance(ast.values[2], bool)
        
        assert ast.values[3] is None
    
    def test_multiple_integers(self):
        """Parse INSERT with multiple integers."""
        parser = Parser()
        ast = parser.parse(
            "INSERT INTO t (a, b, c) VALUES (1, 2, 3)"
        )
        
        assert ast.values == [1, 2, 3]
    
    def test_multiple_strings(self):
        """Parse INSERT with multiple strings."""
        parser = Parser()
        ast = parser.parse(
            "INSERT INTO t (a, b, c) VALUES ('one', 'two', 'three')"
        )
        
        assert ast.values == ["one", "two", "three"]