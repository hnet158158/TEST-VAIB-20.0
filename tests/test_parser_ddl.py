# START_MODULE_CONTRACT
# Module: tests.test_parser_ddl
# Intent: Unit tests для парсинга CREATE TABLE.
# END_MODULE_CONTRACT

import pytest

from mini_db.ast.nodes import CreateTableNode, ColumnDef
from mini_db.parser import Parser, ParseError


class TestParseCreateTable:
    """Tests for CREATE TABLE parsing."""
    
    def test_simple_create_table(self):
        """Parse simple CREATE TABLE with one column."""
        parser = Parser()
        ast = parser.parse("CREATE TABLE users (id INT)")
        
        assert isinstance(ast, CreateTableNode)
        assert ast.name == "users"
        assert len(ast.columns) == 1
        assert ast.columns[0].name == "id"
        assert ast.columns[0].data_type == "INT"
        assert ast.columns[0].unique is False
    
    def test_create_table_multiple_columns(self):
        """Parse CREATE TABLE with multiple columns."""
        parser = Parser()
        ast = parser.parse(
            "CREATE TABLE users (id INT, name TEXT, active BOOL)"
        )
        
        assert isinstance(ast, CreateTableNode)
        assert ast.name == "users"
        assert len(ast.columns) == 3
        
        assert ast.columns[0].name == "id"
        assert ast.columns[0].data_type == "INT"
        
        assert ast.columns[1].name == "name"
        assert ast.columns[1].data_type == "TEXT"
        
        assert ast.columns[2].name == "active"
        assert ast.columns[2].data_type == "BOOL"
    
    def test_create_table_with_unique(self):
        """Parse CREATE TABLE with UNIQUE constraint."""
        parser = Parser()
        ast = parser.parse(
            "CREATE TABLE users (id INT UNIQUE, name TEXT)"
        )
        
        assert isinstance(ast, CreateTableNode)
        assert len(ast.columns) == 2
        
        assert ast.columns[0].name == "id"
        assert ast.columns[0].unique is True
        
        assert ast.columns[1].name == "name"
        assert ast.columns[1].unique is False
    
    def test_create_table_multiple_unique(self):
        """Parse CREATE TABLE with multiple UNIQUE columns."""
        parser = Parser()
        ast = parser.parse(
            "CREATE TABLE users (id INT UNIQUE, email TEXT UNIQUE)"
        )
        
        assert isinstance(ast, CreateTableNode)
        assert ast.columns[0].unique is True
        assert ast.columns[1].unique is True
    
    def test_create_table_with_semicolon(self):
        """Parse CREATE TABLE with trailing semicolon."""
        parser = Parser()
        ast = parser.parse("CREATE TABLE users (id INT);")
        
        assert isinstance(ast, CreateTableNode)
        assert ast.name == "users"
    
    def test_create_table_case_insensitive(self):
        """Parse CREATE TABLE with mixed case keywords."""
        parser = Parser()
        ast = parser.parse("create table Users (Id int, Name text)")
        
        assert isinstance(ast, CreateTableNode)
        assert ast.name == "Users"
        assert ast.columns[0].name == "Id"
        assert ast.columns[0].data_type == "INT"
        assert ast.columns[1].name == "Name"
        assert ast.columns[1].data_type == "TEXT"
    
    def test_create_table_missing_name(self):
        """Error when table name is missing."""
        parser = Parser()
        
        with pytest.raises(ParseError) as exc_info:
            parser.parse("CREATE TABLE")
        
        assert "Expected table name" in str(exc_info.value)
    
    def test_create_table_missing_parenthesis(self):
        """Error when opening parenthesis is missing."""
        parser = Parser()
        
        with pytest.raises(ParseError) as exc_info:
            parser.parse("CREATE TABLE users id INT)")
        
        assert "Expected '('" in str(exc_info.value)
    
    def test_create_table_missing_closing_parenthesis(self):
        """Error when closing parenthesis is missing."""
        parser = Parser()
        
        with pytest.raises(ParseError) as exc_info:
            parser.parse("CREATE TABLE users (id INT")
        
        assert "Expected ')'" in str(exc_info.value)
    
    def test_create_table_missing_type(self):
        """Error when column type is missing."""
        parser = Parser()
        
        with pytest.raises(ParseError) as exc_info:
            parser.parse("CREATE TABLE users (id)")
        
        assert "Expected data type" in str(exc_info.value)
    
    def test_create_table_invalid_type(self):
        """Error when column type is invalid."""
        parser = Parser()
        
        with pytest.raises(ParseError) as exc_info:
            parser.parse("CREATE TABLE users (id VARCHAR)")
        
        assert "Expected data type" in str(exc_info.value)
    
    def test_create_table_empty_columns(self):
        """Error when column list is empty."""
        parser = Parser()
        
        with pytest.raises(ParseError) as exc_info:
            parser.parse("CREATE TABLE users ()")
        
        # Should fail on expecting column name
        assert "Expected column name" in str(exc_info.value)
    
    def test_create_table_trailing_comma(self):
        """Error when trailing comma without column."""
        parser = Parser()
        
        with pytest.raises(ParseError) as exc_info:
            parser.parse("CREATE TABLE users (id INT,)")
        
        assert "Expected column name" in str(exc_info.value)


class TestColumnDef:
    """Tests for ColumnDef dataclass."""
    
    def test_column_def_defaults(self):
        """ColumnDef with default values."""
        col = ColumnDef(name="id", data_type="INT")
        
        assert col.name == "id"
        assert col.data_type == "INT"
        assert col.unique is False
    
    def test_column_def_with_unique(self):
        """ColumnDef with unique=True."""
        col = ColumnDef(name="id", data_type="INT", unique=True)
        
        assert col.unique is True
    
    def test_column_def_all_types(self):
        """ColumnDef with all supported types."""
        col_int = ColumnDef(name="a", data_type="INT")
        col_text = ColumnDef(name="b", data_type="TEXT")
        col_bool = ColumnDef(name="c", data_type="BOOL")
        
        assert col_int.data_type == "INT"
        assert col_text.data_type == "TEXT"
        assert col_bool.data_type == "BOOL"


class TestCreateTableNode:
    """Tests for CreateTableNode dataclass."""
    
    def test_create_table_node_structure(self):
        """CreateTableNode has correct structure."""
        columns = [
            ColumnDef(name="id", data_type="INT", unique=True),
            ColumnDef(name="name", data_type="TEXT"),
        ]
        node = CreateTableNode(name="users", columns=columns)
        
        assert node.name == "users"
        assert len(node.columns) == 2
        assert node.columns[0].unique is True
        assert node.columns[1].unique is False