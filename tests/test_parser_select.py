# Tests for SELECT parser functionality
# Phase 3: DQL + WHERE

import pytest

from mini_db.parser.parser import Parser, ParseError
from mini_db.ast.nodes import (
    SelectNode,
    IdentifierNode,
    LiteralNode,
    ComparisonNode,
    LogicalNode,
)


# START_BLOCK_SELECT_BASIC
class TestSelectBasic:
    """Базовые тесты для SELECT без WHERE."""
    
    def test_select_star(self):
        """SELECT * FROM table_name."""
        parser = Parser()
        ast = parser.parse("SELECT * FROM users")
        
        assert isinstance(ast, SelectNode)
        assert ast.table == "users"
        assert ast.columns is None
        assert ast.where is None
    
    def test_select_single_column(self):
        """SELECT col1 FROM table_name."""
        parser = Parser()
        ast = parser.parse("SELECT name FROM users")
        
        assert isinstance(ast, SelectNode)
        assert ast.table == "users"
        assert ast.columns == ["name"]
        assert ast.where is None
    
    def test_select_multiple_columns(self):
        """SELECT col1, col2, col3 FROM table_name."""
        parser = Parser()
        ast = parser.parse("SELECT id, name, age FROM users")
        
        assert isinstance(ast, SelectNode)
        assert ast.table == "users"
        assert ast.columns == ["id", "name", "age"]
        assert ast.where is None
    
    def test_select_with_semicolon(self):
        """SELECT * FROM table_name;"""
        parser = Parser()
        ast = parser.parse("SELECT * FROM users;")
        
        assert isinstance(ast, SelectNode)
        assert ast.table == "users"
    
    def test_select_case_insensitive(self):
        """select * from users (lowercase keywords)."""
        parser = Parser()
        ast = parser.parse("select * from users")
        
        assert isinstance(ast, SelectNode)
        assert ast.table == "users"
# END_BLOCK_SELECT_BASIC


# START_BLOCK_SELECT_WHERE_SIMPLE
class TestSelectWhereSimple:
    """Тесты для SELECT с простыми WHERE условиями."""
    
    def test_where_equals_number(self):
        """SELECT * FROM t WHERE col = 10."""
        parser = Parser()
        ast = parser.parse("SELECT * FROM users WHERE id = 1")
        
        assert isinstance(ast, SelectNode)
        assert ast.table == "users"
        assert ast.columns is None
        assert ast.where is not None
        assert isinstance(ast.where, ComparisonNode)
        assert ast.where.op == "="
    
    def test_where_equals_string(self):
        """SELECT * FROM t WHERE col = 'text'."""
        parser = Parser()
        ast = parser.parse("SELECT * FROM users WHERE name = 'John'")
        
        assert isinstance(ast, SelectNode)
        assert ast.where is not None
        assert isinstance(ast.where, ComparisonNode)
        assert ast.where.op == "="
        assert isinstance(ast.where.right, LiteralNode)
        assert ast.where.right.value == "John"
    
    def test_where_not_equals(self):
        """SELECT * FROM t WHERE col != value."""
        parser = Parser()
        ast = parser.parse("SELECT * FROM users WHERE status != 'inactive'")
        
        assert isinstance(ast, SelectNode)
        assert isinstance(ast.where, ComparisonNode)
        assert ast.where.op == "!="
    
    def test_where_less_than(self):
        """SELECT * FROM t WHERE col < value."""
        parser = Parser()
        ast = parser.parse("SELECT * FROM products WHERE price < 100")
        
        assert isinstance(ast, SelectNode)
        assert isinstance(ast.where, ComparisonNode)
        assert ast.where.op == "<"
    
    def test_where_greater_than(self):
        """SELECT * FROM t WHERE col > value."""
        parser = Parser()
        ast = parser.parse("SELECT * FROM products WHERE price > 50")
        
        assert isinstance(ast, SelectNode)
        assert isinstance(ast.where, ComparisonNode)
        assert ast.where.op == ">"
    
    def test_where_equals_bool_true(self):
        """SELECT * FROM t WHERE col = true."""
        parser = Parser()
        ast = parser.parse("SELECT * FROM users WHERE active = true")
        
        assert isinstance(ast, SelectNode)
        assert isinstance(ast.where, ComparisonNode)
        assert isinstance(ast.where.right, LiteralNode)
        assert ast.where.right.value is True
    
    def test_where_equals_bool_false(self):
        """SELECT * FROM t WHERE col = false."""
        parser = Parser()
        ast = parser.parse("SELECT * FROM users WHERE deleted = false")
        
        assert isinstance(ast, SelectNode)
        assert isinstance(ast.where, ComparisonNode)
        assert isinstance(ast.where.right, LiteralNode)
        assert ast.where.right.value is False
    
    def test_where_equals_null(self):
        """SELECT * FROM t WHERE col = null."""
        parser = Parser()
        ast = parser.parse("SELECT * FROM users WHERE phone = null")
        
        assert isinstance(ast, SelectNode)
        assert isinstance(ast.where, ComparisonNode)
        assert isinstance(ast.where.right, LiteralNode)
        assert ast.where.right.value is None
# END_BLOCK_SELECT_WHERE_SIMPLE


# START_BLOCK_SELECT_WHERE_LOGICAL
class TestSelectWhereLogical:
    """Тесты для SELECT с логическими операторами AND/OR."""
    
    def test_where_and(self):
        """SELECT * FROM t WHERE col1 = 1 AND col2 = 2."""
        parser = Parser()
        ast = parser.parse("SELECT * FROM users WHERE age > 18 AND active = true")
        
        assert isinstance(ast, SelectNode)
        assert isinstance(ast.where, LogicalNode)
        assert ast.where.op == "AND"
    
    def test_where_or(self):
        """SELECT * FROM t WHERE col1 = 1 OR col2 = 2."""
        parser = Parser()
        ast = parser.parse("SELECT * FROM users WHERE status = 'admin' OR status = 'moderator'")
        
        assert isinstance(ast, SelectNode)
        assert isinstance(ast.where, LogicalNode)
        assert ast.where.op == "OR"
    
    def test_where_multiple_and(self):
        """SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3."""
        parser = Parser()
        ast = parser.parse("SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3")
        
        assert isinstance(ast, SelectNode)
        assert isinstance(ast.where, LogicalNode)
        assert ast.where.op == "AND"
        # Left should be (a = 1 AND b = 2)
        assert isinstance(ast.where.left, LogicalNode)
        assert ast.where.left.op == "AND"
    
    def test_where_and_or_precedence(self):
        """AND has higher precedence than OR: a OR b AND c = a OR (b AND c)."""
        parser = Parser()
        ast = parser.parse("SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3")
        
        assert isinstance(ast, SelectNode)
        assert isinstance(ast.where, LogicalNode)
        assert ast.where.op == "OR"
        # Right should be (b = 2 AND c = 3) due to AND precedence
        assert isinstance(ast.where.right, LogicalNode)
        assert ast.where.right.op == "AND"
# END_BLOCK_SELECT_WHERE_LOGICAL


# START_BLOCK_SELECT_WHERE_PARENS
class TestSelectWhereParens:
    """Тесты для SELECT со скобками в WHERE."""
    
    def test_where_parenthesized(self):
        """SELECT * FROM t WHERE (col = 1)."""
        parser = Parser()
        ast = parser.parse("SELECT * FROM users WHERE (id = 1)")
        
        assert isinstance(ast, SelectNode)
        assert isinstance(ast.where, ComparisonNode)
    
    def test_where_nested_parens(self):
        """SELECT * FROM t WHERE ((col = 1))."""
        parser = Parser()
        ast = parser.parse("SELECT * FROM users WHERE ((id = 1))")
        
        assert isinstance(ast, SelectNode)
        assert isinstance(ast.where, ComparisonNode)
    
    def test_where_parens_change_precedence(self):
        """(a OR b) AND c changes precedence."""
        parser = Parser()
        ast = parser.parse("SELECT * FROM t WHERE (a = 1 OR b = 2) AND c = 3")
        
        assert isinstance(ast, SelectNode)
        assert isinstance(ast.where, LogicalNode)
        assert ast.where.op == "AND"
        # Left should be (a = 1 OR b = 2)
        assert isinstance(ast.where.left, LogicalNode)
        assert ast.where.left.op == "OR"
    
    def test_where_complex_nested(self):
        """Complex nested: ((a = 1 OR b = 2) AND (c = 3 OR d = 4))."""
        parser = Parser()
        ast = parser.parse(
            "SELECT * FROM t WHERE (a = 1 OR b = 2) AND (c = 3 OR d = 4)"
        )
        
        assert isinstance(ast, SelectNode)
        assert isinstance(ast.where, LogicalNode)
        assert ast.where.op == "AND"
# END_BLOCK_SELECT_WHERE_PARENS


# START_BLOCK_SELECT_ERRORS
class TestSelectErrors:
    """Тесты для ошибок парсинга SELECT."""
    
    def test_missing_from(self):
        """SELECT * (missing FROM)."""
        parser = Parser()
        with pytest.raises(ParseError):
            parser.parse("SELECT *")
    
    def test_missing_table_name(self):
        """SELECT * FROM (missing table name)."""
        parser = Parser()
        with pytest.raises(ParseError):
            parser.parse("SELECT * FROM")
    
    def test_invalid_column_list(self):
        """SELECT ,,, FROM t."""
        parser = Parser()
        with pytest.raises(ParseError):
            parser.parse("SELECT ,,, FROM t")
    
    def test_missing_where_expression(self):
        """SELECT * FROM t WHERE."""
        parser = Parser()
        with pytest.raises(ParseError):
            parser.parse("SELECT * FROM t WHERE")
    
    def test_unclosed_paren(self):
        """SELECT * FROM t WHERE (col = 1."""
        parser = Parser()
        with pytest.raises(ParseError):
            parser.parse("SELECT * FROM t WHERE (col = 1")
# END_BLOCK_SELECT_ERRORS