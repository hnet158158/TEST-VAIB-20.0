# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_parser_phase2
# Intent: Comprehensive tests for SQL Parser Phase 2 - DDL, DML, SELECT.
# Dependencies: pytest
# END_MODULE_CONTRACT

"""
Phase 2 B-Tree Index - Parser Tests

Tests cover:
- DDL: CREATE TABLE, CREATE INDEX, DROP TABLE, DROP INDEX
- DML: INSERT, UPDATE, DELETE
- SELECT: FROM, WHERE, ORDER BY, LIMIT, DISTINCT
- Expression parsing with operator precedence
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from mini_db_v2.parser.parser import Parser, ParseError, parse_sql
from mini_db_v2.parser.lexer import Lexer, TokenType
from mini_db_v2.ast.nodes import (
    SelectNode, InsertNode, UpdateNode, DeleteNode,
    CreateTableNode, CreateIndexNode, DropTableNode, DropIndexNode,
    ColumnDef, ColumnRef, TableRef, FromClause, JoinClause,
    SelectColumn, OrderByItem, LiteralNode, BinaryOpNode, UnaryOpNode,
    BinaryOperator, UnaryOperator, DataType, JoinType, StarColumn,
    BetweenNode, InListNode, AggregateNode, AggregateType
)


# =============================================================================
# CREATE TABLE Tests
# =============================================================================

class TestParseCreateTable:
    """Tests for CREATE TABLE parsing."""

    def test_create_table_basic(self):
        """Parse basic CREATE TABLE."""
        sql = "CREATE TABLE users (id INT, name TEXT)"
        ast = parse_sql(sql)
        
        assert isinstance(ast, CreateTableNode)
        assert ast.table_name == "users"
        assert len(ast.columns) == 2
        assert ast.columns[0].name == "id"
        assert ast.columns[0].data_type == DataType.INT
        assert ast.columns[1].name == "name"
        assert ast.columns[1].data_type == DataType.TEXT

    def test_create_table_with_constraints(self):
        """Parse CREATE TABLE with constraints."""
        sql = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)"
        ast = parse_sql(sql)
        
        assert ast.columns[0].primary_key is True
        assert ast.columns[1].nullable is False
        assert ast.columns[2].unique is True

    def test_create_table_all_types(self):
        """Parse CREATE TABLE with all data types."""
        sql = "CREATE TABLE data (a INT, b TEXT, c REAL, d BOOL)"
        ast = parse_sql(sql)
        
        assert ast.columns[0].data_type == DataType.INT
        assert ast.columns[1].data_type == DataType.TEXT
        assert ast.columns[2].data_type == DataType.REAL
        assert ast.columns[3].data_type == DataType.BOOL

    def test_create_table_multiple_columns(self):
        """Parse CREATE TABLE with many columns."""
        sql = "CREATE TABLE big (c1 INT, c2 INT, c3 INT, c4 INT, c5 INT)"
        ast = parse_sql(sql)
        
        assert len(ast.columns) == 5

    def test_create_table_if_not_exists(self):
        """Parse CREATE TABLE IF NOT EXISTS."""
        sql = "CREATE TABLE IF NOT EXISTS users (id INT)"
        ast = parse_sql(sql)
        
        assert ast.if_not_exists is True


# =============================================================================
# CREATE INDEX Tests
# =============================================================================

class TestParseCreateIndex:
    """Tests for CREATE INDEX parsing."""

    def test_create_index_basic(self):
        """Parse basic CREATE INDEX."""
        sql = "CREATE INDEX idx_name ON users (name)"
        ast = parse_sql(sql)
        
        assert isinstance(ast, CreateIndexNode)
        assert ast.index_name == "idx_name"
        assert ast.table_name == "users"
        assert len(ast.columns) == 1
        assert ast.columns[0].column_name == "name"
        assert ast.unique is False

    def test_create_unique_index(self):
        """Parse CREATE UNIQUE INDEX."""
        sql = "CREATE UNIQUE INDEX idx_email ON users (email)"
        ast = parse_sql(sql)
        
        assert ast.unique is True

    def test_create_index_if_not_exists(self):
        """Parse CREATE INDEX IF NOT EXISTS."""
        sql = "CREATE INDEX IF NOT EXISTS idx_name ON users (name)"
        ast = parse_sql(sql)
        
        assert ast.if_not_exists is True


# =============================================================================
# DROP TABLE Tests
# =============================================================================

class TestParseDropTable:
    """Tests for DROP TABLE parsing."""

    def test_drop_table_basic(self):
        """Parse basic DROP TABLE."""
        sql = "DROP TABLE users"
        ast = parse_sql(sql)
        
        assert isinstance(ast, DropTableNode)
        assert ast.table_name == "users"
        assert ast.if_exists is False

    def test_drop_table_if_exists(self):
        """Parse DROP TABLE IF EXISTS."""
        sql = "DROP TABLE IF EXISTS users"
        ast = parse_sql(sql)
        
        assert ast.if_exists is True


# =============================================================================
# DROP INDEX Tests
# =============================================================================

class TestParseDropIndex:
    """Tests for DROP INDEX parsing."""

    def test_drop_index_basic(self):
        """Parse basic DROP INDEX."""
        sql = "DROP INDEX idx_name"
        ast = parse_sql(sql)
        
        assert isinstance(ast, DropIndexNode)
        assert ast.index_name == "idx_name"
        assert ast.if_exists is False

    def test_drop_index_if_exists(self):
        """Parse DROP INDEX IF EXISTS."""
        sql = "DROP INDEX IF EXISTS idx_name"
        ast = parse_sql(sql)
        
        assert ast.if_exists is True


# =============================================================================
# INSERT Tests
# =============================================================================

class TestParseInsert:
    """Tests for INSERT parsing."""

    def test_insert_basic(self):
        """Parse basic INSERT."""
        sql = "INSERT INTO users VALUES (1, 'John')"
        ast = parse_sql(sql)
        
        assert isinstance(ast, InsertNode)
        assert ast.table_name == "users"
        assert len(ast.values) == 1
        assert len(ast.values[0]) == 2

    def test_insert_with_columns(self):
        """Parse INSERT with column names."""
        sql = "INSERT INTO users (id, name) VALUES (1, 'John')"
        ast = parse_sql(sql)
        
        assert ast.columns == ["id", "name"]
        assert len(ast.values) == 1

    def test_insert_multiple_rows(self):
        """Parse INSERT with multiple rows."""
        sql = "INSERT INTO users VALUES (1, 'John'), (2, 'Jane')"
        ast = parse_sql(sql)
        
        assert len(ast.values) == 2

    def test_insert_null(self):
        """Parse INSERT with NULL."""
        sql = "INSERT INTO users VALUES (NULL, 'John')"
        ast = parse_sql(sql)
        
        assert isinstance(ast.values[0][0], LiteralNode)
        assert ast.values[0][0].value is None

    def test_insert_expression(self):
        """Parse INSERT with expression."""
        sql = "INSERT INTO users VALUES (1 + 1, 'John')"
        ast = parse_sql(sql)
        
        assert isinstance(ast.values[0][0], BinaryOpNode)


# =============================================================================
# UPDATE Tests
# =============================================================================

class TestParseUpdate:
    """Tests for UPDATE parsing."""

    def test_update_basic(self):
        """Parse basic UPDATE."""
        sql = "UPDATE users SET name = 'Jane'"
        ast = parse_sql(sql)
        
        assert isinstance(ast, UpdateNode)
        assert ast.table_name == "users"
        assert "name" in ast.assignments
        assert ast.where is None

    def test_update_with_where(self):
        """Parse UPDATE with WHERE."""
        sql = "UPDATE users SET name = 'Jane' WHERE id = 1"
        ast = parse_sql(sql)
        
        assert ast.where is not None
        assert isinstance(ast.where, BinaryOpNode)

    def test_update_multiple_columns(self):
        """Parse UPDATE with multiple columns."""
        sql = "UPDATE users SET name = 'Jane', active = TRUE WHERE id = 1"
        ast = parse_sql(sql)
        
        assert len(ast.assignments) == 2
        assert "name" in ast.assignments
        assert "active" in ast.assignments


# =============================================================================
# DELETE Tests
# =============================================================================

class TestParseDelete:
    """Tests for DELETE parsing."""

    def test_delete_basic(self):
        """Parse basic DELETE."""
        sql = "DELETE FROM users"
        ast = parse_sql(sql)
        
        assert isinstance(ast, DeleteNode)
        assert ast.table_name == "users"
        assert ast.where is None

    def test_delete_with_where(self):
        """Parse DELETE with WHERE."""
        sql = "DELETE FROM users WHERE id = 1"
        ast = parse_sql(sql)
        
        assert ast.where is not None

    def test_delete_with_complex_where(self):
        """Parse DELETE with complex WHERE."""
        sql = "DELETE FROM users WHERE id > 10 AND active = FALSE"
        ast = parse_sql(sql)
        
        assert isinstance(ast.where, BinaryOpNode)
        assert ast.where.operator == BinaryOperator.AND


# =============================================================================
# SELECT Tests
# =============================================================================

class TestParseSelect:
    """Tests for SELECT parsing."""

    def test_select_star(self):
        """Parse SELECT *."""
        sql = "SELECT * FROM users"
        ast = parse_sql(sql)
        
        assert isinstance(ast, SelectNode)
        assert len(ast.columns) == 1
        assert isinstance(ast.columns[0].expression, StarColumn)

    def test_select_columns(self):
        """Parse SELECT with specific columns."""
        sql = "SELECT id, name FROM users"
        ast = parse_sql(sql)
        
        assert len(ast.columns) == 2
        assert ast.columns[0].expression.column_name == "id"
        assert ast.columns[1].expression.column_name == "name"

    def test_select_with_alias(self):
        """Parse SELECT with column alias."""
        sql = "SELECT id AS user_id, name FROM users"
        ast = parse_sql(sql)
        
        assert ast.columns[0].alias == "user_id"

    def test_select_with_where(self):
        """Parse SELECT with WHERE."""
        sql = "SELECT * FROM users WHERE id = 1"
        ast = parse_sql(sql)
        
        assert ast.where is not None
        assert isinstance(ast.where, BinaryOpNode)

    def test_select_with_order_by(self):
        """Parse SELECT with ORDER BY."""
        sql = "SELECT * FROM users ORDER BY name"
        ast = parse_sql(sql)
        
        assert len(ast.order_by) == 1
        assert ast.order_by[0].ascending is True

    def test_select_with_order_by_desc(self):
        """Parse SELECT with ORDER BY DESC."""
        sql = "SELECT * FROM users ORDER BY name DESC"
        ast = parse_sql(sql)
        
        assert ast.order_by[0].ascending is False

    def test_select_with_limit(self):
        """Parse SELECT with LIMIT."""
        sql = "SELECT * FROM users LIMIT 10"
        ast = parse_sql(sql)
        
        assert ast.limit == 10

    def test_select_with_offset(self):
        """Parse SELECT with OFFSET."""
        sql = "SELECT * FROM users LIMIT 10 OFFSET 5"
        ast = parse_sql(sql)
        
        assert ast.limit == 10
        assert ast.offset == 5

    def test_select_distinct(self):
        """Parse SELECT DISTINCT."""
        sql = "SELECT DISTINCT name FROM users"
        ast = parse_sql(sql)
        
        assert ast.distinct is True

    def test_select_table_alias(self):
        """Parse SELECT with table alias."""
        sql = "SELECT * FROM users AS u"
        ast = parse_sql(sql)
        
        assert ast.from_clause.table.alias == "u"


# =============================================================================
# Expression Parsing Tests
# =============================================================================

class TestExpressionParsing:
    """Tests for expression parsing."""

    def test_comparison_operators(self):
        """Parse comparison operators."""
        operators = [
            ("id = 1", BinaryOperator.EQ),
            ("id != 1", BinaryOperator.NE),
            ("id < 1", BinaryOperator.LT),
            ("id <= 1", BinaryOperator.LE),
            ("id > 1", BinaryOperator.GT),
            ("id >= 1", BinaryOperator.GE),
        ]
        
        for sql_fragment, expected_op in operators:
            sql = f"SELECT * FROM users WHERE {sql_fragment}"
            ast = parse_sql(sql)
            assert ast.where.operator == expected_op

    def test_logical_operators(self):
        """Parse logical operators."""
        sql = "SELECT * FROM users WHERE id = 1 AND active = TRUE"
        ast = parse_sql(sql)
        
        assert ast.where.operator == BinaryOperator.AND

    def test_or_operator(self):
        """Parse OR operator."""
        sql = "SELECT * FROM users WHERE id = 1 OR id = 2"
        ast = parse_sql(sql)
        
        assert ast.where.operator == BinaryOperator.OR

    def test_not_operator(self):
        """Parse NOT operator."""
        sql = "SELECT * FROM users WHERE NOT active = TRUE"
        ast = parse_sql(sql)
        
        assert isinstance(ast.where, UnaryOpNode)
        assert ast.where.operator == UnaryOperator.NOT

    def test_arithmetic_operators(self):
        """Parse arithmetic operators."""
        sql = "SELECT id + 1 FROM users"
        ast = parse_sql(sql)
        
        assert isinstance(ast.columns[0].expression, BinaryOpNode)
        assert ast.columns[0].expression.operator == BinaryOperator.ADD

    def test_operator_precedence(self):
        """Test operator precedence (AND before OR)."""
        sql = "SELECT * FROM users WHERE a = 1 OR b = 2 AND c = 3"
        ast = parse_sql(sql)
        
        # Should be: a = 1 OR (b = 2 AND c = 3)
        assert ast.where.operator == BinaryOperator.OR
        assert ast.where.right.operator == BinaryOperator.AND

    def test_parentheses_override_precedence(self):
        """Parentheses override operator precedence."""
        sql = "SELECT * FROM users WHERE (a = 1 OR b = 2) AND c = 3"
        ast = parse_sql(sql)
        
        # Should be: (a = 1 OR b = 2) AND c = 3
        assert ast.where.operator == BinaryOperator.AND
        assert ast.where.left.operator == BinaryOperator.OR

    def test_between_expression(self):
        """Parse BETWEEN expression."""
        sql = "SELECT * FROM users WHERE id BETWEEN 1 AND 10"
        ast = parse_sql(sql)
        
        assert isinstance(ast.where, BetweenNode)

    def test_in_list_expression(self):
        """Parse IN list expression."""
        sql = "SELECT * FROM users WHERE id IN (1, 2, 3)"
        ast = parse_sql(sql)
        
        assert isinstance(ast.where, InListNode)
        assert len(ast.where.values) == 3

    def test_like_expression(self):
        """Parse LIKE expression."""
        sql = "SELECT * FROM users WHERE name LIKE 'John%'"
        ast = parse_sql(sql)
        
        assert isinstance(ast.where, BinaryOpNode)
        assert ast.where.operator == BinaryOperator.LIKE

    def test_is_null(self):
        """Parse IS NULL expression."""
        sql = "SELECT * FROM users WHERE name IS NULL"
        ast = parse_sql(sql)
        
        assert isinstance(ast.where, UnaryOpNode)
        assert ast.where.operator == UnaryOperator.IS_NULL

    def test_is_not_null(self):
        """Parse IS NOT NULL expression."""
        sql = "SELECT * FROM users WHERE name IS NOT NULL"
        ast = parse_sql(sql)
        
        assert isinstance(ast.where, UnaryOpNode)
        assert ast.where.operator == UnaryOperator.IS_NOT_NULL


# =============================================================================
# Aggregate Function Tests
# =============================================================================

class TestAggregateParsing:
    """Tests for aggregate function parsing."""

    def test_count_star(self):
        """Parse COUNT(*)."""
        sql = "SELECT COUNT(*) FROM users"
        ast = parse_sql(sql)
        
        assert isinstance(ast.columns[0].expression, AggregateNode)
        assert ast.columns[0].expression.agg_type == AggregateType.COUNT

    def test_count_column(self):
        """Parse COUNT(column)."""
        sql = "SELECT COUNT(id) FROM users"
        ast = parse_sql(sql)
        
        assert ast.columns[0].expression.agg_type == AggregateType.COUNT
        assert ast.columns[0].expression.arg is not None

    def test_sum(self):
        """Parse SUM."""
        sql = "SELECT SUM(price) FROM products"
        ast = parse_sql(sql)
        
        assert ast.columns[0].expression.agg_type == AggregateType.SUM

    def test_avg(self):
        """Parse AVG."""
        sql = "SELECT AVG(price) FROM products"
        ast = parse_sql(sql)
        
        assert ast.columns[0].expression.agg_type == AggregateType.AVG

    def test_min_max(self):
        """Parse MIN and MAX."""
        sql = "SELECT MIN(price), MAX(price) FROM products"
        ast = parse_sql(sql)
        
        assert ast.columns[0].expression.agg_type == AggregateType.MIN
        assert ast.columns[1].expression.agg_type == AggregateType.MAX

    def test_count_distinct(self):
        """Parse COUNT(DISTINCT column)."""
        sql = "SELECT COUNT(DISTINCT name) FROM users"
        ast = parse_sql(sql)
        
        assert ast.columns[0].expression.distinct is True


# =============================================================================
# JOIN Parsing Tests
# =============================================================================

class TestJoinParsing:
    """Tests for JOIN parsing."""

    def test_inner_join(self):
        """Parse INNER JOIN."""
        sql = "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
        ast = parse_sql(sql)
        
        assert len(ast.from_clause.joins) == 1
        assert ast.from_clause.joins[0].join_type == JoinType.INNER

    def test_left_join(self):
        """Parse LEFT JOIN."""
        sql = "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id"
        ast = parse_sql(sql)
        
        assert ast.from_clause.joins[0].join_type == JoinType.LEFT

    def test_right_join(self):
        """Parse RIGHT JOIN."""
        sql = "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id"
        ast = parse_sql(sql)
        
        assert ast.from_clause.joins[0].join_type == JoinType.RIGHT

    def test_cross_join(self):
        """Parse CROSS JOIN."""
        sql = "SELECT * FROM users CROSS JOIN orders"
        ast = parse_sql(sql)
        
        assert ast.from_clause.joins[0].join_type == JoinType.CROSS

    def test_multiple_joins(self):
        """Parse multiple JOINs."""
        sql = """
            SELECT * FROM users
            JOIN orders ON users.id = orders.user_id
            JOIN products ON orders.product_id = products.id
        """
        ast = parse_sql(sql)
        
        assert len(ast.from_clause.joins) == 2


# =============================================================================
# Error Handling Tests
# =============================================================================

class TestParseErrors:
    """Tests for parse error handling."""

    def test_invalid_syntax(self):
        """Invalid SQL raises ParseError."""
        with pytest.raises(ParseError):
            parse_sql("SELECT FROM")

    def test_unexpected_token(self):
        """Unexpected token raises ParseError."""
        with pytest.raises(ParseError):
            parse_sql("SELECT * users")  # Missing FROM

    def test_missing_table_name(self):
        """Missing table name raises ParseError."""
        with pytest.raises(ParseError):
            parse_sql("CREATE TABLE (id INT)")

    def test_missing_column_type(self):
        """Missing column type raises ParseError."""
        with pytest.raises(ParseError):
            parse_sql("CREATE TABLE users (id)")


# =============================================================================
# Literal Parsing Tests
# =============================================================================

class TestLiteralParsing:
    """Tests for literal value parsing."""

    def test_integer_literal(self):
        """Parse integer literal."""
        sql = "SELECT 42"
        ast = parse_sql(sql)
        
        assert isinstance(ast.columns[0].expression, LiteralNode)
        assert ast.columns[0].expression.value == 42
        assert ast.columns[0].expression.data_type == DataType.INT

    def test_float_literal(self):
        """Parse float literal."""
        sql = "SELECT 3.14"
        ast = parse_sql(sql)
        
        assert isinstance(ast.columns[0].expression, LiteralNode)
        assert ast.columns[0].expression.value == 3.14

    def test_string_literal(self):
        """Parse string literal."""
        sql = "SELECT 'hello'"
        ast = parse_sql(sql)
        
        assert isinstance(ast.columns[0].expression, LiteralNode)
        assert ast.columns[0].expression.value == "hello"

    def test_boolean_literal(self):
        """Parse boolean literals."""
        sql_true = "SELECT TRUE"
        ast_true = parse_sql(sql_true)
        
        assert ast_true.columns[0].expression.value is True
        
        sql_false = "SELECT FALSE"
        ast_false = parse_sql(sql_false)
        
        assert ast_false.columns[0].expression.value is False

    def test_null_literal(self):
        """Parse NULL literal."""
        sql = "SELECT NULL"
        ast = parse_sql(sql)
        
        assert isinstance(ast.columns[0].expression, LiteralNode)
        assert ast.columns[0].expression.value is None


# =============================================================================
# Run Tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])