# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_lexer
# Intent: Comprehensive tests for SQL lexer (Phase 1 Foundation).
# Dependencies: pytest
# END_MODULE_CONTRACT

"""
Phase 1 Lexer Tests - Comprehensive Coverage

Tests:
- Keywords (SELECT, FROM, WHERE, JOIN, GROUP BY, HAVING, ORDER BY, LIMIT, etc.)
- Data types (INT, TEXT, REAL, BOOL)
- Literals (strings, numbers, NULL, booleans)
- Operators (=, !=, <, >, <=, >=, <>, +, -, *, /, %)
- Punctuation (, ; . ( ))
- Comments (-- and /* */)
- Identifiers
- Edge cases and adversarial inputs
"""

import pytest
import sys
import os

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from mini_db_v2.parser.lexer import (
    Lexer, Token, TokenType, LexerError, tokenize, KEYWORDS
)


# =============================================================================
# Basic Token Tests
# =============================================================================

class TestBasicTokens:
    """Tests for basic token types."""

    def test_empty_input(self):
        """Empty input returns only EOF."""
        tokens = tokenize("")
        assert len(tokens) == 1
        assert tokens[0].type == TokenType.EOF

    def test_whitespace_only(self):
        """Whitespace only returns only EOF."""
        tokens = tokenize("   \t\n\r\n   ")
        assert len(tokens) == 1
        assert tokens[0].type == TokenType.EOF

    def test_single_keyword_select(self):
        """Single SELECT keyword."""
        tokens = tokenize("SELECT")
        assert len(tokens) == 2  # SELECT + EOF
        assert tokens[0].type == TokenType.SELECT
        assert tokens[0].value == "SELECT"

    def test_keyword_case_insensitive(self):
        """Keywords are case-insensitive."""
        for variant in ["SELECT", "select", "Select", "SeLeCt"]:
            tokens = tokenize(variant)
            assert tokens[0].type == TokenType.SELECT

    def test_multiple_keywords(self):
        """Multiple keywords in sequence."""
        tokens = tokenize("SELECT FROM WHERE")
        assert len(tokens) == 4  # 3 keywords + EOF
        assert tokens[0].type == TokenType.SELECT
        assert tokens[1].type == TokenType.FROM
        assert tokens[2].type == TokenType.WHERE


# =============================================================================
# Keyword Tests
# =============================================================================

class TestKeywords:
    """Tests for all SQL keywords."""

    def test_dml_keywords(self):
        """DML keywords: SELECT, INSERT, UPDATE, DELETE."""
        tokens = tokenize("SELECT INSERT UPDATE DELETE")
        assert tokens[0].type == TokenType.SELECT
        assert tokens[1].type == TokenType.INSERT
        assert tokens[2].type == TokenType.UPDATE
        assert tokens[3].type == TokenType.DELETE

    def test_ddl_keywords(self):
        """DDL keywords: CREATE, DROP, TABLE, INDEX."""
        tokens = tokenize("CREATE DROP TABLE INDEX")
        assert tokens[0].type == TokenType.CREATE
        assert tokens[1].type == TokenType.DROP
        assert tokens[2].type == TokenType.TABLE
        assert tokens[3].type == TokenType.INDEX

    def test_join_keywords(self):
        """JOIN keywords: JOIN, INNER, LEFT, RIGHT, FULL, OUTER, CROSS, ON."""
        tokens = tokenize("JOIN INNER LEFT RIGHT FULL OUTER CROSS ON")
        assert tokens[0].type == TokenType.JOIN
        assert tokens[1].type == TokenType.INNER
        assert tokens[2].type == TokenType.LEFT
        assert tokens[3].type == TokenType.RIGHT
        assert tokens[4].type == TokenType.FULL
        assert tokens[5].type == TokenType.OUTER
        assert tokens[6].type == TokenType.CROSS
        assert tokens[7].type == TokenType.ON

    def test_group_by_having_keywords(self):
        """GROUP BY, HAVING keywords."""
        tokens = tokenize("GROUP BY HAVING")
        assert tokens[0].type == TokenType.GROUP
        assert tokens[1].type == TokenType.BY
        assert tokens[2].type == TokenType.HAVING

    def test_order_by_limit_keywords(self):
        """ORDER BY, ASC, DESC, LIMIT, OFFSET keywords."""
        tokens = tokenize("ORDER BY ASC DESC LIMIT OFFSET")
        assert tokens[0].type == TokenType.ORDER
        assert tokens[1].type == TokenType.BY
        assert tokens[2].type == TokenType.ASC
        assert tokens[3].type == TokenType.DESC
        assert tokens[4].type == TokenType.LIMIT
        assert tokens[5].type == TokenType.OFFSET

    def test_aggregate_keywords(self):
        """Aggregate keywords: COUNT, SUM, AVG, MIN, MAX, DISTINCT."""
        tokens = tokenize("COUNT SUM AVG MIN MAX DISTINCT")
        assert tokens[0].type == TokenType.COUNT
        assert tokens[1].type == TokenType.SUM
        assert tokens[2].type == TokenType.AVG
        assert tokens[3].type == TokenType.MIN
        assert tokens[4].type == TokenType.MAX
        assert tokens[5].type == TokenType.DISTINCT

    def test_subquery_keywords(self):
        """Subquery keywords: IN, EXISTS, BETWEEN, LIKE, IS."""
        tokens = tokenize("IN EXISTS BETWEEN LIKE IS")
        assert tokens[0].type == TokenType.IN
        assert tokens[1].type == TokenType.EXISTS
        assert tokens[2].type == TokenType.BETWEEN
        assert tokens[3].type == TokenType.LIKE
        assert tokens[4].type == TokenType.IS

    def test_case_expression_keywords(self):
        """CASE expression keywords: CASE, WHEN, THEN, ELSE, END."""
        tokens = tokenize("CASE WHEN THEN ELSE END")
        assert tokens[0].type == TokenType.CASE
        assert tokens[1].type == TokenType.WHEN
        assert tokens[2].type == TokenType.THEN
        assert tokens[3].type == TokenType.ELSE
        assert tokens[4].type == TokenType.END

    def test_transaction_keywords(self):
        """Transaction keywords: BEGIN, COMMIT, ROLLBACK, TRANSACTION."""
        tokens = tokenize("BEGIN COMMIT ROLLBACK TRANSACTION")
        assert tokens[0].type == TokenType.BEGIN
        assert tokens[1].type == TokenType.COMMIT
        assert tokens[2].type == TokenType.ROLLBACK
        assert tokens[3].type == TokenType.TRANSACTION

    def test_isolation_level_keywords(self):
        """Isolation level keywords: ISOLATION, LEVEL, READ, COMMITTED, REPEATABLE."""
        tokens = tokenize("ISOLATION LEVEL READ COMMITTED REPEATABLE")
        assert tokens[0].type == TokenType.ISOLATION
        assert tokens[1].type == TokenType.LEVEL
        assert tokens[2].type == TokenType.READ
        assert tokens[3].type == TokenType.COMMITTED
        assert tokens[4].type == TokenType.REPEATABLE

    def test_explain_keywords(self):
        """EXPLAIN, ANALYZE keywords."""
        tokens = tokenize("EXPLAIN ANALYZE")
        assert tokens[0].type == TokenType.EXPLAIN
        assert tokens[1].type == TokenType.ANALYZE

    def test_constraint_keywords(self):
        """Constraint keywords: PRIMARY, KEY, UNIQUE, DEFAULT, NOT."""
        tokens = tokenize("PRIMARY KEY UNIQUE DEFAULT NOT")
        assert tokens[0].type == TokenType.PRIMARY
        assert tokens[1].type == TokenType.KEY
        assert tokens[2].type == TokenType.UNIQUE
        assert tokens[3].type == TokenType.DEFAULT
        assert tokens[4].type == TokenType.NOT

    def test_data_type_keywords(self):
        """Data type keywords: INT, INTEGER, TEXT, VARCHAR, REAL, FLOAT, BOOL, BOOLEAN."""
        tokens = tokenize("INT INTEGER TEXT VARCHAR REAL FLOAT BOOL BOOLEAN")
        assert tokens[0].type == TokenType.INT
        assert tokens[1].type == TokenType.INT  # INTEGER -> INT
        assert tokens[2].type == TokenType.TEXT
        assert tokens[3].type == TokenType.TEXT  # VARCHAR -> TEXT
        assert tokens[4].type == TokenType.REAL_KW
        assert tokens[5].type == TokenType.REAL_KW  # FLOAT -> REAL
        assert tokens[6].type == TokenType.BOOL
        assert tokens[7].type == TokenType.BOOL  # BOOLEAN -> BOOL


# =============================================================================
# Literal Tests
# =============================================================================

class TestLiterals:
    """Tests for literal values."""

    def test_integer_literal(self):
        """Integer literals."""
        tokens = tokenize("42")
        assert tokens[0].type == TokenType.INTEGER
        assert tokens[0].value == "42"

    def test_negative_integer(self):
        """Negative integer literals."""
        tokens = tokenize("-42")
        assert tokens[0].type == TokenType.INTEGER
        assert tokens[0].value == "-42"

    def test_large_integer(self):
        """Large integer literals."""
        tokens = tokenize("999999999999")
        assert tokens[0].type == TokenType.INTEGER
        assert tokens[0].value == "999999999999"

    def test_real_literal_with_decimal(self):
        """Real literals with decimal point."""
        tokens = tokenize("3.14")
        assert tokens[0].type == TokenType.REAL
        assert tokens[0].value == "3.14"

    def test_negative_real(self):
        """Negative real literals."""
        tokens = tokenize("-3.14")
        assert tokens[0].type == TokenType.REAL
        assert tokens[0].value == "-3.14"

    def test_real_with_scientific_notation(self):
        """Real literals with scientific notation."""
        tokens = tokenize("1.5e10")
        assert tokens[0].type == TokenType.REAL
        assert tokens[0].value == "1.5e10"

    def test_real_with_negative_exponent(self):
        """Real literals with negative exponent."""
        tokens = tokenize("1.5e-10")
        assert tokens[0].type == TokenType.REAL
        assert tokens[0].value == "1.5e-10"

    def test_string_literal_single_quote(self):
        """String literals with single quotes."""
        tokens = tokenize("'hello world'")
        assert tokens[0].type == TokenType.STRING
        assert tokens[0].value == "hello world"

    def test_string_literal_double_quote(self):
        """String literals with double quotes."""
        tokens = tokenize('"hello world"')
        assert tokens[0].type == TokenType.STRING
        assert tokens[0].value == "hello world"

    def test_empty_string(self):
        """Empty string literal."""
        tokens = tokenize("''")
        assert tokens[0].type == TokenType.STRING
        assert tokens[0].value == ""

    def test_string_with_escaped_quotes(self):
        """String with escaped quotes."""
        tokens = tokenize("'it''s a test'")
        assert tokens[0].type == TokenType.STRING
        assert tokens[0].value == "it's a test"

    def test_string_with_spaces(self):
        """String with spaces."""
        tokens = tokenize("'  spaces  '")
        assert tokens[0].type == TokenType.STRING
        assert tokens[0].value == "  spaces  "

    def test_null_literal(self):
        """NULL literal."""
        tokens = tokenize("NULL")
        assert tokens[0].type == TokenType.NULL
        assert tokens[0].value == "NULL"

    def test_boolean_true(self):
        """TRUE boolean literal."""
        tokens = tokenize("TRUE")
        assert tokens[0].type == TokenType.BOOLEAN
        assert tokens[0].value == "TRUE"

    def test_boolean_false(self):
        """FALSE boolean literal."""
        tokens = tokenize("FALSE")
        assert tokens[0].type == TokenType.BOOLEAN
        assert tokens[0].value == "FALSE"

    def test_boolean_case_insensitive(self):
        """Boolean literals are case-insensitive."""
        for variant in ["TRUE", "true", "True"]:
            tokens = tokenize(variant)
            assert tokens[0].type == TokenType.BOOLEAN
            assert tokens[0].value == variant.upper()


# =============================================================================
# Identifier Tests
# =============================================================================

class TestIdentifiers:
    """Tests for identifiers."""

    def test_simple_identifier(self):
        """Simple identifier."""
        tokens = tokenize("my_table")
        assert tokens[0].type == TokenType.IDENTIFIER
        assert tokens[0].value == "my_table"

    def test_identifier_with_underscore(self):
        """Identifier with underscore."""
        tokens = tokenize("_my_table_")
        assert tokens[0].type == TokenType.IDENTIFIER
        assert tokens[0].value == "_my_table_"

    def test_identifier_with_numbers(self):
        """Identifier with numbers."""
        tokens = tokenize("table123")
        assert tokens[0].type == TokenType.IDENTIFIER
        assert tokens[0].value == "table123"

    def test_identifier_starting_with_underscore(self):
        """Identifier starting with underscore."""
        tokens = tokenize("_private")
        assert tokens[0].type == TokenType.IDENTIFIER
        assert tokens[0].value == "_private"

    def test_identifier_not_a_keyword(self):
        """Identifier that looks like keyword but isn't."""
        tokens = tokenize("selection")  # Not SELECT
        assert tokens[0].type == TokenType.IDENTIFIER
        assert tokens[0].value == "selection"


# =============================================================================
# Operator Tests
# =============================================================================

class TestOperators:
    """Tests for operators."""

    def test_plus(self):
        tokens = tokenize("+")
        assert tokens[0].type == TokenType.PLUS

    def test_minus(self):
        tokens = tokenize("-")
        assert tokens[0].type == TokenType.MINUS

    def test_star(self):
        tokens = tokenize("*")
        assert tokens[0].type == TokenType.STAR

    def test_slash(self):
        tokens = tokenize("/")
        assert tokens[0].type == TokenType.SLASH

    def test_percent(self):
        tokens = tokenize("%")
        assert tokens[0].type == TokenType.PERCENT

    def test_equal(self):
        tokens = tokenize("=")
        assert tokens[0].type == TokenType.EQ

    def test_not_equal_bang(self):
        tokens = tokenize("!=")
        assert tokens[0].type == TokenType.NE
        assert tokens[0].value == "!="

    def test_not_equal_angle(self):
        tokens = tokenize("<>")
        assert tokens[0].type == TokenType.NE
        assert tokens[0].value == "<>"

    def test_less_than(self):
        tokens = tokenize("<")
        assert tokens[0].type == TokenType.LT

    def test_less_equal(self):
        tokens = tokenize("<=")
        assert tokens[0].type == TokenType.LE

    def test_greater_than(self):
        tokens = tokenize(">")
        assert tokens[0].type == TokenType.GT

    def test_greater_equal(self):
        tokens = tokenize(">=")
        assert tokens[0].type == TokenType.GE

    def test_all_operators(self):
        """All operators in sequence."""
        tokens = tokenize("+ - * / % = != <> < <= > >=")
        assert tokens[0].type == TokenType.PLUS
        assert tokens[1].type == TokenType.MINUS
        assert tokens[2].type == TokenType.STAR
        assert tokens[3].type == TokenType.SLASH
        assert tokens[4].type == TokenType.PERCENT
        assert tokens[5].type == TokenType.EQ
        assert tokens[6].type == TokenType.NE
        assert tokens[7].type == TokenType.NE
        assert tokens[8].type == TokenType.LT
        assert tokens[9].type == TokenType.LE
        assert tokens[10].type == TokenType.GT
        assert tokens[11].type == TokenType.GE


# =============================================================================
# Punctuation Tests
# =============================================================================

class TestPunctuation:
    """Tests for punctuation."""

    def test_left_paren(self):
        tokens = tokenize("(")
        assert tokens[0].type == TokenType.LPAREN

    def test_right_paren(self):
        tokens = tokenize(")")
        assert tokens[0].type == TokenType.RPAREN

    def test_comma(self):
        tokens = tokenize(",")
        assert tokens[0].type == TokenType.COMMA

    def test_dot(self):
        tokens = tokenize(".")
        assert tokens[0].type == TokenType.DOT

    def test_semicolon(self):
        tokens = tokenize(";")
        assert tokens[0].type == TokenType.SEMICOLON

    def test_all_punctuation(self):
        tokens = tokenize("( ) , . ;")
        assert tokens[0].type == TokenType.LPAREN
        assert tokens[1].type == TokenType.RPAREN
        assert tokens[2].type == TokenType.COMMA
        assert tokens[3].type == TokenType.DOT
        assert tokens[4].type == TokenType.SEMICOLON


# =============================================================================
# Comment Tests
# =============================================================================

class TestComments:
    """Tests for comments."""

    def test_line_comment(self):
        """Line comment is skipped."""
        tokens = tokenize("SELECT -- this is a comment\nFROM")
        assert len(tokens) == 3  # SELECT, FROM, EOF
        assert tokens[0].type == TokenType.SELECT
        assert tokens[1].type == TokenType.FROM

    def test_line_comment_at_end(self):
        """Line comment at end of input."""
        tokens = tokenize("SELECT -- comment")
        assert len(tokens) == 2  # SELECT, EOF
        assert tokens[0].type == TokenType.SELECT

    def test_block_comment(self):
        """Block comment is skipped."""
        tokens = tokenize("SELECT /* comment */ FROM")
        assert len(tokens) == 3  # SELECT, FROM, EOF
        assert tokens[0].type == TokenType.SELECT
        assert tokens[1].type == TokenType.FROM

    def test_multiline_block_comment(self):
        """Multiline block comment."""
        tokens = tokenize("SELECT /* line1\nline2 */ FROM")
        assert len(tokens) == 3
        assert tokens[0].type == TokenType.SELECT
        assert tokens[1].type == TokenType.FROM


# =============================================================================
# Complex Query Tests
# =============================================================================

class TestComplexQueries:
    """Tests for complex SQL queries."""

    def test_simple_select(self):
        """Simple SELECT query."""
        tokens = tokenize("SELECT * FROM users")
        assert tokens[0].type == TokenType.SELECT
        assert tokens[1].type == TokenType.STAR
        assert tokens[2].type == TokenType.FROM
        assert tokens[3].type == TokenType.IDENTIFIER

    def test_select_with_where(self):
        """SELECT with WHERE clause."""
        tokens = tokenize("SELECT * FROM users WHERE id = 1")
        assert tokens[0].type == TokenType.SELECT
        assert tokens[1].type == TokenType.STAR
        assert tokens[2].type == TokenType.FROM
        assert tokens[3].type == TokenType.IDENTIFIER
        assert tokens[4].type == TokenType.WHERE
        assert tokens[5].type == TokenType.IDENTIFIER
        assert tokens[6].type == TokenType.EQ
        assert tokens[7].type == TokenType.INTEGER

    def test_select_with_join(self):
        """SELECT with JOIN."""
        tokens = tokenize("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id")
        assert tokens[0].type == TokenType.SELECT
        assert tokens[2].type == TokenType.FROM
        assert tokens[4].type == TokenType.INNER
        assert tokens[5].type == TokenType.JOIN
        assert tokens[7].type == TokenType.ON

    def test_select_with_group_by_having(self):
        """SELECT with GROUP BY and HAVING."""
        tokens = tokenize("SELECT COUNT(*) FROM users GROUP BY status HAVING COUNT(*) > 5")
        assert tokens[0].type == TokenType.SELECT
        assert tokens[1].type == TokenType.COUNT
        # tokens: SELECT(0), COUNT(1), ((2), *(3), )(4), FROM(5), users(6), GROUP(7), BY(8), status(9), HAVING(10)
        assert tokens[7].type == TokenType.GROUP
        assert tokens[8].type == TokenType.BY
        assert tokens[10].type == TokenType.HAVING

    def test_select_with_order_by_limit(self):
        """SELECT with ORDER BY and LIMIT."""
        tokens = tokenize("SELECT * FROM users ORDER BY name ASC LIMIT 10")
        # tokens: SELECT, *, FROM, users, ORDER, BY, name, ASC, LIMIT, 10
        assert tokens[4].type == TokenType.ORDER
        assert tokens[5].type == TokenType.BY
        assert tokens[7].type == TokenType.ASC
        assert tokens[8].type == TokenType.LIMIT
        assert tokens[9].type == TokenType.INTEGER

    def test_insert_statement(self):
        """INSERT statement."""
        tokens = tokenize("INSERT INTO users (id, name) VALUES (1, 'John')")
        assert tokens[0].type == TokenType.INSERT
        assert tokens[1].type == TokenType.INTO
        assert tokens[2].type == TokenType.IDENTIFIER
        assert tokens[3].type == TokenType.LPAREN
        # tokens: INSERT, INTO, users, (, id, ,, name, ), VALUES, (, 1, ,, 'John', )
        assert tokens[8].type == TokenType.VALUES

    def test_update_statement(self):
        """UPDATE statement."""
        tokens = tokenize("UPDATE users SET name = 'Jane' WHERE id = 1")
        assert tokens[0].type == TokenType.UPDATE
        assert tokens[1].type == TokenType.IDENTIFIER
        assert tokens[2].type == TokenType.SET
        # tokens: UPDATE, users, SET, name, =, 'Jane', WHERE, id, =, 1
        assert tokens[6].type == TokenType.WHERE

    def test_delete_statement(self):
        """DELETE statement."""
        tokens = tokenize("DELETE FROM users WHERE id = 1")
        assert tokens[0].type == TokenType.DELETE
        assert tokens[1].type == TokenType.FROM
        assert tokens[3].type == TokenType.WHERE

    def test_create_table(self):
        """CREATE TABLE statement."""
        tokens = tokenize("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)")
        assert tokens[0].type == TokenType.CREATE
        assert tokens[1].type == TokenType.TABLE
        assert tokens[2].type == TokenType.IDENTIFIER
        assert tokens[4].type == TokenType.IDENTIFIER
        assert tokens[5].type == TokenType.INT
        assert tokens[6].type == TokenType.PRIMARY
        assert tokens[7].type == TokenType.KEY

    def test_create_index(self):
        """CREATE INDEX statement."""
        tokens = tokenize("CREATE INDEX idx_name ON users (name)")
        assert tokens[0].type == TokenType.CREATE
        assert tokens[1].type == TokenType.INDEX
        assert tokens[2].type == TokenType.IDENTIFIER
        assert tokens[3].type == TokenType.ON

    def test_begin_transaction(self):
        """BEGIN TRANSACTION statement."""
        tokens = tokenize("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")
        assert tokens[0].type == TokenType.BEGIN
        assert tokens[1].type == TokenType.TRANSACTION
        assert tokens[2].type == TokenType.ISOLATION
        assert tokens[3].type == TokenType.LEVEL
        assert tokens[4].type == TokenType.READ
        assert tokens[5].type == TokenType.COMMITTED


# =============================================================================
# Position Tracking Tests
# =============================================================================

class TestPositionTracking:
    """Tests for token position tracking."""

    def test_line_tracking(self):
        """Line numbers are tracked correctly."""
        tokens = tokenize("SELECT\nFROM")
        assert tokens[0].line == 1
        assert tokens[1].line == 2

    def test_column_tracking(self):
        """Column numbers are tracked correctly."""
        tokens = tokenize("SELECT FROM")
        assert tokens[0].column == 1
        assert tokens[1].column == 8

    def test_multiline_position(self):
        """Position tracking across multiple lines."""
        tokens = tokenize("SELECT\nFROM\nWHERE")
        assert tokens[0].line == 1
        assert tokens[0].column == 1
        assert tokens[1].line == 2
        assert tokens[1].column == 1
        assert tokens[2].line == 3
        assert tokens[2].column == 1


# =============================================================================
# Edge Cases and Adversarial Tests
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases and adversarial inputs."""

    def test_long_identifier(self):
        """Very long identifier."""
        long_id = "a" * 1000
        tokens = tokenize(long_id)
        assert tokens[0].type == TokenType.IDENTIFIER
        assert tokens[0].value == long_id

    def test_long_string(self):
        """Very long string."""
        long_str = "x" * 1000
        tokens = tokenize(f"'{long_str}'")
        assert tokens[0].type == TokenType.STRING
        assert tokens[0].value == long_str

    def test_unicode_in_string(self):
        """Unicode characters in string."""
        tokens = tokenize("'привет мир'")
        assert tokens[0].type == TokenType.STRING
        assert tokens[0].value == "привет мир"

    def test_unicode_identifier(self):
        """Unicode in identifier (if supported)."""
        # Most SQL dialects don't support Unicode identifiers
        # but lexer should handle gracefully
        tokens = tokenize("table_日本語")
        # Should be treated as identifier
        assert tokens[0].type == TokenType.IDENTIFIER

    def test_special_characters_in_string(self):
        """Special characters in string."""
        tokens = tokenize("'!@#$%^&*()'")
        assert tokens[0].type == TokenType.STRING
        assert tokens[0].value == "!@#$%^&*()"

    def test_newline_in_string(self):
        """Newline in string literal."""
        tokens = tokenize("'line1\nline2'")
        assert tokens[0].type == TokenType.STRING
        assert "\n" in tokens[0].value

    def test_consecutive_operators(self):
        """Consecutive operators."""
        tokens = tokenize("+-*/%")
        assert tokens[0].type == TokenType.PLUS
        assert tokens[1].type == TokenType.MINUS
        assert tokens[2].type == TokenType.STAR
        assert tokens[3].type == TokenType.SLASH
        assert tokens[4].type == TokenType.PERCENT

    def test_mixed_content(self):
        """Mixed content query."""
        tokens = tokenize("SELECT id, name FROM users WHERE id = 1 AND name LIKE '%test%'")
        assert len(tokens) > 10
        assert tokens[0].type == TokenType.SELECT

    def test_numbers_in_identifier(self):
        """Numbers in identifier."""
        tokens = tokenize("table123abc")
        assert tokens[0].type == TokenType.IDENTIFIER
        assert tokens[0].value == "table123abc"

    def test_underscore_in_identifier(self):
        """Underscores in identifier."""
        tokens = tokenize("__private_table__")
        assert tokens[0].type == TokenType.IDENTIFIER
        assert tokens[0].value == "__private_table__"

    def test_multiple_strings(self):
        """Multiple string literals."""
        tokens = tokenize("'a' 'b' 'c'")
        assert len(tokens) == 4  # 3 strings + EOF
        assert all(t.type == TokenType.STRING for t in tokens[:3])

    def test_multiple_numbers(self):
        """Multiple number literals."""
        tokens = tokenize("1 2.5 -3 4e10")
        assert tokens[0].type == TokenType.INTEGER
        assert tokens[1].type == TokenType.REAL
        assert tokens[2].type == TokenType.INTEGER
        assert tokens[3].type == TokenType.REAL

    def test_unknown_character(self):
        """Unknown character handling."""
        tokens = tokenize("@")
        assert tokens[0].type == TokenType.UNKNOWN

    def test_bang_alone(self):
        """Bang character alone."""
        tokens = tokenize("!")
        assert tokens[0].type == TokenType.UNKNOWN


# =============================================================================
# EOF Tests
# =============================================================================

class TestEOF:
    """Tests for EOF token."""

    def test_eof_at_end(self):
        """EOF token is always at end."""
        tokens = tokenize("SELECT")
        assert tokens[-1].type == TokenType.EOF
        assert tokens[-1].value == ""

    def test_eof_position(self):
        """EOF position is at end of input."""
        tokens = tokenize("SELECT")
        eof = tokens[-1]
        assert eof.type == TokenType.EOF
        # Position should be after SELECT (line 1, column 7)
        assert eof.line == 1
        assert eof.column == 7


# =============================================================================
# Lexer Class Tests
# =============================================================================

class TestLexerClass:
    """Tests for Lexer class methods."""

    def test_lexer_init(self):
        """Lexer initialization."""
        lexer = Lexer("SELECT")
        assert lexer.text == "SELECT"
        assert lexer.pos == 0
        assert lexer.line == 1
        assert lexer.column == 1

    def test_lexer_tokenize_method(self):
        """Lexer.tokenize() method."""
        lexer = Lexer("SELECT FROM")
        tokens = lexer.tokenize()
        assert len(tokens) == 3
        assert tokens[0].type == TokenType.SELECT
        assert tokens[1].type == TokenType.FROM

    def test_tokenize_function(self):
        """tokenize() utility function."""
        tokens = tokenize("SELECT")
        assert len(tokens) == 2
        assert tokens[0].type == TokenType.SELECT


# =============================================================================
# Run Tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])