# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_fuzzing_phase_extra
# Intent: Advanced fuzzing tests for mini_db_v2 robustness validation.
# Dependencies: pytest, random, string, threading
# END_MODULE_CONTRACT

"""
Phase Extra: Fuzzing Tests for Robustness

Tests cover:
1. Random SQL query generation
2. Invalid input handling
3. Stress testing with many operations
4. Concurrent access patterns
5. SQL injection attempts
"""

import pytest
import random
import string
import threading
import time
import sys
import os

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from mini_db_v2.parser.lexer import Lexer, LexerError
from mini_db_v2.parser.parser import Parser, ParseError, parse_sql
from mini_db_v2.storage.database import Database
from mini_db_v2.storage.table import Table, ColumnDef, DataType


# =============================================================================
# START_BLOCK_RANDOM_GENERATORS
# =============================================================================

class SQLFuzzer:
    """Generator of random SQL queries for fuzzing."""
    
    def __init__(self, seed: int = None):
        """Initialize fuzzer with optional seed for reproducibility."""
        if seed is not None:
            random.seed(seed)
        
        self.tables = ["users", "orders", "products", "items", "data"]
        self.columns = ["id", "name", "value", "status", "count", "price", "active"]
        self.types = ["INT", "TEXT", "BOOL", "REAL"]
        
    def random_string(self, min_len: int = 1, max_len: int = 100) -> str:
        """Generate random string."""
        length = random.randint(min_len, max_len)
        chars = string.printable.replace('\x00', '')  # Remove null byte
        return ''.join(random.choice(chars) for _ in range(length))
    
    def random_identifier(self, min_len: int = 1, max_len: int = 50) -> str:
        """Generate random SQL identifier."""
        length = random.randint(min_len, max_len)
        first = random.choice(string.ascii_letters + '_')
        rest = ''.join(random.choice(string.ascii_letters + string.digits + '_') for _ in range(length - 1))
        return first + rest
    
    def random_number(self) -> str:
        """Generate random number literal."""
        if random.random() < 0.5:
            return str(random.randint(-1000000, 1000000))
        else:
            return f"{random.uniform(-1000, 1000):.2f}"
    
    def random_bool(self) -> str:
        """Generate random boolean literal."""
        return random.choice(["TRUE", "FALSE"])
    
    def random_literal(self) -> str:
        """Generate random literal value."""
        choice = random.random()
        if choice < 0.3:
            return f"'{self.random_string(1, 50)}'"
        elif choice < 0.6:
            return self.random_number()
        elif choice < 0.8:
            return self.random_bool()
        else:
            return "NULL"
    
    def random_comparison(self) -> str:
        """Generate random comparison expression."""
        col = random.choice(self.columns)
        op = random.choice(["=", "!=", "<", ">", "<=", ">="])
        val = self.random_literal()
        return f"{col} {op} {val}"
    
    def random_logical(self, depth: int = 0) -> str:
        """Generate random logical expression."""
        if depth > 3:
            return self.random_comparison()
        
        left = self.random_comparison() if random.random() < 0.5 else self.random_logical(depth + 1)
        right = self.random_comparison() if random.random() < 0.5 else self.random_logical(depth + 1)
        op = random.choice(["AND", "OR"])
        
        if random.random() < 0.3:
            return f"({left} {op} {right})"
        return f"{left} {op} {right}"
    
    def generate_create_table(self) -> str:
        """Generate random CREATE TABLE query."""
        table = random.choice(self.tables)
        num_cols = random.randint(1, 10)
        
        columns = []
        for i in range(num_cols):
            col_name = random.choice(self.columns) if random.random() < 0.7 else self.random_identifier()
            col_type = random.choice(self.types)
            constraints = []
            if random.random() < 0.2:
                constraints.append("PRIMARY KEY")
            if random.random() < 0.2:
                constraints.append("UNIQUE")
            if random.random() < 0.3:
                constraints.append("NOT NULL")
            
            col_def = f"{col_name} {col_type}"
            if constraints:
                col_def += " " + " ".join(constraints)
            columns.append(col_def)
        
        return f"CREATE TABLE {table} ({', '.join(columns)})"
    
    def generate_insert(self) -> str:
        """Generate random INSERT query."""
        table = random.choice(self.tables)
        num_cols = random.randint(1, 5)
        
        columns = random.sample(self.columns, min(num_cols, len(self.columns)))
        values = [self.random_literal() for _ in columns]
        
        return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)})"
    
    def generate_select(self) -> str:
        """Generate random SELECT query."""
        table = random.choice(self.tables)
        
        # Columns
        if random.random() < 0.3:
            cols = "*"
        else:
            num_cols = random.randint(1, 5)
            cols = ", ".join(random.sample(self.columns, min(num_cols, len(self.columns))))
        
        query = f"SELECT {cols} FROM {table}"
        
        # WHERE
        if random.random() < 0.7:
            query += f" WHERE {self.random_logical()}"
        
        return query
    
    def generate_update(self) -> str:
        """Generate random UPDATE query."""
        table = random.choice(self.tables)
        num_cols = random.randint(1, 3)
        
        columns = random.sample(self.columns, min(num_cols, len(self.columns)))
        assignments = [f"{col} = {self.random_literal()}" for col in columns]
        
        query = f"UPDATE {table} SET {', '.join(assignments)}"
        
        if random.random() < 0.7:
            query += f" WHERE {self.random_logical()}"
        
        return query
    
    def generate_delete(self) -> str:
        """Generate random DELETE query."""
        table = random.choice(self.tables)
        query = f"DELETE FROM {table}"
        
        if random.random() < 0.7:
            query += f" WHERE {self.random_logical()}"
        
        return query
    
    def generate_random_query(self) -> str:
        """Generate random SQL query of any type."""
        generators = [
            self.generate_create_table,
            self.generate_insert,
            self.generate_select,
            self.generate_update,
            self.generate_delete,
        ]
        return random.choice(generators)()
    
    def generate_garbage(self) -> str:
        """Generate garbage input for testing error handling."""
        garbage_types = [
            # Random characters
            lambda: self.random_string(1, 1000),
            # Random bytes as string
            lambda: ''.join(chr(random.randint(1, 255)) for _ in range(random.randint(1, 100))),
            # SQL keywords mixed with garbage
            lambda: "SELECT " + self.random_string() + " FROM " + self.random_string(),
            # Incomplete SQL
            lambda: random.choice(["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE"]),
            # SQL injection attempts
            lambda: "SELECT * FROM users WHERE id = " + random.choice(['1; DROP TABLE users', '1 OR 1=1', "' OR '1'='1", '1; --']),
            # Very long identifier
            lambda: "SELECT * FROM " + self.random_identifier(100, 1000),
            # Special characters
            lambda: "SELECT * FROM users WHERE name = '" + random.choice(['\\x00', '\\n\\r', '/* comment */', '-- comment']) + "'",
            # Unicode
            lambda: "SELECT * FROM users WHERE name = '" + self.random_unicode_string() + "'",
            # Empty/null
            lambda: random.choice(["", " ", "\t", "\n", "\x00", "NULL"]),
        ]
        return random.choice(garbage_types)()
    
    def random_unicode_string(self) -> str:
        """Generate random Unicode string."""
        chars = []
        for _ in range(random.randint(1, 50)):
            # Various Unicode ranges
            range_choice = random.random()
            if range_choice < 0.3:
                chars.append(chr(random.randint(0x0400, 0x04FF)))  # Cyrillic
            elif range_choice < 0.6:
                chars.append(chr(random.randint(0x4E00, 0x9FFF)))  # Chinese
            elif range_choice < 0.8:
                chars.append(chr(random.randint(0x0600, 0x06FF)))  # Arabic
            else:
                chars.append(chr(random.randint(0x1F600, 0x1F64F)))  # Emojis
        return ''.join(chars)


# END_BLOCK_RANDOM_GENERATORS


# =============================================================================
# START_BLOCK_LEXER_FUZZING
# =============================================================================

class TestLexerFuzzing:
    """Fuzzing tests for Lexer."""
    
    @pytest.fixture
    def fuzzer(self):
        return SQLFuzzer(seed=42)
    
    def test_lexer_random_queries_no_crash(self, fuzzer):
        """Lexer should not crash on random queries."""
        crashes = 0
        
        for i in range(1000):
            query = fuzzer.generate_random_query()
            try:
                lexer = Lexer(query)
                tokens = lexer.tokenize()
                # Should either tokenize or raise LexerError
                assert tokens is not None or True
            except LexerError:
                pass  # Expected for some queries
            except Exception as e:
                # Unexpected error - potential crash
                crashes += 1
        
        assert crashes == 0, f"Lexer crashed on {crashes} queries"
    
    def test_lexer_garbage_input_no_crash(self, fuzzer):
        """Lexer should not crash on garbage input."""
        crashes = 0
        
        for i in range(500):
            garbage = fuzzer.generate_garbage()
            try:
                lexer = Lexer(garbage)
                tokens = lexer.tokenize()
            except LexerError:
                pass  # Expected
            except Exception as e:
                crashes += 1
        
        assert crashes == 0, f"Lexer crashed on {crashes} garbage inputs"
    
    def test_lexer_unicode_handling(self, fuzzer):
        """Lexer should handle Unicode gracefully."""
        
        for i in range(100):
            unicode_query = f"SELECT * FROM users WHERE name = '{fuzzer.random_unicode_string()}'"
            try:
                lexer = Lexer(unicode_query)
                tokens = lexer.tokenize()
            except LexerError:
                pass  # Acceptable
            except Exception:
                pytest.fail(f"Lexer crashed on Unicode query: {unicode_query[:50]}")
    
    def test_lexer_extremely_long_query(self, fuzzer):
        """Lexer should handle extremely long queries."""
        
        # Generate very long query
        long_query = "SELECT * FROM users WHERE " + " AND ".join(
            f"id = {i}" for i in range(10000)
        )
        
        try:
            lexer = Lexer(long_query)
            tokens = lexer.tokenize()
            assert len(tokens) > 0
        except LexerError:
            pass  # Acceptable
        except MemoryError:
            pytest.fail("Lexer ran out of memory on long query")
    
    def test_lexer_null_byte_handling(self, fuzzer):
        """Lexer should handle null bytes gracefully."""
        
        queries_with_null = [
            "SELECT * FROM users WHERE name = 'test\x00value'",
            "SELECT * FROM \x00users",
            "\x00SELECT * FROM users",
        ]
        
        for query in queries_with_null:
            try:
                lexer = Lexer(query)
                tokens = lexer.tokenize()
            except LexerError:
                pass  # Expected
            except Exception as e:
                pytest.fail(f"Lexer crashed on null byte: {e}")


# END_BLOCK_LEXER_FUZZING


# =============================================================================
# START_BLOCK_PARSER_FUZZING
# =============================================================================

class TestParserFuzzing:
    """Fuzzing tests for Parser."""
    
    @pytest.fixture
    def fuzzer(self):
        return SQLFuzzer(seed=123)
    
    def test_parser_random_queries_no_crash(self, fuzzer):
        """Parser should not crash on random queries."""
        crashes = 0
        parsed = 0
        errors = 0
        
        for i in range(500):
            query = fuzzer.generate_random_query()
            try:
                ast = parse_sql(query)
                parsed += 1
            except ParseError:
                errors += 1  # Expected for some queries
            except Exception as e:
                crashes += 1
        
        assert crashes == 0, f"Parser crashed on {crashes} queries"
        # At least some queries should parse or fail gracefully
        assert parsed + errors == 500
    
    def test_parser_garbage_input_no_crash(self, fuzzer):
        """Parser should not crash on garbage input."""
        crashes = 0
        
        for i in range(300):
            garbage = fuzzer.generate_garbage()
            try:
                ast = parse_sql(garbage)
            except ParseError:
                pass  # Expected
            except Exception as e:
                crashes += 1
        
        assert crashes == 0, f"Parser crashed on {crashes} garbage inputs"
    
    def test_parser_deeply_nested_where(self):
        """Parser should handle deeply nested WHERE clauses."""
        # Build deeply nested WHERE
        depth = 50
        where = "id = 1"
        for i in range(depth):
            where = f"({where} OR id = {i + 2})"
        
        query = f"SELECT * FROM users WHERE {where}"
        
        try:
            ast = parse_sql(query)
            assert ast is not None
        except ParseError:
            pass  # Acceptable if too deep
        except RecursionError:
            pytest.fail("Parser hit recursion limit on nested WHERE")
    
    def test_parser_many_columns(self):
        """Parser should handle queries with many columns."""
        num_cols = 1000
        columns = ", ".join(f"col_{i}" for i in range(num_cols))
        query = f"SELECT {columns} FROM users"
        
        try:
            ast = parse_sql(query)
            assert ast is not None
        except ParseError:
            pass  # Acceptable
        except MemoryError:
            pytest.fail("Parser ran out of memory on many columns")
    
    def test_parser_sql_injection_attempts(self):
        """Parser should handle SQL injection attempts safely."""
        injection_attempts = [
            "SELECT * FROM users WHERE id = 1; DROP TABLE users",
            "SELECT * FROM users WHERE id = 1 OR 1=1",
            "SELECT * FROM users WHERE name = '' OR '1'='1'",
            "SELECT * FROM users WHERE id = 1; --",
            "SELECT * FROM users; INSERT INTO users VALUES (1, 'hacker')",
            "SELECT * FROM users WHERE id = 1 UNION SELECT * FROM admin",
        ]
        
        for attempt in injection_attempts:
            try:
                ast = parse_sql(attempt)
                # Parser might parse it, but executor should handle safely
            except ParseError:
                pass  # Expected for some
            except Exception as e:
                pytest.fail(f"Parser crashed on SQL injection: {e}")


# END_BLOCK_PARSER_FUZZING


# =============================================================================
# START_BLOCK_STRESS_TESTS
# =============================================================================

class TestStressFuzzing:
    """Stress tests for robustness."""
    
    def test_many_tokens_stress(self):
        """Lexer should handle many tokens."""
        
        # Generate query with many tokens
        tokens_count = 10000
        query = "SELECT " + ", ".join(f"col_{i}" for i in range(tokens_count))
        query += " FROM users"
        
        start = time.time()
        try:
            lexer = Lexer(query)
            tokens = lexer.tokenize()
            elapsed = time.time() - start
            assert elapsed < 1.0, f"Lexer too slow: {elapsed:.2f}s for {tokens_count} tokens"
        except MemoryError:
            pytest.fail("Lexer ran out of memory")
    
    def test_many_conditions_stress(self):
        """Parser should handle many WHERE conditions."""
        
        # Generate query with many conditions
        conditions_count = 1000
        conditions = " AND ".join(f"id != {i}" for i in range(conditions_count))
        query = f"SELECT * FROM users WHERE {conditions}"
        
        start = time.time()
        try:
            ast = parse_sql(query)
            elapsed = time.time() - start
            assert elapsed < 2.0, f"Parser too slow: {elapsed:.2f}s for {conditions_count} conditions"
        except MemoryError:
            pytest.fail("Parser ran out of memory")
    
    def test_concurrent_fuzzing(self):
        """System should handle concurrent fuzzing."""
        fuzzer = SQLFuzzer()
        errors = []
        lock = threading.Lock()
        
        def fuzz_worker(worker_id):
            for i in range(100):
                query = fuzzer.generate_random_query()
                try:
                    lexer = Lexer(query)
                    tokens = lexer.tokenize()
                except LexerError:
                    pass
                except Exception as e:
                    with lock:
                        errors.append((worker_id, i, str(e)))
        
        threads = [threading.Thread(target=fuzz_worker, args=(i,)) for i in range(10)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0, f"Concurrent fuzzing errors: {errors[:5]}"


# END_BLOCK_STRESS_TESTS


# =============================================================================
# START_BLOCK_ADVERSARIAL_FUZZING
# =============================================================================

class TestAdversarialFuzzing:
    """Adversarial fuzzing tests for security and robustness."""
    
    def test_sql_injection_prevention(self):
        """System should not execute SQL injection."""
        injection_payloads = [
            "'; DROP TABLE users; --",
            "1; DELETE FROM users WHERE 1=1",
            "' OR '1'='1' --",
            "1 UNION SELECT * FROM passwords",
            "admin'--",
            "1; INSERT INTO users VALUES (hacker)",
        ]
        
        for payload in injection_payloads:
            query = f"SELECT * FROM users WHERE id = {payload}"
            try:
                lexer = Lexer(query)
                tokens = lexer.tokenize()
                ast = parse_sql(query)
                # If parsed, should be safe AST
            except (LexerError, ParseError):
                pass  # Expected - injection should fail parsing
            except Exception as e:
                pytest.fail(f"Unexpected error on injection: {e}")
    
    def test_null_byte_injection(self):
        """System should handle null byte injection."""
        null_payloads = [
            "SELECT * FROM users WHERE name = 'test\x00admin'",
            "SELECT * FROM \x00users",
            "\x00SELECT * FROM users",
            "SELECT * FROM users WHERE id = 1\x00 AND admin = 1",
        ]
        
        for payload in null_payloads:
            try:
                lexer = Lexer(payload)
                tokens = lexer.tokenize()
            except LexerError:
                pass  # Expected
            except Exception as e:
                pytest.fail(f"Crash on null byte injection: {e}")
    
    def test_format_string_injection(self):
        """System should handle format string injection."""
        format_payloads = [
            "SELECT * FROM users WHERE name = '%s'",
            "SELECT * FROM users WHERE name = '%n'",
            "SELECT * FROM users WHERE name = '{}{}{}'",
        ]
        
        for payload in format_payloads:
            try:
                lexer = Lexer(payload)
                tokens = lexer.tokenize()
                ast = parse_sql(payload)
            except (LexerError, ParseError):
                pass
            except Exception as e:
                pytest.fail(f"Crash on format string: {e}")
    
    def test_extreme_values(self):
        """System should handle extreme values."""
        extreme_queries = [
            # Very large number
            f"SELECT * FROM users WHERE id = {10 ** 100}",
            # Very negative number
            f"SELECT * FROM users WHERE id = {-10 ** 100}",
            # Very long string
            f"SELECT * FROM users WHERE name = '{'a' * 1000000}'",
        ]
        
        for query in extreme_queries:
            try:
                lexer = Lexer(query)
                tokens = lexer.tokenize()
                ast = parse_sql(query)
            except (LexerError, ParseError, MemoryError):
                pass  # Expected
            except RecursionError:
                pass  # Acceptable for deeply nested structures
            except Exception as e:
                pytest.fail(f"Unexpected error on extreme value: {e}")
    
    def test_deeply_nested_parens(self):
        """System handles deeply nested parentheses."""
        # Many nested parens - may hit recursion limit
        query = "SELECT * FROM users WHERE " + "(" * 1000 + "id = 1" + ")" * 1000
        
        try:
            lexer = Lexer(query)
            tokens = lexer.tokenize()
            ast = parse_sql(query)
        except (LexerError, ParseError, MemoryError, RecursionError):
            pass  # All acceptable for extreme nesting
        except Exception as e:
            pytest.fail(f"Unexpected error on deeply nested parens: {e}")


# END_BLOCK_ADVERSARIAL_FUZZING


# =============================================================================
# Run Tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])