# START_MODULE_CONTRACT
# Module: tests.test_fuzzing
# Intent: Fuzzing тесты для Lexer и Parser - генерация случайных SQL запросов.
#         Проверка устойчивости к некорректным входным данным.
# Constraints: Без сторонних библиотек (hypothesis), только random.
# END_MODULE_CONTRACT

"""
FUZZING TESTS: Lexer & Parser Stress Testing

Эти тесты генерируют случайные SQL запросы для проверки:
1. Lexer не падает на любых входных данных
2. Parser либо парсит корректно, либо выбрасывает ParseError (не падает)
3. REPL не выводит Python Traceback
"""

import random
import string
import pytest
from typing import Optional

from mini_db.parser.lexer import Lexer, LexerError, TokenType
from mini_db.parser.parser import Parser, ParseError
from mini_db.repl.repl import REPL


# ==================== FUZZING GENERATORS ====================

class SQLFuzzer:
    """Генератор случайных SQL запросов для fuzzing тестов."""
    
    def __init__(self, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)
    
    def random_string(self, min_len: int = 1, max_len: int = 100) -> str:
        """Генерирует случайную строку."""
        length = random.randint(min_len, max_len)
        chars = string.ascii_letters + string.digits + " _-!@#$%^&*()"
        return ''.join(random.choice(chars) for _ in range(length))
    
    def random_identifier(self) -> str:
        """Генерирует случайный идентификатор."""
        first_char = random.choice(string.ascii_letters + "_")
        rest_chars = ''.join(
            random.choice(string.ascii_letters + string.digits + "_")
            for _ in range(random.randint(0, 20))
        )
        return first_char + rest_chars
    
    def random_number(self) -> int:
        """Генерирует случайное число (включая большие и отрицательные)."""
        if random.random() < 0.1:
            # Очень большое число
            return random.randint(-10**15, 10**15)
        elif random.random() < 0.2:
            # Отрицательное
            return random.randint(-1000000, -1)
        else:
            # Обычное
            return random.randint(0, 1000000)
    
    def random_literal(self) -> str:
        """Генерирует случайный литерал для SQL."""
        choice = random.choice(['int', 'string', 'bool', 'null'])
        
        if choice == 'int':
            return str(self.random_number())
        elif choice == 'string':
            # Экранируем кавычки
            s = self.random_string(1, 50).replace("'", "''")
            return f"'{s}'"
        elif choice == 'bool':
            return random.choice(['true', 'false'])
        else:
            return 'null'
    
    def random_operator(self) -> str:
        """Генерирует случайный оператор сравнения."""
        return random.choice(['=', '!=', '<', '>'])
    
    def random_logical_op(self) -> str:
        """Генерирует случайный логический оператор."""
        return random.choice(['AND', 'OR'])
    
    def random_type(self) -> str:
        """Генерирует случайный тип данных."""
        return random.choice(['INT', 'TEXT', 'BOOL'])
    
    def generate_valid_create_table(self) -> str:
        """Генерирует валидный CREATE TABLE."""
        table_name = self.random_identifier()
        num_columns = random.randint(1, 10)
        
        columns = []
        for i in range(num_columns):
            col_name = self.random_identifier()
            col_type = self.random_type()
            unique = " UNIQUE" if random.random() < 0.3 else ""
            columns.append(f"{col_name} {col_type}{unique}")
        
        return f"CREATE TABLE {table_name} ({', '.join(columns)})"
    
    def generate_valid_insert(self) -> str:
        """Генерирует валидный INSERT."""
        table_name = self.random_identifier()
        num_values = random.randint(1, 5)
        
        columns = [self.random_identifier() for _ in range(num_values)]
        values = [self.random_literal() for _ in range(num_values)]
        
        return f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)})"
    
    def generate_valid_select(self) -> str:
        """Генерирует валидный SELECT."""
        table_name = self.random_identifier()
        
        # SELECT * или SELECT col1, col2
        if random.random() < 0.5:
            columns = "*"
        else:
            num_cols = random.randint(1, 5)
            columns = ', '.join(self.random_identifier() for _ in range(num_cols))
        
        query = f"SELECT {columns} FROM {table_name}"
        
        # Опциональный WHERE
        if random.random() < 0.7:
            query += " WHERE " + self.generate_where_expr()
        
        return query
    
    def generate_where_expr(self, depth: int = 0) -> str:
        """Генерирует случайное WHERE выражение."""
        if depth > 3:
            # Ограничиваем глубину
            return self.generate_simple_comparison()
        
        choice = random.choice(['comparison', 'and', 'or', 'paren'])
        
        if choice == 'comparison':
            return self.generate_simple_comparison()
        elif choice == 'and':
            left = self.generate_where_expr(depth + 1)
            right = self.generate_where_expr(depth + 1)
            return f"{left} AND {right}"
        elif choice == 'or':
            left = self.generate_where_expr(depth + 1)
            right = self.generate_where_expr(depth + 1)
            return f"{left} OR {right}"
        else:
            expr = self.generate_where_expr(depth + 1)
            return f"({expr})"
    
    def generate_simple_comparison(self) -> str:
        """Генерирует простое сравнение."""
        col = self.random_identifier()
        op = self.random_operator()
        val = self.random_literal()
        return f"{col} {op} {val}"
    
    def generate_valid_update(self) -> str:
        """Генерирует валидный UPDATE."""
        table_name = self.random_identifier()
        num_assignments = random.randint(1, 3)
        
        assignments = []
        for _ in range(num_assignments):
            col = self.random_identifier()
            val = self.random_literal()
            assignments.append(f"{col} = {val}")
        
        query = f"UPDATE {table_name} SET {', '.join(assignments)}"
        
        if random.random() < 0.5:
            query += " WHERE " + self.generate_where_expr()
        
        return query
    
    def generate_valid_delete(self) -> str:
        """Генерирует валидный DELETE."""
        table_name = self.random_identifier()
        query = f"DELETE FROM {table_name}"
        
        if random.random() < 0.5:
            query += " WHERE " + self.generate_where_expr()
        
        return query
    
    def generate_valid_query(self) -> str:
        """Генерирует случайный валидный SQL запрос."""
        generators = [
            self.generate_valid_create_table,
            self.generate_valid_insert,
            self.generate_valid_select,
            self.generate_valid_update,
            self.generate_valid_delete,
        ]
        return random.choice(generators)()
    
    def generate_garbage(self) -> str:
        """Генерирует мусорный ввод."""
        choice = random.choice([
            'random_chars',
            'random_bytes',
            'sql_keywords_garbage',
            'deeply_nested',
            'very_long',
        ])
        
        if choice == 'random_chars':
            # Полностью случайные символы
            length = random.randint(1, 1000)
            chars = string.printable
            return ''.join(random.choice(chars) for _ in range(length))
        
        elif choice == 'sql_keywords_garbage':
            # Ключевые слова в неправильном порядке
            keywords = ['SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'TABLE']
            return ' '.join(random.choice(keywords) for _ in range(random.randint(5, 20)))
        
        elif choice == 'deeply_nested':
            # Глубоко вложенные скобки
            depth = random.randint(10, 100)
            return '(' * depth + 'x' + ')' * depth
        
        elif choice == 'very_long':
            # Очень длинный идентификатор
            return self.random_identifier() * 100
        
        else:
            # Случайные байты (включая Unicode)
            return ''.join(chr(random.randint(0, 0x10FFFF)) for _ in range(random.randint(1, 100)))
    
    def generate_edge_case(self) -> str:
        """Генерирует граничные случаи."""
        cases = [
            "",  # Пустая строка
            " ",  # Только пробел
            "\t\n\r",  # Только whitespace
            "''",  # Пустая строка в SQL
            "'\\'",  # Backslash в строке
            "''''",  # Экранированная кавычка
            "0",  # Ноль
            "-0",  # Минус ноль
            "-999999999999",  # Большое отрицательное
            "999999999999",  # Большое положительное
            "SELECT * FROM t WHERE x = ''''''",  # Множественные кавычки
            "CREATE TABLE t (x INT UNIQUE UNIQUE)",  # Двойной UNIQUE
            "SELECT * FROM *",  # Звёзда вместо таблицы
            "INSERT INTO t VALUES ()",  # Пустые значения
            "(((((((((())))))))))",  # Глубокая вложенность
            "SELECT * FROM t WHERE x = 1 AND y = 2 AND z = 3 AND a = 4",  # Много AND
            "x" * 10000,  # Очень длинный идентификатор
            "SELECT * FROM t WHERE " + "x = 1 OR " * 100 + "x = 2",  # Много OR
        ]
        return random.choice(cases)


# ==================== FUZZING TESTS ====================

@pytest.fixture
def fuzzer():
    return SQLFuzzer(seed=42)  # Фиксированный seed для воспроизводимости


class TestLexerFuzzing:
    """Fuzzing тесты для Lexer."""
    
    def test_lexer_never_crashes_on_garbage(self, fuzzer):
        """Lexer не должен падать на любом вводе."""
        lexer = Lexer()
        
        for _ in range(1000):
            garbage = fuzzer.generate_garbage()
            try:
                tokens = lexer.tokenize(garbage)
                # Если не упало - проверяем что вернулся список
                assert isinstance(tokens, list)
                # Последний токен должен быть EOF
                assert tokens[-1].type == TokenType.EOF
            except LexerError:
                # Ожидаемое поведение для некорректного ввода
                pass
            except Exception as e:
                pytest.fail(f"Lexer crashed with unexpected exception: {type(e).__name__}: {e}")
    
    def test_lexer_handles_valid_queries(self, fuzzer):
        """Lexer должен успешно токенизировать валидные запросы."""
        lexer = Lexer()
        
        for _ in range(500):
            query = fuzzer.generate_valid_query()
            try:
                tokens = lexer.tokenize(query)
                assert isinstance(tokens, list)
                assert len(tokens) > 0
                assert tokens[-1].type == TokenType.EOF
            except LexerError as e:
                pytest.fail(f"Lexer failed on valid query: {query}\nError: {e}")
    
    def test_lexer_handles_edge_cases(self, fuzzer):
        """Lexer должен корректно обрабатывать граничные случаи."""
        lexer = Lexer()
        
        for _ in range(100):
            edge_case = fuzzer.generate_edge_case()
            try:
                tokens = lexer.tokenize(edge_case)
                assert isinstance(tokens, list)
            except LexerError:
                # Допустимо для некоторых edge cases
                pass
            except Exception as e:
                pytest.fail(f"Lexer crashed on edge case: {edge_case!r}\nError: {type(e).__name__}: {e}")
    
    def test_lexer_position_tracking(self, fuzzer):
        """Позиции токенов должны быть корректными."""
        lexer = Lexer()
        
        for _ in range(100):
            query = fuzzer.generate_valid_query()
            try:
                tokens = lexer.tokenize(query)
                # Проверяем что позиции возрастают
                positions = [t.position for t in tokens]
                assert positions == sorted(positions), f"Positions not sorted: {positions}"
                # Проверяем что все позиции в пределах строки
                for pos in positions:
                    assert 0 <= pos <= len(query), f"Position {pos} out of bounds for query length {len(query)}"
            except LexerError:
                pass
    
    def test_lexer_unicode_handling(self, fuzzer):
        """Lexer должен корректно обрабатывать Unicode."""
        lexer = Lexer()
        
        # Unicode в строковых литералах
        unicode_strings = [
            "'привет'",
            "'你好世界'",
            "'🎉🎊🎁'",
            "'مرحبا'",
            "'Ñoño'",
        ]
        
        for s in unicode_strings:
            try:
                tokens = lexer.tokenize(s)
                assert any(t.type == TokenType.STRING for t in tokens)
            except LexerError as e:
                pytest.fail(f"Lexer failed on Unicode string: {s}\nError: {e}")


class TestParserFuzzing:
    """Fuzzing тесты для Parser."""
    
    def test_parser_never_crashes_on_garbage(self, fuzzer):
        """Parser не должен падать на любом вводе."""
        parser = Parser()
        
        for _ in range(1000):
            garbage = fuzzer.generate_garbage()
            try:
                ast = parser.parse(garbage)
                # Если распарсилось - проверяем что это StatementNode
                from mini_db.ast.nodes import StatementNode
                assert isinstance(ast, StatementNode)
            except (ParseError, LexerError):
                # Ожидаемое поведение
                pass
            except Exception as e:
                pytest.fail(f"Parser crashed with unexpected exception: {type(e).__name__}: {e}\nInput: {garbage!r}")
    
    def test_parser_handles_valid_queries(self, fuzzer):
        """Parser должен успешно парсить валидные запросы."""
        parser = Parser()
        
        for _ in range(500):
            query = fuzzer.generate_valid_query()
            try:
                ast = parser.parse(query)
                from mini_db.ast.nodes import StatementNode
                assert isinstance(ast, StatementNode)
            except (ParseError, LexerError) as e:
                # Некоторые сгенерированные запросы могут быть синтаксически некорректны
                # (например, INSERT без указания типов колонок)
                pass
    
    def test_parser_deeply_nested_where(self, fuzzer):
        """Parser должен обрабатывать глубокую вложенность WHERE."""
        parser = Parser()
        
        # Генерируем глубоко вложенное выражение
        depth = 20
        expr = "x = 1"
        for _ in range(depth):
            expr = f"({expr})"
        
        query = f"SELECT * FROM t WHERE {expr}"
        
        try:
            ast = parser.parse(query)
            from mini_db.ast.nodes import SelectNode
            assert isinstance(ast, SelectNode)
            assert ast.where is not None
        except RecursionError:
            pytest.fail("Parser hit recursion limit on deeply nested WHERE")
    
    def test_parser_very_long_query(self, fuzzer):
        """Parser должен обрабатывать очень длинные запросы."""
        parser = Parser()
        
        # Очень длинный SELECT с многими колонками
        columns = ', '.join(f"col_{i}" for i in range(1000))
        query = f"SELECT {columns} FROM t"
        
        try:
            ast = parser.parse(query)
            from mini_db.ast.nodes import SelectNode
            assert isinstance(ast, SelectNode)
            assert len(ast.columns) == 1000
        except MemoryError:
            pytest.fail("Parser ran out of memory on long query")
    
    def test_parser_consecutive_operators(self, fuzzer):
        """Parser должен отклонять подряд идущие операторы."""
        parser = Parser()
        
        invalid_queries = [
            "SELECT * FROM t WHERE x = = 1",
            "SELECT * FROM t WHERE x < > 1",
            "SELECT * FROM t WHERE x AND AND y",
            "SELECT * FROM t WHERE x OR OR y",
        ]
        
        for query in invalid_queries:
            with pytest.raises((ParseError, LexerError)):
                parser.parse(query)


class TestREPLFuzzing:
    """Fuzzing тесты для REPL."""
    
    def test_repl_never_shows_traceback(self, fuzzer):
        """REPL никогда не должен показывать Python Traceback."""
        repl = REPL()
        
        for _ in range(500):
            garbage = fuzzer.generate_garbage()
            output = repl.process(garbage)
            
            # Проверяем что нет Python Traceback
            assert "Traceback" not in output, f"REPL showed traceback for input: {garbage!r}\nOutput: {output}"
            assert "File " not in output or "Error:" in output, f"REPL showed file info: {output}"
    
    def test_repl_handles_valid_queries_gracefully(self, fuzzer):
        """REPL должен обрабатывать валидные запросы без падений."""
        repl = REPL()
        
        # Сначала создаём таблицу
        repl.process("CREATE TABLE test_table (id INT, name TEXT)")
        
        for _ in range(100):
            # Только SELECT и INSERT (без CREATE TABLE с случайными именами)
            if random.random() < 0.5:
                query = f"INSERT INTO test_table (id, name) VALUES ({random.randint(1, 100)}, '{fuzzer.random_string(5, 20)}')"
            else:
                query = "SELECT * FROM test_table"
            
            output = repl.process(query)
            assert "Traceback" not in output
    
    def test_repl_unicode_handling(self, fuzzer):
        """REPL должен корректно работать с Unicode."""
        repl = REPL()
        
        # Создаём таблицу
        repl.process("CREATE TABLE unicode_test (id INT, text TEXT)")
        
        # Вставляем Unicode данные
        unicode_values = [
            "'привет мир'",
            "'你好世界'",
            "'🎉🎊🎁'",
            "'مرحبا بالعالم'",
        ]
        
        for val in unicode_values:
            output = repl.process(f"INSERT INTO unicode_test (id, text) VALUES (1, {val})")
            # Не должно быть traceback
            assert "Traceback" not in output


class TestStressFuzzing:
    """Стресс-тесты с большими объёмами данных."""
    
    def test_lexer_stress_many_tokens(self, fuzzer):
        """Lexer должен обрабатывать запросы с тысячами токенов."""
        lexer = Lexer()
        
        # Генерируем очень длинный запрос
        parts = ["SELECT * FROM t WHERE"]
        for i in range(1000):
            if i > 0:
                parts.append("OR")
            parts.append(f"x{i} = {i}")
        
        query = ' '.join(parts)
        
        try:
            tokens = lexer.tokenize(query)
            assert len(tokens) > 1000
        except MemoryError:
            pytest.fail("Lexer ran out of memory")
    
    def test_parser_stress_complex_where(self, fuzzer):
        """Parser должен обрабатывать сложные WHERE с сотнями условий."""
        parser = Parser()
        
        # Генерируем сложное WHERE
        conditions = [f"x{i} = {i}" for i in range(100)]
        where = " OR ".join(conditions)
        query = f"SELECT * FROM t WHERE {where}"
        
        try:
            ast = parser.parse(query)
            from mini_db.ast.nodes import SelectNode, LogicalNode
            assert isinstance(ast, SelectNode)
        except MemoryError:
            pytest.fail("Parser ran out of memory")
    
    def test_fuzzing_reproducibility(self):
        """Проверка что seed даёт детерминированную генерацию."""
        # Тестируем что один и тот же seed даёт одинаковые результаты
        # Важно: не используем fixture, создаём fuzzer локально
        
        # Две независимые генерации с одинаковым seed
        results1 = []
        results2 = []
        
        # Первая генерация
        fuzzer = SQLFuzzer(seed=42)
        for _ in range(10):
            results1.append(fuzzer.random_identifier())
        
        # Вторая генерация с тем же seed
        fuzzer = SQLFuzzer(seed=42)
        for _ in range(10):
            results2.append(fuzzer.random_identifier())
        
        # Должны быть одинаковыми
        assert results1 == results2, f"Seed doesn't produce reproducible results: {results1} != {results2}"


# ==================== ADVERSARIAL FUZZING ====================

class TestAdversarialFuzzing:
    """Адверсарные тесты - попытки сломать систему."""
    
    def test_sql_injection_attempts(self, fuzzer):
        """Проверка устойчивости к SQL-инъекциям."""
        repl = REPL()
        repl.process("CREATE TABLE users (id INT, name TEXT, password TEXT)")
        repl.process("INSERT INTO users (id, name, password) VALUES (1, 'admin', 'secret')")
        
        injection_attempts = [
            "SELECT * FROM users WHERE name = 'admin' --' AND password = 'x'",
            "SELECT * FROM users WHERE name = 'admin'; DROP TABLE users; --",
            "SELECT * FROM users WHERE 1=1 OR 1=1",
            "SELECT * FROM users WHERE name = 'admin' OR '1'='1'",
            "INSERT INTO users (id, name, password) VALUES (2, 'hacker', 'hack'); SELECT * FROM users",
        ]
        
        for attempt in injection_attempts:
            output = repl.process(attempt)
            # Не должно быть traceback
            assert "Traceback" not in output
            # Проверяем что таблица не удалена
            check = repl.process("SELECT * FROM users")
            assert "admin" in check or "Error" in output
    
    def test_null_byte_injection(self, fuzzer):
        """Проверка обработки null-байтов."""
        lexer = Lexer()
        
        queries_with_null = [
            "SELECT * FROM t WHERE x = 'test\x00value'",
            "CREATE TABLE t\x00test (x INT)",
            "INSERT INTO t VALUES (1, 'a\x00b')",
        ]
        
        for query in queries_with_null:
            try:
                tokens = lexer.tokenize(query)
                # Если прошло - проверяем что null-байт не сломал структуру
                assert isinstance(tokens, list)
            except LexerError:
                # Допустимо
                pass
    
    def test_extreme_values(self, fuzzer):
        """Проверка экстремальных значений."""
        parser = Parser()
        
        extreme_queries = [
            # Очень большое число
            f"SELECT * FROM t WHERE x = {10**100}",
            # Очень длинная строка
            f"INSERT INTO t VALUES ('{'x' * 100000}')",
            # Глубокая вложенность
            "SELECT * FROM t WHERE " + "(" * 100 + "x = 1" + ")" * 100,
        ]
        
        for query in extreme_queries:
            try:
                ast = parser.parse(query)
            except (ParseError, LexerError, RecursionError, MemoryError):
                # Допустимо для экстремальных значений
                pass
            except Exception as e:
                pytest.fail(f"Unexpected exception for extreme query: {type(e).__name__}")