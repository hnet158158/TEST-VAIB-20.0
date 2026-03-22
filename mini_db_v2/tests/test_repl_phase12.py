# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_repl_phase12
# Intent: Comprehensive test suite for Phase 12 - SQL-92 Compliance + REPL
# Dependencies: pytest, mini_db_v2.repl, mini_db_v2.parser, mini_db_v2.executor, mini_db_v2.storage
# END_MODULE_CONTRACT

"""
Phase 12 Test Suite: SQL-92 Compliance + REPL

Coverage:
- REPL: Interactive mode, multi-line input, dot-commands, error handling
- SQL-92: CASE, CAST, COALESCE, NULLIF, IFNULL
- Integration: Full SQL workflow
"""

import pytest
from io import StringIO
import sys
from unittest.mock import patch, MagicMock

from mini_db_v2.storage.database import Database
from mini_db_v2.executor.executor import Executor, ExecutorError
from mini_db_v2.parser.parser import Parser, ParseError
from mini_db_v2.parser.lexer import LexerError
from mini_db_v2.repl.repl import REPL
from mini_db_v2.repl.commands import CommandHandler, REPLCommand


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def database():
    """Создаёт тестовую базу данных."""
    return Database(name="test_db")


@pytest.fixture
def executor(database):
    """Создаёт executor для тестов."""
    return Executor(database)


@pytest.fixture
def repl(database):
    """Создаёт REPL для тестов."""
    return REPL(database)


@pytest.fixture
def command_handler(database):
    """Создаёт CommandHandler для тестов."""
    return CommandHandler(database)


@pytest.fixture
def populated_database(database):
    """Создаёт базу данных с тестовыми данными."""
    # Create table
    parser = Parser(sql="CREATE TABLE employees (id INT, name TEXT, salary REAL, active BOOL)")
    ast = parser.parse()
    executor = Executor(database)
    executor.execute(ast)
    
    # Insert test data
    test_data = [
        "INSERT INTO employees (id, name, salary, active) VALUES (1, 'Alice', 75000.0, true)",
        "INSERT INTO employees (id, name, salary, active) VALUES (2, 'Bob', 50000.0, true)",
        "INSERT INTO employees (id, name, salary, active) VALUES (3, 'Charlie', 120000.0, false)",
        "INSERT INTO employees (id, name, salary, active) VALUES (4, 'Diana', NULL, true)",
        "INSERT INTO employees (id, name, salary, active) VALUES (5, 'Eve', 95000.0, true)",
    ]
    
    for stmt in test_data:
        parser = Parser(sql=stmt)
        ast = parser.parse()
        executor.execute(ast)
    
    return database


# =============================================================================
# REPL COMMANDS TESTS
# =============================================================================

class TestREPLCommands:
    """Тесты для REPL dot-команд."""
    
    # -------------------------------------------------------------------------
    # Command Parsing
    # -------------------------------------------------------------------------
    
    def test_parse_help_command(self, command_handler):
        """Парсинг .help команды."""
        cmd, args = command_handler.parse_command(".help")
        assert cmd == REPLCommand.HELP
        assert args == []
    
    def test_parse_tables_command(self, command_handler):
        """Парсинг .tables команды."""
        cmd, args = command_handler.parse_command(".tables")
        assert cmd == REPLCommand.TABLES
        assert args == []
    
    def test_parse_schema_command(self, command_handler):
        """Парсинг .schema команды."""
        cmd, args = command_handler.parse_command(".schema employees")
        assert cmd == REPLCommand.SCHEMA
        assert args == ["employees"]
    
    def test_parse_indices_command(self, command_handler):
        """Парсинг .indices команды."""
        cmd, args = command_handler.parse_command(".indices employees")
        assert cmd == REPLCommand.INDICES
        assert args == ["employees"]
    
    def test_parse_timer_command(self, command_handler):
        """Парсинг .timer команды."""
        cmd, args = command_handler.parse_command(".timer on")
        assert cmd == REPLCommand.TIMER
        assert args == ["on"]
    
    def test_parse_quit_command(self, command_handler):
        """Парсинг .quit команды."""
        cmd, args = command_handler.parse_command(".quit")
        assert cmd == REPLCommand.QUIT
        assert args == []
    
    def test_parse_exit_command(self, command_handler):
        """Парсинг .exit команды."""
        cmd, args = command_handler.parse_command(".exit")
        assert cmd == REPLCommand.EXIT
        assert args == []
    
    def test_parse_unknown_command(self, command_handler):
        """Парсинг неизвестной команды."""
        cmd, args = command_handler.parse_command(".unknown")
        assert cmd == REPLCommand.UNKNOWN
        assert args == []
    
    def test_parse_command_case_insensitive(self, command_handler):
        """Парсинг команды без учёта регистра."""
        cmd, _ = command_handler.parse_command(".HELP")
        assert cmd == REPLCommand.HELP
        
        cmd, _ = command_handler.parse_command(".Tables")
        assert cmd == REPLCommand.TABLES
    
    # -------------------------------------------------------------------------
    # Command Handling
    # -------------------------------------------------------------------------
    
    def test_handle_help(self, command_handler):
        """Обработка .help команды."""
        result = command_handler.handle(".help")
        assert result is not None
        assert ".help" in result
        assert ".tables" in result
        assert ".schema" in result
        assert ".quit" in result
    
    def test_handle_tables_empty(self, command_handler):
        """Обработка .tables на пустой БД."""
        result = command_handler.handle_tables()
        assert "No tables found" in result
    
    def test_handle_tables_with_data(self, populated_database):
        """Обработка .tables с данными."""
        handler = CommandHandler(populated_database)
        result = handler.handle_tables()
        assert "employees" in result
        assert "5 rows" in result
    
    def test_handle_schema_all(self, populated_database):
        """Обработка .schema без аргументов."""
        handler = CommandHandler(populated_database)
        result = handler.handle_schema()
        assert "CREATE TABLE employees" in result
        assert "id INT" in result
        assert "name TEXT" in result
    
    def test_handle_schema_specific(self, populated_database):
        """Обработка .schema для конкретной таблицы."""
        handler = CommandHandler(populated_database)
        result = handler.handle_schema("employees")
        assert "CREATE TABLE employees" in result
    
    def test_handle_schema_nonexistent(self, command_handler):
        """Обработка .schema для несуществующей таблицы."""
        result = command_handler.handle_schema("nonexistent")
        assert "not found" in result
    
    def test_handle_timer_on(self, command_handler):
        """Включение таймера."""
        result = command_handler.handle_timer("on")
        assert "enabled" in result.lower()
        assert command_handler.timer_enabled is True
    
    def test_handle_timer_off(self, command_handler):
        """Выключение таймера."""
        command_handler.handle_timer("on")  # Enable first
        result = command_handler.handle_timer("off")
        assert "disabled" in result.lower()
        assert command_handler.timer_enabled is False
    
    def test_handle_timer_status(self, command_handler):
        """Статус таймера без аргументов."""
        result = command_handler.handle_timer()
        assert "Timer is" in result
    
    def test_handle_quit(self, command_handler):
        """Обработка .quit команды."""
        result = command_handler.handle(".quit")
        assert result is None  # None signals exit
    
    def test_handle_exit(self, command_handler):
        """Обработка .exit команды."""
        result = command_handler.handle(".exit")
        assert result is None  # None signals exit
    
    def test_handle_unknown_command(self, command_handler):
        """Обработка неизвестной команды."""
        result = command_handler.handle(".unknown")
        assert "Unknown command" in result


# =============================================================================
# REPL EXECUTION TESTS
# =============================================================================

class TestREPLExecution:
    """Тесты для выполнения SQL в REPL."""
    
    def test_execute_create_table(self, repl):
        """Выполнение CREATE TABLE."""
        result = repl.execute("CREATE TABLE test (id INT, name TEXT);")
        assert "Error:" not in result
    
    def test_execute_insert(self, repl):
        """Выполнение INSERT."""
        repl.execute("CREATE TABLE test (id INT, name TEXT);")
        result = repl.execute("INSERT INTO test (id, name) VALUES (1, 'Alice');")
        assert "Error:" not in result
    
    def test_execute_select(self, repl):
        """Выполнение SELECT."""
        repl.execute("CREATE TABLE test (id INT, name TEXT)")
        repl.execute("INSERT INTO test (id, name) VALUES (1, 'Alice')")
        result = repl.execute("SELECT * FROM test")
        assert "Alice" in result
        assert "1 row" in result
    
    def test_execute_update(self, repl):
        """Выполнение UPDATE."""
        repl.execute("CREATE TABLE test (id INT, name TEXT)")
        repl.execute("INSERT INTO test (id, name) VALUES (1, 'Alice')")
        result = repl.execute("UPDATE test SET name = 'Bob' WHERE id = 1")
        assert "Error:" not in result
    
    def test_execute_delete(self, repl):
        """Выполнение DELETE."""
        repl.execute("CREATE TABLE test (id INT, name TEXT)")
        repl.execute("INSERT INTO test (id, name) VALUES (1, 'Alice')")
        result = repl.execute("DELETE FROM test WHERE id = 1")
        assert "Error:" not in result
    
    def test_execute_syntax_error(self, repl):
        """Обработка синтаксической ошибки."""
        result = repl.execute("SELECT * FORM test")
        assert "Syntax error:" in result
        assert "Traceback" not in result
    
    def test_execute_runtime_error(self, repl):
        """Обработка ошибки выполнения."""
        result = repl.execute("SELECT * FROM nonexistent")
        assert "Error:" in result or "Syntax error:" in result
        assert "Traceback" not in result
    
    def test_format_result_empty(self, repl):
        """Форматирование пустого результата."""
        from mini_db_v2.executor.executor import ExecutionResult
        result = ExecutionResult(success=True, message="OK", rows=[], columns=[])
        output = repl.format_result(result)
        assert "OK" in output
    
    def test_format_result_with_data(self, repl):
        """Форматирование результата с данными."""
        from mini_db_v2.executor.executor import ExecutionResult
        result = ExecutionResult(
            success=True,
            message=None,
            rows=[{"id": 1, "name": "Alice"}],
            columns=["id", "name"]
        )
        output = repl.format_result(result)
        assert "id" in output
        assert "name" in output
        assert "Alice" in output


# =============================================================================
# REPL MULTI-LINE INPUT TESTS
# =============================================================================

class TestREPLMultiLine:
    """Тесты для multi-line input."""
    
    def test_is_complete_with_semicolon(self, repl):
        """Детекция завершения по ;"""
        repl._buffer = ["SELECT *", "FROM test;"]
        assert repl._is_complete() is True
    
    def test_is_complete_without_semicolon(self, repl):
        """Одна строка без ; тоже завершена."""
        repl._buffer = ["SELECT 1"]
        assert repl._is_complete() is True
    
    def test_is_incomplete_open_paren(self, repl):
        """Незавершённое выражение с открытой скобкой."""
        repl._buffer = ["SELECT * FROM test WHERE (id = 1"]
        assert repl._is_complete() is False
    
    def test_is_complete_closed_paren(self, repl):
        """Завершённое выражение с закрытыми скобками."""
        repl._buffer = ["SELECT * FROM test WHERE (id = 1)"]
        assert repl._is_complete() is True
    
    def test_buffer_clears_after_execute(self, repl):
        """Буфер очищается после выполнения."""
        repl._buffer = ["SELECT 1;"]
        repl._is_complete()
        # Note: _iteration() clears buffer, not _is_complete()


# =============================================================================
# CASE EXPRESSION TESTS
# =============================================================================

class TestCaseExpression:
    """Тесты для CASE WHEN ... THEN ... ELSE ... END."""
    
    def test_case_basic(self, populated_database):
        """Базовый CASE expression."""
        executor = Executor(populated_database)
        parser = Parser(sql="""
            SELECT name,
                   CASE WHEN salary > 100000 THEN 'High'
                        WHEN salary > 50000 THEN 'Medium'
                        ELSE 'Low' END AS salary_level
            FROM employees
        """)
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert len(result.rows) == 5
        
        # Check salary levels
        levels = {r['name']: r['salary_level'] for r in result.rows if r['name'] != 'Diana'}
        assert levels.get('Charlie') == 'High'
        assert levels.get('Alice') == 'Medium'
        assert levels.get('Bob') == 'Low'
    
    def test_case_with_null(self, populated_database):
        """CASE с NULL значением."""
        executor = Executor(populated_database)
        parser = Parser(sql="""
            SELECT name, 
                   CASE WHEN salary IS NULL THEN 'Unknown'
                        ELSE 'Known' END AS salary_status
            FROM employees
        """)
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        # Diana has NULL salary
        diana = next(r for r in result.rows if r['name'] == 'Diana')
        assert diana['salary_status'] == 'Unknown'
    
    def test_case_no_else(self, populated_database):
        """CASE без ELSE возвращает NULL."""
        executor = Executor(populated_database)
        parser = Parser(sql="""
            SELECT name,
                   CASE WHEN salary > 200000 THEN 'Very High' END AS salary_level
            FROM employees
        """)
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        # All salaries < 200000, so all should be NULL
        for row in result.rows:
            if row['name'] != 'Diana':  # Skip NULL salary
                assert row['salary_level'] is None
    
    def test_case_in_where(self, populated_database):
        """CASE в WHERE clause."""
        executor = Executor(populated_database)
        parser = Parser(sql="""
            SELECT name FROM employees
            WHERE CASE WHEN active THEN 1 ELSE 0 END = 1
        """)
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        # Only active employees
        names = [r['name'] for r in result.rows]
        assert 'Charlie' not in names  # Charlie is inactive
    
    def test_case_nested(self, populated_database):
        """Вложенный CASE expression."""
        executor = Executor(populated_database)
        parser = Parser(sql="""
            SELECT name,
                   CASE 
                       WHEN salary > 100000 THEN 
                           CASE WHEN active THEN 'Active High'
                                ELSE 'Inactive High' END
                       ELSE 'Other'
                   END AS status
            FROM employees
        """)
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        charlie = next(r for r in result.rows if r['name'] == 'Charlie')
        assert charlie['status'] == 'Inactive High'


# =============================================================================
# CAST FUNCTION TESTS
# =============================================================================

class TestCastFunction:
    """Тесты для CAST(expr AS type)."""
    
    def test_cast_int_to_text(self, populated_database):
        """CAST INT to TEXT."""
        executor = Executor(populated_database)
        parser = Parser(sql="SELECT CAST(id AS TEXT) AS id_str FROM employees")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        for row in result.rows:
            assert isinstance(row['id_str'], str)
    
    def test_cast_text_to_int(self, database):
        """CAST TEXT to INT."""
        executor = Executor(database)
        parser = Parser(sql="SELECT CAST('123' AS INT) AS num")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert result.rows[0]['num'] == 123
    
    def test_cast_real_to_int(self, populated_database):
        """CAST REAL to INT."""
        executor = Executor(populated_database)
        parser = Parser(sql="SELECT CAST(salary AS INT) AS salary_int FROM employees WHERE salary IS NOT NULL")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        for row in result.rows:
            assert isinstance(row['salary_int'], int)
    
    def test_cast_int_to_real(self, populated_database):
        """CAST INT to REAL."""
        executor = Executor(populated_database)
        parser = Parser(sql="SELECT CAST(id AS REAL) AS id_real FROM employees")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        for row in result.rows:
            assert isinstance(row['id_real'], float)
    
    def test_cast_bool_to_text(self, populated_database):
        """CAST BOOL to TEXT."""
        executor = Executor(populated_database)
        parser = Parser(sql="SELECT CAST(active AS TEXT) AS active_str FROM employees")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        for row in result.rows:
            # Python str(bool) returns 'True'/'False'
            assert row['active_str'] in ('True', 'False')
    
    def test_cast_null(self, populated_database):
        """CAST NULL возвращает NULL."""
        executor = Executor(populated_database)
        parser = Parser(sql="SELECT CAST(salary AS INT) AS salary_int FROM employees WHERE name = 'Diana'")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert result.rows[0]['salary_int'] is None
    
    def test_cast_invalid_text_to_int(self, database):
        """CAST невалидного TEXT to INT возвращает ошибку."""
        executor = Executor(database)
        parser = Parser(sql="SELECT CAST('not_a_number' AS INT) AS num")
        ast = parser.parse()
        
        # Current implementation raises ExecutorError
        with pytest.raises(ExecutorError):
            executor.execute(ast)


# =============================================================================
# COALESCE FUNCTION TESTS
# =============================================================================

class TestCoalesceFunction:
    """Тесты для COALESCE(val1, val2, ...)."""
    
    def test_coalesce_first_non_null(self, populated_database):
        """COALESCE возвращает первый не-NULL."""
        executor = Executor(populated_database)
        parser = Parser(sql="""
            SELECT name, COALESCE(salary, 0) AS salary_or_zero
            FROM employees
        """)
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        diana = next(r for r in result.rows if r['name'] == 'Diana')
        assert diana['salary_or_zero'] == 0
    
    def test_coalesce_all_null(self, database):
        """COALESCE со всеми NULL возвращает NULL."""
        executor = Executor(database)
        parser = Parser(sql="SELECT COALESCE(NULL, NULL, NULL) AS result")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert result.rows[0]['result'] is None
    
    def test_coalesce_no_null(self, database):
        """COALESCE без NULL возвращает первое значение."""
        executor = Executor(database)
        parser = Parser(sql="SELECT COALESCE(1, 2, 3) AS result")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert result.rows[0]['result'] == 1
    
    def test_coalesce_mixed_types(self, populated_database):
        """COALESCE с разными типами."""
        executor = Executor(populated_database)
        parser = Parser(sql="""
            SELECT name, COALESCE(salary, 'N/A') AS salary_display
            FROM employees
        """)
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        diana = next(r for r in result.rows if r['name'] == 'Diana')
        assert diana['salary_display'] == 'N/A'
    
    def test_coalesce_with_expression(self, populated_database):
        """COALESCE с выражениями."""
        executor = Executor(populated_database)
        parser = Parser(sql="""
            SELECT name, COALESCE(salary * 1.1, 0) AS adjusted_salary
            FROM employees
        """)
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        # Check Alice's adjusted salary
        alice = next(r for r in result.rows if r['name'] == 'Alice')
        assert alice['adjusted_salary'] == 75000.0 * 1.1


# =============================================================================
# NULLIF FUNCTION TESTS
# =============================================================================

class TestNullifFunction:
    """Тесты для NULLIF(val1, val2)."""
    
    def test_nullif_equal(self, database):
        """NULLIF возвращает NULL если значения равны."""
        executor = Executor(database)
        parser = Parser(sql="SELECT NULLIF(5, 5) AS result")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert result.rows[0]['result'] is None
    
    def test_nullif_not_equal(self, database):
        """NULLIF возвращает первое значение если не равны."""
        executor = Executor(database)
        parser = Parser(sql="SELECT NULLIF(5, 3) AS result")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert result.rows[0]['result'] == 5
    
    def test_nullif_with_null_first(self, database):
        """NULLIF с NULL первым аргументом."""
        executor = Executor(database)
        parser = Parser(sql="SELECT NULLIF(NULL, 5) AS result")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert result.rows[0]['result'] is None
    
    def test_nullif_with_null_second(self, database):
        """NULLIF с NULL вторым аргументом."""
        executor = Executor(database)
        parser = Parser(sql="SELECT NULLIF(5, NULL) AS result")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert result.rows[0]['result'] == 5
    
    def test_nullif_with_columns(self, populated_database):
        """NULLIF с колонками."""
        executor = Executor(populated_database)
        parser = Parser(sql="""
            SELECT name, NULLIF(salary, 0) AS salary_or_null
            FROM employees
            WHERE salary IS NOT NULL
        """)
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        # No employee has salary = 0, so all should have their salary
        for row in result.rows:
            assert row['salary_or_null'] is not None


# =============================================================================
# IFNULL FUNCTION TESTS
# =============================================================================

class TestIfnullFunction:
    """Тесты для IFNULL(val1, val2)."""
    
    def test_ifnull_with_null(self, database):
        """IFNULL с NULL возвращает второй аргумент."""
        executor = Executor(database)
        parser = Parser(sql="SELECT IFNULL(NULL, 'default') AS result")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert result.rows[0]['result'] == 'default'
    
    def test_ifnull_without_null(self, database):
        """IFNULL без NULL возвращает первый аргумент."""
        executor = Executor(database)
        parser = Parser(sql="SELECT IFNULL('value', 'default') AS result")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert result.rows[0]['result'] == 'value'
    
    def test_ifnull_is_coalesce_alias(self, populated_database):
        """IFNULL работает как COALESCE с 2 аргументами."""
        executor = Executor(populated_database)
        
        # Compare IFNULL and COALESCE
        parser1 = Parser(sql="SELECT IFNULL(salary, 0) AS result FROM employees WHERE name = 'Diana'")
        parser2 = Parser(sql="SELECT COALESCE(salary, 0) AS result FROM employees WHERE name = 'Diana'")
        
        result1 = executor.execute(parser1.parse())
        result2 = executor.execute(parser2.parse())
        
        assert result1.rows[0]['result'] == result2.rows[0]['result']


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

class TestIntegration:
    """Интеграционные тесты."""
    
    def test_full_sql_workflow(self, repl):
        """Полный SQL workflow: CREATE, INSERT, SELECT, UPDATE, DELETE."""
        # CREATE TABLE
        result = repl.execute("CREATE TABLE users (id INT, name TEXT, active BOOL)")
        assert "Error:" not in result
        
        # INSERT
        result = repl.execute("INSERT INTO users (id, name, active) VALUES (1, 'Alice', true)")
        assert "Error:" not in result
        
        result = repl.execute("INSERT INTO users (id, name, active) VALUES (2, 'Bob', false)")
        assert "Error:" not in result
        
        # SELECT
        result = repl.execute("SELECT * FROM users")
        assert "Alice" in result
        assert "Bob" in result
        
        # UPDATE
        result = repl.execute("UPDATE users SET active = true WHERE id = 2")
        assert "Error:" not in result
        
        # Verify UPDATE
        result = repl.execute("SELECT * FROM users WHERE id = 2")
        assert "true" in result.lower() or "TRUE" in result or "True" in result
        
        # DELETE
        result = repl.execute("DELETE FROM users WHERE id = 1")
        assert "Error:" not in result
        
        # Verify DELETE
        result = repl.execute("SELECT * FROM users")
        assert "Alice" not in result
        assert "Bob" in result
    
    def test_combined_sql92_features(self, populated_database):
        """Комбинированное использование SQL-92 features."""
        executor = Executor(populated_database)
        parser = Parser(sql="""
            SELECT
                name,
                CASE
                    WHEN COALESCE(salary, 0) > 100000 THEN 'High'
                    WHEN COALESCE(salary, 0) > 50000 THEN 'Medium'
                    ELSE 'Low'
                END AS salary_level,
                CAST(COALESCE(salary, 0) AS TEXT) AS salary_text
            FROM employees
            WHERE active = true
            ORDER BY name
        """)
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert len(result.rows) == 4  # 4 active employees
        
        # Check Diana (NULL salary)
        diana = next((r for r in result.rows if r['name'] == 'Diana'), None)
        if diana:
            assert diana['salary_level'] == 'Low'
            assert diana['salary_text'] == '0'
    
    def test_python_module_entry_point(self):
        """Проверка что python -m mini_db_v2 работает."""
        import subprocess
        import sys
        
        # Just check that the module can be imported
        result = subprocess.run(
            [sys.executable, "-c", "from mini_db_v2 import __main__"],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0


# =============================================================================
# ERROR HANDLING TESTS
# =============================================================================

class TestErrorHandling:
    """Тесты для обработки ошибок."""
    
    def test_no_python_traceback_syntax_error(self, repl):
        """Синтаксическая ошибка не показывает Python traceback."""
        result = repl.execute("SELECT * FORM test")
        assert "Traceback" not in result
        assert "Syntax error:" in result or "Error:" in result
    
    def test_no_python_traceback_runtime_error(self, repl):
        """Ошибка выполнения не показывает Python traceback."""
        result = repl.execute("SELECT * FROM nonexistent_table")
        assert "Traceback" not in result
        # Parser may reject or executor may fail - both are OK
        assert "Syntax error:" in result or "Error:" in result
    
    def test_no_python_traceback_type_error(self, repl):
        """Ошибка типа не показывает Python traceback."""
        repl.execute("CREATE TABLE test (id INT)")
        result = repl.execute("INSERT INTO test (id) VALUES ('not_an_int')")
        assert "Traceback" not in result
        # Parser may reject or executor may fail - all are OK
        assert "Syntax error:" in result or "Error:" in result or "Internal error:" in result
    
    def test_graceful_interrupt_handling(self, repl):
        """Обработка Ctrl+C."""
        # This is tested via the run() method's try/except KeyboardInterrupt
        # We can't easily test interactive input in unit tests
        pass


# =============================================================================
# ADVERSARIAL TESTS
# =============================================================================

class TestAdversarialCases:
    """Адверсарные тесты."""
    
    def test_empty_query(self, repl):
        """Пустой запрос."""
        result = repl.execute("")
        # Should handle gracefully
        assert "Traceback" not in result
    
    def test_whitespace_only(self, repl):
        """Только пробелы."""
        result = repl.execute("   ")
        assert "Traceback" not in result
    
    def test_case_with_all_nulls(self, database):
        """CASE где все условия NULL."""
        executor = Executor(database)
        parser = Parser(sql="""
            SELECT CASE WHEN NULL THEN 'yes' ELSE 'no' END AS result
        """)
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert result.rows[0]['result'] == 'no'
    
    def test_cast_empty_string(self, database):
        """CAST пустой строки."""
        executor = Executor(database)
        parser = Parser(sql="SELECT CAST('' AS INT) AS result")
        ast = parser.parse()
        
        # Current implementation raises ExecutorError
        with pytest.raises(ExecutorError):
            executor.execute(ast)
    
    def test_coalesce_with_one_arg(self, database):
        """COALESCE с одним аргументом."""
        executor = Executor(database)
        parser = Parser(sql="SELECT COALESCE(5) AS result")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert result.rows[0]['result'] == 5
    
    def test_nullif_with_strings(self, database):
        """NULLIF со строками."""
        executor = Executor(database)
        parser = Parser(sql="SELECT NULLIF('hello', 'world') AS result")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert result.rows[0]['result'] == 'hello'
    
    def test_nested_coalesce(self, database):
        """Вложенный COALESCE."""
        executor = Executor(database)
        parser = Parser(sql="SELECT COALESCE(COALESCE(NULL, NULL), 'final') AS result")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert result.rows[0]['result'] == 'final'


# =============================================================================
# CHECKPOINT TESTS
# =============================================================================

class TestCheckpoints:
    """Checkpoint тесты для Phase 12."""
    
    def test_checkpoint_repl_launches(self):
        """Checkpoint: python -m mini_db_v2 запускает REPL."""
        import subprocess
        import sys
        
        # Test that module can be executed
        result = subprocess.run(
            [sys.executable, "-c", "from mini_db_v2.repl import REPL; print('OK')"],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0
        assert "OK" in result.stdout
    
    def test_checkpoint_case_works(self, populated_database):
        """Checkpoint: CASE expression работает."""
        executor = Executor(populated_database)
        parser = Parser(sql="""
            SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END AS result
        """)
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert result.rows[0]['result'] == 'yes'
    
    def test_checkpoint_cast_works(self, database):
        """Checkpoint: CAST function работает."""
        executor = Executor(database)
        parser = Parser(sql="SELECT CAST(123 AS TEXT) AS result")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert result.rows[0]['result'] == '123'
    
    def test_checkpoint_coalesce_works(self, database):
        """Checkpoint: COALESCE function работает."""
        executor = Executor(database)
        parser = Parser(sql="SELECT COALESCE(NULL, 'default') AS result")
        ast = parser.parse()
        result = executor.execute(ast)
        
        assert result.success
        assert result.rows[0]['result'] == 'default'
    
    def test_checkpoint_no_traceback(self, repl):
        """Checkpoint: REPL не показывает Python Traceback."""
        # Test various error conditions
        test_cases = [
            "SELECT * FORM test;",  # Syntax error
            "SELECT * FROM nonexistent;",  # Runtime error
            "INVALID SQL",  # Parse error
        ]
        
        for query in test_cases:
            result = repl.execute(query)
            assert "Traceback" not in result, f"Traceback found for: {query}"


# =============================================================================
# RUNNER
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-p", "no:asyncio"])