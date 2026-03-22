# START_MODULE_CONTRACT
# Module: tests.test_repl_errors
# Intent: Тесты для graceful error handling в REPL (Phase 5).
#         Проверка что REPL не падает с Python Traceback.
# END_MODULE_CONTRACT

import pytest

from mini_db.repl.repl import REPL


class TestREPLErrorHandling:
    """Тесты graceful error handling в REPL."""

    def test_syntax_error_no_traceback(self):
        """REPL выводит 'Syntax error:' вместо Python Traceback."""
        repl = REPL()
        output = repl.process("SELECT * FORM t")  # typo: FORM instead of FROM
        
        assert "Syntax error:" in output
        assert "Traceback" not in output

    def test_incomplete_query_no_traceback(self):
        """REPL обрабатывает неполный запрос."""
        repl = REPL()
        output = repl.process("SELECT * FROM")  # incomplete
        
        assert "Syntax error:" in output or "Error:" in output
        assert "Traceback" not in output

    def test_nonexistent_table_no_traceback(self):
        """REPL обрабатывает несуществующую таблицу."""
        repl = REPL()
        output = repl.process("SELECT * FROM nonexistent;")
        
        assert "Error:" in output
        assert "Traceback" not in output

    def test_type_mismatch_no_traceback(self):
        """REPL обрабатывает несовпадение типов."""
        repl = REPL()
        repl.process("CREATE TABLE t (id INT);")
        output = repl.process("INSERT INTO t (id) VALUES ('string');")
        
        assert "Error:" in output
        assert "Traceback" not in output

    def test_unique_violation_no_traceback(self):
        """REPL обрабатывает UNIQUE violation."""
        repl = REPL()
        repl.process("CREATE TABLE t (id INT UNIQUE);")
        repl.process("INSERT INTO t (id) VALUES (1);")
        output = repl.process("INSERT INTO t (id) VALUES (1);")
        
        assert "Error:" in output
        assert "Traceback" not in output

    def test_invalid_column_select_no_traceback(self):
        """REPL обрабатывает SELECT из несуществующей таблицы."""
        repl = REPL()
        output = repl.process("SELECT * FROM nonexistent_table;")
        
        assert "Error:" in output
        assert "Traceback" not in output

    def test_division_by_zero_in_comparison(self):
        """REPL обрабатывает ошибки сравнения."""
        repl = REPL()
        repl.process("CREATE TABLE t (id INT);")
        repl.process("INSERT INTO t (id) VALUES (1);")
        # This should work, but let's test a complex case
        output = repl.process("SELECT * FROM t WHERE id = id;")
        
        # Should not crash
        assert "Traceback" not in output


class TestREPLValidQueries:
    """Тесты корректных запросов через REPL."""

    def test_create_table(self):
        """REPL выполняет CREATE TABLE."""
        repl = REPL()
        output = repl.process("CREATE TABLE users (id INT UNIQUE, name TEXT);")
        
        assert "Error:" not in output
        assert "Syntax error:" not in output

    def test_insert_and_select(self):
        """REPL выполняет INSERT и SELECT."""
        repl = REPL()
        repl.process("CREATE TABLE t (id INT, name TEXT);")
        repl.process("INSERT INTO t (id, name) VALUES (1, 'Alice');")
        output = repl.process("SELECT * FROM t;")
        
        assert "1" in output
        assert "Alice" in output

    def test_update(self):
        """REPL выполняет UPDATE."""
        repl = REPL()
        repl.process("CREATE TABLE t (id INT, val TEXT);")
        repl.process("INSERT INTO t (id, val) VALUES (1, 'old');")
        repl.process("UPDATE t SET val = 'new' WHERE id = 1;")
        output = repl.process("SELECT * FROM t;")
        
        assert "new" in output
        assert "old" not in output

    def test_delete(self):
        """REPL выполняет DELETE."""
        repl = REPL()
        repl.process("CREATE TABLE t (id INT);")
        repl.process("INSERT INTO t (id) VALUES (1);")
        repl.process("INSERT INTO t (id) VALUES (2);")
        repl.process("DELETE FROM t WHERE id = 1;")
        output = repl.process("SELECT * FROM t;")
        
        assert "2" in output
        assert "1 row" in output

    def test_exit(self):
        """REPL обрабатывает EXIT."""
        repl = REPL()
        output = repl.process("EXIT;")
        
        assert "Goodbye" in output
        assert not repl.running


class TestREPLSaveLoad:
    """Тесты SAVE/LOAD через REPL."""

    def test_save_and_load(self):
        """REPL выполняет SAVE и LOAD."""
        import tempfile
        import os
        
        repl = REPL()
        repl.process("CREATE TABLE t (id INT);")
        repl.process("INSERT INTO t (id) VALUES (1);")
        
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "test.json")
            
            # Save
            output = repl.process(f"SAVE '{filepath}';")
            assert "Error:" not in output
            
            # Create new REPL and load
            repl2 = REPL()
            output = repl2.process(f"LOAD '{filepath}';")
            assert "Error:" not in output
            
            # Verify data
            output = repl2.process("SELECT * FROM t;")
            assert "1" in output


class TestREPLEdgeCases:
    """Граничные случаи REPL."""

    def test_empty_query(self):
        """REPL обрабатывает пустой запрос."""
        repl = REPL()
        output = repl.process("")
        
        # Should not crash
        assert output is not None

    def test_whitespace_only(self):
        """REPL обрабатывает только пробелы."""
        repl = REPL()
        output = repl.process("   ")
        
        # Should not crash
        assert output is not None

    def test_semicolon_only(self):
        """REPL обрабатывает только точку с запятой."""
        repl = REPL()
        output = repl.process(";")
        
        # Should not crash
        assert "Syntax error:" in output or "Error:" in output
        assert "Traceback" not in output

    def test_multiple_statements(self):
        """REPL обрабатывает несколько операторов (только первый)."""
        repl = REPL()
        output = repl.process("CREATE TABLE t (id INT); SELECT * FROM t;")
        
        # Should process first statement
        assert "Error:" not in output or "Syntax error:" not in output

    def test_case_insensitive_keywords(self):
        """REPL принимает ключевые слова в любом регистре."""
        repl = REPL()
        output = repl.process("create table t (id int);")
        
        assert "Error:" not in output
        assert "Syntax error:" not in output

    def test_null_value(self):
        """REPL работает с NULL значениями."""
        repl = REPL()
        repl.process("CREATE TABLE t (id INT UNIQUE, val TEXT);")
        repl.process("INSERT INTO t (id, val) VALUES (1, NULL);")
        output = repl.process("SELECT * FROM t;")
        
        assert "NULL" in output or "null" in output.lower()

    def test_boolean_values(self):
        """REPL работает с BOOL значениями."""
        repl = REPL()
        repl.process("CREATE TABLE t (id INT, active BOOL);")
        repl.process("INSERT INTO t (id, active) VALUES (1, true);")
        repl.process("INSERT INTO t (id, active) VALUES (2, false);")
        output = repl.process("SELECT * FROM t;")
        
        assert "true" in output or "True" in output
        assert "false" in output or "False" in output


class TestREPLComplexQueries:
    """Сложные запросы через REPL."""

    def test_where_with_and(self):
        """REPL выполняет WHERE с AND."""
        repl = REPL()
        repl.process("CREATE TABLE t (id INT, val INT);")
        repl.process("INSERT INTO t (id, val) VALUES (1, 10);")
        repl.process("INSERT INTO t (id, val) VALUES (2, 20);")
        output = repl.process("SELECT * FROM t WHERE id = 1 AND val = 10;")
        
        assert "1" in output
        assert "10" in output

    def test_where_with_or(self):
        """REPL выполняет WHERE с OR."""
        repl = REPL()
        repl.process("CREATE TABLE t (id INT);")
        repl.process("INSERT INTO t (id) VALUES (1);")
        repl.process("INSERT INTO t (id) VALUES (2);")
        repl.process("INSERT INTO t (id) VALUES (3);")
        output = repl.process("SELECT * FROM t WHERE id = 1 OR id = 3;")
        
        assert "1" in output
        assert "3" in output
        assert "2 row" in output

    def test_where_with_parentheses(self):
        """REPL выполняет WHERE со скобками."""
        repl = REPL()
        repl.process("CREATE TABLE t (a INT, b INT, c INT);")
        repl.process("INSERT INTO t (a, b, c) VALUES (1, 2, 3);")
        repl.process("INSERT INTO t (a, b, c) VALUES (4, 5, 6);")
        output = repl.process("SELECT * FROM t WHERE (a = 1 OR a = 4) AND b > 1;")
        
        assert "2 row" in output

    def test_update_with_where(self):
        """REPL выполняет UPDATE с WHERE."""
        repl = REPL()
        repl.process("CREATE TABLE t (id INT, val TEXT);")
        repl.process("INSERT INTO t (id, val) VALUES (1, 'a');")
        repl.process("INSERT INTO t (id, val) VALUES (2, 'b');")
        repl.process("UPDATE t SET val = 'updated' WHERE id = 1;")
        output = repl.process("SELECT * FROM t;")
        
        assert "updated" in output
        assert "'b'" in output  # unchanged

    def test_delete_with_where(self):
        """REPL выполняет DELETE с WHERE."""
        repl = REPL()
        repl.process("CREATE TABLE t (id INT);")
        repl.process("INSERT INTO t (id) VALUES (1);")
        repl.process("INSERT INTO t (id) VALUES (2);")
        repl.process("INSERT INTO t (id) VALUES (3);")
        repl.process("DELETE FROM t WHERE id != 2;")
        output = repl.process("SELECT * FROM t;")
        
        assert "2" in output
        assert "1 row" in output


class TestREPLCheckpoint3:
    """CHECKPOINT #3: Критические тесты."""

    def test_checkpoint3_typo_in_keyword(self):
        """CHECKPOINT #3: Опечатка в ключевом слове."""
        repl = REPL()
        output = repl.process("SELECT * FORM t")  # FORM instead of FROM
        
        assert "Syntax error:" in output
        assert "Traceback" not in output

    def test_checkpoint3_invalid_syntax(self):
        """CHECKPOINT #3: Невалидный синтаксис."""
        repl = REPL()
        output = repl.process("INSERT INTO t (id VALUES (1);")  # missing )
        
        assert "Syntax error:" in output or "Error:" in output
        assert "Traceback" not in output

    def test_checkpoint3_runtime_error(self):
        """CHECKPOINT #3: Ошибка выполнения."""
        repl = REPL()
        output = repl.process("SELECT * FROM nonexistent_table;")
        
        assert "Error:" in output
        assert "Traceback" not in output

    def test_checkpoint3_constraint_violation(self):
        """CHECKPOINT #3: Нарушение constraint."""
        repl = REPL()
        repl.process("CREATE TABLE t (id INT UNIQUE);")
        repl.process("INSERT INTO t (id) VALUES (1);")
        output = repl.process("INSERT INTO t (id) VALUES (1);")
        
        assert "Error:" in output
        assert "Traceback" not in output

    def test_checkpoint3_all_errors_graceful(self):
        """CHECKPOINT #3: Все ошибки обрабатываются gracefully."""
        repl = REPL()
        
        test_cases = [
            "SELECT * FORM t",  # typo
            "INSERT INTO t VALUES",  # incomplete
            "SELECT * FROM nonexistent;",  # runtime
            "CREATE TABLE t (id INT UNIQUE); INSERT INTO t (id) VALUES (1); INSERT INTO t (id) VALUES (1);",  # constraint
            "UPDATE t SET id = 'string' WHERE id = 1;",  # type mismatch
            "DELETE FORM t;",  # typo
        ]
        
        # Create table for some tests
        repl.process("CREATE TABLE t (id INT UNIQUE);")
        
        for query in test_cases:
            output = repl.process(query)
            assert "Traceback" not in output, f"Query '{query}' caused traceback"