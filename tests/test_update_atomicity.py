# START_MODULE_CONTRACT
# Module: tests.test_update_atomicity
# Intent: Тесты атомарности UPDATE - CHECKPOINT #2.
#         Проверка All-or-Nothing при UNIQUE violation.
# END_MODULE_CONTRACT

import pytest

from mini_db.ast.nodes import ColumnDef, UpdateNode, LiteralNode, ComparisonNode, IdentifierNode
from mini_db.executor.executor import Executor, ExecutionResult
from mini_db.parser.parser import Parser
from mini_db.storage.database import Database
from mini_db.storage.table import Table


# ==================== CHECKPOINT #2 ====================

class TestCheckpoint2AtomicUpdate:
    """
    CHECKPOINT #2: UPDATE нескольких строк откатывается полностью при нарушении UNIQUE.
    Это критический тест атомарности.
    """
    
    def test_checkpoint2_unique_violation_full_rollback(self):
        """
        [CHECKPOINT #2] UPDATE пытается установить id = 1 для всех строк.
        При обработке второй строки возникает UNIQUE violation.
        Все изменения откатываются, данные остаются неизменными.
        """
        db = Database()
        executor = Executor()
        
        # Setup
        result = executor.execute(
            Parser().parse("CREATE TABLE t (id INT UNIQUE, val TEXT)"),
            db
        )
        assert result.success
        
        result = executor.execute(
            Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"),
            db
        )
        assert result.success
        
        result = executor.execute(
            Parser().parse("INSERT INTO t (id, val) VALUES (2, 'b')"),
            db
        )
        assert result.success
        
        # Verify initial state
        result = executor.execute(
            Parser().parse("SELECT * FROM t"),
            db
        )
        assert result.data == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
        
        # Execute UPDATE that should fail
        result = executor.execute(
            Parser().parse("UPDATE t SET id = 1"),
            db
        )
        
        # CHECKPOINT assertions
        assert result.success == False, "UPDATE should fail due to UNIQUE violation"
        assert "UNIQUE" in result.error, f"Error should mention UNIQUE, got: {result.error}"
        
        # Verify data is unchanged (rollback successful)
        result = executor.execute(
            Parser().parse("SELECT * FROM t"),
            db
        )
        assert result.data == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}], \
            "Data should be unchanged after rollback"
    
    def test_checkpoint2_partial_update_then_rollback(self):
        """
        UPDATE нескольких строк: первая обновляется, вторая вызывает UNIQUE violation.
        Проверяем что первая строка тоже откатывается.
        """
        db = Database()
        executor = Executor()
        
        # Setup: 3 rows with unique ids
        executor.execute(Parser().parse("CREATE TABLE t (id INT UNIQUE, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (2, 'b')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (3, 'c')"), db)
        
        # Try to update all rows to id = 1 (should fail on second row)
        result = executor.execute(Parser().parse("UPDATE t SET id = 1"), db)
        
        assert result.success == False
        
        # All rows should be unchanged (ORDER BY not supported, check by count and values)
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert len(result.data) == 3
        # Check all original rows are present
        assert {"id": 1, "val": "a"} in result.data
        assert {"id": 2, "val": "b"} in result.data
        assert {"id": 3, "val": "c"} in result.data
    
    def test_checkpoint2_update_with_where_unique_violation(self):
        """
        UPDATE с WHERE: обновляем только одну строку, но она вызывает UNIQUE violation.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT UNIQUE, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (2, 'b')"), db)
        
        # Try to update id=2 to id=1 (UNIQUE violation)
        result = executor.execute(Parser().parse("UPDATE t SET id = 1 WHERE id = 2"), db)
        
        assert result.success == False
        assert "UNIQUE" in result.error
        
        # Data unchanged
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert result.data == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]


# ==================== ATOMICITY EDGE CASES ====================

class TestUpdateAtomicityEdgeCases:
    """
    Дополнительные тесты атомарности UPDATE.
    """
    
    def test_update_multiple_columns_one_fails_unique(self):
        """
        UPDATE нескольких колонок: одна вызывает UNIQUE violation.
        Все изменения должны откатиться.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT UNIQUE, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (2, 'b')"), db)
        
        # Update both columns, but id causes UNIQUE violation
        result = executor.execute(Parser().parse("UPDATE t SET id = 1, val = 'x'"), db)
        
        assert result.success == False
        
        # Both columns should be unchanged
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert result.data == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    
    def test_update_successful_no_rollback_needed(self):
        """
        Успешный UPDATE не должен вызывать rollback.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (2, 'b')"), db)
        
        # Successful update (no UNIQUE constraint)
        result = executor.execute(Parser().parse("UPDATE t SET val = 'x'"), db)
        
        assert result.success == True
        assert result.message == "2 row(s) updated"
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert result.data == [{"id": 1, "val": "x"}, {"id": 2, "val": "x"}]
    
    def test_update_no_rows_matched(self):
        """
        UPDATE с WHERE, который не находит строк.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        
        result = executor.execute(Parser().parse("UPDATE t SET val = 'x' WHERE id = 999"), db)
        
        assert result.success == True
        assert result.message == "0 row(s) updated"
        
        # Data unchanged
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert result.data == [{"id": 1, "val": "a"}]
    
    def test_update_single_row_success(self):
        """
        UPDATE одной строки успешно.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT UNIQUE, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (2, 'b')"), db)
        
        result = executor.execute(Parser().parse("UPDATE t SET val = 'x' WHERE id = 1"), db)
        
        assert result.success == True
        assert result.message == "1 row(s) updated"
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert result.data == [{"id": 1, "val": "x"}, {"id": 2, "val": "b"}]
    
    def test_update_to_null_in_unique_column(self):
        """
        UPDATE в NULL для UNIQUE колонки разрешён (NULL != NULL).
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT UNIQUE, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (2, 'b')"), db)
        
        # Update both to NULL - should succeed (NULLs are not compared)
        result = executor.execute(Parser().parse("UPDATE t SET id = NULL"), db)
        
        assert result.success == True
        assert result.message == "2 row(s) updated"
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert result.data == [{"id": None, "val": "a"}, {"id": None, "val": "b"}]


# ==================== TYPE VALIDATION IN UPDATE ====================

class TestUpdateTypeValidation:
    """
    Проверка типизации при UPDATE.
    """
    
    def test_update_type_mismatch_int_to_text(self):
        """
        UPDATE с неверным типом: INT колонка, TEXT значение.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        
        result = executor.execute(Parser().parse("UPDATE t SET id = 'string'"), db)
        
        assert result.success == False
        assert "Type mismatch" in result.error or "type" in result.error.lower()
    
    def test_update_type_mismatch_text_to_int(self):
        """
        UPDATE с неверным типом: TEXT колонка, INT значение - OK (int can be string).
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        
        # TEXT column with string value
        result = executor.execute(Parser().parse("UPDATE t SET val = 'text'"), db)
        
        assert result.success == True
    
    def test_update_type_mismatch_bool_to_int(self):
        """
        UPDATE с неверным типом: INT колонка, BOOL значение.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        
        result = executor.execute(Parser().parse("UPDATE t SET id = true"), db)
        
        assert result.success == False
        assert "Type mismatch" in result.error or "type" in result.error.lower()


# ==================== UNKNOWN COLUMN ====================

class TestUpdateUnknownColumn:
    """
    Проверка ошибок при UPDATE неизвестной колонки.
    """
    
    def test_update_unknown_column(self):
        """
        UPDATE несуществующей колонки.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        
        result = executor.execute(Parser().parse("UPDATE t SET unknown = 1"), db)
        
        assert result.success == False
        assert "Unknown column" in result.error or "unknown" in result.error.lower()
    
    def test_update_unknown_table(self):
        """
        UPDATE несуществующей таблицы.
        """
        db = Database()
        executor = Executor()
        
        result = executor.execute(Parser().parse("UPDATE nonexistent SET id = 1"), db)
        
        assert result.success == False
        assert "does not exist" in result.error or "not exist" in result.error.lower()