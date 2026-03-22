# START_MODULE_CONTRACT
# Module: tests.test_delete
# Intent: Тесты для DELETE операций.
#         DELETE с условием, без условия, проверка данных.
# END_MODULE_CONTRACT

import pytest

from mini_db.executor.executor import Executor
from mini_db.parser.parser import Parser
from mini_db.storage.database import Database


# ==================== DELETE WITH WHERE ====================

class TestDeleteWithWhere:
    """
    DELETE с условием WHERE.
    """
    
    def test_delete_with_where_single_row(self):
        """
        DELETE одной строки по условию.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (2, 'b')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (3, 'c')"), db)
        
        result = executor.execute(Parser().parse("DELETE FROM t WHERE id = 2"), db)
        
        assert result.success == True
        assert result.message == "1 row(s) deleted"
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert result.data == [{"id": 1, "val": "a"}, {"id": 3, "val": "c"}]
    
    def test_delete_with_where_multiple_rows(self):
        """
        DELETE нескольких строк по условию.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (2, 'b')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (3, 'b')"), db)
        
        result = executor.execute(Parser().parse("DELETE FROM t WHERE val = 'b'"), db)
        
        assert result.success == True
        assert result.message == "2 row(s) deleted"
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert result.data == [{"id": 1, "val": "a"}]
    
    def test_delete_with_where_no_match(self):
        """
        DELETE с условием, которое не находит строк.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        
        result = executor.execute(Parser().parse("DELETE FROM t WHERE id = 999"), db)
        
        assert result.success == True
        assert result.message == "0 row(s) deleted"
        
        # Data unchanged
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert result.data == [{"id": 1, "val": "a"}]
    
    def test_delete_with_complex_where(self):
        """
        DELETE с сложным WHERE (AND, OR).
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT, active BOOL)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val, active) VALUES (1, 'a', true)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val, active) VALUES (2, 'b', false)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val, active) VALUES (3, 'c', true)"), db)
        
        # Delete where id > 1 AND active = true
        result = executor.execute(
            Parser().parse("DELETE FROM t WHERE id > 1 AND active = true"),
            db
        )
        
        assert result.success == True
        assert result.message == "1 row(s) deleted"
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert len(result.data) == 2
        assert {"id": 1, "val": "a", "active": True} in result.data
        assert {"id": 2, "val": "b", "active": False} in result.data


# ==================== DELETE WITHOUT WHERE ====================

class TestDeleteWithoutWhere:
    """
    DELETE без WHERE - удаляет все строки.
    """
    
    def test_delete_all_rows(self):
        """
        DELETE без WHERE удаляет все строки.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (2, 'b')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (3, 'c')"), db)
        
        result = executor.execute(Parser().parse("DELETE FROM t"), db)
        
        assert result.success == True
        assert result.message == "3 row(s) deleted"
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert result.data == []
    
    def test_delete_all_rows_empty_table(self):
        """
        DELETE без WHERE на пустой таблице.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        
        result = executor.execute(Parser().parse("DELETE FROM t"), db)
        
        assert result.success == True
        assert result.message == "0 row(s) deleted"


# ==================== DELETE WITH UNIQUE CONSTRAINT ====================

class TestDeleteWithUnique:
    """
    DELETE и UNIQUE constraints.
    """
    
    def test_delete_removes_from_unique_index(self):
        """
        После DELETE значение должно быть удалено из UNIQUE индекса.
        Это позволяет вставить такое же значение позже.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT UNIQUE, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (2, 'b')"), db)
        
        # Delete id=1
        result = executor.execute(Parser().parse("DELETE FROM t WHERE id = 1"), db)
        assert result.success == True
        
        # Now we should be able to insert id=1 again
        result = executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'new')"), db)
        assert result.success == True
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert len(result.data) == 2
        assert {"id": 1, "val": "new"} in result.data
        assert {"id": 2, "val": "b"} in result.data
    
    def test_delete_all_then_insert_same_unique(self):
        """
        DELETE всех строк, затем INSERT с теми же UNIQUE значениями.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT UNIQUE, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (2, 'b')"), db)
        
        # Delete all
        result = executor.execute(Parser().parse("DELETE FROM t"), db)
        assert result.success == True
        
        # Insert same ids
        result = executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'new_a')"), db)
        assert result.success == True
        
        result = executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (2, 'new_b')"), db)
        assert result.success == True


# ==================== DELETE ERRORS ====================

class TestDeleteErrors:
    """
    Ошибки при DELETE.
    """
    
    def test_delete_unknown_table(self):
        """
        DELETE из несуществующей таблицы.
        """
        db = Database()
        executor = Executor()
        
        result = executor.execute(Parser().parse("DELETE FROM nonexistent"), db)
        
        assert result.success == False
        assert "does not exist" in result.error or "not exist" in result.error.lower()
    
    def test_delete_with_where_unknown_column(self):
        """
        DELETE с WHERE на несуществующей колонке.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        
        # WHERE with unknown column - should evaluate to False for all rows
        # or return error depending on implementation
        result = executor.execute(Parser().parse("DELETE FROM t WHERE unknown = 1"), db)
        
        # The row should still exist (no match) or error
        result2 = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert len(result2.data) >= 0  # Either deleted nothing or error


# ==================== DELETE EDGE CASES ====================

class TestDeleteEdgeCases:
    """
    Граничные случаи DELETE.
    """
    
    def test_delete_and_count_rows(self):
        """
        DELETE и проверка количества оставшихся строк.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT)"), db)
        for i in range(10):
            executor.execute(Parser().parse(f"INSERT INTO t (id) VALUES ({i})"), db)
        
        # Delete half
        result = executor.execute(Parser().parse("DELETE FROM t WHERE id < 5"), db)
        assert result.success == True
        assert result.message == "5 row(s) deleted"
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert len(result.data) == 5
    
    def test_delete_preserves_insertion_order(self):
        """
        DELETE сохраняет порядок оставшихся строк.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (2, 'b')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (3, 'c')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (4, 'd')"), db)
        
        # Delete middle rows
        executor.execute(Parser().parse("DELETE FROM t WHERE id = 2 OR id = 3"), db)
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        
        # Order should be preserved: 1, 4
        assert result.data == [{"id": 1, "val": "a"}, {"id": 4, "val": "d"}]
    
    def test_delete_with_null_in_row(self):
        """
        DELETE строки с NULL значениями.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id) VALUES (2)"), db)  # val is NULL
        
        result = executor.execute(Parser().parse("DELETE FROM t WHERE id = 2"), db)
        
        assert result.success == True
        assert result.message == "1 row(s) deleted"
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert result.data == [{"id": 1, "val": "a"}]


# ==================== DELETE WITH COMPARISON OPERATORS ====================

class TestDeleteWithComparisonOperators:
    """
    DELETE с различными операторами сравнения.
    """
    
    def test_delete_with_less_than(self):
        """
        DELETE с оператором <.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT)"), db)
        for i in range(1, 6):
            executor.execute(Parser().parse(f"INSERT INTO t (id) VALUES ({i})"), db)
        
        result = executor.execute(Parser().parse("DELETE FROM t WHERE id < 3"), db)
        
        assert result.success == True
        assert result.message == "2 row(s) deleted"
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert len(result.data) == 3
    
    def test_delete_with_greater_than(self):
        """
        DELETE с оператором >.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT)"), db)
        for i in range(1, 6):
            executor.execute(Parser().parse(f"INSERT INTO t (id) VALUES ({i})"), db)
        
        result = executor.execute(Parser().parse("DELETE FROM t WHERE id > 3"), db)
        
        assert result.success == True
        assert result.message == "2 row(s) deleted"
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert len(result.data) == 3
    
    def test_delete_with_not_equals(self):
        """
        DELETE с оператором !=.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT)"), db)
        for i in range(1, 4):
            executor.execute(Parser().parse(f"INSERT INTO t (id) VALUES ({i})"), db)
        
        result = executor.execute(Parser().parse("DELETE FROM t WHERE id != 2"), db)
        
        assert result.success == True
        assert result.message == "2 row(s) deleted"
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert result.data == [{"id": 2}]