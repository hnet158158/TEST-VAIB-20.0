# START_MODULE_CONTRACT
# Module: tests.test_update
# Intent: Тесты для UPDATE операций (парсинг и выполнение).
#         UPDATE с WHERE, без WHERE, несколько колонок.
# END_MODULE_CONTRACT

import pytest

from mini_db.ast.nodes import UpdateNode, LiteralNode, ComparisonNode, IdentifierNode
from mini_db.executor.executor import Executor
from mini_db.parser.parser import Parser
from mini_db.storage.database import Database


# ==================== UPDATE PARSER TESTS ====================

class TestUpdateParser:
    """
    Тесты парсинга UPDATE statements.
    """
    
    def test_parse_update_simple(self):
        """
        Парсинг простого UPDATE без WHERE.
        """
        parser = Parser()
        ast = parser.parse("UPDATE t SET val = 'x'")
        
        assert isinstance(ast, UpdateNode)
        assert ast.table == "t"
        assert ast.assignments == {"val": "x"}
        assert ast.where is None
    
    def test_parse_update_multiple_columns(self):
        """
        Парсинг UPDATE с несколькими колонками.
        """
        parser = Parser()
        ast = parser.parse("UPDATE t SET id = 1, val = 'x', active = true")
        
        assert isinstance(ast, UpdateNode)
        assert ast.assignments == {"id": 1, "val": "x", "active": True}
    
    def test_parse_update_with_where(self):
        """
        Парсинг UPDATE с WHERE.
        """
        parser = Parser()
        ast = parser.parse("UPDATE t SET val = 'x' WHERE id = 1")
        
        assert isinstance(ast, UpdateNode)
        assert ast.table == "t"
        assert ast.assignments == {"val": "x"}
        assert ast.where is not None
        assert isinstance(ast.where, ComparisonNode)
    
    def test_parse_update_with_complex_where(self):
        """
        Парсинг UPDATE со сложным WHERE.
        """
        parser = Parser()
        ast = parser.parse("UPDATE t SET val = 'x' WHERE id > 1 AND active = true")
        
        assert isinstance(ast, UpdateNode)
        assert ast.where is not None
    
    def test_parse_update_with_null(self):
        """
        Парсинг UPDATE с NULL значением.
        """
        parser = Parser()
        ast = parser.parse("UPDATE t SET val = NULL")
        
        assert isinstance(ast, UpdateNode)
        assert ast.assignments == {"val": None}
    
    def test_parse_update_with_int_value(self):
        """
        Парсинг UPDATE с INT значением.
        """
        parser = Parser()
        ast = parser.parse("UPDATE t SET id = 42")
        
        assert isinstance(ast, UpdateNode)
        assert ast.assignments == {"id": 42}
        assert isinstance(ast.assignments["id"], int)
    
    def test_parse_update_with_bool_value(self):
        """
        Парсинг UPDATE с BOOL значением.
        """
        parser = Parser()
        ast = parser.parse("UPDATE t SET active = false")
        
        assert isinstance(ast, UpdateNode)
        assert ast.assignments == {"active": False}
        assert isinstance(ast.assignments["active"], bool)


# ==================== UPDATE WITHOUT WHERE ====================

class TestUpdateWithoutWhere:
    """
    UPDATE без WHERE - обновляет все строки.
    """
    
    def test_update_all_rows_single_column(self):
        """
        UPDATE всех строк одной колонки.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (2, 'b')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (3, 'c')"), db)
        
        result = executor.execute(Parser().parse("UPDATE t SET val = 'x'"), db)
        
        assert result.success == True
        assert result.message == "3 row(s) updated"
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert result.data == [
            {"id": 1, "val": "x"},
            {"id": 2, "val": "x"},
            {"id": 3, "val": "x"}
        ]
    
    def test_update_all_rows_multiple_columns(self):
        """
        UPDATE всех строк нескольких колонок.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT, active BOOL)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val, active) VALUES (1, 'a', false)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val, active) VALUES (2, 'b', false)"), db)
        
        result = executor.execute(Parser().parse("UPDATE t SET val = 'x', active = true"), db)
        
        assert result.success == True
        assert result.message == "2 row(s) updated"
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert result.data == [
            {"id": 1, "val": "x", "active": True},
            {"id": 2, "val": "x", "active": True}
        ]
    
    def test_update_all_rows_empty_table(self):
        """
        UPDATE на пустой таблице.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        
        result = executor.execute(Parser().parse("UPDATE t SET val = 'x'"), db)
        
        assert result.success == True
        assert result.message == "0 row(s) updated"


# ==================== UPDATE WITH WHERE ====================

class TestUpdateWithWhere:
    """
    UPDATE с условием WHERE.
    """
    
    def test_update_with_where_single_row(self):
        """
        UPDATE одной строки по условию.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (2, 'b')"), db)
        
        result = executor.execute(Parser().parse("UPDATE t SET val = 'x' WHERE id = 1"), db)
        
        assert result.success == True
        assert result.message == "1 row(s) updated"
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert result.data == [
            {"id": 1, "val": "x"},
            {"id": 2, "val": "b"}
        ]
    
    def test_update_with_where_multiple_rows(self):
        """
        UPDATE нескольких строк по условию.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (2, 'a')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (3, 'b')"), db)
        
        result = executor.execute(Parser().parse("UPDATE t SET val = 'x' WHERE val = 'a'"), db)
        
        assert result.success == True
        assert result.message == "2 row(s) updated"
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert result.data == [
            {"id": 1, "val": "x"},
            {"id": 2, "val": "x"},
            {"id": 3, "val": "b"}
        ]
    
    def test_update_with_where_no_match(self):
        """
        UPDATE с условием, которое не находит строк.
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
    
    def test_update_with_complex_where(self):
        """
        UPDATE с сложным WHERE (AND, OR).
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT, active BOOL)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val, active) VALUES (1, 'a', true)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val, active) VALUES (2, 'b', false)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val, active) VALUES (3, 'c', true)"), db)
        
        # Update where id > 1 AND active = true
        result = executor.execute(
            Parser().parse("UPDATE t SET val = 'x' WHERE id > 1 AND active = true"),
            db
        )
        
        assert result.success == True
        assert result.message == "1 row(s) updated"
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert {"id": 1, "val": "a", "active": True} in result.data
        assert {"id": 2, "val": "b", "active": False} in result.data
        assert {"id": 3, "val": "x", "active": True} in result.data


# ==================== UPDATE WITH COMPARISON OPERATORS ====================

class TestUpdateWithComparisonOperators:
    """
    UPDATE с различными операторами сравнения в WHERE.
    """
    
    def test_update_with_less_than(self):
        """
        UPDATE с оператором <.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        for i in range(1, 6):
            executor.execute(Parser().parse(f"INSERT INTO t (id, val) VALUES ({i}, 'a')"), db)
        
        result = executor.execute(Parser().parse("UPDATE t SET val = 'x' WHERE id < 3"), db)
        
        assert result.success == True
        assert result.message == "2 row(s) updated"
    
    def test_update_with_greater_than(self):
        """
        UPDATE с оператором >.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        for i in range(1, 6):
            executor.execute(Parser().parse(f"INSERT INTO t (id, val) VALUES ({i}, 'a')"), db)
        
        result = executor.execute(Parser().parse("UPDATE t SET val = 'x' WHERE id > 3"), db)
        
        assert result.success == True
        assert result.message == "2 row(s) updated"
    
    def test_update_with_not_equals(self):
        """
        UPDATE с оператором !=.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        for i in range(1, 4):
            executor.execute(Parser().parse(f"INSERT INTO t (id, val) VALUES ({i}, 'a')"), db)
        
        result = executor.execute(Parser().parse("UPDATE t SET val = 'x' WHERE id != 2"), db)
        
        assert result.success == True
        assert result.message == "2 row(s) updated"
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert {"id": 1, "val": "x"} in result.data
        assert {"id": 2, "val": "a"} in result.data
        assert {"id": 3, "val": "x"} in result.data


# ==================== UPDATE WITH NULL ====================

class TestUpdateWithNull:
    """
    UPDATE с NULL значениями.
    """
    
    def test_update_to_null(self):
        """
        UPDATE колонки в NULL.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        
        result = executor.execute(Parser().parse("UPDATE t SET val = NULL"), db)
        
        assert result.success == True
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert result.data == [{"id": 1, "val": None}]
    
    def test_update_from_null(self):
        """
        UPDATE строки, где колонка NULL.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id) VALUES (1)"), db)  # val is NULL
        
        result = executor.execute(Parser().parse("UPDATE t SET val = 'x'"), db)
        
        assert result.success == True
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        assert result.data == [{"id": 1, "val": "x"}]


# ==================== UPDATE PRESERVES ORDER ====================

class TestUpdatePreservesOrder:
    """
    UPDATE сохраняет порядок строк.
    """
    
    def test_update_preserves_insertion_order(self):
        """
        После UPDATE порядок строк должен сохраниться.
        """
        db = Database()
        executor = Executor()
        
        executor.execute(Parser().parse("CREATE TABLE t (id INT, val TEXT)"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (1, 'a')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (2, 'b')"), db)
        executor.execute(Parser().parse("INSERT INTO t (id, val) VALUES (3, 'c')"), db)
        
        # Update middle row
        executor.execute(Parser().parse("UPDATE t SET val = 'x' WHERE id = 2"), db)
        
        result = executor.execute(Parser().parse("SELECT * FROM t"), db)
        
        # Order should be preserved
        assert result.data == [
            {"id": 1, "val": "a"},
            {"id": 2, "val": "x"},
            {"id": 3, "val": "c"}
        ]