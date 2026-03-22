# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_subqueries_phase11
# Intent: Comprehensive tests for Phase 11 Subqueries.
# Dependencies: unittest, mini_db_v2.*
# END_MODULE_CONTRACT

"""
Phase 11 Subqueries Test Suite

Done Criteria:
- [ ] Scalar subquery работает (возвращает одно значение)
- [ ] IN/NOT IN с subquery работает
- [ ] Correlated subquery работает
- [ ] EXISTS/NOT EXISTS работают
- [ ] Derived tables в FROM работают
- [ ] NULL handling в subqueries корректен
"""

import unittest
from typing import Any

from mini_db_v2.ast.nodes import (
    SelectNode, InsertNode, CreateTableNode, ColumnDef, DataType,
    ExpressionNode, LiteralNode, ColumnRef, BinaryOpNode, BinaryOperator,
    SubqueryNode, ExistsNode, SelectColumn, FromClause, TableRef,
    StarColumn, UnaryOpNode, UnaryOperator
)
from mini_db_v2.executor.executor import Executor, ExecutionResult
from mini_db_v2.executor.subqueries import (
    SubqueryExecutor, SubqueryContext, SubqueryError, ScalarSubqueryError,
    CorrelatedSubqueryError
)
from mini_db_v2.storage.database import Database
from mini_db_v2.storage.table import Table, ColumnDef as StorageColumnDef, DataType as StorageDataType


# =============================================================================
# Test Data Helpers
# =============================================================================

def create_test_table(db: Database, name: str, columns: dict, rows: list[dict]) -> Table:
    """Создаёт тестовую таблицу с данными."""
    storage_columns = {}
    for col_name, col_type in columns.items():
        storage_columns[col_name] = StorageColumnDef(
            name=col_name,
            data_type=col_type,
            nullable=True
        )
    
    table = db.create_table(name, storage_columns)
    
    for row in rows:
        table.insert(row)
    
    return table


def create_test_database() -> Database:
    """Создаёт тестовую базу данных с таблицами для subquery тестов."""
    db = Database()
    
    # Employees table
    create_test_table(db, "employees", {
        "id": StorageDataType.INT,
        "name": StorageDataType.TEXT,
        "department_id": StorageDataType.INT,
        "salary": StorageDataType.REAL
    }, [
        {"id": 1, "name": "Alice", "department_id": 1, "salary": 100000.0},
        {"id": 2, "name": "Bob", "department_id": 1, "salary": 80000.0},
        {"id": 3, "name": "Charlie", "department_id": 2, "salary": 90000.0},
        {"id": 4, "name": "David", "department_id": 2, "salary": 75000.0},
        {"id": 5, "name": "Eve", "department_id": None, "salary": 60000.0},  # NULL department
        {"id": 6, "name": "Frank", "department_id": 3, "salary": 120000.0},
    ])
    
    # Departments table
    create_test_table(db, "departments", {
        "id": StorageDataType.INT,
        "name": StorageDataType.TEXT,
        "budget": StorageDataType.REAL
    }, [
        {"id": 1, "name": "Engineering", "budget": 500000.0},
        {"id": 2, "name": "Sales", "budget": 300000.0},
        {"id": 3, "name": "Marketing", "budget": 200000.0},
    ])
    
    # Projects table
    create_test_table(db, "projects", {
        "id": StorageDataType.INT,
        "name": StorageDataType.TEXT,
        "department_id": StorageDataType.INT
    }, [
        {"id": 1, "name": "Project A", "department_id": 1},
        {"id": 2, "name": "Project B", "department_id": 1},
        {"id": 3, "name": "Project C", "department_id": 2},
    ])
    
    # Empty table for edge cases
    create_test_table(db, "empty_table", {
        "id": StorageDataType.INT,
        "value": StorageDataType.TEXT
    }, [])
    
    return db


# =============================================================================
# Test SubqueryExecutor - Scalar Subquery
# =============================================================================

class TestScalarSubquery(unittest.TestCase):
    """Тесты scalar subquery (возвращает одно значение)."""
    
    def setUp(self):
        self.db = create_test_database()
        self.executor = Executor(self.db)
        self.subquery_executor = SubqueryExecutor(self.executor)
    
    def test_scalar_subquery_returns_single_value(self):
        """Scalar subquery возвращает одно значение."""
        # SELECT MAX(salary) FROM employees - using aggregation
        from mini_db_v2.ast.nodes import AggregateNode, AggregateType
        
        subquery = SelectNode(
            columns=[SelectColumn(
                expression=AggregateNode(agg_type=AggregateType.MAX, arg=ColumnRef(column_name="salary"))
            )],
            from_clause=FromClause(table=TableRef(table_name="employees"))
        )
        
        result = self.subquery_executor.execute_scalar(subquery)
        
        # Should return max salary value
        self.assertIsNotNone(result)
        self.assertEqual(result, 120000.0)
    
    def test_scalar_subquery_empty_returns_null(self):
        """Scalar subquery возвращает NULL если результат пустой."""
        # SELECT * FROM empty_table
        subquery = SelectNode(
            columns=[SelectColumn(expression=StarColumn())],
            from_clause=FromClause(table=TableRef(table_name="empty_table"))
        )
        
        result = self.subquery_executor.execute_scalar(subquery)
        
        # Empty subquery returns None (NULL)
        self.assertIsNone(result)
    
    def test_scalar_subquery_multiple_rows_raises_error(self):
        """Scalar subquery с несколькими строками выбрасывает ошибку."""
        # SELECT id FROM employees (returns multiple rows)
        subquery = SelectNode(
            columns=[SelectColumn(expression=ColumnRef(column_name="id"))],
            from_clause=FromClause(table=TableRef(table_name="employees"))
        )
        
        with self.assertRaises(ScalarSubqueryError):
            self.subquery_executor.execute_scalar(subquery)
    
    def test_scalar_subquery_in_select_list(self):
        """Scalar subquery в SELECT list через executor."""
        # SELECT name, (SELECT MAX(salary) FROM employees) as max_sal FROM employees
        # Simplified: just test that subquery expression works
        subquery = SelectNode(
            columns=[SelectColumn(expression=ColumnRef(column_name="salary"))],
            from_clause=FromClause(table=TableRef(table_name="employees")),
            limit=1
        )
        
        result = self.subquery_executor.execute_scalar(subquery)
        
        # Should return a salary value
        self.assertIsNotNone(result)


# =============================================================================
# Test SubqueryExecutor - IN/NOT IN Subquery
# =============================================================================

class TestInSubquery(unittest.TestCase):
    """Тесты IN/NOT IN с subquery."""
    
    def setUp(self):
        self.db = create_test_database()
        self.executor = Executor(self.db)
        self.subquery_executor = SubqueryExecutor(self.executor)
    
    def test_in_subquery_basic(self):
        """IN (SELECT ...) работает."""
        # SELECT id FROM departments WHERE id IN (1, 2)
        # Simplified: test IN with subquery
        subquery = SelectNode(
            columns=[SelectColumn(expression=ColumnRef(column_name="id"))],
            from_clause=FromClause(table=TableRef(table_name="departments")),
            where=BinaryOpNode(
                left=ColumnRef(column_name="id"),
                operator=BinaryOperator.LE,
                right=LiteralNode(value=2, data_type=DataType.INT)
            )
        )
        
        # Check if 1 is in the result
        result = self.subquery_executor.execute_in(1, subquery)
        
        self.assertTrue(result)
    
    def test_in_subquery_not_found(self):
        """IN (SELECT ...) возвращает False если значения нет."""
        subquery = SelectNode(
            columns=[SelectColumn(expression=ColumnRef(column_name="id"))],
            from_clause=FromClause(table=TableRef(table_name="departments")),
            where=BinaryOpNode(
                left=ColumnRef(column_name="id"),
                operator=BinaryOperator.EQ,
                right=LiteralNode(value=1, data_type=DataType.INT)
            )
        )
        
        # 99 is not in departments
        result = self.subquery_executor.execute_in(99, subquery)
        
        self.assertFalse(result)
    
    def test_not_in_subquery(self):
        """NOT IN (SELECT ...) работает."""
        subquery = SelectNode(
            columns=[SelectColumn(expression=ColumnRef(column_name="id"))],
            from_clause=FromClause(table=TableRef(table_name="departments")),
            where=BinaryOpNode(
                left=ColumnRef(column_name="id"),
                operator=BinaryOperator.EQ,
                right=LiteralNode(value=1, data_type=DataType.INT)
            )
        )
        
        # 99 NOT IN (1) should be True
        result = self.subquery_executor.execute_in(99, subquery, negated=True)
        
        self.assertTrue(result)
    
    def test_in_subquery_empty_result(self):
        """IN (SELECT ...) с пустым результатом возвращает False."""
        subquery = SelectNode(
            columns=[SelectColumn(expression=ColumnRef(column_name="id"))],
            from_clause=FromClause(table=TableRef(table_name="empty_table"))
        )
        
        result = self.subquery_executor.execute_in(1, subquery)
        
        # 1 IN empty set = False
        self.assertFalse(result)
    
    def test_in_subquery_with_null_value(self):
        """NULL IN (SELECT ...) возвращает NULL (None)."""
        subquery = SelectNode(
            columns=[SelectColumn(expression=ColumnRef(column_name="id"))],
            from_clause=FromClause(table=TableRef(table_name="departments"))
        )
        
        result = self.subquery_executor.execute_in(None, subquery)
        
        # NULL IN (...) is NULL (unknown)
        self.assertIsNone(result)
    
    def test_in_subquery_with_null_in_result(self):
        """IN (SELECT ...) с NULL в результате."""
        # Subquery returns NULL values
        subquery = SelectNode(
            columns=[SelectColumn(expression=ColumnRef(column_name="department_id"))],
            from_clause=FromClause(table=TableRef(table_name="employees")),
            where=BinaryOpNode(
                left=ColumnRef(column_name="name"),
                operator=BinaryOperator.EQ,
                right=LiteralNode(value="Eve", data_type=DataType.TEXT)
            )
        )
        
        # 1 IN (NULL) - should work but with NULL handling
        result = self.subquery_executor.execute_in(1, subquery)
        
        # Result depends on whether NULL is in the set
        # If value not found and NULL present, result is NULL (unknown)
        self.assertIsNone(result)


# =============================================================================
# Test SubqueryExecutor - EXISTS/NOT EXISTS
# =============================================================================

class TestExistsSubquery(unittest.TestCase):
    """Тесты EXISTS/NOT EXISTS."""
    
    def setUp(self):
        self.db = create_test_database()
        self.executor = Executor(self.db)
        self.subquery_executor = SubqueryExecutor(self.executor)
    
    def test_exists_returns_true_when_rows_exist(self):
        """EXISTS возвращает True когда есть строки."""
        subquery = SelectNode(
            columns=[SelectColumn(expression=LiteralNode(value=1, data_type=DataType.INT))],
            from_clause=FromClause(table=TableRef(table_name="employees")),
            where=BinaryOpNode(
                left=ColumnRef(column_name="id"),
                operator=BinaryOperator.EQ,
                right=LiteralNode(value=1, data_type=DataType.INT)
            )
        )
        
        result = self.subquery_executor.execute_exists(subquery)
        
        self.assertTrue(result)
    
    def test_exists_returns_false_when_no_rows(self):
        """EXISTS возвращает False когда нет строк."""
        subquery = SelectNode(
            columns=[SelectColumn(expression=LiteralNode(value=1, data_type=DataType.INT))],
            from_clause=FromClause(table=TableRef(table_name="employees")),
            where=BinaryOpNode(
                left=ColumnRef(column_name="id"),
                operator=BinaryOperator.EQ,
                right=LiteralNode(value=999, data_type=DataType.INT)
            )
        )
        
        result = self.subquery_executor.execute_exists(subquery)
        
        self.assertFalse(result)
    
    def test_exists_empty_table(self):
        """EXISTS с пустой таблицей возвращает False."""
        subquery = SelectNode(
            columns=[SelectColumn(expression=LiteralNode(value=1, data_type=DataType.INT))],
            from_clause=FromClause(table=TableRef(table_name="empty_table"))
        )
        
        result = self.subquery_executor.execute_exists(subquery)
        
        self.assertFalse(result)
    
    def test_not_exists_returns_true_when_no_rows(self):
        """NOT EXISTS возвращает True когда нет строк."""
        subquery = SelectNode(
            columns=[SelectColumn(expression=LiteralNode(value=1, data_type=DataType.INT))],
            from_clause=FromClause(table=TableRef(table_name="employees")),
            where=BinaryOpNode(
                left=ColumnRef(column_name="id"),
                operator=BinaryOperator.EQ,
                right=LiteralNode(value=999, data_type=DataType.INT)
            )
        )
        
        result = self.subquery_executor.execute_exists(subquery, negated=True)
        
        self.assertTrue(result)
    
    def test_not_exists_returns_false_when_rows_exist(self):
        """NOT EXISTS возвращает False когда есть строки."""
        subquery = SelectNode(
            columns=[SelectColumn(expression=LiteralNode(value=1, data_type=DataType.INT))],
            from_clause=FromClause(table=TableRef(table_name="employees")),
            where=BinaryOpNode(
                left=ColumnRef(column_name="id"),
                operator=BinaryOperator.EQ,
                right=LiteralNode(value=1, data_type=DataType.INT)
            )
        )
        
        result = self.subquery_executor.execute_exists(subquery, negated=True)
        
        self.assertFalse(result)


# =============================================================================
# Test SubqueryExecutor - Correlated Subquery
# =============================================================================

class TestCorrelatedSubquery(unittest.TestCase):
    """Тесты correlated subquery."""
    
    def setUp(self):
        self.db = create_test_database()
        self.executor = Executor(self.db)
        self.subquery_executor = SubqueryExecutor(self.executor)
    
    def test_correlated_subquery_basic(self):
        """Correlated subquery ссылается на outer query."""
        # Outer row: employee with id=1, department_id=1
        outer_row = {"id": 1, "department_id": 1, "name": "Alice"}
        
        # Subquery: SELECT name FROM departments WHERE id = outer.department_id
        subquery = SelectNode(
            columns=[SelectColumn(expression=ColumnRef(column_name="name"))],
            from_clause=FromClause(table=TableRef(table_name="departments")),
            where=BinaryOpNode(
                left=ColumnRef(column_name="id"),
                operator=BinaryOperator.EQ,
                right=ColumnRef(column_name="department_id", table_alias="outer")
            ),
            limit=1
        )
        
        context = SubqueryContext(outer_row=outer_row, outer_alias="outer")
        
        # Execute correlated subquery
        result = self.subquery_executor.execute_scalar(subquery, context)
        
        # Should return department name for department_id=1
        # Note: This tests the context resolution mechanism
        self.assertIsNotNone(result)
    
    def test_correlated_subquery_with_parent_context(self):
        """Correlated subquery с parent context (nested subqueries)."""
        outer_row = {"id": 1, "department_id": 1}
        parent_context = SubqueryContext(outer_row={"parent_value": 100})
        
        context = SubqueryContext(
            outer_row=outer_row,
            parent_context=parent_context
        )
        
        # Should have access to both contexts
        self.assertIsNotNone(context.outer_row)
        self.assertIsNotNone(context.parent_context)
    
    def test_subquery_context_resolve_column(self):
        """SubqueryContext.resolve_column работает."""
        context = SubqueryContext(
            outer_row={"id": 1, "name": "Alice", "t.department_id": 1}
        )
        
        # Direct column
        col_ref = ColumnRef(column_name="id")
        result = context.resolve_column(col_ref)
        self.assertEqual(result, 1)
        
        # Column with prefix
        col_ref_prefixed = ColumnRef(column_name="department_id", table_alias="t")
        result = context.resolve_column(col_ref_prefixed)
        self.assertEqual(result, 1)
    
    def test_correlated_subquery_execute(self):
        """execute_correlated метод работает."""
        outer_row = {"id": 1, "department_id": 1}
        
        # Simple subquery
        subquery = SelectNode(
            columns=[SelectColumn(expression=LiteralNode(value=42, data_type=DataType.INT))],
            from_clause=None
        )
        
        result = self.subquery_executor.execute_correlated(subquery, outer_row)
        
        self.assertEqual(result, 42)


# =============================================================================
# Test SubqueryExecutor - Derived Tables
# =============================================================================

class TestDerivedTables(unittest.TestCase):
    """Тесты derived tables (subquery в FROM)."""
    
    def setUp(self):
        self.db = create_test_database()
        self.executor = Executor(self.db)
        self.subquery_executor = SubqueryExecutor(self.executor)
    
    def test_derived_table_basic(self):
        """Derived table в FROM работает."""
        # SELECT * FROM (SELECT id, name FROM employees) AS emp
        subquery = SelectNode(
            columns=[
                SelectColumn(expression=ColumnRef(column_name="id")),
                SelectColumn(expression=ColumnRef(column_name="name"))
            ],
            from_clause=FromClause(table=TableRef(table_name="employees"))
        )
        
        result = self.subquery_executor.execute_derived_table(subquery, "emp")
        
        # Should return rows with prefixed column names
        self.assertGreater(len(result), 0)
        
        # Check column prefix
        first_row = result[0]
        self.assertIn("emp.id", first_row)
        self.assertIn("emp.name", first_row)
    
    def test_derived_table_with_limit(self):
        """Derived table с LIMIT."""
        subquery = SelectNode(
            columns=[SelectColumn(expression=ColumnRef(column_name="id"))],
            from_clause=FromClause(table=TableRef(table_name="employees")),
            limit=2
        )
        
        result = self.subquery_executor.execute_derived_table(subquery, "t")
        
        self.assertEqual(len(result), 2)
    
    def test_derived_table_empty_result(self):
        """Derived table с пустым результатом."""
        subquery = SelectNode(
            columns=[SelectColumn(expression=ColumnRef(column_name="id"))],
            from_clause=FromClause(table=TableRef(table_name="empty_table"))
        )
        
        result = self.subquery_executor.execute_derived_table(subquery, "empty")
        
        self.assertEqual(len(result), 0)


# =============================================================================
# Test SubqueryContext
# =============================================================================

class TestSubqueryContext(unittest.TestCase):
    """Тесты SubqueryContext."""
    
    def test_context_creation(self):
        """Создание контекста."""
        context = SubqueryContext(
            outer_row={"id": 1, "name": "Alice"},
            outer_alias="e",
            outer_table_name="employees"
        )
        
        self.assertEqual(context.outer_row["id"], 1)
        self.assertEqual(context.outer_alias, "e")
    
    def test_resolve_column_direct(self):
        """Разрешение колонки напрямую."""
        context = SubqueryContext(outer_row={"id": 1, "name": "Alice"})
        
        result = context.resolve_column(ColumnRef(column_name="id"))
        self.assertEqual(result, 1)
    
    def test_resolve_column_with_alias(self):
        """Разрешение колонки с алиасом таблицы."""
        context = SubqueryContext(
            outer_row={"e.id": 1, "e.name": "Alice"},
            outer_alias="e"
        )
        
        result = context.resolve_column(ColumnRef(column_name="id", table_alias="e"))
        self.assertEqual(result, 1)
    
    def test_resolve_column_not_found(self):
        """Разрешение несуществующей колонки."""
        context = SubqueryContext(outer_row={"id": 1})
        
        result = context.resolve_column(ColumnRef(column_name="nonexistent"))
        self.assertIsNone(result)
    
    def test_parent_context_resolution(self):
        """Разрешение через parent context."""
        parent = SubqueryContext(outer_row={"parent_id": 100})
        child = SubqueryContext(outer_row={"child_id": 200}, parent_context=parent)
        
        # Resolve from child
        result = child.resolve_column(ColumnRef(column_name="child_id"))
        self.assertEqual(result, 200)
        
        # Resolve from parent
        result = child.resolve_column(ColumnRef(column_name="parent_id"))
        self.assertEqual(result, 100)


# =============================================================================
# Test Errors
# =============================================================================

class TestSubqueryErrors(unittest.TestCase):
    """Тесты ошибок subquery."""
    
    def test_subquery_error_base(self):
        """Базовая ошибка SubqueryError."""
        with self.assertRaises(SubqueryError):
            raise SubqueryError("Test error")
    
    def test_scalar_subquery_error(self):
        """Ошибка ScalarSubqueryError."""
        with self.assertRaises(ScalarSubqueryError):
            raise ScalarSubqueryError("Scalar subquery error")
    
    def test_correlated_subquery_error(self):
        """Ошибка CorrelatedSubqueryError."""
        with self.assertRaises(CorrelatedSubqueryError):
            raise CorrelatedSubqueryError("Correlated subquery error")
    
    def test_error_inheritance(self):
        """Наследование ошибок."""
        self.assertTrue(issubclass(ScalarSubqueryError, SubqueryError))
        self.assertTrue(issubclass(CorrelatedSubqueryError, SubqueryError))


# =============================================================================
# Test Integration with Executor
# =============================================================================

class TestSubqueryIntegration(unittest.TestCase):
    """Интеграционные тесты subquery через Executor."""
    
    def setUp(self):
        self.db = create_test_database()
        self.executor = Executor(self.db)
    
    def test_subquery_in_where(self):
        """Subquery в WHERE через executor."""
        # SELECT * FROM employees WHERE department_id IN (SELECT id FROM departments)
        # Simplified: test that executor handles subquery expressions
        subquery = SubqueryNode(
            query=SelectNode(
                columns=[SelectColumn(expression=ColumnRef(column_name="id"))],
                from_clause=FromClause(table=TableRef(table_name="departments")),
                limit=1
            ),
            subquery_type="scalar"
        )
        
        # Evaluate subquery expression
        result = self.executor._evaluate_expression(subquery, {})
        
        # Should return a value
        self.assertIsNotNone(result)
    
    def test_exists_in_where(self):
        """EXISTS в WHERE через executor."""
        exists_node = ExistsNode(
            subquery=SubqueryNode(
                query=SelectNode(
                    columns=[SelectColumn(expression=LiteralNode(value=1, data_type=DataType.INT))],
                    from_clause=FromClause(table=TableRef(table_name="employees")),
                    where=BinaryOpNode(
                        left=ColumnRef(column_name="id"),
                        operator=BinaryOperator.EQ,
                        right=LiteralNode(value=1, data_type=DataType.INT)
                    )
                ),
                subquery_type="exists"
            ),
            negated=False
        )
        
        result = self.executor._evaluate_expression(exists_node, {})
        
        self.assertTrue(result)
    
    def test_not_exists_in_where(self):
        """NOT EXISTS в WHERE через executor."""
        exists_node = ExistsNode(
            subquery=SubqueryNode(
                query=SelectNode(
                    columns=[SelectColumn(expression=LiteralNode(value=1, data_type=DataType.INT))],
                    from_clause=FromClause(table=TableRef(table_name="employees")),
                    where=BinaryOpNode(
                        left=ColumnRef(column_name="id"),
                        operator=BinaryOperator.EQ,
                        right=LiteralNode(value=999, data_type=DataType.INT)
                    )
                ),
                subquery_type="exists"
            ),
            negated=True
        )
        
        result = self.executor._evaluate_expression(exists_node, {})
        
        self.assertTrue(result)


# =============================================================================
# Test Adversarial Cases
# =============================================================================

class TestSubqueryAdversarial(unittest.TestCase):
    """Адверсарные тесты subquery."""
    
    def setUp(self):
        self.db = create_test_database()
        self.executor = Executor(self.db)
        self.subquery_executor = SubqueryExecutor(self.executor)
    
    def test_null_handling_in_scalar_subquery(self):
        """NULL handling в scalar subquery."""
        # Subquery that returns NULL
        subquery = SelectNode(
            columns=[SelectColumn(expression=ColumnRef(column_name="department_id"))],
            from_clause=FromClause(table=TableRef(table_name="employees")),
            where=BinaryOpNode(
                left=ColumnRef(column_name="name"),
                operator=BinaryOperator.EQ,
                right=LiteralNode(value="Eve", data_type=DataType.TEXT)
            )
        )
        
        result = self.subquery_executor.execute_scalar(subquery)
        
        # Eve has NULL department_id
        self.assertIsNone(result)
    
    def test_large_subquery_result(self):
        """Subquery с большим результатом."""
        # Create table with many rows
        large_table_columns = {
            "id": StorageDataType.INT,
            "value": StorageDataType.TEXT
        }
        
        storage_columns = {
            name: StorageColumnDef(name=name, data_type=dt, nullable=True)
            for name, dt in large_table_columns.items()
        }
        
        large_table = self.db.create_table("large_table", storage_columns)
        
        for i in range(100):
            large_table.insert({"id": i, "value": f"value_{i}"})
        
        # Subquery returning many rows
        subquery = SelectNode(
            columns=[SelectColumn(expression=ColumnRef(column_name="id"))],
            from_clause=FromClause(table=TableRef(table_name="large_table"))
        )
        
        # Should raise error for scalar subquery with multiple rows
        with self.assertRaises(ScalarSubqueryError):
            self.subquery_executor.execute_scalar(subquery)
    
    def test_nested_subqueries(self):
        """Вложенные subqueries."""
        # Create nested context
        outer_context = SubqueryContext(outer_row={"outer_id": 1})
        inner_context = SubqueryContext(
            outer_row={"inner_id": 2},
            parent_context=outer_context
        )
        
        # Should resolve from both levels
        self.assertEqual(inner_context.resolve_column(ColumnRef(column_name="inner_id")), 2)
        self.assertEqual(inner_context.resolve_column(ColumnRef(column_name="outer_id")), 1)
    
    def test_empty_column_list(self):
        """Subquery с пустым списком колонок."""
        # Empty table with empty columns
        subquery = SelectNode(
            columns=[],
            from_clause=FromClause(table=TableRef(table_name="empty_table"))
        )
        
        # Should handle gracefully - empty table returns None
        result = self.subquery_executor.execute_scalar(subquery)
        self.assertIsNone(result)
    
    def test_subquery_with_aggregation(self):
        """Subquery с агрегацией."""
        # SELECT MAX(salary) FROM employees
        from mini_db_v2.ast.nodes import AggregateNode, AggregateType
        
        subquery = SelectNode(
            columns=[SelectColumn(
                expression=AggregateNode(agg_type=AggregateType.MAX, arg=ColumnRef(column_name="salary"))
            )],
            from_clause=FromClause(table=TableRef(table_name="employees"))
        )
        
        # Should work with aggregation
        result = self.subquery_executor.execute_scalar(subquery)
        
        # Max salary is 120000.0
        self.assertEqual(result, 120000.0)


# =============================================================================
# Test Performance
# =============================================================================

class TestSubqueryPerformance(unittest.TestCase):
    """Тесты производительности subquery."""
    
    def setUp(self):
        self.db = Database()
        
        # Create tables with data
        storage_columns = {
            "id": StorageColumnDef(name="id", data_type=StorageDataType.INT, nullable=True),
            "value": StorageColumnDef(name="value", data_type=StorageDataType.TEXT, nullable=True)
        }
        
        self.table = self.db.create_table("test_table", storage_columns)
        
        for i in range(100):
            self.table.insert({"id": i, "value": f"value_{i}"})
        
        self.executor = Executor(self.db)
        self.subquery_executor = SubqueryExecutor(self.executor)
    
    def test_exists_performance(self):
        """EXISTS должен быть быстрым (прекращает после первой строки)."""
        import time
        
        subquery = SelectNode(
            columns=[SelectColumn(expression=LiteralNode(value=1, data_type=DataType.INT))],
            from_clause=FromClause(table=TableRef(table_name="test_table"))
        )
        
        start = time.time()
        result = self.subquery_executor.execute_exists(subquery)
        elapsed = time.time() - start
        
        self.assertTrue(result)
        self.assertLess(elapsed, 0.1)  # Should be fast
    
    def test_in_subquery_performance(self):
        """IN subquery с большим результатом."""
        import time
        
        subquery = SelectNode(
            columns=[SelectColumn(expression=ColumnRef(column_name="id"))],
            from_clause=FromClause(table=TableRef(table_name="test_table"))
        )
        
        start = time.time()
        result = self.subquery_executor.execute_in(50, subquery)
        elapsed = time.time() - start
        
        self.assertTrue(result)
        self.assertLess(elapsed, 0.1)


# =============================================================================
# Test Checkpoints
# =============================================================================

class TestSubqueryCheckpoints(unittest.TestCase):
    """Checkpoint тесты для Phase 11."""
    
    def setUp(self):
        self.db = create_test_database()
        self.executor = Executor(self.db)
        self.subquery_executor = SubqueryExecutor(self.executor)
    
    def test_checkpoint1_scalar_subquery(self):
        """Checkpoint #1: Scalar subquery возвращает одно значение."""
        from mini_db_v2.ast.nodes import AggregateNode, AggregateType
        
        subquery = SelectNode(
            columns=[SelectColumn(
                expression=AggregateNode(agg_type=AggregateType.MAX, arg=ColumnRef(column_name="salary"))
            )],
            from_clause=FromClause(table=TableRef(table_name="employees"))
        )
        
        result = self.subquery_executor.execute_scalar(subquery)
        
        # Max salary is 120000.0
        self.assertEqual(result, 120000.0)
    
    def test_checkpoint2_in_subquery(self):
        """Checkpoint #2: IN/NOT IN работает корректно."""
        subquery = SelectNode(
            columns=[SelectColumn(expression=ColumnRef(column_name="id"))],
            from_clause=FromClause(table=TableRef(table_name="departments")),
            where=BinaryOpNode(
                left=ColumnRef(column_name="id"),
                operator=BinaryOperator.LE,
                right=LiteralNode(value=2, data_type=DataType.INT)
            )
        )
        
        # 1 IN (1, 2)
        result_in = self.subquery_executor.execute_in(1, subquery)
        self.assertTrue(result_in)
        
        # 99 NOT IN (1, 2)
        result_not_in = self.subquery_executor.execute_in(99, subquery, negated=True)
        self.assertTrue(result_not_in)
    
    def test_checkpoint3_exists_subquery(self):
        """Checkpoint #3: EXISTS/NOT EXISTS работает."""
        subquery_exists = SelectNode(
            columns=[SelectColumn(expression=LiteralNode(value=1, data_type=DataType.INT))],
            from_clause=FromClause(table=TableRef(table_name="employees")),
            where=BinaryOpNode(
                left=ColumnRef(column_name="id"),
                operator=BinaryOperator.EQ,
                right=LiteralNode(value=1, data_type=DataType.INT)
            )
        )
        
        result_exists = self.subquery_executor.execute_exists(subquery_exists)
        self.assertTrue(result_exists)
        
        subquery_not_exists = SelectNode(
            columns=[SelectColumn(expression=LiteralNode(value=1, data_type=DataType.INT))],
            from_clause=FromClause(table=TableRef(table_name="employees")),
            where=BinaryOpNode(
                left=ColumnRef(column_name="id"),
                operator=BinaryOperator.EQ,
                right=LiteralNode(value=999, data_type=DataType.INT)
            )
        )
        
        result_not_exists = self.subquery_executor.execute_exists(subquery_not_exists, negated=True)
        self.assertTrue(result_not_exists)
    
    def test_checkpoint4_correlated_subquery(self):
        """Checkpoint #4: Correlated subquery работает."""
        outer_row = {"department_id": 1}
        
        context = SubqueryContext(outer_row=outer_row, outer_alias="outer")
        
        # Subquery that uses outer reference
        subquery = SelectNode(
            columns=[SelectColumn(expression=ColumnRef(column_name="name"))],
            from_clause=FromClause(table=TableRef(table_name="departments")),
            where=BinaryOpNode(
                left=ColumnRef(column_name="id"),
                operator=BinaryOperator.EQ,
                right=ColumnRef(column_name="department_id", table_alias="outer")
            ),
            limit=1
        )
        
        # Should resolve outer.department_id = 1
        result = self.subquery_executor.execute_scalar(subquery, context)
        
        # Department 1 is Engineering
        self.assertEqual(result, "Engineering")
    
    def test_checkpoint5_derived_table(self):
        """Checkpoint #5: Derived tables работают."""
        subquery = SelectNode(
            columns=[
                SelectColumn(expression=ColumnRef(column_name="id")),
                SelectColumn(expression=ColumnRef(column_name="name"))
            ],
            from_clause=FromClause(table=TableRef(table_name="departments")),
            limit=2
        )
        
        result = self.subquery_executor.execute_derived_table(subquery, "d")
        
        # Should return 2 rows with prefixed columns
        self.assertEqual(len(result), 2)
        self.assertIn("d.id", result[0])
        self.assertIn("d.name", result[0])


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    unittest.main(verbosity=2)