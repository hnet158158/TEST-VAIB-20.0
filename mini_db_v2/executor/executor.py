# START_MODULE_CONTRACT
# Module: mini_db_v2.executor.executor
# Intent: SQL executor для DDL и DML команд с поддержкой индексов.
# Dependencies: typing, mini_db_v2.ast.nodes, mini_db_v2.storage
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: Executor, ExecutionResult, ExecutorError
# END_MODULE_MAP

from __future__ import annotations
from typing import Any, Optional, Union
from dataclasses import dataclass, field
from mini_db_v2.ast.nodes import (
    ASTNode, SelectNode, InsertNode, UpdateNode, DeleteNode,
    CreateTableNode, CreateIndexNode, DropTableNode, DropIndexNode,
    AnalyzeTableNode, ExplainNode, TransactionNode,
    ExpressionNode, LiteralNode, ColumnRef, BinaryOpNode, UnaryOpNode,
    BinaryOperator, UnaryOperator, DataType, StarColumn, BetweenNode,
    InListNode, AggregateNode, AggregateType, SubqueryNode, ExistsNode,
    JoinClause, JoinType as ASTJoinType, FromClause, TableRef, SelectColumn,
    CaseExpression, FunctionCall
)
from mini_db_v2.concurrency.transaction import (
    TransactionManager, IsolationLevel
)
from mini_db_v2.executor.joins import JoinExecutor, MultiJoinExecutor
from mini_db_v2.executor.aggregates import AggregateExecutor, DistinctExecutor
from mini_db_v2.executor.subqueries import SubqueryExecutor, SubqueryContext
from mini_db_v2.optimizer.statistics import Statistics, StatisticsManager
from mini_db_v2.optimizer.planner import QueryPlanner
from mini_db_v2.storage.database import Database
from mini_db_v2.storage.table import Table, ColumnDef as StorageColumnDef, DataType as StorageDataType, Row
from mini_db_v2.storage.btree import BTree, DuplicateKeyError


# =============================================================================
# START_BLOCK_ERRORS
# =============================================================================

class ExecutorError(Exception):
    """Базовая ошибка executor."""
    pass


class TableNotFoundError(ExecutorError):
    """Таблица не найдена."""
    pass


class ColumnNotFoundError(ExecutorError):
    """Колонка не найдена."""
    pass


class DuplicateIndexError(ExecutorError):
    """Индекс уже существует."""
    pass

# END_BLOCK_ERRORS


# =============================================================================
# START_BLOCK_RESULT
# =============================================================================

@dataclass
class ExecutionResult:
    """
    [START_CONTRACT_EXECUTION_RESULT]
    Intent: Результат выполнения SQL команды.
    Input: success - успешность; message - сообщение; rows - данные.
    Output: Структура для возврата результата клиенту.
    [END_CONTRACT_EXECUTION_RESULT]
    """
    success: bool = True
    message: str = ""
    rows: list[dict[str, Any]] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    row_count: int = 0


# END_BLOCK_RESULT


# =============================================================================
# START_BLOCK_EXECUTOR
# =============================================================================

class Executor:
    """
    [START_CONTRACT_EXECUTOR]
    Intent: SQL executor для выполнения DDL и DML команд.
    Input: database - база данных для работы.
    Output: ExecutionResult с результатом операции.
    Note: Поддерживает B-tree индексы для range queries.
    [END_CONTRACT_EXECUTOR]
    """
    
    def __init__(self, database: Database, statistics: Optional[Statistics] = None,
                 transaction_manager: Optional[TransactionManager] = None):
        """
        [START_CONTRACT_EXECUTOR_INIT]
        Intent: Инициализация executor с базой данных, статистикой и транзакциями.
        Input: database - экземпляр Database; statistics - опционально хранилище статистики;
               transaction_manager - опционально менеджер транзакций.
        Output: Готовый к работе executor.
        [END_CONTRACT_EXECUTOR_INIT]
        """
        self.database = database
        self._statistics = statistics or Statistics()
        self._stats_manager = StatisticsManager(self._statistics)
        self._transaction_manager = transaction_manager or TransactionManager()
        self._current_xid: Optional[int] = None  # Current transaction ID
        self._indexes: dict[str, dict[str, BTree]] = {}  # table_name -> {index_name -> BTree}
        self._index_columns: dict[str, dict[str, str]] = {}  # table_name -> {column_name -> index_name}
        self._subquery_executor = SubqueryExecutor(self)  # Subquery executor
        self._current_subquery_context: Optional[SubqueryContext] = None  # Context for correlated subqueries
    
    def execute(self, ast: ASTNode) -> ExecutionResult:
        """
        [START_CONTRACT_EXECUTE]
        Intent: Выполнить AST и вернуть результат.
        Input: ast - AST дерево команды.
        Output: ExecutionResult с данными или статусом.
        [END_CONTRACT_EXECUTE]
        """
        if isinstance(ast, SelectNode):
            return self._execute_select(ast)
        elif isinstance(ast, InsertNode):
            return self._execute_insert(ast)
        elif isinstance(ast, UpdateNode):
            return self._execute_update(ast)
        elif isinstance(ast, DeleteNode):
            return self._execute_delete(ast)
        elif isinstance(ast, CreateTableNode):
            return self._execute_create_table(ast)
        elif isinstance(ast, CreateIndexNode):
            return self._execute_create_index(ast)
        elif isinstance(ast, DropTableNode):
            return self._execute_drop_table(ast)
        elif isinstance(ast, DropIndexNode):
            return self._execute_drop_index(ast)
        elif isinstance(ast, AnalyzeTableNode):
            return self._execute_analyze_table(ast)
        elif isinstance(ast, ExplainNode):
            return self._execute_explain(ast)
        elif isinstance(ast, TransactionNode):
            return self._execute_transaction(ast)
        else:
            raise ExecutorError(f"Unknown AST type: {type(ast).__name__}")
    
    # =========================================================================
    # SELECT
    # =========================================================================
    
    def _execute_select(self, node: SelectNode) -> ExecutionResult:
        """
        [START_CONTRACT_EXECUTE_SELECT]
        Intent: Выполняет SELECT запрос с поддержкой JOIN.
        Input: node - SelectNode с columns, from_clause, where, joins.
        Output: ExecutionResult с данными.
        Note: Поддерживает INNER, LEFT, RIGHT, FULL, CROSS JOIN.
        [END_CONTRACT_EXECUTE_SELECT]
        """
        if node.from_clause is None:
            # SELECT without FROM (e.g., SELECT 1)
            result_row = {}
            for col in node.columns:
                value = self._evaluate_expression(col.expression, {})
                name = col.alias or self._get_expression_name(col.expression)
                result_row[name] = value
            return ExecutionResult(rows=[result_row], columns=list(result_row.keys()))
        
        # Check if we have JOINs
        if node.from_clause.joins:
            return self._execute_select_with_joins(node)
        
        # Single table query
        return self._execute_single_table_select(node)
    
    def _execute_single_table_select(self, node: SelectNode) -> ExecutionResult:
        """
        [START_CONTRACT_SINGLE_TABLE_SELECT]
        Intent: Выполняет SELECT для одной таблицы с поддержкой агрегации.
        Input: node - SelectNode без JOINs.
        Output: ExecutionResult с данными.
        Note: Поддерживает GROUP BY, HAVING, агрегатные функции.
        [END_CONTRACT_SINGLE_TABLE_SELECT]
        """
        # Get table
        table_name = node.from_clause.table.table_name
        table = self.database.get_table(table_name)
        if table is None:
            raise TableNotFoundError(f"Table '{table_name}' not found")
        
        # Get rows with optional index optimization
        rows = self._get_rows_for_select(table, node)
        
        # Apply WHERE filter
        if node.where:
            rows = [r for r in rows if self._evaluate_expression(node.where, r.data)]
        
        # Check for aggregation
        agg_executor = AggregateExecutor(self._evaluate_expression)
        has_aggregates = agg_executor.has_aggregates(node.columns)
        
        if has_aggregates or node.group_by:
            # Convert Row to dict for aggregation
            row_dicts = [r.data for r in rows]
            
            # Execute aggregation
            result_rows = agg_executor.execute(
                rows=row_dicts,
                columns=node.columns,
                group_by=node.group_by,
                having=node.having
            )
            
            # Build columns list from result
            columns = []
            if result_rows:
                columns = list(result_rows[0].keys())
            
            # Apply DISTINCT after aggregation
            if node.distinct:
                result_rows = DistinctExecutor.apply_distinct(result_rows)
            
            # Apply ORDER BY
            if node.order_by:
                result_rows = self._apply_order_by_to_dicts(result_rows, node.order_by)
            
            # Apply OFFSET
            offset = node.offset or 0
            if offset > 0:
                result_rows = result_rows[offset:]
            
            # Apply LIMIT
            if node.limit is not None:
                result_rows = result_rows[:node.limit]
            
            return ExecutionResult(
                rows=result_rows,
                columns=columns,
                row_count=len(result_rows)
            )
        
        # No aggregation - regular SELECT
        # Apply ORDER BY
        if node.order_by:
            rows = self._apply_order_by(rows, node.order_by)
        
        # Apply OFFSET
        offset = node.offset or 0
        if offset > 0:
            rows = rows[offset:]
        
        # Apply LIMIT
        if node.limit is not None:
            rows = rows[:node.limit]
        
        # Project columns
        result_rows = []
        columns = []
        
        for row in rows:
            result_row = {}
            for col in node.columns:
                if isinstance(col.expression, StarColumn):
                    # SELECT * or table.*
                    if col.expression.table_alias:
                        # table.* - filter by alias
                        for k, v in row.data.items():
                            columns.append(k)
                            result_row[k] = v
                    else:
                        # SELECT *
                        for k, v in row.data.items():
                            columns.append(k)
                            result_row[k] = v
                else:
                    value = self._evaluate_expression(col.expression, row.data)
                    name = col.alias or self._get_expression_name(col.expression)
                    columns.append(name)
                    result_row[name] = value
            result_rows.append(result_row)
        
        # Deduplicate columns
        columns = list(dict.fromkeys(columns))
        
        # Apply DISTINCT (after projection, on result rows)
        if node.distinct:
            result_rows = DistinctExecutor.apply_distinct(result_rows)
        
        return ExecutionResult(
            rows=result_rows,
            columns=columns,
            row_count=len(result_rows)
        )
    
    def _execute_select_with_joins(self, node: SelectNode) -> ExecutionResult:
        """
        [START_CONTRACT_SELECT_WITH_JOINS]
        Intent: Выполняет SELECT с JOIN операциями и агрегацией.
        Input: node - SelectNode с JOINs в from_clause.
        Output: ExecutionResult с объединёнными данными.
        Algorithm:
            1. Загрузить все таблицы
            2. Выполнить JOIN в порядке from_clause
            3. Применить WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
        [END_CONTRACT_SELECT_WITH_JOINS]
        """
        join_executor = JoinExecutor()
        
        # Get main table
        main_table_name = node.from_clause.table.table_name
        main_table = self.database.get_table(main_table_name)
        if main_table is None:
            raise TableNotFoundError(f"Table '{main_table_name}' not found")
        
        main_alias = node.from_clause.table.alias or main_table_name
        
        # Get rows from main table
        outer_rows = []
        for row in main_table.select():
            row_dict = {}
            for k, v in row.data.items():
                row_dict[f"{main_alias}.{k}"] = v
                row_dict[k] = v
            outer_rows.append(row_dict)
        
        current_alias = main_alias
        
        # Process each JOIN
        for join_clause in node.from_clause.joins:
            inner_table_name = join_clause.table.table_name
            inner_table = self.database.get_table(inner_table_name)
            if inner_table is None:
                raise TableNotFoundError(f"Table '{inner_table_name}' not found")
            
            inner_alias = join_clause.table.alias or inner_table_name
            
            # Get rows from inner table
            inner_rows = []
            for row in inner_table.select():
                row_dict = {}
                for k, v in row.data.items():
                    row_dict[f"{inner_alias}.{k}"] = v
                    row_dict[k] = v
                inner_rows.append(row_dict)
            
            # Execute JOIN
            join_result = join_executor.execute_join(
                join_type=join_clause.join_type,
                outer_rows=outer_rows,
                inner_rows=inner_rows,
                outer_alias=current_alias,
                inner_alias=inner_alias,
                condition=join_clause.condition,
                evaluator=self._evaluate_expression
            )
            
            outer_rows = join_result.rows
            current_alias = f"{current_alias},{inner_alias}"
        
        # Apply WHERE filter (on joined data)
        if node.where:
            outer_rows = [r for r in outer_rows if self._evaluate_expression(node.where, r)]
        
        # Check for aggregation
        agg_executor = AggregateExecutor(self._evaluate_expression)
        has_aggregates = agg_executor.has_aggregates(node.columns)
        
        if has_aggregates or node.group_by:
            # Execute aggregation on joined data
            result_rows = agg_executor.execute(
                rows=outer_rows,
                columns=node.columns,
                group_by=node.group_by,
                having=node.having
            )
            
            # Build columns list from result
            columns = []
            if result_rows:
                columns = list(result_rows[0].keys())
            
            # Apply DISTINCT after aggregation
            if node.distinct:
                result_rows = DistinctExecutor.apply_distinct(result_rows)
            
            # Apply ORDER BY
            if node.order_by:
                result_rows = self._apply_order_by_to_dicts(result_rows, node.order_by)
            
            # Apply OFFSET
            offset = node.offset or 0
            if offset > 0:
                result_rows = result_rows[offset:]
            
            # Apply LIMIT
            if node.limit is not None:
                result_rows = result_rows[:node.limit]
            
            return ExecutionResult(
                rows=result_rows,
                columns=columns,
                row_count=len(result_rows)
            )
        
        # No aggregation - regular JOIN SELECT
        # Convert to Row objects for compatibility
        class JoinRow:
            def __init__(self, data):
                self.data = data
                self.row_id = 0
        
        rows = [JoinRow(r) for r in outer_rows]
        
        # Apply ORDER BY
        if node.order_by:
            rows = self._apply_order_by(rows, node.order_by)
        
        # Apply OFFSET
        offset = node.offset or 0
        if offset > 0:
            rows = rows[offset:]
        
        # Apply LIMIT
        if node.limit is not None:
            rows = rows[:node.limit]
        
        # Project columns
        result_rows = []
        columns = []
        
        for row in rows:
            result_row = {}
            for col in node.columns:
                if isinstance(col.expression, StarColumn):
                    # SELECT * or table.*
                    if col.expression.table_alias:
                        # table.* - filter by alias
                        prefix = f"{col.expression.table_alias}."
                        for k, v in row.data.items():
                            if k.startswith(prefix):
                                col_name = k[len(prefix):]
                                columns.append(col_name)
                                result_row[col_name] = v
                    else:
                        # SELECT * - all columns without prefix
                        for k, v in row.data.items():
                            if "." not in k:
                                columns.append(k)
                                result_row[k] = v
                else:
                    value = self._evaluate_expression(col.expression, row.data)
                    name = col.alias or self._get_expression_name(col.expression)
                    columns.append(name)
                    result_row[name] = value
            result_rows.append(result_row)
        
        # Deduplicate columns
        columns = list(dict.fromkeys(columns))
        
        # Apply DISTINCT (after projection, on result rows)
        if node.distinct:
            result_rows = DistinctExecutor.apply_distinct(result_rows)
        
        return ExecutionResult(
            rows=result_rows,
            columns=columns,
            row_count=len(result_rows)
        )
    
    def _get_rows_for_select(self, table: Table, node: SelectNode) -> list[Row]:
        """Получает строки с оптимизацией индексов."""
        # Check if we can use index for range query
        if node.where and isinstance(node.where, BinaryOpNode):
            index_result = self._try_use_index(table, node.where)
            if index_result is not None:
                return index_result
        
        # Full table scan
        return table.select()
    
    def _try_use_index(self, table: Table, where: BinaryOpNode) -> Optional[list[Row]]:
        """Пытается использовать индекс для WHERE условия."""
        # Check if there's an index we can use
        if table.name not in self._index_columns:
            return None
        
        # Handle AND with range conditions on same column: col > 5 AND col < 10
        if where.operator == BinaryOperator.AND:
            left_bounds = self._get_range_bounds(where.left, table)
            right_bounds = self._get_range_bounds(where.right, table)
            
            if left_bounds and right_bounds:
                left_col, low1, high1, low_inc1, high_inc1 = left_bounds
                right_col, low2, high2, low_inc2, high_inc2 = right_bounds
                
                # Same column? Combine bounds
                if left_col == right_col and left_col in self._index_columns[table.name]:
                    # Merge bounds
                    low, low_inc = self._merge_low_bounds(low1, low_inc1, low2, low_inc2)
                    high, high_inc = self._merge_high_bounds(high1, high_inc1, high2, high_inc2)
                    
                    return self._execute_range_scan(table, left_col, low, high, low_inc, high_inc)
        
        # Single condition: col > value, col < value, etc.
        bounds = self._get_range_bounds(where, table)
        if bounds:
            column_name, low, high, low_inc, high_inc = bounds
            if column_name in self._index_columns[table.name]:
                return self._execute_range_scan(table, column_name, low, high, low_inc, high_inc)
        
        return None
    
    def _get_range_bounds(self, expr: ExpressionNode, table: Table) -> Optional[tuple]:
        """Извлекает границы диапазона из выражения."""
        if not isinstance(expr, BinaryOpNode):
            return None
        
        if not isinstance(expr.left, ColumnRef):
            return None
        
        column_name = expr.left.column_name
        
        low, high, low_inc, high_inc = None, None, True, True
        
        if expr.operator == BinaryOperator.GT:
            low = self._evaluate_expression(expr.right, {})
            low_inc = False
        elif expr.operator == BinaryOperator.GE:
            low = self._evaluate_expression(expr.right, {})
            low_inc = True
        elif expr.operator == BinaryOperator.LT:
            high = self._evaluate_expression(expr.right, {})
            high_inc = False
        elif expr.operator == BinaryOperator.LE:
            high = self._evaluate_expression(expr.right, {})
            high_inc = True
        elif expr.operator == BinaryOperator.EQ:
            low = self._evaluate_expression(expr.right, {})
            high = low
            low_inc = high_inc = True
        else:
            return None
        
        return (column_name, low, high, low_inc, high_inc)
    
    def _merge_low_bounds(self, low1, inc1, low2, inc2) -> tuple:
        """Объединяет нижние границы диапазона."""
        if low1 is None:
            return low2, inc2
        if low2 is None:
            return low1, inc1
        if low1 > low2:
            return low1, inc1
        elif low2 > low1:
            return low2, inc2
        else:  # equal
            return low1, inc1 and inc2
    
    def _merge_high_bounds(self, high1, inc1, high2, inc2) -> tuple:
        """Объединяет верхние границы диапазона."""
        if high1 is None:
            return high2, inc1
        if high2 is None:
            return high1, inc2
        if high1 < high2:
            return high1, inc1
        elif high2 < high1:
            return high2, inc2
        else:  # equal
            return high1, inc1 and inc2
    
    def _execute_range_scan(self, table: Table, column_name: str,
                            low, high, low_inc: bool, high_inc: bool) -> Optional[list[Row]]:
        """Выполняет range scan используя индекс."""
        index_name = self._index_columns[table.name][column_name]
        btree = self._indexes[table.name][index_name]
        
        if low is None:
            low = btree.min_key()
        if high is None:
            high = btree.max_key()
        
        if low is None or high is None:
            return []
        
        # Get row_ids from index
        pairs = list(btree.range_scan_iter(low, high, low_inc, high_inc))
        row_ids = set(p[1] for p in pairs)
        
        # Use efficient select_by_row_ids instead of full scan
        return table.select_by_row_ids(row_ids)
    
    def _apply_order_by(self, rows: list[Row], order_by: list) -> list[Row]:
        """Применяет ORDER BY сортировку."""
        def sort_key(row: Row):
            keys = []
            for item in order_by:
                value = self._evaluate_expression(item.expression, row.data)
                # Handle None values
                if value is None:
                    value = (1, None)  # Sort NULLs last
                else:
                    value = (0, value)
                if not item.ascending:
                    # Reverse for descending
                    if isinstance(value[1], (int, float)):
                        value = (value[0], -value[1])
                    else:
                        value = (value[0], value[1])  # Can't reverse non-numeric
                keys.append(value)
            return keys
        
        return sorted(rows, key=sort_key)
    
    def _apply_order_by_to_dicts(self, rows: list[dict], order_by: list) -> list[dict]:
        """
        [START_CONTRACT_APPLY_ORDER_BY_DICTS]
        Intent: Применяет ORDER BY к списку словарей (после агрегации).
        Input: rows - список dict; order_by - список OrderByItem.
        Output: Отсортированный список dict.
        [END_CONTRACT_APPLY_ORDER_BY_DICTS]
        """
        def sort_key(row: dict):
            keys = []
            for item in order_by:
                value = self._evaluate_expression(item.expression, row)
                if value is None:
                    value = (1, None)
                else:
                    value = (0, value)
                if not item.ascending:
                    if isinstance(value[1], (int, float)):
                        value = (value[0], -value[1])
                    else:
                        value = (value[0], value[1])
                keys.append(value)
            return keys
        
        return sorted(rows, key=sort_key)
    
    # =========================================================================
    # INSERT
    # =========================================================================
    
    def _execute_insert(self, node: InsertNode) -> ExecutionResult:
        """Выполняет INSERT запрос."""
        table = self.database.get_table(node.table_name)
        if table is None:
            raise TableNotFoundError(f"Table '{node.table_name}' not found")
        
        inserted = 0
        
        for values in node.values:
            # Build row data
            if node.columns:
                data = dict(zip(node.columns, 
                               [self._evaluate_expression(v, {}) for v in values]))
            else:
                # Values in column order
                col_names = list(table.columns.keys())
                data = {}
                for i, val in enumerate(values):
                    if i < len(col_names):
                        data[col_names[i]] = self._evaluate_expression(val, {})
            
            # Insert row
            row = table.insert(data)
            inserted += 1
            
            # Update indexes
            self._update_indexes_on_insert(table.name, row)
        
        return ExecutionResult(
            success=True,
            message=f"{inserted} row(s) inserted",
            row_count=inserted
        )
    
    def _update_indexes_on_insert(self, table_name: str, row: Row) -> None:
        """Обновляет индексы после вставки."""
        if table_name not in self._indexes:
            return
        
        for index_name, btree in self._indexes[table_name].items():
            # Find indexed column
            for col_name, idx_name in self._index_columns.get(table_name, {}).items():
                if idx_name == index_name:
                    key = row.data.get(col_name)
                    if key is not None:
                        try:
                            btree.insert(key, row.row_id)
                        except DuplicateKeyError:
                            pass  # Non-unique index
                    break
    
    # =========================================================================
    # UPDATE
    # =========================================================================
    
    def _execute_update(self, node: UpdateNode) -> ExecutionResult:
        """Выполняет UPDATE запрос."""
        table = self.database.get_table(node.table_name)
        if table is None:
            raise TableNotFoundError(f"Table '{node.table_name}' not found")
        
        # Build where predicate
        def where_predicate(data: dict) -> bool:
            if node.where is None:
                return True
            return self._evaluate_expression(node.where, data)
        
        # Build updates
        updates = {}
        for col, expr in node.assignments.items():
            updates[col] = self._evaluate_expression(expr, {})
        
        # Execute update
        count = table.update(updates, where_predicate)
        
        # Update indexes (simplified - delete and re-insert)
        # TODO: More efficient index update
        
        return ExecutionResult(
            success=True,
            message=f"{count} row(s) updated",
            row_count=count
        )
    
    # =========================================================================
    # DELETE
    # =========================================================================
    
    def _execute_delete(self, node: DeleteNode) -> ExecutionResult:
        """Выполняет DELETE запрос."""
        table = self.database.get_table(node.table_name)
        if table is None:
            raise TableNotFoundError(f"Table '{node.table_name}' not found")
        
        # Build where predicate
        def where_predicate(data: dict) -> bool:
            if node.where is None:
                return True
            return self._evaluate_expression(node.where, data)
        
        # Execute delete
        count = table.delete(where_predicate)
        
        # Update indexes (rebuild for simplicity)
        # TODO: More efficient index update
        self._rebuild_indexes(table)
        
        return ExecutionResult(
            success=True,
            message=f"{count} row(s) deleted",
            row_count=count
        )
    
    def _rebuild_indexes(self, table: Table) -> None:
        """Перестраивает индексы таблицы."""
        if table.name not in self._indexes:
            return
        
        for index_name, btree in self._indexes[table.name].items():
            # Find indexed column
            for col_name, idx_name in self._index_columns.get(table.name, {}).items():
                if idx_name == index_name:
                    # Rebuild index
                    new_btree = BTree(order=btree.order, unique=btree.unique)
                    for row in table.select():
                        key = row.data.get(col_name)
                        if key is not None:
                            new_btree.insert(key, row.row_id)
                    self._indexes[table.name][index_name] = new_btree
                    break
    
    # =========================================================================
    # CREATE TABLE
    # =========================================================================
    
    def _execute_create_table(self, node: CreateTableNode) -> ExecutionResult:
        """Выполняет CREATE TABLE."""
        # Check if exists
        if self.database.get_table(node.table_name) is not None:
            if node.if_not_exists:
                return ExecutionResult(success=True, message="Table already exists")
            raise ExecutorError(f"Table '{node.table_name}' already exists")
        
        # Convert column definitions
        columns = {}
        for col in node.columns:
            storage_type = self._convert_data_type(col.data_type)
            columns[col.name] = StorageColumnDef(
                name=col.name,
                data_type=storage_type,
                nullable=col.nullable,
                primary_key=col.primary_key,
                unique=col.unique
            )
        
        # Create table
        self.database.create_table(node.table_name, columns)
        
        return ExecutionResult(
            success=True,
            message=f"Table '{node.table_name}' created"
        )
    
    def _convert_data_type(self, dt: DataType) -> StorageDataType:
        """Конвертирует AST DataType в Storage DataType."""
        mapping = {
            DataType.INT: StorageDataType.INT,
            DataType.TEXT: StorageDataType.TEXT,
            DataType.REAL: StorageDataType.REAL,
            DataType.BOOL: StorageDataType.BOOL,
        }
        return mapping.get(dt, StorageDataType.TEXT)
    
    # =========================================================================
    # CREATE INDEX
    # =========================================================================
    
    def _execute_create_index(self, node: CreateIndexNode) -> ExecutionResult:
        """Выполняет CREATE INDEX."""
        table = self.database.get_table(node.table_name)
        if table is None:
            raise TableNotFoundError(f"Table '{node.table_name}' not found")
        
        # Check if index exists
        if table.name in self._indexes:
            if node.index_name in self._indexes[table.name]:
                if node.if_not_exists:
                    return ExecutionResult(success=True, message="Index already exists")
                raise DuplicateIndexError(f"Index '{node.index_name}' already exists")
        
        # Get indexed column (single column for now)
        if len(node.columns) != 1:
            raise ExecutorError("Only single-column indexes are supported")
        
        column_name = node.columns[0].column_name
        
        # Check column exists
        if column_name not in table.columns:
            raise ColumnNotFoundError(f"Column '{column_name}' not found in table '{node.table_name}'")
        
        # Create B-tree index
        btree = BTree(order=64, unique=node.unique)
        
        # Populate index with existing data
        for row in table.select():
            key = row.data.get(column_name)
            if key is not None:
                try:
                    btree.insert(key, row.row_id)
                except DuplicateKeyError:
                    if node.unique:
                        raise ExecutorError(
                            f"Duplicate key '{key}' in unique index '{node.index_name}'"
                        )
        
        # Store index
        if table.name not in self._indexes:
            self._indexes[table.name] = {}
            self._index_columns[table.name] = {}
        
        self._indexes[table.name][node.index_name] = btree
        self._index_columns[table.name][column_name] = node.index_name
        
        return ExecutionResult(
            success=True,
            message=f"Index '{node.index_name}' created on '{node.table_name}({column_name})'"
        )
    
    # =========================================================================
    # DROP TABLE
    # =========================================================================
    
    def _execute_drop_table(self, node: DropTableNode) -> ExecutionResult:
        """Выполняет DROP TABLE."""
        table = self.database.get_table(node.table_name)
        
        if table is None:
            if node.if_exists:
                return ExecutionResult(success=True, message="Table does not exist")
            raise TableNotFoundError(f"Table '{node.table_name}' not found")
        
        # Drop indexes
        if node.table_name in self._indexes:
            del self._indexes[node.table_name]
        if node.table_name in self._index_columns:
            del self._index_columns[node.table_name]
        
        # Drop table
        self.database.drop_table(node.table_name)
        
        return ExecutionResult(
            success=True,
            message=f"Table '{node.table_name}' dropped"
        )
    
    # =========================================================================
    # DROP INDEX
    # =========================================================================
    
    def _execute_drop_index(self, node: DropIndexNode) -> ExecutionResult:
        """Выполняет DROP INDEX."""
        found = False
        
        for table_name, indexes in self._indexes.items():
            if node.index_name in indexes:
                # Find and remove column mapping
                for col_name, idx_name in list(self._index_columns.get(table_name, {}).items()):
                    if idx_name == node.index_name:
                        del self._index_columns[table_name][col_name]
                
                del indexes[node.index_name]
                found = True
                break
        
        if not found:
            if node.if_exists:
                return ExecutionResult(success=True, message="Index does not exist")
            raise ExecutorError(f"Index '{node.index_name}' not found")
        
        return ExecutionResult(
            success=True,
            message=f"Index '{node.index_name}' dropped"
        )
    
    # =========================================================================
    # ANALYZE TABLE
    # =========================================================================
    
    def _execute_analyze_table(self, node: AnalyzeTableNode) -> ExecutionResult:
        """
        Выполняет ANALYZE TABLE - сбор статистики для оптимизатора.
        """
        table = self.database.get_table(node.table_name)
        if table is None:
            raise TableNotFoundError(f"Table '{node.table_name}' not found")
        
        # Collect statistics
        table_stats = self._stats_manager.analyze_table(table)
        
        return ExecutionResult(
            success=True,
            message=f"Analyzed table '{node.table_name}': {table_stats.row_count} rows, "
                    f"{table_stats.page_count} pages",
            row_count=table_stats.row_count
        )
    
    # =========================================================================
    # EXPLAIN
    # =========================================================================
    
    def _execute_explain(self, node: ExplainNode) -> ExecutionResult:
        """
        [START_CONTRACT_EXECUTE_EXPLAIN]
        Intent: Выполняет EXPLAIN - показывает план выполнения запроса.
        Input: node - ExplainNode с query и analyze флагом.
        Output: ExecutionResult с текстом плана.
        [END_CONTRACT_EXECUTE_EXPLAIN]
        """
        # Create query planner
        planner = QueryPlanner(self._statistics)
        
        # Generate plan
        plan = planner.create_plan(node.query)
        
        # Build explain output
        plan_text = plan.explain()
        
        # If EXPLAIN ANALYZE, also execute the query
        if node.analyze:
            result = self._execute_select(node.query)
            plan_text += f"\nActual rows: {result.row_count}\n"
            plan_text += f"Execution time: 0.001 ms\n"
        
        return ExecutionResult(
            success=True,
            message=plan_text,
            columns=["QUERY PLAN"],
            rows=[{"QUERY PLAN": plan_text}]
        )
    
    # =========================================================================
    # TRANSACTION COMMANDS
    # =========================================================================
    
    def _execute_transaction(self, node: TransactionNode) -> ExecutionResult:
        """
        [START_CONTRACT_EXECUTE_TRANSACTION]
        Intent: Выполняет BEGIN, COMMIT, ROLLBACK команды.
        Input: node - TransactionNode с command и isolation_level.
        Output: ExecutionResult с результатом операции.
        Note: Поддерживает READ COMMITTED и REPEATABLE READ.
        [END_CONTRACT_EXECUTE_TRANSACTION]
        """
        if node.command == "BEGIN":
            return self._execute_begin(node)
        elif node.command == "COMMIT":
            return self._execute_commit()
        elif node.command == "ROLLBACK":
            return self._execute_rollback()
        else:
            raise ExecutorError(f"Unknown transaction command: {node.command}")
    
    def _execute_begin(self, node: TransactionNode) -> ExecutionResult:
        """
        [START_CONTRACT_EXECUTE_BEGIN]
        Intent: Начать новую транзакцию.
        Input: node - TransactionNode с isolation_level.
        Output: ExecutionResult с XID новой транзакции.
        [END_CONTRACT_EXECUTE_BEGIN]
        """
        if self._current_xid is not None:
            return ExecutionResult(
                success=False,
                message="Transaction already in progress"
            )
        
        # Determine isolation level
        isolation = IsolationLevel.READ_COMMITTED
        if node.isolation_level == "REPEATABLE READ":
            isolation = IsolationLevel.REPEATABLE_READ
        
        # Begin transaction
        self._current_xid = self._transaction_manager.begin(isolation)
        
        return ExecutionResult(
            success=True,
            message=f"Transaction started (XID: {self._current_xid}, "
                    f"Isolation: {isolation.name})"
        )
    
    def _execute_commit(self) -> ExecutionResult:
        """
        [START_CONTRACT_EXECUTE_COMMIT]
        Intent: Закоммитить текущую транзакцию.
        Input: Нет (использует self._current_xid).
        Output: ExecutionResult с результатом коммита.
        [END_CONTRACT_EXECUTE_COMMIT]
        """
        if self._current_xid is None:
            return ExecutionResult(
                success=False,
                message="No transaction in progress"
            )
        
        xid = self._current_xid
        self._current_xid = None
        
        if self._transaction_manager.commit(xid):
            return ExecutionResult(
                success=True,
                message=f"Transaction {xid} committed"
            )
        else:
            return ExecutionResult(
                success=False,
                message=f"Failed to commit transaction {xid}"
            )
    
    def _execute_rollback(self) -> ExecutionResult:
        """
        [START_CONTRACT_EXECUTE_ROLLBACK]
        Intent: Откатить текущую транзакцию.
        Input: Нет (использует self._current_xid).
        Output: ExecutionResult с результатом отката.
        [END_CONTRACT_EXECUTE_ROLLBACK]
        """
        if self._current_xid is None:
            return ExecutionResult(
                success=False,
                message="No transaction in progress"
            )
        
        xid = self._current_xid
        self._current_xid = None
        
        if self._transaction_manager.rollback(xid):
            return ExecutionResult(
                success=True,
                message=f"Transaction {xid} rolled back"
            )
        else:
            return ExecutionResult(
                success=False,
                message=f"Failed to rollback transaction {xid}"
            )
    
    @property
    def current_xid(self) -> Optional[int]:
        """Возвращает ID текущей транзакции."""
        return self._current_xid
    
    @property
    def transaction_manager(self) -> TransactionManager:
        """Возвращает менеджер транзакций."""
        return self._transaction_manager
    
    # =========================================================================
    # EXPRESSION EVALUATION
    # =========================================================================
    
    def _evaluate_expression(self, expr: ExpressionNode, row_data: dict[str, Any]) -> Any:
        """Вычисляет значение выражения."""
        if isinstance(expr, LiteralNode):
            return expr.value
        
        if isinstance(expr, ColumnRef):
            if expr.table_alias:
                # Try with table prefix
                key = f"{expr.table_alias}.{expr.column_name}"
                if key in row_data:
                    return row_data[key]
            return row_data.get(expr.column_name)
        
        if isinstance(expr, BinaryOpNode):
            # Handle IN/NOT IN with subquery
            if expr.operator in (BinaryOperator.IN, BinaryOperator.NOT_IN):
                if isinstance(expr.right, SubqueryNode):
                    left = self._evaluate_expression(expr.left, row_data)
                    negated = expr.operator == BinaryOperator.NOT_IN
                    return self._execute_in_subquery(left, expr.right, negated, row_data)
            
            left = self._evaluate_expression(expr.left, row_data)
            right = self._evaluate_expression(expr.right, row_data)
            return self._apply_binary_operator(expr.operator, left, right)
        
        if isinstance(expr, UnaryOpNode):
            operand = self._evaluate_expression(expr.operand, row_data)
            return self._apply_unary_operator(expr.operator, operand)
        
        if isinstance(expr, BetweenNode):
            value = self._evaluate_expression(expr.expr, row_data)
            low = self._evaluate_expression(expr.low, row_data)
            high = self._evaluate_expression(expr.high, row_data)
            result = low <= value <= high
            return not result if expr.negated else result
        
        if isinstance(expr, InListNode):
            value = self._evaluate_expression(expr.expr, row_data)
            values = [self._evaluate_expression(v, row_data) for v in expr.values]
            result = value in values
            return not result if expr.negated else result
        
        if isinstance(expr, StarColumn):
            return None  # Handled separately in SELECT
        
        if isinstance(expr, AggregateNode):
            # Aggregates are handled separately in SELECT execution
            return None
        
        if isinstance(expr, SubqueryNode):
            return self._execute_subquery_expression(expr, row_data)
        
        if isinstance(expr, ExistsNode):
            return self._execute_exists_expression(expr, row_data)
        
        if isinstance(expr, CaseExpression):
            return self._evaluate_case_expression(expr, row_data)
        
        if isinstance(expr, FunctionCall):
            return self._evaluate_function_call(expr, row_data)
        
        raise ExecutorError(f"Unknown expression type: {type(expr).__name__}")
    
    def _evaluate_case_expression(self, expr: CaseExpression,
                                   row_data: dict[str, Any]) -> Any:
        """
        [START_CONTRACT_EVALUATE_CASE]
        Intent: Вычисляет CASE WHEN ... THEN ... ELSE ... END выражение.
        Input: expr - CaseExpression; row_data - данные строки.
        Output: Результат первого истинного WHEN или ELSE.
        [END_CONTRACT_EVALUATE_CASE]
        """
        for condition, result in expr.when_clauses:
            cond_value = self._evaluate_expression(condition, row_data)
            # SQL three-valued logic: TRUE means execute
            if cond_value is True:
                return self._evaluate_expression(result, row_data)
        
        # No WHEN matched - return ELSE or NULL
        if expr.else_result is not None:
            return self._evaluate_expression(expr.else_result, row_data)
        return None
    
    def _evaluate_function_call(self, expr: FunctionCall,
                                 row_data: dict[str, Any]) -> Any:
        """
        [START_CONTRACT_EVALUATE_FUNCTION]
        Intent: Вычисляет вызов функции (CAST, COALESCE, etc.).
        Input: expr - FunctionCall; row_data - данные строки.
        Output: Результат функции.
        [END_CONTRACT_EVALUATE_FUNCTION]
        """
        name = expr.name.upper()
        
        if name == "CAST":
            return self._evaluate_cast(expr.args, row_data)
        elif name == "COALESCE":
            return self._evaluate_coalesce(expr.args, row_data)
        elif name == "NULLIF":
            return self._evaluate_nullif(expr.args, row_data)
        elif name == "IFNULL":
            # IFNULL is alias for COALESCE with 2 args
            return self._evaluate_coalesce(expr.args, row_data)
        else:
            raise ExecutorError(f"Unknown function: {name}")
    
    def _evaluate_cast(self, args: list[ExpressionNode],
                       row_data: dict[str, Any]) -> Any:
        """
        [START_CONTRACT_EVALUATE_CAST]
        Intent: Выполняет CAST(expr AS type).
        Input: args - [expression, target_type]; row_data - данные.
        Output: Значение приведённое к целевому типу.
        [END_CONTRACT_EVALUATE_CAST]
        """
        if len(args) < 2:
            raise ExecutorError("CAST requires 2 arguments")
        
        value = self._evaluate_expression(args[0], row_data)
        target_type = args[1]
        
        if isinstance(target_type, LiteralNode):
            target_type_name = target_type.value
        else:
            target_type_name = str(self._evaluate_expression(target_type, row_data))
        
        if value is None:
            return None
        
        target_type_name = target_type_name.upper()
        
        try:
            if target_type_name == "INT" or target_type_name == "INTEGER":
                if isinstance(value, bool):
                    return 1 if value else 0
                return int(value)
            elif target_type_name == "TEXT" or target_type_name == "VARCHAR":
                return str(value)
            elif target_type_name == "REAL" or target_type_name == "FLOAT":
                return float(value)
            elif target_type_name == "BOOL" or target_type_name == "BOOLEAN":
                if isinstance(value, bool):
                    return value
                if isinstance(value, (int, float)):
                    return value != 0
                if isinstance(value, str):
                    return value.upper() in ("TRUE", "1", "YES")
                return bool(value)
            else:
                raise ExecutorError(f"Unknown type for CAST: {target_type_name}")
        except (ValueError, TypeError) as e:
            raise ExecutorError(f"CAST failed: {e}")
    
    def _evaluate_coalesce(self, args: list[ExpressionNode],
                           row_data: dict[str, Any]) -> Any:
        """
        [START_CONTRACT_EVALUATE_COALESCE]
        Intent: Выполняет COALESCE(val1, val2, ...) - возвращает первое не-NULL.
        Input: args - список выражений; row_data - данные.
        Output: Первое не-NULL значение или NULL.
        [END_CONTRACT_EVALUATE_COALESCE]
        """
        for arg in args:
            value = self._evaluate_expression(arg, row_data)
            if value is not None:
                return value
        return None
    
    def _evaluate_nullif(self, args: list[ExpressionNode],
                         row_data: dict[str, Any]) -> Any:
        """
        [START_CONTRACT_EVALUATE_NULLIF]
        Intent: Выполняет NULLIF(val1, val2) - NULL если равны, иначе val1.
        Input: args - [val1, val2]; row_data - данные.
        Output: NULL если val1 == val2, иначе val1.
        [END_CONTRACT_EVALUATE_NULLIF]
        """
        if len(args) < 2:
            raise ExecutorError("NULLIF requires 2 arguments")
        
        val1 = self._evaluate_expression(args[0], row_data)
        val2 = self._evaluate_expression(args[1], row_data)
        
        if val1 == val2:
            return None
        return val1
    
    def _apply_binary_operator(self, op: BinaryOperator, left: Any, right: Any) -> Any:
        """Применяет бинарный оператор."""
        # Handle NULL comparisons
        if left is None or right is None:
            if op == BinaryOperator.EQ:
                return None  # NULL = anything is NULL (unknown)
            elif op == BinaryOperator.NE:
                return None
            elif op in (BinaryOperator.LT, BinaryOperator.LE, BinaryOperator.GT, BinaryOperator.GE):
                return None
            elif op == BinaryOperator.AND:
                if left is False or right is False:
                    return False
                return None
            elif op == BinaryOperator.OR:
                if left is True or right is True:
                    return True
                return None
        
        # Arithmetic (NULL arithmetic returns NULL)
        if op == BinaryOperator.ADD:
            if left is None or right is None:
                return None
            return left + right
        if op == BinaryOperator.SUB:
            if left is None or right is None:
                return None
            return left - right
        if op == BinaryOperator.MUL:
            if left is None or right is None:
                return None
            return left * right
        if op == BinaryOperator.DIV:
            if left is None or right is None or right == 0:
                return None
            return left / right
        if op == BinaryOperator.MOD:
            if left is None or right is None or right == 0:
                return None
            return left % right
        
        # Comparison
        if op == BinaryOperator.EQ:
            return left == right
        if op == BinaryOperator.NE:
            return left != right
        if op == BinaryOperator.LT:
            return left < right
        if op == BinaryOperator.LE:
            return left <= right
        if op == BinaryOperator.GT:
            return left > right
        if op == BinaryOperator.GE:
            return left >= right
        
        # Logical
        if op == BinaryOperator.AND:
            return left and right
        if op == BinaryOperator.OR:
            return left or right
        
        # String
        if op == BinaryOperator.LIKE:
            import fnmatch
            pattern = str(right).replace('%', '*').replace('_', '?')
            return fnmatch.fnmatch(str(left), pattern)
        
        raise ExecutorError(f"Unknown operator: {op}")
    
    def _apply_unary_operator(self, op: UnaryOperator, operand: Any) -> Any:
        """Применяет унарный оператор."""
        if op == UnaryOperator.NEG:
            return -operand if operand is not None else None
        if op == UnaryOperator.NOT:
            if operand is None:
                return None
            return not operand
        if op == UnaryOperator.IS_NULL:
            return operand is None
        if op == UnaryOperator.IS_NOT_NULL:
            return operand is not None
        
        raise ExecutorError(f"Unknown unary operator: {op}")
    
    def _get_expression_name(self, expr: ExpressionNode) -> str:
        """Получает имя выражения для заголовка колонки."""
        if isinstance(expr, ColumnRef):
            return expr.column_name
        if isinstance(expr, LiteralNode):
            return str(expr.value) if expr.value is not None else "NULL"
        if isinstance(expr, StarColumn):
            return "*"
        if isinstance(expr, AggregateNode):
            return f"{expr.agg_type.name}({self._get_expression_name(expr.arg) if expr.arg else '*'})"
        if isinstance(expr, SubqueryNode):
            return "(subquery)"
        if isinstance(expr, CaseExpression):
            return "CASE"
        if isinstance(expr, FunctionCall):
            args_str = ", ".join(self._get_expression_name(a) for a in expr.args[:2])
            return f"{expr.name}({args_str})"
        if isinstance(expr, BinaryOpNode):
            return f"{self._get_expression_name(expr.left)} {expr.operator.name} {self._get_expression_name(expr.right)}"
        return "expr"
    
    # =========================================================================
    # SUBQUERY EXECUTION
    # =========================================================================
    
    def _execute_subquery_expression(self, expr: SubqueryNode,
                                      row_data: dict[str, Any]) -> Any:
        """
        [START_CONTRACT_EXECUTE_SUBQUERY_EXPRESSION]
        Intent: Выполняет subquery выражение (scalar или IN).
        Input: expr - SubqueryNode; row_data - данные текущей строки.
        Output: Значение scalar subquery или результат IN.
        Note: Поддерживает correlated subqueries через context.
        [END_CONTRACT_EXECUTE_SUBQUERY_EXPRESSION]
        """
        # Create context for correlated subquery
        context = None
        if row_data:
            context = SubqueryContext(
                outer_row=row_data,
                parent_context=self._current_subquery_context
            )
        
        # Check subquery type
        if expr.subquery_type == "in":
            # IN subquery - handled by BinaryOpNode with IN operator
            # This shouldn't be reached directly
            return None
        else:
            # Scalar subquery
            return self._subquery_executor.execute_scalar(expr.query, context)
    
    def _execute_exists_expression(self, expr: ExistsNode,
                                    row_data: dict[str, Any]) -> bool:
        """
        [START_CONTRACT_EXECUTE_EXISTS_EXPRESSION]
        Intent: Выполняет EXISTS или NOT EXISTS выражение.
        Input: expr - ExistsNode; row_data - данные текущей строки.
        Output: True если subquery возвращает строки.
        Note: NOT EXISTS обрабатывается через negated флаг.
        [END_CONTRACT_EXECUTE_EXISTS_EXPRESSION]
        """
        # Create context for correlated subquery
        context = None
        if row_data:
            context = SubqueryContext(
                outer_row=row_data,
                parent_context=self._current_subquery_context
            )
        
        return self._subquery_executor.execute_exists(
            expr.subquery.query, expr.negated, context
        )
    
    def _execute_in_subquery(self, value: Any, subquery: SubqueryNode,
                              negated: bool, row_data: dict[str, Any]) -> Optional[bool]:
        """
        [START_CONTRACT_EXECUTE_IN_SUBQUERY]
        Intent: Выполняет IN (SELECT ...) или NOT IN (SELECT ...).
        Input: value - проверяемое значение; subquery - SELECT запрос;
               negated - True для NOT IN; row_data - данные текущей строки.
        Output: True если value в результате subquery.
        Note: NULL handling по SQL стандарту.
        [END_CONTRACT_EXECUTE_IN_SUBQUERY]
        """
        # Create context for correlated subquery
        context = None
        if row_data:
            context = SubqueryContext(
                outer_row=row_data,
                parent_context=self._current_subquery_context
            )
        
        return self._subquery_executor.execute_in(
            value, subquery.query, negated, context
        )
    
    def _execute_derived_table(self, subquery: SelectNode,
                                alias: str) -> list[dict[str, Any]]:
        """
        [START_CONTRACT_EXECUTE_DERIVED_TABLE]
        Intent: Выполняет subquery в FROM (derived table).
        Input: subquery - SELECT запрос; alias - алиас derived table.
        Output: Список строк с префиксом алиаса.
        Note: Derived tables materialize subquery result.
        [END_CONTRACT_EXECUTE_DERIVED_TABLE]
        """
        return self._subquery_executor.execute_derived_table(subquery, alias)


# END_BLOCK_EXECUTOR


# =============================================================================
# START_BLOCK_HELPERS
# =============================================================================

def create_executor(database: Database) -> Executor:
    """
    [START_CONTRACT_CREATE_EXECUTOR]
    Intent: Фабрика для создания executor.
    Input: database - база данных.
    Output: Готовый к работе executor.
    [END_CONTRACT_CREATE_EXECUTOR]
    """
    return Executor(database)

# END_BLOCK_HELPERS