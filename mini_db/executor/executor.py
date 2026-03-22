# START_MODULE_CONTRACT
# Module: mini_db.executor.executor
# Intent: Выполнение SQL-операторов над базой данных.
#         Executor принимает AST и Database, возвращает ExecutionResult.
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports:
#   - ExecutionResult: результат выполнения оператора
#   - Executor: класс с методом execute(ast, db) -> ExecutionResult
# END_MODULE_MAP

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from mini_db.ast.nodes import (
    ASTNode,
    ComparisonNode,
    CreateIndexNode,
    CreateTableNode,
    DeleteNode,
    ExitNode,
    ExpressionNode,
    IdentifierNode,
    InsertNode,
    LiteralNode,
    LoadNode,
    LogicalNode,
    SaveNode,
    SelectNode,
    StatementNode,
    UpdateNode,
)
from mini_db.storage.database import Database


# START_BLOCK_EXECUTION_RESULT
@dataclass
class ExecutionResult:
    """
    [START_CONTRACT_EXECUTION_RESULT]
    Intent: Результат выполнения SQL-оператора с флагом успеха и данными.
    Output: success=True с message/data при успехе, иначе success=False с error.
    [END_CONTRACT_EXECUTION_RESULT]
    """
    success: bool
    message: str = ""
    data: Optional[list[dict]] = None
    error: Optional[str] = None
# END_BLOCK_EXECUTION_RESULT


# START_BLOCK_EXECUTOR
class Executor:
    """
    [START_CONTRACT_EXECUTOR]
    Intent: Выполнение SQL-операторов над базой данных.
    Input: ast - ASTNode от Parser; db - Database для выполнения.
    Output: ExecutionResult с результатом операции.
    [END_CONTRACT_EXECUTOR]
    """
    
    def execute(self, ast: ASTNode, db: Database) -> ExecutionResult:
        """
        [START_CONTRACT_EXECUTE]
        Intent: Выполнить AST-оператор над базой данных.
        Input: ast - StatementNode (CreateTable, Insert, etc.); db - Database.
        Output: ExecutionResult с успехом/ошибкой.
        [END_CONTRACT_EXECUTE]
        """
        if not isinstance(ast, StatementNode):
            return ExecutionResult(
                success=False,
                error=f"Cannot execute non-statement node: {type(ast).__name__}"
            )
        
        # Dispatch to appropriate handler
        if isinstance(ast, CreateTableNode):
            return self._execute_create_table(ast, db)
        
        if isinstance(ast, CreateIndexNode):
            return self._execute_create_index(ast, db)
        
        if isinstance(ast, InsertNode):
            return self._execute_insert(ast, db)
        
        if isinstance(ast, SelectNode):
            return self._execute_select(ast, db)
        
        if isinstance(ast, UpdateNode):
            return self._execute_update(ast, db)
        
        if isinstance(ast, DeleteNode):
            return self._execute_delete(ast, db)
        
        if isinstance(ast, SaveNode):
            return self._execute_save(ast, db)
        
        if isinstance(ast, LoadNode):
            return self._execute_load(ast, db)
        
        if isinstance(ast, ExitNode):
            return self._execute_exit(ast, db)
        
        # Unsupported statement
        return ExecutionResult(
            success=False,
            error=f"Unsupported statement: {type(ast).__name__}"
        )
    
    def _execute_create_table(
        self,
        ast: CreateTableNode,
        db: Database
    ) -> ExecutionResult:
        """
        [START_CONTRACT_EXEC_CREATE_TABLE]
        Intent: Создать таблицу в базе данных.
        Input: ast - CreateTableNode с именем и колонками; db - Database.
        Output: ExecutionResult с успехом или ошибкой (таблица уже существует).
        [END_CONTRACT_EXEC_CREATE_TABLE]
        """
        success, error = db.create_table(ast.name, ast.columns)
        
        if success:
            return ExecutionResult(
                success=True,
                message=f"Table '{ast.name}' created"
            )
        
        return ExecutionResult(
            success=False,
            error=error
        )
    
    def _execute_create_index(
        self,
        ast: CreateIndexNode,
        db: Database
    ) -> ExecutionResult:
        """
        [START_CONTRACT_EXEC_CREATE_INDEX]
        Intent: Создать индекс на колонке таблицы.
        Input: ast - CreateIndexNode с именем, таблицей и колонкой; db - Database.
        Output: ExecutionResult с успехом или ошибкой.
        [END_CONTRACT_EXEC_CREATE_INDEX]
        """
        success, error = db.create_index(ast.name, ast.table, ast.column)
        
        if success:
            return ExecutionResult(
                success=True,
                message=f"Index '{ast.name}' created on table '{ast.table}'"
            )
        
        return ExecutionResult(
            success=False,
            error=error
        )
    
    def _execute_insert(
        self,
        ast: InsertNode,
        db: Database
    ) -> ExecutionResult:
        """
        [START_CONTRACT_EXEC_INSERT]
        Intent: Вставить строку в таблицу.
        Input: ast - InsertNode с таблицей, колонками и значениями; db - Database.
        Output: ExecutionResult с успехом или ошибкой (таблица не найдена, типы, UNIQUE).
        [END_CONTRACT_EXEC_INSERT]
        """
        # Get table
        table = db.get_table(ast.table)
        if table is None:
            return ExecutionResult(
                success=False,
                error=f"Table '{ast.table}' does not exist"
            )
        
        # Build row dict
        row: dict[str, Any] = {}
        
        if ast.columns:
            # Explicit columns: map values to columns
            for col_name, value in zip(ast.columns, ast.values):
                row[col_name] = value
        else:
            # No columns specified: need all columns in order
            if len(ast.values) != len(table.column_order):
                return ExecutionResult(
                    success=False,
                    error=f"Expected {len(table.column_order)} values, got {len(ast.values)}"
                )
            for col_name, value in zip(table.column_order, ast.values):
                row[col_name] = value
        
        # Validate all required columns are present
        for col_name in table.column_order:
            if col_name not in row:
                # Column not provided - check if it's nullable (NULL is default)
                row[col_name] = None
        
        # Insert into table
        result = table.insert(row)
        
        if result.success:
            return ExecutionResult(
                success=True,
                message=f"1 row inserted"
            )
        
        return ExecutionResult(
            success=False,
            error=result.error
        )
    
    def _execute_select(
        self,
        ast: SelectNode,
        db: Database
    ) -> ExecutionResult:
        """
        [START_CONTRACT_EXEC_SELECT]
        Intent: Выбрать строки из таблицы с опциональной фильтрацией WHERE.
        Input: ast - SelectNode с таблицей, колонками и WHERE; db - Database.
        Output: ExecutionResult с data=list[dict] или ошибкой.
        [END_CONTRACT_EXEC_SELECT]
        """
        # Get table
        table = db.get_table(ast.table)
        if table is None:
            return ExecutionResult(
                success=False,
                error=f"Table '{ast.table}' does not exist"
            )
        
        # Try to use index for simple equality comparison
        index_result = self._try_use_index(ast.where, table)
        
        if index_result is not None:
            # Index was used - filter rows by indices
            row_indices, column, value = index_result
            result_rows = []
            for idx in row_indices:
                if idx < len(table.rows):
                    row = table.rows[idx]
                    if ast.columns is None:
                        result_row = {col: row.get(col) for col in table.column_order}
                    else:
                        result_row = {col: row.get(col) for col in ast.columns}
                    result_rows.append(result_row)
            
            return ExecutionResult(
                success=True,
                message=f"{len(result_rows)} row(s) selected",
                data=result_rows
            )
        
        # No index - use full scan
        def predicate(row: dict) -> bool:
            if ast.where is None:
                return True
            return self._evaluate_expression(ast.where, row)
        
        # Execute select
        result = table.select(predicate, ast.columns)
        
        if result.success:
            return ExecutionResult(
                success=True,
                message=f"{len(result.data)} row(s) selected",
                data=result.data
            )
        
        return ExecutionResult(
            success=False,
            error=result.error
        )
    
    def _try_use_index(
        self,
        where: Optional[ExpressionNode],
        table
    ) -> Optional[tuple[set[int], str, Any]]:
        """
        [START_CONTRACT_TRY_USE_INDEX]
        Intent: Попробовать использовать индекс для WHERE col = value.
        Input: where - ExpressionNode или None; table - Table.
        Output: (row_indices, column, value) если индекс применим, иначе None.
        [END_CONTRACT_TRY_USE_INDEX]
        """
        if where is None:
            return None
        
        # Check if WHERE is a simple equality comparison
        if not isinstance(where, ComparisonNode):
            return None
        
        if where.op != "=":
            return None
        
        # Check if left is identifier and right is literal
        column_name = None
        value = None
        
        if isinstance(where.left, IdentifierNode) and isinstance(where.right, LiteralNode):
            column_name = where.left.name
            value = where.right.value
        elif isinstance(where.right, IdentifierNode) and isinstance(where.left, LiteralNode):
            column_name = where.right.name
            value = where.left.value
        else:
            return None
        
        # Check if there's an index on this column
        index = table.get_index_for_column(column_name)
        if index is None:
            return None
        
        # Use index to get row indices
        row_indices = index.lookup(value)
        return (row_indices, column_name, value)
    
    def _execute_update(
        self,
        ast: UpdateNode,
        db: Database
    ) -> ExecutionResult:
        """
        [START_CONTRACT_EXEC_UPDATE]
        Intent: Атомарное обновление строк в таблице с проверкой UNIQUE.
        Input: ast - UpdateNode с таблицей, assignments и WHERE; db - Database.
        Output: ExecutionResult с успехом или ошибкой (UNIQUE violation).
        [END_CONTRACT_EXEC_UPDATE]
        """
        # Get table
        table = db.get_table(ast.table)
        if table is None:
            return ExecutionResult(
                success=False,
                error=f"Table '{ast.table}' does not exist"
            )
        
        # Build predicate from WHERE clause
        def predicate(row: dict) -> bool:
            if ast.where is None:
                return True
            return self._evaluate_expression(ast.where, row)
        
        # Execute update
        result = table.update(predicate, ast.assignments)
        
        if result.success:
            return ExecutionResult(
                success=True,
                message=f"{result.rows_affected} row(s) updated"
            )
        
        return ExecutionResult(
            success=False,
            error=result.error
        )
    
    def _execute_delete(
        self,
        ast: DeleteNode,
        db: Database
    ) -> ExecutionResult:
        """
        [START_CONTRACT_EXEC_DELETE]
        Intent: Удаление строк из таблицы по условию.
        Input: ast - DeleteNode с таблицей и WHERE; db - Database.
        Output: ExecutionResult с количеством удалённых строк.
        [END_CONTRACT_EXEC_DELETE]
        """
        # Get table
        table = db.get_table(ast.table)
        if table is None:
            return ExecutionResult(
                success=False,
                error=f"Table '{ast.table}' does not exist"
            )
        
        # Build predicate from WHERE clause
        def predicate(row: dict) -> bool:
            if ast.where is None:
                return True
            return self._evaluate_expression(ast.where, row)
        
        # Execute delete
        result = table.delete(predicate)
        
        if result.success:
            return ExecutionResult(
                success=True,
                message=f"{result.rows_affected} row(s) deleted"
            )
        
        return ExecutionResult(
            success=False,
            error=result.error
        )
    
    def _execute_save(
        self,
        ast: SaveNode,
        db: Database
    ) -> ExecutionResult:
        """
        [START_CONTRACT_EXEC_SAVE]
        Intent: Сохранить базу данных в JSON-файл.
        Input: ast - SaveNode с путём к файлу; db - Database.
        Output: ExecutionResult с успехом или ошибкой.
        [END_CONTRACT_EXEC_SAVE]
        """
        success, error = db.save_to_file(ast.filepath)
        
        if success:
            return ExecutionResult(
                success=True,
                message=f"Database saved to '{ast.filepath}'"
            )
        
        return ExecutionResult(
            success=False,
            error=error
        )
    
    def _execute_load(
        self,
        ast: LoadNode,
        db: Database
    ) -> ExecutionResult:
        """
        [START_CONTRACT_EXEC_LOAD]
        Intent: Загрузить базу данных из JSON-файла.
        Input: ast - LoadNode с путём к файлу; db - Database.
        Output: ExecutionResult с успехом или ошибкой.
        [END_CONTRACT_EXEC_LOAD]
        """
        success, error = db.load_from_file(ast.filepath)
        
        if success:
            return ExecutionResult(
                success=True,
                message=f"Database loaded from '{ast.filepath}'"
            )
        
        return ExecutionResult(
            success=False,
            error=error
        )
    
    def _execute_exit(
        self,
        ast: ExitNode,
        db: Database
    ) -> ExecutionResult:
        """
        [START_CONTRACT_EXEC_EXIT]
        Intent: Завершить работу REPL.
        Input: ast - ExitNode; db - Database (игнорируется).
        Output: ExecutionResult с флагом exit=True.
        [END_CONTRACT_EXEC_EXIT]
        """
        return ExecutionResult(
            success=True,
            message="Goodbye!"
        )
    
    # ==================== EXPRESSION EVALUATOR ====================
    
    def _evaluate_expression(
        self,
        expr: ExpressionNode,
        row: dict
    ) -> bool:
        """
        [START_CONTRACT_EVAL_EXPRESSION]
        Intent: Вычислить выражение WHERE для строки.
        Input: expr - ExpressionNode; row - dict с данными строки.
        Output: bool - результат вычисления выражения.
        [END_CONTRACT_EVAL_EXPRESSION]
        """
        if isinstance(expr, LiteralNode):
            return bool(expr.value)
        
        if isinstance(expr, IdentifierNode):
            value = row.get(expr.name)
            return bool(value) if value is not None else False
        
        if isinstance(expr, ComparisonNode):
            return self._evaluate_comparison(expr, row)
        
        if isinstance(expr, LogicalNode):
            return self._evaluate_logical(expr, row)
        
        return False
    
    def _evaluate_comparison(
        self,
        expr: ComparisonNode,
        row: dict
    ) -> bool:
        """
        [START_CONTRACT_EVAL_COMPARISON]
        Intent: Вычислить операцию сравнения (=, !=, <, >).
        Input: expr - ComparisonNode; row - dict с данными строки.
        Output: bool - результат сравнения.
        [END_CONTRACT_EVAL_COMPARISON]
        """
        left_val = self._get_value(expr.left, row)
        right_val = self._get_value(expr.right, row)
        
        # NULL semantics: any comparison with NULL returns False
        if left_val is None or right_val is None:
            return False
        
        op = expr.op
        
        if op == "=":
            return left_val == right_val
        if op == "!=":
            return left_val != right_val
        if op == "<":
            return left_val < right_val
        if op == ">":
            return left_val > right_val
        
        return False
    
    def _evaluate_logical(
        self,
        expr: LogicalNode,
        row: dict
    ) -> bool:
        """
        [START_CONTRACT_EVAL_LOGICAL]
        Intent: Вычислить логическую операцию (AND, OR).
        Input: expr - LogicalNode; row - dict с данными строки.
        Output: bool - результат логической операции.
        [END_CONTRACT_EVAL_LOGICAL]
        """
        left_result = self._evaluate_expression(expr.left, row)
        right_result = self._evaluate_expression(expr.right, row)
        
        if expr.op == "AND":
            return left_result and right_result
        if expr.op == "OR":
            return left_result or right_result
        
        return False
    
    def _get_value(
        self,
        expr: ExpressionNode,
        row: dict
    ) -> Any:
        """
        [START_CONTRACT_GET_VALUE]
        Intent: Получить значение выражения (литерал или идентификатор).
        Input: expr - ExpressionNode; row - dict с данными строки.
        Output: Значение (int, str, bool, None).
        [END_CONTRACT_GET_VALUE]
        """
        if isinstance(expr, LiteralNode):
            return expr.value
        
        if isinstance(expr, IdentifierNode):
            return row.get(expr.name)
        
        return None
# END_BLOCK_EXECUTOR