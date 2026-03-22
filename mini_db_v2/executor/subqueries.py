# START_MODULE_CONTRACT
# Module: mini_db_v2.executor.subqueries
# Intent: Subquery executor для всех типов подзапросов (scalar, IN, EXISTS, correlated, derived tables).
# Dependencies: typing, mini_db_v2.ast.nodes, mini_db_v2.executor.executor
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: SubqueryExecutor, SubqueryError, SubqueryContext
# END_MODULE_MAP

from __future__ import annotations
from typing import Any, Optional, Callable, TYPE_CHECKING
from dataclasses import dataclass, field

if TYPE_CHECKING:
    from mini_db_v2.executor.executor import Executor
    from mini_db_v2.ast.nodes import (
        SelectNode, ExpressionNode, SubqueryNode, ExistsNode,
        BinaryOpNode, BinaryOperator, ColumnRef
    )


# =============================================================================
# START_BLOCK_ERRORS
# =============================================================================

class SubqueryError(Exception):
    """
    [START_CONTRACT_SUBQUERY_ERROR]
    Intent: Базовая ошибка выполнения subquery.
    Input: message - описание ошибки.
    Output: Исключение для обработки.
    [END_CONTRACT_SUBQUERY_ERROR]
    """
    pass


class ScalarSubqueryError(SubqueryError):
    """Scalar subquery вернул больше одной строки."""
    pass


class CorrelatedSubqueryError(SubqueryError):
    """Ошибка в correlated subquery."""
    pass

# END_BLOCK_ERRORS


# =============================================================================
# START_BLOCK_CONTEXT
# =============================================================================

@dataclass
class SubqueryContext:
    """
    [START_CONTRACT_SUBQUERY_CONTEXT]
    Intent: Контекст выполнения subquery с outer row данными.
    Input: outer_row - данные внешней строки; outer_alias - алиас внешней таблицы.
    Output: Контекст для разрешения correlated references.
    [END_CONTRACT_SUBQUERY_CONTEXT]
    """
    outer_row: dict[str, Any] = field(default_factory=dict)
    outer_alias: Optional[str] = None
    outer_table_name: Optional[str] = None
    parent_context: Optional[SubqueryContext] = None
    
    def resolve_column(self, column_ref: ColumnRef) -> Any:
        """
        [START_CONTRACT_RESOLVE_COLUMN]
        Intent: Разрешает ссылку на колонку из outer query.
        Input: column_ref - ссылка на колонку.
        Output: Значение колонки или None если не найдена.
        [END_CONTRACT_RESOLVE_COLUMN]
        """
        col_name = column_ref.column_name
        table_alias = column_ref.table_alias
        
        # Try with table alias first
        if table_alias:
            key = f"{table_alias}.{col_name}"
            if key in self.outer_row:
                return self.outer_row[key]
            # Try direct match
            if self.outer_alias == table_alias and col_name in self.outer_row:
                return self.outer_row[col_name]
        
        # Try direct column name
        if col_name in self.outer_row:
            return self.outer_row[col_name]
        
        # Try with any prefix
        for key, value in self.outer_row.items():
            if key.endswith(f".{col_name}"):
                return value
        
        # Try parent context (nested subqueries)
        if self.parent_context:
            return self.parent_context.resolve_column(column_ref)
        
        return None

# END_BLOCK_CONTEXT


# =============================================================================
# START_BLOCK_EXECUTOR
# =============================================================================

class SubqueryExecutor:
    """
    [START_CONTRACT_SUBQUERY_EXECUTOR]
    Intent: Выполняет все типы subqueries: scalar, IN, EXISTS, correlated, derived tables.
    Input: executor - главный executor для выполнения SELECT; context - контекст outer query.
    Output: Результат выполнения subquery.
    Note: Поддерживает correlated subqueries через SubqueryContext.
    [END_CONTRACT_SUBQUERY_EXECUTOR]
    """
    
    def __init__(self, executor: Executor):
        """
        [START_CONTRACT_SUBQUERY_EXECUTOR_INIT]
        Intent: Инициализация SubqueryExecutor с ссылкой на главный executor.
        Input: executor - Executor для выполнения SELECT запросов.
        Output: Готовый к работе SubqueryExecutor.
        [END_CONTRACT_SUBQUERY_EXECUTOR_INIT]
        """
        self.executor = executor
        self._context: Optional[SubqueryContext] = None
    
    def set_context(self, context: Optional[SubqueryContext]) -> None:
        """
        [START_CONTRACT_SET_CONTEXT]
        Intent: Устанавливает контекст для correlated subquery.
        Input: context - контекст с outer row данными.
        Output: None (сохраняет контекст для последующих вызовов).
        [END_CONTRACT_SET_CONTEXT]
        """
        self._context = context
    
    def execute_scalar(self, subquery: SelectNode, context: Optional[SubqueryContext] = None) -> Any:
        """
        [START_CONTRACT_EXECUTE_SCALAR]
        Intent: Выполняет scalar subquery и возвращает одно значение.
        Input: subquery - SELECT запрос; context - контекст для correlated references.
        Output: Одно значение или None если subquery пустой.
        Raises: ScalarSubqueryError если subquery вернул больше одной строки.
        Note: Scalar subquery должен возвращать ровно одну строку и одну колонку.
        [END_CONTRACT_EXECUTE_SCALAR]
        """
        old_context = self._context
        if context:
            self._context = context
        
        try:
            # Execute subquery
            result = self._execute_subquery_with_context(subquery)
            
            # Check row count
            if len(result) == 0:
                return None
            
            if len(result) > 1:
                raise ScalarSubqueryError(
                    f"Scalar subquery returned {len(result)} rows, expected 1"
                )
            
            # Return single value
            row = result[0]
            if len(row) != 1:
                raise ScalarSubqueryError(
                    f"Scalar subquery returned {len(row)} columns, expected 1"
                )
            
            return list(row.values())[0]
        finally:
            self._context = old_context
    
    def execute_in(self, value: Any, subquery: SelectNode, 
                   negated: bool = False, context: Optional[SubqueryContext] = None) -> bool:
        """
        [START_CONTRACT_EXECUTE_IN]
        Intent: Выполняет IN (SELECT ...) или NOT IN (SELECT ...).
        Input: value - проверяемое значение; subquery - SELECT запрос; 
               negated - True для NOT IN; context - контекст для correlated references.
        Output: True если value в результате subquery (или НЕ в для NOT IN).
        Note: NULL handling: если value IS NULL или subquery содержит NULL, результат может быть NULL.
        [END_CONTRACT_EXECUTE_IN]
        """
        old_context = self._context
        if context:
            self._context = context
        
        try:
            # Execute subquery
            result = self._execute_subquery_with_context(subquery)
            
            # Extract values from first column
            subquery_values = set()
            has_null = False
            
            for row in result:
                if len(row) > 0:
                    val = list(row.values())[0]
                    if val is None:
                        has_null = True
                    else:
                        subquery_values.add(val)
            
            # NULL handling per SQL standard
            if value is None:
                return None  # NULL IN (...) is NULL (unknown)
            
            # Check membership
            result_bool = value in subquery_values
            
            # If not found and subquery has NULL, result is unknown
            if not result_bool and has_null:
                return None
            
            return not result_bool if negated else result_bool
        finally:
            self._context = old_context
    
    def execute_exists(self, subquery: SelectNode, negated: bool = False,
                       context: Optional[SubqueryContext] = None) -> bool:
        """
        [START_CONTRACT_EXECUTE_EXISTS]
        Intent: Выполняет EXISTS или NOT EXISTS.
        Input: subquery - SELECT запрос; negated - True для NOT EXISTS;
               context - контекст для correlated references.
        Output: True если subquery вернул хотя бы одну строку.
        Note: EXISTS не зависит от того, какие колонки в SELECT - только наличие строк.
        [END_CONTRACT_EXECUTE_EXISTS]
        """
        old_context = self._context
        if context:
            self._context = context
        
        try:
            # Execute subquery
            result = self._execute_subquery_with_context(subquery)
            
            # EXISTS is true if any rows returned
            exists = len(result) > 0
            
            return not exists if negated else exists
        finally:
            self._context = old_context
    
    def execute_correlated(self, subquery: SelectNode, outer_row: dict[str, Any],
                          outer_alias: Optional[str] = None) -> Any:
        """
        [START_CONTRACT_EXECUTE_CORRELATED]
        Intent: Выполняет correlated subquery с outer reference.
        Input: subquery - SELECT запрос; outer_row - данные внешней строки;
               outer_alias - алиас внешней таблицы.
        Output: Результат subquery (scalar или список для IN).
        Note: Correlated subquery выполняется для каждой строки outer query.
        [END_CONTRACT_EXECUTE_CORRELATED]
        """
        context = SubqueryContext(
            outer_row=outer_row,
            outer_alias=outer_alias,
            parent_context=self._context
        )
        
        return self.execute_scalar(subquery, context)
    
    def execute_derived_table(self, subquery: SelectNode, alias: str) -> list[dict[str, Any]]:
        """
        [START_CONTRACT_EXECUTE_DERIVED_TABLE]
        Intent: Выполняет subquery в FROM (derived table).
        Input: subquery - SELECT запрос; alias - алиас derived table.
        Output: Список строк с префиксом алиаса в именах колонок.
        Note: Derived table materializes subquery result.
        [END_CONTRACT_EXECUTE_DERIVED_TABLE]
        """
        # Execute subquery without context (derived tables are independent)
        result = self._execute_subquery_with_context(subquery)
        
        # Prefix column names with alias
        prefixed_result = []
        for row in result:
            prefixed_row = {}
            for col_name, value in row.items():
                # Add both prefixed and non-prefixed versions
                prefixed_row[f"{alias}.{col_name}"] = value
                prefixed_row[col_name] = value
            prefixed_result.append(prefixed_row)
        
        return prefixed_result
    
    def _execute_subquery_with_context(self, subquery: SelectNode) -> list[dict[str, Any]]:
        """
        [START_CONTRACT_EXECUTE_SUBQUERY_WITH_CONTEXT]
        Intent: Выполняет subquery с учётом correlated references.
        Input: subquery - SELECT запрос.
        Output: Список строк результата.
        Note: Модифицирует WHERE для подстановки outer values.
        [END_CONTRACT_EXECUTE_SUBQUERY_WITH_CONTEXT]
        """
        # If no context, execute directly
        if self._context is None:
            return self._execute_select_direct(subquery)
        
        # Check if subquery has correlated references
        if self._has_correlated_references(subquery):
            return self._execute_correlated_subquery(subquery)
        
        # No correlation - execute directly
        return self._execute_select_direct(subquery)
    
    def _has_correlated_references(self, subquery: SelectNode) -> bool:
        """
        [START_CONTRACT_HAS_CORRELATED_REFERENCES]
        Intent: Проверяет, есть ли в subquery correlated references.
        Input: subquery - SELECT запрос.
        Output: True если есть ссылки на outer query.
        [END_CONTRACT_HAS_CORRELATED_REFERENCES]
        """
        if self._context is None:
            return False
        
        # Check WHERE clause for correlated references
        if subquery.where:
            if self._expression_has_correlated_ref(subquery.where, subquery.from_clause):
                return True
        
        # Check SELECT columns for correlated references
        for col in subquery.columns:
            if self._expression_has_correlated_ref(col.expression, subquery.from_clause):
                return True
        
        return False
    
    def _expression_has_correlated_ref(self, expr: ExpressionNode, 
                                        from_clause: Optional[Any]) -> bool:
        """
        [START_CONTRACT_EXPRESSION_HAS_CORRELATED_REF]
        Intent: Проверяет выражение на наличие correlated references.
        Input: expr - выражение; from_clause - FROM clause subquery.
        Output: True если есть correlated reference.
        [END_CONTRACT_EXPRESSION_HAS_CORRELATED_REF]
        """
        from mini_db_v2.ast.nodes import ColumnRef, BinaryOpNode, UnaryOpNode
        
        if isinstance(expr, ColumnRef):
            # Check if column is from outer query
            table_alias = expr.table_alias
            col_name = expr.column_name
            
            # If has table alias, check if it matches outer context
            if table_alias:
                if self._context.outer_alias == table_alias:
                    return True
                if self._context.outer_table_name == table_alias:
                    return True
                # Check if column exists in outer row
                key = f"{table_alias}.{col_name}"
                if key in self._context.outer_row:
                    return True
            else:
                # No table alias - check if column exists in outer row
                if col_name in self._context.outer_row:
                    # But also check if it's NOT in the subquery's own tables
                    if from_clause:
                        table_name = from_clause.table.table_name
                        # This is a simplified check - in real DB we'd resolve properly
                        # For now, assume column without alias in outer_row is correlated
                        # if it's not in the subquery's table
                        return True
            
            return False
        
        if isinstance(expr, BinaryOpNode):
            return (self._expression_has_correlated_ref(expr.left, from_clause) or
                    self._expression_has_correlated_ref(expr.right, from_clause))
        
        if isinstance(expr, UnaryOpNode):
            return self._expression_has_correlated_ref(expr.operand, from_clause)
        
        return False
    
    def _execute_correlated_subquery(self, subquery: SelectNode) -> list[dict[str, Any]]:
        """
        [START_CONTRACT_EXECUTE_CORRELATED_SUBQUERY]
        Intent: Выполняет correlated subquery с подстановкой outer values.
        Input: subquery - SELECT запрос с correlated references.
        Output: Список строк результата.
        Note: Создаёт модифицированный SELECT с подставленными значениями.
        [END_CONTRACT_EXECUTE_CORRELATED_SUBQUERY]
        """
        # Clone subquery and substitute correlated references
        modified_where = self._substitute_correlated_refs(subquery.where, subquery.from_clause)
        
        # Create modified subquery
        from copy import deepcopy
        modified_subquery = deepcopy(subquery)
        modified_subquery.where = modified_where
        
        # Execute modified subquery
        return self._execute_select_direct(modified_subquery)
    
    def _substitute_correlated_refs(self, expr: Optional[ExpressionNode],
                                     from_clause: Optional[Any]) -> Optional[ExpressionNode]:
        """
        [START_CONTRACT_SUBSTITUTE_CORRELATED_REFS]
        Intent: Подставляет значения outer query в выражение.
        Input: expr - выражение; from_clause - FROM clause.
        Output: Модифицированное выражение с подставленными значениями.
        [END_CONTRACT_SUBSTITUTE_CORRELATED_REFS]
        """
        from mini_db_v2.ast.nodes import ColumnRef, BinaryOpNode, UnaryOpNode, LiteralNode, DataType
        
        if expr is None:
            return None
        
        if isinstance(expr, ColumnRef):
            # Check if this is a correlated reference
            value = self._context.resolve_column(expr)
            if value is not None or self._is_correlated_column(expr, from_clause):
                # Substitute with literal value
                data_type = DataType.NULL
                if isinstance(value, int):
                    data_type = DataType.INT
                elif isinstance(value, float):
                    data_type = DataType.REAL
                elif isinstance(value, str):
                    data_type = DataType.TEXT
                elif isinstance(value, bool):
                    data_type = DataType.BOOL
                
                return LiteralNode(value=value, data_type=data_type)
            return expr
        
        if isinstance(expr, BinaryOpNode):
            new_left = self._substitute_correlated_refs(expr.left, from_clause)
            new_right = self._substitute_correlated_refs(expr.right, from_clause)
            return BinaryOpNode(left=new_left, operator=expr.operator, right=new_right)
        
        if isinstance(expr, UnaryOpNode):
            new_operand = self._substitute_correlated_refs(expr.operand, from_clause)
            return UnaryOpNode(operand=new_operand, operator=expr.operator)
        
        return expr
    
    def _is_correlated_column(self, column_ref: ColumnRef, from_clause: Optional[Any]) -> bool:
        """
        [START_CONTRACT_IS_CORRELATED_COLUMN]
        Intent: Проверяет, является ли колонка correlated reference.
        Input: column_ref - ссылка на колонку; from_clause - FROM clause.
        Output: True если колонка из outer query.
        [END_CONTRACT_IS_CORRELATED_COLUMN]
        """
        if self._context is None:
            return False
        
        col_name = column_ref.column_name
        table_alias = column_ref.table_alias
        
        # Check with table alias
        if table_alias:
            key = f"{table_alias}.{col_name}"
            if key in self._context.outer_row:
                return True
            if self._context.outer_alias == table_alias:
                return True
        else:
            # Check if column exists in outer row
            if col_name in self._context.outer_row:
                return True
            # Check with any prefix
            for key in self._context.outer_row:
                if key.endswith(f".{col_name}"):
                    return True
        
        return False
    
    def _execute_select_direct(self, subquery: SelectNode) -> list[dict[str, Any]]:
        """
        [START_CONTRACT_EXECUTE_SELECT_DIRECT]
        Intent: Выполняет SELECT напрямую через executor.
        Input: subquery - SELECT запрос.
        Output: Список строк результата.
        [END_CONTRACT_EXECUTE_SELECT_DIRECT]
        """
        result = self.executor._execute_select(subquery)
        return result.rows

# END_BLOCK_EXECUTOR