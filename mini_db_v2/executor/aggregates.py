# START_MODULE_CONTRACT
# Module: mini_db_v2.executor.aggregates
# Intent: Aggregate functions и hash aggregation для GROUP BY.
# Dependencies: typing, dataclasses, mini_db_v2.ast.nodes
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: AggregateExecutor, AggregateResult, HashAggregator
# END_MODULE_MAP

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional, Callable
from mini_db_v2.ast.nodes import (
    AggregateNode, AggregateType, ExpressionNode, ColumnRef,
    BinaryOpNode, LiteralNode
)


# =============================================================================
# START_BLOCK_ERRORS
# =============================================================================

class AggregateError(Exception):
    """Ошибка при вычислении агрегата."""
    pass


# END_BLOCK_ERRORS


# =============================================================================
# START_BLOCK_RESULT
# =============================================================================

@dataclass
class AggregateResult:
    """
    [START_CONTRACT_AGGREGATE_RESULT]
    Intent: Результат вычисления агрегатной функции.
    Input: value - вычисленное значение; count - количество элементов.
    Output: Структура для хранения промежуточного результата.
    [END_CONTRACT_AGGREGATE_RESULT]
    """
    value: Any = None
    count: int = 0
    sum_value: float = 0.0
    min_value: Any = None
    max_value: Any = None
    has_values: bool = False


@dataclass
class GroupResult:
    """
    [START_CONTRACT_GROUP_RESULT]
    Intent: Результат GROUP BY для одной группы.
    Input: key - ключ группы; aggregates - результаты агрегатов.
    Output: Структура для хранения результатов группы.
    [END_CONTRACT_GROUP_RESULT]
    """
    key: tuple
    aggregates: dict[str, AggregateResult] = field(default_factory=dict)
    rows: list[dict] = field(default_factory=list)


# END_BLOCK_RESULT


# =============================================================================
# START_BLOCK_AGGREGATE_FUNCTIONS
# =============================================================================

class AggregateFunctions:
    """
    [START_CONTRACT_AGGREGATE_FUNCTIONS]
    Intent: Статические методы для вычисления агрегатных функций.
    Output: Вычисленные значения COUNT, SUM, AVG, MIN, MAX.
    Note: Все функции игнорируют NULL кроме COUNT(*).
    [END_CONTRACT_AGGREGATE_FUNCTIONS]
    """
    
    @staticmethod
    def count_star(rows: list[dict]) -> int:
        """
        [START_CONTRACT_COUNT_STAR]
        Intent: COUNT(*) - подсчёт всех строк.
        Input: rows - список строк.
        Output: Количество строк (включая NULL).
        [END_CONTRACT_COUNT_STAR]
        """
        return len(rows)
    
    @staticmethod
    def count_column(values: list[Any]) -> int:
        """
        [START_CONTRACT_COUNT_COLUMN]
        Intent: COUNT(col) - подсчёт non-NULL значений.
        Input: values - список значений (может содержать NULL).
        Output: Количество non-NULL значений.
        [END_CONTRACT_COUNT_COLUMN]
        """
        return sum(1 for v in values if v is not None)
    
    @staticmethod
    def sum(values: list[Any]) -> Optional[float]:
        """
        [START_CONTRACT_SUM]
        Intent: SUM(col) - сумма значений.
        Input: values - список значений (NULL игнорируются).
        Output: Сумма или NULL если нет значений.
        [END_CONTRACT_SUM]
        """
        non_null = [v for v in values if v is not None]
        if not non_null:
            return None
        return sum(non_null)
    
    @staticmethod
    def avg(values: list[Any]) -> Optional[float]:
        """
        [START_CONTRACT_AVG]
        Intent: AVG(col) - среднее значение.
        Input: values - список значений (NULL игнорируются).
        Output: Среднее или NULL если нет значений.
        [END_CONTRACT_AVG]
        """
        non_null = [v for v in values if v is not None]
        if not non_null:
            return None
        return sum(non_null) / len(non_null)
    
    @staticmethod
    def min(values: list[Any]) -> Any:
        """
        [START_CONTRACT_MIN]
        Intent: MIN(col) - минимальное значение.
        Input: values - список значений (NULL игнорируются).
        Output: Минимум или NULL если нет значений.
        [END_CONTRACT_MIN]
        """
        non_null = [v for v in values if v is not None]
        if not non_null:
            return None
        return min(non_null)
    
    @staticmethod
    def max(values: list[Any]) -> Any:
        """
        [START_CONTRACT_MAX]
        Intent: MAX(col) - максимальное значение.
        Input: values - список значений (NULL игнорируются).
        Output: Максимум или NULL если нет значений.
        [END_CONTRACT_MAX]
        """
        non_null = [v for v in values if v is not None]
        if not non_null:
            return None
        return max(non_null)


# END_BLOCK_AGGREGATE_FUNCTIONS


# =============================================================================
# START_BLOCK_HASH_AGGREGATOR
# =============================================================================

class HashAggregator:
    """
    [START_CONTRACT_HASH_AGGREGATOR]
    Intent: Hash aggregation для GROUP BY с O(N) сложностью.
    Input: rows - данные; group_by - колонки группировки; aggregates - агрегаты.
    Output: Словарь {group_key: GroupResult}.
    Algorithm:
        1. Для каждой строки вычислить ключ группы
        2. Добавить строку в соответствующую группу
        3. Вычислить агрегаты для каждой группы
    [END_CONTRACT_HASH_AGGREGATOR]
    """
    
    def __init__(self, evaluator: Callable):
        """
        [START_CONTRACT_HASH_AGGREGATOR_INIT]
        Intent: Инициализация с функцией вычисления выражений.
        Input: evaluator - функция (expr, row_data) -> value.
        Output: Готовый к работе агрегатор.
        [END_CONTRACT_HASH_AGGREGATOR_INIT]
        """
        self._evaluator = evaluator
        self._groups: dict[tuple, GroupResult] = {}
    
    def aggregate(
        self,
        rows: list[dict],
        group_by: list[ColumnRef],
        aggregates: list[tuple[str, AggregateNode]]
    ) -> dict[tuple, GroupResult]:
        """
        [START_CONTRACT_AGGREGATE]
        Intent: Выполняет hash aggregation для строк.
        Input: rows - данные; group_by - колонки группировки;
               aggregates - список (alias, AggregateNode).
        Output: Словарь {group_key: GroupResult}.
        Note: Если group_by пуст, все строки в одной группе с ключом ().
        [END_CONTRACT_AGGREGATE]
        """
        self._groups = {}
        
        # Phase 1: Group rows
        for row in rows:
            key = self._compute_group_key(row, group_by)
            if key not in self._groups:
                self._groups[key] = GroupResult(key=key)
            self._groups[key].rows.append(row)
        
        # Phase 2: Compute aggregates for each group
        for key, group in self._groups.items():
            for alias, agg_node in aggregates:
                result = self._compute_aggregate(group.rows, agg_node)
                group.aggregates[alias] = result
        
        return self._groups
    
    def _compute_group_key(self, row: dict, group_by: list[ColumnRef]) -> tuple:
        """
        [START_CONTRACT_COMPUTE_GROUP_KEY]
        Intent: Вычисляет ключ группы для строки.
        Input: row - данные строки; group_by - колонки группировки.
        Output: Кортеж значений колонок группировки.
        Note: NULL значения включаются в ключ как None.
        [END_CONTRACT_COMPUTE_GROUP_KEY]
        """
        if not group_by:
            return ()
        
        key_parts = []
        for col_ref in group_by:
            value = self._evaluator(col_ref, row)
            key_parts.append(value)
        
        return tuple(key_parts)
    
    def _compute_aggregate(
        self,
        rows: list[dict],
        agg_node: AggregateNode
    ) -> AggregateResult:
        """
        [START_CONTRACT_COMPUTE_AGGREGATE]
        Intent: Вычисляет агрегатную функцию для группы.
        Input: rows - строки группы; agg_node - узел агрегата.
        Output: AggregateResult с вычисленным значением.
        [END_CONTRACT_COMPUTE_AGGREGATE]
        """
        result = AggregateResult()
        
        # Extract values for aggregate argument
        values = []
        if agg_node.arg is not None:
            for row in rows:
                value = self._evaluator(agg_node.arg, row)
                values.append(value)
        
        # Compute aggregate based on type
        if agg_node.agg_type == AggregateType.COUNT:
            if agg_node.arg is None:
                # COUNT(*)
                result.value = len(rows)
            else:
                # COUNT(col) - count non-NULL
                result.value = AggregateFunctions.count_column(values)
        
        elif agg_node.agg_type == AggregateType.SUM:
            result.value = AggregateFunctions.sum(values)
        
        elif agg_node.agg_type == AggregateType.AVG:
            result.value = AggregateFunctions.avg(values)
        
        elif agg_node.agg_type == AggregateType.MIN:
            result.value = AggregateFunctions.min(values)
        
        elif agg_node.agg_type == AggregateType.MAX:
            result.value = AggregateFunctions.max(values)
        
        result.has_values = True
        result.count = len(rows)
        
        return result
    
    def get_result_rows(
        self,
        group_by: list[ColumnRef],
        aggregates: list[tuple[str, AggregateNode]]
    ) -> list[dict]:
        """
        [START_CONTRACT_GET_RESULT_ROWS]
        Intent: Преобразует результаты группировки в строки.
        Input: group_by - колонки группировки; aggregates - агрегаты.
        Output: Список словарей с результатами.
        [END_CONTRACT_GET_RESULT_ROWS]
        """
        result_rows = []
        
        for key, group in self._groups.items():
            row = {}
            
            # Add group by columns
            for i, col_ref in enumerate(group_by):
                col_name = col_ref.column_name
                row[col_name] = key[i] if i < len(key) else None
            
            # Add aggregate values
            for alias, agg_node in aggregates:
                if alias in group.aggregates:
                    row[alias] = group.aggregates[alias].value
            
            result_rows.append(row)
        
        return result_rows


# END_BLOCK_HASH_AGGREGATOR


# =============================================================================
# START_BLOCK_AGGREGATE_EXECUTOR
# =============================================================================

class AggregateExecutor:
    """
    [START_CONTRACT_AGGREGATE_EXECUTOR]
    Intent: Выполняет агрегацию для SELECT запросов.
    Input: rows - данные; select_columns - колонки SELECT; group_by - GROUP BY.
    Output: Результаты агрегации.
    Note: Поддерживает implicit aggregation (агрегаты без GROUP BY).
    [END_CONTRACT_AGGREGATE_EXECUTOR]
    """
    
    def __init__(self, evaluator: Callable):
        """
        [START_CONTRACT_AGGREGATE_EXECUTOR_INIT]
        Intent: Инициализация с функцией вычисления выражений.
        Input: evaluator - функция (expr, row_data) -> value.
        Output: Готовый к работе executor.
        [END_CONTRACT_AGGREGATE_EXECUTOR_INIT]
        """
        self._evaluator = evaluator
        self._hash_aggregator = HashAggregator(evaluator)
    
    def has_aggregates(self, columns: list) -> bool:
        """
        [START_CONTRACT_HAS_AGGREGATES]
        Intent: Проверяет, есть ли агрегаты в колонках SELECT.
        Input: columns - список SelectColumn.
        Output: True если есть хотя бы один AggregateNode.
        [END_CONTRACT_HAS_AGGREGATES]
        """
        for col in columns:
            if self._contains_aggregate(col.expression):
                return True
        return False
    
    def _contains_aggregate(self, expr: ExpressionNode) -> bool:
        """Рекурсивно проверяет наличие агрегата в выражении."""
        if isinstance(expr, AggregateNode):
            return True
        # Check nested expressions (e.g., COUNT(*) + 1)
        # For now, only direct aggregates are supported
        return False
    
    def execute(
        self,
        rows: list[dict],
        columns: list,
        group_by: list[ColumnRef],
        having: Optional[ExpressionNode] = None
    ) -> list[dict]:
        """
        [START_CONTRACT_EXECUTE_AGGREGATION]
        Intent: Выполняет агрегацию для SELECT запроса.
        Input: rows - данные; columns - SelectColumn; group_by - GROUP BY;
               having - HAVING условие.
        Output: Список словарей с результатами.
        Algorithm:
            1. Извлечь агрегаты из columns
            2. Если нет GROUP BY и есть агрегаты - implicit aggregation
            3. Иначе - hash aggregation
            4. Применить HAVING фильтр
        [END_CONTRACT_EXECUTE_AGGREGATION]
        """
        # Extract aggregates from columns
        aggregates = self._extract_aggregates(columns)
        
        if not aggregates and not group_by:
            # No aggregation needed
            return rows
        
        # Perform aggregation
        if not group_by and aggregates:
            # Implicit aggregation (all rows in one group)
            return self._implicit_aggregation(rows, aggregates)
        else:
            # GROUP BY aggregation
            return self._group_by_aggregation(rows, group_by, aggregates, having)
    
    def _extract_aggregates(self, columns: list) -> list[tuple[str, AggregateNode]]:
        """
        [START_CONTRACT_EXTRACT_AGGREGATES]
        Intent: Извлекает агрегаты из списка колонок.
        Input: columns - список SelectColumn.
        Output: Список (alias, AggregateNode).
        [END_CONTRACT_EXTRACT_AGGREGATES]
        """
        aggregates = []
        
        for col in columns:
            if isinstance(col.expression, AggregateNode):
                alias = col.alias or self._get_aggregate_name(col.expression)
                aggregates.append((alias, col.expression))
        
        return aggregates
    
    def _get_aggregate_name(self, agg: AggregateNode) -> str:
        """Генерирует имя агрегата для заголовка колонки."""
        if agg.arg is None:
            return f"{agg.agg_type.name}(*)"
        elif isinstance(agg.arg, ColumnRef):
            return f"{agg.agg_type.name}({agg.arg.column_name})"
        else:
            return f"{agg.agg_type.name}(expr)"
    
    def _implicit_aggregation(
        self,
        rows: list[dict],
        aggregates: list[tuple[str, AggregateNode]]
    ) -> list[dict]:
        """
        [START_CONTRACT_IMPLICIT_AGGREGATION]
        Intent: Выполняет агрегацию без GROUP BY (все строки в одной группе).
        Input: rows - данные; aggregates - список агрегатов.
        Output: Одна строка с результатами агрегатов.
        [END_CONTRACT_IMPLICIT_AGGREGATION]
        """
        result = {}
        
        for alias, agg_node in aggregates:
            agg_result = self._hash_aggregator._compute_aggregate(rows, agg_node)
            result[alias] = agg_result.value
        
        return [result]
    
    def _group_by_aggregation(
        self,
        rows: list[dict],
        group_by: list[ColumnRef],
        aggregates: list[tuple[str, AggregateNode]],
        having: Optional[ExpressionNode]
    ) -> list[dict]:
        """
        [START_CONTRACT_GROUP_BY_AGGREGATION]
        Intent: Выполняет GROUP BY агрегацию.
        Input: rows - данные; group_by - колонки группировки;
               aggregates - агрегаты; having - HAVING условие.
        Output: Список строк с результатами по группам.
        [END_CONTRACT_GROUP_BY_AGGREGATION]
        """
        # Perform hash aggregation
        groups = self._hash_aggregator.aggregate(rows, group_by, aggregates)
        
        # Convert to result rows with HAVING filter
        result_rows = []
        for key, group in groups.items():
            row = {}
            
            # Add group by columns
            for i, col_ref in enumerate(group_by):
                col_name = col_ref.column_name
                row[col_name] = key[i] if i < len(key) else None
            
            # Add aggregate values
            for alias, agg_node in aggregates:
                if alias in group.aggregates:
                    row[alias] = group.aggregates[alias].value
            
            # Apply HAVING filter - need to evaluate aggregates in HAVING
            if having:
                if not self._evaluate_having(having, row, group.rows, aggregates):
                    continue
            
            result_rows.append(row)
        
        return result_rows
    
    def _evaluate_having(
        self,
        having: ExpressionNode,
        row: dict,
        group_rows: list[dict],
        aggregates: list[tuple[str, AggregateNode]]
    ) -> bool:
        """
        [START_CONTRACT_EVALUATE_HAVING]
        Intent: Вычисляет HAVING условие с поддержкой агрегатов.
        Input: having - выражение HAVING; row - текущая строка результата;
               group_rows - строки группы; aggregates - список агрегатов.
        Output: True если условие выполняется.
        [END_CONTRACT_EVALUATE_HAVING]
        """
        # For HAVING, we need to evaluate aggregates in the condition
        # Create a modified evaluator that can compute aggregates
        if isinstance(having, BinaryOpNode):
            left = self._evaluate_having_value(having.left, row, group_rows, aggregates)
            right = self._evaluate_having_value(having.right, row, group_rows, aggregates)
            
            from mini_db_v2.ast.nodes import BinaryOperator
            if having.operator == BinaryOperator.GT:
                return left > right
            elif having.operator == BinaryOperator.GE:
                return left >= right
            elif having.operator == BinaryOperator.LT:
                return left < right
            elif having.operator == BinaryOperator.LE:
                return left <= right
            elif having.operator == BinaryOperator.EQ:
                return left == right
            elif having.operator == BinaryOperator.NE:
                return left != right
            elif having.operator == BinaryOperator.AND:
                return left and right
            elif having.operator == BinaryOperator.OR:
                return left or right
        
        # Fallback to regular evaluator
        return bool(self._evaluator(having, row))
    
    def _evaluate_having_value(
        self,
        expr: ExpressionNode,
        row: dict,
        group_rows: list[dict],
        aggregates: list[tuple[str, AggregateNode]]
    ) -> Any:
        """Вычисляет значение выражения в HAVING."""
        if isinstance(expr, AggregateNode):
            # Compute aggregate for this group
            result = self._hash_aggregator._compute_aggregate(group_rows, expr)
            return result.value
        elif isinstance(expr, ColumnRef):
            # Column reference - check row first
            col_name = expr.column_name
            if col_name in row:
                return row[col_name]
            return self._evaluator(expr, row)
        elif isinstance(expr, BinaryOpNode):
            # Handle nested binary operations (AND/OR in HAVING)
            left = self._evaluate_having_value(expr.left, row, group_rows, aggregates)
            right = self._evaluate_having_value(expr.right, row, group_rows, aggregates)
            
            from mini_db_v2.ast.nodes import BinaryOperator
            if expr.operator == BinaryOperator.AND:
                return left and right
            elif expr.operator == BinaryOperator.OR:
                return left or right
            elif expr.operator == BinaryOperator.GT:
                return left > right
            elif expr.operator == BinaryOperator.GE:
                return left >= right
            elif expr.operator == BinaryOperator.LT:
                return left < right
            elif expr.operator == BinaryOperator.LE:
                return left <= right
            elif expr.operator == BinaryOperator.EQ:
                return left == right
            elif expr.operator == BinaryOperator.NE:
                return left != right
            else:
                return self._evaluator(expr, row)
        elif hasattr(expr, 'value'):  # LiteralNode
            return expr.value
        else:
            return self._evaluator(expr, row)


# END_BLOCK_AGGREGATE_EXECUTOR


# =============================================================================
# START_BLOCK_DISTINCT
# =============================================================================

class DistinctExecutor:
    """
    [START_CONTRACT_DISTINCT_EXECUTOR]
    Intent: Выполняет DISTINCT операцию.
    Input: rows - данные для дедупликации.
    Output: Уникальные строки.
    [END_CONTRACT_DISTINCT_EXECUTOR]
    """
    
    @staticmethod
    def apply_distinct(rows: list[dict]) -> list[dict]:
        """
        [START_CONTRACT_APPLY_DISTINCT]
        Intent: Удаляет дубликаты из списка строк.
        Input: rows - список словарей.
        Output: Список уникальных строк.
        Algorithm: Использует set с tuple ключами для O(N).
        [END_CONTRACT_APPLY_DISTINCT]
        """
        seen = set()
        unique_rows = []
        
        for row in rows:
            # Create hashable key from row
            key = tuple(sorted((k, v if not isinstance(v, (list, dict)) else str(v)) 
                              for k, v in row.items()))
            if key not in seen:
                seen.add(key)
                unique_rows.append(row)
        
        return unique_rows


# END_BLOCK_DISTINCT