# START_MODULE_CONTRACT
# Module: mini_db_v2.executor.joins
# Intent: JOIN algorithms для SQL execution (Nested Loop, Hash Join, Merge Join).
# Dependencies: typing, mini_db_v2.ast.nodes
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: JoinExecutor, JoinResult, JoinType
# END_MODULE_MAP

from __future__ import annotations
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from mini_db_v2.ast.nodes import (
    ExpressionNode, ColumnRef, BinaryOpNode, BinaryOperator,
    JoinType as ASTJoinType
)


# =============================================================================
# START_BLOCK_RESULT
# =============================================================================

@dataclass
class JoinResult:
    """
    [START_CONTRACT_JOIN_RESULT]
    Intent: Результат JOIN операции.
    Input: rows - список объединённых строк; row_count - количество.
    Output: Структура для возврата результата JOIN.
    [END_CONTRACT_JOIN_RESULT]
    """
    rows: list[dict[str, Any]] = field(default_factory=list)
    row_count: int = 0


# END_BLOCK_RESULT


# =============================================================================
# START_BLOCK_JOIN_EXECUTOR
# =============================================================================

class JoinExecutor:
    """
    [START_CONTRACT_JOIN_EXECUTOR]
    Intent: Выполнение JOIN операций с различными алгоритмами.
    Input: outer_rows, inner_rows - данные таблиц; condition - условие JOIN.
    Output: JoinResult с объединёнными строками.
    Algorithms:
        - nested_loop_join: O(M*N) - для малых таблиц
        - hash_join: O(M+N) - для equality joins
        - merge_join: O(M+N) - для sorted inputs
    [END_CONTRACT_JOIN_EXECUTOR]
    """
    
    def __init__(self, null_value: Any = None):
        """
        [START_CONTRACT_JOIN_EXECUTOR_INIT]
        Intent: Инициализация executor с настройками NULL.
        Input: null_value - значение для NULL в результатах.
        Output: Готовый к работе JoinExecutor.
        [END_CONTRACT_JOIN_EXECUTOR_INIT]
        """
        self.null_value = null_value
    
    # =========================================================================
    # MAIN JOIN ENTRY POINT
    # =========================================================================
    
    def execute_join(
        self,
        join_type: ASTJoinType,
        outer_rows: list[dict[str, Any]],
        inner_rows: list[dict[str, Any]],
        outer_alias: str,
        inner_alias: str,
        condition: Optional[ExpressionNode] = None,
        evaluator: Optional[Callable] = None
    ) -> JoinResult:
        """
        [START_CONTRACT_EXECUTE_JOIN]
        Intent: Выполняет JOIN указанного типа.
        Input: join_type - тип JOIN; outer/inner_rows - данные; condition - условие.
        Output: JoinResult с объединёнными строками.
        [END_CONTRACT_EXECUTE_JOIN]
        """
        if join_type == ASTJoinType.CROSS:
            return self.cross_join(outer_rows, inner_rows, outer_alias, inner_alias)
        
        if join_type == ASTJoinType.INNER:
            return self._execute_inner_join(
                outer_rows, inner_rows, outer_alias, inner_alias, condition, evaluator
            )
        elif join_type == ASTJoinType.LEFT:
            return self._execute_left_join(
                outer_rows, inner_rows, outer_alias, inner_alias, condition, evaluator
            )
        elif join_type == ASTJoinType.RIGHT:
            return self._execute_right_join(
                outer_rows, inner_rows, outer_alias, inner_alias, condition, evaluator
            )
        elif join_type == ASTJoinType.FULL:
            return self._execute_full_join(
                outer_rows, inner_rows, outer_alias, inner_alias, condition, evaluator
            )
        else:
            # Default to inner join
            return self._execute_inner_join(
                outer_rows, inner_rows, outer_alias, inner_alias, condition, evaluator
            )
    
    # =========================================================================
    # CROSS JOIN
    # =========================================================================
    
    def cross_join(
        self,
        outer_rows: list[dict[str, Any]],
        inner_rows: list[dict[str, Any]],
        outer_alias: str,
        inner_alias: str
    ) -> JoinResult:
        """
        [START_CONTRACT_CROSS_JOIN]
        Intent: Декартово произведение двух таблиц.
        Input: outer_rows, inner_rows - данные таблиц.
        Output: JoinResult с M*N строками.
        [END_CONTRACT_CROSS_JOIN]
        """
        result = []
        
        for outer_row in outer_rows:
            for inner_row in inner_rows:
                merged = self._merge_rows(outer_row, inner_row, outer_alias, inner_alias)
                result.append(merged)
        
        return JoinResult(rows=result, row_count=len(result))
    
    # =========================================================================
    # INNER JOIN
    # =========================================================================
    
    def _execute_inner_join(
        self,
        outer_rows: list[dict[str, Any]],
        inner_rows: list[dict[str, Any]],
        outer_alias: str,
        inner_alias: str,
        condition: Optional[ExpressionNode],
        evaluator: Optional[Callable]
    ) -> JoinResult:
        """
        [START_CONTRACT_INNER_JOIN]
        Intent: INNER JOIN - только совпадающие строки.
        Input: outer/inner_rows - данные; condition - условие ON.
        Output: JoinResult с совпадающими строками.
        [END_CONTRACT_INNER_JOIN]
        """
        # Try to extract equality condition for hash join
        join_keys = self._extract_join_keys(condition, outer_alias, inner_alias)
        
        if join_keys:
            outer_key, inner_key = join_keys
            return self.hash_join(
                outer_rows, inner_rows, outer_alias, inner_alias,
                outer_key, inner_key
            )
        else:
            # Fall back to nested loop for complex conditions
            return self.nested_loop_join(
                outer_rows, inner_rows, outer_alias, inner_alias,
                condition, evaluator, ASTJoinType.INNER
            )
    
    # =========================================================================
    # LEFT JOIN
    # =========================================================================
    
    def _execute_left_join(
        self,
        outer_rows: list[dict[str, Any]],
        inner_rows: list[dict[str, Any]],
        outer_alias: str,
        inner_alias: str,
        condition: Optional[ExpressionNode],
        evaluator: Optional[Callable]
    ) -> JoinResult:
        """
        [START_CONTRACT_LEFT_JOIN]
        Intent: LEFT JOIN - все строки из left + совпадающие из right.
        Input: outer/inner_rows - данные; condition - условие ON.
        Output: JoinResult с NULL для несовпадающих строк right.
        [END_CONTRACT_LEFT_JOIN]
        """
        join_keys = self._extract_join_keys(condition, outer_alias, inner_alias)
        
        if join_keys:
            outer_key, inner_key = join_keys
            return self.hash_join_left(
                outer_rows, inner_rows, outer_alias, inner_alias,
                outer_key, inner_key
            )
        else:
            return self.nested_loop_join(
                outer_rows, inner_rows, outer_alias, inner_alias,
                condition, evaluator, ASTJoinType.LEFT
            )
    
    # =========================================================================
    # RIGHT JOIN
    # =========================================================================
    
    def _execute_right_join(
        self,
        outer_rows: list[dict[str, Any]],
        inner_rows: list[dict[str, Any]],
        outer_alias: str,
        inner_alias: str,
        condition: Optional[ExpressionNode],
        evaluator: Optional[Callable]
    ) -> JoinResult:
        """
        [START_CONTRACT_RIGHT_JOIN]
        Intent: RIGHT JOIN - все строки из right + совпадающие из left.
        Input: outer/inner_rows - данные; condition - условие ON.
        Output: JoinResult с NULL для несовпадающих строк left.
        [END_CONTRACT_RIGHT_JOIN]
        """
        # RIGHT JOIN = LEFT JOIN with swapped tables
        join_keys = self._extract_join_keys(condition, outer_alias, inner_alias)
        
        if join_keys:
            outer_key, inner_key = join_keys
            # Swap keys for right join
            return self.hash_join_left(
                inner_rows, outer_rows, inner_alias, outer_alias,
                inner_key, outer_key
            )
        else:
            return self.nested_loop_join(
                inner_rows, outer_rows, inner_alias, outer_alias,
                condition, evaluator, ASTJoinType.RIGHT,
                swapped=True
            )
    
    # =========================================================================
    # FULL OUTER JOIN
    # =========================================================================
    
    def _execute_full_join(
        self,
        outer_rows: list[dict[str, Any]],
        inner_rows: list[dict[str, Any]],
        outer_alias: str,
        inner_alias: str,
        condition: Optional[ExpressionNode],
        evaluator: Optional[Callable]
    ) -> JoinResult:
        """
        [START_CONTRACT_FULL_JOIN]
        Intent: FULL OUTER JOIN - все строки из обеих таблиц.
        Input: outer/inner_rows - данные; condition - условие ON.
        Output: JoinResult с NULL для несовпадающих строк с обеих сторон.
        [END_CONTRACT_FULL_JOIN]
        """
        join_keys = self._extract_join_keys(condition, outer_alias, inner_alias)
        
        if join_keys:
            outer_key, inner_key = join_keys
            return self.hash_join_full(
                outer_rows, inner_rows, outer_alias, inner_alias,
                outer_key, inner_key
            )
        else:
            return self.nested_loop_join(
                outer_rows, inner_rows, outer_alias, inner_alias,
                condition, evaluator, ASTJoinType.FULL
            )
    
    # =========================================================================
    # NESTED LOOP JOIN (O(M*N))
    # =========================================================================
    
    def nested_loop_join(
        self,
        outer_rows: list[dict[str, Any]],
        inner_rows: list[dict[str, Any]],
        outer_alias: str,
        inner_alias: str,
        condition: Optional[ExpressionNode],
        evaluator: Optional[Callable],
        join_type: ASTJoinType = ASTJoinType.INNER,
        swapped: bool = False
    ) -> JoinResult:
        """
        [START_CONTRACT_NESTED_LOOP_JOIN]
        Intent: Nested Loop Join - O(M*N) алгоритм для малых таблиц.
        Input: outer/inner_rows - данные; condition - условие ON.
        Output: JoinResult с объединёнными строками.
        Note: Универсальный алгоритм для любых условий JOIN.
        [END_CONTRACT_NESTED_LOOP_JOIN]
        """
        result = []
        matched_inner = set()  # For FULL/RIGHT joins
        
        for i, outer_row in enumerate(outer_rows):
            matched = False
            
            for j, inner_row in enumerate(inner_rows):
                merged = self._merge_rows(outer_row, inner_row, outer_alias, inner_alias)
                
                # Check condition
                if condition and evaluator:
                    try:
                        condition_result = evaluator(condition, merged)
                        if condition_result:
                            result.append(merged)
                            matched = True
                            matched_inner.add(j)
                    except Exception:
                        # Condition evaluation failed - treat as no match
                        pass
                elif condition is None:
                    # No condition - cross join behavior
                    result.append(merged)
                    matched = True
                    matched_inner.add(j)
            
            # Handle LEFT/FULL JOIN - add outer row with NULLs if no match
            if not matched and join_type in (ASTJoinType.LEFT, ASTJoinType.FULL):
                null_inner = self._create_null_row(inner_rows[0] if inner_rows else {}, inner_alias)
                merged = self._merge_rows(outer_row, null_inner, outer_alias, inner_alias)
                result.append(merged)
        
        # Handle FULL/RIGHT JOIN - add inner rows with NULLs if no match
        if join_type in (ASTJoinType.FULL, ASTJoinType.RIGHT):
            for j, inner_row in enumerate(inner_rows):
                if j not in matched_inner:
                    null_outer = self._create_null_row(outer_rows[0] if outer_rows else {}, outer_alias)
                    merged = self._merge_rows(null_outer, inner_row, outer_alias, inner_alias)
                    result.append(merged)
        
        return JoinResult(rows=result, row_count=len(result))
    
    # =========================================================================
    # HASH JOIN (O(M+N))
    # =========================================================================
    
    def hash_join(
        self,
        outer_rows: list[dict[str, Any]],
        inner_rows: list[dict[str, Any]],
        outer_alias: str,
        inner_alias: str,
        outer_key: str,
        inner_key: str
    ) -> JoinResult:
        """
        [START_CONTRACT_HASH_JOIN]
        Intent: Hash Join - O(M+N) для equality joins.
        Input: outer/inner_rows - данные; outer/inner_key - ключи JOIN.
        Output: JoinResult с совпадающими строками.
        Algorithm:
            1. Build hash table from inner (smaller) table
            2. Probe with outer (larger) table
        [END_CONTRACT_HASH_JOIN]
        """
        result = []
        
        # Build phase: create hash table from inner
        hash_table: dict[Any, list[dict[str, Any]]] = {}
        for inner_row in inner_rows:
            key = self._get_key_value(inner_row, inner_key, inner_alias)
            if key is not None:
                if key not in hash_table:
                    hash_table[key] = []
                hash_table[key].append(inner_row)
        
        # Probe phase: match outer rows
        for outer_row in outer_rows:
            key = self._get_key_value(outer_row, outer_key, outer_alias)
            if key is not None and key in hash_table:
                for inner_row in hash_table[key]:
                    merged = self._merge_rows(outer_row, inner_row, outer_alias, inner_alias)
                    result.append(merged)
        
        return JoinResult(rows=result, row_count=len(result))
    
    def hash_join_left(
        self,
        outer_rows: list[dict[str, Any]],
        inner_rows: list[dict[str, Any]],
        outer_alias: str,
        inner_alias: str,
        outer_key: str,
        inner_key: str
    ) -> JoinResult:
        """
        [START_CONTRACT_HASH_JOIN_LEFT]
        Intent: Hash Left Join - все строки из outer + совпадающие из inner.
        Input: outer/inner_rows - данные; outer/inner_key - ключи JOIN.
        Output: JoinResult с NULL для несовпадающих inner строк.
        [END_CONTRACT_HASH_JOIN_LEFT]
        """
        result = []
        
        # Build phase
        hash_table: dict[Any, list[dict[str, Any]]] = {}
        for inner_row in inner_rows:
            key = self._get_key_value(inner_row, inner_key, inner_alias)
            if key is not None:
                if key not in hash_table:
                    hash_table[key] = []
                hash_table[key].append(inner_row)
        
        # Get inner columns for NULL row
        inner_columns = set()
        if inner_rows:
            inner_columns = set(inner_rows[0].keys())
        
        # Probe phase
        for outer_row in outer_rows:
            key = self._get_key_value(outer_row, outer_key, outer_alias)
            
            if key is not None and key in hash_table:
                for inner_row in hash_table[key]:
                    merged = self._merge_rows(outer_row, inner_row, outer_alias, inner_alias)
                    result.append(merged)
            else:
                # No match - add with NULL inner
                null_inner = self._create_null_row({}, inner_alias)
                for col in inner_columns:
                    null_inner[f"{inner_alias}.{col}"] = self.null_value
                    null_inner[col] = self.null_value
                merged = self._merge_rows(outer_row, null_inner, outer_alias, inner_alias)
                result.append(merged)
        
        return JoinResult(rows=result, row_count=len(result))
    
    def hash_join_full(
        self,
        outer_rows: list[dict[str, Any]],
        inner_rows: list[dict[str, Any]],
        outer_alias: str,
        inner_alias: str,
        outer_key: str,
        inner_key: str
    ) -> JoinResult:
        """
        [START_CONTRACT_HASH_JOIN_FULL]
        Intent: Hash Full Outer Join - все строки из обеих таблиц.
        Input: outer/inner_rows - данные; outer/inner_key - ключи JOIN.
        Output: JoinResult с NULL для несовпадающих строк с обеих сторон.
        [END_CONTRACT_HASH_JOIN_FULL]
        """
        result = []
        
        # Build phase
        hash_table: dict[Any, list[dict[str, Any]]] = {}
        matched_keys = set()
        
        for inner_row in inner_rows:
            key = self._get_key_value(inner_row, inner_key, inner_alias)
            if key is not None:
                if key not in hash_table:
                    hash_table[key] = []
                hash_table[key].append(inner_row)
        
        # Get columns
        outer_columns = set()
        if outer_rows:
            outer_columns = set(outer_rows[0].keys())
        inner_columns = set()
        if inner_rows:
            inner_columns = set(inner_rows[0].keys())
        
        # Probe phase
        for outer_row in outer_rows:
            key = self._get_key_value(outer_row, outer_key, outer_alias)
            
            if key is not None and key in hash_table:
                matched_keys.add(key)
                for inner_row in hash_table[key]:
                    merged = self._merge_rows(outer_row, inner_row, outer_alias, inner_alias)
                    result.append(merged)
            else:
                # No match - add with NULL inner
                null_inner = self._create_null_row({}, inner_alias)
                for col in inner_columns:
                    null_inner[f"{inner_alias}.{col}"] = self.null_value
                    null_inner[col] = self.null_value
                merged = self._merge_rows(outer_row, null_inner, outer_alias, inner_alias)
                result.append(merged)
        
        # Add unmatched inner rows
        for key, inner_rows_list in hash_table.items():
            if key not in matched_keys:
                for inner_row in inner_rows_list:
                    null_outer = self._create_null_row({}, outer_alias)
                    for col in outer_columns:
                        null_outer[f"{outer_alias}.{col}"] = self.null_value
                        null_outer[col] = self.null_value
                    merged = self._merge_rows(null_outer, inner_row, outer_alias, inner_alias)
                    result.append(merged)
        
        return JoinResult(rows=result, row_count=len(result))
    
    # =========================================================================
    # MERGE JOIN (O(M+N) для sorted inputs)
    # =========================================================================
    
    def merge_join(
        self,
        outer_rows: list[dict[str, Any]],
        inner_rows: list[dict[str, Any]],
        outer_alias: str,
        inner_alias: str,
        outer_key: str,
        inner_key: str,
        outer_sorted: bool = False,
        inner_sorted: bool = False
    ) -> JoinResult:
        """
        [START_CONTRACT_MERGE_JOIN]
        Intent: Merge Join - O(M+N) для предварительно отсортированных данных.
        Input: outer/inner_rows - данные; outer/inner_key - ключи JOIN.
        Output: JoinResult с совпадающими строками.
        Note: Если данные не отсортированы, выполняется сортировка O(N log N).
        [END_CONTRACT_MERGE_JOIN]
        """
        result = []
        
        # Sort if needed
        if not outer_sorted:
            outer_rows = sorted(
                outer_rows,
                key=lambda r: self._get_key_value(r, outer_key, outer_alias) or ()
            )
        if not inner_sorted:
            inner_rows = sorted(
                inner_rows,
                key=lambda r: self._get_key_value(r, inner_key, inner_alias) or ()
            )
        
        # Merge phase
        i, j = 0, 0
        while i < len(outer_rows) and j < len(inner_rows):
            outer_key_val = self._get_key_value(outer_rows[i], outer_key, outer_alias)
            inner_key_val = self._get_key_value(inner_rows[j], inner_key, inner_alias)
            
            if outer_key_val is None:
                i += 1
                continue
            if inner_key_val is None:
                j += 1
                continue
            
            if outer_key_val == inner_key_val:
                # Found match - collect all matching rows
                outer_matches = [outer_rows[i]]
                inner_matches = [inner_rows[j]]
                
                # Collect all outer rows with same key
                while i + 1 < len(outer_rows):
                    next_key = self._get_key_value(outer_rows[i + 1], outer_key, outer_alias)
                    if next_key == outer_key_val:
                        i += 1
                        outer_matches.append(outer_rows[i])
                    else:
                        break
                
                # Collect all inner rows with same key
                while j + 1 < len(inner_rows):
                    next_key = self._get_key_value(inner_rows[j + 1], inner_key, inner_alias)
                    if next_key == inner_key_val:
                        j += 1
                        inner_matches.append(inner_rows[j])
                    else:
                        break
                
                # Cross product of matches
                for outer_row in outer_matches:
                    for inner_row in inner_matches:
                        merged = self._merge_rows(outer_row, inner_row, outer_alias, inner_alias)
                        result.append(merged)
                
                i += 1
                j += 1
            elif outer_key_val < inner_key_val:
                i += 1
            else:
                j += 1
        
        return JoinResult(rows=result, row_count=len(result))
    
    # =========================================================================
    # HELPER METHODS
    # =========================================================================
    
    def _extract_join_keys(
        self,
        condition: Optional[ExpressionNode],
        outer_alias: str,
        inner_alias: str
    ) -> Optional[tuple[str, str]]:
        """
        [START_CONTRACT_EXTRACT_JOIN_KEYS]
        Intent: Извлекает ключи JOIN из условия ON (t1.col = t2.col).
        Input: condition - условие ON; outer/inner_alias - алиасы таблиц.
        Output: (outer_key, inner_key) или None если условие сложное.
        [END_CONTRACT_EXTRACT_JOIN_KEYS]
        """
        if not condition:
            return None
        
        if isinstance(condition, BinaryOpNode):
            if condition.operator == BinaryOperator.EQ:
                left_col = self._get_column_ref(condition.left)
                right_col = self._get_column_ref(condition.right)
                
                if left_col and right_col:
                    left_table = left_col.table_alias or ""
                    right_table = right_col.table_alias or ""
                    
                    # Match aliases
                    if left_table == outer_alias and right_table == inner_alias:
                        return (left_col.column_name, right_col.column_name)
                    elif left_table == inner_alias and right_table == outer_alias:
                        return (right_col.column_name, left_col.column_name)
        
        return None
    
    def _get_column_ref(self, expr: ExpressionNode) -> Optional[ColumnRef]:
        """Извлекает ColumnRef из выражения."""
        if isinstance(expr, ColumnRef):
            return expr
        return None
    
    def _get_key_value(
        self,
        row: dict[str, Any],
        key: str,
        alias: str
    ) -> Any:
        """
        [START_CONTRACT_GET_KEY_VALUE]
        Intent: Получает значение ключа из строки.
        Input: row - данные; key - имя колонки; alias - алиас таблицы.
        Output: Значение ключа или None.
        [END_CONTRACT_GET_KEY_VALUE]
        """
        # Try with alias prefix first
        aliased_key = f"{alias}.{key}"
        if aliased_key in row:
            return row[aliased_key]
        # Try without alias
        if key in row:
            return row[key]
        return None
    
    def _merge_rows(
        self,
        outer_row: dict[str, Any],
        inner_row: dict[str, Any],
        outer_alias: str,
        inner_alias: str
    ) -> dict[str, Any]:
        """
        [START_CONTRACT_MERGE_ROWS]
        Intent: Объединяет две строки из разных таблиц.
        Input: outer/inner_row - данные; outer/inner_alias - алиасы.
        Output: Объединённая строка с префиксами алиасов.
        [END_CONTRACT_MERGE_ROWS]
        """
        merged = {}
        
        # Add outer row columns
        for key, value in outer_row.items():
            if "." in key:
                # Already has prefix
                merged[key] = value
            else:
                merged[f"{outer_alias}.{key}"] = value
                merged[key] = value  # Also keep without prefix for compatibility
        
        # Add inner row columns
        for key, value in inner_row.items():
            if "." in key:
                merged[key] = value
            else:
                merged[f"{inner_alias}.{key}"] = value
                # Only add without prefix if not already present
                if key not in merged:
                    merged[key] = value
        
        return merged
    
    def _create_null_row(
        self,
        template: dict[str, Any],
        alias: str
    ) -> dict[str, Any]:
        """
        [START_CONTRACT_CREATE_NULL_ROW]
        Intent: Создаёт строку с NULL значениями для OUTER JOIN.
        Input: template - шаблон строки; alias - алиас таблицы.
        Output: Строка с NULL значениями.
        [END_CONTRACT_CREATE_NULL_ROW]
        """
        null_row = {}
        for key in template.keys():
            if "." in key:
                null_row[key] = self.null_value
            else:
                null_row[f"{alias}.{key}"] = self.null_value
                null_row[key] = self.null_value
        return null_row


# END_BLOCK_JOIN_EXECUTOR


# =============================================================================
# START_BLOCK_MULTI_JOIN
# =============================================================================

class MultiJoinExecutor:
    """
    [START_CONTRACT_MULTI_JOIN_EXECUTOR]
    Intent: Выполнение JOIN для нескольких таблиц (до 10).
    Input: Список таблиц и условий JOIN.
    Output: JoinResult с объединёнными данными.
    Note: Поддерживает bushy join trees.
    [END_CONTRACT_MULTI_JOIN_EXECUTOR]
    """
    
    def __init__(self):
        """Инициализация MultiJoinExecutor."""
        self.join_executor = JoinExecutor()
    
    def execute_multi_join(
        self,
        tables: list[tuple[str, list[dict[str, Any]]]],
        joins: list[tuple[str, str, ASTJoinType, Optional[ExpressionNode]]],
        evaluator: Optional[Callable] = None
    ) -> JoinResult:
        """
        [START_CONTRACT_EXECUTE_MULTI_JOIN]
        Intent: Выполняет последовательность JOIN операций.
        Input: tables - [(alias, rows)]; joins - [(outer_alias, inner_alias, type, condition)].
        Output: JoinResult с объединёнными данными всех таблиц.
        Algorithm: Left-deep tree (последовательные JOIN слева направо).
        [END_CONTRACT_EXECUTE_MULTI_JOIN]
        """
        if not tables:
            return JoinResult()
        
        if len(tables) == 1:
            alias, rows = tables[0]
            result_rows = []
            for row in rows:
                result_row = {}
                for k, v in row.items():
                    result_row[f"{alias}.{k}"] = v
                    result_row[k] = v
                result_rows.append(result_row)
            return JoinResult(rows=result_rows, row_count=len(result_rows))
        
        # Build table lookup
        table_data = {alias: rows for alias, rows in tables}
        
        # Start with first table
        current_alias, current_rows = tables[0]
        result_rows = []
        for row in current_rows:
            result_row = {}
            for k, v in row.items():
                result_row[f"{current_alias}.{k}"] = v
                result_row[k] = v
            result_rows.append(result_row)
        
        # Process joins
        for outer_alias, inner_alias, join_type, condition in joins:
            if inner_alias not in table_data:
                continue
            
            inner_rows = table_data[inner_alias]
            
            # Execute join
            join_result = self.join_executor.execute_join(
                join_type=join_type,
                outer_rows=result_rows,
                inner_rows=self._prefix_rows(inner_rows, inner_alias),
                outer_alias=outer_alias,
                inner_alias=inner_alias,
                condition=condition,
                evaluator=evaluator
            )
            
            result_rows = join_result.rows
            current_alias = f"{outer_alias},{inner_alias}"
        
        return JoinResult(rows=result_rows, row_count=len(result_rows))
    
    def _prefix_rows(
        self,
        rows: list[dict[str, Any]],
        alias: str
    ) -> list[dict[str, Any]]:
        """Добавляет префикс алиаса к колонкам."""
        result = []
        for row in rows:
            prefixed = {}
            for k, v in row.items():
                if "." not in k:
                    prefixed[f"{alias}.{k}"] = v
                prefixed[k] = v
            result.append(prefixed)
        return result


# END_BLOCK_MULTI_JOIN