# START_MODULE_CONTRACT
# Module: mini_db_v2.optimizer.planner
# Intent: Query planner с cost-based optimization и System R join ordering.
# Dependencies: dataclasses, typing, mini_db_v2.optimizer.statistics, mini_db_v2.optimizer.cost_model
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: QueryPlanner, QueryPlan, PlanNode, ScanNode, JoinNode
# END_MODULE_MAP

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional
from mini_db_v2.optimizer.statistics import Statistics, TableStats, ColumnStats
from mini_db_v2.optimizer.cost_model import CostModel, CostEstimate, JoinType, ScanType
from mini_db_v2.ast.nodes import (
    SelectNode, FromClause, JoinClause, ExpressionNode, ColumnRef,
    BinaryOpNode, BinaryOperator, TableRef, JoinType as ASTJoinType, LiteralNode
)


# =============================================================================
# START_BLOCK_PLAN_NODES
# =============================================================================

@dataclass
class PlanNode:
    """
    [START_CONTRACT_PLAN_NODE]
    Intent: Базовый узел плана выполнения.
    Input: cost - оценка стоимости; rows - ожидаемое количество строк.
    Output: Узел дерева плана.
    [END_CONTRACT_PLAN_NODE]
    """
    cost: float = 0.0
    rows: float = 0.0
    plan_type: str = "Plan"
    children: list[PlanNode] = field(default_factory=list)
    
    def explain(self, indent: int = 0) -> str:
        """Возвращает текстовое представление плана."""
        prefix = "  " * indent
        result = f"{prefix}{self.plan_type}  (cost={self.cost:.1f} rows={int(self.rows)})\n"
        for child in self.children:
            result += child.explain(indent + 1)
        return result


@dataclass
class ScanNode(PlanNode):
    """
    [START_CONTRACT_SCAN_NODE]
    Intent: Узел сканирования таблицы (SeqScan или IndexScan).
    Input: table_name - таблица; scan_type - тип сканирования.
    Output: Узел для чтения данных из таблицы.
    [END_CONTRACT_SCAN_NODE]
    """
    table_name: str = ""
    alias: Optional[str] = None
    scan_type: str = "SeqScan"  # SeqScan, IndexScan
    index_name: Optional[str] = None
    filter_condition: Optional[str] = None
    
    def explain(self, indent: int = 0) -> str:
        prefix = "  " * indent
        result = f"{prefix}{self.scan_type} on {self.table_name}"
        if self.alias and self.alias != self.table_name:
            result += f" AS {self.alias}"
        result += f"  (cost={self.cost:.1f} rows={int(self.rows)})"
        if self.filter_condition:
            result += f"\n{prefix}  Filter: {self.filter_condition}"
        result += "\n"
        return result


@dataclass
class JoinPlanNode(PlanNode):
    """
    [START_CONTRACT_JOIN_NODE]
    Intent: Узел JOIN операции.
    Input: join_type - тип JOIN; condition - условие соединения.
    Output: Узел для соединения двух таблиц.
    [END_CONTRACT_JOIN_NODE]
    """
    join_type: str = "HashJoin"  # HashJoin, NestedLoop, MergeJoin
    condition: Optional[str] = None
    outer_table: str = ""
    inner_table: str = ""
    
    def explain(self, indent: int = 0) -> str:
        prefix = "  " * indent
        result = f"{prefix}{self.join_type}"
        result += f"  (cost={self.cost:.1f} rows={int(self.rows)})"
        if self.condition:
            result += f"\n{prefix}  Hash Cond: {self.condition}"
        result += "\n"
        for child in self.children:
            result += child.explain(indent + 1)
        return result


@dataclass
class QueryPlan:
    """
    [START_CONTRACT_QUERY_PLAN]
    Intent: Полный план выполнения запроса.
    Input: root - корневой узел плана; total_cost - общая стоимость.
    Output: Структура для EXPLAIN и выполнения.
    [END_CONTRACT_QUERY_PLAN]
    """
    root: Optional[PlanNode] = None
    total_cost: float = 0.0
    estimated_rows: float = 0.0
    plan_text: str = ""
    
    def explain(self) -> str:
        """Возвращает текстовое представление плана."""
        if self.root is None:
            return "No plan"
        
        result = "QUERY PLAN\n"
        result += "-" * 40 + "\n"
        result += self.root.explain()
        return result


# END_BLOCK_PLAN_NODES


# =============================================================================
# START_BLOCK_QUERY_PLANNER
# =============================================================================

class QueryPlanner:
    """
    [START_CONTRACT_QUERY_PLANNER]
    Intent: Cost-based query planner с System R join ordering.
    Input: statistics - статистика таблиц; cost_model - модель стоимости.
    Output: Оптимальный план выполнения запроса.
    Algorithm:
        1. Генерация альтернативных планов
        2. Оценка стоимости каждого плана
        3. Выбор плана с минимальной cost
    [END_CONTRACT_QUERY_PLANNER]
    """
    
    def __init__(self, statistics: Statistics, cost_model: Optional[CostModel] = None):
        """
        [START_CONTRACT_QP_INIT]
        Intent: Инициализация planner с статистикой и cost model.
        Input: statistics - статистика таблиц; cost_model - опционально.
        Output: Готовый к работе planner.
        [END_CONTRACT_QP_INIT]
        """
        self.statistics = statistics
        self.cost_model = cost_model or CostModel()
    
    def create_plan(self, ast: SelectNode) -> QueryPlan:
        """
        [START_CONTRACT_CREATE_PLAN]
        Intent: Создаёт оптимальный план выполнения SELECT запроса.
        Input: ast - SELECT AST узел.
        Output: QueryPlan с оптимальным планом.
        [END_CONTRACT_CREATE_PLAN]
        """
        if ast.from_clause is None:
            # SELECT without FROM
            return QueryPlan(
                root=ScanNode(cost=0.0, rows=1.0, plan_type="Result"),
                total_cost=0.0,
                estimated_rows=1.0
            )
        
        # Get tables from FROM clause
        tables = self._extract_tables(ast.from_clause)
        
        if len(tables) == 1:
            # Single table - simple plan
            plan = self._create_single_table_plan(ast, tables[0])
        else:
            # Multiple tables - need join ordering
            plan = self._create_join_plan(ast, tables, ast.from_clause.joins)
        
        return plan
    
    def _extract_tables(self, from_clause: FromClause) -> list[tuple[str, Optional[str]]]:
        """Извлекает список таблиц из FROM clause."""
        tables = []
        
        # Main table
        main_table = from_clause.table.table_name
        main_alias = from_clause.table.alias or main_table
        tables.append((main_table, main_alias))
        
        # Joined tables
        for join in from_clause.joins:
            table_name = join.table.table_name
            alias = join.table.alias or table_name
            tables.append((table_name, alias))
        
        return tables
    
    def _create_single_table_plan(self, ast: SelectNode, 
                                   table_info: tuple[str, Optional[str]]) -> QueryPlan:
        """Создаёт план для одной таблицы."""
        table_name, alias = table_info
        
        # Get statistics
        table_stats = self.statistics.get_table_stats(table_name)
        if table_stats is None:
            # Default stats if not analyzed
            table_stats = TableStats(row_count=1000, page_count=10)
        
        # Estimate selectivity from WHERE
        selectivity = 1.0
        filter_str = None
        if ast.where:
            selectivity = self._estimate_where_selectivity(table_name, ast.where)
            filter_str = self._expression_to_string(ast.where)
        
        # Choose scan type
        scan_type = "SeqScan"
        index_name = None
        
        # Check for index usage
        if ast.where and selectivity < 0.3:
            index_info = self._find_usable_index(table_name, ast.where)
            if index_info:
                scan_type = "IndexScan"
                index_name = index_info
        
        # Estimate cost
        if scan_type == "SeqScan":
            cost_est = self.cost_model.estimate_seq_scan_cost(
                table_stats.row_count, table_stats.page_count, selectivity
            )
        else:
            cost_est = self.cost_model.estimate_index_scan_cost(
                table_stats.row_count, selectivity
            )
        
        # Create scan node
        scan_node = ScanNode(
            cost=cost_est.total_cost,
            rows=cost_est.estimated_rows,
            plan_type=scan_type,
            table_name=table_name,
            alias=alias,
            scan_type=scan_type,
            index_name=index_name,
            filter_condition=filter_str
        )
        
        return QueryPlan(
            root=scan_node,
            total_cost=cost_est.total_cost,
            estimated_rows=cost_est.estimated_rows
        )
    
    def _create_join_plan(self, ast: SelectNode, 
                          tables: list[tuple[str, Optional[str]]],
                          joins: list[JoinClause]) -> QueryPlan:
        """
        Создаёт план для JOIN с оптимальным порядком (System R algorithm).
        """
        # System R join ordering: dynamic programming
        best_order = self._find_best_join_order(tables, joins)
        
        # Build plan from best order
        plan = self._build_join_tree(best_order, joins, ast.where)
        
        return plan
    
    def _find_best_join_order(self, 
                               tables: list[tuple[str, Optional[str]]],
                               joins: list[JoinClause]) -> list[tuple[str, Optional[str]]]:
        """
        System R algorithm для join ordering.
        Выбирает порядок таблиц с минимальной cost.
        """
        if len(tables) <= 2:
            # For 1-2 tables, just use the given order
            return tables
        
        # Build best plans for each subset of tables
        # best_plans[frozenset of table names] = (cost, order)
        best_plans: dict[frozenset, tuple[float, list]] = {}
        
        # Base case: single tables
        for table in tables:
            table_name = table[0]
            stats = self.statistics.get_table_stats(table_name)
            row_count = stats.row_count if stats else 1000
            
            cost = self.cost_model.estimate_seq_scan_cost(row_count, max(1, row_count // 100)).total_cost
            best_plans[frozenset([table_name])] = (cost, [table])
        
        # Inductive case: join with one more table
        for size in range(2, len(tables) + 1):
            for subset in self._subsets_of_size(tables, size):
                subset_key = frozenset(t[0] for t in subset)
                
                for table in subset:
                    table_name = table[0]
                    prev_key = frozenset(k for k in subset_key if k != table_name)
                    
                    if prev_key not in best_plans:
                        continue
                    
                    prev_cost, prev_order = best_plans[prev_key]
                    
                    # Get table stats
                    stats = self.statistics.get_table_stats(table_name)
                    row_count = stats.row_count if stats else 1000
                    
                    # Estimate join cost
                    prev_rows = self._estimate_rows_for_plan(prev_order)
                    join_cost = self._estimate_join_cost(prev_rows, row_count)
                    
                    total_cost = prev_cost + join_cost
                    
                    if subset_key not in best_plans or total_cost < best_plans[subset_key][0]:
                        best_plans[subset_key] = (total_cost, prev_order + [table])
        
        # Return best plan for all tables
        all_tables_key = frozenset(t[0] for t in tables)
        if all_tables_key in best_plans:
            return best_plans[all_tables_key][1]
        
        return tables  # Fallback to original order
    
    def _subsets_of_size(self, tables: list, size: int) -> list[list]:
        """Генерирует все подмножества заданного размера."""
        from itertools import combinations
        return [list(c) for c in combinations(tables, size)]
    
    def _estimate_rows_for_plan(self, order: list[tuple[str, Optional[str]]]) -> float:
        """Оценивает количество строк после соединения."""
        if not order:
            return 0.0
        
        total = 1.0
        for table_name, _ in order:
            stats = self.statistics.get_table_stats(table_name)
            row_count = stats.row_count if stats else 1000
            total *= row_count
        
        # Apply join selectivity (simplified)
        if len(order) > 1:
            total *= 0.1 ** (len(order) - 1)
        
        return total
    
    def _estimate_join_cost(self, outer_rows: float, inner_rows: float) -> float:
        """Оценивает стоимость JOIN."""
        # Choose join type based on sizes
        join_type = self.cost_model.choose_join_type(int(outer_rows), int(inner_rows))
        
        if join_type == JoinType.HASH_JOIN:
            cost = self.cost_model.estimate_hash_join_cost(
                int(outer_rows), int(inner_rows), 100, 100
            )
            return cost.total_cost
        elif join_type == JoinType.MERGE_JOIN:
            cost = self.cost_model.estimate_merge_join_cost(
                int(outer_rows), int(inner_rows), False, False
            )
            return cost.total_cost
        else:
            cost = self.cost_model.estimate_nested_loop_join_cost(
                int(outer_rows), int(inner_rows), inner_rows * 0.01
            )
            return cost.total_cost
    
    def _build_join_tree(self, order: list[tuple[str, Optional[str]]],
                         joins: list[JoinClause],
                         where: Optional[ExpressionNode]) -> QueryPlan:
        """Строит дерево JOIN из порядка таблиц."""
        if len(order) == 1:
            return self._create_single_table_plan(
                SelectNode(columns=[], from_clause=None, where=where),
                order[0]
            )
        
        # Build join tree bottom-up
        # First table is the outer
        outer_table, outer_alias = order[0]
        outer_stats = self.statistics.get_table_stats(outer_table)
        outer_rows = outer_stats.row_count if outer_stats else 1000
        
        outer_node = ScanNode(
            cost=outer_rows * 0.01,
            rows=outer_rows,
            plan_type="SeqScan",
            table_name=outer_table,
            alias=outer_alias,
            scan_type="SeqScan"
        )
        
        total_cost = outer_node.cost
        
        # Join remaining tables
        for i, (table_name, alias) in enumerate(order[1:], 1):
            # Get table stats
            inner_stats = self.statistics.get_table_stats(table_name)
            inner_rows = inner_stats.row_count if inner_stats else 1000
            
            # Create inner scan
            inner_node = ScanNode(
                cost=inner_rows * 0.01,
                rows=inner_rows,
                plan_type="SeqScan",
                table_name=table_name,
                alias=alias,
                scan_type="SeqScan"
            )
            
            # Find join condition
            join_condition = self._find_join_condition(outer_table, table_name, joins, where)
            condition_str = self._expression_to_string(join_condition) if join_condition else None
            
            # Choose join type
            join_type_enum = self.cost_model.choose_join_type(int(outer_rows), int(inner_rows))
            join_type_str = {
                JoinType.HASH_JOIN: "HashJoin",
                JoinType.MERGE_JOIN: "MergeJoin",
                JoinType.NESTED_LOOP: "NestedLoop"
            }.get(join_type_enum, "HashJoin")
            
            # Estimate join cost
            join_cost = self._estimate_join_cost(outer_rows, inner_rows)
            total_cost += join_cost
            
            # Estimate output rows
            output_rows = outer_rows * inner_rows * 0.1  # Simplified selectivity
            
            # Create join node
            join_node = JoinPlanNode(
                cost=total_cost,
                rows=output_rows,
                plan_type=join_type_str,
                join_type=join_type_str,
                condition=condition_str,
                outer_table=outer_alias or outer_table,
                inner_table=alias or table_name,
                children=[outer_node, inner_node]
            )
            
            outer_node = join_node
            outer_rows = output_rows
            outer_table = table_name
        
        return QueryPlan(
            root=outer_node,
            total_cost=total_cost,
            estimated_rows=outer_rows
        )
    
    def _find_join_condition(self, outer_table: str, inner_table: str,
                             joins: list[JoinClause],
                             where: Optional[ExpressionNode]) -> Optional[ExpressionNode]:
        """Находит условие JOIN между двумя таблицами."""
        # Check explicit JOIN conditions
        for join in joins:
            if join.table.table_name == inner_table:
                return join.condition
        
        # Check WHERE for implicit join
        if where and isinstance(where, BinaryOpNode):
            if where.operator == BinaryOperator.AND:
                # Check both sides
                left = self._find_join_condition(outer_table, inner_table, joins, where.left)
                right = self._find_join_condition(outer_table, inner_table, joins, where.right)
                return left or right
            elif where.operator == BinaryOperator.EQ:
                # Check if this is a join condition
                left_col = self._get_column_ref(where.left)
                right_col = self._get_column_ref(where.right)
                
                if left_col and right_col:
                    left_table = left_col.table_alias or ""
                    right_table = right_col.table_alias or ""
                    
                    if {left_table, right_table} == {outer_table, inner_table}:
                        return where
        
        return None
    
    def _get_column_ref(self, expr: ExpressionNode) -> Optional[ColumnRef]:
        """Извлекает ColumnRef из выражения."""
        if isinstance(expr, ColumnRef):
            return expr
        return None
    
    def _estimate_where_selectivity(self, table_name: str, where: ExpressionNode) -> float:
        """Оценивает selectivity WHERE условия."""
        if isinstance(where, BinaryOpNode):
            if where.operator == BinaryOperator.AND:
                left_sel = self._estimate_where_selectivity(table_name, where.left)
                right_sel = self._estimate_where_selectivity(table_name, where.right)
                return left_sel * right_sel
            elif where.operator == BinaryOperator.OR:
                left_sel = self._estimate_where_selectivity(table_name, where.left)
                right_sel = self._estimate_where_selectivity(table_name, where.right)
                return left_sel + right_sel - (left_sel * right_sel)
            elif where.operator in (BinaryOperator.EQ, BinaryOperator.NE,
                                    BinaryOperator.LT, BinaryOperator.LE,
                                    BinaryOperator.GT, BinaryOperator.GE):
                # Get column and value
                if isinstance(where.left, ColumnRef):
                    col_name = where.left.column_name
                    op = {
                        BinaryOperator.EQ: "=",
                        BinaryOperator.NE: "!=",
                        BinaryOperator.LT: "<",
                        BinaryOperator.LE: "<=",
                        BinaryOperator.GT: ">",
                        BinaryOperator.GE: ">="
                    }.get(where.operator, "=")
                    
                    # Get value from right side if it's a literal
                    value = None
                    if isinstance(where.right, LiteralNode):
                        value = where.right.value
                    
                    return self.statistics.estimate_selectivity(table_name, col_name, op, value)
        
        return 0.1  # Default selectivity
    
    def _find_usable_index(self, table_name: str, where: ExpressionNode) -> Optional[str]:
        """Находит индекс, который можно использовать для WHERE."""
        # Simplified: just check if there's an index on the column
        if isinstance(where, BinaryOpNode):
            if isinstance(where.left, ColumnRef):
                col_name = where.left.column_name
                col_stats = self.statistics.get_column_stats(table_name, col_name)
                if col_stats and col_stats.distinct_values > 0:
                    # Assume there's an index if column has stats
                    return f"idx_{table_name}_{col_name}"
            
            if where.operator == BinaryOperator.AND:
                left_idx = self._find_usable_index(table_name, where.left)
                if left_idx:
                    return left_idx
                return self._find_usable_index(table_name, where.right)
        
        return None
    
    def _expression_to_string(self, expr: Optional[ExpressionNode]) -> Optional[str]:
        """Конвертирует выражение в строку для EXPLAIN."""
        if expr is None:
            return None
        
        if isinstance(expr, ColumnRef):
            if expr.table_alias:
                return f"{expr.table_alias}.{expr.column_name}"
            return expr.column_name
        
        if isinstance(expr, LiteralNode):
            if expr.value is None:
                return "NULL"
            if isinstance(expr.value, str):
                return f"'{expr.value}'"
            return str(expr.value)
        
        if isinstance(expr, BinaryOpNode):
            left = self._expression_to_string(expr.left)
            right = self._expression_to_string(expr.right)
            op = {
                BinaryOperator.EQ: "=",
                BinaryOperator.NE: "!=",
                BinaryOperator.LT: "<",
                BinaryOperator.LE: "<=",
                BinaryOperator.GT: ">",
                BinaryOperator.GE: ">=",
                BinaryOperator.AND: "AND",
                BinaryOperator.OR: "OR"
            }.get(expr.operator, str(expr.operator))
            return f"{left} {op} {right}"
        
        return str(expr)


# END_BLOCK_QUERY_PLANNER