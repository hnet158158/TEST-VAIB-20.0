# START_MODULE_CONTRACT
# Module: mini_db_v2.optimizer.cost_model
# Intent: Cost model для оценки стоимости планов выполнения запросов.
# Dependencies: dataclasses, typing, enum
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: CostModel, CostEstimate, OperatorCost
# END_MODULE_MAP

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional
from enum import Enum, auto


# =============================================================================
# START_BLOCK_ENUMS
# =============================================================================

class JoinType(Enum):
    """Типы JOIN для cost estimation."""
    NESTED_LOOP = auto()
    HASH_JOIN = auto()
    MERGE_JOIN = auto()


class ScanType(Enum):
    """Типы сканирования таблицы."""
    SEQ_SCAN = auto()      # Full table scan
    INDEX_SCAN = auto()    # Index scan
    BITMAP_SCAN = auto()   # Bitmap index scan


# END_BLOCK_ENUMS


# =============================================================================
# START_BLOCK_DATA_CLASSES
# =============================================================================

@dataclass
class OperatorCost:
    """
    [START_CONTRACT_OPERATOR_COST]
    Intent: Стоимость отдельной операции.
    Input: startup - начальная стоимость; total - общая стоимость.
    Output: Структура для хранения стоимости операции.
    [END_CONTRACT_OPERATOR_COST]
    """
    startup: float = 0.0
    total: float = 0.0
    rows: float = 0.0
    width: float = 0.0  # Average row width in bytes
    
    def __add__(self, other: OperatorCost) -> OperatorCost:
        return OperatorCost(
            startup=self.startup + other.startup,
            total=self.total + other.total,
            rows=other.rows,
            width=other.width
        )


@dataclass
class CostEstimate:
    """
    [START_CONTRACT_COST_ESTIMATE]
    Intent: Оценка стоимости выполнения плана запроса.
    Input: total_cost - общая стоимость; rows - ожидаемое количество строк.
    Output: Структура для сравнения планов.
    [END_CONTRACT_COST_ESTIMATE]
    """
    total_cost: float = 0.0
    startup_cost: float = 0.0
    estimated_rows: float = 0.0
    estimated_width: float = 0.0
    
    # Detailed breakdown
    cpu_cost: float = 0.0
    io_cost: float = 0.0
    
    # Plan info
    plan_type: str = ""
    children: list[CostEstimate] = field(default_factory=list)
    
    def __lt__(self, other: CostEstimate) -> bool:
        return self.total_cost < other.total_cost
    
    def __le__(self, other: CostEstimate) -> bool:
        return self.total_cost <= other.total_cost


# END_BLOCK_DATA_CLASSES


# =============================================================================
# START_BLOCK_COST_MODEL
# =============================================================================

class CostModel:
    """
    [START_CONTRACT_COST_MODEL]
    Intent: Модель оценки стоимости операций для query optimizer.
    Input: statistics - статистика таблиц; config - параметры стоимости.
    Output: API для оценки стоимости scan, join, aggregate.
    [END_CONTRACT_COST_MODEL]
    """
    
    # Default cost constants (similar to PostgreSQL)
    DEFAULT_SEQ_PAGE_COST = 1.0
    DEFAULT_RANDOM_PAGE_COST = 4.0
    DEFAULT_CPU_TUPLE_COST = 0.01
    DEFAULT_CPU_INDEX_TUPLE_COST = 0.005
    DEFAULT_CPU_OPERATOR_COST = 0.0025
    DEFAULT_EFFECTIVE_CACHE_SIZE = 128 * 1024  # 128MB in pages
    
    def __init__(
        self,
        seq_page_cost: float = DEFAULT_SEQ_PAGE_COST,
        random_page_cost: float = DEFAULT_RANDOM_PAGE_COST,
        cpu_tuple_cost: float = DEFAULT_CPU_TUPLE_COST,
        cpu_index_tuple_cost: float = DEFAULT_CPU_INDEX_TUPLE_COST,
        cpu_operator_cost: float = DEFAULT_CPU_OPERATOR_COST,
        effective_cache_size: int = DEFAULT_EFFECTIVE_CACHE_SIZE
    ):
        """
        [START_CONTRACT_CM_INIT]
        Intent: Инициализация cost model с параметрами.
        Input: Параметры стоимости операций.
        Output: Готовая к работе cost model.
        [END_CONTRACT_CM_INIT]
        """
        self.seq_page_cost = seq_page_cost
        self.random_page_cost = random_page_cost
        self.cpu_tuple_cost = cpu_tuple_cost
        self.cpu_index_tuple_cost = cpu_index_tuple_cost
        self.cpu_operator_cost = cpu_operator_cost
        self.effective_cache_size = effective_cache_size
    
    def estimate_seq_scan_cost(
        self,
        row_count: int,
        page_count: int,
        selectivity: float = 1.0
    ) -> CostEstimate:
        """
        [START_CONTRACT_SEQ_SCAN_COST]
        Intent: Оценка стоимости sequential scan.
        Input: row_count - строки; page_count - страницы; selectivity - селективность.
        Output: CostEstimate с оценкой стоимости.
        Formula: IO_cost = pages * seq_page_cost
                 CPU_cost = rows * cpu_tuple_cost * selectivity
        [END_CONTRACT_SEQ_SCAN_COST]
        """
        # IO cost: read all pages
        io_cost = page_count * self.seq_page_cost
        
        # CPU cost: process each row
        cpu_cost = row_count * self.cpu_tuple_cost
        
        # Apply selectivity for output rows
        output_rows = row_count * selectivity
        
        total_cost = io_cost + cpu_cost
        
        return CostEstimate(
            total_cost=total_cost,
            startup_cost=0.0,
            estimated_rows=output_rows,
            cpu_cost=cpu_cost,
            io_cost=io_cost,
            plan_type="SeqScan"
        )
    
    def estimate_index_scan_cost(
        self,
        row_count: int,
        selectivity: float,
        index_height: int = 3,
        has_index: bool = True
    ) -> CostEstimate:
        """
        [START_CONTRACT_INDEX_SCAN_COST]
        Intent: Оценка стоимости index scan.
        Input: row_count - строки; selectivity - селективность; index_height - высота B-tree.
        Output: CostEstimate с оценкой стоимости.
        Formula: IO_cost = log(N) + K * random_page_cost
                 CPU_cost = K * cpu_index_tuple_cost
        [END_CONTRACT_INDEX_SCAN_COST]
        """
        if not has_index or selectivity >= 1.0:
            # Fall back to seq scan estimate
            return self.estimate_seq_scan_cost(row_count, max(1, row_count // 100), selectivity)
        
        # Number of rows to fetch
        k = max(1, int(row_count * selectivity))
        
        # Index traversal cost (log N)
        index_traversal_cost = index_height * self.random_page_cost
        
        # Data fetch cost (random I/O for each row)
        # But if selectivity is high, use bitmap scan logic
        if selectivity > 0.1:
            # Many rows - use bitmap scan
            io_cost = index_traversal_cost + (k * self.random_page_cost * 0.5)
        else:
            # Few rows - use index scan
            io_cost = index_traversal_cost + (k * self.random_page_cost)
        
        # CPU cost for index tuples
        cpu_cost = k * self.cpu_index_tuple_cost
        
        # Add operator cost for each comparison
        cpu_cost += k * self.cpu_operator_cost
        
        output_rows = k
        
        total_cost = io_cost + cpu_cost
        
        return CostEstimate(
            total_cost=total_cost,
            startup_cost=index_traversal_cost,
            estimated_rows=output_rows,
            cpu_cost=cpu_cost,
            io_cost=io_cost,
            plan_type="IndexScan"
        )
    
    def estimate_nested_loop_join_cost(
        self,
        outer_rows: int,
        inner_rows: int,
        inner_cost: float,
        selectivity: float = 1.0
    ) -> CostEstimate:
        """
        [START_CONTRACT_NESTED_LOOP_COST]
        Intent: Оценка стоимости nested loop join.
        Input: outer_rows, inner_rows - размеры таблиц; inner_cost - стоимость inner scan.
        Output: CostEstimate с оценкой стоимости.
        Formula: O(M * (inner_cost + cpu_operator_cost))
        [END_CONTRACT_NESTED_LOOP_COST]
        """
        # For each outer row, scan inner
        total_inner_scans = outer_rows
        
        # CPU cost for comparisons
        cpu_cost = outer_rows * inner_rows * self.cpu_operator_cost
        
        # Total inner scan cost
        inner_total = total_inner_scans * (inner_cost + self.cpu_tuple_cost)
        
        output_rows = outer_rows * inner_rows * selectivity
        
        total_cost = inner_total + cpu_cost
        
        return CostEstimate(
            total_cost=total_cost,
            startup_cost=0.0,
            estimated_rows=output_rows,
            cpu_cost=cpu_cost,
            io_cost=inner_total - cpu_cost,
            plan_type="NestedLoop"
        )
    
    def estimate_hash_join_cost(
        self,
        outer_rows: int,
        inner_rows: int,
        outer_width: int,
        inner_width: int,
        selectivity: float = 1.0
    ) -> CostEstimate:
        """
        [START_CONTRACT_HASH_JOIN_COST]
        Intent: Оценка стоимости hash join.
        Input: outer_rows, inner_rows - размеры таблиц; widths - ширина строк.
        Output: CostEstimate с оценкой стоимости.
        Formula: Build hash: O(N), Probe: O(M)
        [END_CONTRACT_HASH_JOIN_COST]
        """
        # Build phase: hash inner table
        build_cost = inner_rows * self.cpu_tuple_cost
        build_cost += inner_rows * self.cpu_operator_cost  # Hash computation
        
        # Probe phase: scan outer and probe hash
        probe_cost = outer_rows * self.cpu_tuple_cost
        probe_cost += outer_rows * self.cpu_operator_cost  # Hash lookup
        
        # Memory consideration (simplified)
        hash_table_size = inner_rows * inner_width
        if hash_table_size > self.effective_cache_size * 4096:
            # Hash table doesn't fit in memory - extra I/O
            build_cost *= 1.5
        
        cpu_cost = build_cost + probe_cost
        output_rows = outer_rows * inner_rows * selectivity
        
        # Hash join is typically CPU-bound
        total_cost = cpu_cost
        
        return CostEstimate(
            total_cost=total_cost,
            startup_cost=build_cost,  # Must build hash table first
            estimated_rows=output_rows,
            cpu_cost=cpu_cost,
            io_cost=0.0,
            plan_type="HashJoin"
        )
    
    def estimate_merge_join_cost(
        self,
        outer_rows: int,
        inner_rows: int,
        outer_sorted: bool,
        inner_sorted: bool,
        selectivity: float = 1.0
    ) -> CostEstimate:
        """
        [START_CONTRACT_MERGE_JOIN_COST]
        Intent: Оценка стоимости merge join.
        Input: outer_rows, inner_rows - размеры; sorted - отсортированы ли.
        Output: CostEstimate с оценкой стоимости.
        Formula: O(M + N) if sorted, O(M*log(M) + N*log(N)) otherwise.
        [END_CONTRACT_MERGE_JOIN_COST]
        """
        # Sort costs if not already sorted
        outer_sort_cost = 0.0
        inner_sort_cost = 0.0
        
        if not outer_sorted:
            # O(M * log(M))
            outer_sort_cost = outer_rows * (self.cpu_tuple_cost + self.cpu_operator_cost)
            if outer_rows > 1:
                import math
                outer_sort_cost *= math.log2(outer_rows)
        
        if not inner_sorted:
            inner_sort_cost = inner_rows * (self.cpu_tuple_cost + self.cpu_operator_cost)
            if inner_rows > 1:
                import math
                inner_sort_cost *= math.log2(inner_rows)
        
        # Merge phase: O(M + N)
        merge_cost = (outer_rows + inner_rows) * self.cpu_tuple_cost
        merge_cost += (outer_rows + inner_rows) * self.cpu_operator_cost
        
        startup_cost = outer_sort_cost + inner_sort_cost
        cpu_cost = startup_cost + merge_cost
        output_rows = (outer_rows + inner_rows) * selectivity * 0.5
        
        return CostEstimate(
            total_cost=cpu_cost,
            startup_cost=startup_cost,
            estimated_rows=output_rows,
            cpu_cost=cpu_cost,
            io_cost=0.0,
            plan_type="MergeJoin"
        )
    
    def estimate_aggregate_cost(
        self,
        input_rows: int,
        group_count: int,
        aggregate_functions: int = 1
    ) -> CostEstimate:
        """
        [START_CONTRACT_AGGREGATE_COST]
        Intent: Оценка стоимости агрегации.
        Input: input_rows - входные строки; group_count - количество групп.
        Output: CostEstimate с оценкой стоимости.
        Formula: O(N) for hash aggregate, O(N * log(G)) for sort aggregate.
        [END_CONTRACT_AGGREGATE_COST]
        """
        # Hash aggregate: O(N)
        cpu_cost = input_rows * self.cpu_tuple_cost
        cpu_cost += input_rows * self.cpu_operator_cost * aggregate_functions
        
        # Output one row per group
        output_rows = group_count
        
        return CostEstimate(
            total_cost=cpu_cost,
            startup_cost=0.0,
            estimated_rows=output_rows,
            cpu_cost=cpu_cost,
            io_cost=0.0,
            plan_type="Aggregate"
        )
    
    def estimate_sort_cost(
        self,
        input_rows: int,
        width: int
    ) -> CostEstimate:
        """
        [START_CONTRACT_SORT_COST]
        Intent: Оценка стоимости сортировки.
        Input: input_rows - строки; width - ширина строки.
        Output: CostEstimate с оценкой стоимости.
        Formula: O(N * log(N)) comparisons.
        [END_CONTRACT_SORT_COST]
        """
        if input_rows <= 1:
            return CostEstimate(
                total_cost=0.0,
                estimated_rows=input_rows,
                plan_type="Sort"
            )
        
        import math
        log_n = math.log2(input_rows)
        
        # Comparison cost
        cpu_cost = input_rows * log_n * self.cpu_operator_cost
        
        # Tuple handling
        cpu_cost += input_rows * self.cpu_tuple_cost
        
        # Memory consideration
        sort_size = input_rows * width
        if sort_size > self.effective_cache_size * 4096:
            # External sort - extra I/O
            io_cost = sort_size / 4096 * self.seq_page_cost * 2  # Read + write
        else:
            io_cost = 0.0
        
        total_cost = cpu_cost + io_cost
        
        return CostEstimate(
            total_cost=total_cost,
            startup_cost=total_cost,  # Must complete sort before output
            estimated_rows=input_rows,
            cpu_cost=cpu_cost,
            io_cost=io_cost,
            plan_type="Sort"
        )
    
    def choose_join_type(
        self,
        outer_rows: int,
        inner_rows: int,
        outer_sorted: bool = False,
        inner_sorted: bool = False,
        selectivity: float = 0.1
    ) -> JoinType:
        """
        [START_CONTRACT_CHOOSE_JOIN]
        Intent: Выбрать оптимальный тип JOIN.
        Input: Размеры таблиц и их свойства.
        Output: Оптимальный JoinType.
        [END_CONTRACT_CHOOSE_JOIN]
        """
        # Small tables: nested loop is fine
        if outer_rows < 100 and inner_rows < 100:
            return JoinType.NESTED_LOOP
        
        # Both sorted and medium size: merge join
        if outer_sorted and inner_sorted and outer_rows < 10000 and inner_rows < 10000:
            return JoinType.MERGE_JOIN
        
        # Large tables with equality: hash join
        if outer_rows * inner_rows > 10000:
            return JoinType.HASH_JOIN
        
        # Default: nested loop for small, hash for large
        if outer_rows * inner_rows < 1000:
            return JoinType.NESTED_LOOP
        
        return JoinType.HASH_JOIN
    
    def choose_scan_type(
        self,
        row_count: int,
        selectivity: float,
        has_index: bool
    ) -> ScanType:
        """
        [START_CONTRACT_CHOOSE_SCAN]
        Intent: Выбрать оптимальный тип сканирования.
        Input: row_count - строки; selectivity - селективность; has_index - есть индекс.
        Output: Оптимальный ScanType.
        [END_CONTRACT_CHOOSE_SCAN]
        """
        if not has_index:
            return ScanType.SEQ_SCAN
        
        # Heuristic: use index for low selectivity
        if selectivity < 0.1:
            return ScanType.INDEX_SCAN
        elif selectivity < 0.3:
            return ScanType.BITMAP_SCAN
        else:
            return ScanType.SEQ_SCAN


# END_BLOCK_COST_MODEL