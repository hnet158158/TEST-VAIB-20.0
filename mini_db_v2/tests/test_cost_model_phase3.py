# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_cost_model_phase3
# Intent: Тесты для Phase 3 - Cost Model Module
# Dependencies: pytest, mini_db_v2.optimizer.cost_model
# END_MODULE_CONTRACT

"""
Phase 3 Cost Model Tests

Тестирует:
- CostEstimate, OperatorCost data classes
- CostModel.estimate_seq_scan_cost()
- CostModel.estimate_index_scan_cost()
- CostModel.estimate_nested_loop_join_cost()
- CostModel.estimate_hash_join_cost()
- CostModel.estimate_merge_join_cost()
- CostModel.choose_join_type()
- CostModel.choose_scan_type()
"""

import pytest
import math
from typing import Any

from mini_db_v2.optimizer.cost_model import (
    CostModel,
    CostEstimate,
    OperatorCost,
    JoinType,
    ScanType,
)


# =============================================================================
# OperatorCost Tests
# =============================================================================

class TestOperatorCost:
    """Тесты для OperatorCost dataclass."""

    def test_operator_cost_default_values(self):
        """OperatorCost с default значениями."""
        cost = OperatorCost()
        assert cost.startup == 0.0
        assert cost.total == 0.0
        assert cost.rows == 0.0
        assert cost.width == 0.0

    def test_operator_cost_with_values(self):
        """OperatorCost с заданными значениями."""
        cost = OperatorCost(startup=10.0, total=100.0, rows=50.0, width=128.0)
        assert cost.startup == 10.0
        assert cost.total == 100.0
        assert cost.rows == 50.0
        assert cost.width == 128.0

    def test_operator_cost_addition(self):
        """Сложение OperatorCost."""
        cost1 = OperatorCost(startup=10.0, total=100.0, rows=50.0, width=64.0)
        cost2 = OperatorCost(startup=5.0, total=50.0, rows=25.0, width=32.0)
        
        result = cost1 + cost2
        
        assert result.startup == 15.0
        assert result.total == 150.0
        assert result.rows == 25.0  # Takes second's rows
        assert result.width == 32.0  # Takes second's width


# =============================================================================
# CostEstimate Tests
# =============================================================================

class TestCostEstimate:
    """Тесты для CostEstimate dataclass."""

    def test_cost_estimate_default_values(self):
        """CostEstimate с default значениями."""
        estimate = CostEstimate()
        assert estimate.total_cost == 0.0
        assert estimate.startup_cost == 0.0
        assert estimate.estimated_rows == 0.0
        assert estimate.estimated_width == 0.0
        assert estimate.cpu_cost == 0.0
        assert estimate.io_cost == 0.0
        assert estimate.plan_type == ""
        assert estimate.children == []

    def test_cost_estimate_with_values(self):
        """CostEstimate с заданными значениями."""
        estimate = CostEstimate(
            total_cost=100.0,
            startup_cost=10.0,
            estimated_rows=50.0,
            estimated_width=128.0,
            cpu_cost=40.0,
            io_cost=60.0,
            plan_type="SeqScan"
        )
        assert estimate.total_cost == 100.0
        assert estimate.startup_cost == 10.0
        assert estimate.estimated_rows == 50.0
        assert estimate.cpu_cost == 40.0
        assert estimate.io_cost == 60.0
        assert estimate.plan_type == "SeqScan"

    def test_cost_estimate_comparison(self):
        """Сравнение CostEstimate."""
        low = CostEstimate(total_cost=10.0)
        high = CostEstimate(total_cost=100.0)
        
        assert low < high
        assert low <= high
        assert high > low
        assert high >= low
        assert low <= low

    def test_cost_estimate_with_children(self):
        """CostEstimate с дочерними планами."""
        child1 = CostEstimate(total_cost=10.0, plan_type="SeqScan")
        child2 = CostEstimate(total_cost=20.0, plan_type="IndexScan")
        parent = CostEstimate(
            total_cost=35.0,
            plan_type="NestedLoop",
            children=[child1, child2]
        )
        
        assert len(parent.children) == 2
        assert parent.children[0].plan_type == "SeqScan"
        assert parent.children[1].plan_type == "IndexScan"


# =============================================================================
# CostModel Initialization Tests
# =============================================================================

class TestCostModelInit:
    """Тесты инициализации CostModel."""

    def test_cost_model_default_params(self):
        """CostModel с default параметрами."""
        model = CostModel()
        
        assert model.seq_page_cost == CostModel.DEFAULT_SEQ_PAGE_COST
        assert model.random_page_cost == CostModel.DEFAULT_RANDOM_PAGE_COST
        assert model.cpu_tuple_cost == CostModel.DEFAULT_CPU_TUPLE_COST
        assert model.cpu_index_tuple_cost == CostModel.DEFAULT_CPU_INDEX_TUPLE_COST
        assert model.cpu_operator_cost == CostModel.DEFAULT_CPU_OPERATOR_COST

    def test_cost_model_custom_params(self):
        """CostModel с кастомными параметрами."""
        model = CostModel(
            seq_page_cost=2.0,
            random_page_cost=8.0,
            cpu_tuple_cost=0.02,
            cpu_index_tuple_cost=0.01,
            cpu_operator_cost=0.005
        )
        
        assert model.seq_page_cost == 2.0
        assert model.random_page_cost == 8.0
        assert model.cpu_tuple_cost == 0.02
        assert model.cpu_index_tuple_cost == 0.01
        assert model.cpu_operator_cost == 0.005


# =============================================================================
# Sequential Scan Cost Tests
# =============================================================================

class TestSeqScanCost:
    """Тесты для estimate_seq_scan_cost()."""

    def test_seq_scan_basic(self):
        """Базовая оценка seq scan."""
        model = CostModel()
        
        estimate = model.estimate_seq_scan_cost(
            row_count=1000,
            page_count=100,
            selectivity=1.0
        )
        
        # IO cost = 100 * 1.0 = 100
        # CPU cost = 1000 * 0.01 = 10
        # Total = 110
        assert estimate.total_cost == pytest.approx(110.0, rel=0.01)
        assert estimate.io_cost == pytest.approx(100.0, rel=0.01)
        assert estimate.cpu_cost == pytest.approx(10.0, rel=0.01)
        assert estimate.estimated_rows == 1000
        assert estimate.plan_type == "SeqScan"

    def test_seq_scan_with_selectivity(self):
        """Seq scan с селективностью."""
        model = CostModel()
        
        estimate = model.estimate_seq_scan_cost(
            row_count=1000,
            page_count=100,
            selectivity=0.1
        )
        
        # IO cost = 100 (still read all pages)
        # CPU cost = 1000 * 0.01 = 10
        # Output rows = 1000 * 0.1 = 100
        assert estimate.total_cost == pytest.approx(110.0, rel=0.01)
        assert estimate.estimated_rows == 100

    def test_seq_scan_empty_table(self):
        """Seq scan для пустой таблицы."""
        model = CostModel()
        
        estimate = model.estimate_seq_scan_cost(
            row_count=0,
            page_count=0,
            selectivity=1.0
        )
        
        assert estimate.total_cost == 0.0
        assert estimate.estimated_rows == 0

    def test_seq_scan_large_table(self):
        """Seq scan для большой таблицы."""
        model = CostModel()
        
        estimate = model.estimate_seq_scan_cost(
            row_count=1_000_000,
            page_count=100_000,
            selectivity=0.01
        )
        
        # IO cost = 100000 * 1.0 = 100000
        # CPU cost = 1000000 * 0.01 = 10000
        assert estimate.total_cost == pytest.approx(110000.0, rel=0.01)
        assert estimate.estimated_rows == 10000


# =============================================================================
# Index Scan Cost Tests
# =============================================================================

class TestIndexScanCost:
    """Тесты для estimate_index_scan_cost()."""

    def test_index_scan_basic(self):
        """Базовая оценка index scan."""
        model = CostModel()
        
        estimate = model.estimate_index_scan_cost(
            row_count=1000,
            selectivity=0.01,
            index_height=3,
            has_index=True
        )
        
        # K = 1000 * 0.01 = 10 rows
        # Index traversal = 3 * 4.0 = 12
        # Data fetch = 10 * 4.0 = 40
        # Total IO = 12 + 40 = 52
        assert estimate.total_cost > 0
        assert estimate.plan_type == "IndexScan"
        assert estimate.estimated_rows == 10

    def test_index_scan_no_index(self):
        """Index scan без индекса - fallback to seq scan."""
        model = CostModel()
        
        estimate = model.estimate_index_scan_cost(
            row_count=1000,
            selectivity=0.01,
            has_index=False
        )
        
        # Should fallback to seq scan
        assert estimate.plan_type == "SeqScan"

    def test_index_scan_high_selectivity(self):
        """Index scan с высокой селективностью - bitmap scan logic."""
        model = CostModel()
        
        # Selectivity > 0.1 triggers bitmap scan logic
        estimate = model.estimate_index_scan_cost(
            row_count=1000,
            selectivity=0.2,
            index_height=3,
            has_index=True
        )
        
        # K = 200 rows
        assert estimate.estimated_rows == 200
        assert estimate.total_cost > 0

    def test_index_scan_full_table(self):
        """Index scan с selectivity = 1.0 - fallback to seq scan."""
        model = CostModel()
        
        estimate = model.estimate_index_scan_cost(
            row_count=1000,
            selectivity=1.0,
            has_index=True
        )
        
        # Should fallback to seq scan for full table
        assert estimate.plan_type == "SeqScan"


# =============================================================================
# Nested Loop Join Cost Tests
# =============================================================================

class TestNestedLoopJoinCost:
    """Тесты для estimate_nested_loop_join_cost()."""

    def test_nested_loop_basic(self):
        """Базовая оценка nested loop join."""
        model = CostModel()
        
        estimate = model.estimate_nested_loop_join_cost(
            outer_rows=100,
            inner_rows=10,
            inner_cost=5.0,
            selectivity=0.1
        )
        
        # Total = 100 * (5.0 + 0.01) + 100 * 10 * 0.0025
        assert estimate.total_cost > 0
        assert estimate.plan_type == "NestedLoop"
        assert estimate.estimated_rows == 100 * 10 * 0.1

    def test_nested_loop_small_tables(self):
        """Nested loop для маленьких таблиц."""
        model = CostModel()
        
        estimate = model.estimate_nested_loop_join_cost(
            outer_rows=10,
            inner_rows=5,
            inner_cost=1.0,
            selectivity=1.0
        )
        
        assert estimate.estimated_rows == 50  # 10 * 5 * 1.0

    def test_nested_loop_large_tables(self):
        """Nested loop для больших таблиц - дорого!"""
        model = CostModel()
        
        estimate = model.estimate_nested_loop_join_cost(
            outer_rows=10000,
            inner_rows=1000,
            inner_cost=100.0,
            selectivity=0.01
        )
        
        # This should be expensive
        assert estimate.total_cost > 100000
        assert estimate.estimated_rows == 100000


# =============================================================================
# Hash Join Cost Tests
# =============================================================================

class TestHashJoinCost:
    """Тесты для estimate_hash_join_cost()."""

    def test_hash_join_basic(self):
        """Базовая оценка hash join."""
        model = CostModel()
        
        estimate = model.estimate_hash_join_cost(
            outer_rows=1000,
            inner_rows=500,
            outer_width=64,
            inner_width=32,
            selectivity=0.1
        )
        
        assert estimate.total_cost > 0
        assert estimate.plan_type == "HashJoin"
        assert estimate.startup_cost > 0  # Build phase
        assert estimate.estimated_rows == 1000 * 500 * 0.1

    def test_hash_join_small_tables(self):
        """Hash join для маленьких таблиц."""
        model = CostModel()
        
        estimate = model.estimate_hash_join_cost(
            outer_rows=10,
            inner_rows=5,
            outer_width=32,
            inner_width=32,
            selectivity=1.0
        )
        
        assert estimate.estimated_rows == 50

    def test_hash_join_large_tables(self):
        """Hash join для больших таблиц."""
        model = CostModel()
        
        estimate = model.estimate_hash_join_cost(
            outer_rows=100000,
            inner_rows=50000,
            outer_width=128,
            inner_width=64,
            selectivity=0.01
        )
        
        # Hash join scales linearly
        assert estimate.total_cost > 0
        assert estimate.estimated_rows == 50000000


# =============================================================================
# Merge Join Cost Tests
# =============================================================================

class TestMergeJoinCost:
    """Тесты для estimate_merge_join_cost()."""

    def test_merge_join_sorted(self):
        """Merge join для отсортированных данных."""
        model = CostModel()
        
        estimate = model.estimate_merge_join_cost(
            outer_rows=1000,
            inner_rows=500,
            outer_sorted=True,
            inner_sorted=True,
            selectivity=0.1
        )
        
        # No sort cost - just merge
        assert estimate.startup_cost == 0
        assert estimate.total_cost > 0
        assert estimate.plan_type == "MergeJoin"

    def test_merge_join_unsorted(self):
        """Merge join для несортированных данных."""
        model = CostModel()
        
        estimate = model.estimate_merge_join_cost(
            outer_rows=1000,
            inner_rows=500,
            outer_sorted=False,
            inner_sorted=False,
            selectivity=0.1
        )
        
        # Sort cost included
        assert estimate.startup_cost > 0
        assert estimate.total_cost > estimate.startup_cost

    def test_merge_join_partial_sorted(self):
        """Merge join с частично отсортированными данными."""
        model = CostModel()
        
        estimate_sorted_outer = model.estimate_merge_join_cost(
            outer_rows=1000,
            inner_rows=500,
            outer_sorted=True,
            inner_sorted=False,
            selectivity=0.1
        )
        
        estimate_sorted_inner = model.estimate_merge_join_cost(
            outer_rows=1000,
            inner_rows=500,
            outer_sorted=False,
            inner_sorted=True,
            selectivity=0.1
        )
        
        # Both should have some sort cost
        assert estimate_sorted_outer.startup_cost > 0
        assert estimate_sorted_inner.startup_cost > 0


# =============================================================================
# Aggregate Cost Tests
# =============================================================================

class TestAggregateCost:
    """Тесты для estimate_aggregate_cost()."""

    def test_aggregate_basic(self):
        """Базовая оценка агрегации."""
        model = CostModel()
        
        estimate = model.estimate_aggregate_cost(
            input_rows=1000,
            group_count=10,
            aggregate_functions=1
        )
        
        assert estimate.total_cost > 0
        assert estimate.estimated_rows == 10
        assert estimate.plan_type == "Aggregate"

    def test_aggregate_multiple_functions(self):
        """Агрегация с несколькими функциями."""
        model = CostModel()
        
        estimate1 = model.estimate_aggregate_cost(
            input_rows=1000,
            group_count=10,
            aggregate_functions=1
        )
        
        estimate3 = model.estimate_aggregate_cost(
            input_rows=1000,
            group_count=10,
            aggregate_functions=3
        )
        
        # More functions = higher cost
        assert estimate3.total_cost > estimate1.total_cost

    def test_aggregate_no_groups(self):
        """Агрегация без GROUP BY."""
        model = CostModel()
        
        estimate = model.estimate_aggregate_cost(
            input_rows=1000,
            group_count=1,  # Single group
            aggregate_functions=1
        )
        
        assert estimate.estimated_rows == 1


# =============================================================================
# Sort Cost Tests
# =============================================================================

class TestSortCost:
    """Тесты для estimate_sort_cost()."""

    def test_sort_basic(self):
        """Базовая оценка сортировки."""
        model = CostModel()
        
        estimate = model.estimate_sort_cost(
            input_rows=1000,
            width=64
        )
        
        # O(N * log(N))
        expected_comparisons = 1000 * math.log2(1000)
        assert estimate.total_cost > 0
        assert estimate.plan_type == "Sort"

    def test_sort_single_row(self):
        """Сортировка одной строки."""
        model = CostModel()
        
        estimate = model.estimate_sort_cost(
            input_rows=1,
            width=64
        )
        
        assert estimate.total_cost == 0.0

    def test_sort_empty(self):
        """Сортировка пустого набора."""
        model = CostModel()
        
        estimate = model.estimate_sort_cost(
            input_rows=0,
            width=64
        )
        
        assert estimate.total_cost == 0.0

    def test_sort_large_dataset(self):
        """Сортировка большого набора."""
        model = CostModel()
        
        estimate = model.estimate_sort_cost(
            input_rows=100000,
            width=128
        )
        
        # Should be expensive
        assert estimate.total_cost > 1000


# =============================================================================
# Choose Join Type Tests
# =============================================================================

class TestChooseJoinType:
    """Тесты для choose_join_type()."""

    def test_choose_join_small_tables(self):
        """Выбор JOIN для маленьких таблиц."""
        model = CostModel()
        
        # Very small tables: nested loop
        result = model.choose_join_type(
            outer_rows=10,
            inner_rows=5
        )
        assert result == JoinType.NESTED_LOOP

    def test_choose_join_sorted_medium(self):
        """Выбор JOIN для отсортированных средних таблиц."""
        model = CostModel()
        
        # Sorted medium tables: merge join
        result = model.choose_join_type(
            outer_rows=1000,
            inner_rows=1000,
            outer_sorted=True,
            inner_sorted=True
        )
        assert result == JoinType.MERGE_JOIN

    def test_choose_join_large_tables(self):
        """Выбор JOIN для больших таблиц."""
        model = CostModel()
        
        # Large tables: hash join
        result = model.choose_join_type(
            outer_rows=10000,
            inner_rows=10000
        )
        assert result == JoinType.HASH_JOIN

    def test_choose_join_medium_unsorted(self):
        """Выбор JOIN для средних несортированных таблиц."""
        model = CostModel()
        
        result = model.choose_join_type(
            outer_rows=1000,
            inner_rows=1000,
            outer_sorted=False,
            inner_sorted=False
        )
        # Should prefer hash join for medium unsorted
        assert result in [JoinType.HASH_JOIN, JoinType.NESTED_LOOP]


# =============================================================================
# Choose Scan Type Tests
# =============================================================================

class TestChooseScanType:
    """Тесты для choose_scan_type()."""

    def test_choose_scan_no_index(self):
        """Выбор scan без индекса."""
        model = CostModel()
        
        result = model.choose_scan_type(
            row_count=1000,
            selectivity=0.01,
            has_index=False
        )
        assert result == ScanType.SEQ_SCAN

    def test_choose_scan_low_selectivity(self):
        """Выбор scan с низкой селективностью."""
        model = CostModel()
        
        # Low selectivity: index scan
        result = model.choose_scan_type(
            row_count=1000,
            selectivity=0.01,
            has_index=True
        )
        assert result == ScanType.INDEX_SCAN

    def test_choose_scan_medium_selectivity(self):
        """Выбор scan с средней селективностью."""
        model = CostModel()
        
        # Medium selectivity: bitmap scan
        result = model.choose_scan_type(
            row_count=1000,
            selectivity=0.2,
            has_index=True
        )
        assert result == ScanType.BITMAP_SCAN

    def test_choose_scan_high_selectivity(self):
        """Выбор scan с высокой селективностью."""
        model = CostModel()
        
        # High selectivity: seq scan
        result = model.choose_scan_type(
            row_count=1000,
            selectivity=0.5,
            has_index=True
        )
        assert result == ScanType.SEQ_SCAN


# =============================================================================
# Integration Tests
# =============================================================================

class TestCostModelIntegration:
    """Интеграционные тесты CostModel."""

    def test_compare_scan_costs(self):
        """Сравнение стоимости scan методов."""
        model = CostModel()
        
        # Small selectivity
        seq_cost = model.estimate_seq_scan_cost(10000, 1000, 0.01)
        idx_cost = model.estimate_index_scan_cost(10000, 0.01, has_index=True)
        
        # Index should be cheaper for low selectivity
        assert idx_cost.total_cost < seq_cost.total_cost

    def test_compare_join_costs(self):
        """Сравнение стоимости JOIN методов."""
        model = CostModel()
        
        # Large tables
        nl_cost = model.estimate_nested_loop_join_cost(10000, 1000, 100.0, 0.1)
        hash_cost = model.estimate_hash_join_cost(10000, 1000, 64, 64, 0.1)
        
        # Hash join should be cheaper for large tables
        assert hash_cost.total_cost < nl_cost.total_cost

    def test_full_query_cost(self):
        """Оценка стоимости полного запроса."""
        model = CostModel()
        
        # Query: SELECT * FROM users WHERE age > 30
        # Assume: 10000 rows, 1000 pages, selectivity 0.3
        
        scan_cost = model.estimate_seq_scan_cost(10000, 1000, 0.3)
        
        # Total query cost
        assert scan_cost.total_cost > 0
        assert scan_cost.estimated_rows == 3000


# =============================================================================
# Edge Cases Tests
# =============================================================================

class TestCostModelEdgeCases:
    """Граничные случаи для CostModel."""

    def test_zero_rows(self):
        """Оценка для 0 строк."""
        model = CostModel()
        
        seq = model.estimate_seq_scan_cost(0, 0, 1.0)
        assert seq.total_cost == 0.0
        
        idx = model.estimate_index_scan_cost(0, 0.1, has_index=True)
        assert idx.total_cost >= 0

    def test_very_high_selectivity(self):
        """Очень высокая селективность."""
        model = CostModel()
        
        # selectivity = 1.0 triggers fallback to seq scan
        estimate = model.estimate_index_scan_cost(
            row_count=1000,
            selectivity=1.0,
            has_index=True
        )
        
        # Should fallback to seq scan when selectivity >= 1.0
        assert estimate.plan_type == "SeqScan"

    def test_very_low_selectivity(self):
        """Очень низкая селективность."""
        model = CostModel()
        
        estimate = model.estimate_index_scan_cost(
            row_count=1000000,
            selectivity=0.0001,
            has_index=True
        )
        
        # Index scan should be very efficient
        assert estimate.estimated_rows == 100
        assert estimate.plan_type == "IndexScan"

    def test_custom_cost_parameters(self):
        """Кастомные параметры стоимости."""
        model = CostModel(
            seq_page_cost=10.0,  # Very expensive I/O
            random_page_cost=100.0,
            cpu_tuple_cost=0.001
        )
        
        estimate = model.estimate_seq_scan_cost(1000, 100, 1.0)
        
        # IO cost should dominate
        assert estimate.io_cost > estimate.cpu_cost