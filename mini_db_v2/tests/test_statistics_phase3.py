# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_statistics_phase3
# Intent: Тесты для Phase 3 - Statistics Module
# Dependencies: pytest, mini_db_v2.optimizer.statistics
# END_MODULE_CONTRACT

"""
Phase 3 Statistics Tests

Тестирует:
- TableStats, ColumnStats, HistogramBucket data classes
- Statistics class (get/set/drop operations)
- StatisticsManager.analyze_table()
- estimate_selectivity() для =, <, >, BETWEEN
"""

import pytest
from datetime import datetime
from typing import Any

from mini_db_v2.optimizer.statistics import (
    Statistics,
    TableStats,
    ColumnStats,
    HistogramBucket,
    StatisticsManager,
)


# =============================================================================
# TableStats Tests
# =============================================================================

class TestTableStats:
    """Тесты для TableStats dataclass."""

    def test_table_stats_default_values(self):
        """TableStats с default значениями."""
        stats = TableStats()
        assert stats.row_count == 0
        assert stats.page_count == 0
        assert stats.last_analyze is None
        assert stats.table_name == ""
        assert stats.avg_row_size == 0

    def test_table_stats_with_values(self):
        """TableStats с заданными значениями."""
        now = datetime.now()
        stats = TableStats(
            row_count=1000,
            page_count=100,
            last_analyze=now,
            table_name="users",
            avg_row_size=128
        )
        assert stats.row_count == 1000
        assert stats.page_count == 100
        assert stats.last_analyze == now
        assert stats.table_name == "users"
        assert stats.avg_row_size == 128

    def test_table_stats_large_values(self):
        """TableStats с большими значениями."""
        stats = TableStats(
            row_count=10_000_000,
            page_count=1_000_000,
            avg_row_size=4096
        )
        assert stats.row_count == 10_000_000
        assert stats.page_count == 1_000_000


# =============================================================================
# ColumnStats Tests
# =============================================================================

class TestColumnStats:
    """Тесты для ColumnStats dataclass."""

    def test_column_stats_default_values(self):
        """ColumnStats с default значениями."""
        stats = ColumnStats()
        assert stats.column_name == ""
        assert stats.distinct_values == 0
        assert stats.null_count == 0
        assert stats.null_fraction == 0.0
        assert stats.min_value is None
        assert stats.max_value is None
        assert stats.histogram == []
        assert stats.most_common_values == []
        assert stats.most_common_freqs == []

    def test_column_stats_with_values(self):
        """ColumnStats с заданными значениями."""
        histogram = [
            HistogramBucket(lower_bound=0, upper_bound=10, count=100, distinct_count=10),
            HistogramBucket(lower_bound=10, upper_bound=20, count=150, distinct_count=10),
        ]
        stats = ColumnStats(
            column_name="age",
            distinct_values=100,
            null_count=5,
            null_fraction=0.05,
            min_value=0,
            max_value=99,
            histogram=histogram,
            most_common_values=[25, 30, 35],
            most_common_freqs=[0.1, 0.08, 0.06]
        )
        assert stats.column_name == "age"
        assert stats.distinct_values == 100
        assert stats.null_count == 5
        assert stats.null_fraction == 0.05
        assert stats.min_value == 0
        assert stats.max_value == 99
        assert len(stats.histogram) == 2
        assert len(stats.most_common_values) == 3


# =============================================================================
# HistogramBucket Tests
# =============================================================================

class TestHistogramBucket:
    """Тесты для HistogramBucket dataclass."""

    def test_histogram_bucket_default_values(self):
        """HistogramBucket с default значениями."""
        bucket = HistogramBucket(lower_bound=0, upper_bound=10)
        assert bucket.lower_bound == 0
        assert bucket.upper_bound == 10
        assert bucket.count == 0
        assert bucket.distinct_count == 0

    def test_histogram_bucket_with_values(self):
        """HistogramBucket с заданными значениями."""
        bucket = HistogramBucket(
            lower_bound=10,
            upper_bound=20,
            count=150,
            distinct_count=10
        )
        assert bucket.lower_bound == 10
        assert bucket.upper_bound == 20
        assert bucket.count == 150
        assert bucket.distinct_count == 10

    def test_histogram_bucket_string_bounds(self):
        """HistogramBucket со строковыми границами."""
        bucket = HistogramBucket(
            lower_bound="apple",
            upper_bound="banana",
            count=50,
            distinct_count=5
        )
        assert bucket.lower_bound == "apple"
        assert bucket.upper_bound == "banana"


# =============================================================================
# Statistics Tests
# =============================================================================

class TestStatistics:
    """Тесты для Statistics class."""

    def test_statistics_init(self):
        """Инициализация Statistics."""
        stats = Statistics()
        assert stats.get_table_stats("nonexistent") is None
        assert stats.get_column_stats("table", "col") is None

    def test_statistics_set_get_table_stats(self):
        """Установка и получение статистики таблицы."""
        stats = Statistics()
        table_stats = TableStats(row_count=1000, page_count=100)
        
        stats.set_table_stats("users", table_stats)
        
        result = stats.get_table_stats("users")
        assert result is not None
        assert result.row_count == 1000
        assert result.page_count == 100
        assert result.table_name == "users"

    def test_statistics_set_get_column_stats(self):
        """Установка и получение статистики колонки."""
        stats = Statistics()
        col_stats = ColumnStats(
            column_name="age",
            distinct_values=50,
            null_count=10
        )
        
        stats.set_column_stats("users", "age", col_stats)
        
        result = stats.get_column_stats("users", "age")
        assert result is not None
        assert result.column_name == "age"
        assert result.distinct_values == 50

    def test_statistics_drop_table_stats(self):
        """Удаление статистики таблицы."""
        stats = Statistics()
        
        # Add table and column stats
        stats.set_table_stats("users", TableStats(row_count=100))
        stats.set_column_stats("users", "id", ColumnStats(distinct_values=100))
        stats.set_column_stats("users", "name", ColumnStats(distinct_values=50))
        
        # Add another table
        stats.set_table_stats("orders", TableStats(row_count=500))
        stats.set_column_stats("orders", "id", ColumnStats(distinct_values=500))
        
        # Drop users
        stats.drop_table_stats("users")
        
        # Verify users is gone
        assert stats.get_table_stats("users") is None
        assert stats.get_column_stats("users", "id") is None
        assert stats.get_column_stats("users", "name") is None
        
        # Verify orders still exists
        assert stats.get_table_stats("orders") is not None
        assert stats.get_column_stats("orders", "id") is not None

    def test_statistics_overwrite_table_stats(self):
        """Перезапись статистики таблицы."""
        stats = Statistics()
        
        stats.set_table_stats("users", TableStats(row_count=100))
        stats.set_table_stats("users", TableStats(row_count=200))
        
        result = stats.get_table_stats("users")
        assert result.row_count == 200


# =============================================================================
# Estimate Selectivity Tests
# =============================================================================

class TestEstimateSelectivity:
    """Тесты для estimate_selectivity()."""

    def test_estimate_selectivity_no_stats(self):
        """Оценка selectivity без статистики."""
        stats = Statistics()
        
        # Should return default selectivity
        selectivity = stats.estimate_selectivity("users", "age", "=", 25)
        assert selectivity == 0.1

    def test_estimate_selectivity_equality_with_distinct(self):
        """Оценка selectivity для equality по distinct values."""
        stats = Statistics()
        stats.set_table_stats("users", TableStats(row_count=1000))
        stats.set_column_stats("users", "status", ColumnStats(
            distinct_values=10,
            most_common_values=[],
            most_common_freqs=[]
        ))
        
        selectivity = stats.estimate_selectivity("users", "status", "=", "active")
        # 1 / 10 distinct values = 0.1
        assert selectivity == pytest.approx(0.1, rel=0.01)

    def test_estimate_selectivity_equality_with_mcv(self):
        """Оценка selectivity для equality по MCV."""
        stats = Statistics()
        stats.set_table_stats("users", TableStats(row_count=1000))
        stats.set_column_stats("users", "status", ColumnStats(
            distinct_values=10,
            most_common_values=["active", "inactive", "pending"],
            most_common_freqs=[0.5, 0.3, 0.1]
        ))
        
        # active is in MCV with freq 0.5
        selectivity = stats.estimate_selectivity("users", "status", "=", "active")
        assert selectivity == pytest.approx(0.5, rel=0.01)
        
        # inactive is in MCV with freq 0.3
        selectivity = stats.estimate_selectivity("users", "status", "=", "inactive")
        assert selectivity == pytest.approx(0.3, rel=0.01)

    def test_estimate_selectivity_range_less_than(self):
        """Оценка selectivity для col < value."""
        stats = Statistics()
        stats.set_table_stats("users", TableStats(row_count=1000))
        stats.set_column_stats("users", "age", ColumnStats(
            distinct_values=100,
            min_value=0,
            max_value=99
        ))
        
        # age < 50 should be ~0.5
        selectivity = stats.estimate_selectivity("users", "age", "<", 50)
        assert 0.4 <= selectivity <= 0.6
        
        # age < 0 should be 0
        selectivity = stats.estimate_selectivity("users", "age", "<", 0)
        assert selectivity == 0.0
        
        # age < 100 should be 1
        selectivity = stats.estimate_selectivity("users", "age", "<", 100)
        assert selectivity == 1.0

    def test_estimate_selectivity_range_greater_than(self):
        """Оценка selectivity для col > value."""
        stats = Statistics()
        stats.set_table_stats("users", TableStats(row_count=1000))
        stats.set_column_stats("users", "age", ColumnStats(
            distinct_values=100,
            min_value=0,
            max_value=99
        ))
        
        # age > 50 should be ~0.5
        selectivity = stats.estimate_selectivity("users", "age", ">", 50)
        assert 0.4 <= selectivity <= 0.6
        
        # age > 99 should be 0
        selectivity = stats.estimate_selectivity("users", "age", ">", 99)
        assert selectivity == 0.0
        
        # age > -1 should be 1
        selectivity = stats.estimate_selectivity("users", "age", ">", -1)
        assert selectivity == 1.0

    def test_estimate_selectivity_with_histogram(self):
        """Оценка selectivity с гистограммой."""
        stats = Statistics()
        stats.set_table_stats("users", TableStats(row_count=1000))
        
        # Create histogram with 10 buckets
        histogram = []
        for i in range(10):
            histogram.append(HistogramBucket(
                lower_bound=i * 10,
                upper_bound=(i + 1) * 10 - 1,
                count=100,
                distinct_count=10
            ))
        
        stats.set_column_stats("users", "score", ColumnStats(
            distinct_values=100,
            min_value=0,
            max_value=99,
            histogram=histogram
        ))
        
        # score < 50 should be ~0.5 (5 buckets out of 10)
        selectivity = stats.estimate_selectivity("users", "score", "<", 50)
        assert 0.4 <= selectivity <= 0.6

    def test_estimate_selectivity_between(self):
        """Оценка selectivity для BETWEEN."""
        stats = Statistics()
        stats.set_table_stats("users", TableStats(row_count=1000))
        stats.set_column_stats("users", "age", ColumnStats(
            distinct_values=100,
            min_value=0,
            max_value=99
        ))
        
        # BETWEEN uses default selectivity
        selectivity = stats.estimate_selectivity("users", "age", "BETWEEN", (20, 40))
        assert selectivity == 0.3

    def test_estimate_selectivity_less_equal(self):
        """Оценка selectivity для <=."""
        stats = Statistics()
        stats.set_table_stats("users", TableStats(row_count=1000))
        stats.set_column_stats("users", "age", ColumnStats(
            distinct_values=100,
            min_value=0,
            max_value=99
        ))
        
        selectivity = stats.estimate_selectivity("users", "age", "<=", 50)
        assert 0.4 <= selectivity <= 0.6

    def test_estimate_selectivity_greater_equal(self):
        """Оценка selectivity для >=."""
        stats = Statistics()
        stats.set_table_stats("users", TableStats(row_count=1000))
        stats.set_column_stats("users", "age", ColumnStats(
            distinct_values=100,
            min_value=0,
            max_value=99
        ))
        
        selectivity = stats.estimate_selectivity("users", "age", ">=", 50)
        assert 0.4 <= selectivity <= 0.6


# =============================================================================
# StatisticsManager Tests
# =============================================================================

class TestStatisticsManager:
    """Тесты для StatisticsManager."""

    def test_statistics_manager_init(self):
        """Инициализация StatisticsManager."""
        stats = Statistics()
        manager = StatisticsManager(stats)
        assert manager._statistics is stats

    def test_analyze_table_empty(self):
        """Анализ пустой таблицы."""
        from dataclasses import dataclass
        
        @dataclass
        class MockRow:
            data: dict
        
        class MockTable:
            name = "empty_table"
            column_names = ["id", "name"]
            def select(self):
                return []
        
        stats = Statistics()
        manager = StatisticsManager(stats)
        
        table_stats = manager.analyze_table(MockTable())
        
        assert table_stats.row_count == 0
        assert table_stats.page_count >= 0

    def test_analyze_table_with_data(self):
        """Анализ таблицы с данными."""
        from dataclasses import dataclass
        
        @dataclass
        class MockRow:
            data: dict
        
        class MockTable:
            name = "users"
            column_names = ["id", "name", "age"]
            def select(self):
                return [
                    MockRow({"id": 1, "name": "Alice", "age": 25}),
                    MockRow({"id": 2, "name": "Bob", "age": 30}),
                    MockRow({"id": 3, "name": "Charlie", "age": 25}),
                    MockRow({"id": 4, "name": "Diana", "age": 30}),
                    MockRow({"id": 5, "name": "Eve", "age": 35}),
                ]
        
        stats = Statistics()
        manager = StatisticsManager(stats)
        
        table_stats = manager.analyze_table(MockTable())
        
        assert table_stats.row_count == 5
        assert table_stats.last_analyze is not None
        
        # Check column stats
        id_stats = stats.get_column_stats("users", "id")
        assert id_stats is not None
        assert id_stats.distinct_values == 5
        assert id_stats.null_count == 0
        
        age_stats = stats.get_column_stats("users", "age")
        assert age_stats is not None
        assert age_stats.distinct_values == 3  # 25, 30, 35
        assert age_stats.min_value == 25
        assert age_stats.max_value == 35

    def test_analyze_table_with_nulls(self):
        """Анализ таблицы с NULL значениями."""
        from dataclasses import dataclass
        
        @dataclass
        class MockRow:
            data: dict
        
        class MockTable:
            name = "users"
            column_names = ["id", "name"]
            def select(self):
                return [
                    MockRow({"id": 1, "name": "Alice"}),
                    MockRow({"id": 2, "name": None}),
                    MockRow({"id": 3, "name": "Bob"}),
                    MockRow({"id": 4, "name": None}),
                ]
        
        stats = Statistics()
        manager = StatisticsManager(stats)
        
        manager.analyze_table(MockTable())
        
        name_stats = stats.get_column_stats("users", "name")
        assert name_stats is not None
        assert name_stats.null_count == 2
        assert name_stats.null_fraction == 0.5

    def test_analyze_table_mcv(self):
        """Анализ таблицы - MCV вычисляется корректно."""
        from dataclasses import dataclass
        
        @dataclass
        class MockRow:
            data: dict
        
        class MockTable:
            name = "logs"
            column_names = ["level"]
            def select(self):
                return [
                    MockRow({"level": "INFO"}),
                    MockRow({"level": "INFO"}),
                    MockRow({"level": "INFO"}),
                    MockRow({"level": "INFO"}),
                    MockRow({"level": "WARN"}),
                    MockRow({"level": "WARN"}),
                    MockRow({"level": "ERROR"}),
                ]
        
        stats = Statistics()
        manager = StatisticsManager(stats)
        
        manager.analyze_table(MockTable())
        
        level_stats = stats.get_column_stats("logs", "level")
        assert level_stats is not None
        assert level_stats.most_common_values[0] == "INFO"
        assert level_stats.most_common_freqs[0] == pytest.approx(4/7, rel=0.01)

    def test_analyze_table_histogram(self):
        """Анализ таблицы - гистограмма строится корректно."""
        from dataclasses import dataclass
        
        @dataclass
        class MockRow:
            data: dict
        
        class MockTable:
            name = "metrics"
            column_names = ["value"]
            def select(self):
                return [MockRow({"value": i}) for i in range(100)]
        
        stats = Statistics()
        manager = StatisticsManager(stats)
        
        manager.analyze_table(MockTable())
        
        value_stats = stats.get_column_stats("metrics", "value")
        assert value_stats is not None
        assert len(value_stats.histogram) > 0
        assert value_stats.min_value == 0
        assert value_stats.max_value == 99


# =============================================================================
# Thread Safety Tests
# =============================================================================

class TestStatisticsThreadSafety:
    """Тесты для thread safety Statistics."""

    def test_concurrent_set_get(self):
        """Конкурентные set/get операции."""
        import threading
        
        stats = Statistics()
        errors = []
        
        def writer(table_name, count):
            try:
                for i in range(100):
                    stats.set_table_stats(table_name, TableStats(row_count=count + i))
            except Exception as e:
                errors.append(e)
        
        def reader(table_name):
            try:
                for _ in range(100):
                    result = stats.get_table_stats(table_name)
                    # Result can be None or valid
            except Exception as e:
                errors.append(e)
        
        threads = []
        for i in range(5):
            threads.append(threading.Thread(target=writer, args=(f"table_{i}", i * 100)))
            threads.append(threading.Thread(target=reader, args=(f"table_{i}",)))
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0


# =============================================================================
# Edge Cases Tests
# =============================================================================

class TestStatisticsEdgeCases:
    """Граничные случаи для Statistics."""

    def test_estimate_selectivity_incomparable_types(self):
        """Оценка selectivity для несравнимых типов."""
        stats = Statistics()
        stats.set_table_stats("users", TableStats(row_count=100))
        stats.set_column_stats("users", "data", ColumnStats(
            distinct_values=10,
            min_value=None,  # Incomparable
            max_value=None
        ))
        
        # Should return default for range
        selectivity = stats.estimate_selectivity("users", "data", "<", "value")
        assert 0.0 <= selectivity <= 1.0

    def test_estimate_selectivity_single_value(self):
        """Оценка selectivity для колонки с одним значением."""
        stats = Statistics()
        stats.set_table_stats("users", TableStats(row_count=100))
        stats.set_column_stats("users", "status", ColumnStats(
            distinct_values=1,
            most_common_values=["active"],
            most_common_freqs=[1.0]
        ))
        
        selectivity = stats.estimate_selectivity("users", "status", "=", "active")
        assert selectivity == pytest.approx(1.0, rel=0.01)

    def test_histogram_empty_bucket(self):
        """Гистограмма с пустыми bucket'ами."""
        bucket = HistogramBucket(lower_bound=0, upper_bound=10, count=0)
        assert bucket.count == 0

    def test_column_stats_negative_correlation(self):
        """ColumnStats с отрицательной корреляцией."""
        stats = ColumnStats(
            column_name="id",
            correlation=-0.8
        )
        assert stats.correlation == -0.8

    def test_estimate_row_size_various_types(self):
        """Оценка размера строки для различных типов."""
        from dataclasses import dataclass
        
        @dataclass
        class MockRow:
            data: dict
        
        class MockTable:
            name = "mixed"
            column_names = ["int_col", "str_col", "bool_col", "float_col"]
            def select(self):
                return [
                    MockRow({
                        "int_col": 42,
                        "str_col": "hello world",
                        "bool_col": True,
                        "float_col": 3.14159
                    })
                ]
        
        stats = Statistics()
        manager = StatisticsManager(stats)
        
        table_stats = manager.analyze_table(MockTable())
        
        # Row size should be positive
        assert table_stats.avg_row_size > 0