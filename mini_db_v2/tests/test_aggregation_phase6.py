# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_aggregation_phase6
# Intent: Comprehensive tests for Phase 6 Aggregation functionality.
# Dependencies: pytest, mini_db_v2.executor.aggregates, mini_db_v2.ast.nodes
# END_MODULE_CONTRACT

"""
Phase 6 Aggregation Tests

Coverage:
1. Aggregate Functions: COUNT(*), COUNT(col), SUM, AVG, MIN, MAX
2. GROUP BY: single column, multiple columns, NULL in keys
3. HAVING: filter after GROUP BY, aggregates in HAVING
4. DISTINCT: remove duplicates, with ORDER BY, with LIMIT
5. NULL Handling: COUNT(*) vs COUNT(col), SUM/AVG/MIN/MAX ignore NULL
"""

import pytest
from typing import Any, Optional
from mini_db_v2.executor.aggregates import (
    AggregateFunctions,
    AggregateExecutor,
    AggregateResult,
    GroupResult,
    HashAggregator,
    DistinctExecutor
)
from mini_db_v2.ast.nodes import (
    AggregateNode,
    AggregateType,
    ColumnRef,
    SelectColumn,
    BinaryOpNode,
    BinaryOperator,
    LiteralNode
)


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def sample_rows():
    """Sample rows for testing aggregates."""
    return [
        {"id": 1, "name": "Alice", "age": 25, "salary": 50000},
        {"id": 2, "name": "Bob", "age": 30, "salary": 60000},
        {"id": 3, "name": "Charlie", "age": 25, "salary": 55000},
        {"id": 4, "name": "Diana", "age": None, "salary": 70000},
    ]


@pytest.fixture
def rows_with_nulls():
    """Rows with NULL values for NULL handling tests."""
    return [
        {"id": 1, "value": 10},
        {"id": 2, "value": 20},
        {"id": 3, "value": None},
        {"id": 4, "value": 30},
        {"id": 5, "value": None},
    ]


@pytest.fixture
def rows_for_grouping():
    """Rows for GROUP BY tests."""
    return [
        {"dept": "IT", "salary": 50000},
        {"dept": "IT", "salary": 60000},
        {"dept": "HR", "salary": 45000},
        {"dept": "HR", "salary": 55000},
        {"dept": None, "salary": 40000},
    ]


@pytest.fixture
def rows_for_distinct():
    """Rows with duplicates for DISTINCT tests."""
    return [
        {"name": "Alice", "age": 25},
        {"name": "Bob", "age": 30},
        {"name": "Alice", "age": 25},
        {"name": "Charlie", "age": 25},
        {"name": "Bob", "age": 30},
    ]


@pytest.fixture
def simple_evaluator():
    """Simple evaluator function for testing."""
    def evaluator(expr, row):
        if isinstance(expr, ColumnRef):
            return row.get(expr.column_name)
        elif isinstance(expr, LiteralNode):
            return expr.value
        return None
    return evaluator


# =============================================================================
# Test AggregateFunctions
# =============================================================================

class TestAggregateFunctionsCount:
    """Tests for COUNT aggregate functions."""
    
    def test_count_star_returns_all_rows(self, sample_rows):
        """COUNT(*) returns total number of rows including NULL."""
        result = AggregateFunctions.count_star(sample_rows)
        assert result == 4
    
    def test_count_star_empty_list_returns_zero(self):
        """COUNT(*) on empty list returns 0."""
        result = AggregateFunctions.count_star([])
        assert result == 0
    
    def test_count_column_counts_non_null(self, rows_with_nulls):
        """COUNT(col) counts only non-NULL values."""
        values = [row["value"] for row in rows_with_nulls]
        result = AggregateFunctions.count_column(values)
        assert result == 3  # 10, 20, 30 - excluding two NULLs
    
    def test_count_column_all_null_returns_zero(self):
        """COUNT(col) with all NULL values returns 0."""
        values = [None, None, None]
        result = AggregateFunctions.count_column(values)
        assert result == 0
    
    def test_count_column_empty_list_returns_zero(self):
        """COUNT(col) on empty list returns 0."""
        result = AggregateFunctions.count_column([])
        assert result == 0


class TestAggregateFunctionsSum:
    """Tests for SUM aggregate function."""
    
    def test_sum_returns_total(self, rows_with_nulls):
        """SUM returns sum of non-NULL values."""
        values = [row["value"] for row in rows_with_nulls]
        result = AggregateFunctions.sum(values)
        assert result == 60  # 10 + 20 + 30
    
    def test_sum_ignores_null(self):
        """SUM ignores NULL values."""
        values = [10, None, 20, None, 30]
        result = AggregateFunctions.sum(values)
        assert result == 60
    
    def test_sum_all_null_returns_none(self):
        """SUM with all NULL values returns None."""
        values = [None, None, None]
        result = AggregateFunctions.sum(values)
        assert result is None
    
    def test_sum_empty_list_returns_none(self):
        """SUM on empty list returns None."""
        result = AggregateFunctions.sum([])
        assert result is None
    
    def test_sum_negative_values(self):
        """SUM works with negative values."""
        values = [-10, 20, -5, 15]
        result = AggregateFunctions.sum(values)
        assert result == 20


class TestAggregateFunctionsAvg:
    """Tests for AVG aggregate function."""
    
    def test_avg_returns_mean(self, rows_with_nulls):
        """AVG returns mean of non-NULL values."""
        values = [row["value"] for row in rows_with_nulls]
        result = AggregateFunctions.avg(values)
        assert result == 20.0  # (10 + 20 + 30) / 3
    
    def test_avg_ignores_null(self):
        """AVG ignores NULL values."""
        values = [10, None, 20, None, 30]
        result = AggregateFunctions.avg(values)
        assert result == 20.0
    
    def test_avg_all_null_returns_none(self):
        """AVG with all NULL values returns None."""
        values = [None, None, None]
        result = AggregateFunctions.avg(values)
        assert result is None
    
    def test_avg_empty_list_returns_none(self):
        """AVG on empty list returns None."""
        result = AggregateFunctions.avg([])
        assert result is None
    
    def test_avg_single_value(self):
        """AVG with single value returns that value."""
        values = [42]
        result = AggregateFunctions.avg(values)
        assert result == 42.0


class TestAggregateFunctionsMin:
    """Tests for MIN aggregate function."""
    
    def test_min_returns_smallest(self, rows_with_nulls):
        """MIN returns smallest non-NULL value."""
        values = [row["value"] for row in rows_with_nulls]
        result = AggregateFunctions.min(values)
        assert result == 10
    
    def test_min_ignores_null(self):
        """MIN ignores NULL values."""
        values = [None, 20, None, 10, 30]
        result = AggregateFunctions.min(values)
        assert result == 10
    
    def test_min_all_null_returns_none(self):
        """MIN with all NULL values returns None."""
        values = [None, None, None]
        result = AggregateFunctions.min(values)
        assert result is None
    
    def test_min_empty_list_returns_none(self):
        """MIN on empty list returns None."""
        result = AggregateFunctions.min([])
        assert result is None
    
    def test_min_with_negative(self):
        """MIN works with negative values."""
        values = [-10, 5, -20, 15]
        result = AggregateFunctions.min(values)
        assert result == -20


class TestAggregateFunctionsMax:
    """Tests for MAX aggregate function."""
    
    def test_max_returns_largest(self, rows_with_nulls):
        """MAX returns largest non-NULL value."""
        values = [row["value"] for row in rows_with_nulls]
        result = AggregateFunctions.max(values)
        assert result == 30
    
    def test_max_ignores_null(self):
        """MAX ignores NULL values."""
        values = [None, 20, None, 10, 30]
        result = AggregateFunctions.max(values)
        assert result == 30
    
    def test_max_all_null_returns_none(self):
        """MAX with all NULL values returns None."""
        values = [None, None, None]
        result = AggregateFunctions.max(values)
        assert result is None
    
    def test_max_empty_list_returns_none(self):
        """MAX on empty list returns None."""
        result = AggregateFunctions.max([])
        assert result is None
    
    def test_max_with_negative(self):
        """MAX works with negative values."""
        values = [-10, 5, -20, 15]
        result = AggregateFunctions.max(values)
        assert result == 15


# =============================================================================
# Test HashAggregator
# =============================================================================

class TestHashAggregatorGroupBy:
    """Tests for GROUP BY functionality."""
    
    def test_group_by_single_column(self, rows_for_grouping, simple_evaluator):
        """GROUP BY single column groups rows correctly."""
        aggregator = HashAggregator(simple_evaluator)
        group_by = [ColumnRef("dept")]
        aggregates = [("count", AggregateNode(AggregateType.COUNT))]
        
        result = aggregator.aggregate(rows_for_grouping, group_by, aggregates)
        
        # Should have 3 groups: IT, HR, None
        assert len(result) == 3
        
        # Check IT group
        it_key = ("IT",)
        assert it_key in result
        assert len(result[it_key].rows) == 2
        
        # Check HR group
        hr_key = ("HR",)
        assert hr_key in result
        assert len(result[hr_key].rows) == 2
        
        # Check NULL group
        null_key = (None,)
        assert null_key in result
        assert len(result[null_key].rows) == 1
    
    def test_group_by_multiple_columns(self, simple_evaluator):
        """GROUP BY multiple columns groups correctly."""
        rows = [
            {"dept": "IT", "role": "dev", "salary": 50000},
            {"dept": "IT", "role": "dev", "salary": 55000},
            {"dept": "IT", "role": "qa", "salary": 45000},
            {"dept": "HR", "role": "dev", "salary": 40000},
        ]
        
        aggregator = HashAggregator(simple_evaluator)
        group_by = [ColumnRef("dept"), ColumnRef("role")]
        aggregates = [("count", AggregateNode(AggregateType.COUNT))]
        
        result = aggregator.aggregate(rows, group_by, aggregates)
        
        # Should have 3 groups
        assert len(result) == 3
        
        # Check (IT, dev) group
        assert ("IT", "dev") in result
        assert len(result[("IT", "dev")].rows) == 2
        
        # Check (IT, qa) group
        assert ("IT", "qa") in result
        assert len(result[("IT", "qa")].rows) == 1
    
    def test_group_by_with_null_key(self, rows_for_grouping, simple_evaluator):
        """GROUP BY handles NULL in grouping keys."""
        aggregator = HashAggregator(simple_evaluator)
        group_by = [ColumnRef("dept")]
        aggregates = [("count", AggregateNode(AggregateType.COUNT))]
        
        result = aggregator.aggregate(rows_for_grouping, group_by, aggregates)
        
        # NULL should be a separate group
        null_key = (None,)
        assert null_key in result
        assert len(result[null_key].rows) == 1
    
    def test_implicit_aggregation_no_group_by(self, sample_rows, simple_evaluator):
        """Aggregation without GROUP BY treats all rows as one group."""
        aggregator = HashAggregator(simple_evaluator)
        group_by = []
        aggregates = [("count", AggregateNode(AggregateType.COUNT))]
        
        result = aggregator.aggregate(sample_rows, group_by, aggregates)
        
        # Should have one group with empty key
        assert len(result) == 1
        assert () in result
        assert len(result[()].rows) == 4


class TestHashAggregatorAggregates:
    """Tests for aggregate computation in groups."""
    
    def test_count_star_in_group(self, rows_for_grouping, simple_evaluator):
        """COUNT(*) counts all rows in group."""
        aggregator = HashAggregator(simple_evaluator)
        group_by = [ColumnRef("dept")]
        aggregates = [("cnt", AggregateNode(AggregateType.COUNT))]
        
        result = aggregator.aggregate(rows_for_grouping, group_by, aggregates)
        
        # IT group has 2 rows
        assert result[("IT",)].aggregates["cnt"].value == 2
        # HR group has 2 rows
        assert result[("HR",)].aggregates["cnt"].value == 2
    
    def test_sum_in_group(self, rows_for_grouping, simple_evaluator):
        """SUM computes sum for each group."""
        aggregator = HashAggregator(simple_evaluator)
        group_by = [ColumnRef("dept")]
        aggregates = [
            ("total", AggregateNode(AggregateType.SUM, arg=ColumnRef("salary")))
        ]
        
        result = aggregator.aggregate(rows_for_grouping, group_by, aggregates)
        
        # IT: 50000 + 60000 = 110000
        assert result[("IT",)].aggregates["total"].value == 110000
        # HR: 45000 + 55000 = 100000
        assert result[("HR",)].aggregates["total"].value == 100000
    
    def test_avg_in_group(self, rows_for_grouping, simple_evaluator):
        """AVG computes average for each group."""
        aggregator = HashAggregator(simple_evaluator)
        group_by = [ColumnRef("dept")]
        aggregates = [
            ("avg_sal", AggregateNode(AggregateType.AVG, arg=ColumnRef("salary")))
        ]
        
        result = aggregator.aggregate(rows_for_grouping, group_by, aggregates)
        
        # IT: (50000 + 60000) / 2 = 55000
        assert result[("IT",)].aggregates["avg_sal"].value == 55000.0
        # HR: (45000 + 55000) / 2 = 50000
        assert result[("HR",)].aggregates["avg_sal"].value == 50000.0
    
    def test_min_max_in_group(self, rows_for_grouping, simple_evaluator):
        """MIN/MAX compute min/max for each group."""
        aggregator = HashAggregator(simple_evaluator)
        group_by = [ColumnRef("dept")]
        aggregates = [
            ("min_sal", AggregateNode(AggregateType.MIN, arg=ColumnRef("salary"))),
            ("max_sal", AggregateNode(AggregateType.MAX, arg=ColumnRef("salary")))
        ]
        
        result = aggregator.aggregate(rows_for_grouping, group_by, aggregates)
        
        # IT: min=50000, max=60000
        assert result[("IT",)].aggregates["min_sal"].value == 50000
        assert result[("IT",)].aggregates["max_sal"].value == 60000
        # HR: min=45000, max=55000
        assert result[("HR",)].aggregates["min_sal"].value == 45000
        assert result[("HR",)].aggregates["max_sal"].value == 55000


class TestHashAggregatorResultRows:
    """Tests for get_result_rows method."""
    
    def test_get_result_rows_returns_correct_format(self, rows_for_grouping, simple_evaluator):
        """get_result_rows returns list of dicts with correct columns."""
        aggregator = HashAggregator(simple_evaluator)
        group_by = [ColumnRef("dept")]
        aggregates = [
            ("cnt", AggregateNode(AggregateType.COUNT)),
            ("total", AggregateNode(AggregateType.SUM, arg=ColumnRef("salary")))
        ]
        
        aggregator.aggregate(rows_for_grouping, group_by, aggregates)
        result_rows = aggregator.get_result_rows(group_by, aggregates)
        
        assert len(result_rows) == 3
        
        # Find IT row
        it_row = next(r for r in result_rows if r.get("dept") == "IT")
        assert it_row["cnt"] == 2
        assert it_row["total"] == 110000
    
    def test_get_result_rows_includes_null_group(self, rows_for_grouping, simple_evaluator):
        """get_result_rows includes NULL group in results."""
        aggregator = HashAggregator(simple_evaluator)
        group_by = [ColumnRef("dept")]
        aggregates = [("cnt", AggregateNode(AggregateType.COUNT))]
        
        aggregator.aggregate(rows_for_grouping, group_by, aggregates)
        result_rows = aggregator.get_result_rows(group_by, aggregates)
        
        # Should include NULL group
        null_row = next((r for r in result_rows if r.get("dept") is None), None)
        assert null_row is not None
        assert null_row["cnt"] == 1


# =============================================================================
# Test AggregateExecutor
# =============================================================================

class TestAggregateExecutorHasAggregates:
    """Tests for has_aggregates method."""
    
    def test_has_aggregates_returns_true_for_count(self, simple_evaluator):
        """has_aggregates returns True for COUNT."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=AggregateNode(AggregateType.COUNT))
        ]
        assert executor.has_aggregates(columns) is True
    
    def test_has_aggregates_returns_true_for_sum(self, simple_evaluator):
        """has_aggregates returns True for SUM."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=AggregateNode(AggregateType.SUM, arg=ColumnRef("value")))
        ]
        assert executor.has_aggregates(columns) is True
    
    def test_has_aggregates_returns_false_for_column(self, simple_evaluator):
        """has_aggregates returns False for regular column."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=ColumnRef("name"))
        ]
        assert executor.has_aggregates(columns) is False
    
    def test_has_aggregates_returns_true_for_mixed(self, simple_evaluator):
        """has_aggregates returns True if any column has aggregate."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=ColumnRef("name")),
            SelectColumn(expression=AggregateNode(AggregateType.COUNT))
        ]
        assert executor.has_aggregates(columns) is True


class TestAggregateExecutorImplicitAggregation:
    """Tests for implicit aggregation (no GROUP BY)."""
    
    def test_implicit_count_star(self, sample_rows, simple_evaluator):
        """Implicit COUNT(*) returns single row with count."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=AggregateNode(AggregateType.COUNT), alias="cnt")
        ]
        
        result = executor.execute(sample_rows, columns, group_by=[])
        
        assert len(result) == 1
        assert result[0]["cnt"] == 4
    
    def test_implicit_sum(self, sample_rows, simple_evaluator):
        """Implicit SUM returns single row with sum."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(
                expression=AggregateNode(AggregateType.SUM, arg=ColumnRef("salary")),
                alias="total"
            )
        ]
        
        result = executor.execute(sample_rows, columns, group_by=[])
        
        assert len(result) == 1
        assert result[0]["total"] == 235000  # 50000 + 60000 + 55000 + 70000
    
    def test_implicit_avg(self, sample_rows, simple_evaluator):
        """Implicit AVG returns single row with average."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(
                expression=AggregateNode(AggregateType.AVG, arg=ColumnRef("age")),
                alias="avg_age"
            )
        ]
        
        result = executor.execute(sample_rows, columns, group_by=[])
        
        assert len(result) == 1
        # (25 + 30 + 25) / 3 = 26.67 (NULL excluded)
        assert abs(result[0]["avg_age"] - 26.666666) < 0.01
    
    def test_implicit_multiple_aggregates(self, sample_rows, simple_evaluator):
        """Implicit aggregation with multiple aggregates."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=AggregateNode(AggregateType.COUNT), alias="cnt"),
            SelectColumn(
                expression=AggregateNode(AggregateType.SUM, arg=ColumnRef("salary")),
                alias="total"
            ),
            SelectColumn(
                expression=AggregateNode(AggregateType.AVG, arg=ColumnRef("salary")),
                alias="avg_sal"
            )
        ]
        
        result = executor.execute(sample_rows, columns, group_by=[])
        
        assert len(result) == 1
        assert result[0]["cnt"] == 4
        assert result[0]["total"] == 235000
        assert result[0]["avg_sal"] == 58750.0


class TestAggregateExecutorGroupBy:
    """Tests for GROUP BY aggregation."""
    
    def test_group_by_with_count(self, rows_for_grouping, simple_evaluator):
        """GROUP BY with COUNT returns correct groups."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=ColumnRef("dept")),
            SelectColumn(expression=AggregateNode(AggregateType.COUNT), alias="cnt")
        ]
        group_by = [ColumnRef("dept")]
        
        result = executor.execute(rows_for_grouping, columns, group_by)
        
        assert len(result) == 3
        
        # Find IT group
        it_row = next(r for r in result if r.get("dept") == "IT")
        assert it_row["cnt"] == 2
    
    def test_group_by_with_sum_avg(self, rows_for_grouping, simple_evaluator):
        """GROUP BY with SUM and AVG."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=ColumnRef("dept")),
            SelectColumn(
                expression=AggregateNode(AggregateType.SUM, arg=ColumnRef("salary")),
                alias="total"
            ),
            SelectColumn(
                expression=AggregateNode(AggregateType.AVG, arg=ColumnRef("salary")),
                alias="avg"
            )
        ]
        group_by = [ColumnRef("dept")]
        
        result = executor.execute(rows_for_grouping, columns, group_by)
        
        # IT group
        it_row = next(r for r in result if r.get("dept") == "IT")
        assert it_row["total"] == 110000
        assert it_row["avg"] == 55000.0
    
    def test_group_by_null_key_included(self, rows_for_grouping, simple_evaluator):
        """GROUP BY includes NULL as a group."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=ColumnRef("dept")),
            SelectColumn(expression=AggregateNode(AggregateType.COUNT), alias="cnt")
        ]
        group_by = [ColumnRef("dept")]
        
        result = executor.execute(rows_for_grouping, columns, group_by)
        
        # NULL group should be present
        null_row = next((r for r in result if r.get("dept") is None), None)
        assert null_row is not None
        assert null_row["cnt"] == 1


class TestAggregateExecutorHaving:
    """Tests for HAVING clause."""
    
    def test_having_filters_groups(self, rows_for_grouping, simple_evaluator):
        """HAVING filters groups after aggregation."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=ColumnRef("dept")),
            SelectColumn(expression=AggregateNode(AggregateType.COUNT), alias="cnt")
        ]
        group_by = [ColumnRef("dept")]
        
        # HAVING COUNT(*) > 1
        having = BinaryOpNode(
            left=AggregateNode(AggregateType.COUNT),
            operator=BinaryOperator.GT,
            right=LiteralNode(1)
        )
        
        result = executor.execute(rows_for_grouping, columns, group_by, having)
        
        # Only IT and HR have count > 1
        assert len(result) == 2
        depts = {r["dept"] for r in result}
        assert depts == {"IT", "HR"}
    
    def test_having_with_sum(self, rows_for_grouping, simple_evaluator):
        """HAVING with SUM aggregate."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=ColumnRef("dept")),
            SelectColumn(
                expression=AggregateNode(AggregateType.SUM, arg=ColumnRef("salary")),
                alias="total"
            )
        ]
        group_by = [ColumnRef("dept")]
        
        # HAVING SUM(salary) > 100000
        having = BinaryOpNode(
            left=AggregateNode(AggregateType.SUM, arg=ColumnRef("salary")),
            operator=BinaryOperator.GT,
            right=LiteralNode(100000)
        )
        
        result = executor.execute(rows_for_grouping, columns, group_by, having)
        
        # Only IT has total > 100000 (110000)
        assert len(result) == 1
        assert result[0]["dept"] == "IT"
    
    def test_having_with_and(self, rows_for_grouping, simple_evaluator):
        """HAVING with AND condition."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=ColumnRef("dept")),
            SelectColumn(expression=AggregateNode(AggregateType.COUNT), alias="cnt"),
            SelectColumn(
                expression=AggregateNode(AggregateType.SUM, arg=ColumnRef("salary")),
                alias="total"
            )
        ]
        group_by = [ColumnRef("dept")]
        
        # HAVING COUNT(*) >= 2 AND SUM(salary) < 120000
        having = BinaryOpNode(
            left=BinaryOpNode(
                left=AggregateNode(AggregateType.COUNT),
                operator=BinaryOperator.GE,
                right=LiteralNode(2)
            ),
            operator=BinaryOperator.AND,
            right=BinaryOpNode(
                left=AggregateNode(AggregateType.SUM, arg=ColumnRef("salary")),
                operator=BinaryOperator.LT,
                right=LiteralNode(120000)
            )
        )
        
        result = executor.execute(rows_for_grouping, columns, group_by, having)
        
        # HR: count=2, total=100000 (matches)
        # IT: count=2, total=110000 (matches)
        assert len(result) == 2


# =============================================================================
# Test DistinctExecutor
# =============================================================================

class TestDistinctExecutor:
    """Tests for DISTINCT functionality."""
    
    def test_distinct_removes_duplicates(self, rows_for_distinct):
        """DISTINCT removes duplicate rows."""
        result = DistinctExecutor.apply_distinct(rows_for_distinct)
        
        # Original: 5 rows, after distinct: 3 unique
        assert len(result) == 3
        
        # Check unique rows present
        names = {r["name"] for r in result}
        assert names == {"Alice", "Bob", "Charlie"}
    
    def test_distinct_preserves_first_occurrence(self, rows_for_distinct):
        """DISTINCT keeps first occurrence of duplicate."""
        result = DistinctExecutor.apply_distinct(rows_for_distinct)
        
        # Find Alice row
        alice_row = next(r for r in result if r["name"] == "Alice")
        assert alice_row["age"] == 25
    
    def test_distinct_empty_list(self):
        """DISTINCT on empty list returns empty list."""
        result = DistinctExecutor.apply_distinct([])
        assert result == []
    
    def test_distinct_all_unique(self):
        """DISTINCT on all unique rows returns same count."""
        rows = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
        result = DistinctExecutor.apply_distinct(rows)
        assert len(result) == 3
    
    def test_distinct_all_same(self):
        """DISTINCT on all same rows returns single row."""
        rows = [
            {"name": "Alice", "age": 25},
            {"name": "Alice", "age": 25},
            {"name": "Alice", "age": 25},
        ]
        result = DistinctExecutor.apply_distinct(rows)
        assert len(result) == 1
    
    def test_distinct_with_null_values(self):
        """DISTINCT handles NULL values correctly."""
        rows = [
            {"id": 1, "value": None},
            {"id": 2, "value": None},
            {"id": 3, "value": 10},
        ]
        result = DistinctExecutor.apply_distinct(rows)
        # All rows are unique because id differs
        assert len(result) == 3


# =============================================================================
# Test NULL Handling
# =============================================================================

class TestNullHandling:
    """Comprehensive tests for NULL handling in aggregates."""
    
    def test_count_star_vs_count_column(self, rows_with_nulls, simple_evaluator):
        """COUNT(*) includes NULL, COUNT(col) excludes NULL."""
        executor = AggregateExecutor(simple_evaluator)
        
        # COUNT(*)
        columns_star = [
            SelectColumn(expression=AggregateNode(AggregateType.COUNT), alias="cnt")
        ]
        result_star = executor.execute(rows_with_nulls, columns_star, group_by=[])
        
        # COUNT(value)
        columns_col = [
            SelectColumn(
                expression=AggregateNode(AggregateType.COUNT, arg=ColumnRef("value")),
                alias="cnt"
            )
        ]
        result_col = executor.execute(rows_with_nulls, columns_col, group_by=[])
        
        assert result_star[0]["cnt"] == 5  # All rows
        assert result_col[0]["cnt"] == 3   # Non-NULL only
    
    def test_sum_ignores_null(self, rows_with_nulls, simple_evaluator):
        """SUM ignores NULL values."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(
                expression=AggregateNode(AggregateType.SUM, arg=ColumnRef("value")),
                alias="total"
            )
        ]
        
        result = executor.execute(rows_with_nulls, columns, group_by=[])
        
        # 10 + 20 + 30 = 60 (NULLs ignored)
        assert result[0]["total"] == 60
    
    def test_avg_ignores_null(self, rows_with_nulls, simple_evaluator):
        """AVG ignores NULL values."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(
                expression=AggregateNode(AggregateType.AVG, arg=ColumnRef("value")),
                alias="avg"
            )
        ]
        
        result = executor.execute(rows_with_nulls, columns, group_by=[])
        
        # (10 + 20 + 30) / 3 = 20
        assert result[0]["avg"] == 20.0
    
    def test_min_max_ignores_null(self, rows_with_nulls, simple_evaluator):
        """MIN/MAX ignore NULL values."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(
                expression=AggregateNode(AggregateType.MIN, arg=ColumnRef("value")),
                alias="min_val"
            ),
            SelectColumn(
                expression=AggregateNode(AggregateType.MAX, arg=ColumnRef("value")),
                alias="max_val"
            )
        ]
        
        result = executor.execute(rows_with_nulls, columns, group_by=[])
        
        assert result[0]["min_val"] == 10
        assert result[0]["max_val"] == 30
    
    def test_all_null_sum_returns_none(self, simple_evaluator):
        """SUM with all NULL values returns None."""
        rows = [
            {"id": 1, "value": None},
            {"id": 2, "value": None},
        ]
        
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(
                expression=AggregateNode(AggregateType.SUM, arg=ColumnRef("value")),
                alias="total"
            )
        ]
        
        result = executor.execute(rows, columns, group_by=[])
        
        assert result[0]["total"] is None
    
    def test_all_null_avg_returns_none(self, simple_evaluator):
        """AVG with all NULL values returns None."""
        rows = [
            {"id": 1, "value": None},
            {"id": 2, "value": None},
        ]
        
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(
                expression=AggregateNode(AggregateType.AVG, arg=ColumnRef("value")),
                alias="avg"
            )
        ]
        
        result = executor.execute(rows, columns, group_by=[])
        
        assert result[0]["avg"] is None
    
    def test_null_in_group_by_key(self, simple_evaluator):
        """NULL values form separate group in GROUP BY."""
        rows = [
            {"category": "A", "value": 10},
            {"category": "B", "value": 20},
            {"category": None, "value": 30},
            {"category": None, "value": 40},
        ]
        
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=ColumnRef("category")),
            SelectColumn(
                expression=AggregateNode(AggregateType.SUM, arg=ColumnRef("value")),
                alias="total"
            )
        ]
        group_by = [ColumnRef("category")]
        
        result = executor.execute(rows, columns, group_by)
        
        # Should have 3 groups
        assert len(result) == 3
        
        # NULL group
        null_row = next((r for r in result if r.get("category") is None), None)
        assert null_row is not None
        assert null_row["total"] == 70  # 30 + 40


# =============================================================================
# Adversarial Tests
# =============================================================================

class TestAdversarialCases:
    """Edge cases and adversarial tests."""
    
    def test_empty_table_aggregation(self, simple_evaluator):
        """Aggregation on empty table returns appropriate results."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=AggregateNode(AggregateType.COUNT), alias="cnt"),
            SelectColumn(
                expression=AggregateNode(AggregateType.SUM, arg=ColumnRef("value")),
                alias="total"
            )
        ]
        
        result = executor.execute([], columns, group_by=[])
        
        assert len(result) == 1
        assert result[0]["cnt"] == 0
        assert result[0]["total"] is None
    
    def test_single_row_aggregation(self, simple_evaluator):
        """Aggregation on single row works correctly."""
        rows = [{"id": 1, "value": 42}]
        
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=AggregateNode(AggregateType.COUNT), alias="cnt"),
            SelectColumn(
                expression=AggregateNode(AggregateType.SUM, arg=ColumnRef("value")),
                alias="total"
            ),
            SelectColumn(
                expression=AggregateNode(AggregateType.AVG, arg=ColumnRef("value")),
                alias="avg"
            )
        ]
        
        result = executor.execute(rows, columns, group_by=[])
        
        assert result[0]["cnt"] == 1
        assert result[0]["total"] == 42
        assert result[0]["avg"] == 42.0
    
    def test_large_numbers_sum(self, simple_evaluator):
        """SUM handles large numbers correctly."""
        rows = [
            {"value": 1000000000},
            {"value": 2000000000},
            {"value": 3000000000},
        ]
        
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(
                expression=AggregateNode(AggregateType.SUM, arg=ColumnRef("value")),
                alias="total"
            )
        ]
        
        result = executor.execute(rows, columns, group_by=[])
        
        assert result[0]["total"] == 6000000000
    
    def test_negative_numbers_aggregation(self, simple_evaluator):
        """Aggregates work with negative numbers."""
        rows = [
            {"value": -10},
            {"value": 20},
            {"value": -5},
        ]
        
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(
                expression=AggregateNode(AggregateType.SUM, arg=ColumnRef("value")),
                alias="total"
            ),
            SelectColumn(
                expression=AggregateNode(AggregateType.MIN, arg=ColumnRef("value")),
                alias="min_val"
            ),
            SelectColumn(
                expression=AggregateNode(AggregateType.MAX, arg=ColumnRef("value")),
                alias="max_val"
            )
        ]
        
        result = executor.execute(rows, columns, group_by=[])
        
        assert result[0]["total"] == 5  # -10 + 20 - 5
        assert result[0]["min_val"] == -10
        assert result[0]["max_val"] == 20
    
    def test_floating_point_avg(self, simple_evaluator):
        """AVG handles floating point correctly."""
        rows = [
            {"value": 1.5},
            {"value": 2.5},
            {"value": 3.0},
        ]
        
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(
                expression=AggregateNode(AggregateType.AVG, arg=ColumnRef("value")),
                alias="avg"
            )
        ]
        
        result = executor.execute(rows, columns, group_by=[])
        
        assert abs(result[0]["avg"] - 2.333333) < 0.01
    
    def test_many_groups(self, simple_evaluator):
        """GROUP BY handles many groups efficiently."""
        rows = [{"id": i, "group": i % 100} for i in range(1000)]
        
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=ColumnRef("group")),
            SelectColumn(expression=AggregateNode(AggregateType.COUNT), alias="cnt")
        ]
        group_by = [ColumnRef("group")]
        
        result = executor.execute(rows, columns, group_by)
        
        # 100 groups, each with 10 rows
        assert len(result) == 100
        for row in result:
            assert row["cnt"] == 10
    
    def test_distinct_large_dataset(self):
        """DISTINCT handles large dataset efficiently."""
        rows = [{"id": i % 100, "value": i} for i in range(1000)]
        
        result = DistinctExecutor.apply_distinct(rows)
        
        # All rows are unique due to different 'value'
        assert len(result) == 1000


# =============================================================================
# Integration Tests
# =============================================================================

class TestAggregationIntegration:
    """Integration tests combining multiple features."""
    
    def test_group_by_with_having_and_multiple_aggregates(self, simple_evaluator):
        """GROUP BY with HAVING and multiple aggregates."""
        rows = [
            {"dept": "IT", "salary": 50000, "bonus": 5000},
            {"dept": "IT", "salary": 60000, "bonus": 6000},
            {"dept": "HR", "salary": 45000, "bonus": 4500},
            {"dept": "HR", "salary": 55000, "bonus": 5500},
        ]
        
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=ColumnRef("dept")),
            SelectColumn(expression=AggregateNode(AggregateType.COUNT), alias="cnt"),
            SelectColumn(
                expression=AggregateNode(AggregateType.SUM, arg=ColumnRef("salary")),
                alias="total_sal"
            ),
            SelectColumn(
                expression=AggregateNode(AggregateType.AVG, arg=ColumnRef("bonus")),
                alias="avg_bonus"
            )
        ]
        group_by = [ColumnRef("dept")]
        
        # HAVING COUNT(*) >= 2
        having = BinaryOpNode(
            left=AggregateNode(AggregateType.COUNT),
            operator=BinaryOperator.GE,
            right=LiteralNode(2)
        )
        
        result = executor.execute(rows, columns, group_by, having)
        
        assert len(result) == 2
        
        it_row = next(r for r in result if r["dept"] == "IT")
        assert it_row["cnt"] == 2
        assert it_row["total_sal"] == 110000
        assert it_row["avg_bonus"] == 5500.0
    
    def test_implicit_aggregation_all_types(self, sample_rows, simple_evaluator):
        """Implicit aggregation with all aggregate types."""
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=AggregateNode(AggregateType.COUNT), alias="cnt_star"),
            SelectColumn(
                expression=AggregateNode(AggregateType.COUNT, arg=ColumnRef("age")),
                alias="cnt_age"
            ),
            SelectColumn(
                expression=AggregateNode(AggregateType.SUM, arg=ColumnRef("salary")),
                alias="total_sal"
            ),
            SelectColumn(
                expression=AggregateNode(AggregateType.AVG, arg=ColumnRef("age")),
                alias="avg_age"
            ),
            SelectColumn(
                expression=AggregateNode(AggregateType.MIN, arg=ColumnRef("salary")),
                alias="min_sal"
            ),
            SelectColumn(
                expression=AggregateNode(AggregateType.MAX, arg=ColumnRef("salary")),
                alias="max_sal"
            )
        ]
        
        result = executor.execute(sample_rows, columns, group_by=[])
        
        assert len(result) == 1
        row = result[0]
        
        assert row["cnt_star"] == 4
        assert row["cnt_age"] == 3  # One NULL
        assert row["total_sal"] == 235000
        assert abs(row["avg_age"] - 26.666666) < 0.01
        assert row["min_sal"] == 50000
        assert row["max_sal"] == 70000


# =============================================================================
# Performance Tests
# =============================================================================

class TestAggregationPerformance:
    """Performance tests for aggregation."""
    
    def test_large_group_by_performance(self, simple_evaluator):
        """GROUP BY on large dataset completes in reasonable time."""
        import time
        
        rows = [{"id": i, "group": i % 1000, "value": i} for i in range(10000)]
        
        executor = AggregateExecutor(simple_evaluator)
        columns = [
            SelectColumn(expression=ColumnRef("group")),
            SelectColumn(
                expression=AggregateNode(AggregateType.SUM, arg=ColumnRef("value")),
                alias="total"
            )
        ]
        group_by = [ColumnRef("group")]
        
        start = time.time()
        result = executor.execute(rows, columns, group_by)
        elapsed = time.time() - start
        
        assert len(result) == 1000
        assert elapsed < 5.0  # Should complete in under 5 seconds
    
    def test_large_distinct_performance(self):
        """DISTINCT on large dataset completes in reasonable time."""
        import time
        
        rows = [{"id": i, "group": i % 100} for i in range(10000)]
        
        start = time.time()
        result = DistinctExecutor.apply_distinct(rows)
        elapsed = time.time() - start
        
        # All rows unique due to different id
        assert len(result) == 10000
        assert elapsed < 5.0  # Should complete in under 5 seconds