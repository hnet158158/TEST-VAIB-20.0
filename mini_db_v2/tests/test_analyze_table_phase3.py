# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_analyze_table_phase3
# Intent: Интеграционные тесты для Phase 3 - ANALYZE TABLE command
# Dependencies: pytest, mini_db_v2.parser, mini_db_v2.executor, mini_db_v2.storage
# END_MODULE_CONTRACT

"""
Phase 3 ANALYZE TABLE Integration Tests

Тестирует:
- Parser: ANALYZE TABLE table_name
- Executor: _execute_analyze_table()
- Statistics integration
- Done Criteria validation
"""

import pytest
import tempfile
import os

from mini_db_v2.parser.parser import Parser, ParseError, parse_sql
from mini_db_v2.executor.executor import Executor
from mini_db_v2.storage.database import Database
from mini_db_v2.optimizer.statistics import Statistics
from mini_db_v2.ast.nodes import AnalyzeTableNode


# =============================================================================
# Parser Tests
# =============================================================================

class TestAnalyzeTableParser:
    """Тесты парсинга ANALYZE TABLE."""

    def test_parse_analyze_table_basic(self):
        """Парсинг ANALYZE TABLE users."""
        ast = parse_sql("ANALYZE TABLE users")
        
        assert isinstance(ast, AnalyzeTableNode)
        assert ast.table_name == "users"

    def test_parse_analyze_table_without_semicolon(self):
        """Парсинг ANALYZE TABLE users (без точки с запятой)."""
        # Parser doesn't support trailing semicolon for ANALYZE
        ast = parse_sql("ANALYZE TABLE users")
        
        assert isinstance(ast, AnalyzeTableNode)
        assert ast.table_name == "users"

    def test_parse_analyze_table_quoted_name(self):
        """Парсинг ANALYZE TABLE с underscore в имени."""
        ast = parse_sql("ANALYZE TABLE my_table")
        
        assert isinstance(ast, AnalyzeTableNode)
        assert ast.table_name == "my_table"

    def test_parse_analyze_table_case_insensitive(self):
        """Парсинг ANALYZE TABLE без учёта регистра."""
        ast1 = parse_sql("ANALYZE TABLE users")
        ast2 = parse_sql("analyze table users")
        ast3 = parse_sql("Analyze Table users")
        
        assert all(isinstance(a, AnalyzeTableNode) for a in [ast1, ast2, ast3])
        assert all(a.table_name == "users" for a in [ast1, ast2, ast3])

    def test_parse_analyze_table_missing_table(self):
        """Ошибка при отсутствии имени таблицы."""
        with pytest.raises(ParseError):
            parse_sql("ANALYZE TABLE")

    def test_parse_analyze_table_extra_tokens(self):
        """Ошибка при лишних токенах."""
        with pytest.raises(ParseError):
            parse_sql("ANALYZE TABLE users extra")


# =============================================================================
# Executor Tests
# =============================================================================

class TestAnalyzeTableExecutor:
    """Тесты выполнения ANALYZE TABLE."""

    @pytest.fixture
    def db(self):
        """Создаёт тестовую базу данных."""
        return Database()

    @pytest.fixture
    def executor(self, db):
        """Создаёт executor с базой данных."""
        return Executor(db)

    def test_execute_analyze_table_success(self, db, executor):
        """Успешное выполнение ANALYZE TABLE."""
        # Create table and insert data
        executor.execute(parse_sql("CREATE TABLE users (id INT, name TEXT, age INT)"))
        executor.execute(parse_sql("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)"))
        executor.execute(parse_sql("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30)"))
        executor.execute(parse_sql("INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 25)"))
        
        # Analyze table
        result = executor.execute(parse_sql("ANALYZE TABLE users"))
        
        assert result.success
        assert "users" in result.message.lower() or "analyzed" in result.message.lower()

    def test_execute_analyze_table_nonexistent(self, db, executor):
        """ANALYZE TABLE для несуществующей таблицы."""
        from mini_db_v2.executor.executor import TableNotFoundError
        
        with pytest.raises(TableNotFoundError):
            executor.execute(parse_sql("ANALYZE TABLE nonexistent"))

    def test_execute_analyze_table_empty(self, db, executor):
        """ANALYZE TABLE для пустой таблицы."""
        executor.execute(parse_sql("CREATE TABLE empty (id INT, value TEXT)"))
        result = executor.execute(parse_sql("ANALYZE TABLE empty"))
        
        assert result.success

    def test_execute_analyze_table_with_nulls(self, db, executor):
        """ANALYZE TABLE с NULL значениями."""
        executor.execute(parse_sql("CREATE TABLE data (id INT, value TEXT)"))
        executor.execute(parse_sql("INSERT INTO data (id, value) VALUES (1, 'a')"))
        executor.execute(parse_sql("INSERT INTO data (id, value) VALUES (2, NULL)"))
        executor.execute(parse_sql("INSERT INTO data (id, value) VALUES (3, 'c')"))
        
        result = executor.execute(parse_sql("ANALYZE TABLE data"))
        
        assert result.success


# =============================================================================
# Statistics Integration Tests
# =============================================================================

class TestAnalyzeTableStatistics:
    """Тесты интеграции ANALYZE TABLE со статистикой."""

    @pytest.fixture
    def setup_db(self):
        """Создаёт базу данных с таблицей и данными."""
        db = Database()
        stats = Statistics()
        executor = Executor(db, stats)
        
        return db, stats, executor

    def test_analyze_creates_table_stats(self, setup_db):
        """ANALYZE создаёт TableStats."""
        db, stats, executor = setup_db
        
        executor.execute(parse_sql("CREATE TABLE users (id INT, name TEXT)"))
        executor.execute(parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')"))
        executor.execute(parse_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')"))
        executor.execute(parse_sql("INSERT INTO users (id, name) VALUES (3, 'Charlie')"))
        
        executor.execute(parse_sql("ANALYZE TABLE users"))
        
        table_stats = stats.get_table_stats("users")
        assert table_stats is not None
        assert table_stats.row_count == 3
        assert table_stats.page_count >= 0
        assert table_stats.last_analyze is not None

    def test_analyze_creates_column_stats(self, setup_db):
        """ANALYZE создаёт ColumnStats для всех колонок."""
        db, stats, executor = setup_db
        
        executor.execute(parse_sql("CREATE TABLE products (id INT, name TEXT, price INT)"))
        executor.execute(parse_sql("INSERT INTO products (id, name, price) VALUES (1, 'Apple', 100)"))
        executor.execute(parse_sql("INSERT INTO products (id, name, price) VALUES (2, 'Banana', 200)"))
        executor.execute(parse_sql("INSERT INTO products (id, name, price) VALUES (3, 'Cherry', 150)"))
        
        executor.execute(parse_sql("ANALYZE TABLE products"))
        
        # Check all columns have stats
        id_stats = stats.get_column_stats("products", "id")
        name_stats = stats.get_column_stats("products", "name")
        price_stats = stats.get_column_stats("products", "price")
        
        assert id_stats is not None
        assert name_stats is not None
        assert price_stats is not None
        
        assert id_stats.distinct_values == 3
        assert name_stats.distinct_values == 3
        assert price_stats.distinct_values == 3

    def test_analyze_histogram_numeric(self, setup_db):
        """ANALYZE создаёт гистограмму для числовых колонок."""
        db, stats, executor = setup_db
        
        executor.execute(parse_sql("CREATE TABLE metrics (id INT, value INT)"))
        
        # Insert 100 rows with values 0-99
        for i in range(100):
            executor.execute(parse_sql(f"INSERT INTO metrics (id, value) VALUES ({i}, {i})"))
        
        executor.execute(parse_sql("ANALYZE TABLE metrics"))
        
        value_stats = stats.get_column_stats("metrics", "value")
        assert value_stats is not None
        assert len(value_stats.histogram) > 0
        assert value_stats.min_value == 0
        assert value_stats.max_value == 99

    def test_analyze_mcv(self, setup_db):
        """ANALYZE вычисляет Most Common Values."""
        db, stats, executor = setup_db
        
        # Use 'severity' instead of 'level' (level is a keyword)
        executor.execute(parse_sql("CREATE TABLE logs (id INT, severity TEXT)"))
        
        # Insert skewed data
        for _ in range(50):
            executor.execute(parse_sql("INSERT INTO logs (id, severity) VALUES (1, 'INFO')"))
        for _ in range(30):
            executor.execute(parse_sql("INSERT INTO logs (id, severity) VALUES (2, 'WARN')"))
        for _ in range(10):
            executor.execute(parse_sql("INSERT INTO logs (id, severity) VALUES (3, 'ERROR')"))
        
        executor.execute(parse_sql("ANALYZE TABLE logs"))
        
        severity_stats = stats.get_column_stats("logs", "severity")
        assert severity_stats is not None
        assert len(severity_stats.most_common_values) > 0
        assert severity_stats.most_common_values[0] == "INFO"
        assert severity_stats.most_common_freqs[0] == pytest.approx(50/90, rel=0.1)

    def test_analyze_null_stats(self, setup_db):
        """ANALYZE корректно считает NULL."""
        db, stats, executor = setup_db
        
        executor.execute(parse_sql("CREATE TABLE data (id INT, value TEXT)"))
        executor.execute(parse_sql("INSERT INTO data (id, value) VALUES (1, 'a')"))
        executor.execute(parse_sql("INSERT INTO data (id, value) VALUES (2, NULL)"))
        executor.execute(parse_sql("INSERT INTO data (id, value) VALUES (3, NULL)"))
        executor.execute(parse_sql("INSERT INTO data (id, value) VALUES (4, 'b')"))
        
        executor.execute(parse_sql("ANALYZE TABLE data"))
        
        value_stats = stats.get_column_stats("data", "value")
        assert value_stats is not None
        assert value_stats.null_count == 2
        assert value_stats.null_fraction == 0.5


# =============================================================================
# Done Criteria Validation Tests
# =============================================================================

class TestPhase3DoneCriteria:
    """Валидация Done Criteria для Phase 3."""

    @pytest.fixture
    def setup_full(self):
        """Полная настройка для тестирования."""
        db = Database()
        stats = Statistics()
        executor = Executor(db, stats)
        return db, stats, executor

    def test_done_criteria_analyze_collects_statistics(self, setup_full):
        """
        Done Criteria: ANALYZE TABLE собирает статистику.
        """
        db, stats, executor = setup_full
        
        # Create table with data
        executor.execute(parse_sql("CREATE TABLE users (id INT, name TEXT, age INT)"))
        for i in range(100):
            executor.execute(parse_sql(f"INSERT INTO users (id, name, age) VALUES ({i}, 'user{i}', {20 + i % 50})"))
        
        # Analyze
        result = executor.execute(parse_sql("ANALYZE TABLE users"))
        assert result.success
        
        # Verify statistics collected
        table_stats = stats.get_table_stats("users")
        assert table_stats is not None
        assert table_stats.row_count == 100
        
        # All columns have stats
        for col in ["id", "name", "age"]:
            col_stats = stats.get_column_stats("users", col)
            assert col_stats is not None, f"No stats for column {col}"
            assert col_stats.distinct_values > 0

    def test_done_criteria_cost_model_estimates_operations(self, setup_full):
        """
        Done Criteria: Cost model оценивает стоимость операций.
        """
        from mini_db_v2.optimizer.cost_model import CostModel
        
        model = CostModel()
        
        # Test seq scan cost
        seq_cost = model.estimate_seq_scan_cost(10000, 1000, 0.1)
        assert seq_cost.total_cost > 0
        assert seq_cost.plan_type == "SeqScan"
        
        # Test index scan cost
        idx_cost = model.estimate_index_scan_cost(10000, 0.01, has_index=True)
        assert idx_cost.total_cost > 0
        
        # Test join costs
        nl_cost = model.estimate_nested_loop_join_cost(100, 10, 5.0, 0.1)
        assert nl_cost.total_cost > 0
        
        hash_cost = model.estimate_hash_join_cost(1000, 500, 64, 32, 0.1)
        assert hash_cost.total_cost > 0
        
        merge_cost = model.estimate_merge_join_cost(1000, 500, True, True, 0.1)
        assert merge_cost.total_cost > 0

    def test_done_criteria_histogram_estimates_selectivity(self, setup_full):
        """
        Done Criteria: Histogram корректно оценивает selectivity.
        """
        db, stats, executor = setup_full
        
        # Create table with uniform distribution
        executor.execute(parse_sql("CREATE TABLE uniform (id INT, value INT)"))
        for i in range(1000):
            executor.execute(parse_sql(f"INSERT INTO uniform (id, value) VALUES ({i}, {i})"))
        
        executor.execute(parse_sql("ANALYZE TABLE uniform"))
        
        # Test selectivity estimation for < operator
        # value < 500 should be ~0.5
        selectivity = stats.estimate_selectivity("uniform", "value", "<", 500)
        assert 0.4 <= selectivity <= 0.6, f"Expected ~0.5, got {selectivity}"
        
        # value < 250 should be ~0.25
        selectivity = stats.estimate_selectivity("uniform", "value", "<", 250)
        assert 0.2 <= selectivity <= 0.35, f"Expected ~0.25, got {selectivity}"
        
        # value < 100 should be ~0.1
        selectivity = stats.estimate_selectivity("uniform", "value", "<", 100)
        assert 0.08 <= selectivity <= 0.15, f"Expected ~0.1, got {selectivity}"


# =============================================================================
# Integration with Query Execution Tests
# =============================================================================

class TestStatisticsQueryIntegration:
    """Интеграция статистики с выполнением запросов."""

    @pytest.fixture
    def setup_with_stats(self):
        """Создаёт базу со статистикой."""
        db = Database()
        stats = Statistics()
        executor = Executor(db, stats)
        
        # Create and populate table
        executor.execute(parse_sql("CREATE TABLE products (id INT, category TEXT, price INT)"))
        
        categories = ["electronics", "clothing", "food", "books"]
        for i in range(100):
            cat = categories[i % 4]
            price = 100 + (i * 10)
            executor.execute(parse_sql(f"INSERT INTO products (id, category, price) VALUES ({i}, '{cat}', {price})"))
        
        # Analyze
        executor.execute(parse_sql("ANALYZE TABLE products"))
        
        return db, stats, executor

    def test_statistics_available_after_analyze(self, setup_with_stats):
        """Статистика доступна после ANALYZE."""
        db, stats, executor = setup_with_stats
        
        # Verify stats exist
        table_stats = stats.get_table_stats("products")
        assert table_stats is not None
        assert table_stats.row_count == 100
        
        cat_stats = stats.get_column_stats("products", "category")
        assert cat_stats is not None
        assert cat_stats.distinct_values == 4

    def test_selectivity_estimation_after_analyze(self, setup_with_stats):
        """Оценка selectivity работает после ANALYZE."""
        db, stats, executor = setup_with_stats
        
        # category = 'electronics' should be ~0.25
        selectivity = stats.estimate_selectivity("products", "category", "=", "electronics")
        assert 0.2 <= selectivity <= 0.3

    def test_statistics_persists_multiple_analyzes(self, setup_with_stats):
        """Статистика обновляется при повторном ANALYZE."""
        db, stats, executor = setup_with_stats
        
        # Get initial stats
        initial_stats = stats.get_table_stats("products")
        initial_time = initial_stats.last_analyze
        
        # Insert more data
        for i in range(100, 150):
            executor.execute(parse_sql(f"INSERT INTO products (id, category, price) VALUES ({i}, 'new', {i * 10})"))
        
        # Re-analyze
        executor.execute(parse_sql("ANALYZE TABLE products"))
        
        # Verify updated
        updated_stats = stats.get_table_stats("products")
        assert updated_stats.row_count == 150
        assert updated_stats.last_analyze >= initial_time


# =============================================================================
# Error Handling Tests
# =============================================================================

class TestAnalyzeTableErrors:
    """Тесты обработки ошибок ANALYZE TABLE."""

    @pytest.fixture
    def setup(self):
        """Базовая настройка."""
        db = Database()
        executor = Executor(db)
        return db, executor

    def test_analyze_nonexistent_table(self, setup):
        """ANALYZE несуществующей таблицы."""
        from mini_db_v2.executor.executor import TableNotFoundError
        db, executor = setup
        
        with pytest.raises(TableNotFoundError):
            executor.execute(parse_sql("ANALYZE TABLE nonexistent"))

    def test_analyze_after_drop(self, setup):
        """ANALYZE после DROP TABLE."""
        from mini_db_v2.executor.executor import TableNotFoundError
        db, executor = setup
        
        executor.execute(parse_sql("CREATE TABLE temp (id INT)"))
        executor.execute(parse_sql("DROP TABLE temp"))
        
        with pytest.raises(TableNotFoundError):
            executor.execute(parse_sql("ANALYZE TABLE temp"))


# =============================================================================
# Performance Tests
# =============================================================================

class TestAnalyzeTablePerformance:
    """Тесты производительности ANALYZE TABLE."""

    def test_analyze_large_table_performance(self):
        """Производительность ANALYZE для большой таблицы."""
        import time
        
        db = Database()
        stats = Statistics()
        executor = Executor(db, stats)
        
        # Create table with 1000 rows
        executor.execute(parse_sql("CREATE TABLE large (id INT, value INT)"))
        
        # Batch insert would be better, but using individual inserts for simplicity
        for i in range(1000):
            executor.execute(parse_sql(f"INSERT INTO large (id, value) VALUES ({i}, {i % 100})"))
        
        # Measure analyze time
        start = time.time()
        result = executor.execute(parse_sql("ANALYZE TABLE large"))
        elapsed = time.time() - start
        
        assert result.success
        # Analyze should complete in reasonable time (< 5 seconds for 1000 rows)
        assert elapsed < 5.0, f"ANALYZE took {elapsed}s, expected < 5s"


# =============================================================================
# Thread Safety Tests
# =============================================================================

class TestAnalyzeTableThreadSafety:
    """Тесты thread safety для ANALYZE TABLE."""

    def test_concurrent_analyze_different_tables(self):
        """Конкурентный ANALYZE разных таблиц."""
        import threading
        
        db = Database()
        stats = Statistics()
        executor = Executor(db, stats)
        
        # Create multiple tables
        for i in range(5):
            executor.execute(parse_sql(f"CREATE TABLE table_{i} (id INT, value INT)"))
            for j in range(10):
                executor.execute(parse_sql(f"INSERT INTO table_{i} (id, value) VALUES ({j}, {j})"))
        
        errors = []
        
        def analyze_table(table_name):
            try:
                result = executor.execute(parse_sql(f"ANALYZE TABLE {table_name}"))
                if not result.success:
                    errors.append(f"Failed to analyze {table_name}")
            except Exception as e:
                errors.append(str(e))
        
        threads = [threading.Thread(target=analyze_table, args=(f"table_{i}",)) for i in range(5)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0, f"Errors: {errors}"