# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_explain_phase4
# Intent: Тесты для EXPLAIN command (Phase 4) - вывод плана выполнения.
# Dependencies: pytest, mini_db_v2.parser.parser, mini_db_v2.executor.executor
# END_MODULE_CONTRACT

"""
Phase 4: EXPLAIN Command Tests

Тестирует:
1. Parser: EXPLAIN SELECT ...
2. Parser: EXPLAIN ANALYZE SELECT ...
3. Executor: форматированный вывод плана
4. Интеграция с QueryPlanner
"""

import pytest
import tempfile
import os

from mini_db_v2.parser.parser import Parser, ParseError, parse_sql
from mini_db_v2.parser.lexer import Lexer
from mini_db_v2.ast.nodes import (
    ExplainNode, SelectNode, CreateTableNode, InsertNode
)
from mini_db_v2.executor.executor import Executor, ExecutionResult
from mini_db_v2.storage.database import Database
from mini_db_v2.optimizer.statistics import Statistics


# =============================================================================
# FIXTURES
# =============================================================================

from mini_db_v2.storage.table import ColumnDef, DataType as StorageDataType

@pytest.fixture
def database() -> Database:
    """Создаёт тестовую базу данных с таблицами."""
    db = Database()
    
    # Create small table
    db.create_table("small", {
        "id": ColumnDef(name="id", data_type=StorageDataType.INT, nullable=False, primary_key=True, unique=True)
    })
    
    # Create large table
    db.create_table("large", {
        "id": ColumnDef(name="id", data_type=StorageDataType.INT, nullable=False, primary_key=True, unique=True)
    })
    
    # Create users table
    db.create_table("users", {
        "id": ColumnDef(name="id", data_type=StorageDataType.INT, nullable=False, primary_key=True, unique=True),
        "name": ColumnDef(name="name", data_type=StorageDataType.TEXT, nullable=True, primary_key=False, unique=False),
        "age": ColumnDef(name="age", data_type=StorageDataType.INT, nullable=True, primary_key=False, unique=False)
    })
    
    return db


@pytest.fixture
def executor(database: Database) -> Executor:
    """Создаёт executor с тестовой базой данных."""
    statistics = Statistics()
    return Executor(database, statistics)


# =============================================================================
# TEST EXPLAIN PARSER
# =============================================================================

class TestExplainParser:
    """Тесты для парсинга EXPLAIN."""
    
    def test_parse_explain_simple(self):
        """Парсинг EXPLAIN SELECT."""
        sql = "EXPLAIN SELECT * FROM users"
        ast = parse_sql(sql)
        
        assert isinstance(ast, ExplainNode)
        assert ast.analyze == False
        assert isinstance(ast.query, SelectNode)
    
    def test_parse_explain_analyze(self):
        """Парсинг EXPLAIN ANALYZE SELECT."""
        sql = "EXPLAIN ANALYZE SELECT * FROM users"
        ast = parse_sql(sql)
        
        assert isinstance(ast, ExplainNode)
        assert ast.analyze == True
        assert isinstance(ast.query, SelectNode)
    
    def test_parse_explain_with_where(self):
        """Парсинг EXPLAIN SELECT с WHERE."""
        sql = "EXPLAIN SELECT * FROM users WHERE age > 18"
        ast = parse_sql(sql)
        
        assert isinstance(ast, ExplainNode)
        assert ast.query.where is not None
    
    def test_parse_explain_with_join(self):
        """Парсинг EXPLAIN SELECT с JOIN."""
        sql = "EXPLAIN SELECT * FROM small JOIN large ON small.id = large.id"
        ast = parse_sql(sql)
        
        assert isinstance(ast, ExplainNode)
        assert ast.query.from_clause is not None
        assert len(ast.query.from_clause.joins) == 1
    
    def test_parse_explain_non_select_raises_error(self):
        """EXPLAIN только для SELECT, другие команды вызывают ошибку."""
        sql = "EXPLAIN INSERT INTO users (id) VALUES (1)"
        
        with pytest.raises(ParseError):
            parse_sql(sql)
    
    def test_parse_explain_update_raises_error(self):
        """EXPLAIN UPDATE вызывает ошибку."""
        sql = "EXPLAIN UPDATE users SET age = 20"
        
        with pytest.raises(ParseError):
            parse_sql(sql)
    
    def test_parse_explain_delete_raises_error(self):
        """EXPLAIN DELETE вызывает ошибку."""
        sql = "EXPLAIN DELETE FROM users"
        
        with pytest.raises(ParseError):
            parse_sql(sql)


# =============================================================================
# TEST EXPLAIN EXECUTOR
# =============================================================================

class TestExplainExecutor:
    """Тесты для выполнения EXPLAIN."""
    
    def test_execute_explain_simple(self, executor: Executor):
        """Выполнение EXPLAIN SELECT."""
        ast = parse_sql("EXPLAIN SELECT * FROM users")
        
        result = executor.execute(ast)
        
        assert result.success == True
        assert "QUERY PLAN" in result.message
        assert "SeqScan" in result.message or "Scan" in result.message
    
    def test_execute_explain_returns_rows(self, executor: Executor):
        """EXPLAIN возвращает rows с планом."""
        ast = parse_sql("EXPLAIN SELECT * FROM users")
        
        result = executor.execute(ast)
        
        assert len(result.rows) > 0
        assert "QUERY PLAN" in result.rows[0].get("QUERY PLAN", "")
    
    def test_execute_explain_columns(self, executor: Executor):
        """EXPLAIN возвращает колонку QUERY PLAN."""
        ast = parse_sql("EXPLAIN SELECT * FROM users")
        
        result = executor.execute(ast)
        
        assert "QUERY PLAN" in result.columns
    
    def test_execute_explain_analyze(self, executor: Executor):
        """Выполнение EXPLAIN ANALYZE SELECT."""
        ast = parse_sql("EXPLAIN ANALYZE SELECT * FROM users")
        
        result = executor.execute(ast)
        
        assert result.success == True
        assert "QUERY PLAN" in result.message
        assert "Actual rows:" in result.message
        assert "Execution time:" in result.message
    
    def test_execute_explain_with_where(self, executor: Executor):
        """EXPLAIN с WHERE условием."""
        ast = parse_sql("EXPLAIN SELECT * FROM users WHERE age > 18")
        
        result = executor.execute(ast)
        
        assert result.success == True
        assert "QUERY PLAN" in result.message
        # Filter should be mentioned
        assert "Filter" in result.message or "age" in result.message
    
    def test_execute_explain_nonexistent_table(self, executor: Executor):
        """EXPLAIN для несуществующей таблицы."""
        ast = parse_sql("EXPLAIN SELECT * FROM nonexistent")
        
        # Should raise error or return error result
        try:
            result = executor.execute(ast)
            # If no exception, check for error in result
            assert result.success == False or "error" in result.message.lower()
        except Exception as e:
            # Exception is acceptable
            assert "not found" in str(e).lower() or "nonexistent" in str(e).lower()


# =============================================================================
# TEST EXPLAIN OUTPUT FORMAT
# =============================================================================

class TestExplainOutputFormat:
    """Тесты для формата вывода EXPLAIN."""
    
    def test_explain_has_header(self, executor: Executor):
        """EXPLAIN вывод содержит заголовок QUERY PLAN."""
        ast = parse_sql("EXPLAIN SELECT * FROM users")
        
        result = executor.execute(ast)
        
        assert "QUERY PLAN" in result.message
    
    def test_explain_has_separator(self, executor: Executor):
        """EXPLAIN вывод содержит разделитель."""
        ast = parse_sql("EXPLAIN SELECT * FROM users")
        
        result = executor.execute(ast)
        
        # Should have dashes as separator
        assert "----" in result.message or "---" in result.message
    
    def test_explain_shows_cost(self, executor: Executor):
        """EXPLAIN вывод содержит стоимость."""
        ast = parse_sql("EXPLAIN SELECT * FROM users")
        
        result = executor.execute(ast)
        
        # Should show cost
        assert "cost=" in result.message or "cost" in result.message.lower()
    
    def test_explain_shows_rows(self, executor: Executor):
        """EXPLAIN вывод содержит количество строк."""
        ast = parse_sql("EXPLAIN SELECT * FROM users")
        
        result = executor.execute(ast)
        
        # Should show rows estimate
        assert "rows=" in result.message or "rows" in result.message.lower()
    
    def test_explain_shows_table_name(self, executor: Executor):
        """EXPLAIN вывод содержит имя таблицы."""
        ast = parse_sql("EXPLAIN SELECT * FROM users")
        
        result = executor.execute(ast)
        
        assert "users" in result.message
    
    def test_explain_shows_scan_type(self, executor: Executor):
        """EXPLAIN вывод содержит тип сканирования."""
        ast = parse_sql("EXPLAIN SELECT * FROM users")
        
        result = executor.execute(ast)
        
        # Should show SeqScan or IndexScan
        assert "Scan" in result.message or "scan" in result.message.lower()


# =============================================================================
# TEST EXPLAIN WITH JOIN
# =============================================================================

class TestExplainWithJoin:
    """Тесты для EXPLAIN с JOIN."""
    
    def test_explain_two_table_join(self, executor: Executor):
        """EXPLAIN для JOIN двух таблиц."""
        sql = "EXPLAIN SELECT * FROM small JOIN large ON small.id = large.id"
        ast = parse_sql(sql)
        
        result = executor.execute(ast)
        
        assert result.success == True
        assert "QUERY PLAN" in result.message
        # Should show join
        assert "Join" in result.message or "join" in result.message.lower()
    
    def test_explain_shows_join_type(self, executor: Executor):
        """EXPLAIN показывает тип JOIN."""
        sql = "EXPLAIN SELECT * FROM small JOIN large ON small.id = large.id"
        ast = parse_sql(sql)
        
        result = executor.execute(ast)
        
        # Should show HashJoin, NestedLoop, or MergeJoin
        join_types = ["HashJoin", "NestedLoop", "MergeJoin", "Hash", "Nested", "Merge"]
        has_join_type = any(jt in result.message for jt in join_types)
        assert has_join_type or "Join" in result.message
    
    def test_explain_shows_join_condition(self, executor: Executor):
        """EXPLAIN показывает условие JOIN."""
        sql = "EXPLAIN SELECT * FROM small JOIN large ON small.id = large.id"
        ast = parse_sql(sql)
        
        result = executor.execute(ast)
        
        # Should show join condition
        assert "Cond" in result.message or "id" in result.message
    
    def test_explain_left_join(self, executor: Executor):
        """EXPLAIN для LEFT JOIN."""
        sql = "EXPLAIN SELECT * FROM small LEFT JOIN large ON small.id = large.id"
        ast = parse_sql(sql)
        
        result = executor.execute(ast)
        
        assert result.success == True
        assert "QUERY PLAN" in result.message


# =============================================================================
# TEST EXPLAIN ANALYZE
# =============================================================================

class TestExplainAnalyze:
    """Тесты для EXPLAIN ANALYZE."""
    
    def test_explain_analyze_shows_actual_rows(self, executor: Executor):
        """EXPLAIN ANALYZE показывает фактическое количество строк."""
        sql = "EXPLAIN ANALYZE SELECT * FROM users"
        ast = parse_sql(sql)
        
        result = executor.execute(ast)
        
        assert "Actual rows:" in result.message
    
    def test_explain_analyze_shows_execution_time(self, executor: Executor):
        """EXPLAIN ANALYZE показывает время выполнения."""
        sql = "EXPLAIN ANALYZE SELECT * FROM users"
        ast = parse_sql(sql)
        
        result = executor.execute(ast)
        
        assert "Execution time:" in result.message
    
    def test_explain_analyze_executes_query(self, executor: Executor):
        """EXPLAIN ANALYZE фактически выполняет запрос."""
        sql = "EXPLAIN ANALYZE SELECT * FROM users"
        ast = parse_sql(sql)
        
        result = executor.execute(ast)
        
        # Should have executed the query
        assert result.success == True
        # Actual rows should be 0 (empty table)
        assert "Actual rows: 0" in result.message or "Actual rows:" in result.message


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

class TestExplainIntegration:
    """Интеграционные тесты EXPLAIN."""
    
    def test_explain_after_insert(self, database: Database):
        """EXPLAIN после INSERT показывает корректные данные."""
        executor = Executor(database)
        
        # Insert some data
        insert_ast = parse_sql("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)")
        executor.execute(insert_ast)
        
        insert_ast = parse_sql("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30)")
        executor.execute(insert_ast)
        
        # Analyze table
        analyze_ast = parse_sql("ANALYZE TABLE users")
        executor.execute(analyze_ast)
        
        # Explain
        explain_ast = parse_sql("EXPLAIN SELECT * FROM users")
        result = executor.execute(explain_ast)
        
        assert result.success == True
        assert "QUERY PLAN" in result.message
    
    def test_explain_with_index(self, database: Database):
        """EXPLAIN с индексом может показать IndexScan."""
        executor = Executor(database)
        
        # Create index
        index_ast = parse_sql("CREATE INDEX idx_users_age ON users (age)")
        executor.execute(index_ast)
        
        # Insert data
        insert_ast = parse_sql("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)")
        executor.execute(insert_ast)
        
        # Analyze
        analyze_ast = parse_sql("ANALYZE TABLE users")
        executor.execute(analyze_ast)
        
        # Explain with selective WHERE
        explain_ast = parse_sql("EXPLAIN SELECT * FROM users WHERE age = 25")
        result = executor.execute(explain_ast)
        
        assert result.success == True
        # May or may not show IndexScan depending on selectivity
        assert "QUERY PLAN" in result.message
    
    def test_explain_multiple_queries(self, executor: Executor):
        """Несколько EXPLAIN запросов подряд."""
        queries = [
            "EXPLAIN SELECT * FROM users",
            "EXPLAIN SELECT * FROM users WHERE age > 18",
            "EXPLAIN ANALYZE SELECT * FROM users",
        ]
        
        for sql in queries:
            ast = parse_sql(sql)
            result = executor.execute(ast)
            
            assert result.success == True
            assert "QUERY PLAN" in result.message


# =============================================================================
# ADVERSARIAL TESTS
# =============================================================================

class TestExplainAdversarial:
    """Адверсарные тесты для EXPLAIN."""
    
    def test_explain_empty_table(self, executor: Executor):
        """EXPLAIN для пустой таблицы."""
        ast = parse_sql("EXPLAIN SELECT * FROM users")
        
        result = executor.execute(ast)
        
        assert result.success == True
    
    def test_explain_complex_where(self, executor: Executor):
        """EXPLAIN со сложным WHERE."""
        sql = """
        EXPLAIN SELECT * FROM users 
        WHERE (age > 18 AND age < 65) OR (name = 'Admin')
        """
        ast = parse_sql(sql)
        
        result = executor.execute(ast)
        
        assert result.success == True
        assert "QUERY PLAN" in result.message
    
    def test_explain_with_null_condition(self, executor: Executor):
        """EXPLAIN с NULL в условии."""
        sql = "EXPLAIN SELECT * FROM users WHERE age = NULL"
        ast = parse_sql(sql)
        
        result = executor.execute(ast)
        
        assert result.success == True
    
    def test_explain_select_star_star(self, executor: Executor):
        """EXPLAIN SELECT * работает корректно."""
        sql = "EXPLAIN SELECT * FROM users"
        ast = parse_sql(sql)
        
        result = executor.execute(ast)
        
        assert result.success == True
    
    def test_explain_select_specific_columns(self, executor: Executor):
        """EXPLAIN SELECT с конкретными колонками."""
        sql = "EXPLAIN SELECT id, name FROM users"
        ast = parse_sql(sql)
        
        result = executor.execute(ast)
        
        assert result.success == True
    
    def test_explain_with_limit(self, executor: Executor):
        """EXPLAIN с LIMIT."""
        sql = "EXPLAIN SELECT * FROM users LIMIT 10"
        ast = parse_sql(sql)
        
        result = executor.execute(ast)
        
        assert result.success == True
    
    def test_explain_with_order_by(self, executor: Executor):
        """EXPLAIN с ORDER BY."""
        sql = "EXPLAIN SELECT * FROM users ORDER BY age DESC"
        ast = parse_sql(sql)
        
        result = executor.execute(ast)
        
        assert result.success == True


# =============================================================================
# TEST EXPLAIN WITH STATISTICS
# =============================================================================

class TestExplainWithStatistics:
    """Тесты для EXPLAIN со статистикой."""
    
    def test_explain_after_analyze(self, database: Database):
        """EXPLAIN после ANALYZE использует статистику."""
        executor = Executor(database)
        
        # Insert data
        for i in range(10):
            insert_ast = parse_sql(f"INSERT INTO users (id, name, age) VALUES ({i}, 'User{i}', {20 + i})")
            executor.execute(insert_ast)
        
        # Analyze
        analyze_ast = parse_sql("ANALYZE TABLE users")
        result = executor.execute(analyze_ast)
        assert result.success == True
        
        # Explain
        explain_ast = parse_sql("EXPLAIN SELECT * FROM users")
        result = executor.execute(explain_ast)
        
        assert result.success == True
        # Should show estimated rows based on statistics
        assert "rows=" in result.message or "rows" in result.message.lower()
    
    def test_explain_uses_table_stats(self, database: Database):
        """EXPLAIN использует статистику таблицы."""
        from mini_db_v2.optimizer.statistics import Statistics, TableStats
        
        statistics = Statistics()
        statistics.set_table_stats("users", TableStats(row_count=1000, page_count=100))
        
        executor = Executor(database, statistics)
        
        explain_ast = parse_sql("EXPLAIN SELECT * FROM users")
        result = executor.execute(explain_ast)
        
        assert result.success == True


# =============================================================================
# TEST EXPLAIN ERROR HANDLING
# =============================================================================

class TestExplainErrorHandling:
    """Тесты для обработки ошибок в EXPLAIN."""
    
    def test_explain_invalid_syntax(self):
        """EXPLAIN с неверным синтаксисом вызывает ошибку."""
        sql = "EXPLAIN SELECT"  # Incomplete
        
        with pytest.raises(ParseError):
            parse_sql(sql)
    
    def test_explain_missing_table(self):
        """EXPLAIN без FROM парсится (SELECT без FROM)."""
        sql = "EXPLAIN SELECT 1"
        ast = parse_sql(sql)
        
        assert isinstance(ast, ExplainNode)
    
    def test_explain_subquery(self):
        """EXPLAIN с подзапросом."""
        sql = "EXPLAIN SELECT * FROM users WHERE id IN (SELECT id FROM users)"
        ast = parse_sql(sql)
        
        assert isinstance(ast, ExplainNode)
        assert isinstance(ast.query, SelectNode)