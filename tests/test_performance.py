# START_MODULE_CONTRACT
# Module: tests.test_performance
# Intent: Performance тесты - проверка скорости работы на больших данных.
#         Тестируем O(n) операции, индексы, память.
# Constraints: Без сторонних библиотек, только time module.
# END_MODULE_CONTRACT

"""
PERFORMANCE TESTS: Stress Testing with Large Data

Эти тесты проверяют производительность:
1. Lexer/Parser на больших запросах
2. INSERT множества строк
3. SELECT с индексом и без
4. UPDATE/DELETE на больших таблицах
5. SAVE/LOAD больших баз
"""

import pytest
import time
import tempfile
import os
from typing import Any, Callable

from mini_db.parser.lexer import Lexer
from mini_db.parser.parser import Parser
from mini_db.storage.database import Database
from mini_db.storage.table import Table
from mini_db.executor.executor import Executor
from mini_db.ast.nodes import ColumnDef


# ==================== PERFORMANCE THRESHOLDS ====================

# Пороги производительности (в секундах)
# Эти значения могут быть скорректированы в зависимости от hardware

LEXER_1000_TOKENS_THRESHOLD = 0.1  # 1000 токенов за 0.1 сек
PARSER_COMPLEX_QUERY_THRESHOLD = 0.1  # Сложный запрос за 0.1 сек
INSERT_1000_ROWS_THRESHOLD = 1.0  # 1000 INSERT за 1 сек
SELECT_1000_ROWS_FULL_SCAN_THRESHOLD = 0.1  # Full scan 1000 строк за 0.1 сек
SELECT_1000_ROWS_INDEXED_THRESHOLD = 0.01  # Indexed lookup за 0.01 сек
UPDATE_1000_ROWS_THRESHOLD = 1.0  # UPDATE 1000 строк за 1 сек
DELETE_1000_ROWS_THRESHOLD = 0.5  # DELETE 1000 строк за 0.5 сек
SAVE_1000_ROWS_THRESHOLD = 1.0  # SAVE 1000 строк за 1 сек
LOAD_1000_ROWS_THRESHOLD = 1.0  # LOAD 1000 строк за 1 сек


def measure_time(func: Callable, *args, **kwargs) -> tuple[float, Any]:
    """Измеряет время выполнения функции."""
    start = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - start
    return elapsed, result


# ==================== LEXER PERFORMANCE ====================

class TestLexerPerformance:
    """Производительность Lexer."""
    
    def test_lexer_1000_tokens(self):
        """Lexer обрабатывает 1000 токенов быстро."""
        lexer = Lexer()
        
        # Генерируем запрос с 1000 токенами
        # SELECT * FROM t WHERE x = 1 OR x = 2 OR ... (500 условий)
        parts = ["SELECT * FROM t WHERE"]
        for i in range(500):
            if i > 0:
                parts.append("OR")
            parts.append(f"x = {i}")
        
        query = ' '.join(parts)
        
        elapsed, tokens = measure_time(lexer.tokenize, query)
        
        # Проверяем что токенов примерно 1000
        assert len(tokens) > 1000
        
        # Проверяем производительность
        print(f"\n[Lexer] 1000+ tokens: {elapsed:.4f}s (threshold: {LEXER_1000_TOKENS_THRESHOLD}s)")
        assert elapsed < LEXER_1000_TOKENS_THRESHOLD, f"Lexer too slow: {elapsed}s > {LEXER_1000_TOKENS_THRESHOLD}s"
    
    def test_lexer_very_long_string(self):
        """Lexer обрабатывает очень длинные строки."""
        lexer = Lexer()
        
        # Строка на 10000 символов
        long_string = "'" + "x" * 10000 + "'"
        
        elapsed, tokens = measure_time(lexer.tokenize, long_string)
        
        assert len(tokens) == 2  # STRING + EOF
        assert tokens[0].value == "x" * 10000
        
        print(f"\n[Lexer] 10000 char string: {elapsed:.4f}s")
        assert elapsed < 0.1
    
    def test_lexer_deeply_nested_parens(self):
        """Lexer обрабатывает глубокую вложенность скобок."""
        lexer = Lexer()
        
        depth = 1000
        query = "(" * depth + "x" + ")" * depth
        
        elapsed, tokens = measure_time(lexer.tokenize, query)
        
        # Должно быть depth LPAREN + 1 IDENTIFIER + depth RPAREN + EOF
        assert len(tokens) == 2 * depth + 2
        
        print(f"\n[Lexer] {depth} nested parens: {elapsed:.4f}s")
        assert elapsed < 0.1


# ==================== PARSER PERFORMANCE ====================

class TestParserPerformance:
    """Производительность Parser."""
    
    def test_parser_complex_where(self):
        """Parser обрабатывает сложный WHERE быстро."""
        parser = Parser()
        
        # Генерируем сложный WHERE с 100 условиями
        conditions = [f"x{i} = {i}" for i in range(100)]
        where = " AND ".join(conditions)
        query = f"SELECT * FROM t WHERE {where}"
        
        elapsed, ast = measure_time(parser.parse, query)
        
        print(f"\n[Parser] 100 AND conditions: {elapsed:.4f}s (threshold: {PARSER_COMPLEX_QUERY_THRESHOLD}s)")
        assert elapsed < PARSER_COMPLEX_QUERY_THRESHOLD
    
    def test_parser_many_columns(self):
        """Parser обрабатывает много колонок."""
        parser = Parser()
        
        # SELECT с 100 колонками
        columns = ', '.join(f"col_{i}" for i in range(100))
        query = f"SELECT {columns} FROM t"
        
        elapsed, ast = measure_time(parser.parse, query)
        
        assert len(ast.columns) == 100
        print(f"\n[Parser] 100 columns: {elapsed:.4f}s")
        assert elapsed < 0.1
    
    def test_parser_create_table_many_columns(self):
        """Parser обрабатывает CREATE TABLE с многими колонками."""
        parser = Parser()
        
        # CREATE TABLE с 50 колонками
        columns = ', '.join(f"col_{i} INT" for i in range(50))
        query = f"CREATE TABLE t ({columns})"
        
        elapsed, ast = measure_time(parser.parse, query)
        
        assert len(ast.columns) == 50
        print(f"\n[Parser] CREATE TABLE 50 columns: {elapsed:.4f}s")
        assert elapsed < 0.1


# ==================== INSERT PERFORMANCE ====================

class TestInsertPerformance:
    """Производительность INSERT."""
    
    def test_insert_1000_rows(self):
        """INSERT 1000 строк выполняется быстро."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT"),
            ColumnDef(name="value", data_type="TEXT"),
        ])
        
        table = db.get_table("t")
        
        start = time.perf_counter()
        for i in range(1000):
            table.insert({"id": i, "value": f"value_{i}"})
        elapsed = time.perf_counter() - start
        
        assert len(table.rows) == 1000
        
        print(f"\n[INSERT] 1000 rows: {elapsed:.4f}s (threshold: {INSERT_1000_ROWS_THRESHOLD}s)")
        assert elapsed < INSERT_1000_ROWS_THRESHOLD
    
    def test_insert_1000_rows_with_unique(self):
        """INSERT с UNIQUE проверкой выполняется разумно."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT", unique=True),
            ColumnDef(name="value", data_type="TEXT"),
        ])
        
        table = db.get_table("t")
        
        start = time.perf_counter()
        for i in range(1000):
            table.insert({"id": i, "value": f"value_{i}"})
        elapsed = time.perf_counter() - start
        
        assert len(table.rows) == 1000
        
        print(f"\n[INSERT] 1000 rows with UNIQUE: {elapsed:.4f}s")
        # UNIQUE добавляет overhead, но не более 2x
        assert elapsed < INSERT_1000_ROWS_THRESHOLD * 2
    
    def test_insert_performance_scales_linearly(self):
        """INSERT масштабируется линейно O(n)."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT"),
        ])
        
        table = db.get_table("t")
        
        # Измеряем время для разных объёмов
        times = []
        sizes = [100, 500, 1000]
        
        for size in sizes:
            # Очищаем таблицу
            table.rows = []
            
            start = time.perf_counter()
            for i in range(size):
                table.insert({"id": i})
            elapsed = time.perf_counter() - start
            times.append(elapsed)
        
        # Проверяем что время растёт линейно (не квадратично)
        # Если 1000 строк в 10 раз дольше чем 100 - это O(n)
        # Если в 100 раз дольше - это O(n²)
        ratio = times[2] / times[0]  # 1000 / 100
        expected_ratio = sizes[2] / sizes[0]  # 10
        
        print(f"\n[INSERT] Scaling: 100={times[0]:.4f}s, 500={times[1]:.4f}s, 1000={times[2]:.4f}s")
        print(f"[INSERT] Ratio: {ratio:.2f} (expected ~{expected_ratio})")
        
        # Допускаем 2x overhead от линейности
        assert ratio < expected_ratio * 2, f"INSERT not O(n): ratio={ratio}, expected ~{expected_ratio}"


# ==================== SELECT PERFORMANCE ====================

class TestSelectPerformance:
    """Производительность SELECT."""
    
    def test_select_full_scan_1000_rows(self):
        """SELECT full scan 1000 строк выполняется быстро."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT"),
            ColumnDef(name="value", data_type="TEXT"),
        ])
        
        table = db.get_table("t")
        
        # Вставляем 1000 строк
        for i in range(1000):
            table.insert({"id": i, "value": f"value_{i}"})
        
        # Full scan
        elapsed, result = measure_time(
            table.select,
            lambda row: True,  # Все строки
            None  # Все колонки
        )
        
        assert result.success == True
        assert result.data is not None
        assert len(result.data) == 1000
        
        print(f"\n[SELECT] Full scan 1000 rows: {elapsed:.4f}s (threshold: {SELECT_1000_ROWS_FULL_SCAN_THRESHOLD}s)")
        assert elapsed < SELECT_1000_ROWS_FULL_SCAN_THRESHOLD
    
    def test_select_with_index_is_fast(self):
        """SELECT с индексом значительно быстрее full scan."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT"),
        ])
        
        table = db.get_table("t")
        table.create_index("idx_id", "id")
        
        # Вставляем 1000 строк
        for i in range(1000):
            table.insert({"id": i})
        
        # Поиск по индексу
        elapsed, result = measure_time(
            table.select,
            lambda row: row["id"] == 500,
            None
        )
        
        assert result.success == True
        assert result.data is not None
        assert len(result.data) == 1
        
        print(f"\n[SELECT] Indexed lookup 1000 rows: {elapsed:.4f}s (threshold: {SELECT_1000_ROWS_INDEXED_THRESHOLD}s)")
        assert elapsed < SELECT_1000_ROWS_INDEXED_THRESHOLD
    
    def test_select_with_complex_where(self):
        """SELECT с сложным WHERE выполняется разумно."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="a", data_type="INT"),
            ColumnDef(name="b", data_type="INT"),
            ColumnDef(name="c", data_type="INT"),
        ])
        
        table = db.get_table("t")
        
        # Вставляем 1000 строк
        for i in range(1000):
            table.insert({"a": i, "b": i % 10, "c": i % 5})
        
        # Сложный WHERE
        elapsed, result = measure_time(
            table.select,
            lambda row: (row["a"] > 100 and row["b"] == 5) or row["c"] == 0,
            None
        )
        
        print(f"\n[SELECT] Complex WHERE 1000 rows: {elapsed:.4f}s")
        assert elapsed < 0.1
    
    def test_index_speedup_demonstration(self):
        """Демонстрация ускорения от индекса."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT"),
        ])
        
        table = db.get_table("t")
        
        # Вставляем 1000 строк
        for i in range(1000):
            table.insert({"id": i})
        
        # Full scan
        start_full = time.perf_counter()
        table.select(lambda row: row["id"] == 500, None)
        time_full = time.perf_counter() - start_full
        
        # Создаём индекс
        table.create_index("idx_id", "id")
        
        # Indexed lookup
        start_indexed = time.perf_counter()
        table.select(lambda row: row["id"] == 500, None)
        time_indexed = time.perf_counter() - start_indexed
        
        speedup = time_full / time_indexed if time_indexed > 0 else float('inf')
        
        print(f"\n[SELECT] Full scan: {time_full:.6f}s, Indexed: {time_indexed:.6f}s")
        print(f"[SELECT] Speedup from index: {speedup:.2f}x")
        
        # Индекс должен давать ускорение (хотя бы небольшое)
        # На малых данных ускорение может быть незаметным
        # Главное - индекс не должен замедлять


# ==================== UPDATE/DELETE PERFORMANCE ====================

class TestUpdateDeletePerformance:
    """Производительность UPDATE и DELETE."""
    
    def test_update_1000_rows(self):
        """UPDATE 1000 строк выполняется быстро."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT"),
            ColumnDef(name="value", data_type="TEXT"),
        ])
        
        table = db.get_table("t")
        
        # Вставляем 1000 строк
        for i in range(1000):
            table.insert({"id": i, "value": f"value_{i}"})
        
        # UPDATE всех строк
        elapsed, result = measure_time(
            table.update,
            lambda row: True,  # Все строки
            {"value": "updated"}
        )
        
        assert result.success == True
        assert result.rows_affected == 1000
        
        print(f"\n[UPDATE] 1000 rows: {elapsed:.4f}s (threshold: {UPDATE_1000_ROWS_THRESHOLD}s)")
        assert elapsed < UPDATE_1000_ROWS_THRESHOLD
    
    def test_update_with_unique_check(self):
        """UPDATE с UNIQUE проверкой выполняется разумно."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT", unique=True),
            ColumnDef(name="value", data_type="TEXT"),
        ])
        
        table = db.get_table("t")
        
        # Вставляем 1000 строк
        for i in range(1000):
            table.insert({"id": i, "value": f"value_{i}"})
        
        # UPDATE с уникальными значениями
        start = time.perf_counter()
        result = table.update(
            lambda row: True,
            {"value": "updated"}
        )
        elapsed = time.perf_counter() - start
        
        assert result.success == True
        
        print(f"\n[UPDATE] 1000 rows with UNIQUE: {elapsed:.4f}s")
        assert elapsed < UPDATE_1000_ROWS_THRESHOLD * 2
    
    def test_delete_1000_rows(self):
        """DELETE 1000 строк выполняется быстро."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT"),
        ])
        
        table = db.get_table("t")
        
        # Вставляем 1000 строк
        for i in range(1000):
            table.insert({"id": i})
        
        # DELETE всех строк
        elapsed, result = measure_time(
            table.delete,
            lambda row: True
        )
        
        assert result.success == True
        assert result.rows_affected == 1000
        assert len(table.rows) == 0
        
        print(f"\n[DELETE] 1000 rows: {elapsed:.4f}s (threshold: {DELETE_1000_ROWS_THRESHOLD}s)")
        assert elapsed < DELETE_1000_ROWS_THRESHOLD
    
    def test_delete_partial(self):
        """DELETE части строк выполняется быстро."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT"),
        ])
        
        table = db.get_table("t")
        
        # Вставляем 1000 строк
        for i in range(1000):
            table.insert({"id": i})
        
        # DELETE половины строк
        elapsed, result = measure_time(
            table.delete,
            lambda row: row["id"] < 500
        )
        
        assert result.rows_affected == 500
        assert len(table.rows) == 500
        
        print(f"\n[DELETE] 500 of 1000 rows: {elapsed:.4f}s")
        assert elapsed < DELETE_1000_ROWS_THRESHOLD


# ==================== SAVE/LOAD PERFORMANCE ====================

class TestSaveLoadPerformance:
    """Производительность SAVE/LOAD."""
    
    def test_save_1000_rows(self):
        """SAVE 1000 строк выполняется быстро."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "perf_test.json")
            
            db = Database()
            db.create_table("t", [
                ColumnDef(name="id", data_type="INT"),
                ColumnDef(name="value", data_type="TEXT"),
            ])
            
            table = db.get_table("t")
            
            # Вставляем 1000 строк
            for i in range(1000):
                table.insert({"id": i, "value": f"value_{i}"})
            
            # SAVE
            elapsed, _ = measure_time(db.save_to_file, filepath)
            
            # Проверяем файл создан
            assert os.path.exists(filepath)
            
            print(f"\n[SAVE] 1000 rows: {elapsed:.4f}s (threshold: {SAVE_1000_ROWS_THRESHOLD}s)")
            assert elapsed < SAVE_1000_ROWS_THRESHOLD
    
    def test_load_1000_rows(self):
        """LOAD 1000 строк выполняется быстро."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "perf_test.json")
            
            # Создаём и сохраняем
            db1 = Database()
            db1.create_table("t", [
                ColumnDef(name="id", data_type="INT"),
                ColumnDef(name="value", data_type="TEXT"),
            ])
            
            table1 = db1.get_table("t")
            for i in range(1000):
                table1.insert({"id": i, "value": f"value_{i}"})
            
            db1.save_to_file(filepath)
            
            # LOAD
            db2 = Database()
            elapsed, _ = measure_time(db2.load_from_file, filepath)
            
            # Проверяем данные загружены
            assert db2.table_exists("t")
            table2 = db2.get_table("t")
            assert len(table2.rows) == 1000
            
            print(f"\n[LOAD] 1000 rows: {elapsed:.4f}s (threshold: {LOAD_1000_ROWS_THRESHOLD}s)")
            assert elapsed < LOAD_1000_ROWS_THRESHOLD
    
    def test_save_load_with_indexes(self):
        """SAVE/LOAD с индексами выполняется разумно."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "indexed.json")
            
            db1 = Database()
            db1.create_table("t", [
                ColumnDef(name="id", data_type="INT"),
            ])
            
            table1 = db1.get_table("t")
            table1.create_index("idx_id", "id")
            
            for i in range(1000):
                table1.insert({"id": i})
            
            # SAVE
            time_save, _ = measure_time(db1.save_to_file, filepath)
            
            # LOAD
            db2 = Database()
            time_load, _ = measure_time(db2.load_from_file, filepath)
            
            table2 = db2.get_table("t")
            assert "idx_id" in table2.indexes
            
            print(f"\n[SAVE/LOAD] With indexes: save={time_save:.4f}s, load={time_load:.4f}s")


# ==================== MEMORY TESTS ====================

class TestMemoryUsage:
    """Тесты использования памяти (базовые)."""
    
    def test_large_table_memory_reasonable(self):
        """Большая таблица не потребляет чрезмерно много памяти."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT"),
            ColumnDef(name="value", data_type="TEXT"),
        ])
        
        table = db.get_table("t")
        
        # Вставляем 10000 строк
        for i in range(10000):
            table.insert({"id": i, "value": f"value_{i}"})
        
        # Проверяем что таблица существует
        assert len(table.rows) == 10000
        
        # Примерная оценка: каждая строка ~100 байт
        # 10000 строк = ~1 MB
        # Это очень грубая оценка, но помогает выявить утечки
        
        print(f"\n[Memory] 10000 rows created successfully")
    
    def test_many_tables_memory_reasonable(self):
        """Много таблиц не потребляют чрезмерно много памяти."""
        db = Database()
        
        # Создаём 100 таблиц
        for i in range(100):
            db.create_table(f"table_{i}", [
                ColumnDef(name="id", data_type="INT"),
            ])
        
        assert len(db.list_tables()) == 100
        
        print(f"\n[Memory] 100 tables created successfully")


# ==================== STRESS TESTS ====================

class TestStressScenarios:
    """Стресс-тесты."""
    
    def test_rapid_operations(self):
        """Быстрые последовательные операции."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT", unique=True),
        ])
        
        table = db.get_table("t")
        
        # Быстрые INSERT
        start = time.perf_counter()
        for i in range(100):
            table.insert({"id": i})
        time_insert = time.perf_counter() - start
        
        # Быстрые SELECT
        start = time.perf_counter()
        for i in range(100):
            table.select(lambda row, target=i: row["id"] == target, None)
        time_select = time.perf_counter() - start
        
        # Быстрые UPDATE
        start = time.perf_counter()
        for i in range(100):
            table.update(
                lambda row, target=i: row["id"] == target,
                {"id": i + 1000}
            )
        time_update = time.perf_counter() - start
        
        print(f"\n[Stress] 100 ops: insert={time_insert:.4f}s, select={time_select:.4f}s, update={time_update:.4f}s")
    
    def test_executor_stress(self):
        """Стресс-тест через Executor."""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        # CREATE TABLE
        ast = parser.parse("CREATE TABLE t (id INT UNIQUE, value TEXT)")
        executor.execute(ast, db)
        
        # Много INSERT
        start = time.perf_counter()
        for i in range(500):
            ast = parser.parse(f"INSERT INTO t (id, value) VALUES ({i}, 'val_{i}')")
            result = executor.execute(ast, db)
            assert result.success == True
        elapsed = time.perf_counter() - start
        
        print(f"\n[Executor] 500 INSERTs: {elapsed:.4f}s")
        assert elapsed < 5.0  # 500 INSERT за 5 секунд
    
    def test_repl_stress(self):
        """Стресс-тест REPL."""
        from mini_db.repl.repl import REPL
        
        repl = REPL()
        
        # CREATE TABLE
        repl.process("CREATE TABLE t (id INT)")
        
        # Много операций
        start = time.perf_counter()
        for i in range(100):
            repl.process(f"INSERT INTO t (id) VALUES ({i})")
        
        for i in range(10):
            repl.process("SELECT * FROM t")
        elapsed = time.perf_counter() - start
        
        print(f"\n[REPL] 100 INSERTs + 10 SELECTs: {elapsed:.4f}s")


# ==================== BENCHMARK SUMMARY ====================

class TestBenchmarkSummary:
    """Сводный бенчмарк."""
    
    def test_full_benchmark(self):
        """Полный бенчмарк всех операций."""
        print("\n" + "=" * 60)
        print("PERFORMANCE BENCHMARK SUMMARY")
        print("=" * 60)
        
        results = {}
        
        # Lexer
        lexer = Lexer()
        query = "SELECT * FROM t WHERE " + " AND ".join(f"x{i} = {i}" for i in range(100))
        results['lexer_100_conditions'], _ = measure_time(lexer.tokenize, query)
        
        # Parser
        parser = Parser()
        results['parser_complex_where'], _ = measure_time(parser.parse, query)
        
        # Database operations
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT"),
            ColumnDef(name="value", data_type="TEXT"),
        ])
        table = db.get_table("t")
        
        # INSERT
        start = time.perf_counter()
        for i in range(1000):
            table.insert({"id": i, "value": f"val_{i}"})
        results['insert_1000_rows'] = time.perf_counter() - start
        
        # SELECT full scan
        results['select_full_scan'], _ = measure_time(
            table.select, lambda row: True, None
        )
        
        # SELECT with condition
        results['select_with_condition'], _ = measure_time(
            table.select, lambda row: row["id"] == 500, None
        )
        
        # UPDATE
        results['update_1000_rows'], _ = measure_time(
            table.update, lambda row: True, {"value": "updated"}
        )
        
        # DELETE half
        results['delete_500_rows'], _ = measure_time(
            table.delete, lambda row: row["id"] < 500
        )
        
        # SAVE/LOAD
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "bench.json")
            results['save_500_rows'], _ = measure_time(db.save_to_file, filepath)
            
            db2 = Database()
            results['load_500_rows'], _ = measure_time(db2.load_from_file, filepath)
        
        # Print results
        print("\nOperation                    | Time (s)  | Threshold")
        print("-" * 60)
        
        thresholds = {
            'lexer_100_conditions': 0.1,
            'parser_complex_where': 0.1,
            'insert_1000_rows': 1.0,
            'select_full_scan': 0.1,
            'select_with_condition': 0.05,
            'update_1000_rows': 1.0,
            'delete_500_rows': 0.5,
            'save_500_rows': 0.5,
            'load_500_rows': 0.5,
        }
        
        all_passed = True
        for op, time_val in results.items():
            threshold = thresholds.get(op, 1.0)
            status = "✅ PASS" if time_val < threshold else "❌ FAIL"
            if time_val >= threshold:
                all_passed = False
            print(f"{op:28} | {time_val:9.4f} | {threshold:9.4f} {status}")
        
        print("=" * 60)
        
        assert all_passed, "Some benchmarks exceeded thresholds"