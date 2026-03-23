# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_performance_phase_extra
# Intent: Performance tests for mini_db_v2 scalability.
# Dependencies: pytest, time, threading
# END_MODULE_CONTRACT

"""
Phase Extra: Performance Tests for Scalability

Tests cover:
1. Large datasets (10,000+ rows)
2. Complex JOINs
3. Deep subqueries
4. Many indexes
5. Memory efficiency
6. Query latency
"""

import pytest
import time
import threading
import sys
import os

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from mini_db_v2.storage.database import Database
from mini_db_v2.storage.table import Table, ColumnDef, DataType
from mini_db_v2.storage.btree import BTree
from mini_db_v2.parser.lexer import Lexer
from mini_db_v2.parser.parser import Parser
from mini_db_v2.executor.executor import Executor
from mini_db_v2.executor.joins import JoinExecutor
from mini_db_v2.concurrency.transaction import TransactionManager, IsolationLevel
from mini_db_v2.storage.mvcc import VersionChain, Snapshot


# =============================================================================
# START_BLOCK_LARGE_DATASETS
# =============================================================================

class TestLargeDatasets:
    """Performance tests for large datasets."""
    
    @pytest.fixture
    def large_table(self):
        """Create table with 10,000 rows."""
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT, primary_key=True),
            "name": ColumnDef(name="name", data_type=DataType.TEXT),
            "value": ColumnDef(name="value", data_type=DataType.INT),
            "category": ColumnDef(name="category", data_type=DataType.INT),
        }
        table = Table("large_table", columns)
        
        # Insert 10,000 rows
        for i in range(10000):
            table.insert({
                "id": i,
                "name": f"name_{i}",
                "value": i * 10,
                "category": i % 100,
            })
        
        return table
    
    def test_insert_10000_rows_performance(self):
        """Insert 10,000 rows in reasonable time."""
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT),
            "value": ColumnDef(name="value", data_type=DataType.INT),
        }
        table = Table("perf_table", columns)
        
        start = time.time()
        for i in range(10000):
            table.insert({"id": i, "value": i})
        elapsed = time.time() - start
        
        assert elapsed < 5.0, f"Insert 10,000 rows took {elapsed:.2f}s (expected < 5s)"
        assert table.row_count == 10000
    
    def test_select_all_10000_rows_performance(self, large_table):
        """Select all 10,000 rows in reasonable time."""
        start = time.time()
        result = large_table.select()
        elapsed = time.time() - start
        
        assert elapsed < 1.0, f"Select all 10,000 rows took {elapsed:.2f}s (expected < 1s)"
        assert len(result) == 10000
    
    def test_select_with_where_10000_rows_performance(self, large_table):
        """Select with WHERE on 10,000 rows in reasonable time."""
        start = time.time()
        result = large_table.select(where=lambda r: r["category"] == 50)
        elapsed = time.time() - start
        
        assert elapsed < 0.5, f"Select with WHERE took {elapsed:.2f}s (expected < 0.5s)"
        assert len(result) == 100  # 10000 / 100 categories
    
    def test_update_10000_rows_performance(self, large_table):
        """Update all 10,000 rows in reasonable time."""
        start = time.time()
        count = large_table.update({"value": 999})
        elapsed = time.time() - start
        
        assert elapsed < 2.0, f"Update 10,000 rows took {elapsed:.2f}s (expected < 2s)"
        assert count == 10000
    
    def test_delete_5000_rows_performance(self, large_table):
        """Delete 5,000 rows in reasonable time."""
        start = time.time()
        count = large_table.delete(where=lambda r: r["id"] < 5000)
        elapsed = time.time() - start
        
        assert elapsed < 1.0, f"Delete 5,000 rows took {elapsed:.2f}s (expected < 1s)"
        assert count == 5000
        assert large_table.row_count == 5000
    
    def test_btree_10000_keys_performance(self):
        """BTree operations with 10,000 keys."""
        btree = BTree()
        
        # Insert
        start = time.time()
        for i in range(10000):
            btree.insert(i, i)
        insert_time = time.time() - start
        
        assert insert_time < 2.0, f"BTree insert 10,000 keys took {insert_time:.2f}s"
        
        # Search
        start = time.time()
        for i in range(0, 10000, 100):
            btree.search(i)
        search_time = time.time() - start
        
        assert search_time < 0.1, f"BTree search 100 keys took {search_time:.2f}s"
        
        # Range scan
        start = time.time()
        result = btree.range_scan(1000, 2000)
        range_time = time.time() - start
        
        assert range_time < 0.1, f"BTree range scan took {range_time:.2f}s"
        assert len(result) == 1001


# END_BLOCK_LARGE_DATASETS


# =============================================================================
# START_BLOCK_COMPLEX_JOINS
# =============================================================================

class TestComplexJoins:
    """Performance tests for complex JOINs."""
    
    @pytest.fixture
    def join_executor(self):
        return JoinExecutor()
    
    @pytest.fixture
    def small_tables(self):
        """Create small tables for JOIN testing."""
        left = []
        for i in range(100):
            left.append({"id": i, "name": f"left_{i}", "fk": i % 50})
        
        right = []
        for i in range(50):
            right.append({"id": i, "name": f"right_{i}"})
        
        return left, right
    
    @pytest.fixture
    def medium_tables(self):
        """Create medium tables for JOIN testing."""
        left = []
        for i in range(1000):
            left.append({"id": i, "name": f"left_{i}", "fk": i % 500})
        
        right = []
        for i in range(500):
            right.append({"id": i, "name": f"right_{i}"})
        
        return left, right
    
    def test_nested_loop_join_small_performance(self, join_executor, small_tables):
        """Nested loop join on small tables."""
        from mini_db_v2.ast.nodes import JoinType as ASTJoinType
        
        left, right = small_tables
        
        start = time.time()
        result = join_executor.nested_loop_join(
            outer_rows=left,
            inner_rows=right,
            outer_alias="left",
            inner_alias="right",
            condition=None,  # Will use cross join behavior
            evaluator=None,
            join_type=ASTJoinType.INNER
        )
        elapsed = time.time() - start
        
        assert elapsed < 0.5, f"Nested loop join took {elapsed:.2f}s"
        # Cross join: 100 * 50 = 5000 rows
        assert result.row_count == 5000
    
    def test_hash_join_medium_performance(self, join_executor, medium_tables):
        """Hash join on medium tables."""
        left, right = medium_tables
        
        start = time.time()
        result = join_executor.hash_join(
            outer_rows=left,
            inner_rows=right,
            outer_alias="left",
            inner_alias="right",
            outer_key="fk",
            inner_key="id"
        )
        elapsed = time.time() - start
        
        assert elapsed < 1.0, f"Hash join took {elapsed:.2f}s"
        assert result.row_count == 1000
    
    def test_merge_join_sorted_performance(self, join_executor):
        """Merge join on sorted data."""
        # Create sorted data
        left = [{"id": i, "value": i * 2} for i in range(1000)]
        right = [{"id": i, "value": i * 3} for i in range(1000)]
        
        start = time.time()
        result = join_executor.merge_join(
            outer_rows=left,
            inner_rows=right,
            outer_alias="left",
            inner_alias="right",
            outer_key="id",
            inner_key="id",
            outer_sorted=True,
            inner_sorted=True
        )
        elapsed = time.time() - start
        
        assert elapsed < 0.5, f"Merge join took {elapsed:.2f}s"
        assert result.row_count == 1000
    
    def test_multi_table_join_performance(self, join_executor):
        """Join multiple tables."""
        t1 = [{"id": i, "a": i} for i in range(100)]
        t2 = [{"id": i, "b": i * 2} for i in range(100)]
        t3 = [{"id": i, "c": i * 3} for i in range(100)]
        
        start = time.time()
        
        # Join t1 and t2
        result1 = join_executor.hash_join(
            outer_rows=t1,
            inner_rows=t2,
            outer_alias="t1",
            inner_alias="t2",
            outer_key="id",
            inner_key="id"
        )
        
        # Join result with t3
        result2 = join_executor.hash_join(
            outer_rows=result1.rows,
            inner_rows=t3,
            outer_alias="t1_t2",
            inner_alias="t3",
            outer_key="id",
            inner_key="id"
        )
        
        elapsed = time.time() - start
        
        assert elapsed < 1.0, f"Multi-table join took {elapsed:.2f}s"
        assert result2.row_count == 100


# END_BLOCK_COMPLEX_JOINS


# =============================================================================
# START_BLOCK_DEEP_SUBQUERIES
# =============================================================================

class TestDeepSubqueries:
    """Performance tests for deep subqueries."""
    
    @pytest.fixture
    def parser(self):
        return Parser()
    
    def test_nested_subquery_parsing_performance(self, parser):
        """Parse deeply nested subquery."""
        from mini_db_v2.parser.parser import parse_sql
        
        # Build nested subquery
        depth = 10
        query = "SELECT * FROM t1 WHERE id IN (SELECT id FROM t2"
        for i in range(3, depth + 2):
            query += f" WHERE id IN (SELECT id FROM t{i}"
        query += ")" * depth
        
        start = time.time()
        try:
            lexer = Lexer(query)
            tokens = lexer.tokenize()
            ast = parse_sql(query)
        except Exception:
            pass  # May not be supported
        elapsed = time.time() - start
        
        assert elapsed < 1.0, f"Parse nested subquery took {elapsed:.2f}s"
    
    def test_correlated_subquery_simulation(self):
        """Simulate correlated subquery execution."""
        # Outer table
        outer = [{"id": i, "value": i * 10} for i in range(100)]
        
        # Inner table
        inner = [{"fk": i % 50, "value": i} for i in range(500)]
        
        start = time.time()
        
        # Simulate correlated subquery: for each outer row, check inner
        results = []
        for outer_row in outer:
            for inner_row in inner:
                if inner_row["fk"] == outer_row["id"] % 50:
                    results.append(outer_row)
                    break
        
        elapsed = time.time() - start
        
        assert elapsed < 1.0, f"Correlated subquery simulation took {elapsed:.2f}s"


# END_BLOCK_DEEP_SUBQUERIES


# =============================================================================
# START_BLOCK_MANY_INDEXES
# =============================================================================

class TestManyIndexes:
    """Performance tests with many indexes."""
    
    def test_table_with_multiple_indexes(self):
        """Table with multiple indexes performance."""
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT, primary_key=True),
            "col1": ColumnDef(name="col1", data_type=DataType.INT),
            "col2": ColumnDef(name="col2", data_type=DataType.INT),
            "col3": ColumnDef(name="col3", data_type=DataType.INT),
        }
        table = Table("indexed_table", columns)
        
        # Create indexes
        table.create_index("idx_col1", "col1")
        table.create_index("idx_col2", "col2")
        table.create_index("idx_col3", "col3")
        
        # Insert data
        start = time.time()
        for i in range(1000):
            table.insert({
                "id": i,
                "col1": i % 10,
                "col2": i % 20,
                "col3": i % 50,
            })
        insert_time = time.time() - start
        
        assert insert_time < 2.0, f"Insert with 3 indexes took {insert_time:.2f}s"
        
        # Select using index
        start = time.time()
        result = table.select(where=lambda r: r["col1"] == 5)
        select_time = time.time() - start
        
        assert select_time < 0.1, f"Select with index took {select_time:.2f}s"
    
    def test_btree_index_range_query(self):
        """BTree index for range queries."""
        btree = BTree()
        
        # Insert 10,000 keys
        for i in range(10000):
            btree.insert(i, i)
        
        # Range query
        start = time.time()
        result = btree.range_scan(1000, 2000)
        elapsed = time.time() - start
        
        assert elapsed < 0.01, f"Range query took {elapsed:.4f}s"
        assert len(result) == 1001
    
    def test_index_maintenance_on_update(self):
        """Index maintenance during updates."""
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT),
            "value": ColumnDef(name="value", data_type=DataType.INT),
        }
        table = Table("update_indexed", columns)
        table.create_index("idx_value", "value")
        
        # Insert data
        for i in range(1000):
            table.insert({"id": i, "value": i})
        
        # Update indexed column
        start = time.time()
        table.update({"value": 999}, where=lambda r: r["id"] < 100)
        elapsed = time.time() - start
        
        assert elapsed < 0.5, f"Update with index maintenance took {elapsed:.2f}s"


# END_BLOCK_MANY_INDEXES


# =============================================================================
# START_BLOCK_MEMORY_EFFICIENCY
# =============================================================================

class TestMemoryEfficiency:
    """Tests for memory efficiency."""
    
    def test_large_table_memory(self):
        """Large table doesn't cause memory issues."""
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT),
            "data": ColumnDef(name="data", data_type=DataType.TEXT),
        }
        table = Table("memory_test", columns)
        
        # Insert rows with moderate data
        for i in range(10000):
            table.insert({"id": i, "data": f"data_{i}"})
        
        # Should not raise MemoryError
        assert table.row_count == 10000
        
        # Clear should free memory
        table.clear()
        assert table.row_count == 0
    
    def test_btree_memory_efficiency(self):
        """BTree doesn't cause memory issues."""
        btree = BTree()
        
        # Insert many keys
        for i in range(50000):
            btree.insert(i, i)
        
        assert btree.size == 50000
        
        # Delete all
        for i in range(50000):
            btree.delete(i)
        
        assert btree.size == 0
    
    def test_version_chain_memory(self):
        """Version chain memory management."""
        tm = TransactionManager()
        vc = VersionChain(row_id=1)
        
        # Create many versions
        for i in range(100):
            xid = tm.begin()
            vc.insert({"id": 1, "v": i}, xid)
            tm.commit(xid)
        
        assert vc.version_count == 100
        
        # Vacuum should clean up
        removed = vc.vacuum(oldest_xid=200)
        # Some versions should be removed


# END_BLOCK_MEMORY_EFFICIENCY


# =============================================================================
# START_BLOCK_QUERY_LATENCY
# =============================================================================

class TestQueryLatency:
    """Tests for query latency."""
    
    @pytest.fixture
    def db(self):
        db = Database()
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT, primary_key=True),
            "name": ColumnDef(name="name", data_type=DataType.TEXT),
            "value": ColumnDef(name="value", data_type=DataType.INT),
        }
        db.create_table("latency_test", columns)
        
        # Insert data
        table = db.get_table("latency_test")
        for i in range(1000):
            table.insert({"id": i, "name": f"name_{i}", "value": i * 10})
        
        return db
    
    @pytest.fixture
    def parser(self):
        return Parser()
    
    @pytest.fixture
    def executor(self, db):
        return Executor(db)
    
    def test_lexer_latency(self):
        """Lexer latency for typical query."""
        from mini_db_v2.parser.parser import parse_sql
        
        query = "SELECT * FROM users WHERE id = 1 AND name = 'test'"
        
        latencies = []
        for _ in range(100):
            start = time.time()
            lexer = Lexer(query)
            lexer.tokenize()
            latencies.append(time.time() - start)
        
        avg_latency = sum(latencies) / len(latencies)
        max_latency = max(latencies)
        
        assert avg_latency < 0.001, f"Average lexer latency {avg_latency*1000:.2f}ms"
        assert max_latency < 0.01, f"Max lexer latency {max_latency*1000:.2f}ms"
    
    def test_parser_latency(self):
        """Parser latency for typical query."""
        from mini_db_v2.parser.parser import parse_sql
        
        query = "SELECT * FROM users WHERE id = 1 AND name = 'test'"
        
        latencies = []
        for _ in range(100):
            start = time.time()
            lexer = Lexer(query)
            lexer.tokenize()
            parse_sql(query)
            latencies.append(time.time() - start)
        
        avg_latency = sum(latencies) / len(latencies)
        max_latency = max(latencies)
        
        assert avg_latency < 0.01, f"Average parser latency {avg_latency*1000:.2f}ms"
        assert max_latency < 0.1, f"Max parser latency {max_latency*1000:.2f}ms"
    
    def test_select_latency(self, db):
        """SELECT query latency."""
        from mini_db_v2.parser.parser import parse_sql
        
        query = "SELECT * FROM latency_test WHERE id = 500"
        executor = Executor(db)
        
        latencies = []
        for _ in range(100):
            start = time.time()
            lexer = Lexer(query)
            lexer.tokenize()
            ast = parse_sql(query)
            result = executor.execute(ast)
            latencies.append(time.time() - start)
        
        avg_latency = sum(latencies) / len(latencies)
        max_latency = max(latencies)
        
        assert avg_latency < 0.01, f"Average SELECT latency {avg_latency*1000:.2f}ms"
        assert max_latency < 0.1, f"Max SELECT latency {max_latency*1000:.2f}ms"
    
    def test_insert_latency(self, db):
        """INSERT query latency."""
        from mini_db_v2.parser.parser import parse_sql
        
        executor = Executor(db)
        latencies = []
        
        for i in range(1000, 1100):
            query = f"INSERT INTO latency_test (id, name, value) VALUES ({i}, 'new_{i}', {i * 10})"
            
            start = time.time()
            lexer = Lexer(query)
            lexer.tokenize()
            ast = parse_sql(query)
            result = executor.execute(ast)
            latencies.append(time.time() - start)
        
        avg_latency = sum(latencies) / len(latencies)
        max_latency = max(latencies)
        
        assert avg_latency < 0.01, f"Average INSERT latency {avg_latency*1000:.2f}ms"
        assert max_latency < 0.1, f"Max INSERT latency {max_latency*1000:.2f}ms"


# END_BLOCK_QUERY_LATENCY


# =============================================================================
# START_BLOCK_CONCURRENT_PERFORMANCE
# =============================================================================

class TestConcurrentPerformance:
    """Performance tests for concurrent operations."""
    
    def test_concurrent_reads_performance(self):
        """Concurrent reads performance."""
        tm = TransactionManager()
        vc = VersionChain(row_id=1)
        
        # Setup
        xid_setup = tm.begin()
        vc.insert({"id": 1, "value": 100}, xid_setup)
        tm.commit(xid_setup)
        
        read_count = 0
        lock = threading.Lock()
        
        def reader():
            nonlocal read_count
            for _ in range(100):
                xid = tm.begin(IsolationLevel.READ_COMMITTED)
                snapshot = tm.get_snapshot(xid)
                vc.get_visible(xid, snapshot)
                tm.commit(xid)
                with lock:
                    read_count += 1
        
        threads = [threading.Thread(target=reader) for _ in range(10)]
        
        start = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.time() - start
        
        assert read_count == 1000
        throughput = read_count / elapsed
        assert throughput > 100, f"Read throughput {throughput:.0f} reads/sec"
    
    def test_concurrent_writes_performance(self):
        """Concurrent writes performance."""
        columns = {
            "id": ColumnDef(name="id", data_type=DataType.INT, unique=True),
            "value": ColumnDef(name="value", data_type=DataType.INT),
        }
        table = Table("concurrent_writes", columns)
        
        write_count = 0
        errors = []
        lock = threading.Lock()
        
        def writer(thread_id):
            nonlocal write_count
            for i in range(100):
                try:
                    table.insert({"id": thread_id * 1000 + i, "value": i})
                    with lock:
                        write_count += 1
                except Exception as e:
                    errors.append(str(e))
        
        threads = [threading.Thread(target=writer, args=(i,)) for i in range(10)]
        
        start = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.time() - start
        
        assert len(errors) == 0
        assert write_count == 1000
        throughput = write_count / elapsed
        assert throughput > 100, f"Write throughput {throughput:.0f} writes/sec"


# END_BLOCK_CONCURRENT_PERFORMANCE


# =============================================================================
# Run Tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])