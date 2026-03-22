# START_MODULE_CONTRACT
# Module: tests.test_indexes
# Intent: Тесты для Phase 6: Indexes (CREATE INDEX, index usage, rebuild on LOAD)
# END_MODULE_CONTRACT

import os
import tempfile
import pytest

from mini_db.parser.parser import Parser, ParseError
from mini_db.executor.executor import Executor, ExecutionResult
from mini_db.storage.database import Database
from mini_db.storage.index import HashIndex
from mini_db.ast.nodes import CreateIndexNode


# ==================== PARSER TESTS ====================

class TestCreateIndexParser:
    """Тесты парсинга CREATE INDEX"""
    
    def test_parse_create_index_basic(self):
        """CREATE INDEX idx ON t (col)"""
        parser = Parser()
        ast = parser.parse("CREATE INDEX idx ON t (col);")
        
        assert isinstance(ast, CreateIndexNode)
        assert ast.name == "idx"
        assert ast.table == "t"
        assert ast.column == "col"
    
    def test_parse_create_index_with_semicolon(self):
        """CREATE INDEX с точкой с запятой"""
        parser = Parser()
        ast = parser.parse("CREATE INDEX my_idx ON users (email);")
        
        assert isinstance(ast, CreateIndexNode)
        assert ast.name == "my_idx"
        assert ast.table == "users"
        assert ast.column == "email"
    
    def test_parse_create_index_case_insensitive(self):
        """CREATE INDEX case-insensitive keywords"""
        parser = Parser()
        ast = parser.parse("create index IdX on users (CoL);")
        
        assert isinstance(ast, CreateIndexNode)
        assert ast.name == "IdX"  # identifier preserves case
        assert ast.table == "users"
        assert ast.column == "CoL"
    
    def test_parse_create_index_missing_on(self):
        """CREATE INDEX без ON — ошибка"""
        parser = Parser()
        with pytest.raises(ParseError) as exc_info:
            parser.parse("CREATE INDEX idx t (col);")
        
        assert "ON" in str(exc_info.value)
    
    def test_parse_create_index_missing_parens(self):
        """CREATE INDEX без скобок — ошибка"""
        parser = Parser()
        with pytest.raises(ParseError) as exc_info:
            parser.parse("CREATE INDEX idx ON t col;")
        
        assert "(" in str(exc_info.value) or "Expected" in str(exc_info.value)
    
    def test_parse_create_index_missing_column(self):
        """CREATE INDEX без колонки — ошибка"""
        parser = Parser()
        with pytest.raises(ParseError) as exc_info:
            parser.parse("CREATE INDEX idx ON t ();")
        
        assert "column" in str(exc_info.value).lower() or "Expected" in str(exc_info.value)


# ==================== EXECUTOR TESTS ====================

class TestCreateIndexExecutor:
    """Тесты выполнения CREATE INDEX"""
    
    def test_execute_create_index_success(self):
        """CREATE INDEX на существующей таблице"""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        # Create table
        ast = parser.parse("CREATE TABLE t (id INT, name TEXT);")
        executor.execute(ast, db)
        
        # Create index
        ast = parser.parse("CREATE INDEX idx_id ON t (id);")
        result = executor.execute(ast, db)
        
        assert result.success == True
        assert "idx_id" in result.message
        assert "created" in result.message.lower()
    
    def test_execute_create_index_nonexistent_table(self):
        """CREATE INDEX на несуществующей таблице — ошибка"""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        ast = parser.parse("CREATE INDEX idx ON nonexistent (col);")
        result = executor.execute(ast, db)
        
        assert result.success == False
        assert "does not exist" in result.error.lower()
    
    def test_execute_create_index_nonexistent_column(self):
        """CREATE INDEX на несуществующей колонке — ошибка"""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        ast = parser.parse("CREATE TABLE t (id INT);")
        executor.execute(ast, db)
        
        ast = parser.parse("CREATE INDEX idx ON t (nonexistent);")
        result = executor.execute(ast, db)
        
        assert result.success == False
        assert "does not exist" in result.error.lower()
    
    def test_execute_create_index_duplicate_name(self):
        """CREATE INDEX с дублирующимся именем — ошибка"""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        ast = parser.parse("CREATE TABLE t (id INT, name TEXT);")
        executor.execute(ast, db)
        
        ast = parser.parse("CREATE INDEX idx ON t (id);")
        result = executor.execute(ast, db)
        assert result.success == True
        
        # Duplicate index name
        ast = parser.parse("CREATE INDEX idx ON t (name);")
        result = executor.execute(ast, db)
        
        assert result.success == False
        assert "already exists" in result.error.lower()


# ==================== INDEX USAGE TESTS ====================

class TestIndexUsage:
    """Тесты использования индекса при SELECT"""
    
    def test_select_with_index_uses_index(self):
        """SELECT с WHERE col = X использует индекс"""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        # Create table and insert data
        parser.parse("CREATE TABLE t (id INT, name TEXT);")
        ast = parser.parse("CREATE TABLE t (id INT, name TEXT);")
        executor.execute(ast, db)
        
        for i in range(10):
            ast = parser.parse(f"INSERT INTO t (id, name) VALUES ({i}, 'name{i}');")
            executor.execute(ast, db)
        
        # Create index
        ast = parser.parse("CREATE INDEX idx_id ON t (id);")
        executor.execute(ast, db)
        
        # Select with index
        ast = parser.parse("SELECT * FROM t WHERE id = 5;")
        result = executor.execute(ast, db)
        
        assert result.success == True
        assert len(result.data) == 1
        assert result.data[0]["id"] == 5
        assert result.data[0]["name"] == "name5"
    
    def test_select_with_index_multiple_matches(self):
        """SELECT с индексом находит несколько строк"""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        ast = parser.parse("CREATE TABLE t (category INT, name TEXT);")
        executor.execute(ast, db)
        
        # Insert rows with same category
        for i in range(5):
            ast = parser.parse(f"INSERT INTO t (category, name) VALUES (1, 'item{i}');")
            executor.execute(ast, db)
        for i in range(5):
            ast = parser.parse(f"INSERT INTO t (category, name) VALUES (2, 'item{i+5}');")
            executor.execute(ast, db)
        
        # Create index
        ast = parser.parse("CREATE INDEX idx_cat ON t (category);")
        executor.execute(ast, db)
        
        # Select with index
        ast = parser.parse("SELECT * FROM t WHERE category = 1;")
        result = executor.execute(ast, db)
        
        assert result.success == True
        assert len(result.data) == 5
        for row in result.data:
            assert row["category"] == 1
    
    def test_select_without_index_full_scan(self):
        """SELECT без индекса использует full scan"""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        ast = parser.parse("CREATE TABLE t (id INT, name TEXT);")
        executor.execute(ast, db)
        
        for i in range(5):
            ast = parser.parse(f"INSERT INTO t (id, name) VALUES ({i}, 'name{i}');")
            executor.execute(ast, db)
        
        # No index created
        ast = parser.parse("SELECT * FROM t WHERE id = 3;")
        result = executor.execute(ast, db)
        
        assert result.success == True
        assert len(result.data) == 1
        assert result.data[0]["id"] == 3
    
    def test_select_with_index_no_match(self):
        """SELECT с индексом не находит несуществующее значение"""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        ast = parser.parse("CREATE TABLE t (id INT);")
        executor.execute(ast, db)
        
        for i in range(5):
            ast = parser.parse(f"INSERT INTO t (id) VALUES ({i});")
            executor.execute(ast, db)
        
        ast = parser.parse("CREATE INDEX idx ON t (id);")
        executor.execute(ast, db)
        
        ast = parser.parse("SELECT * FROM t WHERE id = 999;")
        result = executor.execute(ast, db)
        
        assert result.success == True
        assert len(result.data) == 0
    
    def test_select_with_other_comparison_no_index(self):
        """SELECT с !=, <, > не использует индекс"""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        ast = parser.parse("CREATE TABLE t (id INT);")
        executor.execute(ast, db)
        
        for i in range(5):
            ast = parser.parse(f"INSERT INTO t (id) VALUES ({i});")
            executor.execute(ast, db)
        
        ast = parser.parse("CREATE INDEX idx ON t (id);")
        executor.execute(ast, db)
        
        # != comparison - should still work but without index optimization
        ast = parser.parse("SELECT * FROM t WHERE id != 2;")
        result = executor.execute(ast, db)
        
        assert result.success == True
        assert len(result.data) == 4
        
        # < comparison
        ast = parser.parse("SELECT * FROM t WHERE id < 3;")
        result = executor.execute(ast, db)
        
        assert result.success == True
        assert len(result.data) == 3


# ==================== INDEX MAINTENANCE TESTS ====================

class TestIndexMaintenance:
    """Тесты поддержки индексов при INSERT, UPDATE, DELETE"""
    
    def test_index_updated_on_insert(self):
        """Индекс обновляется при INSERT"""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        ast = parser.parse("CREATE TABLE t (id INT);")
        executor.execute(ast, db)
        
        ast = parser.parse("CREATE INDEX idx ON t (id);")
        executor.execute(ast, db)
        
        # Insert after index creation
        ast = parser.parse("INSERT INTO t (id) VALUES (42);")
        executor.execute(ast, db)
        
        # Find via index
        ast = parser.parse("SELECT * FROM t WHERE id = 42;")
        result = executor.execute(ast, db)
        
        assert result.success == True
        assert len(result.data) == 1
        assert result.data[0]["id"] == 42
    
    def test_index_updated_on_update(self):
        """Индекс обновляется при UPDATE"""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        ast = parser.parse("CREATE TABLE t (id INT, val INT);")
        executor.execute(ast, db)
        
        ast = parser.parse("INSERT INTO t (id, val) VALUES (1, 100);")
        executor.execute(ast, db)
        
        ast = parser.parse("CREATE INDEX idx ON t (val);")
        executor.execute(ast, db)
        
        # Update indexed column
        ast = parser.parse("UPDATE t SET val = 200 WHERE id = 1;")
        result = executor.execute(ast, db)
        assert result.success == True
        
        # Find via index with new value
        ast = parser.parse("SELECT * FROM t WHERE val = 200;")
        result = executor.execute(ast, db)
        
        assert result.success == True
        assert len(result.data) == 1
        assert result.data[0]["val"] == 200
        
        # Old value should not be found
        ast = parser.parse("SELECT * FROM t WHERE val = 100;")
        result = executor.execute(ast, db)
        
        assert result.success == True
        assert len(result.data) == 0
    
    def test_index_updated_on_delete(self):
        """Индекс обновляется при DELETE"""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        ast = parser.parse("CREATE TABLE t (id INT);")
        executor.execute(ast, db)
        
        ast = parser.parse("INSERT INTO t (id) VALUES (1);")
        executor.execute(ast, db)
        ast = parser.parse("INSERT INTO t (id) VALUES (2);")
        executor.execute(ast, db)
        
        ast = parser.parse("CREATE INDEX idx ON t (id);")
        executor.execute(ast, db)
        
        # Delete one row
        ast = parser.parse("DELETE FROM t WHERE id = 1;")
        result = executor.execute(ast, db)
        assert result.success == True
        
        # Deleted value should not be found
        ast = parser.parse("SELECT * FROM t WHERE id = 1;")
        result = executor.execute(ast, db)
        
        assert result.success == True
        assert len(result.data) == 0
        
        # Other value should still be found
        ast = parser.parse("SELECT * FROM t WHERE id = 2;")
        result = executor.execute(ast, db)
        
        assert result.success == True
        assert len(result.data) == 1


# ==================== INDEX REBUILD ON LOAD TESTS ====================

class TestIndexRebuildOnLoad:
    """Тесты перестроения индексов при LOAD"""
    
    def test_index_rebuild_on_load(self):
        """Индексы перестраиваются при LOAD"""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "test_db.json")
            
            db = Database()
            executor = Executor()
            parser = Parser()
            
            # Create table and index
            ast = parser.parse("CREATE TABLE t (id INT, name TEXT);")
            executor.execute(ast, db)
            
            for i in range(5):
                ast = parser.parse(f"INSERT INTO t (id, name) VALUES ({i}, 'name{i}');")
                executor.execute(ast, db)
            
            ast = parser.parse("CREATE INDEX idx_id ON t (id);")
            executor.execute(ast, db)
            
            # Save
            ast = parser.parse(f"SAVE '{filepath}';")
            result = executor.execute(ast, db)
            assert result.success == True
            
            # Clear and load
            db.clear()
            ast = parser.parse(f"LOAD '{filepath}';")
            result = executor.execute(ast, db)
            assert result.success == True
            
            # Index should be rebuilt and work
            ast = parser.parse("SELECT * FROM t WHERE id = 3;")
            result = executor.execute(ast, db)
            
            assert result.success == True
            assert len(result.data) == 1
            assert result.data[0]["id"] == 3
            assert result.data[0]["name"] == "name3"
    
    def test_index_persisted_in_json(self):
        """Индексы сохраняются в JSON"""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "test_db.json")
            
            db = Database()
            executor = Executor()
            parser = Parser()
            
            ast = parser.parse("CREATE TABLE t (id INT);")
            executor.execute(ast, db)
            
            ast = parser.parse("CREATE INDEX idx_id ON t (id);")
            executor.execute(ast, db)
            
            # Save
            ast = parser.parse(f"SAVE '{filepath}';")
            executor.execute(ast, db)
            
            # Read JSON and check index is saved
            import json
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            assert "t" in data["tables"]
            table_data = data["tables"]["t"]
            assert "indexes" in table_data
            assert "idx_id" in table_data["indexes"]
            assert table_data["indexes"]["idx_id"]["column"] == "id"


# ==================== HASH INDEX UNIT TESTS ====================

class TestHashIndex:
    """Unit-тесты для HashIndex"""
    
    def test_hash_index_add_lookup(self):
        """HashIndex.add и lookup"""
        index = HashIndex("id")
        
        index.add(1, 0)
        index.add(2, 1)
        index.add(1, 2)  # Duplicate value, different row
        
        assert index.lookup(1) == {0, 2}
        assert index.lookup(2) == {1}
        assert index.lookup(999) == set()
    
    def test_hash_index_remove(self):
        """HashIndex.remove"""
        index = HashIndex("id")
        
        index.add(1, 0)
        index.add(1, 1)
        index.remove(1, 0)
        
        assert index.lookup(1) == {1}
    
    def test_hash_index_contains(self):
        """HashIndex.contains"""
        index = HashIndex("id")
        
        index.add(1, 0)
        
        assert index.contains(1) == True
        assert index.contains(999) == False
    
    def test_hash_index_rebuild(self):
        """HashIndex.rebuild"""
        index = HashIndex("id")
        
        rows = [
            {"id": 1, "name": "a"},
            {"id": 2, "name": "b"},
            {"id": 1, "name": "c"},  # Duplicate id
        ]
        
        index.rebuild(rows, "id")
        
        assert index.lookup(1) == {0, 2}
        assert index.lookup(2) == {1}
    
    def test_hash_index_rebuild_with_nulls(self):
        """HashIndex.rebuild игнорирует NULL"""
        index = HashIndex("id")
        
        rows = [
            {"id": 1, "name": "a"},
            {"id": None, "name": "b"},  # NULL should be ignored
            {"id": 2, "name": "c"},
        ]
        
        index.rebuild(rows, "id")
        
        assert index.lookup(1) == {0}
        assert index.lookup(2) == {2}
        assert index.contains(None) == False


# ==================== TABLE INDEX METHODS TESTS ====================

class TestTableIndexMethods:
    """Тесты методов Table для работы с индексами"""
    
    def test_table_create_index(self):
        """Table.create_index"""
        from mini_db.storage.table import Table
        from mini_db.ast.nodes import ColumnDef
        
        table = Table("t", [
            ColumnDef(name="id", data_type="INT", unique=False),
            ColumnDef(name="name", data_type="TEXT", unique=False),
        ])
        
        success, error = table.create_index("idx_id", "id")
        
        assert success == True
        assert error is None
        assert "idx_id" in table.indexes
    
    def test_table_create_index_duplicate_name(self):
        """Table.create_index с дублирующимся именем"""
        from mini_db.storage.table import Table
        from mini_db.ast.nodes import ColumnDef
        
        table = Table("t", [
            ColumnDef(name="id", data_type="INT", unique=False),
        ])
        
        table.create_index("idx", "id")
        success, error = table.create_index("idx", "id")
        
        assert success == False
        assert "already exists" in error.lower()
    
    def test_table_create_index_nonexistent_column(self):
        """Table.create_index на несуществующей колонке"""
        from mini_db.storage.table import Table
        from mini_db.ast.nodes import ColumnDef
        
        table = Table("t", [
            ColumnDef(name="id", data_type="INT", unique=False),
        ])
        
        success, error = table.create_index("idx", "nonexistent")
        
        assert success == False
        assert "does not exist" in error.lower()
    
    def test_table_get_index_for_column(self):
        """Table.get_index_for_column"""
        from mini_db.storage.table import Table
        from mini_db.ast.nodes import ColumnDef
        
        table = Table("t", [
            ColumnDef(name="id", data_type="INT", unique=False),
            ColumnDef(name="name", data_type="TEXT", unique=False),
        ])
        
        table.create_index("idx_id", "id")
        
        index = table.get_index_for_column("id")
        assert index is not None
        assert index.column == "id"
        
        index = table.get_index_for_column("name")
        assert index is None
    
    def test_table_rebuild_all_indexes(self):
        """Table.rebuild_all_indexes"""
        from mini_db.storage.table import Table
        from mini_db.ast.nodes import ColumnDef
        
        table = Table("t", [
            ColumnDef(name="id", data_type="INT", unique=False),
        ])
        
        # Insert data before index
        table.insert({"id": 1})
        table.insert({"id": 2})
        
        # Create index (should be populated)
        table.create_index("idx", "id")
        
        # Verify index is populated
        index = table.get_index_for_column("id")
        assert index.lookup(1) == {0}
        assert index.lookup(2) == {1}


# ==================== ADVERSARIAL TESTS ====================

class TestIndexAdversarial:
    """Адверсарные тесты для индексов"""
    
    def test_index_with_null_values(self):
        """Индекс с NULL значениями"""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        ast = parser.parse("CREATE TABLE t (id INT, val INT);")
        executor.execute(ast, db)
        
        ast = parser.parse("INSERT INTO t (id, val) VALUES (1, NULL);")
        executor.execute(ast, db)
        ast = parser.parse("INSERT INTO t (id, val) VALUES (2, 100);")
        executor.execute(ast, db)
        
        ast = parser.parse("CREATE INDEX idx ON t (val);")
        executor.execute(ast, db)
        
        # NULL should not be in index
        ast = parser.parse("SELECT * FROM t WHERE val = NULL;")
        result = executor.execute(ast, db)
        
        # NULL comparison returns False (NULL semantics)
        assert result.success == True
        assert len(result.data) == 0
    
    def test_index_on_text_column(self):
        """Индекс на TEXT колонке"""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        ast = parser.parse("CREATE TABLE t (name TEXT);")
        executor.execute(ast, db)
        
        ast = parser.parse("INSERT INTO t (name) VALUES ('alice');")
        executor.execute(ast, db)
        ast = parser.parse("INSERT INTO t (name) VALUES ('bob');")
        executor.execute(ast, db)
        
        ast = parser.parse("CREATE INDEX idx ON t (name);")
        executor.execute(ast, db)
        
        ast = parser.parse("SELECT * FROM t WHERE name = 'alice';")
        result = executor.execute(ast, db)
        
        assert result.success == True
        assert len(result.data) == 1
        assert result.data[0]["name"] == "alice"
    
    def test_index_on_bool_column(self):
        """Индекс на BOOL колонке"""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        ast = parser.parse("CREATE TABLE t (active BOOL);")
        executor.execute(ast, db)
        
        ast = parser.parse("INSERT INTO t (active) VALUES (true);")
        executor.execute(ast, db)
        ast = parser.parse("INSERT INTO t (active) VALUES (false);")
        executor.execute(ast, db)
        ast = parser.parse("INSERT INTO t (active) VALUES (true);")
        executor.execute(ast, db)
        
        ast = parser.parse("CREATE INDEX idx ON t (active);")
        executor.execute(ast, db)
        
        ast = parser.parse("SELECT * FROM t WHERE active = true;")
        result = executor.execute(ast, db)
        
        assert result.success == True
        assert len(result.data) == 2
    
    def test_multiple_indexes_on_same_table(self):
        """Несколько индексов на одной таблице"""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        ast = parser.parse("CREATE TABLE t (id INT, name TEXT, category INT);")
        executor.execute(ast, db)
        
        ast = parser.parse("CREATE INDEX idx_id ON t (id);")
        result = executor.execute(ast, db)
        assert result.success == True
        
        ast = parser.parse("CREATE INDEX idx_name ON t (name);")
        result = executor.execute(ast, db)
        assert result.success == True
        
        ast = parser.parse("CREATE INDEX idx_cat ON t (category);")
        result = executor.execute(ast, db)
        assert result.success == True
        
        # Verify all indexes work
        for i in range(5):
            ast = parser.parse(f"INSERT INTO t (id, name, category) VALUES ({i}, 'name{i}', {i % 2});")
            executor.execute(ast, db)
        
        ast = parser.parse("SELECT * FROM t WHERE id = 3;")
        result = executor.execute(ast, db)
        assert len(result.data) == 1
        
        ast = parser.parse("SELECT * FROM t WHERE name = 'name2';")
        result = executor.execute(ast, db)
        assert len(result.data) == 1
        
        ast = parser.parse("SELECT * FROM t WHERE category = 0;")
        result = executor.execute(ast, db)
        assert len(result.data) == 3  # 0, 2, 4
    
    def test_index_with_special_characters_in_values(self):
        """Индекс со специальными символами в значениях"""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        ast = parser.parse("CREATE TABLE t (name TEXT);")
        executor.execute(ast, db)
        
        # Insert values with special characters
        ast = parser.parse("INSERT INTO t (name) VALUES ('test@email.com');")
        executor.execute(ast, db)
        ast = parser.parse("INSERT INTO t (name) VALUES ('user-name_123');")
        executor.execute(ast, db)
        
        ast = parser.parse("CREATE INDEX idx ON t (name);")
        executor.execute(ast, db)
        
        ast = parser.parse("SELECT * FROM t WHERE name = 'test@email.com';")
        result = executor.execute(ast, db)
        
        assert result.success == True
        assert len(result.data) == 1
        assert result.data[0]["name"] == "test@email.com"


# ==================== INTEGRATION TESTS ====================

class TestIndexIntegration:
    """Интеграционные тесты индексов"""
    
    def test_full_workflow_with_indexes(self):
        """Полный workflow с индексами"""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "workflow.json")
            
            db = Database()
            executor = Executor()
            parser = Parser()
            
            # Create table
            ast = parser.parse("CREATE TABLE users (id INT, email TEXT, active BOOL);")
            executor.execute(ast, db)
            
            # Insert data
            for i in range(10):
                ast = parser.parse(f"INSERT INTO users (id, email, active) VALUES ({i}, 'user{i}@test.com', {i % 2 == 0});")
                executor.execute(ast, db)
            
            # Create indexes
            ast = parser.parse("CREATE INDEX idx_id ON users (id);")
            executor.execute(ast, db)
            ast = parser.parse("CREATE INDEX idx_email ON users (email);")
            executor.execute(ast, db)
            
            # Query with index
            ast = parser.parse("SELECT * FROM users WHERE id = 5;")
            result = executor.execute(ast, db)
            assert len(result.data) == 1
            assert result.data[0]["email"] == "user5@test.com"
            
            # Update
            ast = parser.parse("UPDATE users SET email = 'updated@test.com' WHERE id = 5;")
            executor.execute(ast, db)
            
            # Query with new value
            ast = parser.parse("SELECT * FROM users WHERE email = 'updated@test.com';")
            result = executor.execute(ast, db)
            assert len(result.data) == 1
            
            # Delete
            ast = parser.parse("DELETE FROM users WHERE id = 5;")
            executor.execute(ast, db)
            
            # Verify deleted
            ast = parser.parse("SELECT * FROM users WHERE id = 5;")
            result = executor.execute(ast, db)
            assert len(result.data) == 0
            
            # Save
            ast = parser.parse(f"SAVE '{filepath}';")
            result = executor.execute(ast, db)
            assert result.success == True
            
            # Load into new database
            db2 = Database()
            ast = parser.parse(f"LOAD '{filepath}';")
            result = executor.execute(ast, db2)
            assert result.success == True
            
            # Verify indexes work after load
            ast = parser.parse("SELECT * FROM users WHERE id = 3;")
            result = executor.execute(ast, db2)
            assert len(result.data) == 1
            assert result.data[0]["email"] == "user3@test.com"