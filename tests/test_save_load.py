# START_MODULE_CONTRACT
# Module: tests.test_save_load
# Intent: Тесты для SAVE/LOAD функциональности (Phase 5).
#         Проверка сериализации/десериализации базы данных.
# END_MODULE_CONTRACT

import json
import os
import tempfile
from pathlib import Path

import pytest

from mini_db.ast.nodes import ColumnDef
from mini_db.executor.executor import Executor
from mini_db.parser.parser import Parser
from mini_db.storage.database import Database
from mini_db.storage.table import Table


class TestSaveLoadParser:
    """Тесты парсинга SAVE/LOAD/EXIT команд."""

    def test_parse_save_basic(self):
        """Парсинг SAVE с путём к файлу."""
        parser = Parser()
        ast = parser.parse("SAVE 'test.json';")
        assert ast.filepath == "test.json"

    def test_parse_save_with_path(self):
        """Парсинг SAVE с полным путём."""
        parser = Parser()
        ast = parser.parse("SAVE '/path/to/database.json';")
        assert ast.filepath == "/path/to/database.json"

    def test_parse_save_windows_path(self):
        """Парсинг SAVE с Windows путём."""
        parser = Parser()
        ast = parser.parse(r"SAVE 'C:\\Users\\test\\db.json';")
        # Windows path with escaped backslashes
        assert "db.json" in ast.filepath

    def test_parse_load_basic(self):
        """Парсинг LOAD с путём к файлу."""
        parser = Parser()
        ast = parser.parse("LOAD 'test.json';")
        assert ast.filepath == "test.json"

    def test_parse_load_with_path(self):
        """Парсинг LOAD с полным путём."""
        parser = Parser()
        ast = parser.parse("LOAD '/path/to/database.json';")
        assert ast.filepath == "/path/to/database.json"

    def test_parse_exit(self):
        """Парсинг EXIT команды."""
        parser = Parser()
        ast = parser.parse("EXIT;")
        assert ast.__class__.__name__ == "ExitNode"

    def test_parse_save_missing_filepath(self):
        """SAVE без пути к файлу - ошибка."""
        parser = Parser()
        with pytest.raises(Exception):  # ParseError
            parser.parse("SAVE;")

    def test_parse_load_missing_filepath(self):
        """LOAD без пути к файлу - ошибка."""
        parser = Parser()
        with pytest.raises(Exception):  # ParseError
            parser.parse("LOAD;")


class TestDatabaseSerialization:
    """Тесты сериализации/десериализации Database."""

    def test_to_dict_empty_database(self):
        """Сериализация пустой базы."""
        db = Database()
        data = db.to_dict()
        assert data == {"tables": {}}

    def test_from_dict_empty_database(self):
        """Десериализация пустой базы."""
        db = Database()
        db.from_dict({"tables": {}})
        assert len(db.list_tables()) == 0

    def test_to_dict_single_table(self):
        """Сериализация базы с одной таблицей."""
        db = Database()
        db.create_table("users", [
            ColumnDef(name="id", data_type="INT", unique=True),
            ColumnDef(name="name", data_type="TEXT", unique=False)
        ])
        
        table = db.get_table("users")
        table.insert({"id": 1, "name": "Alice"})
        table.insert({"id": 2, "name": "Bob"})
        
        data = db.to_dict()
        
        assert "tables" in data
        assert "users" in data["tables"]
        assert data["tables"]["users"]["name"] == "users"
        assert len(data["tables"]["users"]["rows"]) == 2

    def test_from_dict_single_table(self):
        """Десериализация базы с одной таблицей."""
        db = Database()
        
        # Create table and insert data
        db.create_table("users", [
            ColumnDef(name="id", data_type="INT", unique=True),
            ColumnDef(name="name", data_type="TEXT", unique=False)
        ])
        table = db.get_table("users")
        table.insert({"id": 1, "name": "Alice"})
        
        # Serialize
        data = db.to_dict()
        
        # Create new database and deserialize
        db2 = Database()
        db2.from_dict(data)
        
        assert db2.table_exists("users")
        table2 = db2.get_table("users")
        result = table2.select(predicate=lambda row: True)  # select all rows
        rows = result.data
        assert len(rows) == 1
        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "Alice"

    def test_to_dict_multiple_tables(self):
        """Сериализация базы с несколькими таблицами."""
        db = Database()
        db.create_table("users", [
            ColumnDef(name="id", data_type="INT", unique=True)
        ])
        db.create_table("products", [
            ColumnDef(name="id", data_type="INT", unique=True)
        ])
        
        data = db.to_dict()
        
        assert "users" in data["tables"]
        assert "products" in data["tables"]

    def test_from_dict_multiple_tables(self):
        """Десериализация базы с несколькими таблицами."""
        db = Database()
        db.create_table("users", [
            ColumnDef(name="id", data_type="INT", unique=True)
        ])
        db.create_table("products", [
            ColumnDef(name="id", data_type="INT", unique=True)
        ])
        
        data = db.to_dict()
        
        db2 = Database()
        db2.from_dict(data)
        
        assert db2.table_exists("users")
        assert db2.table_exists("products")


class TestTableSerialization:
    """Тесты сериализации/десериализации Table."""

    def test_table_to_dict(self):
        """Сериализация таблицы."""
        table = Table("test", [
            ColumnDef(name="id", data_type="INT", unique=True),
            ColumnDef(name="value", data_type="TEXT", unique=False)
        ])
        table.insert({"id": 1, "value": "test1"})
        table.insert({"id": 2, "value": "test2"})
        
        data = table.to_dict()
        
        assert data["name"] == "test"
        assert len(data["columns"]) == 2
        assert len(data["rows"]) == 2

    def test_table_from_dict(self):
        """Десериализация таблицы."""
        table = Table("test", [])
        
        data = {
            "name": "test",
            "columns": [
                {"name": "id", "data_type": "INT", "unique": True},
                {"name": "value", "data_type": "TEXT", "unique": False}
            ],
            "rows": [
                {"id": 1, "value": "test1"},
                {"id": 2, "value": "test2"}
            ]
        }
        
        table.from_dict(data)
        
        assert table.name == "test"
        assert len(table.columns) == 2
        result = table.select(predicate=lambda row: True)  # select all rows
        rows = result.data
        assert len(rows) == 2

    def test_table_serialization_preserves_types(self):
        """Сериализация сохраняет типы данных."""
        table = Table("test", [
            ColumnDef(name="int_col", data_type="INT", unique=False),
            ColumnDef(name="text_col", data_type="TEXT", unique=False),
            ColumnDef(name="bool_col", data_type="BOOL", unique=False)
        ])
        table.insert({"int_col": 42, "text_col": "hello", "bool_col": True})
        
        data = table.to_dict()
        
        assert data["rows"][0]["int_col"] == 42
        assert data["rows"][0]["text_col"] == "hello"
        assert data["rows"][0]["bool_col"] is True

    def test_table_serialization_preserves_null(self):
        """Сериализация сохраняет NULL значения."""
        table = Table("test", [
            ColumnDef(name="id", data_type="INT", unique=True),
            ColumnDef(name="value", data_type="TEXT", unique=False)
        ])
        table.insert({"id": 1, "value": None})
        
        data = table.to_dict()
        
        assert data["rows"][0]["value"] is None


class TestSaveLoadExecution:
    """Тесты выполнения SAVE/LOAD команд."""

    def test_execute_save_creates_file(self):
        """SAVE создаёт файл."""
        db = Database()
        db.create_table("test", [
            ColumnDef(name="id", data_type="INT", unique=True)
        ])
        
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "test.json")
            
            result = db.save_to_file(filepath)
            
            assert result[0] is True
            assert os.path.exists(filepath)

    def test_execute_save_valid_json(self):
        """SAVE создаёт валидный JSON."""
        db = Database()
        db.create_table("test", [
            ColumnDef(name="id", data_type="INT", unique=True)
        ])
        table = db.get_table("test")
        table.insert({"id": 1})
        
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "test.json")
            
            db.save_to_file(filepath)
            
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            assert "tables" in data
            assert "test" in data["tables"]

    def test_execute_load_restores_data(self):
        """LOAD восстанавливает данные."""
        # Create and save database
        db1 = Database()
        db1.create_table("test", [
            ColumnDef(name="id", data_type="INT", unique=True),
            ColumnDef(name="value", data_type="TEXT", unique=False)
        ])
        table1 = db1.get_table("test")
        table1.insert({"id": 1, "value": "test1"})
        table1.insert({"id": 2, "value": "test2"})
        
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "test.json")
            db1.save_to_file(filepath)
            
            # Load into new database
            db2 = Database()
            result = db2.load_from_file(filepath)
            
            assert result[0] is True
            assert db2.table_exists("test")
            
            table2 = db2.get_table("test")
            result = table2.select(predicate=lambda row: True)  # select all rows
            rows = result.data
            assert len(rows) == 2

    def test_execute_load_nonexistent_file(self):
        """LOAD несуществующего файла - ошибка."""
        db = Database()
        result = db.load_from_file("/nonexistent/path/file.json")
        
        assert result[0] is False
        assert "not found" in result[1].lower() or "File not found" in result[1]

    def test_execute_load_invalid_json(self):
        """LOAD невалидного JSON - ошибка."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "invalid.json")
            
            with open(filepath, 'w') as f:
                f.write("not valid json {{{")
            
            db = Database()
            result = db.load_from_file(filepath)
            
            assert result[0] is False
            assert "JSON" in result[1] or "Invalid" in result[1]

    def test_save_load_preserves_unique_constraint(self):
        """SAVE/LOAD сохраняет UNIQUE constraint."""
        db1 = Database()
        db1.create_table("test", [
            ColumnDef(name="id", data_type="INT", unique=True)
        ])
        table1 = db1.get_table("test")
        table1.insert({"id": 1})
        
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "test.json")
            db1.save_to_file(filepath)
            
            db2 = Database()
            db2.load_from_file(filepath)
            
            table2 = db2.get_table("test")
            
            # Should succeed - different value
            result1 = table2.insert({"id": 2})
            assert result1.success
            
            # Should fail - UNIQUE violation
            result2 = table2.insert({"id": 1})
            assert not result2.success

    def test_save_load_roundtrip(self):
        """Полный цикл SAVE/LOAD сохраняет все данные."""
        db1 = Database()
        
        # Create tables
        db1.create_table("users", [
            ColumnDef(name="id", data_type="INT", unique=True),
            ColumnDef(name="name", data_type="TEXT", unique=False),
            ColumnDef(name="active", data_type="BOOL", unique=False)
        ])
        db1.create_table("products", [
            ColumnDef(name="id", data_type="INT", unique=True),
            ColumnDef(name="name", data_type="TEXT", unique=False)
        ])
        
        # Insert data
        users = db1.get_table("users")
        users.insert({"id": 1, "name": "Alice", "active": True})
        users.insert({"id": 2, "name": "Bob", "active": False})
        users.insert({"id": 3, "name": None, "active": True})  # NULL name
        
        products = db1.get_table("products")
        products.insert({"id": 1, "name": "Product A"})
        products.insert({"id": 2, "name": "Product B"})
        
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "database.json")
            
            # Save
            save_result = db1.save_to_file(filepath)
            assert save_result[0] is True
            
            # Load into new database
            db2 = Database()
            load_result = db2.load_from_file(filepath)
            assert load_result[0] is True
            
            # Verify tables exist
            assert db2.table_exists("users")
            assert db2.table_exists("products")
            
            # Verify data
            users2 = db2.get_table("users")
            users_result = users2.select(predicate=lambda row: True)  # select all rows
            users_data = users_result.data
            assert len(users_data) == 3
            
            products2 = db2.get_table("products")
            products_result = products2.select(predicate=lambda row: True)  # select all rows
            products_data = products_result.data
            assert len(products_data) == 2


class TestExecutorSaveLoad:
    """Тесты SAVE/LOAD через Executor."""

    def test_executor_save(self):
        """Executor выполняет SAVE."""
        parser = Parser()
        executor = Executor()
        db = Database()
        
        db.create_table("test", [
            ColumnDef(name="id", data_type="INT", unique=True)
        ])
        
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "test.json")
            query = f"SAVE '{filepath}';"
            
            ast = parser.parse(query)
            result = executor.execute(ast, db)
            
            assert result.success
            assert os.path.exists(filepath)

    def test_executor_load(self):
        """Executor выполняет LOAD."""
        parser = Parser()
        executor = Executor()
        
        # Create and save database
        db1 = Database()
        db1.create_table("test", [
            ColumnDef(name="id", data_type="INT", unique=True)
        ])
        db1.get_table("test").insert({"id": 1})
        
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "test.json")
            db1.save_to_file(filepath)
            
            # Load into new database
            db2 = Database()
            query = f"LOAD '{filepath}';"
            
            ast = parser.parse(query)
            result = executor.execute(ast, db2)
            
            assert result.success
            assert db2.table_exists("test")

    def test_executor_exit(self):
        """Executor выполняет EXIT."""
        parser = Parser()
        executor = Executor()
        db = Database()
        
        ast = parser.parse("EXIT;")
        result = executor.execute(ast, db)
        
        assert result.success
        assert "Goodbye" in result.message or result.exit


class TestCheckpoint3:
    """CHECKPOINT #3: REPL graceful error handling."""

    def test_repl_syntax_error_no_traceback(self):
        """REPL выводит 'Syntax error:' вместо Python Traceback."""
        from mini_db.repl.repl import REPL
        
        repl = REPL()
        output = repl.process("SELECT * FORM t")  # typo: FORM instead of FROM
        
        assert "Syntax error:" in output
        assert "Traceback" not in output

    def test_repl_parse_error_no_traceback(self):
        """REPL перехватывает ParseError."""
        from mini_db.repl.repl import REPL
        
        repl = REPL()
        output = repl.process("INSERT INTO t VALUES")  # incomplete
        
        assert "Syntax error:" in output or "Error:" in output
        assert "Traceback" not in output

    def test_repl_runtime_error_no_traceback(self):
        """REPL перехватывает ошибки выполнения."""
        from mini_db.repl.repl import REPL
        
        repl = REPL()
        output = repl.process("SELECT * FROM nonexistent_table;")
        
        assert "Error:" in output
        assert "Traceback" not in output

    def test_repl_empty_query(self):
        """REPL обрабатывает пустой запрос."""
        from mini_db.repl.repl import REPL
        
        repl = REPL()
        output = repl.process("")
        
        # Should not crash
        assert output is not None

    def test_repl_valid_query(self):
        """REPL выполняет валидный запрос."""
        from mini_db.repl.repl import REPL
        
        repl = REPL()
        
        # Create table
        repl.process("CREATE TABLE test (id INT UNIQUE);")
        
        # Insert
        output = repl.process("INSERT INTO test (id) VALUES (1);")
        assert "Error:" not in output
        
        # Select
        output = repl.process("SELECT * FROM test;")
        assert "1" in output

    def test_repl_exit_command(self):
        """REPL обрабатывает EXIT."""
        from mini_db.repl.repl import REPL
        
        repl = REPL()
        output = repl.process("EXIT;")
        
        assert "Goodbye" in output
        assert not repl.running


class TestAdversarialSaveLoad:
    """Адверсарные тесты для SAVE/LOAD."""

    def test_save_to_readonly_directory(self):
        """SAVE в readonly директорию - ошибка."""
        # Skip on Windows - permission handling differs
        import platform
        if platform.system() == "Windows":
            pytest.skip("Windows permission handling differs")
        
        db = Database()
        db.create_table("test", [
            ColumnDef(name="id", data_type="INT", unique=True)
        ])
        
        with tempfile.TemporaryDirectory() as tmpdir:
            readonly_dir = os.path.join(tmpdir, "readonly")
            os.makedirs(readonly_dir)
            os.chmod(readonly_dir, 0o555)  # read + execute only
            
            filepath = os.path.join(readonly_dir, "test.json")
            result = db.save_to_file(filepath)
            
            # Should fail - permission denied
            assert result[0] is False

    def test_load_corrupted_json(self):
        """LOAD повреждённого JSON - ошибка."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "corrupted.json")
            
            with open(filepath, 'w') as f:
                f.write('{"tables": {not valid}}')
            
            db = Database()
            result = db.load_from_file(filepath)
            
            assert result[0] is False

    def test_save_with_special_characters_in_path(self):
        """SAVE с спецсимволами в пути."""
        db = Database()
        db.create_table("test", [
            ColumnDef(name="id", data_type="INT", unique=True)
        ])
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # Use path with spaces
            filepath = os.path.join(tmpdir, "test file.json")
            
            result = db.save_to_file(filepath)
            
            assert result[0] is True
            assert os.path.exists(filepath)

    def test_load_empty_file(self):
        """LOAD пустого файла - ошибка."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "empty.json")
            
            with open(filepath, 'w') as f:
                f.write("")
            
            db = Database()
            result = db.load_from_file(filepath)
            
            assert result[0] is False

    def test_save_load_with_unicode_data(self):
        """SAVE/LOAD с Unicode данными."""
        db1 = Database()
        db1.create_table("test", [
            ColumnDef(name="id", data_type="INT", unique=True),
            ColumnDef(name="text", data_type="TEXT", unique=False)
        ])
        
        table = db1.get_table("test")
        table.insert({"id": 1, "text": "Привет мир!"})  # Russian
        table.insert({"id": 2, "text": "你好世界"})  # Chinese
        table.insert({"id": 3, "text": "مرحبا بالعالم"})  # Arabic
        
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "unicode.json")
            
            db1.save_to_file(filepath)
            
            db2 = Database()
            db2.load_from_file(filepath)
            
            table2 = db2.get_table("test")
            result = table2.select(predicate=lambda row: True)  # select all rows
            rows = result.data
            
            assert len(rows) == 3
            assert rows[0]["text"] == "Привет мир!"
            assert rows[1]["text"] == "你好世界"
            assert rows[2]["text"] == "مرحبا بالعالم"