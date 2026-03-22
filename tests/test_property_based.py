# START_MODULE_CONTRACT
# Module: tests.test_property_based
# Intent: Property-based тесты для Storage - проверка инвариантов системы.
#         Используются свойства (properties), которые должны выполняться всегда.
# Constraints: Без сторонних библиотек, только Python assertions.
# END_MODULE_CONTRACT

"""
PROPERTY-BASED TESTS: Storage Invariants

Эти тесты проверяют инварианты - свойства, которые должны выполняться ВСЕГДА:
1. Insertion order preservation - порядок вставки сохраняется
2. UNIQUE constraint - уникальность значений
3. Atomicity - все или ничего
4. Index consistency - индексы соответствуют данным
5. Type safety - строгая типизация
"""

import pytest
import random
import tempfile
import os
from typing import Any, Optional

from mini_db.storage.database import Database
from mini_db.storage.table import Table, InsertResult, UpdateResult, DeleteResult, SelectResult
from mini_db.storage.index import HashIndex
from mini_db.executor.executor import Executor
from mini_db.parser.parser import Parser
from mini_db.ast.nodes import ColumnDef


# ==================== PROPERTY HELPERS ====================

class PropertyVerifier:
    """Вспомогательный класс для проверки свойств."""
    
    @staticmethod
    def check_insertion_order(table: Table, expected_rows: list[dict]) -> bool:
        """Проверяет что порядок строк соответствует порядку вставки."""
        result = table.select(lambda row: True, None)
        actual = result.data if result.data else []
        return actual == expected_rows
    
    @staticmethod
    def check_unique_constraint(table: Table, column: str) -> bool:
        """Проверяет что UNIQUE constraint соблюдается."""
        if column not in table.unique_indexes:
            return True  # Нет UNIQUE - свойство выполняется тривиально
        
        # Собираем все значения в колонке (кроме NULL)
        values = []
        for row in table.rows:
            val = row.get(column)
            if val is not None:
                values.append(val)
        
        # Проверяем что в unique_indexes те же значения
        indexed_values = table.unique_indexes[column]
        
        return set(values) == indexed_values
    
    @staticmethod
    def check_index_consistency(table: Table) -> bool:
        """Проверяет что все индексы соответствуют данным."""
        for index_name, index in table.indexes.items():
            column = index.column
            
            # Собираем значения из строк
            for row_idx, row in enumerate(table.rows):
                val = row.get(column)
                if val is not None:
                    # Проверяем что значение в индексе
                    if row_idx not in index.lookup(val):
                        return False
        
        return True
    
    @staticmethod
    def check_type_safety(table: Table) -> bool:
        """Проверяет что все значения соответствуют типам колонок."""
        type_checkers = {
            'INT': lambda x: isinstance(x, int) and not isinstance(x, bool),
            'TEXT': lambda x: isinstance(x, str),
            'BOOL': lambda x: isinstance(x, bool),
        }
        
        for row in table.rows:
            for col_name, col_def in table.columns.items():
                value = row.get(col_name)
                if value is not None:  # NULL может быть любым типом
                    checker = type_checkers.get(col_def.data_type)
                    if checker and not checker(value):
                        return False
        
        return True


# ==================== INSERTION ORDER PROPERTY ====================

class TestInsertionOrderProperty:
    """Проверка свойства: порядок вставки сохраняется."""
    
    def test_insertion_order_preserved_single_table(self):
        """Порядок вставки сохраняется в одной таблице."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT"),
            ColumnDef(name="value", data_type="TEXT"),
        ])
        
        table = db.get_table("t")
        verifier = PropertyVerifier()
        
        # Вставляем 100 строк в определённом порядке
        expected_rows = []
        for i in range(100):
            row = {"id": i, "value": f"value_{i}"}
            table.insert(row)
            expected_rows.append(row)
        
        # Проверяем свойство
        assert verifier.check_insertion_order(table, expected_rows)
    
    def test_insertion_order_preserved_with_deletes(self):
        """Порядок сохраняется даже после удалений."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT"),
        ])
        
        table = db.get_table("t")
        
        # Вставляем
        for i in range(100):
            table.insert({"id": i})
        
        # Удаляем каждый третий
        table.delete(lambda row: row["id"] % 3 == 0)
        
        # Проверяем что порядок оставшихся сохранён
        result = table.select(lambda row: True, None)
        remaining = result.data if result.data else []
        ids = [r["id"] for r in remaining]
        
        # Должны быть только не делящиеся на 3
        assert all(i % 3 != 0 for i in ids)
        # И в возрастающем порядке
        assert ids == sorted(ids)
    
    def test_insertion_order_preserved_after_rollback(self):
        """Порядок сохраняется после отката неудачного UPDATE."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT", unique=True),
            ColumnDef(name="value", data_type="TEXT"),
        ])
        
        table = db.get_table("t")
        
        # Вставляем
        for i in range(10):
            table.insert({"id": i, "value": f"val_{i}"})
        
        result = table.select(lambda row: True, None)
        original_rows = result.data if result.data else []
        
        # Пытаемся UPDATE с UNIQUE violation
        result = table.update(
            lambda row: True,  # Все строки
            {"id": 999}  # Все в один id - violation
        )
        
        assert result.success == False
        
        # Порядок должен быть неизменным
        result = table.select(lambda row: True, None)
        assert result.data == original_rows


# ==================== UNIQUE CONSTRAINT PROPERTY ====================

class TestUniqueConstraintProperty:
    """Проверка свойства: UNIQUE constraint всегда соблюдается."""
    
    def test_unique_never_violated_on_insert(self):
        """INSERT никогда не создаёт дубликаты в UNIQUE колонке."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT", unique=True),
        ])
        
        table = db.get_table("t")
        verifier = PropertyVerifier()
        
        # Вставляем уникальные значения
        for i in range(100):
            result = table.insert({"id": i})
            assert result.success == True
            assert verifier.check_unique_constraint(table, "id")
        
        # Пытаемся вставить дубликат
        result = table.insert({"id": 50})
        assert result.success == False
        
        # Свойство всё ещё выполняется
        assert verifier.check_unique_constraint(table, "id")
    
    def test_unique_never_violated_on_update(self):
        """UPDATE никогда не создаёт дубликаты в UNIQUE колонке."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT", unique=True),
        ])
        
        table = db.get_table("t")
        verifier = PropertyVerifier()
        
        # Вставляем
        for i in range(10):
            table.insert({"id": i})
        
        # Пытаемся UPDATE с дубликатом
        result = table.update(
            lambda row: row["id"] == 0,
            {"id": 5}  # 5 уже существует
        )
        
        assert result.success == False
        assert verifier.check_unique_constraint(table, "id")
    
    def test_unique_multiple_columns(self):
        """UNIQUE работает для нескольких колонок независимо."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="a", data_type="INT", unique=True),
            ColumnDef(name="b", data_type="INT", unique=True),
        ])
        
        table = db.get_table("t")
        verifier = PropertyVerifier()
        
        # Вставляем с разными комбинациями
        for i in range(10):
            result = table.insert({"a": i, "b": i * 10})
            assert result.success == True
        
        # Проверяем оба UNIQUE
        assert verifier.check_unique_constraint(table, "a")
        assert verifier.check_unique_constraint(table, "b")
        
        # Дубликат в a
        result = table.insert({"a": 5, "b": 999})
        assert result.success == False
        
        # Дубликат в b
        result = table.insert({"a": 999, "b": 50})
        assert result.success == False
    
    def test_unique_with_nulls(self):
        """UNIQUE позволяет множество NULL значений."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT", unique=True),
        ])
        
        table = db.get_table("t")
        
        # NULL не считается дубликатом
        for _ in range(10):
            result = table.insert({"id": None})
            assert result.success == True
        
        # Но конкретное значение - дубликат
        table.insert({"id": 1})
        result = table.insert({"id": 1})
        assert result.success == False


# ==================== ATOMICITY PROPERTY ====================

class TestAtomicityProperty:
    """Проверка свойства: операции атомарны (all-or-nothing)."""
    
    def test_update_atomic_all_or_nothing(self):
        """UPDATE атомарный: либо все строки обновлены, либо ни одной."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT", unique=True),
            ColumnDef(name="value", data_type="TEXT"),
        ])
        
        table = db.get_table("t")
        
        # Вставляем 10 строк
        for i in range(10):
            table.insert({"id": i, "value": f"val_{i}"})
        
        result = table.select(lambda row: True, None)
        original_rows = result.data if result.data else []
        
        # UPDATE который должен провалиться из-за UNIQUE
        result = table.update(
            lambda row: True,  # Все строки
            {"id": 999}  # Все в один id
        )
        
        assert result.success == False
        
        # Ни одна строка не должна измениться
        result = table.select(lambda row: True, None)
        assert result.data == original_rows
    
    def test_update_partial_rollback(self):
        """При ошибке в середине UPDATE все изменения откатываются."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT", unique=True),
        ])
        
        table = db.get_table("t")
        
        # id = 1, 2, 3, 4, 5
        for i in range(1, 6):
            table.insert({"id": i})
        
        result = table.select(lambda row: True, None)
        original = result.data if result.data else []
        
        # UPDATE который провалится на строке с id=3
        # (пытаемся установить id=2, но 2 уже существует)
        result = table.update(
            lambda row: row["id"] in [1, 3, 5],
            {"id": 2}  # UNIQUE violation
        )
        
        assert result.success == False
        result = table.select(lambda row: True, None)
        assert result.data == original
    
    def test_delete_is_atomic(self):
        """DELETE атомарный - удаляются все подходящие строки или ни одной."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT"),
        ])
        
        table = db.get_table("t")
        
        for i in range(10):
            table.insert({"id": i})
        
        # DELETE с условием
        result = table.delete(lambda row: row["id"] < 5)
        
        assert result.success == True
        assert result.rows_affected == 5
        
        # Остались только id >= 5
        result = table.select(lambda row: True, None)
        remaining = result.data if result.data else []
        assert all(r["id"] >= 5 for r in remaining)


# ==================== INDEX CONSISTENCY PROPERTY ====================

class TestIndexConsistencyProperty:
    """Проверка свойства: индексы всегда соответствуют данным."""
    
    def test_index_consistent_after_inserts(self):
        """Индекс соответствует данным после INSERT."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT"),
        ])
        
        table = db.get_table("t")
        table.create_index("idx_id", "id")
        
        verifier = PropertyVerifier()
        
        for i in range(100):
            table.insert({"id": i})
            assert verifier.check_index_consistency(table)
    
    def test_index_consistent_after_updates(self):
        """Индекс соответствует данным после UPDATE."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT"),
        ])
        
        table = db.get_table("t")
        table.create_index("idx_id", "id")
        
        verifier = PropertyVerifier()
        
        for i in range(50):
            table.insert({"id": i})
        
        # Обновляем некоторые значения
        for i in range(0, 50, 5):
            table.update(
                lambda row, oid=i: row["id"] == oid,
                {"id": i + 100}
            )
            assert verifier.check_index_consistency(table)
    
    def test_index_consistent_after_deletes(self):
        """Индекс соответствует данным после DELETE."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT"),
        ])
        
        table = db.get_table("t")
        table.create_index("idx_id", "id")
        
        verifier = PropertyVerifier()
        
        for i in range(100):
            table.insert({"id": i})
        
        # Удаляем каждое третье значение
        table.delete(lambda row: row["id"] % 3 == 0)
        
        assert verifier.check_index_consistency(table)
    
    def test_index_rebuild_on_load(self):
        """Индексы перестраиваются при LOAD из файла."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "test.json")
            
            # Создаём базу с индексом
            db1 = Database()
            db1.create_table("t", [
                ColumnDef(name="id", data_type="INT"),
            ])
            table1 = db1.get_table("t")
            table1.create_index("idx_id", "id")
            
            for i in range(10):
                table1.insert({"id": i})
            
            # Сохраняем
            db1.save_to_file(filepath)
            
            # Загружаем в новую базу
            db2 = Database()
            db2.load_from_file(filepath)
            
            table2 = db2.get_table("t")
            verifier = PropertyVerifier()
            
            # Индекс должен быть перестроен
            assert "idx_id" in table2.indexes
            assert verifier.check_index_consistency(table2)


# ==================== TYPE SAFETY PROPERTY ====================

class TestTypeSafetyProperty:
    """Проверка свойства: строгая типизация."""
    
    def test_type_safety_int_column(self):
        """INT колонка принимает только int (не bool, не str)."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="x", data_type="INT"),
        ])
        
        table = db.get_table("t")
        verifier = PropertyVerifier()
        
        # Правильные значения
        for val in [0, 1, -1, 100, -999, 10**6]:
            result = table.insert({"x": val})
            assert result.success == True
        
        # Неправильные значения
        for val in ["123", True, False, 1.5]:
            result = table.insert({"x": val})
            assert result.success == False
        
        # None допустим (NULL)
        result = table.insert({"x": None})
        assert result.success == True
        
        assert verifier.check_type_safety(table)
    
    def test_type_safety_text_column(self):
        """TEXT колонка принимает только str."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="x", data_type="TEXT"),
        ])
        
        table = db.get_table("t")
        verifier = PropertyVerifier()
        
        # Правильные значения
        for val in ["", "hello", "привет", "🎉"]:
            result = table.insert({"x": val})
            assert result.success == True
        
        # Неправильные значения
        for val in [123, True, False]:
            result = table.insert({"x": val})
            assert result.success == False
        
        assert verifier.check_type_safety(table)
    
    def test_type_safety_bool_column(self):
        """BOOL колонка принимает только bool (не int)."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="x", data_type="BOOL"),
        ])
        
        table = db.get_table("t")
        verifier = PropertyVerifier()
        
        # Правильные значения
        for val in [True, False]:
            result = table.insert({"x": val})
            assert result.success == True
        
        # Неправильные значения (int не принимается!)
        for val in [0, 1, "true", "false"]:
            result = table.insert({"x": val})
            assert result.success == False
        
        assert verifier.check_type_safety(table)
    
    def test_no_implicit_type_conversion(self):
        """Неявное преобразование типов запрещено."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="int_col", data_type="INT"),
            ColumnDef(name="text_col", data_type="TEXT"),
            ColumnDef(name="bool_col", data_type="BOOL"),
        ])
        
        table = db.get_table("t")
        
        # Строка "123" не должна преобразовываться в int
        result = table.insert({"int_col": "123", "text_col": "x", "bool_col": True})
        assert result.success == False
        
        # int 1 не должен преобразовываться в bool
        result = table.insert({"int_col": 1, "text_col": "x", "bool_col": 1})
        assert result.success == False
        
        # int 123 не должен преобразовываться в text
        result = table.insert({"int_col": 1, "text_col": 123, "bool_col": True})
        assert result.success == False


# ==================== SAVE/LOAD ROUNDTRIP PROPERTY ====================

class TestSaveLoadRoundtripProperty:
    """Проверка свойства: SAVE/LOAD сохраняет все данные."""
    
    def test_roundtrip_preserves_data(self):
        """SAVE + LOAD сохраняет все данные."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "test.json")
            
            db1 = Database()
            db1.create_table("t1", [
                ColumnDef(name="id", data_type="INT", unique=True),
                ColumnDef(name="name", data_type="TEXT"),
            ])
            db1.create_table("t2", [
                ColumnDef(name="flag", data_type="BOOL"),
            ])
            
            # Заполняем данными
            t1 = db1.get_table("t1")
            for i in range(100):
                t1.insert({"id": i, "name": f"name_{i}"})
            
            t2 = db1.get_table("t2")
            for i in range(10):
                t2.insert({"flag": i % 2 == 0})
            
            # Создаём индексы
            t1.create_index("idx_id", "id")
            
            # Сохраняем
            db1.save_to_file(filepath)
            
            # Загружаем
            db2 = Database()
            db2.load_from_file(filepath)
            
            # Проверяем что данные идентичны
            assert db2.table_exists("t1")
            assert db2.table_exists("t2")
            
            t1_new = db2.get_table("t1")
            assert len(t1_new.rows) == 100
            assert "idx_id" in t1_new.indexes
            
            t2_new = db2.get_table("t2")
            assert len(t2_new.rows) == 10
    
    def test_roundtrip_preserves_unique_constraints(self):
        """SAVE + LOAD сохраняет UNIQUE constraints."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "test.json")
            
            db1 = Database()
            db1.create_table("t", [
                ColumnDef(name="id", data_type="INT", unique=True),
            ])
            
            t1 = db1.get_table("t")
            for i in range(10):
                t1.insert({"id": i})
            
            db1.save_to_file(filepath)
            
            db2 = Database()
            db2.load_from_file(filepath)
            
            t2 = db2.get_table("t")
            
            # Проверяем что UNIQUE работает
            result = t2.insert({"id": 5})  # Дубликат
            assert result.success == False
    
    def test_roundtrip_with_unicode(self):
        """SAVE + LOAD корректно работает с Unicode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "unicode.json")
            
            db1 = Database()
            db1.create_table("t", [
                ColumnDef(name="text", data_type="TEXT"),
            ])
            
            t1 = db1.get_table("t")
            unicode_values = [
                "привет мир",
                "你好世界",
                "🎉🎊🎁",
                "مرحبا",
                "Ñoño",
            ]
            
            for val in unicode_values:
                t1.insert({"text": val})
            
            db1.save_to_file(filepath)
            
            db2 = Database()
            db2.load_from_file(filepath)
            
            t2 = db2.get_table("t")
            result = t2.select(lambda row: True, None)
            actual_values = [r["text"] for r in result.data] if result.data else []
            
            assert actual_values == unicode_values


# ==================== COMPLEX PROPERTIES ====================

class TestComplexProperties:
    """Комплексные проверки нескольких свойств одновременно."""
    
    def test_all_properties_after_random_operations(self):
        """Все свойства выполняются после случайных операций."""
        db = Database()
        db.create_table("t", [
            ColumnDef(name="id", data_type="INT", unique=True),
            ColumnDef(name="value", data_type="TEXT"),
        ])
        
        table = db.get_table("t")
        table.create_index("idx_id", "id")
        
        verifier = PropertyVerifier()
        
        # Выполняем случайные операции
        next_id = 0
        for _ in range(100):
            op = random.choice(['insert', 'update', 'delete'])
            
            if op == 'insert':
                result = table.insert({"id": next_id, "value": f"val_{next_id}"})
                if result.success:
                    next_id += 1
            
            elif op == 'update' and len(table.rows) > 0:
                # Случайное обновление
                result = table.select(lambda row: True, None)
                rows = result.data if result.data else []
                if rows:
                    old_id = random.choice([r["id"] for r in rows])
                    new_id = next_id + 100
                    result = table.update(
                        lambda row, oid=old_id: row["id"] == oid,
                        {"id": new_id}
                    )
                    if result.success:
                        next_id += 1
            
            elif op == 'delete' and len(table.rows) > 0:
                # Случайное удаление
                result = table.select(lambda row: True, None)
                rows = result.data if result.data else []
                if rows:
                    id_to_delete = random.choice([r["id"] for r in rows])
                    table.delete(lambda row, id=id_to_delete: row["id"] == id)
            
            # Проверяем все свойства после каждой операции
            assert verifier.check_unique_constraint(table, "id")
            assert verifier.check_index_consistency(table)
            assert verifier.check_type_safety(table)
    
    def test_properties_with_executor(self):
        """Свойства выполняются при работе через Executor."""
        db = Database()
        executor = Executor()
        parser = Parser()
        
        verifier = PropertyVerifier()
        
        # CREATE TABLE
        ast = parser.parse("CREATE TABLE t (id INT UNIQUE, value TEXT)")
        executor.execute(ast, db)
        
        # INSERT
        for i in range(50):
            ast = parser.parse(f"INSERT INTO t (id, value) VALUES ({i}, 'val_{i}')")
            executor.execute(ast, db)
        
        table = db.get_table("t")
        
        # Проверяем свойства
        assert verifier.check_unique_constraint(table, "id")
        assert verifier.check_type_safety(table)
        
        # UPDATE с violation
        ast = parser.parse("UPDATE t SET id = 1")
        result = executor.execute(ast, db)
        
        assert result.success == False
        assert verifier.check_unique_constraint(table, "id")


# ==================== HASHINDEX UNIT PROPERTIES ====================

class TestHashIndexProperties:
    """Свойства HashIndex."""
    
    def test_index_lookup_returns_correct_rows(self):
        """lookup возвращает правильные row indices."""
        index = HashIndex("test_col")
        
        # Добавляем значения
        index.add("a", 0)
        index.add("b", 1)
        index.add("a", 2)  # Дубликат значения, другой row
        
        # Проверяем lookup
        assert index.lookup("a") == {0, 2}
        assert index.lookup("b") == {1}
        assert index.lookup("c") == set()
    
    def test_index_remove_updates_correctly(self):
        """remove корректно обновляет индекс."""
        index = HashIndex("test_col")
        
        index.add("x", 0)
        index.add("x", 1)
        index.add("x", 2)
        
        # Удаляем один
        index.remove("x", 1)
        
        assert index.lookup("x") == {0, 2}
        
        # Удаляем все
        index.remove("x", 0)
        index.remove("x", 2)
        
        assert index.lookup("x") == set()
    
    def test_index_rebuild_from_data(self):
        """rebuild корректно перестраивает индекс из данных."""
        index = HashIndex("id")
        
        rows = [
            {"id": 1, "name": "a"},
            {"id": 2, "name": "b"},
            {"id": 1, "name": "c"},  # Дубликат id
        ]
        
        index.rebuild(rows, "id")
        
        assert index.lookup(1) == {0, 2}
        assert index.lookup(2) == {1}
    
    def test_index_handles_none_values(self):
        """Индекс корректно работает с None."""
        index = HashIndex("col")
        
        index.add(None, 0)
        index.add(None, 1)
        index.add("value", 2)
        
        assert index.lookup(None) == {0, 1}
        assert index.lookup("value") == {2}
        assert index.contains(None) == True