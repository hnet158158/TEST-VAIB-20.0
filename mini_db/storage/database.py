# START_MODULE_CONTRACT
# Module: mini_db.storage.database
# Intent: In-memory база данных с коллекцией таблиц.
#         Управление жизненным циклом таблиц, SAVE/LOAD.
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports:
#   - Database: класс базы данных с методами create_table, get_table, drop_table
#   - to_dict, from_dict: JSON serialization
# END_MODULE_MAP

from __future__ import annotations

import json
from typing import Any, Optional

from mini_db.ast.nodes import ColumnDef
from mini_db.storage.table import Table


# START_BLOCK_DATABASE
class Database:
    """
    [START_CONTRACT_DATABASE]
    Intent: In-memory база данных с коллекцией таблиц.
    Input: Нет параметров конструктора.
    Output: Объект с методами управления таблицами.
    [END_CONTRACT_DATABASE]
    """
    
    def __init__(self):
        self.tables: dict[str, Table] = {}
    
    def create_table(self, name: str, columns: list[ColumnDef]) -> tuple[bool, Optional[str]]:
        """
        [START_CONTRACT_CREATE_TABLE]
        Intent: Создать таблицу с указанным именем и колонками.
        Input: name - уникальное имя таблицы; columns - непустой список ColumnDef.
        Output: (True, None) при успехе, (False, error_message) при ошибке.
        [END_CONTRACT_CREATE_TABLE]
        """
        if name in self.tables:
            return False, f"Table '{name}' already exists"
        
        if not columns:
            return False, "Table must have at least one column"
        
        # Validate column names are unique
        col_names = [col.name for col in columns]
        if len(col_names) != len(set(col_names)):
            return False, "Duplicate column names"
        
        self.tables[name] = Table(name, columns)
        return True, None
    
    def get_table(self, name: str) -> Optional[Table]:
        """
        [START_CONTRACT_GET_TABLE]
        Intent: Получить таблицу по имени.
        Input: name - имя существующей таблицы.
        Output: Table или None если не существует.
        [END_CONTRACT_GET_TABLE]
        """
        return self.tables.get(name)
    
    def drop_table(self, name: str) -> tuple[bool, Optional[str]]:
        """
        [START_CONTRACT_DROP_TABLE]
        Intent: Удалить таблицу по имени.
        Input: name - имя существующей таблицы.
        Output: (True, None) при успехе, (False, error_message) при ошибке.
        [END_CONTRACT_DROP_TABLE]
        """
        if name not in self.tables:
            return False, f"Table '{name}' does not exist"
        
        del self.tables[name]
        return True, None
    
    def table_exists(self, name: str) -> bool:
        """
        [START_CONTRACT_TABLE_EXISTS]
        Intent: Проверить существование таблицы.
        Input: name - имя таблицы.
        Output: True если таблица существует, иначе False.
        [END_CONTRACT_TABLE_EXISTS]
        """
        return name in self.tables
    
    def list_tables(self) -> list[str]:
        """
        [START_CONTRACT_LIST_TABLES]
        Intent: Получить список имён всех таблиц.
        Output: list[str] с именами таблиц в порядке создания.
        [END_CONTRACT_LIST_TABLES]
        """
        return list(self.tables.keys())
     
    def create_index(
        self,
        index_name: str,
        table_name: str,
        column: str
    ) -> tuple[bool, Optional[str]]:
        """
        [START_CONTRACT_CREATE_INDEX_DB]
        Intent: Создать индекс на колонке таблицы.
        Input: index_name - уникальное имя; table_name - существующая таблица; column - колонка.
        Output: (True, None) при успехе, (False, error) при ошибке.
        [END_CONTRACT_CREATE_INDEX_DB]
        """
        table = self.get_table(table_name)
        if table is None:
            return False, f"Table '{table_name}' does not exist"
        
        return table.create_index(index_name, column)
    
    def clear(self) -> None:
        """
        [START_CONTRACT_CLEAR]
        Intent: Удалить все таблицы из базы.
        Output: База становится пустой.
        [END_CONTRACT_CLEAR]
        """
        self.tables.clear()
    
    def to_dict(self) -> dict[str, Any]:
        """
        [START_CONTRACT_TO_DICT]
        Intent: Сериализовать базу данных в словарь для JSON.
        Output: dict с таблицами и их данными.
        [END_CONTRACT_TO_DICT]
        """
        return {
            "tables": {
                name: table.to_dict()
                for name, table in self.tables.items()
            }
        }
    
    def from_dict(self, data: dict[str, Any]) -> None:
        """
        [START_CONTRACT_FROM_DICT]
        Intent: Десериализовать базу данных из словаря (JSON).
        Input: data - dict с ключом "tables".
        Output: Восстановленные таблицы в self.tables.
        [END_CONTRACT_FROM_DICT]
        """
        self.tables.clear()
        
        tables_data = data.get("tables", {})
        for name, table_data in tables_data.items():
            table = Table(name, [])
            table.from_dict(table_data)
            self.tables[name] = table
    
    def save_to_file(self, filepath: str) -> tuple[bool, Optional[str]]:
        """
        [START_CONTRACT_SAVE_TO_FILE]
        Intent: Сохранить базу в JSON-файл.
        Input: filepath - путь к файлу.
        Output: (True, None) при успехе, (False, error) при ошибке.
        [END_CONTRACT_SAVE_TO_FILE]
        """
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
            return True, None
        except (IOError, OSError) as e:
            return False, f"Failed to save database: {e}"
    
    def load_from_file(self, filepath: str) -> tuple[bool, Optional[str]]:
        """
        [START_CONTRACT_LOAD_FROM_FILE]
        Intent: Загрузить базу из JSON-файла.
        Input: filepath - путь к файлу.
        Output: (True, None) при успехе, (False, error) при ошибке.
        [END_CONTRACT_LOAD_FROM_FILE]
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            self.from_dict(data)
            return True, None
        except FileNotFoundError:
            return False, f"File not found: {filepath}"
        except json.JSONDecodeError as e:
            return False, f"Invalid JSON format: {e}"
        except (IOError, OSError) as e:
            return False, f"Failed to load database: {e}"
# END_BLOCK_DATABASE