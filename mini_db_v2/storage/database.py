# START_MODULE_CONTRACT
# Module: mini_db_v2.storage.database
# Intent: Менеджер базы данных - создание/удаление таблиц, управление индексами.
# Dependencies: typing, threading
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: Database, DatabaseError
# END_MODULE_MAP

from __future__ import annotations
from typing import Optional
import threading


# =============================================================================
# START_BLOCK_ERRORS
# =============================================================================

class DatabaseError(Exception):
    """
    [START_CONTRACT_DATABASE_ERROR]
    Intent: Базовое исключение для ошибок БД.
    Output: Исключение с контекстом ошибки.
    [END_CONTRACT_DATABASE_ERROR]
    """
    pass


class TableExistsError(DatabaseError):
    """Таблица уже существует."""
    pass


class TableNotFoundError(DatabaseError):
    """Таблица не найдена."""
    pass

# END_BLOCK_ERRORS


# =============================================================================
# START_BLOCK_DATABASE
# =============================================================================

class Database:
    """
    [START_CONTRACT_DATABASE]
    Intent: Менеджер базы данных - управление таблицами и индексами.
    Input: name - имя базы данных.
    Output: API для DDL операций и доступа к таблицам.
    [END_CONTRACT_DATABASE]
    """
    
    def __init__(self, name: str = "default"):
        """
        [START_CONTRACT_DATABASE_INIT]
        Intent: Инициализация пустой базы данных.
        Input: name - имя базы данных.
        Output: Готовая к работе БД без таблиц.
        [END_CONTRACT_DATABASE_INIT]
        """
        self.name = name
        self._tables: dict[str, Table] = {}
        self._lock = threading.RLock()
    
    def create_table(
        self,
        name: str,
        columns: dict[str, ColumnDef],
        if_not_exists: bool = False
    ) -> Table:
        """
        [START_CONTRACT_CREATE_TABLE]
        Intent: Создать новую таблицу с заданными колонками.
        Input: name - имя таблицы; columns - определения колонок;
               if_not_exists - игнорировать если существует.
        Output: Созданная таблица.
        Raises: TableExistsError если таблица уже есть и if_not_exists=False.
        [END_CONTRACT_CREATE_TABLE]
        """
        with self._lock:
            if name in self._tables:
                if if_not_exists:
                    return self._tables[name]
                raise TableExistsError(f"Table '{name}' already exists")
            
            table = Table(name, columns)
            self._tables[name] = table
            return table
    
    def drop_table(self, name: str, if_exists: bool = False) -> None:
        """
        [START_CONTRACT_DROP_TABLE]
        Intent: Удалить таблицу.
        Input: name - имя таблицы; if_exists - игнорировать если нет.
        Output: None.
        Raises: TableNotFoundError если таблицы нет и if_exists=False.
        [END_CONTRACT_DROP_TABLE]
        """
        with self._lock:
            if name not in self._tables:
                if if_exists:
                    return
                raise TableNotFoundError(f"Table '{name}' not found")
            del self._tables[name]
    
    def get_table(self, name: str) -> Optional[Table]:
        """
        [START_CONTRACT_GET_TABLE]
        Intent: Получить таблицу по имени.
        Input: name - имя таблицы.
        Output: Таблица или None если не найдена.
        [END_CONTRACT_GET_TABLE]
        """
        with self._lock:
            return self._tables.get(name)
    
    def table_exists(self, name: str) -> bool:
        """Проверяет существование таблицы."""
        with self._lock:
            return name in self._tables
    
    @property
    def tables(self) -> list[str]:
        """Возвращает список имён таблиц."""
        with self._lock:
            return list(self._tables.keys())
    
    def clear(self) -> None:
        """Удаляет все таблицы."""
        with self._lock:
            self._tables.clear()

# END_BLOCK_DATABASE


# =============================================================================
# START_BLOCK_IMPORTS_FOR_FORWARD_REFERENCES
# =============================================================================

# Импорты после определения классов для избежания циклических зависимостей
from .table import Table, ColumnDef

# END_BLOCK_IMPORTS_FOR_FORWARD_REFERENCES