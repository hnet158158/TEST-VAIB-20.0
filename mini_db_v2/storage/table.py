# START_MODULE_CONTRACT
# Module: mini_db_v2.storage.table
# Intent: Таблица с базовым хранением строк и поддержкой B-tree индексов.
# Dependencies: dataclasses, typing, threading
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: Table, ColumnDef, Row, TableError, DataType
# END_MODULE_MAP

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional, Iterator, Callable
from enum import Enum, auto
import threading


# =============================================================================
# START_BLOCK_ENUMS
# =============================================================================

class DataType(Enum):
    """Типы данных колонок."""
    INT = auto()
    TEXT = auto()
    REAL = auto()
    BOOL = auto()


class ConstraintType(Enum):
    """Типы ограничений."""
    PRIMARY_KEY = auto()
    UNIQUE = auto()
    NOT_NULL = auto()

# END_BLOCK_ENUMS


# =============================================================================
# START_BLOCK_COLUMN_DEF
# =============================================================================

@dataclass
class ColumnDef:
    """
    [START_CONTRACT_COLUMN_DEF]
    Intent: Определение колонки таблицы.
    Input: name - имя; data_type - тип; constraints - ограничения.
    Output: Структура описывающая колонку.
    [END_CONTRACT_COLUMN_DEF]
    """
    name: str
    data_type: DataType
    nullable: bool = True
    primary_key: bool = False
    unique: bool = False
    default: Optional[Any] = None
    
    def validate_value(self, value: Any) -> bool:
        """
        [START_CONTRACT_VALIDATE_VALUE]
        Intent: Проверить соответствие значения типу колонки.
        Input: value - значение для проверки.
        Output: True если значение валидно.
        [END_CONTRACT_VALIDATE_VALUE]
        """
        if value is None:
            return self.nullable
        
        if self.data_type == DataType.INT:
            return isinstance(value, int) and not isinstance(value, bool)
        if self.data_type == DataType.TEXT:
            return isinstance(value, str)
        if self.data_type == DataType.REAL:
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        if self.data_type == DataType.BOOL:
            return isinstance(value, bool)
        
        return False

# END_BLOCK_COLUMN_DEF


# =============================================================================
# START_BLOCK_ERRORS
# =============================================================================

class TableError(Exception):
    """Базовая ошибка таблицы."""
    pass


class DuplicateKeyError(TableError):
    """Нарушение уникальности."""
    pass


class ValidationError(TableError):
    """Ошибка валидации данных."""
    pass


class ColumnNotFoundError(TableError):
    """Колонка не найдена."""
    pass


class IndexNotFoundError(TableError):
    """Индекс не найден."""
    pass

# END_BLOCK_ERRORS


# =============================================================================
# START_BLOCK_INDEX_INFO
# =============================================================================

@dataclass
class IndexInfo:
    """
    [START_CONTRACT_INDEX_INFO]
    Intent: Информация об индексе таблицы.
    Input: name - имя индекса; column_name - колонка; unique - уникальность.
    Output: Структура для хранения информации об индексе.
    [END_CONTRACT_INDEX_INFO]
    """
    name: str
    column_name: str
    unique: bool = False
    index_type: str = "BTREE"  # BTREE or HASH

# END_BLOCK_INDEX_INFO


# =============================================================================
# START_BLOCK_TABLE
# =============================================================================

@dataclass
class Row:
    """
    [START_CONTRACT_ROW]
    Intent: Строка таблицы с данными.
    Input: data - словарь {колонка: значение}; row_id - идентификатор.
    Output: Структура для хранения строки.
    [END_CONTRACT_ROW]
    """
    data: dict[str, Any]
    row_id: int = -1


class Table:
    """
    [START_CONTRACT_TABLE]
    Intent: Таблица БД с хранением строк и поддержкой B-tree индексов.
    Input: name - имя; columns - определения колонок.
    Output: API для CRUD операций с индексами.
    Note: MVCC будет добавлен в Phase 7.
    [END_CONTRACT_TABLE]
    """
    
    def __init__(self, name: str, columns: dict[str, ColumnDef]):
        """
        [START_CONTRACT_TABLE_INIT]
        Intent: Инициализация таблицы с колонками.
        Input: name - имя таблицы; columns - определения колонок.
        Output: Пустая таблица готовая к работе.
        [END_CONTRACT_TABLE_INIT]
        """
        self.name = name
        self.columns = columns
        self._rows: list[Row] = []
        self._next_row_id = 0
        self._lock = threading.RLock()
        
        # Indexes: index_name -> IndexInfo
        self._indexes: dict[str, IndexInfo] = {}
        # Column to index mapping: column_name -> index_name
        self._column_indexes: dict[str, str] = {}
        
        # Unique constraint tracking (includes PRIMARY KEY columns)
        self._unique_values: dict[str, set[Any]] = {
            col.name: set() for col in columns.values() if col.unique or col.primary_key
        }
        
        # Primary key tracking
        self._pk_column: Optional[str] = None
        for col in columns.values():
            if col.primary_key:
                self._pk_column = col.name
                break
    
    @property
    def column_names(self) -> list[str]:
        """Возвращает имена колонок."""
        return list(self.columns.keys())
    
    @property
    def row_count(self) -> int:
        """Возвращает количество строк."""
        with self._lock:
            return len(self._rows)
    
    @property
    def index_names(self) -> list[str]:
        """Возвращает имена индексов."""
        return list(self._indexes.keys())
    
    def has_index(self, column_name: str) -> bool:
        """Проверяет наличие индекса на колонке."""
        return column_name in self._column_indexes
    
    def get_index_name(self, column_name: str) -> Optional[str]:
        """Возвращает имя индекса для колонки."""
        return self._column_indexes.get(column_name)
    
    def create_index(self, index_name: str, column_name: str, 
                     unique: bool = False) -> None:
        """
        [START_CONTRACT_CREATE_INDEX]
        Intent: Создать индекс на колонке.
        Input: index_name - имя индекса; column_name - колонка; unique - уникальность.
        Output: Индекс создан и зарегистрирован.
        Raises: ColumnNotFoundError - колонка не найдена.
        [END_CONTRACT_CREATE_INDEX]
        """
        with self._lock:
            if column_name not in self.columns:
                raise ColumnNotFoundError(f"Column '{column_name}' not found")
            
            if index_name in self._indexes:
                raise TableError(f"Index '{index_name}' already exists")
            
            if column_name in self._column_indexes:
                raise TableError(f"Column '{column_name}' already has an index")
            
            self._indexes[index_name] = IndexInfo(
                name=index_name,
                column_name=column_name,
                unique=unique
            )
            self._column_indexes[column_name] = index_name
    
    def drop_index(self, index_name: str) -> None:
        """Удаляет индекс."""
        with self._lock:
            if index_name not in self._indexes:
                raise IndexNotFoundError(f"Index '{index_name}' not found")
            
            column_name = self._indexes[index_name].column_name
            del self._indexes[index_name]
            del self._column_indexes[column_name]
    
    def insert(self, data: dict[str, Any]) -> Row:
        """
        [START_CONTRACT_INSERT]
        Intent: Вставить новую строку.
        Input: data - словарь значений {колонка: значение}.
        Output: Созданная строка с присвоенным row_id.
        Raises: ValidationError - невалидные данные;
                DuplicateKeyError - нарушение уникальности.
        [END_CONTRACT_INSERT]
        """
        with self._lock:
            # Validate and fill defaults
            validated_data = self._validate_and_fill(data)
            
            # Check unique constraints
            for col_name, values in self._unique_values.items():
                value = validated_data.get(col_name)
                if value is not None and value in values:
                    raise DuplicateKeyError(
                        f"Duplicate value '{value}' for unique column '{col_name}'"
                    )
            
            # Create row
            row = Row(data=validated_data, row_id=self._next_row_id)
            self._next_row_id += 1
            self._rows.append(row)
            
            # Update unique tracking
            for col_name, values in self._unique_values.items():
                value = validated_data.get(col_name)
                if value is not None:
                    values.add(value)
            
            return row
    
    def insert_many(self, rows: list[dict[str, Any]]) -> list[Row]:
        """Вставляет несколько строк."""
        return [self.insert(row) for row in rows]
    
    def select(
        self,
        columns: Optional[list[str]] = None,
        where: Optional[Callable[[dict], bool]] = None
    ) -> list[Row]:
        """
        [START_CONTRACT_SELECT]
        Intent: Выбрать строки из таблицы.
        Input: columns - список колонок (None = все);
               where - функция-предикат для фильтрации.
        Output: Список строк удовлетворяющих условию.
        [END_CONTRACT_SELECT]
        """
        with self._lock:
            result = []
            for row in self._rows:
                if where is None or where(row.data):
                    if columns:
                        filtered_data = {k: row.data.get(k) for k in columns}
                        result.append(Row(data=filtered_data, row_id=row.row_id))
                    else:
                        result.append(row)
            return result
    
    def select_by_row_ids(self, row_ids: set[int]) -> list[Row]:
        """
        [START_CONTRACT_SELECT_BY_ROW_IDS]
        Intent: Выбрать строки по списку row_id (для index scan).
        Input: row_ids - множество идентификаторов строк.
        Output: Список строк с указанными row_id.
        [END_CONTRACT_SELECT_BY_ROW_IDS]
        """
        with self._lock:
            result = []
            for row in self._rows:
                if row.row_id in row_ids:
                    result.append(row)
            return result
    
    def update(
        self,
        updates: dict[str, Any],
        where: Optional[Callable[[dict], bool]] = None
    ) -> int:
        """
        [START_CONTRACT_UPDATE]
        Intent: Обновить строки.
        Input: updates - {колонка: новое_значение};
               where - функция-предикат.
        Output: Количество обновлённых строк.
        [END_CONTRACT_UPDATE]
        """
        with self._lock:
            count = 0
            for row in self._rows:
                if where is None or where(row.data):
                    # Remove old unique values
                    for col_name in self._unique_values:
                        old_val = row.data.get(col_name)
                        if old_val is not None:
                            self._unique_values[col_name].discard(old_val)
                    
                    # Apply updates
                    for col, val in updates.items():
                        if col in self.columns:
                            row.data[col] = val
                    
                    # Validate
                    self._validate_row(row.data)
                    
                    # Add new unique values
                    for col_name in self._unique_values:
                        new_val = row.data.get(col_name)
                        if new_val is not None:
                            if new_val in self._unique_values[col_name]:
                                # Rollback
                                raise DuplicateKeyError(
                                    f"Duplicate value '{new_val}' for '{col_name}'"
                                )
                            self._unique_values[col_name].add(new_val)
                    
                    count += 1
            return count
    
    def delete(self, where: Optional[Callable[[dict], bool]] = None) -> int:
        """
        [START_CONTRACT_DELETE]
        Intent: Удалить строки.
        Input: where - функция-предикат.
        Output: Количество удалённых строк.
        [END_CONTRACT_DELETE]
        """
        with self._lock:
            to_delete = []
            for i, row in enumerate(self._rows):
                if where is None or where(row.data):
                    to_delete.append(i)
                    # Remove from unique tracking
                    for col_name in self._unique_values:
                        val = row.data.get(col_name)
                        if val is not None:
                            self._unique_values[col_name].discard(val)
            
            # Delete from end to preserve indices
            for i in reversed(to_delete):
                del self._rows[i]
            
            return len(to_delete)
    
    def get_row_by_id(self, row_id: int) -> Optional[Row]:
        """Получить строку по row_id."""
        with self._lock:
            for row in self._rows:
                if row.row_id == row_id:
                    return row
            return None
    
    def get_all_row_ids(self) -> list[int]:
        """Возвращает все row_id (для индексации)."""
        with self._lock:
            return [row.row_id for row in self._rows]
    
    def clear(self) -> None:
        """Очищает таблицу."""
        with self._lock:
            self._rows.clear()
            for values in self._unique_values.values():
                values.clear()
    
    def _validate_and_fill(self, data: dict[str, Any]) -> dict[str, Any]:
        """Валидирует данные и заполняет значения по умолчанию."""
        result = {}
        
        for col_name, col_def in self.columns.items():
            value = data.get(col_name)
            
            # Apply default if missing
            if value is None and col_def.default is not None:
                value = col_def.default
            
            # Check nullability
            if value is None and not col_def.nullable:
                raise ValidationError(
                    f"Column '{col_name}' cannot be NULL"
                )
            
            # Validate type
            if value is not None and not col_def.validate_value(value):
                raise ValidationError(
                    f"Invalid value '{value}' for column '{col_name}' "
                    f"(expected {col_def.data_type.name})"
                )
            
            result[col_name] = value
        
        return result
    
    def _validate_row(self, data: dict[str, Any]) -> None:
        """Валидирует строку данных."""
        for col_name, col_def in self.columns.items():
            value = data.get(col_name)
            if value is None and not col_def.nullable:
                raise ValidationError(f"Column '{col_name}' cannot be NULL")
            if value is not None and not col_def.validate_value(value):
                raise ValidationError(
                    f"Invalid value for column '{col_name}'"
                )
    
    def __iter__(self) -> Iterator[Row]:
        """Итератор по строкам."""
        with self._lock:
            return iter(list(self._rows))
    
    def __len__(self) -> int:
        return self.row_count
    
    def __repr__(self) -> str:
        return f"Table(name='{self.name}', columns={len(self.columns)}, rows={len(self._rows)})"

# END_BLOCK_TABLE