# START_MODULE_CONTRACT
# Module: mini_db.storage.table
# Intent: In-memory таблица с операциями CRUD и поддержкой UNIQUE constraints.
#         Insertion order preservation через list[dict].
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports:
#   - InsertResult: результат операции INSERT
#   - UpdateResult: результат операции UPDATE
#   - DeleteResult: результат операции DELETE
#   - SelectResult: результат операции SELECT
#   - Table: класс таблицы с методами insert, update, delete, select
# END_MODULE_MAP

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

from mini_db.ast.nodes import ColumnDef
from mini_db.storage.index import HashIndex


# START_BLOCK_RESULT_TYPES
@dataclass
class InsertResult:
    """
    [START_CONTRACT_INSERT_RESULT]
    Intent: Результат операции INSERT с флагом успеха и сообщением об ошибке.
    Output: success=True при успехе, иначе success=False с error message.
    [END_CONTRACT_INSERT_RESULT]
    """
    success: bool
    rows_affected: int = 0
    error: Optional[str] = None


@dataclass
class UpdateResult:
    """
    [START_CONTRACT_UPDATE_RESULT]
    Intent: Результат операции UPDATE с количеством изменённых строк.
    Output: success=True при успехе, иначе success=False с error message.
    [END_CONTRACT_UPDATE_RESULT]
    """
    success: bool
    rows_affected: int = 0
    error: Optional[str] = None


@dataclass
class DeleteResult:
    """
    [START_CONTRACT_DELETE_RESULT]
    Intent: Результат операции DELETE с количеством удалённых строк.
    Output: success=True при успехе, иначе success=False с error message.
    [END_CONTRACT_DELETE_RESULT]
    """
    success: bool
    rows_affected: int = 0
    error: Optional[str] = None


@dataclass
class SelectResult:
    """
    [START_CONTRACT_SELECT_RESULT]
    Intent: Результат операции SELECT с данными.
    Output: success=True с data=list[dict], иначе success=False с error.
    [END_CONTRACT_SELECT_RESULT]
    """
    success: bool
    data: Optional[list[dict]] = None
    error: Optional[str] = None
# END_BLOCK_RESULT_TYPES


# START_BLOCK_TABLE
class Table:
    """
    [START_CONTRACT_TABLE]
    Intent: In-memory таблица с колонками, строками и UNIQUE constraints.
    Input: name - имя таблицы; columns - список ColumnDef.
    Output: Объект с методами insert, update, delete, select.
    [END_CONTRACT_TABLE]
    """
    
    def __init__(self, name: str, columns: list[ColumnDef]):
        self.name = name
        self.columns: dict[str, ColumnDef] = {col.name: col for col in columns}
        self.column_order: list[str] = [col.name for col in columns]
        self.rows: list[dict] = []  # insertion order
        self.unique_indexes: dict[str, set] = {}  # column -> set of values (UNIQUE constraint)
        self.indexes: dict[str, HashIndex] = {}  # index_name -> HashIndex (user indexes)
        
        # Initialize unique tracking
        for col in columns:
            if col.unique:
                self.unique_indexes[col.name] = set()
    
    def insert(self, row: dict) -> InsertResult:
        """
        [START_CONTRACT_TABLE_INSERT]
        Intent: Вставить строку в таблицу с валидацией типов и UNIQUE.
        Input: row - dict с ключами-именами колонок и значениями.
        Output: InsertResult с успехом/ошибкой.
        [END_CONTRACT_TABLE_INSERT]
        """
        # Validate all columns exist
        for col_name in row:
            if col_name not in self.columns:
                return InsertResult(
                    success=False,
                    error=f"Unknown column '{col_name}'"
                )
        
        # Type validation
        for col_name, value in row.items():
            if value is not None:
                col_def = self.columns[col_name]
                if not self._validate_type(value, col_def.data_type):
                    return InsertResult(
                        success=False,
                        error=f"Type mismatch for column '{col_name}': expected {col_def.data_type}"
                    )
        
        # UNIQUE validation
        for col_name, value in row.items():
            if col_name in self.unique_indexes:
                if value in self.unique_indexes[col_name]:
                    return InsertResult(
                        success=False,
                        error=f"UNIQUE constraint violated on column '{col_name}'"
                    )
        
        # Insert row
        self.rows.append(row)
        row_index = len(self.rows) - 1
        
        # Update unique indexes
        for col_name, value in row.items():
            if col_name in self.unique_indexes and value is not None:
                self.unique_indexes[col_name].add(value)
        
        # Update user indexes
        for index in self.indexes.values():
            if index.column in row and row[index.column] is not None:
                index.add(row[index.column], row_index)
        
        return InsertResult(success=True, rows_affected=1)
    
    def update(
        self,
        predicate: Callable[[dict], bool],
        updates: dict[str, Any]
    ) -> UpdateResult:
        """
        [START_CONTRACT_TABLE_UPDATE]
        Intent: Атомарное обновление строк с проверкой UNIQUE constraints.
        Input: predicate - функция фильтрации; updates - dict колонка->значение.
        Output: UpdateResult с успехом/ошибкой. При UNIQUE violation - полный откат.
        [END_CONTRACT_TABLE_UPDATE]
        """
        # Validate columns exist
        for col_name in updates:
            if col_name not in self.columns:
                return UpdateResult(
                    success=False,
                    error=f"Unknown column '{col_name}'"
                )
        
        # Validate types
        for col_name, value in updates.items():
            if value is not None:
                col_def = self.columns[col_name]
                if not self._validate_type(value, col_def.data_type):
                    return UpdateResult(
                        success=False,
                        error=f"Type mismatch for column '{col_name}'"
                    )
        
        # Find rows to update
        rows_to_update = [row for row in self.rows if predicate(row)]
        
        if not rows_to_update:
            return UpdateResult(success=True, rows_affected=0)
        
        # Create snapshot for rollback
        snapshot_rows = [row.copy() for row in self.rows]
        snapshot_unique = {k: v.copy() for k, v in self.unique_indexes.items()}
        
        # Apply updates and check UNIQUE constraints
        for row in rows_to_update:
            # Remove old values from unique indexes
            for col_name in self.unique_indexes:
                old_val = row.get(col_name)
                if old_val is not None:
                    self.unique_indexes[col_name].discard(old_val)
            
            # Apply updates to row
            for col_name, value in updates.items():
                row[col_name] = value
            
            # Check UNIQUE constraints for new values
            for col_name in self.unique_indexes:
                new_val = row.get(col_name)
                if new_val is not None:
                    if new_val in self.unique_indexes[col_name]:
                        # UNIQUE violation - rollback
                        self._rollback_from_snapshot(snapshot_rows, snapshot_unique)
                        return UpdateResult(
                            success=False,
                            error=f"UNIQUE constraint violated on column '{col_name}'"
                        )
                    self.unique_indexes[col_name].add(new_val)
        
        # Rebuild user indexes after successful update
        self.rebuild_all_indexes()
        
        return UpdateResult(success=True, rows_affected=len(rows_to_update))
    
    def _rollback_from_snapshot(
        self,
        snapshot_rows: list[dict],
        snapshot_unique: dict[str, set]
    ) -> None:
        """
        [START_CONTRACT_ROLLBACK_SNAPSHOT]
        Intent: Восстановить состояние таблицы из снимка при ошибке.
        Input: snapshot_rows, snapshot_unique - сохранённое состояние.
        Output: Восстановленные self.rows и self.unique_indexes.
        [END_CONTRACT_ROLLBACK_SNAPSHOT]
        """
        self.rows.clear()
        self.rows.extend(snapshot_rows)
        for col_name in self.unique_indexes:
            self.unique_indexes[col_name].clear()
            self.unique_indexes[col_name].update(snapshot_unique.get(col_name, set()))
    
    def delete(self, predicate: Callable[[dict], bool]) -> DeleteResult:
        """
        [START_CONTRACT_TABLE_DELETE]
        Intent: Удалить строки, удовлетворяющие predicate.
        Input: predicate - функция фильтрации.
        Output: DeleteResult с количеством удалённых строк.
        [END_CONTRACT_TABLE_DELETE]
        """
        rows_to_delete = [row for row in self.rows if predicate(row)]
        
        for row in rows_to_delete:
            # Remove from unique indexes
            for col_name in self.unique_indexes:
                if col_name in row and row[col_name] is not None:
                    self.unique_indexes[col_name].discard(row[col_name])
            self.rows.remove(row)
        
        # Rebuild user indexes after successful delete
        self.rebuild_all_indexes()
        
        return DeleteResult(success=True, rows_affected=len(rows_to_delete))
    
    def select(
        self,
        predicate: Callable[[dict], bool],
        columns: Optional[list[str]] = None
    ) -> SelectResult:
        """
        [START_CONTRACT_TABLE_SELECT]
        Intent: Выбрать строки, удовлетворяющие predicate, с указанными колонками.
        Input: predicate - функция фильтрации; columns - None (все) или список имён.
        Output: SelectResult с data=list[dict].
        [END_CONTRACT_TABLE_SELECT]
        """
        result_rows = []
        
        for row in self.rows:
            if predicate(row):
                if columns is None:
                    # SELECT * - return all columns in order
                    result_row = {col: row.get(col) for col in self.column_order}
                else:
                    # SELECT col1, col2 - return specified columns
                    result_row = {col: row.get(col) for col in columns}
                result_rows.append(result_row)
        
        return SelectResult(success=True, data=result_rows)
    
    def _validate_type(self, value: Any, expected_type: str) -> bool:
        """
        [START_CONTRACT_VALIDATE_TYPE]
        Intent: Проверить соответствие значения ожидаемому типу.
        Input: value - значение; expected_type - "INT", "TEXT", "BOOL".
        Output: True если тип совпадает, иначе False.
        [END_CONTRACT_VALIDATE_TYPE]
        """
        type_map = {
            "INT": int,
            "TEXT": str,
            "BOOL": bool,
        }
        expected = type_map.get(expected_type)
        if expected is None:
            return False
        # Special case: bool is subclass of int in Python
        if expected_type == "INT" and isinstance(value, bool):
            return False
        return isinstance(value, expected)
    
    def create_index(self, index_name: str, column: str) -> tuple[bool, Optional[str]]:
        """
        [START_CONTRACT_CREATE_INDEX]
        Intent: Создать пользовательский индекс на колонке.
        Input: index_name - уникальное имя индекса; column - имя существующей колонки.
        Output: (True, None) при успехе, (False, error) при ошибке.
        [END_CONTRACT_CREATE_INDEX]
        """
        # Check index name uniqueness
        if index_name in self.indexes:
            return False, f"Index '{index_name}' already exists"
        
        # Check column exists
        if column not in self.columns:
            return False, f"Column '{column}' does not exist"
        
        # Create and populate index
        index = HashIndex(column)
        index.rebuild(self.rows, column)
        self.indexes[index_name] = index
        
        return True, None
    
    def get_index_for_column(self, column: str) -> Optional[HashIndex]:
        """
        [START_CONTRACT_GET_INDEX_FOR_COLUMN]
        Intent: Найти индекс для указанной колонки (если существует).
        Input: column - имя колонки.
        Output: HashIndex или None если индекс не существует.
        [END_CONTRACT_GET_INDEX_FOR_COLUMN]
        """
        for index in self.indexes.values():
            if index.column == column:
                return index
        return None
    
    def rebuild_all_indexes(self) -> None:
        """
        [START_CONTRACT_REBUILD_ALL_INDEXES]
        Intent: Перестроить все пользовательские индексы (после LOAD).
        Output: Все индексы в self.indexes перестроены по текущим rows.
        [END_CONTRACT_REBUILD_ALL_INDEXES]
        """
        for index in self.indexes.values():
            index.rebuild(self.rows, index.column)
    
    def to_dict(self) -> dict[str, Any]:
        """
        [START_CONTRACT_TABLE_TO_DICT]
        Intent: Сериализовать таблицу в словарь для JSON.
        Output: dict с именем, колонками, строками и unique_indexes.
        [END_CONTRACT_TABLE_TO_DICT]
        """
        columns_data = []
        for col_name in self.column_order:
            col_def = self.columns[col_name]
            columns_data.append({
                "name": col_def.name,
                "data_type": col_def.data_type,
                "unique": col_def.unique
            })
        
        return {
            "name": self.name,
            "columns": columns_data,
            "rows": self.rows,
            "unique_indexes": {
                col: list(values)
                for col, values in self.unique_indexes.items()
            },
            "indexes": {
                name: {"column": idx.column}
                for name, idx in self.indexes.items()
            }
        }
    
    def from_dict(self, data: dict[str, Any]) -> None:
        """
        [START_CONTRACT_TABLE_FROM_DICT]
        Intent: Десериализовать таблицу из словаря (JSON).
        Input: data - dict с ключами name, columns, rows, unique_indexes.
        Output: Восстановленные columns, rows, unique_indexes.
        [END_CONTRACT_TABLE_FROM_DICT]
        """
        self.name = data.get("name", "")
        
        # Restore columns
        columns_data = data.get("columns", [])
        self.columns = {}
        self.column_order = []
        self.unique_indexes = {}
        
        for col_data in columns_data:
            col_def = ColumnDef(
                name=col_data["name"],
                data_type=col_data["data_type"],
                unique=col_data.get("unique", False)
            )
            self.columns[col_def.name] = col_def
            self.column_order.append(col_def.name)
            
            if col_def.unique:
                self.unique_indexes[col_def.name] = set()
        
        # Restore rows
        self.rows = data.get("rows", [])
        
        # Restore unique indexes
        unique_data = data.get("unique_indexes", {})
        for col_name, values in unique_data.items():
            if col_name in self.unique_indexes:
                self.unique_indexes[col_name] = set(values)
        
        # Restore user indexes (rebuild from rows)
        indexes_data = data.get("indexes", {})
        for idx_name, idx_data in indexes_data.items():
            column = idx_data.get("column")
            if column and column in self.columns:
                index = HashIndex(column)
                index.rebuild(self.rows, column)
                self.indexes[idx_name] = index
# END_BLOCK_TABLE