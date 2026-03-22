# START_MODULE_CONTRACT
# Module: mini_db.executor.rollback
# Intent: Механизм отката для атомарных операций UPDATE.
#         Создание снимка таблицы перед изменением и восстановление при ошибке.
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports:
#   - TableSnapshot: снимок состояния таблицы
#   - RollbackManager: менеджер отката с методами snapshot/restore
# END_MODULE_MAP

from __future__ import annotations

from copy import deepcopy
from typing import Any


# START_BLOCK_TABLE_SNAPSHOT
class TableSnapshot:
    """
    [START_CONTRACT_TABLE_SNAPSHOT]
    Intent: Снимок состояния таблицы для отката при ошибке.
    Input: rows - список строк; unique_indexes - словарь множеств уникальных значений.
    Output: Immutable хранилище состояния для восстановления.
    [END_CONTRACT_TABLE_SNAPSHOT]
    """
    
    def __init__(
        self,
        rows: list[dict],
        unique_indexes: dict[str, set]
    ):
        self.rows = deepcopy(rows)
        self.unique_indexes = deepcopy(unique_indexes)
# END_BLOCK_TABLE_SNAPSHOT


# START_BLOCK_ROLLBACK_MANAGER
class RollbackManager:
    """
    [START_CONTRACT_ROLLBACK_MANAGER]
    Intent: Управление снимками таблицы для атомарных операций.
    Output: Методы для создания снимка и восстановления состояния.
    [END_CONTRACT_ROLLBACK_MANAGER]
    """
    
    def __init__(self):
        self._snapshots: dict[str, TableSnapshot] = {}
    
    def create_snapshot(
        self,
        table_name: str,
        rows: list[dict],
        unique_indexes: dict[str, set]
    ) -> None:
        """
        [START_CONTRACT_CREATE_SNAPSHOT]
        Intent: Создать снимок состояния таблицы перед изменением.
        Input: table_name - имя таблицы; rows, unique_indexes - текущее состояние.
        Output: Сохранённый снимок в _snapshots.
        [END_CONTRACT_CREATE_SNAPSHOT]
        """
        self._snapshots[table_name] = TableSnapshot(rows, unique_indexes)
    
    def restore(
        self,
        table_name: str
    ) -> tuple[list[dict], dict[str, set]] | None:
        """
        [START_CONTRACT_RESTORE]
        Intent: Восстановить состояние таблицы из снимка.
        Input: table_name - имя таблицы для восстановления.
        Output: tuple (rows, unique_indexes) или None если снимок не найден.
        [END_CONTRACT_RESTORE]
        """
        snapshot = self._snapshots.get(table_name)
        if snapshot is None:
            return None
        return (snapshot.rows, snapshot.unique_indexes)
    
    def clear_snapshot(self, table_name: str) -> None:
        """
        [START_CONTRACT_CLEAR_SNAPSHOT]
        Intent: Удалить снимок после успешной операции.
        Input: table_name - имя таблицы.
        Output: Удалённый снимок из _snapshots.
        [END_CONTRACT_CLEAR_SNAPSHOT]
        """
        self._snapshots.pop(table_name, None)
# END_BLOCK_ROLLBACK_MANAGER