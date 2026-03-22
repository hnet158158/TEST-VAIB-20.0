# START_MODULE_CONTRACT
# Module: mini_db.storage.index
# Intent: Hash index для быстрого поиска строк по значению колонки.
#         Используется для UNIQUE constraint и оптимизации WHERE col = X.
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports:
#   - HashIndex: hash-индекс с методами add, remove, lookup, contains
# END_MODULE_MAP

from __future__ import annotations

from typing import Any


# START_BLOCK_HASH_INDEX
class HashIndex:
    """
    [START_CONTRACT_HASH_INDEX]
    Intent: Hash-индекс для быстрого поиска row indices по значению колонки.
    Input: column - имя колонки, по которой строится индекс.
    Output: Объект с методами add, remove, lookup, contains.
    [END_CONTRACT_HASH_INDEX]
    """
    
    def __init__(self, column: str):
        self.column = column
        self._index: dict[Any, set[int]] = {}
    
    def add(self, value: Any, row_index: int) -> None:
        """
        [START_CONTRACT_INDEX_ADD]
        Intent: Добавить связь value -> row_index в индекс.
        Input: value - значение колонки; row_index - индекс строки в таблице.
        Output: Индекс обновлён, lookup(value) будет содержать row_index.
        [END_CONTRACT_INDEX_ADD]
        """
        if value not in self._index:
            self._index[value] = set()
        self._index[value].add(row_index)
    
    def remove(self, value: Any, row_index: int) -> None:
        """
        [START_CONTRACT_INDEX_REMOVE]
        Intent: Удалить связь value -> row_index из индекса.
        Input: value - значение колонки; row_index - индекс строки для удаления.
        Output: Индекс обновлён, row_index удалён из lookup(value).
        [END_CONTRACT_INDEX_REMOVE]
        """
        if value in self._index:
            self._index[value].discard(row_index)
            if not self._index[value]:
                del self._index[value]
    
    def lookup(self, value: Any) -> set[int]:
        """
        [START_CONTRACT_INDEX_LOOKUP]
        Intent: Найти все row indices для заданного значения.
        Input: value - искомое значение колонки.
        Output: set[int] с индексами строк (пустой если значение не найдено).
        [END_CONTRACT_INDEX_LOOKUP]
        """
        return self._index.get(value, set()).copy()
    
    def contains(self, value: Any) -> bool:
        """
        [START_CONTRACT_INDEX_CONTAINS]
        Intent: Проверить, есть ли значение в индексе.
        Input: value - проверяемое значение.
        Output: True если значение есть в индексе, иначе False.
        [END_CONTRACT_INDEX_CONTAINS]
        """
        return value in self._index
    
    def clear(self) -> None:
        """
        [START_CONTRACT_INDEX_CLEAR]
        Intent: Очистить индекс.
        Output: Индекс становится пустым.
        [END_CONTRACT_INDEX_CLEAR]
        """
        self._index.clear()
    
    def rebuild(self, rows: list[dict], column: str) -> None:
        """
        [START_CONTRACT_INDEX_REBUILD]
        Intent: Перестроить индекс по списку строк.
        Input: rows - список dict с данными; column - имя колонки.
        Output: Индекс содержит все значения из rows.
        [END_CONTRACT_INDEX_REBUILD]
        """
        self.clear()
        for idx, row in enumerate(rows):
            if column in row and row[column] is not None:
                self.add(row[column], idx)
# END_BLOCK_HASH_INDEX