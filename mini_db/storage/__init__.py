# START_MODULE_CONTRACT
# Module: mini_db.storage
# Intent: In-memory хранилище данных: Database, Table, Index.
#         Insertion order preservation, UNIQUE constraints.
# END_MODULE_CONTRACT

from __future__ import annotations

from mini_db.storage.database import Database
from mini_db.storage.table import Table, InsertResult, UpdateResult, DeleteResult, SelectResult
from mini_db.storage.index import HashIndex

__all__ = [
    "Database",
    "Table",
    "InsertResult",
    "UpdateResult",
    "DeleteResult",
    "SelectResult",
    "HashIndex",
]