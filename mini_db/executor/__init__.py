# START_MODULE_CONTRACT
# Module: mini_db.executor
# Intent: Выполнение SQL-операторов над базой данных.
#         Executor: dispatch AST to appropriate handlers.
#         RollbackManager: atomic UPDATE with rollback support.
# END_MODULE_CONTRACT

from __future__ import annotations

from mini_db.executor.executor import Executor, ExecutionResult
from mini_db.executor.rollback import RollbackManager, TableSnapshot

__all__ = [
    "Executor",
    "ExecutionResult",
    "RollbackManager",
    "TableSnapshot",
]