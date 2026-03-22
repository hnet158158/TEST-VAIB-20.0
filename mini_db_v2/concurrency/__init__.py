# START_MODULE_CONTRACT
# Module: mini_db_v2.concurrency
# Intent: Управление транзакциями, блокировками и deadlocks для MVCC.
# Dependencies: threading
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports:
#   - TransactionManager, TransactionInfo, IsolationLevel, TransactionState
#   - LockManager, LockType, LockMode, LockError, LockTimeoutError, LockConflictError, DeadlockError
#   - DeadlockDetector, VictimSelectionPolicy, DeadlockInfo
# END_MODULE_MAP

from mini_db_v2.concurrency.transaction import (
    TransactionManager,
    TransactionInfo,
    IsolationLevel,
    TransactionState,
)

from mini_db_v2.concurrency.lock_manager import (
    LockManager,
    LockType,
    LockMode,
    LockError,
    LockTimeoutError,
    LockConflictError,
    DeadlockError,
    LockCompatibility,
    LockEntry,
    WaitEntry,
    create_lock_manager,
    resource_key,
)

from mini_db_v2.concurrency.deadlock import (
    DeadlockDetector,
    VictimSelectionPolicy,
    DeadlockInfo,
    TarjanSCCDetector,
    create_deadlock_detector,
)

__all__ = [
    # Transaction
    "TransactionManager",
    "TransactionInfo",
    "IsolationLevel",
    "TransactionState",
    # Lock Manager
    "LockManager",
    "LockType",
    "LockMode",
    "LockError",
    "LockTimeoutError",
    "LockConflictError",
    "DeadlockError",
    "LockCompatibility",
    "LockEntry",
    "WaitEntry",
    "create_lock_manager",
    "resource_key",
    # Deadlock Detection
    "DeadlockDetector",
    "VictimSelectionPolicy",
    "DeadlockInfo",
    "TarjanSCCDetector",
    "create_deadlock_detector",
]