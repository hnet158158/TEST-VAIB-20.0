# START_MODULE_CONTRACT
# Module: mini_db_v2.concurrency.transaction
# Intent: Менеджер транзакций с поддержкой MVCC и isolation levels.
# Dependencies: threading, dataclasses, mini_db_v2.storage.mvcc
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: TransactionManager, TransactionInfo, IsolationLevel, TransactionState
# END_MODULE_MAP

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Dict, Set
from datetime import datetime
from enum import Enum, auto
import threading
from mini_db_v2.storage.mvcc import (
    Snapshot,
    TransactionInfo as MVCCInfo,
    TransactionState as MVCCState,
    IsolationLevel as MVCCIsolationLevel,
    TransactionError,
    TransactionNotFoundError,
    SnapshotError,
)


# =============================================================================
# START_BLOCK_ENUMS
# =============================================================================

class IsolationLevel(Enum):
    """Уровни изоляции транзакций."""
    READ_COMMITTED = auto()
    REPEATABLE_READ = auto()


class TransactionState(Enum):
    """Состояния транзакции."""
    ACTIVE = auto()
    COMMITTED = auto()
    ABORTED = auto()

# END_BLOCK_ENUMS


# =============================================================================
# START_BLOCK_TRANSACTION_INFO
# =============================================================================

@dataclass
class TransactionInfo:
    """
    [START_CONTRACT_TRANSACTION_INFO]
    Intent: Информация о транзакции для отслеживания состояния.
    Input: xid - ID транзакции; isolation_level - уровень изоляции.
    Output: Структура с полным состоянием транзакции.
    [END_CONTRACT_TRANSACTION_INFO]
    """
    xid: int
    state: TransactionState = TransactionState.ACTIVE
    isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED
    snapshot: Optional[Snapshot] = None
    started_at: datetime = field(default_factory=datetime.now)
    committed_at: Optional[datetime] = None
    
    def is_active(self) -> bool:
        """Проверяет, активна ли транзакция."""
        return self.state == TransactionState.ACTIVE
    
    def is_committed(self) -> bool:
        """Проверяет, закоммичена ли транзакция."""
        return self.state == TransactionState.COMMITTED
    
    def is_aborted(self) -> bool:
        """Проверяет, отменена ли транзакция."""
        return self.state == TransactionState.ABORTED

# END_BLOCK_TRANSACTION_INFO


# =============================================================================
# START_BLOCK_TRANSACTION_MANAGER
# =============================================================================

class TransactionManager:
    """
    [START_CONTRACT_TRANSACTION_MANAGER]
    Intent: Управление транзакциями с MVCC поддержкой.
    Input: Операции BEGIN, COMMIT, ROLLBACK.
    Output: XID, snapshots, управление видимостью.
    Note: Поддерживает READ COMMITTED и REPEATABLE READ.
    [END_CONTRACT_TRANSACTION_MANAGER]
    """
    
    def __init__(self):
        """
        [START_CONTRACT_TM_INIT]
        Intent: Инициализация менеджера транзакций.
        Input: Нет.
        Output: Готовый к работе менеджер с next_xid = 1.
        [END_CONTRACT_TM_INIT]
        """
        self._next_xid = 1
        self._transactions: Dict[int, TransactionInfo] = {}
        self._active_xids: Set[int] = set()
        self._lock = threading.RLock()
        
        # For vacuum: track oldest active transaction
        self._oldest_active_xid: Optional[int] = None
    
    @property
    def next_xid(self) -> int:
        """Возвращает следующий XID."""
        with self._lock:
            return self._next_xid
    
    @property
    def active_xids(self) -> Set[int]:
        """Возвращает копию множества активных XID."""
        with self._lock:
            return set(self._active_xids)
    
    @property
    def oldest_active_xid(self) -> Optional[int]:
        """Возвращает XID самой старой активной транзакции."""
        with self._lock:
            return self._oldest_active_xid
    
    def begin(
        self,
        isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED
    ) -> int:
        """
        [START_CONTRACT_TM_BEGIN]
        Intent: Начать новую транзакцию.
        Input: isolation_level - уровень изоляции.
        Output: XID новой транзакции.
        Note: Для REPEATABLE READ создаётся snapshot сразу.
        [END_CONTRACT_TM_BEGIN]
        """
        with self._lock:
            xid = self._next_xid
            self._next_xid += 1
            
            # Create transaction info
            tx_info = TransactionInfo(
                xid=xid,
                state=TransactionState.ACTIVE,
                isolation_level=isolation_level
            )
            
            # For REPEATABLE READ, create snapshot now
            if isolation_level == IsolationLevel.REPEATABLE_READ:
                tx_info.snapshot = self._create_snapshot(xid)
            
            self._transactions[xid] = tx_info
            self._active_xids.add(xid)
            
            # Update oldest active
            if self._oldest_active_xid is None or xid < self._oldest_active_xid:
                self._oldest_active_xid = xid
            
            return xid
    
    def commit(self, xid: int) -> bool:
        """
        [START_CONTRACT_TM_COMMIT]
        Intent: Закоммитить транзакцию.
        Input: xid - ID транзакции.
        Output: True если успешно, False если транзакция не найдена.
        [END_CONTRACT_TM_COMMIT]
        """
        with self._lock:
            tx_info = self._transactions.get(xid)
            if tx_info is None:
                raise TransactionNotFoundError(f"Transaction {xid} not found")
            
            if not tx_info.is_active():
                return False
            
            tx_info.state = TransactionState.COMMITTED
            tx_info.committed_at = datetime.now()
            
            self._active_xids.discard(xid)
            self._update_oldest_active()
            
            return True
    
    def rollback(self, xid: int) -> bool:
        """
        [START_CONTRACT_TM_ROLLBACK]
        Intent: Откатить транзакцию.
        Input: xid - ID транзакции.
        Output: True если успешно, False если транзакция не найдена.
        [END_CONTRACT_TM_ROLLBACK]
        """
        with self._lock:
            tx_info = self._transactions.get(xid)
            if tx_info is None:
                raise TransactionNotFoundError(f"Transaction {xid} not found")
            
            if not tx_info.is_active():
                return False
            
            tx_info.state = TransactionState.ABORTED
            
            self._active_xids.discard(xid)
            self._update_oldest_active()
            
            return True
    
    def get_snapshot(self, xid: int) -> Snapshot:
        """
        [START_CONTRACT_TM_GET_SNAPSHOT]
        Intent: Получить snapshot для транзакции.
        Input: xid - ID транзакции.
        Output: Snapshot для visibility checks.
        Note: READ COMMITTED создаёт новый snapshot каждый раз.
              REPEATABLE READ возвращает snapshot созданный при BEGIN.
        [END_CONTRACT_TM_GET_SNAPSHOT]
        """
        with self._lock:
            tx_info = self._transactions.get(xid)
            if tx_info is None:
                raise TransactionNotFoundError(f"Transaction {xid} not found")
            
            if not tx_info.is_active():
                raise TransactionError(f"Transaction {xid} is not active")
            
            # REPEATABLE READ: return cached snapshot
            if tx_info.isolation_level == IsolationLevel.REPEATABLE_READ:
                if tx_info.snapshot is None:
                    # Should not happen, but create if missing
                    tx_info.snapshot = self._create_snapshot(xid)
                return tx_info.snapshot
            
            # READ COMMITTED: create fresh snapshot
            return self._create_snapshot(xid)
    
    def _create_snapshot(self, xid: int) -> Snapshot:
        """
        [START_CONTRACT_TM_CREATE_SNAPSHOT]
        Intent: Создать новый snapshot.
        Input: xid - ID текущей транзакции.
        Output: Snapshot с активными транзакциями.
        [END_CONTRACT_TM_CREATE_SNAPSHOT]
        """
        active_xids = set(self._active_xids)
        
        # Calculate xmin (lowest active) and xmax (next XID to be assigned)
        # xmax is the "high water mark" - all XID >= xmax are future transactions
        xmin = min(active_xids) if active_xids else xid
        xmax = self._next_xid  # Next XID to be assigned (not max active + 1)
        
        return Snapshot(
            xid=xid,
            active_xids=active_xids,
            xmin=xmin,
            xmax=xmax
        )
    
    def _update_oldest_active(self) -> None:
        """Обновляет oldest_active_xid после изменения active_xids."""
        if self._active_xids:
            self._oldest_active_xid = min(self._active_xids)
        else:
            self._oldest_active_xid = None
    
    def get_transaction_info(self, xid: int) -> Optional[TransactionInfo]:
        """Возвращает информацию о транзакции."""
        with self._lock:
            return self._transactions.get(xid)
    
    def is_active(self, xid: int) -> bool:
        """Проверяет, активна ли транзакция."""
        with self._lock:
            return xid in self._active_xids
    
    def is_committed(self, xid: int) -> bool:
        """Проверяет, закоммичена ли транзакция."""
        with self._lock:
            tx_info = self._transactions.get(xid)
            return tx_info is not None and tx_info.is_committed()
    
    def get_all_active_xids(self) -> Set[int]:
        """Возвращает все активные XID."""
        with self._lock:
            return set(self._active_xids)
    
    def get_transaction_count(self) -> int:
        """Возвращает количество транзакций."""
        with self._lock:
            return len(self._transactions)
    
    def get_active_transaction_count(self) -> int:
        """Возвращает количество активных транзакций."""
        with self._lock:
            return len(self._active_xids)
    
    def cleanup_old_transactions(self, keep_last: int = 1000) -> int:
        """
        [START_CONTRACT_TM_CLEANUP]
        Intent: Удалить старые завершённые транзакции из памяти.
        Input: keep_last - сколько последних транзакций оставить.
        Output: Количество удалённых транзакций.
        [END_CONTRACT_TM_CLEANUP]
        """
        with self._lock:
            if len(self._transactions) <= keep_last:
                return 0
            
            # Get all non-active transaction xids
            non_active = [
                xid for xid, tx in self._transactions.items()
                if not tx.is_active()
            ]
            
            # Sort and remove oldest
            non_active.sort()
            to_remove = non_active[:-keep_last] if len(non_active) > keep_last else []
            
            for xid in to_remove:
                del self._transactions[xid]
            
            return len(to_remove)

# END_BLOCK_TRANSACTION_MANAGER


# =============================================================================
# START_BLOCK_HELPERS
# =============================================================================

def create_transaction_manager() -> TransactionManager:
    """
    [START_CONTRACT_CREATE_TM]
    Intent: Фабрика для создания TransactionManager.
    Input: Нет.
    Output: Готовый к работе TransactionManager.
    [END_CONTRACT_CREATE_TM]
    """
    return TransactionManager()

# END_BLOCK_HELPERS