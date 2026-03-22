# START_MODULE_CONTRACT
# Module: mini_db_v2.storage.mvcc
# Intent: Multi-Version Concurrency Control structures и visibility rules.
# Dependencies: dataclasses, typing, datetime, threading
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: RowVersion, Snapshot, VisibilityChecker, MVCCError
# END_MODULE_MAP

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional, Set
from datetime import datetime
from enum import Enum, auto
import threading


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
# START_BLOCK_ERRORS
# =============================================================================

class MVCCError(Exception):
    """Базовая ошибка MVCC."""
    pass


class TransactionError(MVCCError):
    """Ошибка транзакции."""
    pass


class TransactionNotFoundError(MVCCError):
    """Транзакция не найдена."""
    pass


class TransactionAbortedError(MVCCError):
    """Транзакция была отменена."""
    pass


class SnapshotError(MVCCError):
    """Ошибка создания snapshot."""
    pass


# END_BLOCK_ERRORS


# =============================================================================
# START_BLOCK_ROW_VERSION
# =============================================================================

@dataclass
class RowVersion:
    """
    [START_CONTRACT_ROW_VERSION]
    Intent: Версия строки для MVCC version chain.
    Input: data - данные строки; xmin - XID создателя; xmax - XID удалившего.
    Output: Структура для хранения версии строки с MVCC метаданными.
    Note: xmax == 0 означает что версия активна (не удалена/обновлена).
    [END_CONTRACT_ROW_VERSION]
    """
    data: dict[str, Any]
    xmin: int  # XID that created this version
    xmax: int = 0  # XID that deleted/updated this version (0 if alive)
    created_at: datetime = field(default_factory=datetime.now)
    row_id: int = -1  # Physical row identifier
    
    def is_visible_to(self, xid: int, snapshot: 'Snapshot') -> bool:
        """
        [START_CONTRACT_IS_VISIBLE_TO]
        Intent: Проверить видимость версии строки для транзакции.
        Input: xid - ID транзакции; snapshot - snapshot транзакции.
        Output: True если версия видима.
        [END_CONTRACT_IS_VISIBLE_TO]
        """
        return VisibilityChecker.is_visible(self, xid, snapshot)
    
    def is_alive(self) -> bool:
        """Проверяет, активна ли версия (не удалена)."""
        return self.xmax == 0


# END_BLOCK_ROW_VERSION


# =============================================================================
# START_BLOCK_SNAPSHOT
# =============================================================================

@dataclass
class Snapshot:
    """
    [START_CONTRACT_SNAPSHOT]
    Intent: Snapshot транзакции для определения видимости строк.
    Input: xid - ID текущей транзакции; active_xids - активные транзакции.
    Output: Структура для visibility checks.
    Note: Строка видима если создана закоммиченной транзакцией до snapshot.
    [END_CONTRACT_SNAPSHOT]
    """
    xid: int  # Current transaction ID
    active_xids: Set[int]  # Active transactions at snapshot time
    xmin: int  # Lowest active XID
    xmax: int  # Highest XID + 1
    created_at: datetime = field(default_factory=datetime.now)
    
    def is_active(self, xid: int) -> bool:
        """Проверяет, была ли транзакция активна на момент snapshot."""
        return xid in self.active_xids
    
    def is_committed_before_snapshot(self, xid: int) -> bool:
        """
        [START_CONTRACT_IS_COMMITTED_BEFORE]
        Intent: Проверить, была ли транзакция закоммичена до snapshot.
        Input: xid - ID транзакции для проверки.
        Output: True если транзакция не была активна (закоммичена или не существует).
        [END_CONTRACT_IS_COMMITTED_BEFORE]
        """
        # Транзакция не может быть закоммичена до snapshot если это сама текущая транзакция
        if xid == self.xid:
            return False
        # Если XID не в active_xids и XID < self.xmax, значит была закоммичена
        return xid not in self.active_xids and xid < self.xmax


# END_BLOCK_SNAPSHOT


# =============================================================================
# START_BLOCK_VISIBILITY
# =============================================================================

class VisibilityChecker:
    """
    [START_CONTRACT_VISIBILITY_CHECKER]
    Intent: Проверка видимости версий строк по правилам MVCC.
    Input: RowVersion, XID, Snapshot.
    Output: Результат visibility check.
    Note: Реализует PostgreSQL-style visibility rules.
    [END_CONTRACT_VISIBILITY_CHECKER]
    """
    
    @staticmethod
    def is_visible(version: RowVersion, xid: int, snapshot: Snapshot) -> bool:
        """
        [START_CONTRACT_IS_VISIBLE]
        Intent: Проверить видимость версии строки для транзакции.
        Input: version - версия строки; xid - ID транзакции; snapshot - snapshot.
        Output: True если версия видима транзакции.
        
        Visibility Rules (PostgreSQL-style):
        1. xmin must be committed and not in active_xids
        2. xmax must be 0 (alive) OR xmax not committed OR xmax in active_xids
        3. xmin < snapshot.xmax (created before snapshot)
        4. Special case: own inserts are visible
        [END_CONTRACT_IS_VISIBLE]
        """
        # Rule 1: Own inserts are always visible
        if version.xmin == xid:
            # Created by current transaction
            if version.xmax == 0:
                return True
            # Deleted by current transaction - not visible
            if version.xmax == xid:
                return False
        
        # Rule 2: xmin must be committed (not in active list)
        if version.xmin in snapshot.active_xids:
            return False
        
        # Rule 3: xmin must be before our snapshot
        if version.xmin >= snapshot.xmax:
            return False
        
        # Rule 4: xmax check
        if version.xmax == 0:
            # Version is alive
            return True
        
        # Rule 5: If deleted by current transaction - not visible
        if version.xmax == xid:
            return False
        
        # Rule 6: If xmax is active (not committed) - version is still visible
        if version.xmax in snapshot.active_xids:
            return True
        
        # Rule 7: If xmax committed before snapshot - not visible
        if version.xmax < snapshot.xmax and version.xmax not in snapshot.active_xids:
            return False
        
        # Default: visible
        return True
    
    @staticmethod
    def find_visible_version(
        versions: list[RowVersion],
        xid: int,
        snapshot: Snapshot
    ) -> Optional[RowVersion]:
        """
        [START_CONTRACT_FIND_VISIBLE]
        Intent: Найти видимую версию из цепочки версий.
        Input: versions - список версий строки; xid - ID транзакции; snapshot.
        Output: Видимая версия или None.
        [END_CONTRACT_FIND_VISIBLE]
        """
        # Versions are ordered newest to oldest
        for version in versions:
            if VisibilityChecker.is_visible(version, xid, snapshot):
                return version
        return None


# END_BLOCK_VISIBILITY


# =============================================================================
# START_BLOCK_VERSION_CHAIN
# =============================================================================

class VersionChain:
    """
    [START_CONTRACT_VERSION_CHAIN]
    Intent: Цепочка версий строки для MVCC.
    Input: row_id - идентификатор строки.
    Output: Управление версиями строки с поддержкой MVCC.
    [END_CONTRACT_VERSION_CHAIN]
    """
    
    def __init__(self, row_id: int):
        """
        [START_CONTRACT_VERSION_CHAIN_INIT]
        Intent: Инициализация цепочки версий.
        Input: row_id - идентификатор строки.
        Output: Пустая цепочка версий.
        [END_CONTRACT_VERSION_CHAIN_INIT]
        """
        self.row_id = row_id
        self._versions: list[RowVersion] = []
        self._lock = threading.RLock()
    
    def insert(self, data: dict[str, Any], xid: int) -> RowVersion:
        """
        [START_CONTRACT_VC_INSERT]
        Intent: Вставить новую версию строки.
        Input: data - данные; xid - ID транзакции.
        Output: Созданная версия строки.
        [END_CONTRACT_VC_INSERT]
        """
        with self._lock:
            version = RowVersion(
                data=data.copy(),
                xmin=xid,
                xmax=0,
                row_id=self.row_id
            )
            self._versions.insert(0, version)  # Newest first
            return version
    
    def update(
        self,
        new_data: dict[str, Any],
        xid: int,
        snapshot: Snapshot
    ) -> Optional[RowVersion]:
        """
        [START_CONTRACT_VC_UPDATE]
        Intent: Обновить строку (создать новую версию).
        Input: new_data - новые данные; xid - ID транзакции; snapshot.
        Output: Новая версия или None если текущая не видима.
        [END_CONTRACT_VC_UPDATE]
        """
        with self._lock:
            # Find current visible version
            current = VisibilityChecker.find_visible_version(
                self._versions, xid, snapshot
            )
            if current is None:
                return None
            
            # Mark current as deleted by this transaction
            current.xmax = xid
            
            # Create new version
            new_version = RowVersion(
                data=new_data.copy(),
                xmin=xid,
                xmax=0,
                row_id=self.row_id
            )
            self._versions.insert(0, new_version)
            return new_version
    
    def delete(self, xid: int, snapshot: Snapshot) -> bool:
        """
        [START_CONTRACT_VC_DELETE]
        Intent: Удалить строку (пометить версию как удалённую).
        Input: xid - ID транзакции; snapshot.
        Output: True если удаление успешно.
        [END_CONTRACT_VC_DELETE]
        """
        with self._lock:
            current = VisibilityChecker.find_visible_version(
                self._versions, xid, snapshot
            )
            if current is None:
                return False
            
            current.xmax = xid
            return True
    
    def get_visible(self, xid: int, snapshot: Snapshot) -> Optional[RowVersion]:
        """Получить видимую версию."""
        with self._lock:
            return VisibilityChecker.find_visible_version(
                self._versions, xid, snapshot
            )
    
    def get_all_versions(self) -> list[RowVersion]:
        """Получить все версии (для vacuum)."""
        with self._lock:
            return list(self._versions)
    
    def vacuum(self, oldest_xid: int) -> int:
        """
        [START_CONTRACT_VC_VACUUM]
        Intent: Удалить старые версии, не нужные ни одной транзакции.
        Input: oldest_xid - ID самой старой активной транзакции.
        Output: Количество удалённых версий.
        [END_CONTRACT_VC_VACUUM]
        """
        with self._lock:
            # Keep at least one visible version
            if len(self._versions) <= 1:
                return 0
            
            # Remove versions that are:
            # 1. Not the latest
            # 2. xmax != 0 (deleted/updated)
            # 3. xmax < oldest_xid (deleted by committed transaction)
            to_remove = []
            for i, version in enumerate(self._versions[1:], 1):  # Skip latest
                if version.xmax != 0 and version.xmax < oldest_xid:
                    to_remove.append(i)
            
            # Remove from end to preserve indices
            for i in reversed(to_remove):
                del self._versions[i]
            
            return len(to_remove)
    
    @property
    def version_count(self) -> int:
        """Количество версий в цепочке."""
        with self._lock:
            return len(self._versions)


# END_BLOCK_VERSION_CHAIN


# =============================================================================
# START_BLOCK_TRANSACTION_INFO
# =============================================================================

@dataclass
class TransactionInfo:
    """
    [START_CONTRACT_TRANSACTION_INFO]
    Intent: Информация о транзакции для MVCC.
    Input: xid - ID транзакции; isolation_level - уровень изоляции.
    Output: Структура для отслеживания состояния транзакции.
    [END_CONTRACT_TRANSACTION_INFO]
    """
    xid: int
    state: TransactionState = TransactionState.ACTIVE
    isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED
    snapshot: Optional[Snapshot] = None
    started_at: datetime = field(default_factory=datetime.now)
    committed_at: Optional[datetime] = None


# END_BLOCK_TRANSACTION_INFO