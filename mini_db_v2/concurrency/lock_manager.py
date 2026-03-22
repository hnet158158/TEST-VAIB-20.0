# START_MODULE_CONTRACT
# Module: mini_db_v2.concurrency.lock_manager
# Intent: Менеджер блокировок для MVCC с поддержкой row-level и table-level locks.
# Dependencies: threading, dataclasses, enum, time, typing
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: LockManager, LockType, LockMode, LockError, LockTimeoutError, LockConflictError
# END_MODULE_MAP

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Dict, Set, List, Tuple
from enum import Enum, auto
from datetime import datetime
import threading
import time
import logging

logger = logging.getLogger(__name__)


# =============================================================================
# START_BLOCK_ENUMS
# =============================================================================

class LockType(Enum):
    """Типы блокировок."""
    SHARE = auto()          # S - для чтения
    EXCLUSIVE = auto()      # X - для записи
    INTENT_SHARE = auto()   # IS - намерение получить S lock на строки
    INTENT_EXCLUSIVE = auto()  # IX - намерение получить X lock на строки


class LockMode(Enum):
    """Режимы блокировки."""
    WAIT = auto()      # Ждать освобождения
    NOWAIT = auto()    # Не ждать, сразу вернуть ошибку
    SKIP = auto()      # Пропустить если заблокировано


# END_BLOCK_ENUMS


# =============================================================================
# START_BLOCK_ERRORS
# =============================================================================

class LockError(Exception):
    """Базовая ошибка блокировки."""
    pass


class LockTimeoutError(LockError):
    """Таймаут при ожидании блокировки."""
    pass


class LockConflictError(LockError):
    """Конфликт блокировок."""
    pass


class DeadlockError(LockError):
    """Обнаружен deadlock."""
    pass


# END_BLOCK_ERRORS


# =============================================================================
# START_BLOCK_LOCK_COMPATIBILITY
# =============================================================================

class LockCompatibility:
    """
    [START_CONTRACT_LOCK_COMPATIBILITY]
    Intent: Матрица совместимости блокировок.
    Input: Два типа блокировок.
    Output: True если совместимы, False если конфликтуют.
    Note: Матрица соответствует стандарту SQL-92.
    [END_CONTRACT_LOCK_COMPATIBILITY]
    """
    
    # Lock Compatibility Matrix:
    #      S   X   IS  IX
    # S    ✓   ✗   ✓   ✗
    # X    ✗   ✗   ✗   ✗
    # IS   ✓   ✗   ✓   ✓
    # IX   ✗   ✗   ✓   ✓
    
    COMPATIBILITY_MATRIX: Dict[LockType, Set[LockType]] = {
        LockType.SHARE: {LockType.SHARE, LockType.INTENT_SHARE},
        LockType.EXCLUSIVE: set(),  # Не совместим ни с чем
        LockType.INTENT_SHARE: {
            LockType.SHARE, LockType.INTENT_SHARE, LockType.INTENT_EXCLUSIVE
        },
        LockType.INTENT_EXCLUSIVE: {LockType.INTENT_SHARE, LockType.INTENT_EXCLUSIVE},
    }
    
    @classmethod
    def is_compatible(cls, held: LockType, requested: LockType) -> bool:
        """
        [START_CONTRACT_IS_COMPATIBLE]
        Intent: Проверить совместимость двух блокировок.
        Input: held - удерживаемая блокировка; requested - запрашиваемая.
        Output: True если совместимы.
        [END_CONTRACT_IS_COMPATIBLE]
        """
        return requested in cls.COMPATIBILITY_MATRIX.get(held, set())
    
    @classmethod
    def can_grant(
        cls,
        held_locks: Set[LockType],
        requested: LockType
    ) -> bool:
        """
        [START_CONTRACT_CAN_GRANT]
        Intent: Проверить, можно ли предоставить блокировку.
        Input: held_locks - множество удерживаемых блокировок; requested - запрашиваемая.
        Output: True если можно предоставить.
        [END_CONTRACT_CAN_GRANT]
        """
        for held in held_locks:
            if not cls.is_compatible(held, requested):
                return False
        return True


# END_BLOCK_LOCK_COMPATIBILITY


# =============================================================================
# START_BLOCK_LOCK_ENTRY
# =============================================================================

@dataclass
class LockEntry:
    """
    [START_CONTRACT_LOCK_ENTRY]
    Intent: Запись о блокировке ресурса.
    Input: resource - идентификатор ресурса; lock_type - тип блокировки; xid - ID транзакции.
    Output: Структура для отслеживания блокировки.
    [END_CONTRACT_LOCK_ENTRY]
    """
    resource: str           # Идентификатор ресурса (table:row_id или table)
    lock_type: LockType     # Тип блокировки
    xid: int                # ID транзакции
    acquired_at: datetime = field(default_factory=datetime.now)
    waiting_since: Optional[datetime] = None
    
    def is_waiting(self) -> bool:
        """Проверяет, находится ли блокировка в ожидании."""
        return self.waiting_since is not None


# END_BLOCK_LOCK_ENTRY


# =============================================================================
# START_BLOCK_WAIT_ENTRY
# =============================================================================

@dataclass
class WaitEntry:
    """
    [START_CONTRACT_WAIT_ENTRY]
    Intent: Запись об ожидающей блокировке.
    Input: resource, lock_type, xid, blocked_by.
    Output: Структура для wait-for graph.
    [END_CONTRACT_WAIT_ENTRY]
    """
    resource: str
    lock_type: LockType
    xid: int
    blocked_by: Set[int]  # XID транзакций, блокирующих эту
    waiting_since: datetime = field(default_factory=datetime.now)
    timeout: float = 30.0
    event: threading.Event = field(default_factory=threading.Event)
    
    def is_timeout_expired(self) -> bool:
        """Проверяет, истёк ли таймаут."""
        elapsed = (datetime.now() - self.waiting_since).total_seconds()
        return elapsed >= self.timeout


# END_BLOCK_WAIT_ENTRY


# =============================================================================
# START_BLOCK_LOCK_MANAGER
# =============================================================================

class LockManager:
    """
    [START_CONTRACT_LOCK_MANAGER]
    Intent: Управление блокировками ресурсов для транзакций.
    Input: acquire_lock, release_lock операции.
    Output: Управление доступом к ресурсам с обнаружением deadlocks.
    Note: Поддерживает row-level и table-level блокировки.
          Default timeout = 30 секунд.
    [END_CONTRACT_LOCK_MANAGER]
    """
    
    DEFAULT_TIMEOUT = 30.0
    
    def __init__(self, deadlock_detector: Optional['DeadlockDetector'] = None):
        """
        [START_CONTRACT_LM_INIT]
        Intent: Инициализация менеджера блокировок.
        Input: deadlock_detector - опциональный детектор deadlocks.
        Output: Готовый к работе менеджер блокировок.
        [END_CONTRACT_LM_INIT]
        """
        self._locks: Dict[str, Dict[int, LockEntry]] = {}  # resource -> xid -> LockEntry
        self._waiting: Dict[int, WaitEntry] = {}  # xid -> WaitEntry
        self._lock = threading.RLock()
        self._deadlock_detector = deadlock_detector
        self._timeout = self.DEFAULT_TIMEOUT
    
    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------
    
    def acquire_lock(
        self,
        resource: str,
        lock_type: LockType,
        xid: int,
        timeout: Optional[float] = None,
        mode: LockMode = LockMode.WAIT
    ) -> bool:
        """
        [START_CONTRACT_ACQUIRE_LOCK]
        Intent: Приобрести блокировку ресурса.
        Input: resource - идентификатор ресурса; lock_type - тип блокировки;
               xid - ID транзакции; timeout - таймаут ожидания; mode - режим.
        Output: True если блокировка получена, False если нет.
        Raises: LockTimeoutError - истёк таймаут;
                DeadlockError - обнаружен deadlock.
        [END_CONTRACT_ACQUIRE_LOCK]
        """
        if timeout is None:
            timeout = self._timeout
        
        logger.debug(
            f"[LockManager][acquire_lock] Belief: Acquiring lock | "
            f"Resource: {resource} | Type: {lock_type.name} | XID: {xid}"
        )
        
        with self._lock:
            # Попытка получить блокировку без ожидания
            if self._try_acquire(resource, lock_type, xid):
                logger.debug(
                    f"[LockManager][acquire_lock] Belief: Lock acquired immediately | "
                    f"Resource: {resource} | XID: {xid}"
                )
                return True
            
            # Режим NOWAIT - не ждать
            if mode == LockMode.NOWAIT:
                raise LockConflictError(
                    f"Lock conflict on {resource} for transaction {xid}"
                )
            
            # Режим SKIP - пропустить
            if mode == LockMode.SKIP:
                return False
            
            # Режим WAIT - ждать освобождения
            return self._wait_for_lock(resource, lock_type, xid, timeout)
    
    def release_lock(self, resource: str, xid: int) -> bool:
        """
        [START_CONTRACT_RELEASE_LOCK]
        Intent: Освободить блокировку ресурса.
        Input: resource - идентификатор ресурса; xid - ID транзакции.
        Output: True если блокировка освобождена, False если не найдена.
        [END_CONTRACT_RELEASE_LOCK]
        """
        logger.debug(
            f"[LockManager][release_lock] Belief: Releasing lock | "
            f"Resource: {resource} | XID: {xid}"
        )
        
        with self._lock:
            if resource not in self._locks:
                return False
            
            if xid not in self._locks[resource]:
                return False
            
            # Удаляем блокировку
            del self._locks[resource][xid]
            
            # Если ресурс больше не заблокирован, удаляем запись
            if not self._locks[resource]:
                del self._locks[resource]
            
            # Пробуждаем ожидающие транзакции
            self._notify_waiters(resource)
            
            logger.debug(
                f"[LockManager][release_lock] Belief: Lock released | "
                f"Resource: {resource} | XID: {xid}"
            )
            
            return True
    
    def release_all_locks(self, xid: int) -> int:
        """
        [START_CONTRACT_RELEASE_ALL]
        Intent: Освободить все блокировки транзакции.
        Input: xid - ID транзакции.
        Output: Количество освобождённых блокировок.
        [END_CONTRACT_RELEASE_ALL]
        """
        logger.debug(
            f"[LockManager][release_all_locks] Belief: Releasing all locks | XID: {xid}"
        )
        
        with self._lock:
            count = 0
            resources_to_remove = []
            
            for resource in list(self._locks.keys()):
                if xid in self._locks[resource]:
                    del self._locks[resource][xid]
                    count += 1
                    
                    if not self._locks[resource]:
                        resources_to_remove.append(resource)
            
            for resource in resources_to_remove:
                del self._locks[resource]
                self._notify_waiters(resource)
            
            # Удаляем из ожидающих
            if xid in self._waiting:
                del self._waiting[xid]
            
            return count
    
    def is_locked(self, resource: str) -> bool:
        """Проверяет, заблокирован ли ресурс."""
        with self._lock:
            return resource in self._locks and len(self._locks[resource]) > 0
    
    def get_lock_holders(self, resource: str) -> Set[int]:
        """Возвращает XID транзакций, удерживающих блокировку."""
        with self._lock:
            if resource not in self._locks:
                return set()
            return set(self._locks[resource].keys())
    
    def get_locks_held_by(self, xid: int) -> List[str]:
        """Возвращает список ресурсов, заблокированных транзакцией."""
        with self._lock:
            resources = []
            for resource, locks in self._locks.items():
                if xid in locks:
                    resources.append(resource)
            return resources
    
    def get_waiting_transactions(self) -> Set[int]:
        """Возвращает XID ожидающих транзакций."""
        with self._lock:
            return set(self._waiting.keys())
    
    def get_wait_for_graph(self) -> Dict[int, Set[int]]:
        """
        [START_CONTRACT_GET_WFG]
        Intent: Построить wait-for graph.
        Input: Нет.
        Output: Словарь xid -> set(blocked_by_xids).
        [END_CONTRACT_GET_WFG]
        """
        with self._lock:
            graph: Dict[int, Set[int]] = {}
            
            for xid, wait_entry in self._waiting.items():
                graph[xid] = set(wait_entry.blocked_by)
            
            return graph
    
    # -------------------------------------------------------------------------
    # Private Methods
    # -------------------------------------------------------------------------
    
    def _try_acquire(
        self,
        resource: str,
        lock_type: LockType,
        xid: int
    ) -> bool:
        """Попытка получить блокировку без ожидания."""
        # Если транзакция уже держит блокировку на этом ресурсе
        if resource in self._locks and xid in self._locks[resource]:
            existing = self._locks[resource][xid]
            
            # Если уже есть такая же блокировка
            if existing.lock_type == lock_type:
                return True
            
            # Upgrade: S -> X
            if existing.lock_type == LockType.SHARE and lock_type == LockType.EXCLUSIVE:
                # Проверяем, есть ли другие S locks
                other_holders = [
                    other_xid for other_xid in self._locks[resource]
                    if other_xid != xid
                ]
                if not other_holders:
                    existing.lock_type = LockType.EXCLUSIVE
                    return True
                return False
            
            # Downgrade: X -> S (необычно, но допустимо)
            if existing.lock_type == LockType.EXCLUSIVE and lock_type == LockType.SHARE:
                existing.lock_type = LockType.SHARE
                return True
            
            return True
        
        # Проверяем совместимость с существующими блокировками
        if resource in self._locks:
            held_types = {
                entry.lock_type for entry in self._locks[resource].values()
            }
            if not LockCompatibility.can_grant(held_types, lock_type):
                return False
        
        # Создаём новую блокировку
        if resource not in self._locks:
            self._locks[resource] = {}
        
        self._locks[resource][xid] = LockEntry(
            resource=resource,
            lock_type=lock_type,
            xid=xid
        )
        
        return True
    
    def _wait_for_lock(
        self,
        resource: str,
        lock_type: LockType,
        xid: int,
        timeout: float
    ) -> bool:
        """Ждать освобождения блокировки."""
        # Определяем, кто блокирует
        blocked_by = self._get_blocking_transactions(resource, lock_type, xid)
        
        if not blocked_by:
            # Никто не блокирует - пробуем снова
            return self._try_acquire(resource, lock_type, xid)
        
        # Создаём wait entry
        wait_entry = WaitEntry(
            resource=resource,
            lock_type=lock_type,
            xid=xid,
            blocked_by=blocked_by,
            timeout=timeout
        )
        
        self._waiting[xid] = wait_entry
        
        # Проверяем deadlock
        if self._deadlock_detector:
            victim_xid = self._deadlock_detector.detect(self)
            if victim_xid is not None:
                self._waiting.pop(xid, None)
                if victim_xid == xid:
                    raise DeadlockError(
                        f"Deadlock detected, transaction {xid} selected as victim"
                    )
                # Если жертва другая транзакция, ждём её завершения
        
        # Ждём освобождения
        released = wait_entry.event.wait(timeout=timeout)
        
        # Удаляем из ожидающих
        self._waiting.pop(xid, None)
        
        if not released:
            raise LockTimeoutError(
                f"Lock timeout on {resource} for transaction {xid}"
            )
        
        # Пробуем снова
        return self._try_acquire(resource, lock_type, xid)
    
    def _get_blocking_transactions(
        self,
        resource: str,
        lock_type: LockType,
        xid: int
    ) -> Set[int]:
        """Определить транзакции, блокирующие получение блокировки."""
        if resource not in self._locks:
            return set()
        
        blocked_by = set()
        for holder_xid, entry in self._locks[resource].items():
            if holder_xid == xid:
                continue
            if not LockCompatibility.is_compatible(entry.lock_type, lock_type):
                blocked_by.add(holder_xid)
        
        return blocked_by
    
    def _notify_waiters(self, resource: str) -> None:
        """Пробудить ожидающие транзакции."""
        for xid, wait_entry in list(self._waiting.items()):
            if wait_entry.resource == resource:
                # Проверяем, можно ли теперь получить блокировку
                blocked_by = self._get_blocking_transactions(
                    resource, wait_entry.lock_type, xid
                )
                if not blocked_by:
                    wait_entry.event.set()


# END_BLOCK_LOCK_MANAGER


# =============================================================================
# START_BLOCK_HELPERS
# =============================================================================

def create_lock_manager(
    deadlock_detector: Optional['DeadlockDetector'] = None
) -> LockManager:
    """
    [START_CONTRACT_CREATE_LM]
    Intent: Фабрика для создания LockManager.
    Input: deadlock_detector - опциональный детектор deadlocks.
    Output: Готовый к работе LockManager.
    [END_CONTRACT_CREATE_LM]
    """
    return LockManager(deadlock_detector=deadlock_detector)


def resource_key(table: str, row_id: Optional[int] = None) -> str:
    """
    [START_CONTRACT_RESOURCE_KEY]
    Intent: Создать ключ ресурса для блокировки.
    Input: table - имя таблицы; row_id - опциональный ID строки.
    Output: Строка-ключ ресурса.
    [END_CONTRACT_RESOURCE_KEY]
    """
    if row_id is None:
        return f"table:{table}"
    return f"row:{table}:{row_id}"


# END_BLOCK_HELPERS