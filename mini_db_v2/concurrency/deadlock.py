# START_MODULE_CONTRACT
# Module: mini_db_v2.concurrency.deadlock
# Intent: Обнаружение deadlocks через wait-for graph с victim selection.
# Dependencies: typing, dataclasses, enum, logging
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: DeadlockDetector, VictimSelectionPolicy, DeadlockInfo
# END_MODULE_MAP

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Dict, Set, List, TYPE_CHECKING
from enum import Enum, auto
from datetime import datetime
import logging

if TYPE_CHECKING:
    from mini_db_v2.concurrency.lock_manager import LockManager

logger = logging.getLogger(__name__)


# =============================================================================
# START_BLOCK_ENUMS
# =============================================================================

class VictimSelectionPolicy(Enum):
    """
    Политика выбора жертвы при deadlock.
    
    YOUNGEST - самая молодая транзакция (наименее работы)
    OLDEST - самая старая транзакция (больше всего работы)
    MOST_LOCKS - транзакция с наибольшим числом блокировок
    FEWEST_LOCKS - транзакция с наименьшим числом блокировок
    RANDOM - случайный выбор
    """
    YOUNGEST = auto()
    OLDEST = auto()
    MOST_LOCKS = auto()
    FEWEST_LOCKS = auto()
    RANDOM = auto()


# END_BLOCK_ENUMS


# =============================================================================
# START_BLOCK_DEADLOCK_INFO
# =============================================================================

@dataclass
class DeadlockInfo:
    """
    [START_CONTRACT_DEADLOCK_INFO]
    Intent: Информация об обнаруженном deadlock.
    Input: cycle - список XID в цикле; victim_xid - выбранная жертва.
    Output: Структура для логирования и анализа deadlocks.
    [END_CONTRACT_DEADLOCK_INFO]
    """
    cycle: List[int]  # XID в цикле deadlock
    victim_xid: int   # Выбранная жертва
    detected_at: datetime = field(default_factory=datetime.now)
    policy_used: VictimSelectionPolicy = VictimSelectionPolicy.YOUNGEST
    
    def __str__(self) -> str:
        cycle_str = " -> ".join(str(xid) for xid in self.cycle)
        return f"Deadlock: {cycle_str}, victim: {self.victim_xid}"


# END_BLOCK_DEADLOCK_INFO


# =============================================================================
# START_BLOCK_DEADLOCK_DETECTOR
# =============================================================================

class DeadlockDetector:
    """
    [START_CONTRACT_DEADLOCK_DETECTOR]
    Intent: Обнаружение deadlocks через wait-for graph.
    Input: LockManager для построения wait-for graph.
    Output: XID жертвы для разрешения deadlock или None.
    Note: Использует DFS для обнаружения циклов.
          Поддерживает различные политики выбора жертвы.
    [END_CONTRACT_DEADLOCK_DETECTOR]
    """
    
    def __init__(
        self,
        policy: VictimSelectionPolicy = VictimSelectionPolicy.YOUNGEST
    ):
        """
        [START_CONTRACT_DD_INIT]
        Intent: Инициализация детектора deadlocks.
        Input: policy - политика выбора жертвы.
        Output: Готовый к работе детектор.
        [END_CONTRACT_DD_INIT]
        """
        self._policy = policy
        self._detected_deadlocks: List[DeadlockInfo] = []
    
    @property
    def policy(self) -> VictimSelectionPolicy:
        """Возвращает текущую политику выбора жертвы."""
        return self._policy
    
    @policy.setter
    def policy(self, value: VictimSelectionPolicy) -> None:
        """Устанавливает политику выбора жертвы."""
        self._policy = value
    
    def detect(self, lock_manager: 'LockManager') -> Optional[int]:
        """
        [START_CONTRACT_DETECT]
        Intent: Обнаружить deadlock и выбрать жертву.
        Input: lock_manager - менеджер блокировок.
        Output: XID жертвы или None если deadlock не обнаружен.
        [END_CONTRACT_DETECT]
        """
        # Строим wait-for graph
        wfg = lock_manager.get_wait_for_graph()
        
        if not wfg:
            return None
        
        logger.debug(
            f"[DeadlockDetector][detect] Belief: Checking for deadlocks | "
            f"WFG nodes: {len(wfg)}"
        )
        
        # Ищем циклы
        cycle = self._find_cycle(wfg)
        
        if cycle is None:
            return None
        
        logger.warning(
            f"[DeadlockDetector][detect] Belief: Deadlock detected | "
            f"Cycle: {cycle}"
        )
        
        # Выбираем жертву
        victim_xid = self._select_victim(cycle, lock_manager)
        
        # Сохраняем информацию о deadlock
        deadlock_info = DeadlockInfo(
            cycle=cycle,
            victim_xid=victim_xid,
            policy_used=self._policy
        )
        self._detected_deadlocks.append(deadlock_info)
        
        logger.warning(
            f"[DeadlockDetector][detect] Belief: Victim selected | "
            f"XID: {victim_xid} | Policy: {self._policy.name}"
        )
        
        return victim_xid
    
    def _find_cycle(self, wfg: Dict[int, Set[int]]) -> Optional[List[int]]:
        """
        [START_CONTRACT_FIND_CYCLE]
        Intent: Найти цикл в wait-for graph используя DFS.
        Input: wfg - wait-for graph (xid -> blocked_by_xids).
        Output: Список XID в цикле или None.
        [END_CONTRACT_FIND_CYCLE]
        """
        # Состояния вершин: 0 = не посещена, 1 = в стеке, 2 = обработана
        visited: Dict[int, int] = {}
        path: List[int] = []
        
        # Инициализация
        for xid in wfg:
            visited[xid] = 0
        
        # DFS для каждой непосещённой вершины
        for start_xid in wfg:
            if visited[start_xid] != 0:
                continue
            
            cycle = self._dfs_cycle(start_xid, wfg, visited, path)
            if cycle is not None:
                return cycle
        
        return None
    
    def _dfs_cycle(
        self,
        current: int,
        wfg: Dict[int, Set[int]],
        visited: Dict[int, int],
        path: List[int]
    ) -> Optional[List[int]]:
        """DFS для поиска цикла."""
        visited[current] = 1  # В стеке
        path.append(current)
        
        # Получаем транзакции, которые блокируют current
        blocked_by = wfg.get(current, set())
        
        for blocker in blocked_by:
            if blocker not in visited:
                # blocker не в WFG как ожидающий, но может быть в графе
                continue
            
            if visited[blocker] == 1:
                # Нашли цикл - blocker уже в стеке
                # Возвращаем часть пути от blocker до current
                cycle_start = path.index(blocker)
                return path[cycle_start:] + [blocker]
            
            if visited[blocker] == 0:
                # Рекурсивно посещаем
                cycle = self._dfs_cycle(blocker, wfg, visited, path)
                if cycle is not None:
                    return cycle
        
        visited[current] = 2  # Обработана
        path.pop()
        return None
    
    def _select_victim(
        self,
        cycle: List[int],
        lock_manager: 'LockManager'
    ) -> int:
        """
        [START_CONTRACT_SELECT_VICTIM]
        Intent: Выбрать жертву из цикла deadlock.
        Input: cycle - список XID в цикле; lock_manager - менеджер блокировок.
        Output: XID выбранной жертвы.
        [END_CONTRACT_SELECT_VICTIM]
        """
        if len(cycle) <= 1:
            return cycle[0]
        
        # Убираем последний элемент (дубликат первого)
        candidates = cycle[:-1] if cycle[0] == cycle[-1] else cycle
        
        if len(candidates) == 1:
            return candidates[0]
        
        if self._policy == VictimSelectionPolicy.YOUNGEST:
            return max(candidates)  # Наибольший XID = самая молодая
        
        if self._policy == VictimSelectionPolicy.OLDEST:
            return min(candidates)  # Наименьший XID = самая старая
        
        if self._policy == VictimSelectionPolicy.MOST_LOCKS:
            return max(
                candidates,
                key=lambda xid: len(lock_manager.get_locks_held_by(xid))
            )
        
        if self._policy == VictimSelectionPolicy.FEWEST_LOCKS:
            return min(
                candidates,
                key=lambda xid: len(lock_manager.get_locks_held_by(xid))
            )
        
        if self._policy == VictimSelectionPolicy.RANDOM:
            import random
            return random.choice(candidates)
        
        # Default: youngest
        return max(candidates)
    
    def get_detected_deadlocks(self) -> List[DeadlockInfo]:
        """Возвращает список обнаруженных deadlocks."""
        return list(self._detected_deadlocks)
    
    def clear_history(self) -> None:
        """Очищает историю обнаруженных deadlocks."""
        self._detected_deadlocks.clear()
    
    def get_deadlock_count(self) -> int:
        """Возвращает количество обнаруженных deadlocks."""
        return len(self._detected_deadlocks)


# END_BLOCK_DEADLOCK_DETECTOR


# =============================================================================
# START_BLOCK_TARJAN_SCC
# =============================================================================

class TarjanSCCDetector:
    """
    [START_CONTRACT_TARJAN_SCC]
    Intent: Альтернативный детектор deadlocks через алгоритм Тарьяна.
    Input: Wait-for graph.
    Output: Список сильно связных компонент (SCC).
    Note: SCC размера > 1 содержит deadlock.
          Сложность O(V + E).
    [END_CONTRACT_TARJAN_SCC]
    """
    
    def __init__(self):
        self._index = 0
        self._stack: List[int] = []
        self._on_stack: Set[int] = set()
        self._indices: Dict[int, int] = {}
        self._lowlink: Dict[int, int] = {}
        self._sccs: List[List[int]] = []
    
    def find_sccs(self, wfg: Dict[int, Set[int]]) -> List[List[int]]:
        """
        [START_CONTRACT_FIND_SCCS]
        Intent: Найти все сильно связные компоненты.
        Input: wfg - wait-for graph.
        Output: Список SCC (каждый SCC - список XID).
        [END_CONTRACT_FIND_SCCS]
        """
        # Reset state
        self._index = 0
        self._stack = []
        self._on_stack = set()
        self._indices = {}
        self._lowlink = {}
        self._sccs = []
        
        # Run Tarjan's algorithm
        for v in wfg:
            if v not in self._indices:
                self._strongconnect(v, wfg)
        
        return self._sccs
    
    def _strongconnect(self, v: int, wfg: Dict[int, Set[int]]) -> None:
        """Tarjan's strongconnect procedure."""
        self._indices[v] = self._index
        self._lowlink[v] = self._index
        self._index += 1
        self._stack.append(v)
        self._on_stack.add(v)
        
        # Consider successors of v
        for w in wfg.get(v, set()):
            if w not in self._indices:
                # Successor w has not yet been visited
                self._strongconnect(w, wfg)
                self._lowlink[v] = min(self._lowlink[v], self._lowlink[w])
            elif w in self._on_stack:
                # Successor w is in stack and hence in current SCC
                self._lowlink[v] = min(self._lowlink[v], self._indices[w])
        
        # If v is a root node, pop the stack and generate an SCC
        if self._lowlink[v] == self._indices[v]:
            scc = []
            while True:
                w = self._stack.pop()
                self._on_stack.remove(w)
                scc.append(w)
                if w == v:
                    break
            self._sccs.append(scc)
    
    def find_deadlock_cycles(
        self,
        wfg: Dict[int, Set[int]]
    ) -> List[List[int]]:
        """
        [START_CONTRACT_FIND_CYCLES]
        Intent: Найти все deadlock циклы (SCC размера > 1).
        Input: wfg - wait-for graph.
        Output: Список циклов deadlock.
        [END_CONTRACT_FIND_CYCLES]
        """
        sccs = self.find_sccs(wfg)
        return [scc for scc in sccs if len(scc) > 1]


# END_BLOCK_TARJAN_SCC


# =============================================================================
# START_BLOCK_HELPERS
# =============================================================================

def create_deadlock_detector(
    policy: VictimSelectionPolicy = VictimSelectionPolicy.YOUNGEST
) -> DeadlockDetector:
    """
    [START_CONTRACT_CREATE_DD]
    Intent: Фабрика для создания DeadlockDetector.
    Input: policy - политика выбора жертвы.
    Output: Готовый к работе DeadlockDetector.
    [END_CONTRACT_CREATE_DD]
    """
    return DeadlockDetector(policy=policy)


# END_BLOCK_HELPERS