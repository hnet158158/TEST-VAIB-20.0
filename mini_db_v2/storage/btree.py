# START_MODULE_CONTRACT
# Module: mini_db_v2.storage.btree
# Intent: B+tree индекс для range queries с полной балансировкой.
# Dependencies: dataclasses, typing, bisect, threading
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: BTree, BTreeNode, BTreeError, DuplicateKeyError
# END_MODULE_MAP

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional, Generic, TypeVar, Iterator
from bisect import bisect_left, bisect_right
import threading


# =============================================================================
# START_BLOCK_ERRORS
# =============================================================================

class BTreeError(Exception):
    """Базовая ошибка B-tree."""
    pass


class DuplicateKeyError(BTreeError):
    """Дублирование ключа в unique индексе."""
    pass


class KeyNotFoundError(BTreeError):
    """Ключ не найден."""
    pass

# END_BLOCK_ERRORS


# =============================================================================
# START_BLOCK_BTREE_NODE
# =============================================================================

K = TypeVar('K')  # Key type
V = TypeVar('V')  # Value type (row_id)


@dataclass
class BTreeNode(Generic[K, V]):
    """
    [START_CONTRACT_BTREE_NODE]
    Intent: Узел B+tree (листовой или внутренний).
    Input: is_leaf - является ли листом; order - порядок дерева.
    Output: Структура для хранения ключей и значений/потомков.
    [END_CONTRACT_BTREE_NODE]
    """
    is_leaf: bool = True
    order: int = 64
    keys: list[K] = field(default_factory=list)
    values: list[V] = field(default_factory=list)  # Только для листов (row_ids)
    children: list[BTreeNode[K, V]] = field(default_factory=list)  # Только для внутренних
    next_leaf: Optional[BTreeNode[K, V]] = None  # Связь листов для range scan
    parent: Optional[BTreeNode[K, V]] = field(default=None, repr=False)
    
    def is_full(self) -> bool:
        """Проверяет переполнение узла."""
        return len(self.keys) >= self.order
    
    def is_underflow(self) -> bool:
        """Проверяет недостаток ключей (для удаления)."""
        min_keys = self.order // 2
        if self.parent is None:  # Root can have fewer keys
            return len(self.keys) == 0
        return len(self.keys) < min_keys
    
    def can_lend(self) -> bool:
        """Проверяет, может ли узел отдать ключ."""
        min_keys = self.order // 2
        return len(self.keys) > min_keys

# END_BLOCK_BTREE_NODE


# =============================================================================
# START_BLOCK_BTREE
# =============================================================================

class BTree(Generic[K, V]):
    """
    [START_CONTRACT_BTREE]
    Intent: B+tree индекс для efficient point lookups и range scans с балансировкой.
    Input: order - порядок дерева (max ключей в узле); unique - уникальность ключей.
    Output: API для insert, search, range_scan, delete с автоматической балансировкой.
    [END_CONTRACT_BTREE]
    """
    
    def __init__(self, order: int = 64, unique: bool = False):
        """
        [START_CONTRACT_BTREE_INIT]
        Intent: Инициализация пустого B+tree.
        Input: order - порядок дерева (64-256 типично); unique - уникальные ключи.
        Output: Пустое дерево с корнем-листом.
        [END_CONTRACT_BTREE_INIT]
        """
        self.order = order
        self.unique = unique
        self.root: Optional[BTreeNode[K, V]] = BTreeNode(is_leaf=True, order=order)
        self._size = 0
        self._lock = threading.RLock()
        self._height = 1
    
    @property
    def size(self) -> int:
        """Количество ключей в дереве."""
        return self._size
    
    @property
    def height(self) -> int:
        """Высота дерева."""
        return self._height
    
    @property
    def is_empty(self) -> bool:
        """Проверяет пустоту дерева."""
        return self._size == 0
    
    def insert(self, key: K, value: V) -> None:
        """
        [START_CONTRACT_BTREE_INSERT]
        Intent: Вставить ключ с значением (row_id) с автоматической балансировкой.
        Input: key - ключ для поиска; value - связанное значение.
        Output: None.
        Raises: DuplicateKeyError если unique и ключ уже есть.
        [END_CONTRACT_BTREE_INSERT]
        """
        with self._lock:
            if self.root is None:
                self.root = BTreeNode(is_leaf=True, order=self.order)
            
            # Find leaf node
            leaf = self._find_leaf(key)
            
            # Check for duplicate
            if self.unique:
                idx = bisect_left(leaf.keys, key)
                if idx < len(leaf.keys) and leaf.keys[idx] == key:
                    raise DuplicateKeyError(f"Duplicate key: {key}")
            
            # Insert into leaf
            self._insert_into_leaf(leaf, key, value)
            self._size += 1
    
    def search(self, key: K) -> list[V]:
        """
        [START_CONTRACT_BTREE_SEARCH]
        Intent: Точный поиск по ключу.
        Input: key - искомый ключ.
        Output: Список значений (может быть несколько для non-unique).
        [END_CONTRACT_BTREE_SEARCH]
        """
        with self._lock:
            if self.root is None:
                return []
            
            leaf = self._find_leaf(key)
            result = []
            
            # Find all matching keys in leaf
            idx = bisect_left(leaf.keys, key)
            while idx < len(leaf.keys) and leaf.keys[idx] == key:
                result.append(leaf.values[idx])
                idx += 1
            
            return result
    
    def range_scan(self, low: K, high: K, 
                   low_inclusive: bool = True, 
                   high_inclusive: bool = True) -> list[tuple[K, V]]:
        """
        [START_CONTRACT_BTREE_RANGE_SCAN]
        Intent: Range scan от low до high с опциональной эксклюзивностью границ.
        Input: low - нижняя граница; high - верхняя граница;
               low_inclusive, high_inclusive - включение границ.
        Output: Список (key, value) пар в диапазоне.
        [END_CONTRACT_BTREE_RANGE_SCAN]
        """
        with self._lock:
            if self.root is None:
                return []
            
            result = []
            
            # Find starting leaf
            leaf = self._find_leaf(low)
            
            # Scan through leaves
            while leaf is not None:
                for i, key in enumerate(leaf.keys):
                    # Check bounds
                    low_ok = (low_inclusive and key >= low) or (not low_inclusive and key > low)
                    high_ok = (high_inclusive and key <= high) or (not high_inclusive and key < high)
                    
                    if low_ok and high_ok:
                        result.append((key, leaf.values[i]))
                    elif key > high or (not high_inclusive and key >= high):
                        return result
                
                # Move to next leaf
                leaf = leaf.next_leaf
            
            return result
    
    def range_scan_iter(self, low: K, high: K,
                        low_inclusive: bool = True,
                        high_inclusive: bool = True) -> Iterator[tuple[K, V]]:
        """
        [START_CONTRACT_BTREE_RANGE_SCAN_ITER]
        Intent: Итератор для range scan (memory efficient).
        Input: low, high - границы; low_inclusive, high_inclusive - включение границ.
        Output: Iterator по (key, value) парам.
        [END_CONTRACT_BTREE_RANGE_SCAN_ITER]
        """
        with self._lock:
            if self.root is None:
                return
            
            leaf = self._find_leaf(low)
            
            while leaf is not None:
                for i, key in enumerate(leaf.keys):
                    low_ok = (low_inclusive and key >= low) or (not low_inclusive and key > low)
                    high_ok = (high_inclusive and key <= high) or (not high_inclusive and key < high)
                    
                    if low_ok and high_ok:
                        yield (key, leaf.values[i])
                    elif key > high or (not high_inclusive and key >= high):
                        return
                
                leaf = leaf.next_leaf
    
    def delete(self, key: K) -> int:
        """
        [START_CONTRACT_BTREE_DELETE]
        Intent: Удалить ключ из дерева с автоматической балансировкой.
        Input: key - ключ для удаления.
        Output: Количество удалённых значений.
        [END_CONTRACT_BTREE_DELETE]
        """
        with self._lock:
            if self.root is None:
                return 0
            
            leaf = self._find_leaf(key)
            count = self._delete_from_leaf(leaf, key)
            self._size -= count
            return count
    
    def get_all(self) -> list[tuple[K, V]]:
        """Возвращает все ключи и значения (для отладки)."""
        with self._lock:
            if self.root is None:
                return []
            
            result = []
            leaf = self._find_leftmost_leaf()
            
            while leaf is not None:
                for i, key in enumerate(leaf.keys):
                    result.append((key, leaf.values[i]))
                leaf = leaf.next_leaf
            
            return result
    
    def min_key(self) -> Optional[K]:
        """Возвращает минимальный ключ."""
        with self._lock:
            if self.root is None or self._size == 0:
                return None
            
            leaf = self._find_leftmost_leaf()
            if leaf and leaf.keys:
                return leaf.keys[0]
            return None
    
    def max_key(self) -> Optional[K]:
        """Возвращает максимальный ключ."""
        with self._lock:
            if self.root is None or self._size == 0:
                return None
            
            leaf = self._find_rightmost_leaf()
            if leaf and leaf.keys:
                return leaf.keys[-1]
            return None
    
    def _find_leaf(self, key: K) -> BTreeNode[K, V]:
        """Находит листовой узел для ключа."""
        node = self.root
        while not node.is_leaf:
            idx = bisect_right(node.keys, key)
            node = node.children[idx]
        return node
    
    def _find_leftmost_leaf(self) -> Optional[BTreeNode[K, V]]:
        """Находит самый левый лист."""
        if self.root is None:
            return None
        
        node = self.root
        while not node.is_leaf:
            node = node.children[0] if node.children else None
            if node is None:
                return None
        return node
    
    def _find_rightmost_leaf(self) -> Optional[BTreeNode[K, V]]:
        """Находит самый правый лист."""
        if self.root is None:
            return None
        
        node = self.root
        while not node.is_leaf:
            node = node.children[-1] if node.children else None
            if node is None:
                return None
        return node
    
    def _insert_into_leaf(self, leaf: BTreeNode[K, V], key: K, value: V) -> None:
        """Вставляет ключ в листовой узел с возможным split."""
        idx = bisect_left(leaf.keys, key)
        leaf.keys.insert(idx, key)
        leaf.values.insert(idx, value)
        
        # Split if full
        if leaf.is_full():
            self._split_leaf(leaf)
    
    def _split_leaf(self, leaf: BTreeNode[K, V]) -> None:
        """
        [START_CONTRACT_SPLIT_LEAF]
        Intent: Разделить переполненный листовой узел.
        Input: leaf - переполненный лист.
        Output: Два листа с распределёнными ключами, родитель обновлён.
        [END_CONTRACT_SPLIT_LEAF]
        """
        mid = len(leaf.keys) // 2
        
        # Create new leaf
        new_leaf = BTreeNode(
            is_leaf=True,
            order=self.order,
            keys=leaf.keys[mid:],
            values=leaf.values[mid:],
            next_leaf=leaf.next_leaf,
            parent=leaf.parent
        )
        
        # Update original leaf
        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]
        leaf.next_leaf = new_leaf
        
        # Propagate split up
        self._insert_into_parent(leaf, new_leaf.keys[0], new_leaf)
    
    def _split_internal(self, node: BTreeNode[K, V]) -> None:
        """
        [START_CONTRACT_SPLIT_INTERNAL]
        Intent: Разделить переполненный внутренний узел.
        Input: node - переполненный внутренний узел.
        Output: Два узла, средний ключ поднят к родителю.
        [END_CONTRACT_SPLIT_INTERNAL]
        """
        mid = len(node.keys) // 2
        
        # Middle key goes up
        mid_key = node.keys[mid]
        
        # Create new internal node
        new_node = BTreeNode(
            is_leaf=False,
            order=self.order,
            keys=node.keys[mid + 1:],
            children=node.children[mid + 1:],
            parent=node.parent
        )
        
        # Update children parent references
        for child in new_node.children:
            child.parent = new_node
        
        # Update original node
        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]
        
        # Propagate split up
        self._insert_into_parent(node, mid_key, new_node)
    
    def _insert_into_parent(self, left: BTreeNode[K, V], key: K, right: BTreeNode[K, V]) -> None:
        """Вставляет ключ и нового потомка в родителя."""
        parent = left.parent
        
        # If left is root, create new root
        if parent is None:
            new_root = BTreeNode(
                is_leaf=False,
                order=self.order,
                keys=[key],
                children=[left, right],
                parent=None
            )
            left.parent = new_root
            right.parent = new_root
            self.root = new_root
            self._height += 1
            return
        
        # Find position in parent
        idx = 0
        for i, child in enumerate(parent.children):
            if child is left:
                idx = i
                break
        
        # Insert key and right child
        parent.keys.insert(idx, key)
        parent.children.insert(idx + 1, right)
        right.parent = parent
        
        # Split parent if needed
        if parent.is_full():
            self._split_internal(parent)
    
    def _delete_from_leaf(self, leaf: BTreeNode[K, V], key: K) -> int:
        """Удаляет ключ из листа с балансировкой."""
        count = 0
        
        # Find and remove all matching keys
        idx = bisect_left(leaf.keys, key)
        while idx < len(leaf.keys) and leaf.keys[idx] == key:
            leaf.keys.pop(idx)
            leaf.values.pop(idx)
            count += 1
        
        if count == 0:
            return 0
        
        # Handle underflow
        if leaf.is_underflow() and leaf.parent is not None:
            self._handle_leaf_underflow(leaf)
        
        # Handle empty root
        if self.root and len(self.root.keys) == 0:
            if not self.root.is_leaf and self.root.children:
                self.root = self.root.children[0]
                self.root.parent = None
                self._height -= 1
            elif self.root.is_leaf:
                pass  # Empty tree
        
        return count
    
    def _handle_leaf_underflow(self, leaf: BTreeNode[K, V]) -> None:
        """Обрабатывает underflow в листовом узле."""
        parent = leaf.parent
        if parent is None:
            return
        
        # Find sibling index
        idx = parent.children.index(leaf)
        
        # Try to borrow from left sibling
        if idx > 0:
            left_sibling = parent.children[idx - 1]
            if left_sibling.can_lend():
                self._borrow_from_left_leaf(leaf, left_sibling, parent, idx - 1)
                return
        
        # Try to borrow from right sibling
        if idx < len(parent.children) - 1:
            right_sibling = parent.children[idx + 1]
            if right_sibling.can_lend():
                self._borrow_from_right_leaf(leaf, right_sibling, parent, idx)
                return
        
        # Merge with sibling
        if idx > 0:
            left_sibling = parent.children[idx - 1]
            self._merge_leaves(left_sibling, leaf, parent, idx - 1)
        elif idx < len(parent.children) - 1:
            right_sibling = parent.children[idx + 1]
            self._merge_leaves(leaf, right_sibling, parent, idx)
    
    def _borrow_from_left_leaf(self, leaf: BTreeNode, left_sibling: BTreeNode,
                                parent: BTreeNode, parent_key_idx: int) -> None:
        """Одалживает ключ у левого соседа."""
        # Move last key from left sibling to leaf
        key = left_sibling.keys.pop()
        value = left_sibling.values.pop()
        
        leaf.keys.insert(0, key)
        leaf.values.insert(0, value)
        
        # Update parent key
        parent.keys[parent_key_idx] = leaf.keys[0]
    
    def _borrow_from_right_leaf(self, leaf: BTreeNode, right_sibling: BTreeNode,
                                 parent: BTreeNode, parent_key_idx: int) -> None:
        """Одалживает ключ у правого соседа."""
        # Move first key from right sibling to leaf
        key = right_sibling.keys.pop(0)
        value = right_sibling.values.pop(0)
        
        leaf.keys.append(key)
        leaf.values.append(value)
        
        # Update parent key
        parent.keys[parent_key_idx] = right_sibling.keys[0]
    
    def _merge_leaves(self, left: BTreeNode, right: BTreeNode,
                      parent: BTreeNode, parent_key_idx: int) -> None:
        """Сливает два листовых узла."""
        # Move all keys from right to left
        left.keys.extend(right.keys)
        left.values.extend(right.values)
        left.next_leaf = right.next_leaf
        
        # Remove right from parent
        parent.keys.pop(parent_key_idx)
        parent.children.remove(right)
        
        # Handle parent underflow
        if parent.is_underflow() and parent.parent is not None:
            self._handle_internal_underflow(parent)
        elif parent.parent is None and len(parent.keys) == 0:
            # Parent is root and empty
            left.parent = None
            self.root = left
            self._height -= 1
    
    def _handle_internal_underflow(self, node: BTreeNode[K, V]) -> None:
        """Обрабатывает underflow во внутреннем узле."""
        parent = node.parent
        if parent is None:
            return
        
        idx = parent.children.index(node)
        
        # Try to borrow from left sibling
        if idx > 0:
            left_sibling = parent.children[idx - 1]
            if left_sibling.can_lend():
                self._borrow_from_left_internal(node, left_sibling, parent, idx - 1)
                return
        
        # Try to borrow from right sibling
        if idx < len(parent.children) - 1:
            right_sibling = parent.children[idx + 1]
            if right_sibling.can_lend():
                self._borrow_from_right_internal(node, right_sibling, parent, idx)
                return
        
        # Merge with sibling
        if idx > 0:
            left_sibling = parent.children[idx - 1]
            self._merge_internal(left_sibling, node, parent, idx - 1)
        elif idx < len(parent.children) - 1:
            right_sibling = parent.children[idx + 1]
            self._merge_internal(node, right_sibling, parent, idx)
    
    def _borrow_from_left_internal(self, node: BTreeNode, left_sibling: BTreeNode,
                                    parent: BTreeNode, parent_key_idx: int) -> None:
        """Одалживает ключ у левого соседа (внутренний узел)."""
        # Parent key comes down
        node.keys.insert(0, parent.keys[parent_key_idx])
        
        # Last child from left sibling moves to node
        node.children.insert(0, left_sibling.children.pop())
        node.children[0].parent = node
        
        # Last key from left sibling goes to parent
        parent.keys[parent_key_idx] = left_sibling.keys.pop()
    
    def _borrow_from_right_internal(self, node: BTreeNode, right_sibling: BTreeNode,
                                     parent: BTreeNode, parent_key_idx: int) -> None:
        """Одалживает ключ у правого соседа (внутренний узел)."""
        # Parent key comes down
        node.keys.append(parent.keys[parent_key_idx])
        
        # First child from right sibling moves to node
        node.children.append(right_sibling.children.pop(0))
        node.children[-1].parent = node
        
        # First key from right sibling goes to parent
        parent.keys[parent_key_idx] = right_sibling.keys.pop(0)
    
    def _merge_internal(self, left: BTreeNode, right: BTreeNode,
                        parent: BTreeNode, parent_key_idx: int) -> None:
        """Сливает два внутренних узла."""
        # Parent key comes down
        left.keys.append(parent.keys[parent_key_idx])
        
        # Move all keys and children from right to left
        left.keys.extend(right.keys)
        for child in right.children:
            child.parent = left
        left.children.extend(right.children)
        
        # Remove right from parent
        parent.keys.pop(parent_key_idx)
        parent.children.remove(right)
        
        # Handle parent underflow
        if parent.is_underflow() and parent.parent is not None:
            self._handle_internal_underflow(parent)
        elif parent.parent is None and len(parent.keys) == 0:
            # Parent is root and empty
            left.parent = None
            self.root = left
            self._height -= 1
    
    def __len__(self) -> int:
        return self._size
    
    def __contains__(self, key: K) -> bool:
        return len(self.search(key)) > 0
    
    def __repr__(self) -> str:
        return f"BTree(order={self.order}, unique={self.unique}, size={self._size}, height={self._height})"


# END_BLOCK_BTREE


# =============================================================================
# START_BLOCK_HELPERS
# =============================================================================

def create_btree_index(
    order: int = 64,
    unique: bool = False
) -> BTree[Any, int]:
    """
    [START_CONTRACT_CREATE_BTREE_INDEX]
    Intent: Фабрика для создания B-tree индекса.
    Input: order - порядок; unique - уникальность.
    Output: Готовый к использованию B-tree.
    [END_CONTRACT_CREATE_BTREE_INDEX]
    """
    return BTree(order=order, unique=unique)

# END_BLOCK_HELPERS