# Technology Stack: mini_db_v2

**Проект**: mini_db_v2 — Production-Grade VAIB Stress-Test Benchmark  
**Версия**: 2.0  
**Дата**: 2026-03-22  
**Статус**: Approved

---

## 1. Core Technologies

### 1.1 Language & Runtime
| Компонент | Технология | Версия | Обоснование |
|-----------|------------|--------|-------------|
| Language | Python | 3.11+ | Type hints, performance improvements, match statements |
| Type System | Python typing | 3.11+ | Static type checking, generics, Protocol |
| Concurrency | threading | stdlib | Thread-based parallelism (no asyncio) |
| Testing | unittest | stdlib | Standard library, no external dependencies |

### 1.2 Constraints
| Ограничение | Значение | Обоснование |
|-------------|----------|-------------|
| External dependencies | ❌ None | VAIB stress-test на чистом Python |
| Async runtime | ❌ No asyncio | Только threading module |
| Storage | In-memory + WAL on disk | Performance + durability |

---

## 2. Architecture Patterns

### 2.1 Design Patterns

#### Layered Architecture
```
┌─────────────────────────────────────┐
│           REPL Layer                │  ← User Interface
├─────────────────────────────────────┤
│         Executor Layer              │  ← Query Execution
├─────────────────────────────────────┤
│        Optimizer Layer              │  ← Query Optimization
├─────────────────────────────────────┤
│         Storage Layer               │  ← Data Management
├─────────────────────────────────────┤
│        Recovery Layer               │  ← Durability
└─────────────────────────────────────┘
```

#### Key Patterns Used
| Pattern | Module | Purpose |
|---------|--------|---------|
| Factory | parser, executor | Create AST nodes, execution plans |
| Strategy | optimizer, joins | Pluggable algorithms (join types, cost models) |
| Observer | recovery, wal | Event notification (checkpoints, recovery) |
| Command | executor | Encapsulate SQL operations |
| Iterator | storage, btree | Lazy evaluation of result sets |
| MVCC | storage | Version-based concurrency control |

### 2.2 Data Structures

#### B+Tree Index
```python
# B+Tree node structure
class BTreeNode:
    is_leaf: bool
    keys: list[Any]
    children: list[BTreeNode] | list[int]  # int = row_id for leaves
    next_leaf: Optional[BTreeNode]  # Linked list for range scans
    parent: Optional[BTreeNode]
```

**Properties**:
- Order (branching factor): 64-256 (configurable)
- Leaf nodes linked for efficient range scans
- Self-balancing (splits on insert, merges on delete)

#### MVCC Version Chain
```python
# Row version chain (in-place update with version history)
class RowVersion:
    data: dict[str, Any]
    xmin: int  # Creating transaction ID
    xmax: int  # Deleting/updating transaction ID (0 if current)
    created_at: datetime
    prev_version: Optional[RowVersion]  # Previous version (for vacuum)
```

**Visibility Rules**:
```python
def is_visible(version: RowVersion, snapshot: Snapshot) -> bool:
    # Created by committed transaction before snapshot
    if version.xmin in snapshot.active_xids:
        return False  # Created by active transaction
    if version.xmin > snapshot.xmax:
        return False  # Created after snapshot
    
    # Not deleted or deleted by transaction after snapshot
    if version.xmax == 0:
        return True  # Not deleted
    if version.xmax in snapshot.active_xids:
        return True  # Deleted by active transaction (not committed yet)
    if version.xmax > snapshot.xmax:
        return True  # Deleted after snapshot
    
    return False  # Deleted by committed transaction before snapshot
```

#### WAL Record Format
```
Binary format (fixed header + variable data):
┌────────────┬────────────┬────────────┬────────────┬────────────┬──────────┐
│ LSN (8 B)  │ XID (4 B)  │ Type (1 B) │ Len (2 B)  │ Data (var) │ CRC (4B) │
└────────────┴────────────┴────────────┴────────────┴────────────┴──────────┘

Record Types:
- INSERT (0x01): table_name, row_id, new_data
- UPDATE (0x02): table_name, row_id, old_data, new_data
- DELETE (0x03): table_name, row_id, old_data
- COMMIT (0x10): xid
- ROLLBACK (0x11): xid
- CHECKPOINT (0x20): redo_lsn, active_xids, dirty_pages
- CLR (0x30): undo_lsn, compensation data
```

---

## 3. Algorithms

### 3.1 Query Optimization

#### Cost Model
```python
# Cost estimation formulas
def estimate_seq_scan_cost(table_stats: TableStats) -> float:
    """Sequential scan: I/O cost dominates"""
    return table_stats.page_count * SEQ_PAGE_COST

def estimate_index_scan_cost(table_stats: TableStats, 
                             column_stats: ColumnStats,
                             selectivity: float) -> float:
    """Index scan: random I/O + CPU"""
    rows = table_stats.row_count * selectivity
    return (rows * RANDOM_PAGE_COST +  # Random I/O
            rows * CPU_OPERATOR_COST)   # CPU cost

def estimate_hash_join_cost(outer_stats: TableStats,
                           inner_stats: TableStats) -> float:
    """Hash join: build hash table + probe"""
    return (inner_stats.row_count * CPU_OPERATOR_COST +  # Build
            outer_stats.row_count * CPU_OPERATOR_COST)   # Probe
```

#### Join Ordering (System R Algorithm)
```python
# Dynamic programming for join ordering
def find_best_join_order(tables: list[TableRef], 
                         stats: Statistics) -> list[TableRef]:
    """
    System R algorithm:
    1. Start with single-table plans
    2. For each subset size k, consider all k-table joins
    3. Prune dominated plans (higher cost, same or smaller result)
    4. Return plan with minimum cost
    """
    best_plans = {}  # (frozenset of tables) -> (cost, plan)
    
    # Base case: single tables
    for table in tables:
        cost = estimate_single_table_cost(table, stats)
        best_plans[frozenset([table])] = (cost, [table])
    
    # Inductive case: join with one more table
    for size in range(2, len(tables) + 1):
        for subset in combinations(tables, size):
            for table in subset:
                prev_subset = frozenset(subset) - {table}
                if prev_subset in best_plans:
                    cost = best_plans[prev_subset][0] + \
                           estimate_join_cost(prev_subset, table, stats)
                    if frozenset(subset) not in best_plans or \
                       cost < best_plans[frozenset(subset)][0]:
                        best_plans[frozenset(subset)] = (
                            cost, best_plans[prev_subset][1] + [table]
                        )
    
    return best_plans[frozenset(tables)][1]
```

### 3.2 Join Algorithms

#### Hash Join
```python
def hash_join(outer: list[dict], inner: list[dict],
              outer_key: str, inner_key: str) -> list[dict]:
    """
    Hash Join (for equality joins):
    1. Build phase: hash inner table on join key
    2. Probe phase: for each outer row, lookup in hash table
    
    Complexity: O(M + N) where M = |outer|, N = |inner|
    """
    # Build phase
    hash_table = defaultdict(list)
    for row in inner:
        hash_table[row[inner_key]].append(row)
    
    # Probe phase
    result = []
    for outer_row in outer:
        key = outer_row[outer_key]
        if key in hash_table:
            for inner_row in hash_table[key]:
                result.append({**outer_row, **inner_row})
    
    return result
```

#### Merge Join
```python
def merge_join(outer: list[dict], inner: list[dict],
               key: str) -> list[dict]:
    """
    Merge Join (for sorted inputs):
    1. Both inputs sorted on join key
    2. Merge like in merge sort
    
    Complexity: O(M + N) for sorted inputs
    """
    outer.sort(key=lambda r: r[key])
    inner.sort(key=lambda r: r[key])
    
    result = []
    i, j = 0, 0
    
    while i < len(outer) and j < len(inner):
        if outer[i][key] < inner[j][key]:
            i += 1
        elif outer[i][key] > inner[j][key]:
            j += 1
        else:
            # Match found - handle duplicates
            k = j
            while k < len(inner) and inner[k][key] == outer[i][key]:
                result.append({**outer[i], **inner[k]})
                k += 1
            i += 1
    
    return result
```

### 3.3 B+Tree Operations

#### Insert with Split
```python
def btree_insert(node: BTreeNode, key: Any, row_id: int) -> Optional[BTreeNode]:
    """
    B+Tree insert with split:
    1. Find leaf node for key
    2. Insert key in sorted order
    3. If overflow, split and propagate up
    """
    if node.is_leaf:
        # Insert in leaf
        idx = bisect_left(node.keys, key)
        node.keys.insert(idx, key)
        node.children.insert(idx, row_id)
        
        # Split if needed
        if len(node.keys) > ORDER:
            return split_leaf(node)
    else:
        # Recurse to child
        idx = bisect_right(node.keys, key)
        new_child = btree_insert(node.children[idx], key, row_id)
        
        if new_child:
            # Propagate split
            mid_key = new_child.keys[0]
            node.keys.insert(idx, mid_key)
            node.children.insert(idx + 1, new_child)
            
            if len(node.keys) > ORDER:
                return split_internal(node)
    
    return None
```

#### Range Scan
```python
def btree_range_scan(root: BTreeNode, low: Any, high: Any) -> list[int]:
    """
    B+Tree range scan:
    1. Find leaf for low bound
    2. Scan through linked leaves until high bound
    """
    result = []
    
    # Find starting leaf
    node = root
    while not node.is_leaf:
        idx = bisect_right(node.keys, low)
        node = node.children[idx]
    
    # Scan through leaves
    while node:
        for i, key in enumerate(node.keys):
            if low <= key <= high:
                result.append(node.children[i])
            elif key > high:
                return result
        node = node.next_leaf
    
    return result
```

### 3.4 ARIES Recovery

#### Analysis Phase
```python
def analysis_phase(wal_records: list[WALRecord], 
                   checkpoint: CheckpointRecord) -> AnalysisResult:
    """
    Analysis phase:
    1. Start from checkpoint
    2. Identify dirty pages (modified but not flushed)
    3. Identify active transactions (not committed)
    """
    dirty_pages = set(checkpoint.dirty_pages)
    active_xids = set(checkpoint.active_xids)
    
    for record in wal_records:
        if record.type == RecordType.COMMIT:
            active_xids.discard(record.xid)
        elif record.type == RecordType.INSERT:
            dirty_pages.add(record.page_id)
        # ... other record types
    
    return AnalysisResult(dirty_pages, active_xids)
```

#### REDO Phase
```python
def redo_phase(wal_records: list[WALRecord], 
               redo_lsn: int,
               dirty_pages: set[int]) -> None:
    """
    REDO phase:
    1. Replay all WAL records from REDO point
    2. Only redo if page LSN < record LSN (idempotent)
    """
    for record in wal_records:
        if record.lsn < redo_lsn:
            continue
        
        if record.page_id in dirty_pages:
            page = read_page(record.page_id)
            if page.lsn < record.lsn:
                apply_record(page, record)
                page.lsn = record.lsn
                write_page(page)
```

#### UNDO Phase
```python
def undo_phase(wal_records: list[WALRecord],
               active_xids: set[int]) -> None:
    """
    UNDO phase:
    1. Rollback uncommitted transactions
    2. Write CLRs (Compensation Log Records) for restart safety
    """
    # Build transaction table from end
    xid_to_undo = {}
    for record in reversed(wal_records):
        if record.xid in active_xids:
            if record.xid not in xid_to_undo:
                xid_to_undo[record.xid] = []
            xid_to_undo[record.xid].append(record)
    
    # Undo each transaction
    for xid, records in xid_to_undo.items():
        for record in records:
            # Write CLR
            clr = WALRecord(
                type=RecordType.CLR,
                xid=xid,
                undo_lsn=record.lsn,
                ...
            )
            write_wal(clr)
            
            # Apply undo
            apply_undo(record)
```

---

## 4. Concurrency Control

### 4.1 MVCC Implementation

#### Snapshot Creation
```python
def create_snapshot(tm: TransactionManager) -> Snapshot:
    """
    Create snapshot for transaction:
    - xmin: lowest active XID
    - xmax: next XID to be assigned
    - active_xids: all currently active transactions
    """
    with tm.lock:
        return Snapshot(
            xid=tm.next_xid,
            xmin=min(tm.active_xids) if tm.active_xids else tm.next_xid,
            xmax=tm.next_xid,
            active_xids=frozenset(tm.active_xids)
        )
```

#### Isolation Levels
```python
class IsolationLevel(Enum):
    READ_COMMITTED = "READ COMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"

def get_snapshot(xid: int, level: IsolationLevel, 
                 tm: TransactionManager) -> Snapshot:
    """
    READ COMMITTED: new snapshot for each statement
    REPEATABLE READ: one snapshot for entire transaction
    """
    if level == IsolationLevel.READ_COMMITTED:
        return create_snapshot(tm)  # New snapshot each time
    else:  # REPEATABLE_READ
        if xid not in tm.transaction_snapshots:
            tm.transaction_snapshots[xid] = create_snapshot(tm)
        return tm.transaction_snapshots[xid]
```

### 4.2 Lock Manager

#### Lock Compatibility Matrix
```python
LOCK_COMPATIBILITY = {
    # (held, requested) -> compatible?
    (LockType.SHARE, LockType.SHARE): True,
    (LockType.SHARE, LockType.EXCLUSIVE): False,
    (LockType.EXCLUSIVE, LockType.SHARE): False,
    (LockType.EXCLUSIVE, LockType.EXCLUSIVE): False,
    (LockType.ROW_SHARE, LockType.ROW_SHARE): True,
    (LockType.ROW_SHARE, LockType.ROW_EXCLUSIVE): True,
    (LockType.ROW_EXCLUSIVE, LockType.ROW_SHARE): True,
    (LockType.ROW_EXCLUSIVE, LockType.ROW_EXCLUSIVE): False,
}
```

#### Deadlock Detection
```python
def detect_deadlock(lm: LockManager) -> Optional[int]:
    """
    Deadlock detection via wait-for graph:
    1. Build wait-for graph from lock waits
    2. Find cycle using DFS
    3. Return victim XID (youngest transaction)
    """
    # Build wait-for graph
    wait_for = defaultdict(set)  # xid -> set of xids it waits for
    
    for resource, holders in lm.lock_holders.items():
        for waiter in lm.lock_waiters[resource]:
            for holder in holders:
                wait_for[waiter].add(holder)
    
    # Find cycle using DFS
    visited = set()
    rec_stack = set()
    
    def find_cycle(xid: int, path: list[int]) -> Optional[list[int]]:
        visited.add(xid)
        rec_stack.add(xid)
        path.append(xid)
        
        for waiter in wait_for[xid]:
            if waiter not in visited:
                result = find_cycle(waiter, path)
                if result:
                    return result
            elif waiter in rec_stack:
                # Cycle found
                cycle_start = path.index(waiter)
                return path[cycle_start:]
        
        path.pop()
        rec_stack.remove(xid)
        return None
    
    for xid in wait_for:
        if xid not in visited:
            cycle = find_cycle(xid, [])
            if cycle:
                # Return youngest transaction as victim
                return max(cycle, key=lambda x: x)
    
    return None
```

---

## 5. Performance Considerations

### 5.1 Memory Management

#### Buffer Pool (LRU)
```python
class BufferPool:
    def __init__(self, max_pages: int = 1000):
        self.max_pages = max_pages
        self.pages: OrderedDict[int, Page] = OrderedDict()
        self.lock = threading.Lock()
    
    def get_page(self, page_id: int) -> Page:
        with self.lock:
            if page_id in self.pages:
                # Move to end (most recently used)
                self.pages.move_to_end(page_id)
                return self.pages[page_id]
            
            # Evict LRU if full
            while len(self.pages) >= self.max_pages:
                self.pages.popitem(last=False)
            
            # Load page
            page = self.load_page(page_id)
            self.pages[page_id] = page
            return page
```

### 5.2 WAL Buffering
```python
class WALBuffer:
    def __init__(self, max_size: int = 64 * 1024):  # 64KB
        self.buffer = bytearray()
        self.max_size = max_size
        self.current_lsn = 0
        self.lock = threading.Lock()
    
    def write(self, record: WALRecord) -> int:
        with self.lock:
            record.lsn = self.current_lsn
            self.current_lsn += 1
            
            data = serialize_record(record)
            self.buffer.extend(data)
            
            # Flush if buffer full
            if len(self.buffer) >= self.max_size:
                self.flush()
            
            return record.lsn
    
    def force(self, lsn: int) -> None:
        """Force all records up to LSN to disk"""
        with self.lock:
            self.flush()
            fsync(self.wal_file)
```

### 5.3 Statistics Caching
```python
class StatisticsCache:
    def __init__(self, ttl: int = 300):  # 5 minutes
        self.cache: dict[str, Statistics] = {}
        self.ttl = ttl
        self.lock = threading.Lock()
    
    def get(self, table_name: str) -> Optional[Statistics]:
        with self.lock:
            entry = self.cache.get(table_name)
            if entry and time.time() - entry.timestamp < self.ttl:
                return entry.stats
        return None
    
    def invalidate(self, table_name: str) -> None:
        with self.lock:
            self.cache.pop(table_name, None)
```

---

## 6. Testing Strategy

### 6.1 Test Categories

| Category | Purpose | Tools |
|----------|---------|-------|
| Unit Tests | Individual component testing | unittest |
| Integration Tests | Module interaction testing | unittest |
| Performance Tests | Benchmark validation | unittest + time |
| Stress Tests | Concurrency, memory | unittest + threading |
| Recovery Tests | Crash simulation | unittest + tempfile |

### 6.2 Test Targets

| Metric | Target |
|--------|--------|
| Total Tests | 1000+ |
| Code Coverage | > 80% |
| B-tree Performance | 10x faster than full scan |
| WAL Throughput | > 10,000 records/sec |
| Recovery Time | < 10 seconds for 100K rows |

### 6.3 Critical Test Cases

```python
# Checkpoint #1: Optimizer join ordering
def test_optimizer_join_ordering():
    """Optimizer should choose smallest table as outer"""
    db = create_test_db()
    db.execute("CREATE TABLE small (id INT)")
    db.execute("CREATE TABLE large (id INT)")
    # Insert 10 rows in small, 1000 in large
    ...
    plan = db.explain("SELECT * FROM small JOIN large ON small.id = large.id")
    assert plan.outer_table == "small"

# Checkpoint #2: MVCC snapshot isolation
def test_mvcc_snapshot_isolation():
    """Transaction should see consistent snapshot"""
    db = create_test_db()
    db.execute("CREATE TABLE t (id INT)")
    db.execute("INSERT INTO t VALUES (1)")
    
    xid1 = db.begin_transaction()
    snapshot1 = db.get_snapshot(xid1)
    
    db.execute("INSERT INTO t VALUES (2)")  # Different transaction
    
    # xid1 should not see new row
    rows = db.execute_in_transaction(xid1, "SELECT * FROM t")
    assert len(rows) == 1

# Checkpoint #3: WAL recovery
def test_wal_recovery():
    """Database should recover after crash"""
    db = create_test_db()
    db.execute("CREATE TABLE t (id INT)")
    db.execute("INSERT INTO t VALUES (1)")
    db.execute("CHECKPOINT")
    db.execute("INSERT INTO t VALUES (2)")
    
    # Simulate crash (no checkpoint after last insert)
    db.crash()
    
    # Recover
    db.recover()
    rows = db.execute("SELECT * FROM t")
    assert len(rows) == 2

# Checkpoint #4: B-tree performance
def test_btree_range_performance():
    """B-tree should be 10x faster for range queries"""
    db = create_test_db()
    db.execute("CREATE TABLE t (id INT)")
    # Insert 100K rows
    ...
    
    # Without index
    start = time.time()
    db.execute("SELECT * FROM t WHERE id > 50000 AND id < 60000")
    time_no_index = time.time() - start
    
    # With index
    db.execute("CREATE INDEX idx ON t (id)")
    start = time.time()
    db.execute("SELECT * FROM t WHERE id > 50000 AND id < 60000")
    time_with_index = time.time() - start
    
    assert time_no_index / time_with_index > 10

# Checkpoint #5: Non-blocking reads
def test_mvcc_non_blocking_reads():
    """Readers should not block writers"""
    db = create_test_db()
    db.execute("CREATE TABLE t (id INT)")
    
    results = []
    
    def writer():
        for i in range(100):
            db.execute(f"INSERT INTO t VALUES ({i})")
    
    def reader():
        for _ in range(100):
            rows = db.execute("SELECT * FROM t")
            results.append(len(rows))
    
    t1 = threading.Thread(target=writer)
    t2 = threading.Thread(target=reader)
    
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    # Reader should have completed without blocking
    assert len(results) == 100
```

---

## 7. Configuration

### 7.1 System Parameters
```python
# Default configuration
CONFIG = {
    # B-tree
    "btree_order": 64,
    "btree_max_depth": 10,
    
    # Buffer Pool
    "buffer_pool_size": 1000,  # pages
    "buffer_pool_max_memory": 100 * 1024 * 1024,  # 100MB
    
    # WAL
    "wal_buffer_size": 64 * 1024,  # 64KB
    "wal_max_size": 100 * 1024 * 1024,  # 100MB
    "wal_sync_interval": 1000,  # ms
    
    # Checkpoint
    "checkpoint_interval": 60,  # seconds
    "checkpoint_min_wal_size": 10 * 1024 * 1024,  # 10MB
    
    # Concurrency
    "max_connections": 10,
    "lock_timeout": 30,  # seconds
    "deadlock_check_interval": 1,  # seconds
    
    # Statistics
    "stats_cache_ttl": 300,  # seconds
    "auto_analyze": True,
    "auto_analyze_threshold": 0.1,  # 10% rows changed
}
```

---

## 8. File Formats

### 8.1 WAL File Format
```
WAL File: <database_name>.wal

Header (64 bytes):
┌────────────────┬────────────────┬────────────────┬────────────────┐
│ Magic (8 B)    │ Version (4 B)  │ Page Size (4B) │ Reserved (48B) │
└────────────────┴────────────────┴────────────────┴────────────────┘

Records (variable length):
┌────────────────┬────────────────┬────────────────┬────────────────┐
│ LSN (8 B)      │ XID (4 B)      │ Type (1 B)     │ Length (2 B)   │
├────────────────┴────────────────┴────────────────┴────────────────┤
│ Data (variable)                                                   │
├────────────────────────────────────────────────────────────────────┤
│ CRC32 (4 B)                                                       │
└────────────────────────────────────────────────────────────────────┘
```

### 8.2 Data File Format (for SAVE)
```
Data File: <filename>.json

{
    "version": "2.0",
    "tables": {
        "table_name": {
            "columns": {
                "col1": {"type": "INT", "unique": false},
                "col2": {"type": "TEXT", "unique": true}
            },
            "rows": [
                {"col1": 1, "col2": "value1"},
                {"col1": 2, "col2": "value2"}
            ],
            "indexes": {
                "idx_name": {"columns": ["col1"], "type": "BTREE"}
            }
        }
    },
    "statistics": {
        "table_name": {
            "row_count": 1000,
            "column_stats": {
                "col1": {
                    "distinct_values": 500,
                    "null_count": 0,
                    "min_value": 1,
                    "max_value": 1000
                }
            }
        }
    }
}
```

---

**Документ подготовлен**: Vaib2 Architect  
**На основе**: mini_db_v2/vaib/01-analyst/requirements.md  
**Следующий этап**: Vaib3 Spec → документация технологий