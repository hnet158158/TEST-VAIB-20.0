# Development Plan: mini_db_v2

**Проект**: mini_db_v2 — Production-Grade VAIB Stress-Test Benchmark  
**Версия**: 2.0  
**Дата**: 2026-03-22  
**Статус**: Approved  
**Сложность**: 8-9/10

---

## 1. Project Overview

### 1.1 Назначение
mini_db_v2 — локальная СУБД на чистом Python 3.11+ с production-grade архитектурой, предназначенная для валидации возможностей VAIB-агентов на задачах высокой сложности (8-9/10).

### 1.2 Ключевые отличия от mini_db v1
| Фича | mini_db v1 | mini_db v2 |
|------|------------|------------|
| Индексы | Hash (equality only) | B-tree (range) + Hash |
| JOIN | ❌ | ✅ INNER, LEFT, RIGHT, FULL, CROSS |
| Транзакции | ❌ | ✅ MVCC с isolation levels |
| Персистентность | JSON snapshot | WAL + ARIES recovery |
| Оптимизатор | ❌ | ✅ Cost-based |
| Параллелизм | Single-threaded | Multi-threaded (threading) |

### 1.3 Критические Checkpoint'ы
1. `[TEST]` Query optimizer выбирает оптимальный join order (cost-based)
2. `[TEST]` MVCC обеспечивает snapshot isolation для concurrent transactions
3. `[TEST]` WAL восстанавливает БД после simulated crash
4. `[TEST]` B-tree index ускоряет range query (WHERE x > 10 AND x < 100) в 10x+
5. `[TEST]` Concurrent transactions не блокируют reads (readers don't block writers)

### 1.4 Целевые метрики
| Метрика | Значение |
|---------|----------|
| Max rows per table | 100,000 |
| Concurrent connections | 10 |
| Indexed query latency | < 100ms |
| Throughput | > 1000 queries/sec (simple queries) |
| Tests | 1000+ |

---

## 2. Architecture

### 2.1 Module Structure
```
mini_db_v2/
├── __init__.py
├── __main__.py              # Entry point: python -m mini_db_v2
├── parser/
│   ├── __init__.py
│   ├── lexer.py             # Токенизация SQL
│   └── parser.py            # Recursive descent parser
├── ast/
│   ├── __init__.py
│   └── nodes.py             # AST node classes
├── optimizer/
│   ├── __init__.py
│   ├── statistics.py        # Table/column statistics
│   ├── cost_model.py        # Cost estimation
│   └── planner.py           # Query plan generation
├── executor/
│   ├── __init__.py
│   ├── executor.py          # Command execution
│   ├── joins.py             # Join algorithms (Hash, Merge, Nested Loop)
│   └── aggregates.py        # Aggregate functions
├── storage/
│   ├── __init__.py
│   ├── database.py          # Database manager
│   ├── table.py             # Table with MVCC version chains
│   ├── btree.py             # B+tree implementation
│   ├── mvcc.py              # MVCC structures (RowVersion, Snapshot)
│   └── buffer_pool.py       # Page cache (LRU)
├── recovery/
│   ├── __init__.py
│   ├── wal.py               # Write-Ahead Log
│   ├── checkpoint.py        # Checkpoint manager
│   └── aries.py             # ARIES recovery (REDO/UNDO)
├── concurrency/
│   ├── __init__.py
│   ├── lock_manager.py      # Row/table locks
│   ├── transaction.py       # Transaction manager (XID, snapshots)
│   └── deadlock.py          # Deadlock detection (wait-for graph)
└── repl/
    ├── __init__.py
    └── repl.py              # Read-Eval-Print Loop
```

### 2.2 Module Contracts

#### Parser Module
```python
# parser/parser.py
class Parser:
    def parse(self, query: str) -> ASTNode:
        """
        Парсит SQL-запрос и возвращает AST.
        
        Raises:
            ParseError: Синтаксическая ошибка
        """
```

#### AST Module
```python
# ast/nodes.py
@dataclass
class ASTNode:
    """Base class for all AST nodes"""
    pass

@dataclass
class SelectNode(ASTNode):
    columns: list[ColumnRef]
    from_clause: FromClause
    where: Optional[ExpressionNode]
    group_by: Optional[list[ColumnRef]]
    having: Optional[ExpressionNode]
    order_by: Optional[list[OrderByItem]]
    limit: Optional[int]

@dataclass
class JoinClause:
    join_type: JoinType  # INNER, LEFT, RIGHT, FULL, CROSS
    table: TableRef
    condition: Optional[ExpressionNode]
```

#### Optimizer Module
```python
# optimizer/planner.py
class QueryPlanner:
    def create_plan(self, ast: SelectNode, stats: Statistics) -> QueryPlan:
        """
        Создаёт оптимальный план выполнения запроса.
        
        Алгоритм:
        1. Генерация альтернативных планов
        2. Оценка стоимости каждого плана
        3. Выбор плана с минимальной cost
        """

# optimizer/statistics.py
class Statistics:
    table_stats: dict[str, TableStats]  # table_name -> stats
    column_stats: dict[str, ColumnStats]  # table.col -> stats

@dataclass
class TableStats:
    row_count: int
    page_count: int
    last_analyze: datetime

@dataclass
class ColumnStats:
    distinct_values: int
    null_count: int
    min_value: Any
    max_value: Any
    histogram: list[Any]  # For range selectivity
```

#### Executor Module
```python
# executor/executor.py
class Executor:
    def execute(self, ast: ASTNode, xid: int) -> ExecutionResult:
        """
        Выполняет AST и возвращает результат.
        """

# executor/joins.py
class JoinExecutor:
    def nested_loop_join(self, outer: list[dict], inner: list[dict], 
                         condition: ExpressionNode) -> list[dict]:
        """O(M*N) - для малых таблиц"""
    
    def hash_join(self, outer: list[dict], inner: list[dict],
                  outer_key: str, inner_key: str) -> list[dict]:
        """O(M+N) - для equality joins"""
    
    def merge_join(self, outer: list[dict], inner: list[dict],
                   key: str) -> list[dict]:
        """O(M+N) - для sorted inputs"""
```

#### Storage Module
```python
# storage/table.py
class Table:
    name: str
    columns: dict[str, ColumnDef]
    rows: list[RowVersion]  # MVCC version chain
    indexes: dict[str, BTree | HashIndex]
    
    def insert(self, row: dict, xid: int) -> Result:
        """Вставляет новую версию строки"""
    
    def select(self, predicate: ExpressionNode, snapshot: Snapshot) -> list[dict]:
        """Возвращает видимые строки для snapshot"""

# storage/btree.py
class BTree:
    def insert(self, key: Any, row_id: int) -> None:
        """Вставляет ключ в B+tree"""
    
    def search(self, key: Any) -> list[int]:
        """Точный поиск по ключу"""
    
    def range_scan(self, low: Any, high: Any) -> list[int]:
        """Range scan: low <= key <= high"""

# storage/mvcc.py
@dataclass
class RowVersion:
    data: dict
    xmin: int  # XID that created this version
    xmax: int  # XID that deleted/updated this version (0 if alive)
    created_at: datetime

@dataclass
class Snapshot:
    xid: int  # Current transaction ID
    active_xids: set[int]  # Active transactions at snapshot time
    xmin: int  # Lowest active XID
    xmax: int  # Highest XID + 1
```

#### Concurrency Module
```python
# concurrency/transaction.py
class TransactionManager:
    def begin(self, isolation_level: IsolationLevel) -> int:
        """Начинает новую транзакцию, возвращает XID"""
    
    def commit(self, xid: int) -> None:
        """Коммитит транзакцию"""
    
    def rollback(self, xid: int) -> None:
        """Откатывает транзакцию"""
    
    def get_snapshot(self, xid: int) -> Snapshot:
        """Возвращает snapshot для транзакции"""

# concurrency/lock_manager.py
class LockManager:
    def acquire_lock(self, resource: str, lock_type: LockType, 
                     xid: int, timeout: float) -> bool:
        """Приобретает блокировку"""
    
    def release_lock(self, resource: str, xid: int) -> None:
        """Освобождает блокировку"""

# concurrency/deadlock.py
class DeadlockDetector:
    def detect(self) -> Optional[int]:
        """
        Обнаруживает deadlock через wait-for graph.
        Возвращает XID жертвы или None.
        """
```

#### Recovery Module
```python
# recovery/wal.py
class WAL:
    def write(self, record: WALRecord) -> int:
        """Записывает запись в WAL, возвращает LSN"""
    
    def force(self, lsn: int) -> None:
        """Force WAL to disk up to LSN"""
    
    def recover(self) -> list[WALRecord]:
        """Читает все записи для recovery"""

@dataclass
class WALRecord:
    lsn: int
    xid: int
    type: RecordType  # INSERT, UPDATE, DELETE, COMMIT, ROLLBACK, CHECKPOINT
    table_name: str
    row_id: Optional[int]
    old_data: Optional[dict]  # For UNDO
    new_data: Optional[dict]  # For REDO
    checksum: int

# recovery/aries.py
class ARIESRecovery:
    def recover(self, wal: WAL, last_checkpoint: CheckpointRecord) -> None:
        """
        ARIES recovery algorithm:
        1. Analysis phase: identify dirty pages, active transactions
        2. REDO phase: replay all WAL records from REDO point
        3. UNDO phase: rollback uncommitted transactions
        """
```

### 2.3 Negative Constraints
| Запрет | Обоснование |
|--------|-------------|
| ❌ Сторонние библиотеки | VAIB stress-test на чистом Python |
| ❌ asyncio | Только threading module |
| ❌ Distributed transactions (2PC) | Out of scope |
| ❌ Replication | Out of scope |
| ❌ Partitioning | Out of scope |
| ❌ Full-text search | Out of scope |
| ❌ GIS/spatial indexes | Out of scope |
| ❌ PL/pgSQL stored procedures | Out of scope |

### 2.4 Data Flow
```
SQL Query
    ↓
[Lexer] → Tokens
    ↓
[Parser] → AST
    ↓
[Optimizer] → QueryPlan
    ↓
[Executor] → Result
    ↓
[Storage] → Data (Table, BTree, MVCC)
    ↓
[Recovery] → WAL
```

### 2.5 Transaction Flow
```
BEGIN
    ↓
[TransactionManager.begin()] → XID
    ↓
[Operations] → WAL records
    ↓
COMMIT / ROLLBACK
    ↓
[WAL.force()] → Durability
```

---

## 3. Phases

### Phase 1: Foundation
**Goal**: Базовая инфраструктура для парсинга и хранения

**Scope**:
- Lexer: токенизация расширенного SQL-синтаксиса (JOIN, GROUP BY, HAVING, subqueries)
- AST nodes: все классы узлов
- Storage skeleton: Database, Table (без MVCC)
- B-tree skeleton: базовая структура

**Deliverables**:
1. `mini_db_v2/ast/nodes.py` — все AST классы
2. `mini_db_v2/parser/lexer.py` — токенизация
3. `mini_db_v2/storage/database.py` — Database class
4. `mini_db_v2/storage/table.py` — Table class (basic)
5. `mini_db_v2/storage/btree.py` — B+tree skeleton
6. Unit tests для lexer и AST

**Dependencies**: None

**Done Criteria**:
- [ ] Lexer корректно токенизирует все SQL конструкции
- [ ] AST nodes имеют корректную структуру
- [ ] B+tree skeleton с базовыми операциями

---

### Phase 2: B-Tree Index
**Goal**: Полноценный B+tree для range queries

**Scope**:
- B+tree implementation: insert, delete, search
- Range scans: <, >, <=, >=, BETWEEN
- Leaf node linking для efficient scans
- Integration с Table
- CREATE INDEX command

**Deliverables**:
1. Полный B+tree в `storage/btree.py`
2. CREATE INDEX parser & executor
3. Index usage в SELECT WHERE
4. Tests для B-tree operations
5. Performance test: range query vs full scan

**Dependencies**: Phase 1

**Done Criteria**:
- [ ] B-tree поддерживает range queries
- [ ] CREATE INDEX создаёт B-tree индекс
- [ ] SELECT с WHERE col > X использует индекс
- [ ] **Checkpoint #4**: B-tree index ускоряет range query в 10x+

---

### Phase 3: Statistics + Cost Model
**Goal**: Сбор статистики и оценка стоимости запросов

**Scope**:
- Table statistics: row count, page count
- Column statistics: distinct values, null count, histogram
- Cost model: CPU + I/O cost estimation
- ANALYZE TABLE command

**Deliverables**:
1. `optimizer/statistics.py` — сбор статистики
2. `optimizer/cost_model.py` — оценка стоимости
3. ANALYZE TABLE parser & executor
4. Tests для statistics

**Dependencies**: Phase 2

**Done Criteria**:
- [ ] ANALYZE TABLE собирает статистику
- [ ] Cost model оценивает стоимость операций
- [ ] Histogram корректно оценивает selectivity

---

### Phase 4: Query Optimizer
**Goal**: Cost-based query optimization

**Scope**:
- Plan enumeration: генерация альтернативных планов
- Plan selection: выбор плана с минимальной cost
- Join ordering: System R algorithm (dynamic programming)
- EXPLAIN / EXPLAIN ANALYZE commands

**Deliverables**:
1. `optimizer/planner.py` — генерация планов
2. EXPLAIN parser & executor
3. Tests для optimizer
4. **Checkpoint #1**: Optimizer выбирает оптимальный join order

**Dependencies**: Phase 3

**Done Criteria**:
- [ ] Optimizer генерирует альтернативные планы
- [ ] Optimizer выбирает план с минимальной cost
- [ ] EXPLAIN выводит план выполнения
- [ ] **Checkpoint #1**: Join ordering работает

---

### Phase 5: JOIN Operations
**Goal**: Все типы JOIN с оптимизацией

**Scope**:
- INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN, CROSS JOIN
- Join algorithms: Nested Loop, Hash Join, Merge Join
- Multiple table joins (до 10 таблиц)
- Implicit join syntax (FROM t1, t2 WHERE ...)
- Bushy join trees

**Deliverables**:
1. `executor/joins.py` — алгоритмы JOIN
2. Parser для JOIN syntax
3. Tests для всех типов JOIN
4. Tests для multiple table joins
5. Performance tests для join algorithms

**Dependencies**: Phase 4

**Done Criteria**:
- [ ] Все типы JOIN работают корректно
- [ ] Optimizer выбирает оптимальный алгоритм JOIN
- [ ] Multiple table joins работают (до 10 таблиц)
- [ ] NULL handling в JOIN корректен

---

### Phase 6: Aggregation
**Goal**: Aggregate functions и GROUP BY

**Scope**:
- Aggregate functions: COUNT(*), COUNT(col), SUM, AVG, MIN, MAX
- GROUP BY clause
- HAVING clause
- DISTINCT
- Hash aggregate implementation

**Deliverables**:
1. `executor/aggregates.py` — aggregate functions
2. Parser для GROUP BY, HAVING, DISTINCT
3. Tests для aggregation
4. Performance tests для GROUP BY

**Dependencies**: Phase 5

**Done Criteria**:
- [ ] Все aggregate functions работают
- [ ] GROUP BY группирует корректно
- [ ] HAVING фильтрует после GROUP BY
- [ ] DISTINCT удаляет дубликаты
- [ ] NULL handling в aggregates корректен

---

### Phase 7: MVCC
**Goal**: Multi-Version Concurrency Control

**Scope**:
- Row version chains (RowVersion struct)
- Transaction ID (XID) allocation
- Visibility rules (xmin/xmax logic)
- Snapshot isolation
- Isolation levels: READ COMMITTED, REPEATABLE READ
- Vacuum (basic version cleanup)

**Deliverables**:
1. `storage/mvcc.py` — version chains, visibility
2. `concurrency/transaction.py` — transaction manager
3. Tests для MVCC visibility
4. Tests для isolation levels
5. **Checkpoint #2**: MVCC обеспечивает snapshot isolation
6. **Checkpoint #5**: Readers don't block writers

**Dependencies**: Phase 6

**Done Criteria**:
- [ ] Row version chains работают
- [ ] Visibility rules корректны
- [ ] READ COMMITTED isolation level работает
- [ ] REPEATABLE READ isolation level работает
- [ ] **Checkpoint #2**: Snapshot isolation работает
- [ ] **Checkpoint #5**: Non-blocking reads

---

### Phase 8: Lock Manager + Deadlock Detection
**Goal**: Управление блокировками и обнаружение deadlocks

**Scope**:
- Row-level locks (Share, Exclusive)
- Table-level locks
- Lock compatibility matrix
- Deadlock detection (wait-for graph)
- Lock timeout (configurable, default 30s)
- Deadlock victim selection

**Deliverables**:
1. `concurrency/lock_manager.py` — lock manager
2. `concurrency/deadlock.py` — deadlock detection
3. Tests для locks
4. Tests для deadlock detection
5. Stress tests для concurrent access

**Dependencies**: Phase 7

**Done Criteria**:
- [ ] Lock manager работает корректно
- [ ] Deadlock detection находит cycles
- [ ] Deadlock victim selection работает
- [ ] Lock timeout работает

---

### Phase 9: WAL
**Goal**: Write-Ahead Logging

**Scope**:
- WAL records: INSERT, UPDATE, DELETE, COMMIT, ROLLBACK
- LSN (Log Sequence Number) allocation
- Force log before commit (WAL protocol)
- Checksum для integrity
- Binary format для efficiency

**Deliverables**:
1. `recovery/wal.py` — WAL implementation
2. Tests для WAL
3. Performance tests для WAL throughput

**Dependencies**: Phase 8

**Done Criteria**:
- [ ] WAL записывает все изменения
- [ ] Force log before commit работает
- [ ] Checksum detects corruption
- [ ] LSN monotonic increasing

---

### Phase 10: ARIES Recovery
**Goal**: Crash recovery с ARIES algorithm

**Scope**:
- Checkpoint mechanism (periodic)
- Checkpoint record в WAL
- Analysis phase: identify dirty pages, active transactions
- REDO phase: replay all WAL records from REDO point
- UNDO phase: rollback uncommitted transactions
- Compensation Log Records (CLRs)
- Automatic recovery при startup

**Deliverables**:
1. `recovery/checkpoint.py` — checkpoint manager
2. `recovery/aries.py` — ARIES recovery
3. Tests для recovery
4. **Checkpoint #3**: WAL восстанавливает БД после crash

**Dependencies**: Phase 9

**Done Criteria**:
- [ ] Checkpoint создаётся периодически
- [ ] Recovery восстанавливает БД после crash
- [ ] ARIES REDO phase работает
- [ ] ARIES UNDO phase работает
- [ ] **Checkpoint #3**: Crash recovery работает

---

### Phase 11: Subqueries
**Goal**: Поддержка subqueries

**Scope**:
- Scalar subquery (возвращает одно значение)
- Correlated subquery (ссылается на outer query)
- EXISTS / NOT EXISTS
- IN / NOT IN
- Subquery optimization (materialization, unnesting)

**Deliverables**:
1. Parser для subqueries
2. Executor для subqueries
3. Tests для scalar subquery
4. Tests для correlated subquery
5. Tests для EXISTS / NOT EXISTS
6. Tests для IN / NOT IN

**Dependencies**: Phase 10

**Done Criteria**:
- [ ] Scalar subquery работает
- [ ] Correlated subquery работает
- [ ] EXISTS / NOT EXISTS работают
- [ ] IN / NOT IN работают
- [ ] NULL handling в subqueries корректен

---

### Phase 12: SQL-92 Compliance + REPL
**Goal**: SQL-92 features и интерактивный интерфейс

**Scope**:
- IS NULL / IS NOT NULL syntax
- CASE expressions (searched and simple)
- CAST functions
- COALESCE function
- REPL с graceful error handling
- Entry point: `python -m mini_db_v2`

**Deliverables**:
1. Parser для IS NULL, CASE, CAST, COALESCE
2. `repl/repl.py` — REPL implementation
3. `mini_db_v2/__main__.py` — entry point
4. Tests для SQL-92 features
5. Tests для REPL error handling

**Dependencies**: Phase 11

**Done Criteria**:
- [ ] IS NULL / IS NOT NULL работают
- [ ] CASE expressions работают
- [ ] CAST functions работают
- [ ] COALESCE function работает
- [ ] REPL не падает с Python Traceback
- [ ] `python -m mini_db_v2` запускает REPL

---

## 4. Phase Execution Status

| Phase | Status | Start Date | End Date | Notes |
|-------|--------|------------|----------|-------|
| Phase 1: Foundation | PENDING | - | - | - |
| Phase 2: B-Tree Index | PENDING | - | - | - |
| Phase 3: Statistics + Cost Model | PENDING | - | - | - |
| Phase 4: Query Optimizer | PENDING | - | - | - |
| Phase 5: JOIN Operations | PENDING | - | - | - |
| Phase 6: Aggregation | PENDING | - | - | - |
| Phase 7: MVCC | PENDING | - | - | - |
| Phase 8: Lock Manager + Deadlock | PENDING | - | - | - |
| Phase 9: WAL | PENDING | - | - | - |
| Phase 10: ARIES Recovery | PENDING | - | - | - |
| Phase 11: Subqueries | PENDING | - | - | - |
| Phase 12: SQL-92 + REPL | PENDING | - | - | - |

---

## 5. Checkpoint Coverage

| # | Checkpoint | Phase | Test | Status |
|---|------------|-------|------|--------|
| 1 | Query optimizer выбирает оптимальный join order | Phase 4 | `test_optimizer_join_ordering()` | PENDING |
| 2 | MVCC обеспечивает snapshot isolation | Phase 7 | `test_mvcc_snapshot_isolation()` | PENDING |
| 3 | WAL восстанавливает БД после crash | Phase 10 | `test_wal_recovery()` | PENDING |
| 4 | B-tree index ускоряет range query в 10x+ | Phase 2 | `test_btree_range_performance()` | PENDING |
| 5 | Concurrent transactions не блокируют reads | Phase 7 | `test_mvcc_non_blocking_reads()` | PENDING |

---

## 6. Requirements Coverage

### P0 — Critical (17 требований)
| ID | Требование | Phase | Status |
|----|------------|-------|--------|
| REQ-OPT-001 | Cost-Based Query Optimizer | Phase 4 | PENDING |
| REQ-OPT-002 | Statistics Collection | Phase 3 | PENDING |
| REQ-OPT-003 | EXPLAIN Plan | Phase 4 | PENDING |
| REQ-IDX-001 | B-Tree Index | Phase 2 | PENDING |
| REQ-IDX-002 | Composite Index | Phase 2 | PENDING |
| REQ-IDX-003 | Covering Index | Phase 2 | PENDING |
| REQ-JOIN-001 | INNER JOIN | Phase 5 | PENDING |
| REQ-JOIN-002 | LEFT JOIN | Phase 5 | PENDING |
| REQ-JOIN-003 | RIGHT JOIN | Phase 5 | PENDING |
| REQ-JOIN-004 | FULL OUTER JOIN | Phase 5 | PENDING |
| REQ-JOIN-005 | CROSS JOIN | Phase 5 | PENDING |
| REQ-JOIN-006 | Join Algorithms | Phase 5 | PENDING |
| REQ-JOIN-007 | Multiple Table Joins | Phase 5 | PENDING |
| NFR-ARCH-001 | Modular Architecture | Phase 1 | PENDING |
| NFR-THREAD-001 | Thread Safety | Phase 7-8 | PENDING |

### P1 — High (18 требований)
| ID | Требование | Phase | Status |
|----|------------|-------|--------|
| REQ-CONC-001 | Multi-Threaded Execution | Phase 7-8 | PENDING |
| REQ-CONC-002 | MVCC | Phase 7 | PENDING |
| REQ-CONC-003 | Isolation Levels | Phase 7 | PENDING |
| REQ-CONC-004 | Lock Manager | Phase 8 | PENDING |
| REQ-CONC-005 | Deadlock Detection | Phase 8 | PENDING |
| REQ-DUR-001 | Write-Ahead Log (WAL) | Phase 9 | PENDING |
| REQ-DUR-002 | Checkpoint Mechanism | Phase 10 | PENDING |
| REQ-DUR-003 | Crash Recovery | Phase 10 | PENDING |
| REQ-DUR-004 | ARIES-Style Recovery | Phase 10 | PENDING |
| REQ-AGG-001 | Aggregate Functions | Phase 6 | PENDING |
| REQ-AGG-002 | GROUP BY | Phase 6 | PENDING |
| REQ-AGG-003 | HAVING | Phase 6 | PENDING |
| REQ-AGG-004 | DISTINCT | Phase 6 | PENDING |
| REQ-PERF-001 | Scale Requirements | Phase 12 | PENDING |
| REQ-PERF-002 | Query Performance | Phase 12 | PENDING |
| NFR-MEM-001 | Memory Management | Phase 7 | PENDING |
| NFR-PERSIST-001 | Durability Guarantees | Phase 10 | PENDING |

### P2 — Medium (7 требований)
| ID | Требование | Phase | Status |
|----|------------|-------|--------|
| REQ-SUB-001 | Scalar Subquery | Phase 11 | PENDING |
| REQ-SUB-002 | Correlated Subquery | Phase 11 | PENDING |
| REQ-SUB-003 | EXISTS / NOT EXISTS | Phase 11 | PENDING |
| REQ-SUB-004 | IN / NOT IN | Phase 11 | PENDING |
| REQ-SQL-001 | NULL Handling (IS NULL) | Phase 12 | PENDING |
| REQ-SQL-002 | CASE Expressions | Phase 12 | PENDING |
| REQ-SQL-003 | CAST Functions | Phase 12 | PENDING |

---

## 7. Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Complexity Explosion | High | High | Phased approach, incremental delivery |
| MVCC Integration Hell | High | High | Phase 7 dedicated to MVCC, extensive tests |
| WAL Performance | Medium | Medium | Binary format, buffering |
| B-tree Concurrency | Medium | High | Lock coupling, latches |
| Memory Exhaustion | Medium | High | Buffer pool with LRU eviction |
| Deadlock Frequency | Medium | Medium | Timeout-based detection, victim selection |

---

**Документ подготовлен**: Vaib2 Architect  
**На основе**: mini_db_v2/vaib/01-analyst/requirements.md  
**Следующий этап**: Vaib3 Spec → документация технологий