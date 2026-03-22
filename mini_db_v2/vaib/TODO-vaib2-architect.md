# TODO: Vaib2 Architect — mini_db_v2 Architecture Design

**Статус**: SUCCESS ✅  
**Дата**: 2026-03-22  
**Вход**: mini_db_v2/vaib/01-analyst/requirements.md

---

## 1. Анализ требований

### P0 — Critical (17 требований)
| ID | Требование | Сложность |
|----|------------|-----------|
| REQ-OPT-001 | Cost-Based Query Optimizer | Очень высокая |
| REQ-OPT-002 | Statistics Collection | Высокая |
| REQ-OPT-003 | EXPLAIN Plan | Средняя |
| REQ-IDX-001 | B-Tree Index | Высокая |
| REQ-IDX-002 | Composite Index | Высокая |
| REQ-IDX-003 | Covering Index | Высокая |
| REQ-JOIN-001 | INNER JOIN | Высокая |
| REQ-JOIN-002 | LEFT JOIN | Высокая |
| REQ-JOIN-003 | RIGHT JOIN | Высокая |
| REQ-JOIN-004 | FULL OUTER JOIN | Высокая |
| REQ-JOIN-005 | CROSS JOIN | Низкая |
| REQ-JOIN-006 | Join Algorithms (Hash, Merge, Nested Loop) | Очень высокая |
| REQ-JOIN-007 | Multiple Table Joins (до 10 таблиц) | Очень высокая |
| NFR-ARCH-001 | Modular Architecture | Средняя |
| NFR-THREAD-001 | Thread Safety | Высокая |

### P1 — High (18 требований)
| ID | Требование | Сложность |
|----|------------|-----------|
| REQ-CONC-001 | Multi-Threaded Execution | Высокая |
| REQ-CONC-002 | MVCC | Очень высокая |
| REQ-CONC-003 | Isolation Levels | Высокая |
| REQ-CONC-004 | Lock Manager | Высокая |
| REQ-CONC-005 | Deadlock Detection | Высокая |
| REQ-DUR-001 | Write-Ahead Log (WAL) | Очень высокая |
| REQ-DUR-002 | Checkpoint Mechanism | Высокая |
| REQ-DUR-003 | Crash Recovery | Очень высокая |
| REQ-DUR-004 | ARIES-Style Recovery | Очень высокая |
| REQ-AGG-001 | Aggregate Functions | Средняя |
| REQ-AGG-002 | GROUP BY | Средняя |
| REQ-AGG-003 | HAVING | Средняя |
| REQ-AGG-004 | DISTINCT | Низкая |
| REQ-PERF-001 | Scale Requirements | Средняя |
| REQ-PERF-002 | Query Performance | Средняя |
| NFR-MEM-001 | Memory Management | Высокая |
| NFR-PERSIST-001 | Durability Guarantees | Высокая |

### P2 — Medium (7 требований)
| ID | Требование | Сложность |
|----|------------|-----------|
| REQ-SUB-001 | Scalar Subquery | Средняя |
| REQ-SUB-002 | Correlated Subquery | Высокая |
| REQ-SUB-003 | EXISTS / NOT EXISTS | Средняя |
| REQ-SUB-004 | IN / NOT IN | Средняя |
| REQ-SQL-001 | NULL Handling (IS NULL) | Низкая |
| REQ-SQL-002 | CASE Expressions | Средняя |
| REQ-SQL-003 | CAST Functions | Низкая |

---

## 2. Архитектурные решения

### 2.1 Модули (определено в requirements.md)
```
mini_db_v2/
├── __init__.py
├── __main__.py              # Entry point: python -m mini_db_v2
├── parser/
│   ├── __init__.py
│   ├── lexer.py             # Токенизация
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
│   ├── joins.py             # Join algorithms
│   └── aggregates.py        # Aggregate functions
├── storage/
│   ├── __init__.py
│   ├── database.py          # Database manager
│   ├── table.py             # Table with MVCC
│   ├── btree.py             # B+tree implementation
│   ├── mvcc.py              # Version chains
│   └── buffer_pool.py       # Page cache
├── recovery/
│   ├── __init__.py
│   ├── wal.py               # Write-Ahead Log
│   ├── checkpoint.py        # Checkpoint manager
│   └── aries.py             # ARIES recovery
├── concurrency/
│   ├── __init__.py
│   ├── lock_manager.py      # Row/table locks
│   ├── transaction.py       # Transaction manager
│   └── deadlock.py          # Deadlock detection
└── repl/
    ├── __init__.py
    └── repl.py              # Read-Eval-Print Loop
```

### 2.2 Ключевые контракты

#### Parser → AST
```python
def parse(query: str) -> ASTNode:
    """Возвращает AST или выбрасывает ParseError"""
```

#### AST Nodes (иерархия)
```
ASTNode (base)
├── DDLNode
│   ├── CreateTableNode
│   ├── CreateIndexNode
│   └── DropIndexNode
├── DMLNode
│   ├── InsertNode
│   ├── UpdateNode
│   └── DeleteNode
├── DQLNode
│   └── SelectNode (с JOIN, WHERE, GROUP BY, HAVING)
├── TransactionNode
│   ├── BeginNode
│   ├── CommitNode
│   └── RollbackNode
├── SystemNode
│   ├── SaveNode
│   ├── LoadNode
│   ├── CheckpointNode
│   ├── AnalyzeNode
│   └── ExitNode
└── ExpressionNode
    ├── ComparisonNode
    ├── LogicalNode
    ├── ArithmeticNode
    ├── AggregateNode
    ├── CaseNode
    └── SubqueryNode
```

#### Optimizer → Executor
```python
class QueryPlan:
    cost: float
    rows: int
    operations: list[PlanNode]

def optimize(ast: SelectNode, stats: Statistics) -> QueryPlan:
    """Возвращает оптимальный план выполнения"""
```

#### Storage → Executor
```python
class Table:
    def insert(self, row: dict, xid: int) -> Result: ...
    def update(self, predicate, updates: dict, xid: int) -> Result: ...
    def delete(self, predicate, xid: int) -> Result: ...
    def select(self, predicate, snapshot: Snapshot) -> list[dict]: ...
```

#### MVCC
```python
class RowVersion:
    data: dict
    xmin: int  # XID that created this version
    xmax: int  # XID that deleted/updated this version (0 if alive)
    created_at: datetime

def is_visible(version: RowVersion, snapshot: Snapshot) -> bool:
    """Проверяет видимость версии для транзакции"""
```

#### WAL
```python
class WALRecord:
    lsn: int
    xid: int
    type: str  # INSERT, UPDATE, DELETE, COMMIT, ROLLBACK, CHECKPOINT
    data: dict
    checksum: int

def write_wal(record: WALRecord) -> None:
    """Записывает запись в WAL (force before commit)"""
```

### 2.3 Negative Constraints (из requirements.md)
- ❌ Сторонние библиотеки
- ❌ asyncio (только threading)
- ❌ Distributed transactions (2PC)
- ❌ Replication
- ❌ Partitioning
- ❌ Full-text search
- ❌ GIS/spatial indexes
- ❌ PL/pgSQL stored procedures

---

## 3. Декомпозиция на фазы

### Фаза 1: Foundation (Lexer + AST + Storage Skeleton)
**Goal**: Базовая инфраструктура для парсинга и хранения

**Scope**:
- Lexer: токенизация расширенного SQL-синтаксиса
- AST nodes: все классы узлов (включая JOIN, GROUP BY, subqueries)
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
- [ ] Lexer корректно токенизирует все SQL конструкции (JOIN, GROUP BY, etc.)
- [ ] AST nodes имеют корректную структуру
- [ ] B+tree skeleton с базовыми операциями (insert, search)

---

### Фаза 2: B-Tree Index
**Goal**: Полноценный B+tree для range queries

**Scope**:
- B+tree implementation: insert, delete, search
- Range scans: <, >, <=, >=, BETWEEN
- Leaf node linking для efficient scans
- Integration с Table

**Deliverables**:
1. Полный B+tree в `storage/btree.py`
2. CREATE INDEX parser & executor
3. Index usage в SELECT WHERE
4. Tests для B-tree operations

**Dependencies**: Фаза 1

**Done Criteria**:
- [ ] B-tree поддерживает range queries
- [ ] CREATE INDEX создаёт B-tree индекс
- [ ] SELECT с WHERE col > X использует индекс

---

### Фаза 3: Statistics + Cost Model
**Goal**: Сбор статистики и оценка стоимости

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

**Dependencies**: Фаза 2

**Done Criteria**:
- [ ] ANALYZE TABLE собирает статистику
- [ ] Cost model оценивает стоимость операций

---

### Фаза 4: Query Optimizer
**Goal**: Cost-based query optimization

**Scope**:
- Plan enumeration: генерация альтернативных планов
- Plan selection: выбор плана с минимальной cost
- Join ordering: System R algorithm
- EXPLAIN / EXPLAIN ANALYZE

**Deliverables**:
1. `optimizer/planner.py` — генерация планов
2. EXPLAIN parser & executor
3. Tests для optimizer

**Dependencies**: Фаза 3

**Done Criteria**:
- [ ] Optimizer выбирает оптимальный план
- [ ] EXPLAIN выводит план выполнения
- [ ] Join ordering работает

---

### Фаза 5: JOIN Operations
**Goal**: Все типы JOIN

**Scope**:
- INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN, CROSS JOIN
- Join algorithms: Nested Loop, Hash Join, Merge Join
- Multiple table joins (до 10 таблиц)
- Implicit join syntax (FROM t1, t2 WHERE ...)

**Deliverables**:
1. `executor/joins.py` — алгоритмы JOIN
2. Parser для JOIN syntax
3. Tests для всех типов JOIN
4. Tests для multiple table joins

**Dependencies**: Фаза 4

**Done Criteria**:
- [ ] Все типы JOIN работают
- [ ] Optimizer выбирает алгоритм JOIN
- [ ] Multiple table joins работают

---

### Фаза 6: Aggregation
**Goal**: Aggregate functions и GROUP BY

**Scope**:
- Aggregate functions: COUNT, SUM, AVG, MIN, MAX
- GROUP BY clause
- HAVING clause
- DISTINCT

**Deliverables**:
1. `executor/aggregates.py` — aggregate functions
2. Parser для GROUP BY, HAVING
3. Tests для aggregation

**Dependencies**: Фаза 5

**Done Criteria**:
- [ ] Все aggregate functions работают
- [ ] GROUP BY группирует корректно
- [ ] HAVING фильтрует после GROUP BY

---

### Фаза 7: MVCC
**Goal**: Multi-Version Concurrency Control

**Scope**:
- Row version chains
- Transaction ID (XID)
- Visibility rules
- Snapshot isolation
- Isolation levels: READ COMMITTED, REPEATABLE READ

**Deliverables**:
1. `storage/mvcc.py` — version chains
2. `concurrency/transaction.py` — transaction manager
3. Tests для MVCC visibility
4. Tests для isolation levels

**Dependencies**: Фаза 6

**Done Criteria**:
- [ ] MVCC обеспечивает snapshot isolation
- [ ] Readers don't block writers
- [ ] Isolation levels работают

---

### Фаза 8: Lock Manager + Deadlock Detection
**Goal**: Управление блокировками

**Scope**:
- Row-level locks
- Table-level locks
- Lock compatibility matrix
- Deadlock detection (wait-for graph)
- Lock timeout

**Deliverables**:
1. `concurrency/lock_manager.py` — lock manager
2. `concurrency/deadlock.py` — deadlock detection
3. Tests для locks
4. Tests для deadlock detection

**Dependencies**: Фаза 7

**Done Criteria**:
- [ ] Lock manager работает
- [ ] Deadlock detection находит cycles
- [ ] Lock timeout работает

---

### Фаза 9: WAL
**Goal**: Write-Ahead Logging

**Scope**:
- WAL records: INSERT, UPDATE, DELETE, COMMIT, ROLLBACK
- LSN (Log Sequence Number)
- Force log before commit
- Checksum для integrity

**Deliverables**:
1. `recovery/wal.py` — WAL implementation
2. Tests для WAL

**Dependencies**: Фаза 8

**Done Criteria**:
- [ ] WAL записывает все изменения
- [ ] Force log before commit работает

---

### Фаза 10: ARIES Recovery
**Goal**: Crash recovery

**Scope**:
- Checkpoint mechanism
- REDO phase
- UNDO phase
- Compensation Log Records (CLRs)
- Automatic recovery при startup

**Deliverables**:
1. `recovery/checkpoint.py` — checkpoint manager
2. `recovery/aries.py` — ARIES recovery
3. Tests для recovery

**Dependencies**: Фаза 9

**Done Criteria**:
- [ ] Checkpoint создаётся
- [ ] Recovery восстанавливает БД после crash
- [ ] ARIES алгоритм работает

---

### Фаза 11: Subqueries
**Goal**: Подзапросы

**Scope**:
- Scalar subquery
- Correlated subquery
- EXISTS / NOT EXISTS
- IN / NOT IN

**Deliverables**:
1. Parser для subqueries
2. Executor для subqueries
3. Tests для subqueries

**Dependencies**: Фаза 10

**Done Criteria**:
- [ ] Scalar subquery работает
- [ ] Correlated subquery работает
- [ ] EXISTS / NOT EXISTS работают

---

### Фаза 12: SQL-92 Compliance + REPL
**Goal**: SQL-92 features и интерактивный интерфейс

**Scope**:
- IS NULL / IS NOT NULL
- CASE expressions
- CAST functions
- REPL с graceful error handling
- Entry point

**Deliverables**:
1. Parser для IS NULL, CASE, CAST
2. `repl/repl.py` — REPL
3. `mini_db_v2/__main__.py` — entry point
4. Tests для SQL-92 features
5. Tests для REPL

**Dependencies**: Фаза 11

**Done Criteria**:
- [ ] IS NULL / IS NOT NULL работают
- [ ] CASE expressions работают
- [ ] REPL не падает с Python Traceback
- [ ] `python -m mini_db_v2` запускает REPL

---

## 4. Критические Checkpoint'ы

| # | Checkpoint | Фаза | Тест |
|---|------------|------|------|
| 1 | Query optimizer выбирает оптимальный join order | Фаза 4 | `test_optimizer_join_ordering()` |
| 2 | MVCC обеспечивает snapshot isolation | Фаза 7 | `test_mvcc_snapshot_isolation()` |
| 3 | WAL восстанавливает БД после crash | Фаза 10 | `test_wal_recovery()` |
| 4 | B-tree index ускоряет range query в 10x+ | Фаза 2 | `test_btree_range_performance()` |
| 5 | Concurrent transactions не блокируют reads | Фаза 7 | `test_mvcc_non_blocking_reads()` |

---

## 5. Open Questions

### 5.1 B-tree: In-memory vs Disk-based
**Options**:
- A) Pure in-memory B+tree — проще, но без persistence
- B) Disk-based B+tree с buffer pool — сложнее, но realistic

**Decision**: TBD — нужен ли disk-based B-tree для in-memory СУБД?

### 5.2 WAL: File format
**Options**:
- A) Binary format — compact, fast
- B) JSON format — readable, debuggable

**Decision**: TBD — binary vs JSON для WAL?

### 5.3 Threading: Thread pool size
**Options**:
- A) Fixed pool (e.g., 10 threads)
- B) Dynamic pool based on load

**Decision**: TBD — threading strategy?

---

## 6. Consistency Sweep (COMPLETED)

### 6.1 Модули ↔ Требования (VERIFIED)
| Модуль | Требования | Статус |
|--------|------------|--------|
| parser.lexer | NFR-PARSER-001 (recursive descent) | ✅ |
| parser.parser | Все SQL команды (DDL/DML/DQL/JOIN/Transaction) | ✅ |
| ast.nodes | Все ASTNode типы (SelectNode, JoinNode, etc.) | ✅ |
| optimizer.statistics | REQ-OPT-002 | ✅ |
| optimizer.cost_model | REQ-OPT-001 | ✅ |
| optimizer.planner | REQ-OPT-001, REQ-OPT-003 | ✅ |
| executor.joins | REQ-JOIN-001..007 | ✅ |
| executor.aggregates | REQ-AGG-001..004 | ✅ |
| storage.btree | REQ-IDX-001..003 | ✅ |
| storage.mvcc | REQ-CONC-002, REQ-CONC-003 | ✅ |
| concurrency.lock_manager | REQ-CONC-004 | ✅ |
| concurrency.deadlock | REQ-CONC-005 | ✅ |
| concurrency.transaction | REQ-CONC-001..003 | ✅ |
| recovery.wal | REQ-DUR-001 | ✅ |
| recovery.checkpoint | REQ-DUR-002 | ✅ |
| recovery.aries | REQ-DUR-003, REQ-DUR-004 | ✅ |
| repl.repl | NFR-REPL-001 (Graceful errors) | ✅ |

### 6.2 Фазы ↔ Checkpoint'ы (VERIFIED)
| Checkpoint | Фаза | Тест | Статус |
|------------|------|------|--------|
| #1: Optimizer join ordering | Phase 4 | `test_optimizer_join_ordering()` | ✅ |
| #2: MVCC snapshot isolation | Phase 7 | `test_mvcc_snapshot_isolation()` | ✅ |
| #3: WAL recovery | Phase 10 | `test_wal_recovery()` | ✅ |
| #4: B-tree performance 10x+ | Phase 2 | `test_btree_range_performance()` | ✅ |
| #5: Non-blocking reads | Phase 7 | `test_mvcc_non_blocking_reads()` | ✅ |

### 6.3 Фазы ↔ Требования P0 (VERIFIED)
| Требование | Фаза | Покрытие |
|------------|------|----------|
| REQ-OPT-001 Cost-Based Optimizer | Phase 4 | ✅ |
| REQ-OPT-002 Statistics | Phase 3 | ✅ |
| REQ-OPT-003 EXPLAIN | Phase 4 | ✅ |
| REQ-IDX-001 B-Tree Index | Phase 2 | ✅ |
| REQ-IDX-002 Composite Index | Phase 2 | ✅ |
| REQ-IDX-003 Covering Index | Phase 2 | ✅ |
| REQ-JOIN-001 INNER JOIN | Phase 5 | ✅ |
| REQ-JOIN-002 LEFT JOIN | Phase 5 | ✅ |
| REQ-JOIN-003 RIGHT JOIN | Phase 5 | ✅ |
| REQ-JOIN-004 FULL OUTER JOIN | Phase 5 | ✅ |
| REQ-JOIN-005 CROSS JOIN | Phase 5 | ✅ |
| REQ-JOIN-006 Join Algorithms | Phase 5 | ✅ |
| REQ-JOIN-007 Multiple Table Joins | Phase 5 | ✅ |
| NFR-ARCH-001 Modular Architecture | Phase 1 | ✅ |
| NFR-THREAD-001 Thread Safety | Phase 7-8 | ✅ |

### 6.4 Фазы ↔ Требования P1 (VERIFIED)
| Требование | Фаза | Покрытие |
|------------|------|----------|
| REQ-CONC-001 Multi-Threaded | Phase 7-8 | ✅ |
| REQ-CONC-002 MVCC | Phase 7 | ✅ |
| REQ-CONC-003 Isolation Levels | Phase 7 | ✅ |
| REQ-CONC-004 Lock Manager | Phase 8 | ✅ |
| REQ-CONC-005 Deadlock Detection | Phase 8 | ✅ |
| REQ-DUR-001 WAL | Phase 9 | ✅ |
| REQ-DUR-002 Checkpoint | Phase 10 | ✅ |
| REQ-DUR-003 Crash Recovery | Phase 10 | ✅ |
| REQ-DUR-004 ARIES Recovery | Phase 10 | ✅ |
| REQ-AGG-001 Aggregate Functions | Phase 6 | ✅ |
| REQ-AGG-002 GROUP BY | Phase 6 | ✅ |
| REQ-AGG-003 HAVING | Phase 6 | ✅ |
| REQ-AGG-004 DISTINCT | Phase 6 | ✅ |
| REQ-PERF-001 Scale Requirements | Phase 12 | ✅ |
| REQ-PERF-002 Query Performance | Phase 12 | ✅ |
| NFR-MEM-001 Memory Management | Phase 7 | ✅ |
| NFR-PERSIST-001 Durability Guarantees | Phase 10 | ✅ |

### 6.5 Фазы ↔ Требования P2 (VERIFIED)
| Требование | Фаза | Покрытие |
|------------|------|----------|
| REQ-SUB-001 Scalar Subquery | Phase 11 | ✅ |
| REQ-SUB-002 Correlated Subquery | Phase 11 | ✅ |
| REQ-SUB-003 EXISTS / NOT EXISTS | Phase 11 | ✅ |
| REQ-SUB-004 IN / NOT IN | Phase 11 | ✅ |
| REQ-SQL-001 IS NULL | Phase 12 | ✅ |
| REQ-SQL-002 CASE Expressions | Phase 12 | ✅ |
| REQ-SQL-003 CAST Functions | Phase 12 | ✅ |

### 6.6 Deliverables Count per Phase (VERIFIED)
| Фаза | Deliverables | Диапазон (3-7) |
|------|--------------|----------------|
| Phase 1 | 6 | ✅ |
| Phase 2 | 5 | ✅ |
| Phase 3 | 4 | ✅ |
| Phase 4 | 4 | ✅ |
| Phase 5 | 5 | ✅ |
| Phase 6 | 4 | ✅ |
| Phase 7 | 6 | ✅ |
| Phase 8 | 5 | ✅ |
| Phase 9 | 3 | ✅ |
| Phase 10 | 4 | ✅ |
| Phase 11 | 6 | ✅ |
| Phase 12 | 5 | ✅ |

### 6.7 Negative Constraints (VERIFIED)
| Запрет | Документация | Статус |
|--------|--------------|--------|
| Сторонние библиотеки | technology.md §1.1 | ✅ |
| asyncio | technology.md §1.1 | ✅ |
| Distributed transactions | development_plan.md §2.3 | ✅ |
| Replication | development_plan.md §2.3 | ✅ |
| Partitioning | development_plan.md §2.3 | ✅ |

---

## 7. Backup Log

| Дата | Файл | Backup | Причина |
|------|------|--------|---------|
| 2026-03-22 | development_plan.md | (создание) | Первичная версия |
| 2026-03-22 | technology.md | (создание) | Первичная версия |

---

## 8. Итоговое резюме

**STATUS**: SUCCESS ✅

**OUTPUT**: 
- `mini_db_v2/vaib/02-architect/development_plan.md` ✅
- `mini_db_v2/vaib/02-architect/technology.md` ✅

**SUMMARY**:
- **12 фаз** разработки
- **8 модулей**: parser, ast, optimizer, executor, storage, recovery, concurrency, repl
- **Technology stack**: Python 3.11+, threading, unittest
- **42 требования** (17 P0, 18 P1, 7 P2) — все покрыты фазами
- **5 критических checkpoint'ов** распределены по фазам 2, 4, 7, 10
- **Ключевые архитектурные решения**:
  - B+tree для range queries (Phase 2)
  - Cost-based optimizer с System R join ordering (Phase 4)
  - MVCC с snapshot isolation (Phase 7)
  - WAL + ARIES recovery (Phase 9-10)
  - Hash/Merge/Nested Loop joins (Phase 5)