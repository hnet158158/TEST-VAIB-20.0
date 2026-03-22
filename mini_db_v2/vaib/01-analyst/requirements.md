# Requirements Specification: mini_db_v2

**Проект**: mini_db_v2 — Production-Grade VAIB Stress-Test Benchmark  
**Версия**: 2.0  
**Дата**: 2026-03-22  
**Статус**: Draft  
**Сложность**: 8-9/10

---

## 1. Введение

### 1.1 Назначение
mini_db_v2 — локальная СУБД на чистом Python 3.11+ с production-grade архитектурой, предназначенная для валидации возможностей VAIB-агентов на задачах высокой сложности.

### 1.2 AAG-модель
| Компонент | Описание |
|-----------|----------|
| **Actor** | VAIB-агенты (Coder, Tester, Architect, Skeptic, Expert) |
| **Action** | Реализовать СУБД с optimizer, MVCC, WAL, B-tree, JOIN |
| **Goal** | Доказать способность агентов реализовать систему сложности 8-9/10 |

### 1.3 Критические Checkpoint'ы
1. `[TEST]` Query optimizer выбирает оптимальный join order (cost-based)
2. `[TEST]` MVCC обеспечивает snapshot isolation для concurrent transactions
3. `[TEST]` WAL восстанавливает БД после simulated crash
4. `[TEST]` B-tree index ускоряет range query (WHERE x > 10 AND x < 100) в 10x+
5. `[TEST]` Concurrent transactions не блокируют reads (readers don't block writers)

### 1.4 Отличия от mini_db v1
| Фича | mini_db v1 | mini_db v2 |
|------|------------|------------|
| Индексы | Hash (equality only) | B-tree (range) + Hash |
| JOIN | ❌ | ✅ INNER, LEFT, RIGHT, FULL, CROSS |
| Транзакции | ❌ | ✅ MVCC с isolation levels |
| Персистентность | JSON snapshot | WAL + ARIES recovery |
| Оптимизатор | ❌ | ✅ Cost-based |
| Параллелизм | Single-threaded | Multi-threaded (threading) |

---

## 2. Функциональные требования

### 2.1 Query Optimization (P0)

#### [P0] REQ-OPT-001: Cost-Based Query Optimizer
**Приоритет**: CRITICAL

**Описание**:
Оптимизатор запросов, выбирающий оптимальный план выполнения на основе статистики.

**Требования**:
- Cost model: оценка CPU + I/O cost для каждого оператора
- Plan enumeration: генерация альтернативных планов
- Plan selection: выбор плана с минимальной cost
- Join ordering: выбор оптимального порядка соединения таблиц

**Алгоритмы**:
- Dynamic programming для join ordering (System R algorithm)
- Statistics-based cardinality estimation

**Ошибки**:
- `Error: Query too complex to optimize` — превышен лимит таблиц в JOIN

---

#### [P0] REQ-OPT-002: Statistics Collection
**Приоритет**: CRITICAL

**Описание**:
Сбор статистики о данных для cost estimation.

**Требования**:
- Table statistics: row count, page count
- Column statistics: distinct values (cardinality), null count
- Histogram: распределение значений для range predicates
- Automatic statistics update после DML операций (опционально)

**Синтаксис**:
```sql
ANALYZE TABLE table_name;
ANALYZE TABLE table_name UPDATE STATISTICS;
```

---

#### [P0] REQ-OPT-003: EXPLAIN Plan
**Приоритет**: CRITICAL

**Описание**:
Вывод плана выполнения запроса без его реального выполнения.

**Синтаксис**:
```sql
EXPLAIN SELECT * FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.x > 10;
EXPLAIN ANALYZE SELECT ...; -- с реальным выполнением
```

**Формат вывода**:
```
QUERY PLAN
========================================
Hash Join  (cost=150.00 rows=100)
  Hash Cond: t1.id = t2.id
  -> Seq Scan on t1  (cost=50.00 rows=500)
       Filter: x > 10
  -> Hash  (cost=100.00 rows=1000)
       -> Seq Scan on t2  (cost=100.00 rows=1000)
```

---

### 2.2 Advanced Indexing (P0)

#### [P0] REQ-IDX-001: B-Tree Index
**Приоритет**: CRITICAL

**Описание**:
B-tree индекс для range queries и сортировки.

**Синтаксис**:
```sql
CREATE INDEX idx_name ON table_name (column);
CREATE INDEX idx_name ON table_name (column) TYPE BTREE;
```

**Поддерживаемые операции**:
| Оператор | Поддержка |
|----------|-----------|
| `=` | ✅ |
| `!=` | ✅ (scan) |
| `<` | ✅ |
| `>` | ✅ |
| `<=` | ✅ |
| `>=` | ✅ |
| `BETWEEN` | ✅ |
| `ORDER BY` | ✅ (index scan) |

**Требования**:
- Self-balancing B+tree (не B-tree, а B+tree для range scans)
- Leaf nodes linked для efficient range scans
- Support for INT, TEXT types (BOOL via hash)

---

#### [P0] REQ-IDX-002: Composite Index
**Приоритет**: CRITICAL

**Описание**:
Индекс по нескольким колонкам.

**Синтаксис**:
```sql
CREATE INDEX idx_name ON table_name (col1, col2, col3);
```

**Правила использования**:
- Index used for: `WHERE col1 = x` ✅
- Index used for: `WHERE col1 = x AND col2 = y` ✅
- Index used for: `WHERE col1 = x AND col2 = y AND col3 = z` ✅
- Index NOT used for: `WHERE col2 = y` ❌ (leftmost prefix rule)
- Index partially used for: `WHERE col1 = x AND col3 = z` (only col1)

---

#### [P0] REQ-IDX-003: Covering Index / Index-Only Scan
**Приоритет**: CRITICAL

**Описание**:
Индекс, содержащий все колонки запроса, исключая table lookup.

**Синтаксис**:
```sql
CREATE INDEX idx_covering ON table_name (col1, col2) INCLUDE (col3, col4);
```

**Требования**:
- INCLUDE columns stored only in leaf nodes
- Query `SELECT col2, col3 FROM t WHERE col1 = x` uses index-only scan
- Visibility map для MVCC (проверка без table access)

---

### 2.3 JOIN Operations (P0)

#### [P0] REQ-JOIN-001: INNER JOIN
**Приоритет**: CRITICAL

**Синтаксис**:
```sql
SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id;
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id;  -- INNER is default
SELECT * FROM t1, t2 WHERE t1.id = t2.id;   -- implicit join
```

**Требования**:
- Возвращает только строки с совпадением в обеих таблицах
- NULL handling: NULL не равен NULL в JOIN condition

---

#### [P0] REQ-JOIN-002: LEFT JOIN (LEFT OUTER JOIN)
**Приоритет**: CRITICAL

**Синтаксис**:
```sql
SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id;
SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id;
```

**Требования**:
- Возвращает все строки из левой таблицы
- При отсутствии совпадения — NULL для колонок правой таблицы

---

#### [P0] REQ-JOIN-003: RIGHT JOIN (RIGHT OUTER JOIN)
**Приоритет**: CRITICAL

**Синтаксис**:
```sql
SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id;
```

**Требования**:
- Возвращает все строки из правой таблицы
- При отсутствии совпадения — NULL для колонок левой таблицы

---

#### [P0] REQ-JOIN-004: FULL OUTER JOIN
**Приоритет**: CRITICAL

**Синтаксис**:
```sql
SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;
```

**Требования**:
- Возвращает все строки из обеих таблиц
- NULL для несовпадающих строк с обеих сторон

---

#### [P0] REQ-JOIN-005: CROSS JOIN
**Приоритет**: CRITICAL

**Синтаксис**:
```sql
SELECT * FROM t1 CROSS JOIN t2;
SELECT * FROM t1, t2;  -- implicit cross join without WHERE
```

**Требования**:
- Cartesian product: каждая строка t1 с каждой строкой t2
- Warning при отсутствии WHERE (potential performance issue)

---

#### [P0] REQ-JOIN-006: Join Algorithms
**Приоритет**: CRITICAL

**Описание**:
Реализация нескольких алгоритмов JOIN с автоматическим выбором.

| Алгоритм | Когда используется | Сложность |
|----------|-------------------|-----------|
| Nested Loop | Small tables, indexed join | O(M*N) |
| Hash Join | Large tables, equality join | O(M+N) |
| Merge Join | Sorted inputs, equality join | O(M+N) |

**Требования**:
- Optimizer выбирает алгоритм на основе cost estimation
- Hint syntax для принудительного выбора: `/*+ HASH_JOIN(t1, t2) */`

---

#### [P0] REQ-JOIN-007: Multiple Table Joins
**Приоритет**: CRITICAL

**Синтаксис**:
```sql
SELECT * FROM t1
  JOIN t2 ON t1.id = t2.t1_id
  JOIN t3 ON t2.id = t3.t2_id
  JOIN t4 ON t3.id = t4.t3_id;
```

**Требования**:
- Поддержка до 10 таблиц в одном запросе
- Join order optimization (smallest table first)
- Bushy join trees (не только left-deep)

---

### 2.4 Concurrency Control (P1)

#### [P1] REQ-CONC-001: Multi-Threaded Execution
**Приоритет**: HIGH

**Описание**:
Поддержка параллельного выполнения запросов.

**Требования**:
- Thread pool для query execution
- Connection handling: до 10 concurrent connections
- Thread-safe data structures

**Ограничения**:
- Python GIL ограничивает true parallelism для CPU-bound операций
- I/O operations (WAL writes) могут быть parallel

---

#### [P1] REQ-CONC-002: MVCC (Multi-Version Concurrency Control)
**Приоритет**: HIGH

**Описание**:
Реализация MVCC для non-blocking reads.

**Требования**:
- Each row has version chain: `row -> version_1 -> version_2 -> ...`
- Transaction ID (XID) для каждой версии
- Visibility rules: transaction sees versions committed before its snapshot
- No blocking reads: readers don't block writers

**Структуры данных**:
```python
class RowVersion:
    data: dict
    xmin: int  # XID that created this version
    xmax: int  # XID that deleted/updated this version (0 if alive)
    created_at: datetime
```

---

#### [P1] REQ-CONC-003: Isolation Levels
**Приоритет**: HIGH

**Синтаксис**:
```sql
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
```

**Уровни изоляции**:

| Уровень | Dirty Read | Non-repeatable Read | Phantom Read |
|---------|------------|---------------------|---------------|
| READ COMMITTED | ❌ | ✅ | ✅ |
| REPEATABLE READ | ❌ | ❌ | ❌ (в PostgreSQL) |

**Требования**:
- READ COMMITTED: каждый statement видит новый snapshot
- REPEATABLE READ: вся транзакция видит один snapshot

---

#### [P1] REQ-CONC-004: Lock Manager
**Приоритет**: HIGH

**Описание**:
Управление блокировками для конфликтующих транзакций.

**Типы блокировок**:
| Lock Type | Режим | Конфликтует с |
|-----------|-------|---------------|
| Row Share | SELECT | Exclusive |
| Row Exclusive | INSERT, UPDATE, DELETE | Share, Exclusive |
| Share | CREATE INDEX | Row Exclusive, Exclusive |
| Exclusive | DROP, ALTER | Все |

**Требования**:
- Row-level locks для UPDATE/DELETE
- Table-level locks для DDL
- Lock timeout: configurable (default 30s)

---

#### [P1] REQ-CONC-005: Deadlock Detection
**Приоритет**: HIGH

**Описание**:
Обнаружение и разрешение deadlocks.

**Требования**:
- Wait-for graph для обнаружения cycles
- Deadlock victim selection: youngest transaction
- Error: `Error: Deadlock detected. Transaction aborted.`

---

### 2.5 Durability & Recovery (P1)

#### [P1] REQ-DUR-001: Write-Ahead Log (WAL)
**Приоритет**: HIGH

**Описание**:
Логирование всех изменений перед применением к данным.

**Требования**:
- Log record для каждой модификации (INSERT, UPDATE, DELETE)
- LSN (Log Sequence Number) для каждого record
- Force log to disk before commit (WAL protocol)
- Checksum для обнаружения corruption

**Формат WAL record**:
```
| LSN (8 bytes) | XID (4 bytes) | Type (1 byte) | Length (2 bytes) | Data | Checksum |
```

---

#### [P1] REQ-DUR-002: Checkpoint Mechanism
**Приоритет**: HIGH

**Описание**:
Периодическое сохранение consistent state.

**Требования**:
- Background checkpoint process
- Checkpoint record в WAL
- REDO point: LSN с которого начинается recovery
- Checkpoint frequency: configurable (time-based или WAL-size-based)

**Синтаксис**:
```sql
CHECKPOINT;
```

---

#### [P1] REQ-DUR-003: Crash Recovery
**Приоритет**: HIGH

**Описание**:
Восстановление после crash.

**Процесс recovery**:
1. Read last checkpoint record
2. REDO: apply all WAL records from REDO point
3. UNDO: rollback uncommitted transactions

**Требования**:
- Automatic recovery при startup
- Recovery progress reporting
- Point-in-time recovery (опционально)

---

#### [P1] REQ-DUR-004: ARIES-Style Recovery
**Приоритет**: HIGH

**Описание**:
Реализация ARIES алгоритма для recovery.

**ARIES Principles**:
- **WAL**: Write-Ahead Logging
- **Steal/No-Force**: Dirty pages can be flushed before commit, log not forced at commit
- **Repeating History**: REDO all operations to restore state before crash
- **Logging Undos**: CLRs (Compensation Log Records) для UNDO operations

**Требования**:
- Physiological logging (physical to page, logical within page)
- Dirty page table для optimisation REDO
- Transaction table для UNDO

---

### 2.6 Aggregation & Grouping (P1)

#### [P1] REQ-AGG-001: Aggregate Functions
**Приоритет**: HIGH

**Синтаксис**:
```sql
SELECT COUNT(*) FROM t;
SELECT COUNT(col) FROM t;           -- exclude NULLs
SELECT SUM(col), AVG(col) FROM t;
SELECT MIN(col), MAX(col) FROM t;
```

**Функции**:
| Функция | Описание | NULL Handling |
|---------|----------|---------------|
| COUNT(*) | Количество строк | Включая NULL |
| COUNT(col) | Количество non-NULL значений | Исключая NULL |
| SUM(col) | Сумма | NULL если нет строк |
| AVG(col) | Среднее | NULL если нет строк |
| MIN(col) | Минимум | NULL если нет строк |
| MAX(col) | Максимум | NULL если нет строк |

---

#### [P1] REQ-AGG-002: GROUP BY
**Приоритет**: HIGH

**Синтаксис**:
```sql
SELECT col1, COUNT(*) FROM t GROUP BY col1;
SELECT col1, col2, SUM(x) FROM t GROUP BY col1, col2;
```

**Требования**:
- Все non-aggregate columns должны быть в GROUP BY
- NULL values группируются вместе
- Grouping по expressions: `GROUP BY YEAR(date_col)`

---

#### [P1] REQ-AGG-003: HAVING
**Приоритет**: HIGH

**Синтаксис**:
```sql
SELECT col1, COUNT(*) FROM t GROUP BY col1 HAVING COUNT(*) > 10;
```

**Требования**:
- HAVING фильтрует после GROUP BY
- Aggregate functions в HAVING condition

---

#### [P1] REQ-AGG-004: DISTINCT
**Приоритет**: HIGH

**Синтаксис**:
```sql
SELECT DISTINCT col FROM t;
SELECT DISTINCT col1, col2 FROM t;
SELECT COUNT(DISTINCT col) FROM t;
```

**Требования**:
- Удаление дубликатов из результата
- DISTINCT с ORDER BY: сортировка после deduplication

---

### 2.7 Subqueries (P2)

#### [P2] REQ-SUB-001: Scalar Subquery
**Приоритет**: MEDIUM

**Синтаксис**:
```sql
SELECT * FROM t WHERE x = (SELECT MAX(x) FROM t);
```

**Требования**:
- Subquery должна возвращать ровно одну строку
- Error если subquery возвращает > 1 row

---

#### [P2] REQ-SUB-002: Correlated Subquery
**Приоритет**: MEDIUM

**Синтаксис**:
```sql
SELECT * FROM t1 WHERE x > (SELECT AVG(x) FROM t2 WHERE t2.id = t1.id);
```

**Требования**:
- Subquery выполняется для каждой строки outer query
- Optimization: materialization или transformation to JOIN

---

#### [P2] REQ-SUB-003: EXISTS / NOT EXISTS
**Приоритет**: MEDIUM

**Синтаксис**:
```sql
SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id);
SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id);
```

**Требования**:
- Semijoin optimization для EXISTS
- Antijoin optimization для NOT EXISTS

---

#### [P2] REQ-SUB-004: IN / NOT IN
**Приоритет**: MEDIUM

**Синтаксис**:
```sql
SELECT * FROM t1 WHERE id IN (SELECT id FROM t2);
SELECT * FROM t1 WHERE id NOT IN (SELECT id FROM t2);
```

**Требования**:
- NULL handling: NOT IN с NULL в subquery = empty result
- Transformation to EXISTS/NOT EXISTS для optimization

---

### 2.8 SQL Standard Compliance (P2)

#### [P2] REQ-SQL-001: NULL Handling
**Приоритет**: MEDIUM

**Синтаксис**:
```sql
SELECT * FROM t WHERE col IS NULL;
SELECT * FROM t WHERE col IS NOT NULL;
```

**Требования**:
- IS NULL / IS NOT NULL syntax
- NULL propagation в expressions: `NULL + 1 = NULL`
- COALESCE function: `COALESCE(col, default)`

---

#### [P2] REQ-SQL-002: CASE Expressions
**Приоритет**: MEDIUM

**Синтаксис**:
```sql
SELECT CASE WHEN x > 10 THEN 'big' ELSE 'small' END FROM t;
SELECT CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t;
```

---

#### [P2] REQ-SQL-003: CAST Functions
**Приоритет**: MEDIUM

**Синтаксис**:
```sql
SELECT CAST(x AS TEXT) FROM t;
SELECT CAST('123' AS INT) FROM t;
```

**Поддерживаемые преобразования**:
| From | To | Valid |
|------|-----|-------|
| INT | TEXT | ✅ |
| TEXT | INT | ✅ (если valid number) |
| BOOL | TEXT | ✅ |
| TEXT | BOOL | ✅ ('true'/'false') |

---

### 2.9 Performance Requirements

#### [P1] REQ-PERF-001: Scale Requirements
**Приоритет**: HIGH

**Целевые показатели**:
| Метрика | Значение |
|---------|----------|
| Max rows per table | 100,000 |
| Concurrent connections | 10 |
| Indexed query latency | < 100ms |
| Throughput | > 1000 queries/sec (simple queries) |

---

#### [P1] REQ-PERF-002: Query Performance
**Приоритет**: HIGH

**Требования**:
| Query Type | Latency Target | Notes |
|------------|----------------|-------|
| PK lookup | < 1ms | Index scan |
| Range query (indexed) | < 100ms | B-tree scan |
| Full table scan (10K rows) | < 50ms | Sequential scan |
| JOIN (2 tables, indexed) | < 200ms | Hash/Merge join |
| Aggregation (GROUP BY) | < 150ms | Hash aggregate |

---

## 3. Нефункциональные требования

### 3.1 [P0] NFR-ARCH-001: Modular Architecture
**Приоритет**: CRITICAL

**Требования**:
- Разделение на modules: parser, optimizer, executor, storage, recovery
- Clear interfaces между модулями
- Dependency injection для testability

---

### 3.2 [P0] NFR-THREAD-001: Thread Safety
**Приоритет**: CRITICAL

**Требования**:
- All shared data structures thread-safe
- Lock-free reads где возможно (MVCC)
- Proper synchronization для WAL writes

---

### 3.3 [P1] NFR-MEM-001: Memory Management
**Приоритет**: HIGH

**Требования**:
- Configurable buffer pool size
- LRU eviction для buffer pages
- Memory limit: configurable (default 1GB)

---

### 3.4 [P1] NFR-PERSIST-001: Durability Guarantees
**Приоритет**: HIGH

**Требования**:
- No data loss после commit (WAL persisted)
- Recovery time < 10 seconds для 100K rows
- Checkpoint interval: configurable

---

## 4. Ограничения

### 4.1 Технологические ограничения
| Ограничение | Значение |
|-------------|----------|
| Python версия | 3.11+ |
| Сторонние библиотеки | Запрещены |
| Threading | threading module (без asyncio) |
| Storage | In-memory + WAL on disk |

### 4.2 Scope-ограничения (Out of Scope)
- Distributed transactions (2PC)
- Replication
- Partitioning
- Full-text search
- GIS/spatial indexes
- PL/pgSQL stored procedures

---

## 5. Требования к поставке

### 5.1 Структура модулей
```
mini_db_v2/
├── __init__.py
├── __main__.py
├── parser/
│   ├── lexer.py
│   └── parser.py
├── ast/
│   └── nodes.py
├── optimizer/
│   ├── statistics.py
│   ├── cost_model.py
│   └── planner.py
├── executor/
│   ├── executor.py
│   ├── joins.py
│   └── aggregates.py
├── storage/
│   ├── database.py
│   ├── table.py
│   ├── btree.py
│   ├── mvcc.py
│   └── buffer_pool.py
├── recovery/
│   ├── wal.py
│   ├── checkpoint.py
│   └── aries.py
├── concurrency/
│   ├── lock_manager.py
│   ├── transaction.py
│   └── deadlock.py
└── repl/
    └── repl.py
```

### 5.2 Test Suite
**Обязательное покрытие**:
1. Query optimizer: join ordering, cost estimation
2. MVCC: snapshot isolation, visibility rules
3. WAL: recovery after crash
4. B-tree: range queries, concurrent access
5. JOIN: all types, multiple tables
6. Concurrency: deadlock detection, lock conflicts

**Фреймворк**: pytest
**Target**: 1000+ tests

---

## 6. Приоритизация требований

### P0 — Critical (MVP v2)
- Query Optimization (cost-based, statistics, explain)
- B-tree Indexes (range queries, composite, covering)
- JOIN Operations (all types, algorithms)
- Aggregation (COUNT, SUM, AVG, GROUP BY, HAVING)

### P1 — High
- Concurrency Control (MVCC, isolation levels, locks)
- Durability & Recovery (WAL, checkpoint, ARIES)
- Subqueries (correlated, EXISTS, IN)
- Performance targets

### P2 — Medium
- SQL-92 Compliance (IS NULL, CASE, CAST)
- Advanced optimizations

---

## 7. Acceptance Criteria

### 7.1 Количественные метрики
- [ ] Все тесты проходят (1000+ tests)
- [ ] 5 критических checkpoint'ов покрыты тестами
- [ ] Performance requirements достигнуты

### 7.2 Качественные метрики
- [ ] Архитектура модульная и расширяемая
- [ ] VAIB pipeline отработал без критических эскалаций
- [ ] Recovery успешно восстанавливает БД после crash

---

## 8. Glossary

| Термин | Определение |
|--------|-------------|
| MVCC | Multi-Version Concurrency Control |
| WAL | Write-Ahead Logging |
| ARIES | Algorithm for Recovery and Isolation Exploiting Semantics |
| LSN | Log Sequence Number |
| XID | Transaction ID |
| B+tree | B-tree variant with linked leaf nodes |
| Covering Index | Index containing all queried columns |
| Cardinality | Number of distinct values |
| Selectivity | Fraction of rows matching a predicate |

---

**Документ подготовлен**: Vaib1 Analyst  
**Следующий этап**: Vaib2 Architect → development_plan.md