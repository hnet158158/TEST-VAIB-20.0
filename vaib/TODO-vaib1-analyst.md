# TODO: Vaib1 Analyst - mini_db Extended Requirements

## Статус: BLOCKED ⚠️

## AAG Model (Intent Formalization)
- **Actor**: VAIB-агенты (Coder, Tester, Architect, Skeptic)
- **Action**: Расширить mini_db до production-grade СУБД с оптимизацией запросов, JOIN, MVCC, WAL
- **Goal**: Повысить сложность стресс-теста с 5/10 до 8-9/10 для реальной проверки возможностей VAIB-агентов

## Intent Check ⚠️ КОНФЛИКТ

### Текущий intent.md (vaib/00-intent/intent.md):
> "Scope Creep: Риск добавления фич сверх ТЗ (JOIN, транзакции, etc.) — за рамками бенчмарка"
> "Отвергнутые альтернативы: SQLite wrapper — не проверяет парсер и архитектуру"

### Новый запрос пользователя:
- JOIN Operations (P0) — **противоречит intent.md**
- Concurrency Control / MVCC (P1) — **противоречит intent.md**
- Durability & Recovery / WAL (P1) — **новое требование**

### Решение требуется:
1. Обновить intent.md с новым scope ИЛИ
2. Отклонить часть требований как out-of-scope

---

## Анализ текущей архитектуры

### Реализовано (Phase 1-6 DONE):
| Компонент | Файл | Статус |
|-----------|------|--------|
| Lexer | `mini_db/parser/lexer.py` | ✅ 10,803 chars |
| Parser | `mini_db/parser/parser.py` | ✅ 23,464 chars |
| AST Nodes | `mini_db/ast/nodes.py` | ✅ 6,722 chars |
| Table | `mini_db/storage/table.py` | ✅ 15,798 chars |
| HashIndex | `mini_db/storage/index.py` | ✅ 3,349 chars |
| Executor | `mini_db/executor/executor.py` | ✅ 17,976 chars |
| REPL | `mini_db/repl/repl.py` | ✅ 5,640 chars |

### Текущие возможности:
- DDL: CREATE TABLE с INT/TEXT/BOOL, UNIQUE
- DML: INSERT, UPDATE (atomic), DELETE
- DQL: SELECT с WHERE (AND/OR/скобки)
- Индексы: Hash index для equality lookups
- Персистентность: SAVE/LOAD в JSON

### Ограничения текущей архитектуры:
1. **HashIndex** — только equality (=), не поддерживает range queries
2. **Нет JOIN** — архитектура single-table
3. **Нет потоков** — single-threaded execution
4. **Нет WAL** — только JSON snapshot

---

## Entropy Reduction — Открытые вопросы

### КРИТИЧЕСКИЕ (требуют ответа пользователя):

#### Q1: Intent Update
**Вопрос**: Текущий [`intent.md`](vaib/00-intent/intent.md:22) прямо запрещает JOIN и транзакции:
> "Scope Creep: Риск добавления фич сверх ТЗ (JOIN, транзакции, etc.) — за рамками бенчмарка"

**Варианты**:
- A) Обновить intent.md — mini_db становится production-grade СУБД benchmark
- B) Создать новый проект mini_db_v2 с расширенным scope
- C) Реализовать подмножество (только Query Optimization + Advanced Indexing)

#### Q2: JOIN Implementation Scope
**Вопрос**: Какие типы JOIN требуются?

| Тип | Сложность | Зависимости |
|-----|-----------|-------------|
| INNER JOIN | Medium | Parser, Executor |
| LEFT JOIN | Medium | NULL handling |
| RIGHT JOIN | Medium | Симметричен LEFT |
| FULL OUTER JOIN | High | LEFT + RIGHT |
| CROSS JOIN | Low | Cartesian product |
| SELF JOIN | Low | Alias support |

**Уточнение**: Нужны ли все типы или достаточно INNER + LEFT?

#### Q3: MVCC vs Lock-Based
**Вопрос**: Для Concurrency Control — MVCC или простой Lock Manager?

| Подход | Сложность | Преимущества | Недостатки |
|--------|-----------|--------------|------------|
| MVCC | 9/10 | No blocking reads, snapshots | High memory, vacuum needed |
| 2PL (Two-Phase Locking) | 7/10 | Simpler | Deadlocks possible |
| Optimistic Concurrency | 6/10 | No locks | Retry overhead |

**Уточнение**: MVCC указан явно, но это самый сложный вариант. Подтверждаете?

#### Q4: WAL Implementation
**Вопрос**: Какой уровень WAL требуется?

| Уровень | Описание | Сложность |
|---------|----------|-----------|
| Basic WAL | Log writes, recover on restart | 6/10 |
| ARIES-style | Full redo/undo, checkpoints | 9/10 |
| Group Commit | Batch log writes | 8/10 |

**Уточнение**: ARIES указан явно. Это production-grade алгоритм. Подтверждаете?

#### Q5: Performance Requirements
**Вопрос**: Указаны конкретные метрики:
- 100,000 rows в таблице
- 10 concurrent connections
- Query response time < 100ms для indexed queries
- Throughput > 1000 queries/sec

**Уточнение**: 
- Это硬кие требования или целевые показатели?
- Как измерять throughput (какой mix операций)?
- Какой hardware предполагается?

#### Q6: SQL Standard Compliance
**Вопрос**: Указан "SQL-92 subset". Какой уровень совместимости?

| Фича | SQL-92 | Сложность |
|------|--------|-----------|
| IS NULL / IS NOT NULL | Entry | Low |
| CASE expressions | Intermediate | Medium |
| CAST functions | Entry | Low |
| Subqueries | Intermediate | High |
| Views | Intermediate | Medium |
| Constraints (CHECK, FOREIGN KEY) | Entry | High |

**Уточнение**: Какие именно фичи SQL-92 обязательны?

---

## Предварительный анализ зависимостей

### Dependency Graph (предварительный):

```
Phase 7: B-Tree Indexes
    ↓
Phase 8: Query Optimizer (Statistics)
    ↓
Phase 9: JOIN Operations
    ↓
Phase 10: Aggregation (GROUP BY)
    ↓
Phase 11: Subqueries
    ↓
Phase 12: Concurrency (MVCC)
    ↓
Phase 13: WAL + Recovery
```

### Риски:
1. **B-Tree** — требует новой структуры данных (сейчас только Hash)
2. **Query Optimizer** — требует статистики, cost model
3. **JOIN** — требует переписать Executor (сейчас single-table)
4. **MVCC** — требует переписать Table (сейчас in-place update)
5. **WAL** — требует персистентности поверх JSON

---

## Следующий шаг
**Требуется**: Ответы на вопросы Q1-Q6 для продолжения формализации требований.

---

**Дата**: 2026-03-22
**Режим**: vaib1-analyst