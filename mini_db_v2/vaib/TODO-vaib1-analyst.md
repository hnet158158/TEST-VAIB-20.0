# TODO: Vaib1 Analyst - mini_db_v2 Requirements

## Статус: DONE ✅

## AAG Model (Intent Formalization)
- **Actor**: VAIB-агенты (Coder, Tester, Architect, Skeptic, Expert)
- **Action**: Реализовать production-grade СУБД с optimizer, MVCC, WAL, B-tree, JOIN
- **Goal**: Повысить сложность стресс-теста с 5/10 до 8-9/10

## Intent Check
- [x] Создан новый проект mini_db_v2 (отдельная папка)
- [x] intent.md создан с расширенным scope
- [x] Противоречий нет — новый проект с нуля

## Выполненная работа

### 1. Entropy Reduction (уточнено)
| Вопрос | Решение |
|--------|---------|
| Intent conflict | Создан новый проект mini_db_v2 отдельно от mini_db |
| JOIN scope | Все типы: INNER, LEFT, RIGHT, FULL, CROSS |
| MVCC vs Lock-based | MVCC (подтверждено пользователем) |
| WAL level | ARIES-style (подтверждено пользователем) |
| Performance | Целевые показатели, best-effort на офисном ПК |
| SQL-92 compliance | Максимум возможного |

### 2. Internal Consistency Sweep

#### 2.1 Entities vs Endpoints
| Entity | Commands | Status |
|--------|----------|--------|
| Table | CREATE TABLE, SELECT, INSERT, UPDATE, DELETE | ✅ |
| Index (B-tree) | CREATE INDEX, DROP INDEX | ✅ |
| Transaction | BEGIN, COMMIT, ROLLBACK, SET TRANSACTION | ✅ |
| WAL | CHECKPOINT, SAVE (extended) | ✅ |
| Statistics | ANALYZE TABLE | ✅ |

#### 2.2 Business Rules vs Constraints
| Rule | Constraint | Status |
|------|------------|--------|
| MVCC visibility | xmin/xmax, snapshot | ✅ |
| Join optimization | cost-based, statistics | ✅ |
| WAL durability | force log before commit | ✅ |
| B-tree range queries | leftmost prefix rule | ✅ |

#### 2.3 Error Cases Coverage
| Error Type | Handling | Status |
|------------|----------|--------|
| Deadlock | Detect + abort victim | ✅ |
| Crash | ARIES recovery | ✅ |
| Lock timeout | Configurable, default 30s | ✅ |
| Query too complex | Max 10 tables in JOIN | ✅ |

### 3. Output Generated
- [x] `mini_db_v2/vaib/00-intent/intent.md` создан
- [x] `mini_db_v2/vaib/01-analyst/requirements.md` создан

## Статистика требований

### По приоритету:
| Priority | Count | Description |
|----------|-------|-------------|
| P0 (Critical) | 17 | Query Opt, B-tree, JOIN, Aggregation |
| P1 (High) | 18 | MVCC, WAL, Concurrency, Performance |
| P2 (Medium) | 7 | Subqueries, SQL-92 |
| **Total** | **42** | |

### По категориям:
| Category | P0 | P1 | P2 |
|----------|----|----|-----|
| Query Optimization | 3 | 0 | 0 |
| Advanced Indexing | 3 | 0 | 0 |
| JOIN Operations | 7 | 0 | 0 |
| Concurrency Control | 0 | 5 | 0 |
| Durability & Recovery | 0 | 4 | 0 |
| Aggregation & Grouping | 0 | 4 | 0 |
| Subqueries | 0 | 0 | 4 |
| SQL Compliance | 0 | 0 | 3 |
| Performance | 0 | 2 | 0 |

## Критические ограничения
1. **MVCC** — требует переписать Table с version chains
2. **WAL/ARIES** — требует персистентности и recovery subsystem
3. **B-tree** — новая структура данных, сложнее Hash
4. **Query Optimizer** — требует statistics, cost model, plan enumeration
5. **JOIN** — требует переписать Executor для multi-table

## Критические Checkpoint'ы (5 штук)
1. `[TEST]` Query optimizer выбирает оптимальный join order
2. `[TEST]` MVCC обеспечивает snapshot isolation
3. `[TEST]` WAL восстанавливает БД после crash
4. `[TEST]` B-tree index ускоряет range query в 10x+
5. `[TEST]` Concurrent transactions не блокируют reads

## Dependency Graph (предварительный)

```
Phase 1: B-Tree Index
    ↓
Phase 2: Statistics + Cost Model
    ↓
Phase 3: Query Optimizer
    ↓
Phase 4: JOIN Operations
    ↓
Phase 5: Aggregation
    ↓
Phase 6: MVCC
    ↓
Phase 7: Lock Manager + Deadlock
    ↓
Phase 8: WAL
    ↓
Phase 9: ARIES Recovery
    ↓
Phase 10: Subqueries
    ↓
Phase 11: SQL-92 Compliance
    ↓
Phase 12: Performance Optimization
```

## Следующий этап
- Передача в Vaib2 Architect для создания development_plan.md
- Architect должен определить количество фаз (предположительно 10-12)

---

**Дата**: 2026-03-22
**Режим**: vaib1-analyst
**Статус**: DONE ✅