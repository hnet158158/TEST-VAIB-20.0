# vaib/00-intent/intent.md
## Project: mini_db_v2 (Production-Grade VAIB Stress-Test)
## Date: 2026-03-22

### 1. Current Reality
Проект mini_db (Phase 1-6 DONE, 434 теста PASS) успешно валидировал базовые возможности VAIB-агентов со сложностью 5/10. Для реальной проверки архитектурных решений требуется масштабирование до production-grade СУБД с:
- Query Optimization (cost-based optimizer)
- Advanced Indexing (B-tree, composite, covering)
- JOIN Operations (INNER, LEFT, RIGHT, FULL, CROSS)
- Concurrency Control (MVCC)
- Durability & Recovery (WAL, ARIES)

### 2. Core Intent
**Доказать, что VAIB-агенты способны самостоятельно реализовать production-grade СУБД с архитектурой уровня PostgreSQL/MySQL** и пройти все критические точки:
1. Cost-based query optimizer с join ordering
2. MVCC с isolation levels (Read Committed, Repeatable Read)
3. WAL + ARIES-style crash recovery
4. B-tree indexes для range queries
5. Full JOIN support с optimization

Проект mini_db_v2 — это расширенный VAIB stress-test benchmark, не продакшен-СУБД (но архитектурно эквивалентен).

### 3. Blind Spots & Pivot

**Risks:**
- **Complexity Explosion**: 8-9/10 сложность требует больше фаз и итераций
- **Integration Hell**: MVCC + WAL + B-tree требуют глубокой перестройки storage layer
- **Performance Validation**: 100K rows, 10 concurrent connections — нужны бенчмарки
- **Memory Management**: MVCC создает multiple versions, нужен vacuum

**Pivot:**
Построить на базе mini_db, но с полной переработкой:
- Storage Layer: B-tree indexes, MVCC versions, WAL
- Executor Layer: Query optimizer, join algorithms, parallel execution
- Parser Layer: Extended SQL syntax (JOIN, GROUP BY, subqueries)

**Отвергнутые альтернативы:**
- Расширение mini_db in-place — риск сломать работающий код
- SQLite wrapper — не проверяет архитектурные решения

### 4. Success Metric
Бенчмарк считается пройденным при:

**Количественные метрики:**
1. Все тесты проходят (target: 1000+ tests)
2. 5 критических checkpoint'ов покрыты тестами:
   - `[TEST]` Query optimizer выбирает оптимальный join order
   - `[TEST]` MVCC обеспечивает snapshot isolation
   - `[TEST]` WAL восстанавливает БД после crash
   - `[TEST]` B-tree index ускоряет range query в 10x+
   - `[TEST]` Concurrent transactions не блокируют reads

**Качественные метрики:**
- Архитектура модульная (optimizer, executor, storage, recovery)
- VAIB pipeline отработал без критических эскалаций
- Performance requirements достигнуты (100K rows, 100ms queries)

### 5. Scope Definition

#### P0 — Critical (MVP v2):
- Query Optimization (cost-based, statistics, explain plan)
- B-tree Indexes (range queries, composite, covering)
- JOIN Operations (INNER, LEFT, RIGHT, FULL, CROSS)
- Aggregation (COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING)

#### P1 — High:
- Concurrency Control (MVCC, isolation levels, lock manager)
- Durability & Recovery (WAL, checkpoints, ARIES redo/undo)
- Subqueries (correlated, EXISTS, IN, unnesting)

#### P2 — Medium:
- SQL-92 Compliance (IS NULL, CASE, CAST)
- Performance Optimization (index-only scans, query caching)

### 6. Technical Constraints
| Ограничение | Значение |
|-------------|----------|
| Python версия | 3.11+ |
| Сторонние библиотеки | Запрещены |
| Threading | threading module (без asyncio) |
| Memory | In-memory с WAL на диск |

---

**Документ подготовлен**: Vaib1 Analyst
**На основе**: User request от 2026-03-22
**Следующий этап**: Vaib1 Analyst → requirements.md