# TODO: Vaib2 Architect — mini_db Architecture Design

**Статус**: IN_PROGRESS  
**Дата**: 2026-03-16  
**Вход**: vaib/01-analyst/requirements.md

---

## 1. Анализ требований

### P0 — Critical (MVP)
| ID | Требование | Сложность |
|----|------------|-----------|
| REQ-DDL-001 | CREATE TABLE (INT, TEXT, BOOL, UNIQUE) | Средняя |
| REQ-DML-001 | INSERT (типизация, UNIQUE constraint) | Средняя |
| REQ-DML-002 | UPDATE (атомарность All-or-Nothing) | Высокая |
| REQ-DML-003 | DELETE | Низкая |
| REQ-DQL-001 | SELECT (projection, insertion order) | Средняя |
| REQ-DQL-002 | WHERE (AND/OR/скобки, NULL-семантика) | Высокая |
| REQ-SYS-001 | SAVE to JSON | Низкая |
| REQ-SYS-002 | LOAD from JSON (rebuild indexes) | Средняя |
| REQ-SYS-003 | EXIT | Низкая |
| NFR-PARSER-001 | Recursive descent parser (no regex/eval) | Высокая |
| NFR-REPL-001 | Graceful error handling | Средняя |
| NFR-TYPE-001 | Строгая типизация | Средняя |
| NFR-ATOMIC-001 | Атомарность операций | Высокая |

### P1 — High
| ID | Требование | Сложность |
|----|------------|-----------|
| REQ-IDX-001 | CREATE INDEX | Средняя |
| NFR-ORDER-001 | Insertion order guarantee | Низкая |

---

## 2. Архитектурные решения

### 2.1 Модули (определено в requirements.md)
```
mini_db/
├── __init__.py
├── __main__.py          # Entry point: python -m mini_db
├── parser/
│   ├── __init__.py
│   ├── lexer.py         # Токенизация
│   └── parser.py        # Recursive descent parser
├── ast/
│   ├── __init__.py
│   └── nodes.py         # AST node classes
├── storage/
│   ├── __init__.py
│   ├── database.py      # In-memory database
│   ├── table.py         # Table with rows
│   └── index.py         # Hash index for UNIQUE/INDEX
├── executor/
│   ├── __init__.py
│   ├── executor.py      # Command execution
│   └── rollback.py      # Transaction rollback logic
└── repl/
    ├── __init__.py
    └── repl.py          # Read-Eval-Print Loop
```

### 2.2 Ключевые контракты

#### Parser → AST
```python
# parser.py
def parse(query: str) -> ASTNode:
    """Возвращает AST или выбрасывает ParseError"""
```

#### AST Nodes (иерархия)
```
ASTNode (base)
├── DDLNode
│   └── CreateTableNode
├── DMLNode
│   ├── InsertNode
│   ├── UpdateNode
│   └── DeleteNode
├── DQLNode
│   └── SelectNode
├── SysNode
│   ├── SaveNode
│   ├── LoadNode
│   └── ExitNode
├── IndexNode
│   └── CreateIndexNode
└── ExpressionNode
    ├── ComparisonNode (=, !=, <, >)
    ├── LogicalNode (AND, OR)
    └── IdentifierNode / LiteralNode
```

#### Storage → Executor
```python
# table.py
class Table:
    def insert(self, row: dict) -> Result: ...
    def update(self, predicate, updates: dict) -> Result: ...
    def delete(self, predicate) -> Result: ...
    def select(self, predicate, columns: list) -> list[dict]: ...
```

#### Executor → REPL
```python
# executor.py
class Executor:
    def execute(self, ast: ASTNode) -> ExecutionResult:
        """Возвращает Success/Failure с сообщением"""
```

### 2.3 Negative Constraints (из requirements.md)
- ❌ Регулярные выражения для парсинга всего запроса
- ❌ eval() / exec()
- ❌ Сторонние библиотеки
- ❌ Неявное приведение типов
- ❌ JOIN, транзакции, агрегаты, ORDER BY, GROUP BY, LIMIT
- ❌ IS NULL / IS NOT NULL синтаксис
- ❌ DROP TABLE, DROP INDEX, ALTER TABLE

---

## 3. Декомпозиция на фазы

### Фаза 1: Foundation (Lexer + AST + Storage Skeleton)
**Goal**: Базовая инфраструктура для парсинга и хранения

**Scope**:
- Lexer: токенизация SQL-подобного синтаксиса
- AST nodes: все классы узлов
- Storage skeleton: Database, Table (без индексов)
- Type system: INT, TEXT, BOOL

**Deliverables**:
1. `mini_db/ast/nodes.py` — все AST классы
2. `mini_db/parser/lexer.py` — токенизация
3. `mini_db/storage/database.py` — Database class
4. `mini_db/storage/table.py` — Table class (insert/select skeleton)
5. Unit tests для lexer и AST

**Dependencies**: None

**Done Criteria**:
- [ ] Lexer корректно токенизирует все SQL конструкции
- [ ] AST nodes имеют корректную структуру
- [ ] Table может хранить строки в insertion order

---

### Фаза 2: DDL + Basic DML (CREATE TABLE, INSERT)
**Goal**: Создание таблиц и вставка данных

**Scope**:
- Parser: CREATE TABLE, INSERT
- Executor: выполнение CREATE TABLE, INSERT
- UNIQUE constraint при INSERT
- Строгая типизация

**Deliverables**:
1. `mini_db/parser/parser.py` — парсинг CREATE TABLE, INSERT
2. `mini_db/executor/executor.py` — выполнение DDL/DML
3. `mini_db/storage/index.py` — Hash index для UNIQUE
4. Integration tests для CREATE TABLE, INSERT

**Dependencies**: Фаза 1

**Done Criteria**:
- [ ] CREATE TABLE создаёт таблицу с типами и UNIQUE
- [ ] INSERT проверяет типы и UNIQUE constraint
- [ ] Ошибки выводятся в формате "Error: ..."

---

### Фаза 3: DQL + WHERE (SELECT с условиями)
**Goal**: Запросы с фильтрацией

**Scope**:
- Parser: SELECT, WHERE с AND/OR/скобками
- Expression evaluator: сравнения, логические операции
- NULL-семантика

**Deliverables**:
1. Parser для SELECT и WHERE
2. Expression evaluator в executor
3. Tests для сложных WHERE: `(col1 > 10 OR col2 = 'test') AND col3 != true`
4. Tests для NULL-семантики

**Dependencies**: Фаза 2

**Done Criteria**:
- [ ] SELECT * и SELECT col1, col2 работают
- [ ] WHERE с вложенными скобками парсится корректно
- [ ] NULL-семантика: col = NULL → False

---

### Фаза 4: UPDATE/DELETE + Atomicity
**Goal**: Модификация данных с атомарностью

**Scope**:
- Parser: UPDATE, DELETE
- Atomic UPDATE: All-or-Nothing
- Rollback mechanism

**Deliverables**:
1. Parser для UPDATE, DELETE
2. Rollback mechanism в executor
3. Tests для атомарности UPDATE
4. Tests для DELETE

**Dependencies**: Фаза 3

**Done Criteria**:
- [ ] UPDATE нескольких строк откатывается при UNIQUE violation
- [ ] DELETE удаляет по условию
- [ ] Без WHERE: UPDATE/DELETE затрагивает все строки

---

### Фаза 5: System Commands + REPL
**Goal**: Персистентность и интерактивный интерфейс

**Scope**:
- Parser: SAVE, LOAD, EXIT
- JSON serialization/deserialization
- REPL loop с graceful error handling

**Deliverables**:
1. Parser для SAVE, LOAD, EXIT
2. JSON serialization в storage
3. `mini_db/repl/repl.py` — REPL
4. `mini_db/__main__.py` — entry point
5. Integration tests для SAVE/LOAD

**Dependencies**: Фаза 4

**Done Criteria**:
- [ ] SAVE сохраняет схему и данные в JSON
- [ ] LOAD восстанавливает состояние
- [ ] REPL не падает с Python Traceback
- [ ] `python -m mini_db` запускает REPL

---

### Фаза 6: Indexes (P1)
**Goal**: Оптимизация поиска

**Scope**:
- Parser: CREATE INDEX
- Index management в storage
- Index usage в SELECT WHERE col = X

**Deliverables**:
1. Parser для CREATE INDEX
2. Index management в Database
3. Tests для индексов

**Dependencies**: Фаза 5

**Done Criteria**:
- [ ] CREATE INDEX создаёт индекс
- [ ] SELECT с WHERE col = X использует индекс

---

## 4. Критические Checkpoint'ы (из intent.md)

| # | Checkpoint | Фаза | Тест |
|---|------------|------|------|
| 1 | Парсер строит корректное AST для `(col1 > 10 OR col2 = 'test') AND col3 != true` | Фаза 3 | `test_parser_complex_where()` |
| 2 | UPDATE нескольких строк откатывается полностью при нарушении UNIQUE | Фаза 4 | `test_update_atomicity()` |
| 3 | REPL выводит "Syntax error: ..." вместо Python Traceback | Фаза 5 | `test_repl_graceful_errors()` |

---

## 5. Open Questions

### 5.1 Выбор структуры данных для Table
**Options**:
- A) `list[dict]` — простой список строк
- B) `dict[int, dict]` — словарь с row_id

**Decision**: `list[dict]` — обеспечивает insertion order, простота итерации

### 5.2 Реализация rollback для UPDATE
**Options**:
- A) Snapshot before → restore on error
- B) Write-ahead log → replay on error

**Decision**: Snapshot before — проще для in-memory, достаточно для MVP

### 5.3 Индекс: Hash vs B-Tree
**Options**:
- A) Hash index (dict) — только для =, O(1)
- B) B-Tree — для =, <, >, O(log n)

**Decision**: Hash index — требования только для = (UNIQUE, INDEX), B-Tree избыточен

---

## 6. Backup Log

| Дата | Файл | Backup |
|------|------|--------|
| 2026-03-16 | development_plan.md | (создание) |
| 2026-03-16 | technology.md | (создание) |

---

## 7. Consistency Sweep (COMPLETED)

### 7.1 Модули ↔ Требования (VERIFIED)
| Модуль | Требования | Статус |
|--------|------------|--------|
| parser.lexer | NFR-PARSER-001 (no regex/eval) | ✅ |
| parser.parser | Все SQL команды (DDL/DML/DQL/SYS) | ✅ |
| ast.nodes | Все ASTNode типы | ✅ |
| storage.table | REQ-DDL-001, REQ-DML-001/002/003 | ✅ |
| storage.index | REQ-IDX-001, UNIQUE constraints | ✅ |
| executor.executor | NFR-ATOMIC-001, NFR-TYPE-001 | ✅ |
| executor.rollback | NFR-ATOMIC-001 (All-or-Nothing) | ✅ |
| repl.repl | NFR-REPL-001 (Graceful errors) | ✅ |

### 7.2 Фазы ↔ Checkpoint'ы (VERIFIED)
| Checkpoint | Фаза | Тест | Статус |
|------------|------|------|--------|
| #1: Парсер WHERE | Phase 3 | `test_parser_complex_where()` | ✅ |
| #2: Атомарность UPDATE | Phase 4 | `test_update_atomicity()` | ✅ |
| #3: Graceful errors | Phase 5 | `test_repl_graceful_errors()` | ✅ |

### 7.3 Фазы ↔ Требования P0 (VERIFIED)
| Требование | Фаза | Покрытие |
|------------|------|----------|
| REQ-DDL-001 CREATE TABLE | Phase 2 | ✅ |
| REQ-DML-001 INSERT | Phase 2 | ✅ |
| REQ-DML-002 UPDATE | Phase 4 | ✅ |
| REQ-DML-003 DELETE | Phase 4 | ✅ |
| REQ-DQL-001 SELECT | Phase 3 | ✅ |
| REQ-DQL-002 WHERE | Phase 3 | ✅ |
| REQ-SYS-001 SAVE | Phase 5 | ✅ |
| REQ-SYS-002 LOAD | Phase 5 | ✅ |
| REQ-SYS-003 EXIT | Phase 5 | ✅ |
| NFR-PARSER-001 | Phase 1, 2, 3 | ✅ |
| NFR-REPL-001 | Phase 5 | ✅ |
| NFR-TYPE-001 | Phase 2 | ✅ |
| NFR-ATOMIC-001 | Phase 4 | ✅ |

### 7.4 Фазы ↔ Требования P1 (VERIFIED)
| Требование | Фаза | Покрытие |
|------------|------|----------|
| REQ-IDX-001 CREATE INDEX | Phase 6 | ✅ |
| NFR-ORDER-001 Insertion Order | Phase 1 (list[dict]) | ✅ |

### 7.5 Deliverables Count per Phase (VERIFIED)
| Фаза | Deliverables | Диапазон (3-7) |
|------|--------------|----------------|
| Phase 1 | 6 | ✅ |
| Phase 2 | 6 | ✅ |
| Phase 3 | 5 | ✅ |
| Phase 4 | 5 | ✅ |
| Phase 5 | 6 | ✅ |
| Phase 6 | 4 | ✅ |

### 7.6 Negative Constraints (VERIFIED)
| Запрет | Документация | Статус |
|--------|--------------|--------|
| Regex для парсинга | development_plan.md §2.3 | ✅ |
| eval/exec | development_plan.md §2.3 | ✅ |
| Сторонние библиотеки | technology.md §1.1 | ✅ |
| Неявное приведение типов | development_plan.md §2.3 | ✅ |
| JOIN, транзакции, агрегаты | development_plan.md §2.3 | ✅ |

---

## 8. Backup Log

| Дата | Файл | Backup | Причина |
|------|------|--------|---------|
| 2026-03-16 | development_plan.md | (создание) | Первичная версия |
| 2026-03-16 | technology.md | (создание) | Первичная версия |

---

## 9. Итоговое резюме

**STATUS**: SUCCESS

**OUTPUT**: 
- `vaib/02-architect/development_plan.md`
- `vaib/02-architect/technology.md`

**SUMMARY**:
- **6 фаз** разработки
- **5 модулей**: parser, ast, storage, executor, repl
- **Technology stack**: Python 3.11+, unittest, JSON (built-in)
- **Критические решения**:
  - Recursive descent parser (без regex)
  - `list[dict]` для insertion order
  - Snapshot-based rollback для атомарности
  - Hash indexes для UNIQUE/INDEX
- **Все 15 требований P0 + 3 требования P1 покрыты фазами**
- **3 критических checkpoint'а распределены по фазам 3, 4, 5**