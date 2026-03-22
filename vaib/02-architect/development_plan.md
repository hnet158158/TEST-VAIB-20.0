# Development Plan: mini_db

**Проект**: mini_db — VAIB Stress-Test Benchmark  
**Версия**: 1.0  
**Дата**: 2026-03-16  
**Статус**: Approved

---

## 1. Project Overview

### 1.1 Назначение
mini_db — локальная in-memory СУБД на чистом Python 3.11+, предназначенная для валидации возможностей VAIB-агентов. **Не является продакшен-СУБД.**

### 1.2 Цель
Доказать, что VAIB-агенты способны самостоятельно реализовать систему с нетривиальной архитектурой:
- Recursive descent parser (без regex/eval)
- AST-based query execution
- In-memory storage с индексами
- Atomic UPDATE с rollback
- Graceful error handling

### 1.3 Критические Checkpoint'ы
1. `[TEST]` Парсер строит корректное AST для `(col1 > 10 OR col2 = 'test') AND col3 != true`
2. `[TEST]` UPDATE нескольких строк откатывается полностью при нарушении UNIQUE
3. `[TEST]` REPL выводит "Syntax error: ..." вместо Python Traceback

### 1.4 Технологические ограничения
| Ограничение | Значение |
|-------------|----------|
| Python версия | 3.11+ |
| Сторонние библиотеки | Запрещены |
| Запуск REPL | `python -m mini_db` |

---

## 2. Architecture / Modules

### 2.1 Структура модулей
```
mini_db/
├── __init__.py
├── __main__.py              # Entry point
├── parser/
│   ├── __init__.py
│   ├── lexer.py             # Токенизация SQL
│   └── parser.py            # Recursive descent parser
├── ast/
│   ├── __init__.py
│   └── nodes.py             # AST node classes
├── storage/
│   ├── __init__.py
│   ├── database.py          # In-memory database
│   ├── table.py             # Table with rows
│   └── index.py             # Hash index
├── executor/
│   ├── __init__.py
│   ├── executor.py          # Command execution
│   └── rollback.py          # Transaction rollback
└── repl/
    ├── __init__.py
    └── repl.py              # Read-Eval-Print Loop
```

### 2.2 Module Contracts

#### parser.lexer
```python
class Token:
    type: TokenType
    value: Any
    position: int

class Lexer:
    def tokenize(query: str) -> list[Token]:
        """Возвращает список токенов или выбрасывает LexerError"""
```

#### parser.parser
```python
class Parser:
    def parse(query: str) -> ASTNode:
        """Возвращает AST или выбрасывает ParseError"""
```

#### ast.nodes
```python
# Иерархия ASTNode
ASTNode (abstract base)
├── StatementNode
│   ├── CreateTableNode(name, columns: list[ColumnDef])
│   ├── InsertNode(table, columns, values)
│   ├── UpdateNode(table, assignments, where)
│   ├── DeleteNode(table, where)
│   ├── SelectNode(table, columns, where)
│   ├── CreateIndexNode(name, table, column)
│   ├── SaveNode(filepath)
│   ├── LoadNode(filepath)
│   └── ExitNode()
└── ExpressionNode
    ├── ComparisonNode(left, op, right)  # =, !=, <, >
    ├── LogicalNode(left, op, right)     # AND, OR
    ├── IdentifierNode(name)
    └── LiteralNode(value)
```

#### storage.table
```python
class Table:
    name: str
    columns: dict[str, ColumnDef]
    rows: list[dict]  # insertion order
    unique_indexes: dict[str, set]
    
    def insert(row: dict) -> InsertResult
    def update(predicate, updates: dict) -> UpdateResult
    def delete(predicate) -> DeleteResult
    def select(predicate, columns: list) -> list[dict]
```

#### storage.index
```python
class HashIndex:
    def __init__(column: str)
    def add(value, row_index: int)
    def remove(value, row_index: int)
    def lookup(value) -> set[int]  # row indices
```

#### executor.executor
```python
class Executor:
    def execute(ast: ASTNode, db: Database) -> ExecutionResult:
        """Выполняет AST, возвращает Success/Failure"""

class ExecutionResult:
    success: bool
    message: str
    data: Optional[list[dict]]
```

#### repl.repl
```python
class REPL:
    def run():
        """Main loop: read → parse → execute → print"""
        # Все ошибки перехватываются, выводится "Error: ..." или "Syntax error: ..."
```

### 2.3 Negative Constraints

#### Запрещённые техники
| Область | Запрет | Причина |
|---------|--------|---------|
| Parser | `re` для всего запроса | NFR-PARSER-001 |
| Parser | `eval()`, `exec()` | Security, NFR-PARSER-001 |
| Types | Неявное приведение | NFR-TYPE-001 |
| Dependencies | Сторонние библиотеки | ТЗ |

#### Out of Scope
| Фича | Причина |
|------|---------|
| JOIN | Не требуется для бенчмарка |
| Транзакции (BEGIN/COMMIT) | UPDATE атомарный по умолчанию |
| Агрегаты (COUNT, SUM) | P2 |
| ORDER BY, GROUP BY, LIMIT | P2 |
| IS NULL / IS NOT NULL | Не требуется |
| DROP TABLE, DROP INDEX | P2 |
| ALTER TABLE | P2 |

### 2.4 Data Flow Map

```
User Input (SQL)
       │
       ▼
    Lexer ──────► Tokens
       │
       ▼
    Parser ──────► AST
       │
       ▼
   Executor ──────► ExecutionResult
       │
       ▼
   Storage ◄──────► Database/Table/Index
       │
       ▼
    Output (Success/Error message)
```

---

## 3. Phases

### Phase 1: Foundation (Lexer + AST + Storage Skeleton)

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
5. `tests/test_lexer.py` — unit tests для lexer
6. `tests/test_ast.py` — unit tests для AST

**Dependencies**: None

**Done Criteria**:
- [ ] Lexer корректно токенизирует все SQL конструкции
- [ ] AST nodes имеют корректную структуру
- [ ] Table может хранить строки в insertion order
- [ ] Все tests проходят

**Estimated Coder Sessions**: 1-2

---

### Phase 2: DDL + Basic DML (CREATE TABLE, INSERT)

**Goal**: Создание таблиц и вставка данных с валидацией

**Scope**:
- Parser: CREATE TABLE, INSERT
- Executor: выполнение CREATE TABLE, INSERT
- UNIQUE constraint при INSERT
- Строгая типизация

**Deliverables**:
1. `mini_db/parser/parser.py` — парсинг CREATE TABLE, INSERT
2. `mini_db/executor/executor.py` — выполнение DDL/DML
3. `mini_db/storage/index.py` — Hash index для UNIQUE
4. `tests/test_parser_ddl.py` — tests для CREATE TABLE
5. `tests/test_parser_dml.py` — tests для INSERT
6. `tests/test_insert_unique.py` — tests для UNIQUE constraint

**Dependencies**: Phase 1

**Done Criteria**:
- [ ] CREATE TABLE создаёт таблицу с типами и UNIQUE
- [ ] INSERT проверяет типы и UNIQUE constraint
- [ ] Ошибки выводятся в формате "Error: ..."
- [ ] Все tests проходят

**Estimated Coder Sessions**: 1-2

---

### Phase 3: DQL + WHERE (SELECT с условиями)

**Goal**: Запросы с фильтрацией и сложными условиями

**Scope**:
- Parser: SELECT, WHERE с AND/OR/скобками
- Expression evaluator: сравнения, логические операции
- NULL-семантика

**Deliverables**:
1. Parser для SELECT и WHERE (recursive descent)
2. Expression evaluator в executor
3. `tests/test_parser_select.py` — tests для SELECT
4. `tests/test_where_complex.py` — tests для `(col1 > 10 OR col2 = 'test') AND col3 != true`
5. `tests/test_null_semantics.py` — tests для NULL-семантики

**Dependencies**: Phase 2

**Done Criteria**:
- [ ] SELECT * и SELECT col1, col2 работают
- [ ] WHERE с вложенными скобками парсится корректно
- [ ] NULL-семантика: col = NULL → False
- [ ] Checkpoint #1 пройден
- [ ] Все tests проходят

**Estimated Coder Sessions**: 2

---

### Phase 4: UPDATE/DELETE + Atomicity

**Goal**: Модификация данных с атомарностью

**Scope**:
- Parser: UPDATE, DELETE
- Atomic UPDATE: All-or-Nothing
- Rollback mechanism (snapshot before)

**Deliverables**:
1. Parser для UPDATE, DELETE
2. `mini_db/executor/rollback.py` — rollback mechanism
3. `tests/test_update.py` — tests для UPDATE
4. `tests/test_update_atomicity.py` — tests для атомарности
5. `tests/test_delete.py` — tests для DELETE

**Dependencies**: Phase 3

**Done Criteria**:
- [ ] UPDATE нескольких строк откатывается при UNIQUE violation
- [ ] DELETE удаляет по условию
- [ ] Без WHERE: UPDATE/DELETE затрагивает все строки
- [ ] Checkpoint #2 пройден
- [ ] Все tests проходят

**Estimated Coder Sessions**: 1-2

---

### Phase 5: System Commands + REPL

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
5. `tests/test_save_load.py` — tests для SAVE/LOAD
6. `tests/test_repl_errors.py` — tests для graceful errors

**Dependencies**: Phase 4

**Done Criteria**:
- [ ] SAVE сохраняет схему и данные в JSON
- [ ] LOAD восстанавливает состояние
- [ ] REPL не падает с Python Traceback
- [ ] Checkpoint #3 пройден
- [ ] `python -m mini_db` запускает REPL
- [ ] Все tests проходят

**Estimated Coder Sessions**: 1-2

---

### Phase 6: Indexes (P1)

**Goal**: Оптимизация поиска по индексам

**Scope**:
- Parser: CREATE INDEX
- Index management в storage
- Index usage в SELECT WHERE col = X

**Deliverables**:
1. Parser для CREATE INDEX
2. Index management в Database
3. `tests/test_create_index.py` — tests для CREATE INDEX
4. `tests/test_index_usage.py` — tests для использования индекса

**Dependencies**: Phase 5

**Done Criteria**:
- [ ] CREATE INDEX создаёт индекс
- [ ] SELECT с WHERE col = X использует индекс
- [ ] При LOAD индексы перестраиваются
- [ ] Все tests проходят

**Estimated Coder Sessions**: 1

---

## 4. Phase Execution Status

| Phase | Status | Start Date | End Date | Notes |
|-------|--------|------------|----------|-------|
| Phase 1: Foundation | DONE | 2026-03-16 | 2026-03-16 | 88 tests pass, all Done Criteria met |
| Phase 2: DDL + Basic DML | DONE | 2026-03-16 | 2026-03-16 | 70 tests pass (62 Phase 2 + 8 adversarial), all Done Criteria met |
| Phase 3: DQL + WHERE | DONE | 2026-03-16 | 2026-03-16 | 54 tests, CHECKPOINT #1 VERIFIED |
| Phase 4: UPDATE/DELETE + Atomicity | DONE | 2026-03-16 | 2026-03-16 | 49 tests, CHECKPOINT #2 VERIFIED, 258 total |
| Phase 5: System Commands + REPL | DONE | 2026-03-16 | 2026-03-16 | 68 tests, CHECKPOINT #3 VERIFIED, 326 total |
| Phase 6: Indexes | DONE | 2026-03-16 | 2026-03-16 | 36 tests, all Done Criteria met, 362 total |

---

## 5. Test Coverage Requirements

### 5.1 Обязательные тесты (из requirements.md)
1. Атомарность UPDATE при нарушении UNIQUE
2. Работа индексов (ускорение SELECT с WHERE col = X)
3. Парсинг сложных WHERE со вложенными скобками
4. Строгая типизация (отказ при несовпадении типов)
5. Graceful error handling (нет traceback)

### 5.2 Checkpoint Tests
```python
# Checkpoint #1: Parser WHERE
def test_parser_complex_where():
    query = "SELECT * FROM t WHERE (col1 > 10 OR col2 = 'test') AND col3 != true"
    ast = parser.parse(query)
    assert isinstance(ast.where, LogicalNode)
    assert ast.where.op == "AND"

# Checkpoint #2: UPDATE Atomicity
def test_update_atomicity():
    db = Database()
    db.execute("CREATE TABLE t (id INT UNIQUE, val TEXT)")
    db.execute("INSERT INTO t (id, val) VALUES (1, 'a')")
    db.execute("INSERT INTO t (id, val) VALUES (2, 'b')")
    result = db.execute("UPDATE t SET id = 1")  # UNIQUE violation
    assert result.success == False
    assert db.execute("SELECT * FROM t").data == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]

# Checkpoint #3: Graceful Errors
def test_repl_graceful_errors():
    repl = REPL()
    output = repl.process("SELECT * FORM t")  # typo: FORM
    assert "Syntax error:" in output
    assert "Traceback" not in output
```

---

## 6. Open Questions

*Нет открытых вопросов. Все архитектурные решения задокументированы в TODO-vaib2-architect.md.*

---

## 7. Acceptance Criteria

### 7.1 Количественные метрики
- [ ] Все тесты в Test Suite проходят (0 failures)
- [ ] 3 критических checkpoint'а покрыты тестами

### 7.2 Качественные метрики
- [ ] Код разбит на логические модули
- [ ] VAIB pipeline отработал без ручных корректировок
- [ ] REPL не падает с Python Traceback

---

**Документ подготовлен**: Vaib2 Architect  
**Следующий этап**: Vaib3 Spec → документация технологий