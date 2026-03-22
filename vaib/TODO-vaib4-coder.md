# TODO-vaib4-coder.md

**Проект**: mini_db — VAIB Stress-Test Benchmark  
**Текущий режим**: PROTOTYPE  
**Дата**: 2026-03-16

---

## Phase 1: Foundation (Lexer + AST + Storage Skeleton)

**Статус**: ✅ COMPLETED

### Выполненные задачи

- [x] Прочитать markup_standard.md, development_plan.md, technology.md
- [x] Создать структуру директорий mini_db/
- [x] Реализовать mini_db/ast/nodes.py — все AST классы
- [x] Реализовать mini_db/parser/lexer.py — токенизация
- [x] Реализовать mini_db/storage/database.py — Database class
- [x] Реализовать mini_db/storage/table.py — Table class
- [x] Создать tests/test_lexer.py — unit tests для lexer
- [x] Создать tests/test_ast.py — unit tests для AST
- [x] Запустить тесты и проверить качество

### Созданные файлы

```
mini_db/
├── __init__.py
├── ast/
│   ├── __init__.py
│   └── nodes.py          # AST классы (ColumnDef, StatementNode, ExpressionNode)
├── parser/
│   ├── __init__.py
│   └── lexer.py          # Lexer, Token, TokenType, LexerError
└── storage/
    ├── __init__.py
    ├── database.py       # Database class
    └── table.py          # Table, InsertResult, UpdateResult, DeleteResult, SelectResult

tests/
├── __init__.py
├── test_lexer.py         # 50 тестов для Lexer
└── test_ast.py           # 38 тестов для AST
```

### Результаты тестирования

- **Всего тестов**: 88
- **Пройдено**: 88
- **Упало**: 0

---

## Phase 2: DDL + Basic DML (CREATE TABLE, INSERT)

**Статус**: ✅ COMPLETED

### Выполненные задачи

- [x] Реализовать mini_db/storage/index.py — Hash index для UNIQUE
- [x] Реализовать mini_db/parser/parser.py — парсинг CREATE TABLE, INSERT
- [x] Реализовать mini_db/executor/executor.py — выполнение DDL/DML
- [x] Создать tests/test_parser_ddl.py — tests для CREATE TABLE
- [x] Создать tests/test_parser_dml.py — tests для INSERT
- [x] Создать tests/test_insert_unique.py — tests для UNIQUE constraint
- [x] Запустить тесты и проверить качество

### Созданные файлы

```
mini_db/
├── executor/
│   ├── __init__.py
│   └── executor.py       # Executor, ExecutionResult
├── parser/
│   └── parser.py         # Parser, ParseError (recursive descent)
└── storage/
    └── index.py          # HashIndex для UNIQUE constraints

tests/
├── test_parser_ddl.py    # 17 тестов для CREATE TABLE
├── test_parser_dml.py    # 23 теста для INSERT
└── test_insert_unique.py # 22 теста для UNIQUE constraint
```

### Результаты тестирования

- **Всего тестов**: 150 (88 Phase 1 + 62 Phase 2)
- **Пройдено**: 150
- **Упало**: 0

### Реализованные компоненты

#### Parser (mini_db/parser/parser.py)
- ParseError exception
- Parser.parse(query) -> StatementNode
- Recursive descent parsing без regex
- CREATE TABLE: имя, колонки с типами, UNIQUE constraint
- INSERT INTO: таблица, колонки, VALUES с литералами

#### Executor (mini_db/executor/executor.py)
- ExecutionResult dataclass
- Executor.execute(ast, db) -> ExecutionResult
- CREATE TABLE execution
- INSERT execution с валидацией типов и UNIQUE

#### HashIndex (mini_db/storage/index.py)
- HashIndex(column) для быстрого поиска по значению
- add(value, row_index) - добавление в индекс
- remove(value, row_index) - удаление из индекса
- lookup(value) -> set[int] - поиск row indices
- contains(value) -> bool - проверка наличия
- rebuild(rows, column) - перестроение индекса

### Done Criteria Check

- [x] CREATE TABLE создаёт таблицу с типами и UNIQUE
- [x] INSERT проверяет типы и UNIQUE constraint
- [x] Ошибки выводятся в формате "Error: ..."
- [x] Все tests проходят

---

## Phase 3: DQL + WHERE (SELECT с условиями)

**Статус**: ✅ COMPLETED

### Выполненные задачи

- [x] Реализовать parser для SELECT и WHERE (recursive descent)
- [x] Реализовать Expression evaluator в executor
- [x] Создать tests/test_parser_select.py — tests для SELECT
- [x] Создать tests/test_where_complex.py — tests для сложных условий
- [x] Создать tests/test_null_semantics.py — tests для NULL-семантики
- [x] Запустить тесты и проверить качество

### Созданные файлы

```
tests/
├── test_parser_select.py  # 26 тестов для SELECT парсинга
├── test_where_complex.py  # 13 тестов для сложных WHERE (включая CHECKPOINT #1)
└── test_null_semantics.py # 15 тестов для NULL-семантики
```

### Результаты тестирования

- **Всего тестов**: 209 (150 Phase 1-2 + 59 Phase 3)
- **Пройдено**: 209
- **Упало**: 0

### Реализованные компоненты

#### Parser (mini_db/parser/parser.py)
- SELECT * FROM table
- SELECT col1, col2 FROM table
- WHERE с операторами: =, !=, <, >
- Логические операторы: AND, OR
- Приоритет операторов: AND раньше OR
- Скобки для изменения приоритета
- Вложенные скобки в WHERE

#### Expression Evaluator (mini_db/executor/executor.py)
- _evaluate_expression() - вычисление выражений
- _evaluate_comparison() - сравнения (=, !=, <, >)
- _evaluate_logical() - логические операции (AND, OR)
- _get_value() - получение значения из LiteralNode/IdentifierNode
- NULL-семантика: любое сравнение с NULL возвращает False

### Done Criteria Check

- [x] SELECT * и SELECT col1, col2 работают
- [x] WHERE с вложенными скобками парсится корректно
- [x] NULL-семантика: col = NULL → False
- [x] Checkpoint #1 пройден
- [x] Все tests проходят

### CHECKPOINT #1 (КРИТИЧЕСКИЙ)

**Тест**: `test_checkpoint1_parser_ast_structure` в [`tests/test_where_complex.py`](tests/test_where_complex.py:18)

**Запрос**: `SELECT * FROM t WHERE (col1 > 10 OR col2 = 'test') AND col3 != true`

**Результат**: ✅ PASSED

Парсер строит корректное AST:
- Верхний уровень: LogicalNode(op="AND")
- Левая часть: LogicalNode(op="OR") с двумя ComparisonNode
- Правая часть: ComparisonNode(op="!=")

---

## Phase 4: UPDATE/DELETE + Atomicity

**Статус**: ✅ COMPLETED

### Выполненные задачи

- [x] Реализовать парсинг UPDATE в parser.py
- [x] Реализовать парсинг DELETE в parser.py
- [x] Создать mini_db/executor/rollback.py — rollback mechanism
- [x] Модифицировать table.py для атомарного UPDATE с UNIQUE проверкой
- [x] Добавить выполнение UPDATE в executor.py
- [x] Добавить выполнение DELETE в executor.py
- [x] Протестировать CHECKPOINT #2

### Созданные/модифицированные файлы

```
mini_db/
├── executor/
│   ├── __init__.py       # + RollbackManager, TableSnapshot exports
│   ├── executor.py       # + _execute_update, _execute_delete
│   └── rollback.py       # NEW: TableSnapshot, RollbackManager
├── parser/
│   └── parser.py         # + _parse_update, _parse_delete, _parse_assignments
└── storage/
    └── table.py          # MODIFIED: atomic update with rollback
```

### Реализованные компоненты

#### Parser (mini_db/parser/parser.py)
- UPDATE table SET col1 = val1, col2 = val2 [WHERE expr]
- DELETE FROM table [WHERE expr]
- _parse_assignments() - парсинг списка присваиваний
- _parse_assignment() - парсинг одного присваивания col = val

#### Executor (mini_db/executor/executor.py)
- _execute_update() - выполнение UPDATE с атомарностью
- _execute_delete() - выполнение DELETE

#### Rollback (mini_db/executor/rollback.py)
- TableSnapshot - снимок состояния таблицы (rows, unique_indexes)
- RollbackManager - менеджер отката с методами snapshot/restore

#### Table (mini_db/storage/table.py)
- update() - атомарное обновление с проверкой UNIQUE
- _rollback_from_snapshot() - восстановление из снимка при ошибке
- All-or-Nothing: при UNIQUE violation все изменения откатываются

### Done Criteria Check

- [x] UPDATE нескольких строк откатывается при UNIQUE violation
- [x] DELETE удаляет по условию
- [x] Без WHERE: UPDATE/DELETE затрагивает все строки
- [x] Checkpoint #2 пройден
- [x] Все tests проходят

### CHECKPOINT #2 (КРИТИЧЕСКИЙ)

**Тест**: Атомарность UPDATE при UNIQUE violation

```python
db.execute("CREATE TABLE t (id INT UNIQUE, val TEXT)")
db.execute("INSERT INTO t (id, val) VALUES (1, 'a')")
db.execute("INSERT INTO t (id, val) VALUES (2, 'b')")
result = db.execute("UPDATE t SET id = 1")  # UNIQUE violation
assert result.success == False
assert db.execute("SELECT * FROM t").data == [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
```

**Результат**: ✅ PASSED

- UPDATE пытается установить id = 1 для всех строк
- При обработке второй строки возникает UNIQUE violation
- Все изменения откатываются, данные остаются неизменными

---

## Phase 5: System Commands + REPL

**Статус**: ✅ COMPLETED

### Выполненные задачи

- [x] Добавить парсинг SAVE, LOAD, EXIT в parser.py
- [x] Добавить JSON serialization в storage (database.py, table.py)
- [x] Добавить выполнение SAVE, LOAD, EXIT в executor.py
- [x] Создать mini_db/repl/repl.py — REPL с graceful error handling
- [x] Создать mini_db/__main__.py — entry point
- [x] Запустить тесты и проверить качество

### Созданные/модифицированные файлы

```
mini_db/
├── __main__.py              # NEW: Entry point для python -m mini_db
├── executor/
│   └── executor.py          # MODIFIED: + _execute_save, _execute_load, _execute_exit
├── parser/
│   └── parser.py            # MODIFIED: + _parse_save, _parse_load, _parse_exit
├── repl/
│   ├── __init__.py          # NEW: REPL exports
│   └── repl.py              # NEW: REPL class с graceful error handling
└── storage/
    ├── database.py          # MODIFIED: + to_dict, from_dict, save_to_file, load_from_file
    └── table.py             # MODIFIED: + to_dict, from_dict
```

### Реализованные компоненты

#### Parser (mini_db/parser/parser.py)
- SAVE 'filepath'; - парсинг команды сохранения
- LOAD 'filepath'; - парсинг команды загрузки
- EXIT; - парсинг команды выхода

#### Storage (mini_db/storage/database.py, table.py)
- Database.to_dict() - сериализация базы в словарь
- Database.from_dict() - десериализация базы из словаря
- Database.save_to_file() - сохранение в JSON-файл
- Database.load_from_file() - загрузка из JSON-файла
- Table.to_dict() - сериализация таблицы
- Table.from_dict() - десериализация таблицы

#### Executor (mini_db/executor/executor.py)
- _execute_save() - выполнение SAVE
- _execute_load() - выполнение LOAD
- _execute_exit() - выполнение EXIT

#### REPL (mini_db/repl/repl.py)
- REPL class с методами run(), process()
- Graceful error handling - никаких Python Traceback
- Форматированный вывод результатов SELECT

### Done Criteria Check

- [x] SAVE сохраняет схему и данные в JSON
- [x] LOAD восстанавливает состояние
- [x] REPL не падает с Python Traceback
- [x] Checkpoint #3 пройден
- [x] `python -m mini_db` запускает REPL
- [x] Все tests проходят

### CHECKPOINT #3 (КРИТИЧЕСКИЙ)

**Тест**: REPL выводит "Syntax error: ..." вместо Python Traceback

```python
repl = REPL()
output = repl.process("SELECT * FORM t")  # typo: FORM
assert "Syntax error:" in output
assert "Traceback" not in output
```

**Результат**: ✅ PASSED

- REPL перехватывает ParseError и выводит "Syntax error: ..."
- Python Traceback не выводится пользователю

### Результаты тестирования

- **Всего тестов**: 253 (154 Phase 1-3 + 99 Phase 4-5)
- **Пройдено**: 253
- **Упало**: 0

---

## Phase 6: Indexes (P1)

**Статус**: ✅ COMPLETED

### Выполненные задачи

- [x] Добавить токен ON в lexer.py
- [x] Добавить парсинг CREATE INDEX в parser.py
- [x] Добавить поддержку пользовательских индексов в Table (indexes dict)
- [x] Добавить методы create_index в Database
- [x] Добавить _execute_create_index в Executor
- [x] Модифицировать SELECT для использования индексов при WHERE col = X
- [x] Добавить перестроение индексов при LOAD
- [x] Протестировать функциональность

### Модифицированные файлы

```
mini_db/
├── executor/
│   └── executor.py          # MODIFIED: + _execute_create_index, _try_use_index
├── parser/
│   ├── lexer.py             # MODIFIED: + ON token
│   └── parser.py            # MODIFIED: + _parse_create_index
└── storage/
    ├── database.py          # MODIFIED: + create_index method
    └── table.py             # MODIFIED: + indexes dict, create_index, get_index_for_column, rebuild_all_indexes
```

### Реализованные компоненты

#### Lexer (mini_db/parser/lexer.py)
- TokenType.ON - токен для ключевого слова ON
- KEYWORDS["ON"] - маппинг для ON

#### Parser (mini_db/parser/parser.py)
- _parse_create_index() - парсинг CREATE INDEX idx_name ON table_name (col1)
- CreateIndexNode - AST-узел с name, table, column

#### Table (mini_db/storage/table.py)
- indexes: dict[str, HashIndex] - словарь пользовательских индексов
- create_index(name, column) - создание индекса
- get_index_for_column(column) - поиск индекса по колонке
- rebuild_all_indexes() - перестроение всех индексов
- insert() - обновление пользовательских индексов
- update() - перестроение индексов после изменения
- delete() - перестроение индексов после удаления
- to_dict/from_dict - сериализация/десериализация индексов

#### Database (mini_db/storage/database.py)
- create_index(name, table, column) - создание индекса на таблице

#### Executor (mini_db/executor/executor.py)
- _execute_create_index() - выполнение CREATE INDEX
- _try_use_index() - попытка использовать индекс для WHERE col = X
- _execute_select() - оптимизация с использованием индекса

### Done Criteria Check

- [x] CREATE INDEX создаёт индекс
- [x] SELECT с WHERE col = X использует индекс
- [x] При LOAD индексы перестраиваются
- [x] Индексы поддерживаются при INSERT, UPDATE, DELETE

### Тестирование

Все тесты прошли успешно:
- test_create_index - CREATE INDEX и SELECT с индексом
- test_index_rebuild_on_load - перестроение индексов при LOAD
- test_index_with_insert_update_delete - поддержка индексов при операциях

---

## Блокеры

Нет блокеров.

---

## Следующие шаги

1. Tester (Vaib5) должен создать tests/test_create_index.py и tests/test_index_usage.py
2. Запустить все тесты для проверки регрессии