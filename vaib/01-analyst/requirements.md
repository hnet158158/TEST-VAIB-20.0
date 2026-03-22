# Requirements Specification: mini_db

**Проект**: mini_db — VAIB Stress-Test Benchmark  
**Версия**: 1.0  
**Дата**: 2026-03-16  
**Статус**: Approved

---

## 1. Введение

### 1.1 Назначение
mini_db — локальная in-memory СУБД на чистом Python 3.11+, предназначенная для валидации возможностей VAIB-агентов. **Не является продакшен-СУБД.**

### 1.2 AAG-модель
| Компонент | Описание |
|-----------|----------|
| **Actor** | VAIB-агенты (Coder, Tester, Architect, Skeptic) |
| **Action** | Реализовать in-memory СУБД с REPL-интерфейсом |
| **Goal** | Доказать способность агентов реализовать систему с нетривиальной архитектурой |

### 1.3 Критические Checkpoint'ы
1. `[TEST]` Парсер строит корректное AST для `(col1 > 10 OR col2 = 'test') AND col3 != true`
2. `[TEST]` UPDATE нескольких строк откатывается полностью при нарушении UNIQUE
3. `[TEST]` REPL выводит "Syntax error: ..." вместо Python Traceback

---

## 2. Функциональные требования

### 2.1 DDL — Data Definition Language

#### [P0] REQ-DDL-001: CREATE TABLE
**Приоритет**: CRITICAL

**Синтаксис**:
```sql
CREATE TABLE table_name (col1 INT, col2 TEXT UNIQUE, col3 BOOL);
```

**Требования**:
- Поддерживаемые типы: `INT`, `TEXT`, `BOOL`
- Модификатор `UNIQUE` опционален для каждой колонки
- Имена таблиц и колонок — case-sensitive идентификаторы
- Повторное создание существующей таблицы — ошибка

**Ошибки**:
- `Error: Table 'X' already exists`

---

### 2.2 DML — Data Manipulation Language

#### [P0] REQ-DML-001: INSERT
**Приоритет**: CRITICAL

**Синтаксис**:
```sql
INSERT INTO table_name (col3, col1) VALUES (true, 42);
```

**Требования**:
- Порядок колонок может не совпадать с CREATE TABLE
- Пропущенные колонки заполняются `null` (Python `None`)
- Строгая типизация: тип значения должен соответствовать типу колонки
- Атомарность: при нарушении UNIQUE строка не добавляется

**Типизация**:
| Тип колонки | Допустимые значения | Недопустимые значения |
|-------------|---------------------|----------------------|
| INT | Python `int` | `str`, `bool`, `None` при вставке |
| TEXT | Python `str` | `int`, `bool` |
| BOOL | Python `bool` | `int`, `str` |

**Ошибки**:
- `Error: Table 'X' does not exist`
- `Error: Column 'Y' does not exist in table 'X'`
- `Error: Type mismatch: expected INT, got TEXT`
- `Error: UNIQUE constraint violated on column 'Y'`

**Feedback**: `Success: 1 row inserted`

---

#### [P0] REQ-DML-002: UPDATE
**Приоритет**: CRITICAL

**Синтаксис**:
```sql
UPDATE table_name SET col1 = 99, col2 = 'new' WHERE col1 < 5;
```

**Требования**:
- WHERE опционален (без WHERE обновляются все строки)
- Атомарность **All-or-Nothing**: если обновление любой строки нарушает UNIQUE или типизацию, ВСЯ операция отменяется
- Строгая типизация при присваивании

**Атомарность (CRITICAL)**:
```
Сценарий: UPDATE затрагивает 3 строки, на 2-й строке нарушение UNIQUE
Результат: Ни одна из 3 строк не изменяется
Вывод: Error: UNIQUE constraint violated on column 'X'
```

**Ошибки**:
- `Error: Table 'X' does not exist`
- `Error: Column 'Y' does not exist`
- `Error: Type mismatch: expected INT, got TEXT`
- `Error: UNIQUE constraint violated on column 'Y'`

**Feedback**: `Success: N rows updated`

---

#### [P0] REQ-DML-003: DELETE
**Приоритет**: CRITICAL

**Синтаксис**:
```sql
DELETE FROM table_name WHERE col2 = 'old';
```

**Требования**:
- WHERE опционален (без WHERE удаляются все строки)
- Возвращает количество удалённых строк

**Ошибки**:
- `Error: Table 'X' does not exist`
- `Error: Column 'Y' does not exist`

**Feedback**: `Success: N rows deleted`

---

### 2.3 DQL — Data Query Language

#### [P0] REQ-DQL-001: SELECT
**Приоритет**: CRITICAL

**Синтаксис**:
```sql
SELECT col1, col3 FROM table_name WHERE (col1 > 10 OR col2 = 'test') AND col3 != true;
SELECT * FROM table_name;
```

**Требования**:
- `SELECT *` возвращает все колонки в порядке CREATE TABLE
- WHERE опционален
- Порядок строк: **insertion order гарантирован**
- NULL в результатах отображается как `null`

**Ошибки**:
- `Error: Table 'X' does not exist`
- `Error: Column 'Y' does not exist`

---

#### [P0] REQ-DQL-002: WHERE-условия
**Приоритет**: CRITICAL

**Операторы сравнения**:
| Оператор | Применим к | Семантика |
|----------|------------|-----------|
| `=` | INT, TEXT, BOOL | Равенство |
| `!=` | INT, TEXT, BOOL | Неравенство |
| `>` | INT, TEXT | Больше |
| `<` | INT, TEXT | Меньше |

**Логические операторы**:
- `AND` — логическое И
- `OR` — логическое ИЛИ

**Группировка**:
- Скобки `(` и `)` для изменения приоритета
- Приоритет: `AND` выполняется раньше `OR`, скобки меняют приоритет

**NULL-семантика**:
| Выражение | Результат |
|-----------|-----------|
| `col = NULL` | `False` (строка отбрасывается) |
| `col != NULL` | `False` |
| `col > NULL` | `False` |
| `col < NULL` | `False` |

**Примечание**: Синтаксис `IS NULL` не поддерживается.

---

### 2.4 Индексы

#### [P1] REQ-IDX-001: CREATE INDEX
**Приоритет**: HIGH

**Синтаксис**:
```sql
CREATE INDEX idx_name ON table_name (col1);
```

**Требования**:
- Индекс ускоряет поиск по условиям `=`
- При LOAD индексы перестраиваются автоматически
- Повторное создание индекса с тем же именем — ошибка

**Ошибки**:
- `Error: Index 'X' already exists`
- `Error: Table 'Y' does not exist`
- `Error: Column 'Z' does not exist`

---

### 2.5 Системные команды

#### [P0] REQ-SYS-001: SAVE
**Приоритет**: CRITICAL

**Синтаксис**:
```sql
SAVE 'dump.json';
```

**Требования**:
- Сохраняет схему и данные в JSON-файл
- Путь относительно текущей директории
- Перезаписывает существующий файл

**Ошибки**:
- `Error: Cannot write to file 'X'`

**Feedback**: `Success: Database saved to 'dump.json'`

---

#### [P0] REQ-SYS-002: LOAD
**Приоритет**: CRITICAL

**Синтаксис**:
```sql
LOAD 'dump.json';
```

**Требования**:
- Восстанавливает схему и данные из JSON-файла
- **Индексы перестраиваются автоматически**
- При ошибке загрузки текущее состояние БД сохраняется

**Ошибки**:
- `Error: File not found: 'dump.json'` — БД не изменяется
- `Error: Invalid JSON in file 'dump.json'`

**Feedback**: `Success: Database loaded from 'dump.json'`

---

#### [P0] REQ-SYS-003: EXIT
**Приоритет**: CRITICAL

**Синтаксис**:
```sql
EXIT;
```

**Требования**:
- Завершает работу REPL
- Не требует сохранения (явный SAVE)

---

## 3. Нефункциональные требования

### 3.1 [P0] NFR-PARSER-001: Изоляция парсера
**Приоритет**: CRITICAL

**Запрещено**:
- Регулярные выражения для парсинга всего запроса целиком (ad-hoc)
- `eval()` и `exec()`

**Требуется**:
- Recursive descent parser или аналогичный структурированный подход
- Корректная обработка произвольного количества пробелов, переносов строк, вложенных скобок

---

### 3.2 [P0] NFR-REPL-001: Graceful Error Handling
**Приоритет**: CRITICAL

**Требования**:
- REPL **НЕ** должен падать с Python Traceback
- Все ошибки должны перехватываться и выводиться в формате:
  - `Syntax error: <описание>` — синтаксические ошибки
  - `Error: <описание>` — семантические ошибки
- После ошибки REPL продолжает работу и ожидает следующую команду

---

### 3.3 [P0] NFR-TYPE-001: Строгая типизация
**Приоритет**: CRITICAL

**Требования**:
- Ошибка типа прерывает операцию до изменения данных
- Не допускается неявное приведение типов:
  - `'123'` в INT — ошибка
  - `1` в BOOL — ошибка
  - `true` в TEXT — ошибка

---

### 3.4 [P0] NFR-ATOMIC-001: Атомарность
**Приоритет**: CRITICAL

**Требования**:
- UPDATE нескольких строк: All-or-Nothing
- INSERT: при нарушении UNIQUE строка не добавляется
- При ошибке в середине операции все изменения откатываются

---

### 3.5 [P1] NFR-ORDER-001: Insertion Order
**Приоритет**: HIGH

**Требования**:
- SELECT возвращает строки в порядке вставки
- Гарантия сохраняется после SAVE/LOAD

---

## 4. Ограничения

### 4.1 Технологические ограничения
| Ограничение | Значение |
|-------------|----------|
| Python версия | 3.11+ |
| Сторонние библиотеки | Запрещены |
| Запуск REPL | `python -m mini_db` |

### 4.2 Scope-ограничения (Out of Scope)
- JOIN-операции
- Транзакции (BEGIN/COMMIT/ROLLBACK)
- Агрегатные функции (COUNT, SUM, etc.)
- ORDER BY, GROUP BY, LIMIT
- IS NULL / IS NOT NULL синтаксис
- DROP TABLE, DROP INDEX
- ALTER TABLE

---

## 5. Требования к поставке

### 5.1 Структура модулей
Код должен быть разбит на логические модули:
- `parser` — лексер и парсер
- `ast` — узлы AST
- `storage` — in-memory хранилище, таблицы, индексы
- `executor` — выполнение команд, rollback
- `repl` — интерфейс командной строки

### 5.2 Test Suite
**Обязательное покрытие**:
1. Атомарность UPDATE при нарушении UNIQUE
2. Работа индексов (ускорение SELECT с WHERE col = X)
3. Парсинг сложных WHERE со вложенными скобками
4. Строгая типизация (отказ при несовпадении типов)
5. Graceful error handling (нет traceback)

**Фреймворк**: pytest или unittest

---

## 6. Приоритизация требований

### P0 — Critical (MVP)
- CREATE TABLE, INSERT, SELECT, UPDATE, DELETE
- WHERE с AND/OR/скобками
- SAVE, LOAD, EXIT
- Строгая типизация
- Атомарность UPDATE
- Graceful errors
- Изоляция парсера

### P1 — High
- CREATE INDEX
- SELECT *
- Insertion order guarantee

### P2 — Medium (не входит в текущий scope)
- DROP TABLE, DROP INDEX
- ALTER TABLE
- ORDER BY

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

## 8. Glossary

| Термин | Определение |
|--------|-------------|
| AST | Abstract Syntax Tree — абстрактное синтаксическое дерево |
| REPL | Read-Eval-Print Loop — интерактивный интерфейс |
| DDL | Data Definition Language — CREATE TABLE |
| DML | Data Manipulation Language — INSERT, UPDATE, DELETE |
| DQL | Data Query Language — SELECT |
| All-or-Nothing | Атомарность: либо все изменения применяются, либо ни одного |
| Insertion order | Порядок вставки строк в таблицу |

---

**Документ подготовлен**: Vaib1 Analyst  
**Следующий этап**: Vaib2 Architect → development_plan.md