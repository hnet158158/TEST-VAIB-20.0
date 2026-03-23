# TODO-vaib6-edit.md — Editor Patch Log

**Session**: 2026-03-22
**Phase**: Phase 2 (DDL + Basic DML)

---

## Patches Applied (Previous Session)

### DEFECT-1: TokenType.AS отсутствует в Lexer
- **File**: `mini_db_v2/parser/lexer.py`
- **Fix**: Добавлен `AS = auto()` в enum TokenType (line 97)
- **Fix**: Добавлен `"AS": TokenType.AS` в KEYWORDS mapping (line 265)

### DEFECT-2: TokenType.EXISTS vs EXISTS_KW несоответствие
- **File**: `mini_db_v2/parser/lexer.py`
- **Fix**: Удалён `EXISTS_KW = auto()` из enum (line 121 removed)
- **File**: `mini_db_v2/parser/parser.py`
- **Fix**: Заменён `TokenType.EXISTS_KW` → `TokenType.EXISTS` в:
  - `parse_create_table()` (line 487)
  - `parse_create_index()` (line 563)
  - `parse_drop_table()` (line 609)
  - `parse_drop_index()` (line 622)

### DEFECT-3: Database.get_table() бросает исключение вместо None
- **File**: `mini_db_v2/storage/database.py`
- **Fix**: Метод `get_table()` возвращает `Optional[Table]` вместо бросания `TableNotFoundError`
- **Change**: `raise TableNotFoundError(...)` → `return self._tables.get(name)`

---

## Patches Applied (Current Session)

### DEFECT-4: CREATE UNIQUE INDEX парсинг - двойное чтение INDEX
- **File**: `mini_db_v2/parser/parser.py`
- **Problem**: Парсер дважды читал токен INDEX при парсинге CREATE UNIQUE INDEX
- **Fix**: Удалено дублирующее `self.expect(TokenType.INDEX)` в `parse_create()` (line 471)
- **Change**: Теперь INDEX читается только в `parse_create_index()`

### DEFECT-5: DISTINCT не работает
- **File**: `mini_db_v2/executor/executor.py`
- **Problem**: SELECT DISTINCT не удалял дубликаты (применялся до projection)
- **Fix**: Перемещён DISTINCT после projection (lines 207-214)
- **Change**: DISTINCT теперь применяется к result_rows вместо rows

### DEFECT-6: NULL arithmetic не обрабатывается
- **File**: `mini_db_v2/executor/executor.py`
- **Problem**: Арифметические операции с NULL не возвращали NULL
- **Fix**: Добавлена проверка NULL в arithmetic operations (lines 680-696)
- **Change**: `left + right` → `None if left is None or right is None else left + right`

### DEFECT-7: test_unexpected_token не выбрасывает ParseError
- **File**: `mini_db_v2/parser/parser.py`
- **Problem**: Парсер не проверял лишние токены после парсинга команды
- **Fix**: Добавлена проверка `if not self.match(TokenType.EOF)` в `parse()` (lines 129-131)
- **Change**: Теперь `SELECT * users` (без FROM) выбрасывает ParseError

### DEFECT-8: Checkpoint #4 - ускорение 1.1x вместо 10x
- **File**: `mini_db_v2/executor/executor.py`
- **Problem**: B-tree index не давал ожидаемого ускорения (full scan вместо index scan)
- **Fix**: Заменён `table.select()` на `table.select_by_row_ids(row_ids)` (line 283)
- **Change**: Теперь используется эффективный поиск по row_ids вместо full table scan

### DEFECT-9: Данные повреждены после невалидного запроса
- **File**: `mini_db_v2/storage/table.py`
- **Problem**: PRIMARY KEY не обрабатывался как UNIQUE constraint
- **Fix**: Добавлен `or col.primary_key` в условие unique tracking (line 185)
- **Change**: `if col.unique` → `if col.unique or col.primary_key`

### DEFECT-10: Checkpoint #4 - B-tree index не ускоряет range query (1.2x вместо 10x+)
- **File**: `mini_db_v2/executor/executor.py`
- **Problem**: `_try_use_index()` не обрабатывал комбинированные условия `col > X AND col < Y`
- **Root Cause**: Метод распознавал только простые условия типа `col > X`, но не AND с двумя границами диапазона
- **Fix**: Добавлена обработка AND для range queries (lines 233-310)
- **New Methods**:
  - `_get_range_bounds()` - извлекает границы из выражения
  - `_merge_low_bounds()` - объединяет нижние границы
  - `_merge_high_bounds()` - объединяет верхние границы
  - `_execute_range_scan()` - выполняет scan через индекс
- **Result**: Speedup 12.1x (target 10x+) ✓

---

## Files Changed
1. `mini_db_v2/parser/parser.py` — 3 changes (DEFECT-4, DEFECT-7)
2. `mini_db_v2/executor/executor.py` — 4 changes (DEFECT-5, DEFECT-6, DEFECT-8, DEFECT-10)
3. `mini_db_v2/storage/table.py` — 1 change (DEFECT-9)

---

## Verification
- [x] Syntax check passed (py_compile)
- [x] All 10 defects verified fixed via manual test
- [x] Checkpoint #4 PASSED: Speedup 12.1x (target 10x+)
- [ ] Full test suite to be run by Tester

---

### DEFECT-11: WALReader не читает записи через границы chunk'ов
- **File**: `mini_db_v2/storage/wal_reader.py`
- **Problem**: При `WALReadError` на неполной записи происходил `break`, терялись данные
- **Root Cause**: Метод `_iterate_records()` не сохранял остаток chunk'а для следующей итерации
- **Fix**: Добавлен буфер `buffer = b''` с переносом остатка (lines 261-311)
- **Change**: При `WALReadError` сохраняется `buffer = data[offset:]` вместо `break`
- **Result**: Записи, пересекающие границы chunk'ов, теперь корректно читаются

---

## Files Changed
1. `mini_db_v2/parser/parser.py` — 3 changes (DEFECT-4, DEFECT-7)
2. `mini_db_v2/executor/executor.py` — 4 changes (DEFECT-5, DEFECT-6, DEFECT-8, DEFECT-10)
3. `mini_db_v2/storage/table.py` — 1 change (DEFECT-9)
4. `mini_db_v2/storage/wal_reader.py` — 1 change (DEFECT-11)

---

## Verification
- [x] Syntax check passed (py_compile)
- [x] All 11 defects verified fixed via manual test
- [x] Checkpoint #4 PASSED: Speedup 12.1x (target 10x+)
- [ ] Full test suite to be run by Tester

---

### DEFECT-12: UnicodeEncodeError в WELCOME message REPL
- **File**: `mini_db_v2/repl/repl.py`
- **Problem**: Unicode символы (╔═╗║╠╣╚╝) в WELCOME message не отображаются в Windows console (cp1251)
- **Fix**: Заменены Unicode символы на ASCII аналоги (lines 42-51)
- **Change**: `╔═╗║╠╣╚╝` → `+=+|`
- **Result**: WELCOME message теперь ASCII-only, совместим с Windows console

---

## Files Changed
1. `mini_db_v2/parser/parser.py` — 3 changes (DEFECT-4, DEFECT-7)
2. `mini_db_v2/executor/executor.py` — 4 changes (DEFECT-5, DEFECT-6, DEFECT-8, DEFECT-10)
3. `mini_db_v2/storage/table.py` — 1 change (DEFECT-9)
4. `mini_db_v2/storage/wal_reader.py` — 1 change (DEFECT-11)
5. `mini_db_v2/repl/repl.py` — 1 change (DEFECT-12)

---

## Verification
- [x] Syntax check passed (py_compile)
- [x] All 12 defects verified fixed
- [x] Checkpoint #4 PASSED: Speedup 12.1x (target 10x+)
- [ ] Full test suite to be run by Tester

---

### BUG-TEST-1: test_get_table_not_found ожидает TableNotFoundError
- **File**: `mini_db_v2/tests/test_storage.py`
- **Problem**: Тест ожидал `TableNotFoundError`, но `get_table()` возвращает `None` (см. DEFECT-3)
- **Fix**: Изменён тест на проверку `is None` (lines 117-122)
- **Change**: `with pytest.raises(TableNotFoundError)` → `assert result is None`
- **Result**: Тест проходит ✓

### BUG-TEST-2: test_btree_node_is_underflow возвращает False
- **File**: `mini_db_v2/tests/test_storage.py`
- **Problem**: `is_underflow()` проверяет `if self.parent is None: return len(self.keys) == 0`. Тест не устанавливал parent, поэтому узел считался корнем.
- **Fix**: Добавлен `parent` в тест (lines 655-664)
- **Change**: `BTreeNode(is_leaf=True, order=10)` → `BTreeNode(is_leaf=True, order=10, parent=parent)`
- **Result**: Тест проходит ✓

---

## Files Changed
1. `mini_db_v2/parser/parser.py` — 3 changes (DEFECT-4, DEFECT-7)
2. `mini_db_v2/executor/executor.py` — 4 changes (DEFECT-5, DEFECT-6, DEFECT-8, DEFECT-10)
3. `mini_db_v2/storage/table.py` — 1 change (DEFECT-9)
4. `mini_db_v2/storage/wal_reader.py` — 1 change (DEFECT-11)
5. `mini_db_v2/repl/repl.py` — 1 change (DEFECT-12)
6. `mini_db_v2/tests/test_storage.py` — 2 changes (BUG-TEST-1, BUG-TEST-2)

---

## Verification
- [x] Syntax check passed (py_compile)
- [x] All 14 defects verified fixed
- [x] Checkpoint #4 PASSED: Speedup 12.1x (target 10x+)
- [x] test_get_table_not_found: PASSED
- [x] test_btree_node_is_underflow: PASSED

---

**STATUS**: SUCCESS
**OUTPUT**: [test_storage.py]
**SUMMARY**: 2 failing tests fixed (get_table returns None, is_underflow needs parent)