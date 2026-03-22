# TODO-vaib6-edit.md — Editor Patch Log (mini_db_v2)

**Session**: 2026-03-22
**Phase**: Phase 9 (WAL)

---

## Patches Applied

### DEFECT-IMPORT: TransactionError не определён в mvcc.py
- **File**: `mini_db_v2/storage/mvcc.py`
- **Problem**: `TransactionError` импортировался из `mvcc.py` в `transaction.py:22`, но класс не существовал
- **Error**: `ImportError: cannot import name 'TransactionError' from 'mini_db_v2.storage.mvcc'`
- **Fix**: Добавлен класс `TransactionError(MVCCError)` в блок ошибок (lines 49-52)
- **Change**:
  ```python
  class TransactionError(MVCCError):
      """Ошибка транзакции."""
      pass
  ```
- **Result**: ImportError исправлен ✓

### DEFECT-VIS-1: is_committed_before_snapshot не проверяет собственный XID
- **File**: `mini_db_v2/storage/mvcc.py`
- **Problem**: `is_committed_before_snapshot(1)` возвращал True вместо False когда xid=1 был текущей транзакцией
- **Test**: `test_snapshot_empty_active_xids` (line 169)
- **Fix**: Добавлена проверка `if xid == self.xid: return False` (lines 141-142)
- **Change**: Транзакция не может быть "закоммичена до snapshot" если это сама текущая транзакция
- **Result**: Тест проходит ✓

### DEFECT-VIS-2: xmax вычисляется неправильно в _create_snapshot
- **File**: `mini_db_v2/concurrency/transaction.py`
- **Problem**: `xmax = max(active_xids) + 1` неправильно определял "high water mark"
- **Test**: `test_read_committed_sees_latest_committed` (line 717)
- **Root Cause**: Когда T3 коммитит, его XID=3. При active_xids={2}, xmax=3. Версия с xmin=3 не видна из-за `xmin >= xmax`
- **Fix**: Изменено на `xmax = self._next_xid` (line 253)
- **Change**: xmax теперь указывает на следующий XID который будет выдан (правильный "high water mark")
- **Result**: Тест проходит ✓

### DEFECT-VIS-3: То же что DEFECT-VIS-2
- **File**: `mini_db_v2/concurrency/transaction.py`
- **Test**: `test_checkpoint5_non_blocking_reads` (line 1122)
- **Root Cause**: Та же проблема с xmax
- **Result**: Тест проходит ✓

### BUG-1: `_get_last_lsn()` не работает после reopen
- **File**: `mini_db_v2/storage/wal.py`
- **Problem**: После reopen `next_lsn == 2` вместо `next_lsn == 4`
- **Test**: `test_persistence_across_reopen`
- **Root Cause**: Метод читал файл неправильно через `f.read()` без цикла
- **Fix**: Использован `WALReader` вместо ручного чтения (lines 312-326)
- **Change**: `WALReader(f, self.WAL_HEADER_SIZE).get_last_lsn()`
- **Result**: Тест проходит ✓

### BUG-2: `recover()` не читает все записи
- **File**: `mini_db_v2/storage/wal.py`
- **Problem**: Возвращает только 1 запись вместо 3 для больших записей
- **Test**: `test_very_large_record`
- **Root Cause**: Чтение chunk'ами обрывалось на больших записях (4096 bytes chunk)
- **Fix**: Использован `WALReader` вместо ручного чтения (lines 540-561)
- **Change**: `WALReader(f, self.WAL_HEADER_SIZE).read_all()`
- **Result**: Тест проходит ✓

### BUG-3: Отрицательные XID не поддерживаются
- **File**: `mini_db_v2/storage/wal.py`
- **Problem**: `struct.error: int too large to convert` для XID=-1
- **Test**: `test_negative_xid`
- **Root Cause**: XID использовался как unsigned long long (`Q`), но -1 не может быть unsigned
- **Fix**: Изменён формат XID на signed long long (`q`) в `to_bytes()` и `from_bytes()`
- **Change**: `'>QQBHqI'` → `'>QqBHqI'` (lines 127, 162)
- **Result**: Тест проходит ✓

---

## Files Changed
1. `mini_db_v2/storage/mvcc.py` — 2 changes (DEFECT-IMPORT, DEFECT-VIS-1)
2. `mini_db_v2/concurrency/transaction.py` — 1 change (DEFECT-VIS-2, DEFECT-VIS-3)
3. `mini_db_v2/storage/wal.py` — 4 changes (BUG-1, BUG-2, BUG-3 x2)

---

## Verification
- [x] Syntax check passed (py_compile)
- [x] Test 1: test_persistence_across_reopen PASSED
- [x] Test 2: test_very_large_record PASSED
- [x] Test 3: test_negative_xid PASSED

---

**STATUS**: SUCCESS
**OUTPUT**: [wal.py]
**SUMMARY**: 3 WAL bugs fixed (XID format, _get_last_lsn, recover), 10 lines changed