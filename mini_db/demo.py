#!/usr/bin/env python3
# START_MODULE_CONTRACT
# Module: mini_db.demo
# Intent: Демонстрационный скрипт для mini_db.
#         Показывает все основные операции: CREATE, INSERT, SELECT, UPDATE, DELETE, INDEX.
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports:
#   - main(): entry point для демонстрации
# END_MODULE_MAP

from __future__ import annotations

import sys
from typing import Any

sys.path.insert(0, '.')

from mini_db.storage.database import Database
from mini_db.executor.executor import Executor, ExecutionResult
from mini_db.parser.parser import Parser


# START_BLOCK_HELPERS
def print_result(result: ExecutionResult) -> None:
    """
    [START_CONTRACT_PRINT_RESULT]
    Intent: Вывести результат выполнения SQL-оператора.
    Input: result - ExecutionResult от Executor.
    Output: Печатает success/error message и данные если есть.
    [END_CONTRACT_PRINT_RESULT]
    """
    if result.success:
        print(f"  [OK] {result.message}")
        if result.data:
            print_table(result.data)
    else:
        print(f"  [ERR] Error: {result.error}")


def print_table(rows: list[dict[str, Any]]) -> None:
    """
    [START_CONTRACT_PRINT_TABLE]
    Intent: Вывести таблицу данных в форматированном виде.
    Input: rows - список словарей с данными.
    Output: Печатает таблицу с колонками и значениями.
    [END_CONTRACT_PRINT_TABLE]
    """
    if not rows:
        print("  (no data)")
        return
    
    # Get column names from first row
    columns = list(rows[0].keys())
    
    # Calculate column widths
    widths = {col: len(col) for col in columns}
    for row in rows:
        for col in columns:
            val = row.get(col)
            val_str = str(val) if val is not None else "NULL"
            widths[col] = max(widths[col], len(val_str))
    
    # Print header
    header = "  | " + " | ".join(col.ljust(widths[col]) for col in columns) + " |"
    separator = "  |-" + "-|-".join("-" * widths[col] for col in columns) + "-|"
    print(header)
    print(separator)
    
    # Print rows
    for row in rows:
        values = []
        for col in columns:
            val = row.get(col)
            val_str = str(val) if val is not None else "NULL"
            values.append(val_str.ljust(widths[col]))
        print("  | " + " | ".join(values) + " |")


def execute_sql(
    executor: Executor,
    parser: Parser,
    db: Database,
    sql: str
) -> ExecutionResult:
    """
    [START_CONTRACT_EXECUTE_SQL]
    Intent: Выполнить SQL-запрос и вернуть результат.
    Input: executor, parser, db - компоненты mini_db; sql - строка запроса.
    Output: ExecutionResult с результатом выполнения.
    [END_CONTRACT_EXECUTE_SQL]
    """
    ast = parser.parse(sql)
    return executor.execute(ast, db)
# END_BLOCK_HELPERS


# START_BLOCK_DEMO
def main() -> None:
    """
    [START_CONTRACT_MAIN]
    Intent: Запустить демонстрацию всех операций mini_db.
    Output: Печатает результаты каждого шага демо.
    [END_CONTRACT_MAIN]
    """
    print("=" * 60)
    print("MINI_DB DEMONSTRATION")
    print("=" * 60)
    
    # Initialize components
    db = Database()
    executor = Executor()
    parser = Parser()
    
    # ========== 1. CREATE TABLE ==========
    print("\n=== 1. Creating tables ===")
    
    result = execute_sql(executor, parser, db,
        "CREATE TABLE users (id INT UNIQUE, name TEXT, age INT, active BOOL)")
    print_result(result)
    
    result = execute_sql(executor, parser, db,
        "CREATE TABLE products (id INT UNIQUE, name TEXT, price INT)")
    print_result(result)
    
    # ========== 2. INSERT ==========
    print("\n=== 2. Inserting data ===")
    
    result = execute_sql(executor, parser, db,
        "INSERT INTO users (id, name, age, active) VALUES (1, 'Alice', 30, true)")
    print_result(result)
    
    result = execute_sql(executor, parser, db,
        "INSERT INTO users (id, name, age, active) VALUES (2, 'Bob', 25, false)")
    print_result(result)
    
    result = execute_sql(executor, parser, db,
        "INSERT INTO users (id, name, age, active) VALUES (3, 'Charlie', 35, true)")
    print_result(result)
    
    result = execute_sql(executor, parser, db,
        "INSERT INTO products (id, name, price) VALUES (1, 'Laptop', 1000)")
    print_result(result)
    
    result = execute_sql(executor, parser, db,
        "INSERT INTO products (id, name, price) VALUES (2, 'Mouse', 50)")
    print_result(result)
    
    result = execute_sql(executor, parser, db,
        "INSERT INTO products (id, name, price) VALUES (3, 'Keyboard', 150)")
    print_result(result)
    
    # ========== 3. SELECT ==========
    print("\n=== 3. Selecting data ===")
    
    print("\n--- SELECT * FROM users ---")
    result = execute_sql(executor, parser, db, "SELECT * FROM users")
    print_result(result)
    
    print("\n--- SELECT name, age FROM users WHERE active = true ---")
    result = execute_sql(executor, parser, db,
        "SELECT name, age FROM users WHERE active = true")
    print_result(result)
    
    print("\n--- SELECT * FROM products WHERE price > 100 ---")
    result = execute_sql(executor, parser, db,
        "SELECT * FROM products WHERE price > 100")
    print_result(result)
    
    print("\n--- Complex WHERE: (age > 25 OR active = true) AND id != 3 ---")
    result = execute_sql(executor, parser, db,
        "SELECT * FROM users WHERE (age > 25 OR active = true) AND id != 3")
    print_result(result)
    
    # ========== 4. UPDATE ==========
    print("\n=== 4. Updating data ===")
    
    print("\n--- UPDATE users SET age = 31 WHERE name = 'Alice' ---")
    result = execute_sql(executor, parser, db,
        "UPDATE users SET age = 31 WHERE name = 'Alice'")
    print_result(result)
    
    print("\n--- After update ---")
    result = execute_sql(executor, parser, db, "SELECT * FROM users WHERE name = 'Alice'")
    print_result(result)
    
    print("\n--- UPDATE products SET price = 75 WHERE id = 2 ---")
    result = execute_sql(executor, parser, db,
        "UPDATE products SET price = 75 WHERE id = 2")
    print_result(result)
    
    # ========== 5. DELETE ==========
    print("\n=== 5. Deleting data ===")
    
    print("\n--- DELETE FROM users WHERE active = false ---")
    result = execute_sql(executor, parser, db,
        "DELETE FROM users WHERE active = false")
    print_result(result)
    
    print("\n--- After delete ---")
    result = execute_sql(executor, parser, db, "SELECT * FROM users")
    print_result(result)
    
    # ========== 6. INDEXES ==========
    print("\n=== 6. Creating and using indexes ===")
    
    print("\n--- CREATE INDEX idx_users_name ON users (name) ---")
    result = execute_sql(executor, parser, db,
        "CREATE INDEX idx_users_name ON users (name)")
    print_result(result)
    
    print("\n--- CREATE INDEX idx_products_price ON products (price) ---")
    result = execute_sql(executor, parser, db,
        "CREATE INDEX idx_products_price ON products (price)")
    print_result(result)
    
    print("\n--- SELECT using index: WHERE name = 'Alice' ---")
    result = execute_sql(executor, parser, db,
        "SELECT * FROM users WHERE name = 'Alice'")
    print_result(result)
    
    # ========== 7. UNIQUE CONSTRAINT ==========
    print("\n=== 7. UNIQUE constraint demonstration ===")
    
    print("\n--- Attempting to insert duplicate id ---")
    result = execute_sql(executor, parser, db,
        "INSERT INTO users (id, name, age, active) VALUES (1, 'Duplicate', 99, true)")
    print_result(result)
    
    # ========== 8. SAVE/LOAD ==========
    print("\n=== 8. Save and Load database ===")
    
    print("\n--- SAVE 'demo_db.json' ---")
    result = execute_sql(executor, parser, db, "SAVE 'demo_db.json'")
    print_result(result)
    
    # Create new database and load
    db2 = Database()
    executor2 = Executor()
    
    print("\n--- LOAD 'demo_db.json' into new database ---")
    result = execute_sql(executor2, parser, db2, "LOAD 'demo_db.json'")
    print_result(result)
    
    print("\n--- Verify loaded data ---")
    result = execute_sql(executor2, parser, db2, "SELECT * FROM users")
    print_result(result)
    
    # ========== SUMMARY ==========
    print("\n" + "=" * 60)
    print("DEMONSTRATION COMPLETE")
    print("=" * 60)
    print("\nAll mini_db features demonstrated:")
    print("  [+] CREATE TABLE with types and UNIQUE")
    print("  [+] INSERT with type validation")
    print("  [+] SELECT with WHERE conditions")
    print("  [+] UPDATE with atomic rollback")
    print("  [+] DELETE with conditions")
    print("  [+] CREATE INDEX and index usage")
    print("  [+] UNIQUE constraint enforcement")
    print("  [+] SAVE/LOAD to JSON file")


if __name__ == '__main__':
    main()
# END_BLOCK_DEMO