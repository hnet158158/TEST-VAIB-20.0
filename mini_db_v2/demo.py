#!/usr/bin/env python3
# START_MODULE_CONTRACT
# Module: mini_db_v2.demo
# Intent: Демонстрационный скрипт для mini_db_v2 - показывает все возможности СУБД.
# Dependencies: mini_db_v2.storage, mini_db_v2.executor, mini_db_v2.parser
# END_MODULE_CONTRACT

"""
Demo script for mini_db_v2
Demonstrates: CRUD operations, aggregation, joins, subqueries, transactions

Usage:
    python -m mini_db_v2.demo
"""

from __future__ import annotations
import sys
import os

# Ensure parent directory is in path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mini_db_v2.storage.database import Database
from mini_db_v2.executor.executor import Executor
from mini_db_v2.parser.parser import Parser


def print_separator(title: str = "") -> None:
    """Выводит разделитель с опциональным заголовком."""
    if title:
        print(f"\n{'='*60}")
        print(f" {title}")
        print('='*60)
    else:
        print('-'*60)


def print_result(result, show_columns: bool = True) -> None:
    """
    [START_CONTRACT_PRINT_RESULT]
    Intent: Красивый вывод результата SQL запроса.
    Input: result - ExecutionResult; show_columns - показывать ли заголовки колонок.
    Output: Форматированный вывод в консоль.
    [END_CONTRACT_PRINT_RESULT]
    """
    if not result.success:
        print(f"ERROR: {result.message}")
        return
    
    if result.message and not result.rows:
        print(f"OK: {result.message}")
        return
    
    if result.rows:
        columns = result.columns or list(result.rows[0].keys())
        widths = {col: len(col) for col in columns}
        
        for row in result.rows:
            for col in columns:
                val = row.get(col)
                val_str = str(val) if val is not None else "NULL"
                widths[col] = max(widths[col], len(val_str))
        
        if show_columns:
            header = " | ".join(col.ljust(widths[col]) for col in columns)
            print(header)
            print("-+-".join("-" * widths[col] for col in columns))
        
        for row in result.rows:
            values = []
            for col in columns:
                val = row.get(col)
                val_str = str(val) if val is not None else "NULL"
                values.append(val_str.ljust(widths[col]))
            print(" | ".join(values))
        
        print(f"\n({len(result.rows)} row(s))")


def execute_sql(executor: Executor, sql: str):
    """
    [START_CONTRACT_EXECUTE_SQL]
    Intent: Парсит и выполняет SQL запрос.
    Input: executor - исполнитель; sql - SQL строка.
    Output: ExecutionResult с результатом.
    [END_CONTRACT_EXECUTE_SQL]
    """
    parser = Parser(sql=sql)
    ast = parser.parse()
    return executor.execute(ast)


def demo_create_tables(db: Database, executor: Executor) -> None:
    """
    [START_CONTRACT_DEMO_CREATE_TABLES]
    Intent: Демонстрация CREATE TABLE.
    Input: db, executor - компоненты БД.
    Output: Созданы таблицы users, orders, products.
    [END_CONTRACT_DEMO_CREATE_TABLES]
    """
    print_separator("1. CREATE TABLE - Creating tables")
    
    # Create users table
    sql = """
    CREATE TABLE users (
        id INT PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        age INT,
        salary REAL
    )
    """
    result = execute_sql(executor, sql)
    print(f"SQL: {sql.strip()}")
    print_result(result)
    
    # Create products table
    sql = """
    CREATE TABLE products (
        id INT PRIMARY KEY,
        name TEXT NOT NULL,
        price REAL,
        category TEXT
    )
    """
    result = execute_sql(executor, sql)
    print(f"\nSQL: {sql.strip()}")
    print_result(result)
    
    # Create orders table
    sql = """
    CREATE TABLE orders (
        id INT PRIMARY KEY,
        user_id INT,
        product_id INT,
        quantity INT,
        order_date TEXT
    )
    """
    result = execute_sql(executor, sql)
    print(f"\nSQL: {sql.strip()}")
    print_result(result)


def demo_insert(db: Database, executor: Executor) -> None:
    """
    [START_CONTRACT_DEMO_INSERT]
    Intent: Демонстрация INSERT.
    Input: db, executor - компоненты БД.
    Output: Вставлены данные в таблицы users, products, orders.
    [END_CONTRACT_DEMO_INSERT]
    """
    print_separator("2. INSERT - Inserting records")
    
    # Insert users
    users_data = [
        (1, 'Alice', 'alice@example.com', 30, 75000.0),
        (2, 'Bob', 'bob@example.com', 25, 50000.0),
        (3, 'Charlie', 'charlie@example.com', 35, 90000.0),
        (4, 'Diana', 'diana@example.com', 28, 65000.0),
        (5, 'Eve', 'eve@example.com', 32, 80000.0),
    ]
    
    print("Inserting users:")
    for user in users_data:
        sql = f"INSERT INTO users (id, name, email, age, salary) VALUES ({user[0]}, '{user[1]}', '{user[2]}', {user[3]}, {user[4]})"
        result = execute_sql(executor, sql)
        print(f"  + {user[1]} (age={user[3]}, salary={user[4]})")
    
    # Insert products
    products_data = [
        (1, 'Laptop', 999.99, 'Electronics'),
        (2, 'Mouse', 29.99, 'Electronics'),
        (3, 'Keyboard', 79.99, 'Electronics'),
        (4, 'Book', 19.99, 'Books'),
        (5, 'Headphones', 149.99, 'Electronics'),
    ]
    
    print("\nInserting products:")
    for product in products_data:
        sql = f"INSERT INTO products (id, name, price, category) VALUES ({product[0]}, '{product[1]}', {product[2]}, '{product[3]}')"
        result = execute_sql(executor, sql)
        print(f"  + {product[1]} (${product[2]})")
    
    # Insert orders
    orders_data = [
        (1, 1, 1, 1, '2024-01-15'),
        (2, 1, 2, 2, '2024-01-15'),
        (3, 2, 3, 1, '2024-01-16'),
        (4, 3, 1, 1, '2024-01-17'),
        (5, 3, 5, 1, '2024-01-17'),
        (6, 4, 4, 3, '2024-01-18'),
        (7, 5, 2, 1, '2024-01-19'),
    ]
    
    print("\nInserting orders:")
    for order in orders_data:
        sql = f"INSERT INTO orders (id, user_id, product_id, quantity, order_date) VALUES ({order[0]}, {order[1]}, {order[2]}, {order[3]}, '{order[4]}')"
        result = execute_sql(executor, sql)
        print(f"  + Order #{order[0]}: user_id={order[1]}, product_id={order[2]}")


def demo_select(db: Database, executor: Executor) -> None:
    """
    [START_CONTRACT_DEMO_SELECT]
    Intent: Демонстрация SELECT с различными условиями.
    Input: db, executor - компоненты БД.
    Output: Выведены результаты SELECT запросов.
    [END_CONTRACT_DEMO_SELECT]
    """
    print_separator("3. SELECT - Reading records")
    
    # SELECT *
    print("SELECT * FROM users:")
    sql = "SELECT * FROM users"
    result = execute_sql(executor, sql)
    print_result(result)
    
    # SELECT with WHERE
    print("\nSELECT with WHERE (age > 28):")
    sql = "SELECT name, age, salary FROM users WHERE age > 28"
    result = execute_sql(executor, sql)
    print_result(result)
    
    # SELECT with ORDER BY
    print("\nSELECT with ORDER BY (salary DESC):")
    sql = "SELECT name, salary FROM users ORDER BY salary DESC"
    result = execute_sql(executor, sql)
    print_result(result)
    
    # SELECT with LIMIT
    print("\nSELECT with LIMIT 3:")
    sql = "SELECT name, age FROM users LIMIT 3"
    result = execute_sql(executor, sql)
    print_result(result)


def demo_update(db: Database, executor: Executor) -> None:
    """
    [START_CONTRACT_DEMO_UPDATE]
    Intent: Демонстрация UPDATE.
    Input: db, executor - компоненты БД.
    Output: Обновлены записи в таблице users.
    [END_CONTRACT_DEMO_UPDATE]
    """
    print_separator("4. UPDATE - Updating records")
    
    # Show before
    print("Before update:")
    sql = "SELECT name, salary FROM users WHERE name = 'Alice'"
    result = execute_sql(executor, sql)
    print_result(result)
    
    # Update
    print("\nUPDATE users SET salary = 80000 WHERE name = 'Alice':")
    sql = "UPDATE users SET salary = 80000 WHERE name = 'Alice'"
    result = execute_sql(executor, sql)
    print_result(result)
    
    # Show after
    print("\nAfter update:")
    sql = "SELECT name, salary FROM users WHERE name = 'Alice'"
    result = execute_sql(executor, sql)
    print_result(result)
    
    # Update multiple
    print("\nUPDATE multiple records (salary = salary * 1.1 WHERE age < 30):")
    sql = "UPDATE users SET salary = salary * 1.1 WHERE age < 30"
    result = execute_sql(executor, sql)
    print_result(result)
    
    sql = "SELECT name, age, salary FROM users"
    result = execute_sql(executor, sql)
    print_result(result)


def demo_delete(db: Database, executor: Executor) -> None:
    """
    [START_CONTRACT_DEMO_DELETE]
    Intent: Демонстрация DELETE.
    Input: db, executor - компоненты БД.
    Output: Удалены записи из таблицы users.
    [END_CONTRACT_DEMO_DELETE]
    """
    print_separator("5. DELETE - Deleting records")
    
    # Show before
    print("Before delete (all users):")
    sql = "SELECT name FROM users"
    result = execute_sql(executor, sql)
    print_result(result)
    
    # Delete
    print("\nDELETE FROM users WHERE name = 'Eve':")
    sql = "DELETE FROM users WHERE name = 'Eve'"
    result = execute_sql(executor, sql)
    print_result(result)
    
    # Show after
    print("\nAfter delete:")
    sql = "SELECT name FROM users"
    result = execute_sql(executor, sql)
    print_result(result)
    
    # Re-insert for further demos
    sql = "INSERT INTO users (id, name, email, age, salary) VALUES (5, 'Eve', 'eve@example.com', 32, 80000.0)"
    execute_sql(executor, sql)
    print("\n(Re-inserted Eve for further demos)")


def demo_aggregation(db: Database, executor: Executor) -> None:
    """
    [START_CONTRACT_DEMO_AGGREGATION]
    Intent: Демонстрация агрегатных функций.
    Input: db, executor - компоненты БД.
    Output: Выведены результаты COUNT, SUM, AVG, MIN, MAX, GROUP BY.
    [END_CONTRACT_DEMO_AGGREGATION]
    """
    print_separator("6. AGGREGATION - Aggregate functions")
    
    # COUNT
    print("COUNT - number of users:")
    sql = "SELECT COUNT(*) AS total_users FROM users"
    result = execute_sql(executor, sql)
    print_result(result)
    
    # SUM
    print("\nSUM - total salary:")
    sql = "SELECT SUM(salary) AS total_salary FROM users"
    result = execute_sql(executor, sql)
    print_result(result)
    
    # AVG
    print("\nAVG - average salary:")
    sql = "SELECT AVG(salary) AS avg_salary FROM users"
    result = execute_sql(executor, sql)
    print_result(result)
    
    # MIN/MAX
    print("\nMIN/MAX - min and max salary:")
    sql = "SELECT MIN(salary) AS min_salary, MAX(salary) AS max_salary FROM users"
    result = execute_sql(executor, sql)
    print_result(result)
    
    # GROUP BY
    print("\nGROUP BY - product count by category:")
    sql = "SELECT category, COUNT(*) AS total FROM products GROUP BY category"
    result = execute_sql(executor, sql)
    print_result(result)
    
    # GROUP BY with HAVING
    print("\nGROUP BY with HAVING - categories with more than 1 product:")
    sql = "SELECT category, COUNT(*) AS total FROM products GROUP BY category HAVING COUNT(*) > 1"
    result = execute_sql(executor, sql)
    print_result(result)


def demo_joins(db: Database, executor: Executor) -> None:
    """
    [START_CONTRACT_DEMO_JOINS]
    Intent: Демонстрация JOIN операций.
    Input: db, executor - компоненты БД.
    Output: Выведены результаты INNER, LEFT JOIN.
    [END_CONTRACT_DEMO_JOINS]
    """
    print_separator("7. JOIN - Joining tables")
    
    # INNER JOIN
    print("INNER JOIN - orders with user and product info:")
    sql = """
    SELECT 
        o.id AS order_id,
        u.name AS user_name,
        p.name AS product_name,
        o.quantity
    FROM orders o
    INNER JOIN users u ON o.user_id = u.id
    INNER JOIN products p ON o.product_id = p.id
    """
    result = execute_sql(executor, sql)
    print_result(result)
    
    # LEFT JOIN
    print("\nLEFT JOIN - all users with their orders (if any):")
    sql = """
    SELECT 
        u.name AS user_name,
        o.id AS order_id
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    ORDER BY u.name
    """
    result = execute_sql(executor, sql)
    print_result(result)
    
    # Aggregation with JOIN
    print("\nJOIN with aggregation - total spent by user:")
    sql = """
    SELECT 
        u.name AS user_name,
        SUM(o.quantity * p.price) AS total_spent
    FROM orders o
    INNER JOIN users u ON o.user_id = u.id
    INNER JOIN products p ON o.product_id = p.id
    GROUP BY u.name
    ORDER BY total_spent DESC
    """
    result = execute_sql(executor, sql)
    print_result(result)


def demo_subqueries(db: Database, executor: Executor) -> None:
    """
    [START_CONTRACT_DEMO_SUBQUERIES]
    Intent: Демонстрация подзапросов.
    Input: db, executor - компоненты БД.
    Output: Выведены результаты scalar subquery, IN, EXISTS.
    [END_CONTRACT_DEMO_SUBQUERIES]
    """
    print_separator("8. SUBQUERY - Subqueries")
    
    # Scalar subquery
    print("Scalar subquery - users with above-average salary:")
    sql = """
    SELECT name, salary 
    FROM users 
    WHERE salary > (SELECT AVG(salary) FROM users)
    """
    result = execute_sql(executor, sql)
    print_result(result)
    
    # IN subquery
    print("\nIN subquery - users who placed orders:")
    sql = """
    SELECT name 
    FROM users 
    WHERE id IN (SELECT DISTINCT user_id FROM orders)
    """
    result = execute_sql(executor, sql)
    print_result(result)
    
    # NOT IN subquery - users without orders (using NOT EXISTS)
    print("\nNOT EXISTS - users without orders:")
    sql = """
    SELECT name
    FROM users u
    WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
    """
    result = execute_sql(executor, sql)
    print_result(result)
    
    # EXISTS
    print("\nEXISTS - products that have been ordered:")
    sql = """
    SELECT name, price
    FROM products p
    WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id)
    """
    result = execute_sql(executor, sql)
    print_result(result)


def demo_transactions(db: Database, executor: Executor) -> None:
    """
    [START_CONTRACT_DEMO_TRANSACTIONS]
    Intent: Демонстрация транзакций (BEGIN, COMMIT, ROLLBACK).
    Input: db, executor - компоненты БД.
    Output: Показана работа транзакций с коммитом и откатом.
    [END_CONTRACT_DEMO_TRANSACTIONS]
    """
    print_separator("9. TRANSACTIONS - Transactions")
    
    # Show initial state
    print("Initial state:")
    sql = "SELECT name, salary FROM users WHERE name = 'Bob'"
    result = execute_sql(executor, sql)
    print_result(result)
    
    # Transaction with COMMIT
    print("\n--- Transaction with COMMIT ---")
    print("BEGIN:")
    sql = "BEGIN"
    result = execute_sql(executor, sql)
    print_result(result)
    
    print("\nUPDATE in transaction:")
    sql = "UPDATE users SET salary = 55000 WHERE name = 'Bob'"
    result = execute_sql(executor, sql)
    print_result(result)
    
    print("\nCOMMIT:")
    sql = "COMMIT"
    result = execute_sql(executor, sql)
    print_result(result)
    
    print("\nAfter COMMIT:")
    sql = "SELECT name, salary FROM users WHERE name = 'Bob'"
    result = execute_sql(executor, sql)
    print_result(result)
    
    # Transaction with ROLLBACK
    print("\n--- Transaction with ROLLBACK ---")
    print("BEGIN:")
    sql = "BEGIN"
    result = execute_sql(executor, sql)
    print_result(result)
    
    print("\nUPDATE in transaction:")
    sql = "UPDATE users SET salary = 99999 WHERE name = 'Bob'"
    result = execute_sql(executor, sql)
    print_result(result)
    
    print("\nChange inside transaction (before ROLLBACK):")
    sql = "SELECT name, salary FROM users WHERE name = 'Bob'"
    result = execute_sql(executor, sql)
    print_result(result)
    
    print("\nROLLBACK:")
    sql = "ROLLBACK"
    result = execute_sql(executor, sql)
    print_result(result)
    
    print("\nAfter ROLLBACK (changes reverted):")
    sql = "SELECT name, salary FROM users WHERE name = 'Bob'"
    result = execute_sql(executor, sql)
    print_result(result)


def demo_advanced_features(db: Database, executor: Executor) -> None:
    """
    [START_CONTRACT_DEMO_ADVANCED]
    Intent: Демонстрация дополнительных возможностей (CASE, CAST, COALESCE).
    Input: db, executor - компоненты БД.
    Output: Выведены результаты продвинутых SQL конструкций.
    [END_CONTRACT_DEMO_ADVANCED]
    """
    print_separator("10. ADVANCED - Advanced features")
    
    # CASE expression
    print("CASE expression - salary categorization:")
    sql = """
    SELECT 
        name, 
        salary,
        CASE 
            WHEN salary >= 80000 THEN 'High'
            WHEN salary >= 60000 THEN 'Medium'
            ELSE 'Low'
        END AS salary_level
    FROM users
    ORDER BY salary DESC
    """
    result = execute_sql(executor, sql)
    print_result(result)
    
    # CAST function
    print("\nCAST - type conversion:")
    sql = "SELECT name, CAST(salary AS TEXT) AS salary_text FROM users LIMIT 3"
    result = execute_sql(executor, sql)
    print_result(result)
    
    # COALESCE
    print("\nCOALESCE - NULL handling:")
    sql = """
    SELECT 
        name, 
        COALESCE(email, 'no-email@example.com') AS email
    FROM users
    LIMIT 3
    """
    result = execute_sql(executor, sql)
    print_result(result)
    
    # DISTINCT
    print("\nDISTINCT - unique product categories:")
    sql = "SELECT DISTINCT category FROM products"
    result = execute_sql(executor, sql)
    print_result(result)
    
    # BETWEEN
    print("\nBETWEEN - products with price between 50 and 200:")
    sql = "SELECT name, price FROM products WHERE price BETWEEN 50 AND 200"
    result = execute_sql(executor, sql)
    print_result(result)


def main():
    """
    [START_CONTRACT_MAIN]
    Intent: Главная функция демонстрационного скрипта.
    Input: Нет.
    Output: Запущены все демо-секции.
    [END_CONTRACT_MAIN]
    """
    print("\n" + "="*60)
    print(" MINI_DB_V2 DEMONSTRATION")
    print(" Production-Grade SQL Database in Pure Python")
    print("="*60)
    
    # Create in-memory database
    db = Database("demo_db")
    executor = Executor(db)
    
    # Run demonstrations
    demo_create_tables(db, executor)
    demo_insert(db, executor)
    demo_select(db, executor)
    demo_update(db, executor)
    demo_delete(db, executor)
    demo_aggregation(db, executor)
    demo_joins(db, executor)
    demo_subqueries(db, executor)
    demo_transactions(db, executor)
    demo_advanced_features(db, executor)
    
    print_separator("DEMO COMPLETE")
    print("All demonstrations completed successfully!")
    print("\nmini_db_v2 features:")
    print("  + CREATE TABLE, INSERT, SELECT, UPDATE, DELETE")
    print("  + B-Tree indexes for range queries")
    print("  + INNER, LEFT, RIGHT, FULL, CROSS JOIN")
    print("  + COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING")
    print("  + Scalar subqueries, IN, NOT IN, EXISTS")
    print("  + MVCC transactions with BEGIN, COMMIT, ROLLBACK")
    print("  + CASE, CAST, COALESCE, DISTINCT, BETWEEN")
    print("  + Cost-based query optimizer")
    print("  + WAL and ARIES recovery")
    print_separator()


if __name__ == '__main__':
    main()