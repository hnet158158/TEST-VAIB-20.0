#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Adversarial tests for Phase 2."""

from mini_db.storage import Database
from mini_db.executor import Executor
from mini_db.parser import Parser

def test_unique_constraint():
    """Test UNIQUE constraint enforcement."""
    db = Database()
    parser = Parser()
    executor = Executor()

    # Create table with UNIQUE
    ast = parser.parse('CREATE TABLE users (id INT UNIQUE, name TEXT)')
    result = executor.execute(ast, db)
    assert result.success, f"CREATE TABLE failed: {result.error}"

    # Insert first row
    ast = parser.parse("INSERT INTO users (id, name) VALUES (1, 'Alice')")
    result = executor.execute(ast, db)
    assert result.success, f"INSERT 1 failed: {result.error}"

    # Insert duplicate id - should fail
    ast = parser.parse("INSERT INTO users (id, name) VALUES (1, 'Bob')")
    result = executor.execute(ast, db)
    assert not result.success, "INSERT duplicate should fail"
    assert "UNIQUE constraint violated" in result.error, f"Wrong error: {result.error}"

    # Verify only one row in table
    table = db.get_table('users')
    assert len(table.rows) == 1, f"Expected 1 row, got {len(table.rows)}"
    assert table.rows[0]['id'] == 1
    assert table.rows[0]['name'] == 'Alice'

    print("test_unique_constraint: PASSED")


def test_strict_typing():
    """Test strict type checking."""
    db = Database()
    parser = Parser()
    executor = Executor()

    # Test 1: String to INT should fail
    ast = parser.parse('CREATE TABLE t1 (id INT)')
    executor.execute(ast, db)
    ast = parser.parse("INSERT INTO t1 (id) VALUES ('123')")
    result = executor.execute(ast, db)
    assert not result.success, "String to INT should fail"
    assert "Type mismatch" in result.error, f"Wrong error: {result.error}"
    print("test_strict_typing (String to INT): PASSED")

    # Test 2: Bool to INT should fail
    db2 = Database()
    ast = parser.parse('CREATE TABLE t2 (id INT)')
    executor.execute(ast, db2)
    ast = parser.parse("INSERT INTO t2 (id) VALUES (true)")
    result = executor.execute(ast, db2)
    assert not result.success, "Bool to INT should fail"
    print("test_strict_typing (Bool to INT): PASSED")

    # Test 3: Int to BOOL should fail
    db3 = Database()
    ast = parser.parse('CREATE TABLE t3 (active BOOL)')
    executor.execute(ast, db3)
    ast = parser.parse("INSERT INTO t3 (active) VALUES (1)")
    result = executor.execute(ast, db3)
    assert not result.success, "Int to BOOL should fail"
    print("test_strict_typing (Int to BOOL): PASSED")

    # Test 4: Int to TEXT should fail
    db4 = Database()
    ast = parser.parse('CREATE TABLE t4 (name TEXT)')
    executor.execute(ast, db4)
    ast = parser.parse("INSERT INTO t4 (name) VALUES (123)")
    result = executor.execute(ast, db4)
    assert not result.success, "Int to TEXT should fail"
    print("test_strict_typing (Int to TEXT): PASSED")


def test_error_format():
    """Test error format is 'Error: ...'."""
    db = Database()
    parser = Parser()
    executor = Executor()

    # Create table
    ast = parser.parse('CREATE TABLE t (id INT)')
    executor.execute(ast, db)

    # Try to insert into non-existent table
    ast = parser.parse("INSERT INTO nonexistent (id) VALUES (1)")
    result = executor.execute(ast, db)
    assert not result.success
    assert result.error is not None
    assert "does not exist" in result.error
    print("test_error_format: PASSED")


def test_multiple_unique_columns():
    """Test table with multiple UNIQUE columns."""
    db = Database()
    parser = Parser()
    executor = Executor()

    # Create table with two UNIQUE columns
    ast = parser.parse('CREATE TABLE users (id INT UNIQUE, email TEXT UNIQUE, name TEXT)')
    result = executor.execute(ast, db)
    assert result.success

    # Insert first row
    ast = parser.parse("INSERT INTO users (id, email, name) VALUES (1, 'alice@example.com', 'Alice')")
    result = executor.execute(ast, db)
    assert result.success

    # Insert with duplicate id
    ast = parser.parse("INSERT INTO users (id, email, name) VALUES (1, 'bob@example.com', 'Bob')")
    result = executor.execute(ast, db)
    assert not result.success
    assert "UNIQUE constraint violated" in result.error

    # Insert with duplicate email
    ast = parser.parse("INSERT INTO users (id, email, name) VALUES (2, 'alice@example.com', 'Bob')")
    result = executor.execute(ast, db)
    assert not result.success
    assert "UNIQUE constraint violated" in result.error

    # Insert with all unique values
    ast = parser.parse("INSERT INTO users (id, email, name) VALUES (2, 'bob@example.com', 'Bob')")
    result = executor.execute(ast, db)
    assert result.success

    print("test_multiple_unique_columns: PASSED")


def test_null_in_unique():
    """Test NULL values in UNIQUE column."""
    db = Database()
    parser = Parser()
    executor = Executor()

    # Create table with UNIQUE column
    ast = parser.parse('CREATE TABLE t (id INT UNIQUE)')
    result = executor.execute(ast, db)
    assert result.success

    # Insert NULL
    ast = parser.parse("INSERT INTO t (id) VALUES (null)")
    result = executor.execute(ast, db)
    assert result.success, f"NULL insert should succeed: {result.error}"

    # Insert another NULL - should succeed (NULLs are not tracked in unique index)
    ast = parser.parse("INSERT INTO t (id) VALUES (null)")
    result = executor.execute(ast, db)
    assert result.success, f"Second NULL insert should succeed: {result.error}"

    print("test_null_in_unique: PASSED")


if __name__ == '__main__':
    test_unique_constraint()
    test_strict_typing()
    test_error_format()
    test_multiple_unique_columns()
    test_null_in_unique()
    print("\n=== ALL ADVERSARIAL TESTS PASSED ===")