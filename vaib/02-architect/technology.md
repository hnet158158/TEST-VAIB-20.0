# Technology Stack: mini_db

**Проект**: mini_db — VAIB Stress-Test Benchmark  
**Версия**: 1.0  
**Дата**: 2026-03-16

---

## 1. Technology Overview

### 1.1 Core Stack

| Компонент | Технология | Версия | Обоснование |
|-----------|------------|--------|-------------|
| Language | Python | 3.11+ | Требование ТЗ, type hints, match statement |
| Runtime | CPython | 3.11+ | Стандартный интерпретатор |
| Test Framework | unittest | built-in | Без сторонних библиотек |
| Data Format | JSON | built-in | SAVE/LOAD функциональность |

### 1.2 Architecture Pattern

| Паттерн | Применение |
|---------|------------|
| Interpreter | SQL query execution |
| Composite | AST node hierarchy |
| Strategy | Expression evaluation |
| Snapshot | Rollback for atomic UPDATE |

---

## 2. Language Features (Python 3.11+)

### 2.1 Используемые возможности

| Feature | Применение | Пример |
|---------|------------|--------|
| `dataclasses` | AST nodes, Token | `@dataclass class Token: ...` |
| `enum.Enum` | TokenType, DataType | `class TokenType(Enum): ...` |
| `match` statement | Parser dispatch | `match token.type: case TokenType.SELECT: ...` |
| Type hints | Все сигнатуры | `def parse(query: str) -> ASTNode: ...` |
| `__future__.annotations` | Forward references | `from __future__ import annotations` |
| `contextlib` | Error handling | `@contextmanager def snapshot(): ...` |

### 2.2 Структуры данных

| Структура | Применение | Обоснование |
|-----------|------------|-------------|
| `list[dict]` | Table rows | Insertion order, простота |
| `dict[str, ColumnDef]` | Table schema | Быстрый lookup по имени |
| `dict[str, set]` | UNIQUE indexes | O(1) проверка уникальности |
| `dict[str, list[int]]` | Hash indexes | O(1) lookup для `=` |

---

## 3. Module Specifications

### 3.1 parser.lexer

**Responsibility**: Токенизация SQL-запросов

**Implementation**:
```python
# TokenType enum
class TokenType(Enum):
    # Keywords
    SELECT = auto()
    FROM = auto()
    WHERE = auto()
    INSERT = auto()
    INTO = auto()
    VALUES = auto()
    UPDATE = auto()
    SET = auto()
    DELETE = auto()
    CREATE = auto()
    TABLE = auto()
    INDEX = auto()
    UNIQUE = auto()
    SAVE = auto()
    LOAD = auto()
    EXIT = auto()
    # Types
    INT = auto()
    TEXT = auto()
    BOOL = auto()
    # Literals
    IDENTIFIER = auto()
    STRING = auto()
    NUMBER = auto()
    TRUE = auto()
    FALSE = auto()
    NULL = auto()
    # Operators
    EQ = auto()        # =
    NEQ = auto()       # !=
    LT = auto()        # <
    GT = auto()        # >
    AND = auto()
    OR = auto()
    # Punctuation
    LPAREN = auto()    # (
    RPAREN = auto()    # )
    COMMA = auto()     # ,
    SEMICOLON = auto() # ;
    STAR = auto()      # *
    # Special
    EOF = auto()
```

**Algorithm**: Character-by-character scanning с lookahead

**Error Handling**: `LexerError` с позицией символа

---

### 3.2 parser.parser

**Responsibility**: Recursive descent parsing

**Implementation**:
```python
class Parser:
    def __init__(self, tokens: list[Token]):
        self.tokens = tokens
        self.pos = 0
    
    def parse(self) -> StatementNode:
        return self._parse_statement()
    
    def _parse_statement(self) -> StatementNode:
        match self._current().type:
            case TokenType.SELECT:
                return self._parse_select()
            case TokenType.INSERT:
                return self._parse_insert()
            case TokenType.UPDATE:
                return self._parse_update()
            case TokenType.DELETE:
                return self._parse_delete()
            case TokenType.CREATE:
                return self._parse_create()
            case TokenType.SAVE:
                return self._parse_save()
            case TokenType.LOAD:
                return self._parse_load()
            case TokenType.EXIT:
                return self._parse_exit()
            case _:
                raise ParseError(f"Unexpected token: {self._current()}")
    
    def _parse_expression(self) -> ExpressionNode:
        """Parsing WHERE expressions with precedence:
        OR (lowest) < AND < comparison (highest)
        """
        return self._parse_or_expression()
    
    def _parse_or_expression(self) -> ExpressionNode:
        left = self._parse_and_expression()
        while self._match(TokenType.OR):
            right = self._parse_and_expression()
            left = LogicalNode(left, "OR", right)
        return left
    
    def _parse_and_expression(self) -> ExpressionNode:
        left = self._parse_comparison()
        while self._match(TokenType.AND):
            right = self._parse_comparison()
            left = LogicalNode(left, "AND", right)
        return left
    
    def _parse_comparison(self) -> ExpressionNode:
        left = self._parse_primary()
        if self._match(TokenType.EQ, TokenType.NEQ, TokenType.LT, TokenType.GT):
            op = self._previous().value
            right = self._parse_primary()
            return ComparisonNode(left, op, right)
        return left
    
    def _parse_primary(self) -> ExpressionNode:
        if self._match(TokenType.LPAREN):
            expr = self._parse_expression()
            self._expect(TokenType.RPAREN)
            return expr
        if self._match(TokenType.IDENTIFIER):
            return IdentifierNode(self._previous().value)
        if self._match(TokenType.STRING, TokenType.NUMBER, TokenType.TRUE, TokenType.FALSE, TokenType.NULL):
            return LiteralNode(self._parse_literal_value())
        raise ParseError(f"Expected expression, got {self._current()}")
```

**Error Handling**: `ParseError` с ожидаемым/найденным токеном

---

### 3.3 ast.nodes

**Responsibility**: AST node definitions

**Implementation**:
```python
from dataclasses import dataclass
from typing import Any, Optional
from __future__ import annotations

@dataclass
class ColumnDef:
    name: str
    data_type: str  # "INT", "TEXT", "BOOL"
    unique: bool = False

@dataclass
class ASTNode:
    """Base class for all AST nodes"""
    pass

@dataclass
class StatementNode(ASTNode):
    """Base class for statements"""
    pass

@dataclass
class CreateTableNode(StatementNode):
    name: str
    columns: list[ColumnDef]

@dataclass
class InsertNode(StatementNode):
    table: str
    columns: list[str]
    values: list[Any]

@dataclass
class UpdateNode(StatementNode):
    table: str
    assignments: dict[str, Any]
    where: Optional[ExpressionNode] = None

@dataclass
class DeleteNode(StatementNode):
    table: str
    where: Optional[ExpressionNode] = None

@dataclass
class SelectNode(StatementNode):
    table: str
    columns: Optional[list[str]] = None  # None = SELECT *
    where: Optional[ExpressionNode] = None

@dataclass
class CreateIndexNode(StatementNode):
    name: str
    table: str
    column: str

@dataclass
class SaveNode(StatementNode):
    filepath: str

@dataclass
class LoadNode(StatementNode):
    filepath: str

@dataclass
class ExitNode(StatementNode):
    pass

@dataclass
class ExpressionNode(ASTNode):
    """Base class for expressions"""
    pass

@dataclass
class ComparisonNode(ExpressionNode):
    left: ExpressionNode
    op: str  # "=", "!=", "<", ">"
    right: ExpressionNode

@dataclass
class LogicalNode(ExpressionNode):
    left: ExpressionNode
    op: str  # "AND", "OR"
    right: ExpressionNode

@dataclass
class IdentifierNode(ExpressionNode):
    name: str

@dataclass
class LiteralNode(ExpressionNode):
    value: Any  # int, str, bool, None
```

---

### 3.4 storage.table

**Responsibility**: In-memory table with rows

**Implementation**:
```python
class Table:
    def __init__(self, name: str, columns: list[ColumnDef]):
        self.name = name
        self.columns = {col.name: col for col in columns}
        self.column_order = [col.name for col in columns]
        self.rows: list[dict] = []
        self.unique_indexes: dict[str, set] = {}  # column -> set of values
        self.indexes: dict[str, dict[Any, set[int]]] = {}  # column -> value -> row indices
        
        # Initialize unique tracking
        for col in columns:
            if col.unique:
                self.unique_indexes[col.name] = set()
    
    def insert(self, row: dict) -> InsertResult:
        # Type validation
        for col_name, value in row.items():
            if value is not None:
                col_def = self.columns[col_name]
                if not self._validate_type(value, col_def.data_type):
                    return InsertResult(success=False, error=f"Type mismatch: expected {col_def.data_type}, got {type(value).__name__.upper()}")
        
        # UNIQUE validation
        for col_name, value in row.items():
            if col_name in self.unique_indexes:
                if value in self.unique_indexes[col_name]:
                    return InsertResult(success=False, error=f"UNIQUE constraint violated on column '{col_name}'")
        
        # Insert
        self.rows.append(row)
        row_idx = len(self.rows) - 1
        
        # Update indexes
        for col_name, value in row.items():
            if col_name in self.unique_indexes:
                self.unique_indexes[col_name].add(value)
            if col_name in self.indexes:
                if value not in self.indexes[col_name]:
                    self.indexes[col_name][value] = set()
                self.indexes[col_name][value].add(row_idx)
        
        return InsertResult(success=True, rows_affected=1)
    
    def _validate_type(self, value: Any, expected_type: str) -> bool:
        type_map = {
            "INT": int,
            "TEXT": str,
            "BOOL": bool,
        }
        return isinstance(value, type_map[expected_type])
```

---

### 3.5 executor.rollback

**Responsibility**: Snapshot-based rollback for atomic operations

**Implementation**:
```python
from contextlib import contextmanager
import copy

@contextmanager
def snapshot(table: Table):
    """Context manager for atomic operations"""
    saved_rows = copy.deepcopy(table.rows)
    saved_unique = {k: set(v) for k, v in table.unique_indexes.items()}
    saved_indexes = {k: {kk: set(vv) for kk, vv in v.items()} for k, v in table.indexes.items()}
    
    try:
        yield
    except Exception:
        # Rollback
        table.rows = saved_rows
        table.unique_indexes = saved_unique
        table.indexes = saved_indexes
        raise
```

---

### 3.6 repl.repl

**Responsibility**: Interactive interface with graceful error handling

**Implementation**:
```python
class REPL:
    def __init__(self):
        self.db = Database()
        self.lexer = Lexer()
        self.parser = Parser()
        self.executor = Executor()
    
    def run(self):
        print("mini_db v1.0. Type 'EXIT;' to quit.")
        while True:
            try:
                query = input("mini_db> ").strip()
                if not query:
                    continue
                
                # Tokenize
                try:
                    tokens = self.lexer.tokenize(query)
                except LexerError as e:
                    print(f"Syntax error: {e}")
                    continue
                
                # Parse
                try:
                    ast = self.parser.parse(tokens)
                except ParseError as e:
                    print(f"Syntax error: {e}")
                    continue
                
                # Execute
                result = self.executor.execute(ast, self.db)
                
                if result.success:
                    if result.data:
                        self._print_table(result.data)
                    else:
                        print(result.message)
                else:
                    print(f"Error: {result.message}")
                
                if isinstance(ast, ExitNode):
                    break
                    
            except KeyboardInterrupt:
                print("\nUse 'EXIT;' to quit.")
            except Exception as e:
                # Catch-all for unexpected errors
                print(f"Internal error: {e}")
```

---

## 4. Error Handling Strategy

### 4.1 Error Types

| Error Class | When Raised | Message Format |
|-------------|-------------|----------------|
| `LexerError` | Invalid token | `Syntax error: Unexpected character 'X' at position N` |
| `ParseError` | Invalid syntax | `Syntax error: Expected X, got Y` |
| `ExecutionError` | Runtime error | `Error: <description>` |

### 4.2 Error Propagation

```
Lexer → LexerError → REPL → "Syntax error: ..."
Parser → ParseError → REPL → "Syntax error: ..."
Executor → ExecutionError → REPL → "Error: ..."
Storage → ExecutionError → Executor → REPL
```

### 4.3 Graceful Handling

- REPL перехватывает все исключения
- Python Traceback никогда не показывается пользователю
- После ошибки REPL продолжает работу

---

## 5. Data Persistence

### 5.1 JSON Schema for SAVE/LOAD

```json
{
  "version": "1.0",
  "tables": {
    "table_name": {
      "columns": [
        {"name": "col1", "type": "INT", "unique": false},
        {"name": "col2", "type": "TEXT", "unique": true}
      ],
      "rows": [
        {"col1": 42, "col2": "value"},
        {"col1": 100, "col2": null}
      ]
    }
  },
  "indexes": {
    "idx_name": {"table": "table_name", "column": "col1"}
  }
}
```

### 5.2 LOAD Process

1. Parse JSON file
2. Validate schema version
3. Clear current database
4. Recreate tables with columns
5. Insert rows (validates types, UNIQUE)
6. Recreate indexes (rebuild from data)

---

## 6. Testing Strategy

### 6.1 Test Framework

- **Framework**: `unittest` (built-in)
- **Structure**: `tests/` directory
- **Naming**: `test_*.py`

### 6.2 Test Categories

| Category | Files | Focus |
|----------|-------|-------|
| Unit | `test_lexer.py`, `test_parser_*.py` | Parser components |
| Integration | `test_*.py` | End-to-end commands |
| Checkpoint | `test_checkpoints.py` | Critical scenarios |

### 6.3 Test Commands

```bash
# Run all tests
python -m pytest tests/

# Run specific test
python -m unittest tests.test_checkpoints.TestCheckpoints.test_update_atomicity

# Run with verbose output
python -m unittest discover -v tests/
```

---

## 7. Performance Considerations

### 7.1 Time Complexity

| Operation | Without Index | With Index |
|-----------|---------------|------------|
| INSERT | O(1) | O(1) |
| SELECT WHERE col = X | O(n) | O(1) |
| UPDATE | O(n) | O(n) for scan, O(1) for lookup |
| DELETE | O(n) | O(n) for scan |

### 7.2 Space Complexity

| Structure | Complexity |
|-----------|------------|
| Table rows | O(n * m) where n = rows, m = columns |
| UNIQUE index | O(n) per column |
| Hash index | O(n) per column |

---

## 8. Security Considerations

### 8.1 Input Validation

- Lexer: Rejects invalid characters
- Parser: Validates SQL syntax
- Executor: Validates table/column existence, types

### 8.2 File Operations

- SAVE: Overwrites existing files (documented behavior)
- LOAD: Validates JSON structure before applying

### 8.3 No SQL Injection

- Parser builds AST, not string concatenation
- Values are typed, not interpolated

---

## 9. Limitations

### 9.1 Known Limitations

| Limitation | Reason |
|------------|--------|
| No concurrency | Single-threaded REPL |
| No persistence between sessions | Explicit SAVE/LOAD required |
| No query optimization | Simple execution |
| No transactions | UPDATE is atomic, no BEGIN/COMMIT |

### 9.2 Out of Scope

- JOIN operations
- Aggregate functions
- ORDER BY, GROUP BY, LIMIT
- Subqueries
- Stored procedures

---

**Документ подготовлен**: Vaib2 Architect  
**Следующий этап**: Vaib3 Spec → документация (если требуется)