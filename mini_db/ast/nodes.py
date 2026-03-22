# START_MODULE_CONTRACT
# Module: mini_db.ast.nodes
# Intent: Определения всех AST-узлов для SQL-подобного синтаксиса.
#         Иерархия: ASTNode -> StatementNode | ExpressionNode
# Dependencies: dataclasses, typing
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports:
#   - ColumnDef: определение колонки таблицы
#   - StatementNode иерархия: CreateTable, Insert, Update, Delete, Select, CreateIndex, Save, Load, Exit
#   - ExpressionNode иерархия: Comparison, Logical, Identifier, Literal
# END_MODULE_MAP

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


# START_BLOCK_COLUMN_DEF
@dataclass
class ColumnDef:
    """
    [START_CONTRACT_COLUMN_DEF]
    Intent: Определение колонки таблицы с именем, типом и флагом UNIQUE.
    Output: Immutable dataclass для хранения метаданных колонки.
    [END_CONTRACT_COLUMN_DEF]
    """
    name: str
    data_type: str  # "INT", "TEXT", "BOOL"
    unique: bool = False
# END_BLOCK_COLUMN_DEF


# START_BLOCK_AST_BASE
@dataclass
class ASTNode:
    """
    [START_CONTRACT_AST_NODE]
    Intent: Базовый класс для всех AST-узлов. Обеспечивает единую точку расширения.
    Output: Abstract base class (не используется напрямую).
    [END_CONTRACT_AST_NODE]
    """
    pass


@dataclass
class StatementNode(ASTNode):
    """
    [START_CONTRACT_STATEMENT_NODE]
    Intent: Базовый класс для всех SQL-операторов (CREATE, INSERT, SELECT, etc.).
    Output: Abstract base class для операторов.
    [END_CONTRACT_STATEMENT_NODE]
    """
    pass


@dataclass
class ExpressionNode(ASTNode):
    """
    [START_CONTRACT_EXPRESSION_NODE]
    Intent: Базовый класс для выражений в WHERE и условиях.
    Output: Abstract base class для выражений.
    [END_CONTRACT_EXPRESSION_NODE]
    """
    pass
# END_BLOCK_AST_BASE


# START_BLOCK_DDL_STATEMENTS
@dataclass
class CreateTableNode(StatementNode):
    """
    [START_CONTRACT_CREATE_TABLE_NODE]
    Intent: AST-узел для CREATE TABLE с именем таблицы и списком колонок.
    Input: name - валидный идентификатор; columns - непустой список ColumnDef.
    Output: Immutable node для передачи в Executor.
    [END_CONTRACT_CREATE_TABLE_NODE]
    """
    name: str
    columns: list[ColumnDef]


@dataclass
class CreateIndexNode(StatementNode):
    """
    [START_CONTRACT_CREATE_INDEX_NODE]
    Intent: AST-узел для CREATE INDEX с именем, таблицей и колонкой.
    Input: name, table, column - валидные идентификаторы.
    Output: Immutable node для передачи в Executor.
    [END_CONTRACT_CREATE_INDEX_NODE]
    """
    name: str
    table: str
    column: str
# END_BLOCK_DDL_STATEMENTS


# START_BLOCK_DML_STATEMENTS
@dataclass
class InsertNode(StatementNode):
    """
    [START_CONTRACT_INSERT_NODE]
    Intent: AST-узел для INSERT INTO с таблицей, колонками и значениями.
    Input: table - валидный идентификатор; columns, values - одинаковой длины.
    Output: Immutable node для передачи в Executor.
    [END_CONTRACT_INSERT_NODE]
    """
    table: str
    columns: list[str]
    values: list[Any]


@dataclass
class UpdateNode(StatementNode):
    """
    [START_CONTRACT_UPDATE_NODE]
    Intent: AST-узел для UPDATE с таблицей, присваиваниями и опциональным WHERE.
    Input: table - валидный идентификатор; assignments - dict колонка->значение.
    Output: Immutable node для передачи в Executor.
    [END_CONTRACT_UPDATE_NODE]
    """
    table: str
    assignments: dict[str, Any]
    where: Optional[ExpressionNode] = None


@dataclass
class DeleteNode(StatementNode):
    """
    [START_CONTRACT_DELETE_NODE]
    Intent: AST-узел для DELETE FROM с таблицей и опциональным WHERE.
    Input: table - валидный идентификатор.
    Output: Immutable node для передачи в Executor.
    [END_CONTRACT_DELETE_NODE]
    """
    table: str
    where: Optional[ExpressionNode] = None


@dataclass
class SelectNode(StatementNode):
    """
    [START_CONTRACT_SELECT_NODE]
    Intent: AST-узел для SELECT с таблицей, колонками (None = *) и опциональным WHERE.
    Input: table - валидный идентификатор; columns - None или список имён.
    Output: Immutable node для передачи в Executor.
    [END_CONTRACT_SELECT_NODE]
    """
    table: str
    columns: Optional[list[str]] = None  # None = SELECT *
    where: Optional[ExpressionNode] = None
# END_BLOCK_DML_STATEMENTS


# START_BLOCK_SYSTEM_STATEMENTS
@dataclass
class SaveNode(StatementNode):
    """
    [START_CONTRACT_SAVE_NODE]
    Intent: AST-узел для SAVE - сохранение базы в JSON-файл.
    Input: filepath - путь к файлу для сохранения.
    Output: Immutable node для передачи в Executor.
    [END_CONTRACT_SAVE_NODE]
    """
    filepath: str


@dataclass
class LoadNode(StatementNode):
    """
    [START_CONTRACT_LOAD_NODE]
    Intent: AST-узел для LOAD - загрузка базы из JSON-файла.
    Input: filepath - путь к файлу для загрузки.
    Output: Immutable node для передачи в Executor.
    [END_CONTRACT_LOAD_NODE]
    """
    filepath: str


@dataclass
class ExitNode(StatementNode):
    """
    [START_CONTRACT_EXIT_NODE]
    Intent: AST-узел для EXIT - завершение работы REPL.
    Output: Immutable node-маркер для выхода.
    [END_CONTRACT_EXIT_NODE]
    """
    pass
# END_BLOCK_SYSTEM_STATEMENTS


# START_BLOCK_EXPRESSION_NODES
@dataclass
class ComparisonNode(ExpressionNode):
    """
    [START_CONTRACT_COMPARISON_NODE]
    Intent: AST-узел для операции сравнения (=, !=, <, >).
    Input: left, right - ExpressionNode; op - один из "=", "!=", "<", ">".
    Output: Immutable node для вычисления в ExpressionEvaluator.
    [END_CONTRACT_COMPARISON_NODE]
    """
    left: ExpressionNode
    op: str  # "=", "!=", "<", ">"
    right: ExpressionNode


@dataclass
class LogicalNode(ExpressionNode):
    """
    [START_CONTRACT_LOGICAL_NODE]
    Intent: AST-узел для логической операции (AND, OR).
    Input: left, right - ExpressionNode; op - "AND" или "OR".
    Output: Immutable node для вычисления в ExpressionEvaluator.
    [END_CONTRACT_LOGICAL_NODE]
    """
    left: ExpressionNode
    op: str  # "AND", "OR"
    right: ExpressionNode


@dataclass
class IdentifierNode(ExpressionNode):
    """
    [START_CONTRACT_IDENTIFIER_NODE]
    Intent: AST-узел для идентификатора (имя колонки).
    Input: name - валидное имя колонки.
    Output: Immutable node для разрешения в значение строки.
    [END_CONTRACT_IDENTIFIER_NODE]
    """
    name: str


@dataclass
class LiteralNode(ExpressionNode):
    """
    [START_CONTRACT_LITERAL_NODE]
    Intent: AST-узел для литерального значения (int, str, bool, None).
    Input: value - примитивный тип Python.
    Output: Immutable node, возвращающий значение как есть.
    [END_CONTRACT_LITERAL_NODE]
    """
    value: Any  # int, str, bool, None
# END_BLOCK_EXPRESSION_NODES