# START_MODULE_CONTRACT
# Module: mini_db_v2.ast.nodes
# Intent: AST узлы для SQL-парсинга. Все типы запросов и выражений.
# Dependencies: dataclasses, typing, enum
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: ASTNode, SelectNode, InsertNode, UpdateNode, DeleteNode,
#          CreateTableNode, CreateIndexNode, DropTableNode,
#          JoinClause, ExpressionNode, BinaryOpNode, UnaryOpNode,
#          LiteralNode, ColumnRef, TableRef, SubqueryNode
# END_MODULE_MAP

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional
from enum import Enum, auto


# =============================================================================
# START_BLOCK_ENUMS
# =============================================================================

class JoinType(Enum):
    """Типы JOIN операций."""
    INNER = auto()
    LEFT = auto()
    RIGHT = auto()
    FULL = auto()
    CROSS = auto()


class BinaryOperator(Enum):
    """Бинарные операторы."""
    # Арифметические
    ADD = auto()
    SUB = auto()
    MUL = auto()
    DIV = auto()
    MOD = auto()
    # Сравнения
    EQ = auto()      # =
    NE = auto()      # !=, <>
    LT = auto()      # <
    LE = auto()      # <=
    GT = auto()      # >
    GE = auto()      # >=
    # Логические
    AND = auto()
    OR = auto()
    # Специальные
    LIKE = auto()
    NOT_LIKE = auto()
    IN = auto()
    NOT_IN = auto()
    BETWEEN = auto()
    IS = auto()      # IS NULL, IS NOT NULL


class UnaryOperator(Enum):
    """Унарные операторы."""
    NEG = auto()    # -x
    NOT = auto()    # NOT x
    IS_NULL = auto()
    IS_NOT_NULL = auto()


class AggregateType(Enum):
    """Агрегатные функции."""
    COUNT = auto()
    SUM = auto()
    AVG = auto()
    MIN = auto()
    MAX = auto()


class DataType(Enum):
    """Типы данных SQL."""
    INT = auto()
    TEXT = auto()
    REAL = auto()
    BOOL = auto()
    NULL = auto()


# END_BLOCK_ENUMS


# =============================================================================
# START_BLOCK_BASE_NODES
# =============================================================================

@dataclass
class ASTNode:
    """
    [START_CONTRACT_AST_NODE]
    Intent: Базовый класс для всех AST узлов.
    Output: Общий интерфейс для visitor pattern.
    [END_CONTRACT_AST_NODE]
    """
    pass


@dataclass
class ExpressionNode(ASTNode):
    """
    [START_CONTRACT_EXPRESSION_NODE]
    Intent: Базовый класс для всех выражений (WHERE, SELECT expressions).
    Output: Общий интерфейс для expression evaluation.
    [END_CONTRACT_EXPRESSION_NODE]
    """
    pass

# END_BLOCK_BASE_NODES


# =============================================================================
# START_BLOCK_LITERALS_AND_REFS
# =============================================================================

@dataclass
class LiteralNode(ExpressionNode):
    """
    [START_CONTRACT_LITERAL_NODE]
    Intent: Литеральное значение (число, строка, NULL, булево).
    Input: value - Python значение; data_type - SQL тип.
    Output: Узел представляющий константу.
    [END_CONTRACT_LITERAL_NODE]
    """
    value: Any
    data_type: DataType = DataType.NULL


@dataclass
class ColumnRef(ExpressionNode):
    """
    [START_CONTRACT_COLUMN_REF]
    Intent: Ссылка на колонку таблицы (возможно с алиасом таблицы).
    Input: column_name - имя колонки; table_alias - опциональный алиас.
    Output: Узел для разрешения колонки в runtime.
    [END_CONTRACT_COLUMN_REF]
    """
    column_name: str
    table_alias: Optional[str] = None


@dataclass
class TableRef(ASTNode):
    """
    [START_CONTRACT_TABLE_REF]
    Intent: Ссылка на таблицу с опциональным алиасом.
    Input: table_name - имя таблицы; alias - опциональный алиас.
    Output: Узел для разрешения таблицы в runtime.
    [END_CONTRACT_TABLE_REF]
    """
    table_name: str
    alias: Optional[str] = None


@dataclass
class StarColumn(ExpressionNode):
    """
    [START_CONTRACT_STAR_COLUMN]
    Intent: SELECT * или table.* - все колонки.
    Input: table_alias - опционально для table.*
    Output: Узел для раскрытия * в список колонок.
    [END_CONTRACT_STAR_COLUMN]
    """
    table_alias: Optional[str] = None

# END_BLOCK_LITERALS_AND_REFS


# =============================================================================
# START_BLOCK_EXPRESSIONS
# =============================================================================

@dataclass
class BinaryOpNode(ExpressionNode):
    """
    [START_CONTRACT_BINARY_OP_NODE]
    Intent: Бинарная операция (a + b, a = b, a AND b).
    Input: left, right - операнды; operator - тип операции.
    Output: Узел для вычисления бинарного выражения.
    [END_CONTRACT_BINARY_OP_NODE]
    """
    left: ExpressionNode
    operator: BinaryOperator
    right: ExpressionNode


@dataclass
class UnaryOpNode(ExpressionNode):
    """
    [START_CONTRACT_UNARY_OP_NODE]
    Intent: Унарная операция (-x, NOT x, IS NULL).
    Input: operand - операнд; operator - тип операции.
    Output: Узел для вычисления унарного выражения.
    [END_CONTRACT_UNARY_OP_NODE]
    """
    operand: ExpressionNode
    operator: UnaryOperator


@dataclass
class FunctionCall(ExpressionNode):
    """
    [START_CONTRACT_FUNCTION_CALL]
    Intent: Вызов функции (COUNT, SUM, COALESCE, CAST).
    Input: name - имя функции; args - аргументы.
    Output: Узел для вычисления функции.
    [END_CONTRACT_FUNCTION_CALL]
    """
    name: str
    args: list[ExpressionNode] = field(default_factory=list)


@dataclass
class AggregateNode(ExpressionNode):
    """
    [START_CONTRACT_AGGREGATE_NODE]
    Intent: Агрегатная функция (COUNT, SUM, AVG, MIN, MAX).
    Input: agg_type - тип агрегации; arg - аргумент (или None для COUNT(*)).
    Output: Узел для вычисления агрегата.
    [END_CONTRACT_AGGREGATE_NODE]
    """
    agg_type: AggregateType
    arg: Optional[ExpressionNode] = None
    distinct: bool = False


@dataclass
class BetweenNode(ExpressionNode):
    """
    [START_CONTRACT_BETWEEN_NODE]
    Intent: BETWEEN выражение (x BETWEEN a AND b).
    Input: expr - проверяемое выражение; low, high - границы.
    Output: Узел для проверки диапазона.
    [END_CONTRACT_BETWEEN_NODE]
    """
    expr: ExpressionNode
    low: ExpressionNode
    high: ExpressionNode
    negated: bool = False


@dataclass
class InListNode(ExpressionNode):
    """
    [START_CONTRACT_IN_LIST_NODE]
    Intent: IN список значений (x IN (1, 2, 3)).
    Input: expr - проверяемое выражение; values - список значений.
    Output: Узел для проверки вхождения.
    [END_CONTRACT_IN_LIST_NODE]
    """
    expr: ExpressionNode
    values: list[ExpressionNode]
    negated: bool = False


@dataclass
class CaseExpression(ExpressionNode):
    """
    [START_CONTRACT_CASE_EXPRESSION]
    Intent: CASE WHEN ... THEN ... ELSE ... END выражение.
    Input: when_clauses - список (condition, result); else_result - иначе.
    Output: Узел для условного выбора.
    [END_CONTRACT_CASE_EXPRESSION]
    """
    when_clauses: list[tuple[ExpressionNode, ExpressionNode]] = field(
        default_factory=list
    )
    else_result: Optional[ExpressionNode] = None

# END_BLOCK_EXPRESSIONS


# =============================================================================
# START_BLOCK_SUBQUERIES
# =============================================================================

@dataclass
class SubqueryNode(ExpressionNode):
    """
    [START_CONTRACT_SUBQUERY_NODE]
    Intent: Подзапрос в выражении (scalar, IN, EXISTS).
    Input: query - SELECT запрос; subquery_type - тип использования.
    Output: Узел для выполнения подзапроса.
    [END_CONTRACT_SUBQUERY_NODE]
    """
    query: SelectNode
    subquery_type: str = "scalar"  # scalar, in, exists


@dataclass
class ExistsNode(ExpressionNode):
    """
    [START_CONTRACT_EXISTS_NODE]
    Intent: EXISTS подзапрос.
    Input: subquery - проверяемый подзапрос; negated - NOT EXISTS.
    Output: Узел для проверки существования.
    [END_CONTRACT_EXISTS_NODE]
    """
    subquery: SubqueryNode
    negated: bool = False

# END_BLOCK_SUBQUERIES


# =============================================================================
# START_BLOCK_SELECT_STRUCTURES
# =============================================================================

@dataclass
class SelectColumn(ASTNode):
    """
    [START_CONTRACT_SELECT_COLUMN]
    Intent: Колонка в SELECT с опциональным алиасом.
    Input: expression - выражение; alias - опциональный алиас.
    Output: Узел для проекции результата.
    [END_CONTRACT_SELECT_COLUMN]
    """
    expression: ExpressionNode
    alias: Optional[str] = None


@dataclass
class JoinClause(ASTNode):
    """
    [START_CONTRACT_JOIN_CLAUSE]
    Intent: JOIN предложение (INNER, LEFT, RIGHT, FULL, CROSS).
    Input: join_type - тип JOIN; table - таблица; condition - ON условие.
    Output: Узел для выполнения JOIN операции.
    [END_CONTRACT_JOIN_CLAUSE]
    """
    join_type: JoinType
    table: TableRef
    condition: Optional[ExpressionNode] = None


@dataclass
class FromClause(ASTNode):
    """
    [START_CONTRACT_FROM_CLAUSE]
    Intent: FROM предложение с таблицей и JOINs.
    Input: table - основная таблица; joins - список JOIN предложений.
    Output: Узел для разрешения источников данных.
    [END_CONTRACT_FROM_CLAUSE]
    """
    table: TableRef
    joins: list[JoinClause] = field(default_factory=list)


@dataclass
class OrderByItem(ASTNode):
    """
    [START_CONTRACT_ORDER_BY_ITEM]
    Intent: Элемент ORDER BY.
    Input: expression - выражение; ascending - порядок сортировки.
    Output: Узел для сортировки результата.
    [END_CONTRACT_ORDER_BY_ITEM]
    """
    expression: ExpressionNode
    ascending: bool = True

# END_BLOCK_SELECT_STRUCTURES


# =============================================================================
# START_BLOCK_STATEMENTS
# =============================================================================

@dataclass
class SelectNode(ASTNode):
    """
    [START_CONTRACT_SELECT_NODE]
    Intent: SELECT запрос с полной поддержкой SQL синтаксиса.
    Input: columns - список колонок; from_clause - источник; where - фильтр;
           group_by - группировка; having - фильтр групп; order_by - сортировка;
           limit, offset - пагинация; distinct - уникальность.
    Output: AST для SELECT запроса.
    [END_CONTRACT_SELECT_NODE]
    """
    columns: list[SelectColumn]
    from_clause: Optional[FromClause] = None
    where: Optional[ExpressionNode] = None
    group_by: list[ColumnRef] = field(default_factory=list)
    having: Optional[ExpressionNode] = None
    order_by: list[OrderByItem] = field(default_factory=list)
    limit: Optional[int] = None
    offset: Optional[int] = None
    distinct: bool = False


@dataclass
class InsertNode(ASTNode):
    """
    [START_CONTRACT_INSERT_NODE]
    Intent: INSERT INTO запрос.
    Input: table_name - таблица; columns - колонки; values - значения.
    Output: AST для INSERT запроса.
    [END_CONTRACT_INSERT_NODE]
    """
    table_name: str
    columns: list[str] = field(default_factory=list)
    values: list[list[ExpressionNode]] = field(default_factory=list)
    select: Optional[SelectNode] = None  # INSERT ... SELECT


@dataclass
class UpdateNode(ASTNode):
    """
    [START_CONTRACT_UPDATE_NODE]
    Intent: UPDATE запрос.
    Input: table_name - таблица; assignments - {колонка: выражение};
           where - условие фильтрации.
    Output: AST для UPDATE запроса.
    [END_CONTRACT_UPDATE_NODE]
    """
    table_name: str
    assignments: dict[str, ExpressionNode] = field(default_factory=dict)
    where: Optional[ExpressionNode] = None


@dataclass
class DeleteNode(ASTNode):
    """
    [START_CONTRACT_DELETE_NODE]
    Intent: DELETE запрос.
    Input: table_name - таблица; where - условие фильтрации.
    Output: AST для DELETE запроса.
    [END_CONTRACT_DELETE_NODE]
    """
    table_name: str
    where: Optional[ExpressionNode] = None

# END_BLOCK_STATEMENTS


# =============================================================================
# START_BLOCK_DDL
# =============================================================================

@dataclass
class ColumnDef(ASTNode):
    """
    [START_CONTRACT_COLUMN_DEF]
    Intent: Определение колонки в CREATE TABLE.
    Input: name - имя; data_type - тип; constraints - ограничения.
    Output: Узел для создания колонки.
    [END_CONTRACT_COLUMN_DEF]
    """
    name: str
    data_type: DataType
    nullable: bool = True
    primary_key: bool = False
    unique: bool = False
    default: Optional[ExpressionNode] = None


@dataclass
class CreateTableNode(ASTNode):
    """
    [START_CONTRACT_CREATE_TABLE_NODE]
    Intent: CREATE TABLE запрос.
    Input: table_name - имя таблицы; columns - определения колонок;
           if_not_exists - игнорировать если существует.
    Output: AST для CREATE TABLE.
    [END_CONTRACT_CREATE_TABLE_NODE]
    """
    table_name: str
    columns: list[ColumnDef] = field(default_factory=list)
    if_not_exists: bool = False


@dataclass
class CreateIndexNode(ASTNode):
    """
    [START_CONTRACT_CREATE_INDEX_NODE]
    Intent: CREATE INDEX запрос.
    Input: index_name - имя индекса; table_name - таблица;
           columns - колонки; unique - уникальность.
    Output: AST для CREATE INDEX.
    [END_CONTRACT_CREATE_INDEX_NODE]
    """
    index_name: str
    table_name: str
    columns: list[ColumnRef] = field(default_factory=list)
    unique: bool = False
    if_not_exists: bool = False


@dataclass
class DropTableNode(ASTNode):
    """
    [START_CONTRACT_DROP_TABLE_NODE]
    Intent: DROP TABLE запрос.
    Input: table_name - имя таблицы; if_exists - игнорировать если нет.
    Output: AST для DROP TABLE.
    [END_CONTRACT_DROP_TABLE_NODE]
    """
    table_name: str
    if_exists: bool = False


@dataclass
class DropIndexNode(ASTNode):
    """
    [START_CONTRACT_DROP_INDEX_NODE]
    Intent: DROP INDEX запрос.
    Input: index_name - имя индекса; if_exists - игнорировать если нет.
    Output: AST для DROP INDEX.
    [END_CONTRACT_DROP_INDEX_NODE]
    """
    index_name: str
    if_exists: bool = False


@dataclass
class AnalyzeTableNode(ASTNode):
    """
    [START_CONTRACT_ANALYZE_TABLE_NODE]
    Intent: ANALYZE TABLE запрос - сбор статистики для оптимизатора.
    Input: table_name - имя таблицы для анализа.
    Output: AST для ANALYZE TABLE.
    [END_CONTRACT_ANALYZE_TABLE_NODE]
    """
    table_name: str

# END_BLOCK_DDL


# =============================================================================
# START_BLOCK_UTILITIES
# =============================================================================

@dataclass
class TransactionNode(ASTNode):
    """
    [START_CONTRACT_TRANSACTION_NODE]
    Intent: BEGIN, COMMIT, ROLLBACK команды.
    Input: command - тип команды; isolation_level - уровень изоляции.
    Output: AST для управления транзакцией.
    [END_CONTRACT_TRANSACTION_NODE]
    """
    command: str  # BEGIN, COMMIT, ROLLBACK
    isolation_level: Optional[str] = None  # READ COMMITTED, REPEATABLE READ


@dataclass
class ExplainNode(ASTNode):
    """
    [START_CONTRACT_EXPLAIN_NODE]
    Intent: EXPLAIN запрос - показать план выполнения.
    Input: query - анализируемый запрос; analyze - выполнить ли запрос.
    Output: AST для EXPLAIN.
    [END_CONTRACT_EXPLAIN_NODE]
    """
    query: SelectNode
    analyze: bool = False

# END_BLOCK_UTILITIES