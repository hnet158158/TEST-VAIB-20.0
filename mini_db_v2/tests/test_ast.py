# START_MODULE_CONTRACT
# Module: mini_db_v2.tests.test_ast
# Intent: Comprehensive tests for AST nodes (Phase 1 Foundation).
# Dependencies: pytest
# END_MODULE_CONTRACT

"""
Phase 1 Foundation - AST Node Tests

Tests cover:
- Base node classes (ASTNode, ExpressionNode)
- Literal nodes (LiteralNode, NULL handling)
- Reference nodes (ColumnRef, TableRef, StarColumn)
- Expression nodes (BinaryOpNode, UnaryOpNode, FunctionCall)
- Aggregate nodes (AggregateNode, BetweenNode, InListNode, CaseExpression)
- Subquery nodes (SubqueryNode, ExistsNode)
- Select structures (SelectColumn, JoinClause, FromClause, OrderByItem)
- Statement nodes (SelectNode, InsertNode, UpdateNode, DeleteNode)
- DDL nodes (CreateTableNode, CreateIndexNode, DropTableNode, DropIndexNode)
- Utility nodes (TransactionNode, ExplainNode)
- Enums (JoinType, BinaryOperator, UnaryOperator, AggregateType, DataType)
"""

import pytest
import sys
import os

# Add parent to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from mini_db_v2.ast.nodes import (
    # Enums
    JoinType, BinaryOperator, UnaryOperator, AggregateType, DataType,
    # Base nodes
    ASTNode, ExpressionNode,
    # Literals and refs
    LiteralNode, ColumnRef, TableRef, StarColumn,
    # Expressions
    BinaryOpNode, UnaryOpNode, FunctionCall,
    AggregateNode, BetweenNode, InListNode, CaseExpression,
    # Subqueries
    SubqueryNode, ExistsNode,
    # Select structures
    SelectColumn, JoinClause, FromClause, OrderByItem,
    # Statements
    SelectNode, InsertNode, UpdateNode, DeleteNode,
    # DDL
    ColumnDef, CreateTableNode, CreateIndexNode, DropTableNode, DropIndexNode,
    # Utilities
    TransactionNode, ExplainNode,
)


# =============================================================================
# Enum Tests
# =============================================================================

class TestEnums:
    """Tests for all enum types."""

    def test_join_type_values(self):
        """JoinType enum has all expected values."""
        assert JoinType.INNER.value == 1
        assert JoinType.LEFT.value == 2
        assert JoinType.RIGHT.value == 3
        assert JoinType.FULL.value == 4
        assert JoinType.CROSS.value == 5

    def test_binary_operator_comparison(self):
        """BinaryOperator comparison operators exist."""
        assert BinaryOperator.EQ is not None
        assert BinaryOperator.NE is not None
        assert BinaryOperator.LT is not None
        assert BinaryOperator.LE is not None
        assert BinaryOperator.GT is not None
        assert BinaryOperator.GE is not None

    def test_binary_operator_logical(self):
        """BinaryOperator logical operators exist."""
        assert BinaryOperator.AND is not None
        assert BinaryOperator.OR is not None

    def test_binary_operator_arithmetic(self):
        """BinaryOperator arithmetic operators exist."""
        assert BinaryOperator.ADD is not None
        assert BinaryOperator.SUB is not None
        assert BinaryOperator.MUL is not None
        assert BinaryOperator.DIV is not None
        assert BinaryOperator.MOD is not None

    def test_binary_operator_special(self):
        """BinaryOperator special operators exist."""
        assert BinaryOperator.LIKE is not None
        assert BinaryOperator.NOT_LIKE is not None
        assert BinaryOperator.IN is not None
        assert BinaryOperator.NOT_IN is not None
        assert BinaryOperator.BETWEEN is not None
        assert BinaryOperator.IS is not None

    def test_unary_operator(self):
        """UnaryOperator enum values."""
        assert UnaryOperator.NEG.value == 1
        assert UnaryOperator.NOT.value == 2
        assert UnaryOperator.IS_NULL.value == 3
        assert UnaryOperator.IS_NOT_NULL.value == 4

    def test_aggregate_type(self):
        """AggregateType enum values."""
        assert AggregateType.COUNT.value == 1
        assert AggregateType.SUM.value == 2
        assert AggregateType.AVG.value == 3
        assert AggregateType.MIN.value == 4
        assert AggregateType.MAX.value == 5

    def test_data_type(self):
        """DataType enum values."""
        assert DataType.INT.value == 1
        assert DataType.TEXT.value == 2
        assert DataType.REAL.value == 3
        assert DataType.BOOL.value == 4
        assert DataType.NULL.value == 5


# =============================================================================
# Base Node Tests
# =============================================================================

class TestBaseNodes:
    """Tests for base AST node classes."""

    def test_ast_node_is_dataclass(self):
        """ASTNode is a dataclass."""
        node = ASTNode()
        assert hasattr(node, '__dataclass_fields__')

    def test_expression_node_inherits_ast_node(self):
        """ExpressionNode inherits from ASTNode."""
        assert issubclass(ExpressionNode, ASTNode)

    def test_expression_node_is_dataclass(self):
        """ExpressionNode is a dataclass."""
        node = ExpressionNode()
        assert hasattr(node, '__dataclass_fields__')


# =============================================================================
# Literal and Reference Node Tests
# =============================================================================

class TestLiteralNodes:
    """Tests for literal and reference nodes."""

    def test_literal_node_integer(self):
        """LiteralNode with integer value."""
        node = LiteralNode(value=42, data_type=DataType.INT)
        assert node.value == 42
        assert node.data_type == DataType.INT

    def test_literal_node_string(self):
        """LiteralNode with string value."""
        node = LiteralNode(value="hello", data_type=DataType.TEXT)
        assert node.value == "hello"
        assert node.data_type == DataType.TEXT

    def test_literal_node_null(self):
        """LiteralNode with NULL value."""
        node = LiteralNode(value=None, data_type=DataType.NULL)
        assert node.value is None
        assert node.data_type == DataType.NULL

    def test_literal_node_boolean(self):
        """LiteralNode with boolean value."""
        node_true = LiteralNode(value=True, data_type=DataType.BOOL)
        node_false = LiteralNode(value=False, data_type=DataType.BOOL)
        assert node_true.value is True
        assert node_false.value is False

    def test_literal_node_real(self):
        """LiteralNode with real value."""
        node = LiteralNode(value=3.14, data_type=DataType.REAL)
        assert node.value == 3.14
        assert node.data_type == DataType.REAL

    def test_literal_node_default_data_type(self):
        """LiteralNode default data_type is NULL."""
        node = LiteralNode(value=None)
        assert node.data_type == DataType.NULL

    def test_column_ref_simple(self):
        """ColumnRef with simple name."""
        node = ColumnRef(column_name="id")
        assert node.column_name == "id"
        assert node.table_alias is None

    def test_column_ref_with_alias(self):
        """ColumnRef with table alias."""
        node = ColumnRef(column_name="id", table_alias="users")
        assert node.column_name == "id"
        assert node.table_alias == "users"

    def test_table_ref_simple(self):
        """TableRef with simple name."""
        node = TableRef(table_name="users")
        assert node.table_name == "users"
        assert node.alias is None

    def test_table_ref_with_alias(self):
        """TableRef with alias."""
        node = TableRef(table_name="users", alias="u")
        assert node.table_name == "users"
        assert node.alias == "u"

    def test_star_column(self):
        """StarColumn for SELECT *."""
        node = StarColumn()
        assert node.table_alias is None

    def test_star_column_with_alias(self):
        """StarColumn for table.*."""
        node = StarColumn(table_alias="users")
        assert node.table_alias == "users"


# =============================================================================
# Expression Node Tests
# =============================================================================

class TestExpressionNodes:
    """Tests for expression nodes."""

    def test_binary_op_node_comparison(self):
        """BinaryOpNode with comparison operator."""
        left = LiteralNode(value=1, data_type=DataType.INT)
        right = LiteralNode(value=2, data_type=DataType.INT)
        node = BinaryOpNode(left=left, operator=BinaryOperator.LT, right=right)
        assert node.left == left
        assert node.operator == BinaryOperator.LT
        assert node.right == right

    def test_binary_op_node_logical(self):
        """BinaryOpNode with logical operator."""
        left = LiteralNode(value=True, data_type=DataType.BOOL)
        right = LiteralNode(value=False, data_type=DataType.BOOL)
        node = BinaryOpNode(left=left, operator=BinaryOperator.AND, right=right)
        assert node.operator == BinaryOperator.AND

    def test_binary_op_node_arithmetic(self):
        """BinaryOpNode with arithmetic operator."""
        left = LiteralNode(value=10, data_type=DataType.INT)
        right = LiteralNode(value=5, data_type=DataType.INT)
        node = BinaryOpNode(left=left, operator=BinaryOperator.ADD, right=right)
        assert node.operator == BinaryOperator.ADD

    def test_unary_op_node_neg(self):
        """UnaryOpNode with negation."""
        operand = LiteralNode(value=42, data_type=DataType.INT)
        node = UnaryOpNode(operand=operand, operator=UnaryOperator.NEG)
        assert node.operand == operand
        assert node.operator == UnaryOperator.NEG

    def test_unary_op_node_not(self):
        """UnaryOpNode with NOT."""
        operand = LiteralNode(value=True, data_type=DataType.BOOL)
        node = UnaryOpNode(operand=operand, operator=UnaryOperator.NOT)
        assert node.operator == UnaryOperator.NOT

    def test_unary_op_node_is_null(self):
        """UnaryOpNode with IS NULL."""
        operand = ColumnRef(column_name="name")
        node = UnaryOpNode(operand=operand, operator=UnaryOperator.IS_NULL)
        assert node.operator == UnaryOperator.IS_NULL

    def test_function_call_no_args(self):
        """FunctionCall with no arguments."""
        node = FunctionCall(name="NOW")
        assert node.name == "NOW"
        assert node.args == []

    def test_function_call_with_args(self):
        """FunctionCall with arguments."""
        arg1 = LiteralNode(value="hello", data_type=DataType.TEXT)
        node = FunctionCall(name="UPPER", args=[arg1])
        assert node.name == "UPPER"
        assert len(node.args) == 1

    def test_aggregate_node_count_star(self):
        """AggregateNode for COUNT(*)."""
        node = AggregateNode(agg_type=AggregateType.COUNT)
        assert node.agg_type == AggregateType.COUNT
        assert node.arg is None
        assert node.distinct is False

    def test_aggregate_node_count_column(self):
        """AggregateNode for COUNT(column)."""
        arg = ColumnRef(column_name="id")
        node = AggregateNode(agg_type=AggregateType.COUNT, arg=arg)
        assert node.agg_type == AggregateType.COUNT
        assert node.arg == arg

    def test_aggregate_node_sum(self):
        """AggregateNode for SUM."""
        arg = ColumnRef(column_name="price")
        node = AggregateNode(agg_type=AggregateType.SUM, arg=arg)
        assert node.agg_type == AggregateType.SUM

    def test_aggregate_node_distinct(self):
        """AggregateNode with DISTINCT."""
        arg = ColumnRef(column_name="category")
        node = AggregateNode(agg_type=AggregateType.COUNT, arg=arg, distinct=True)
        assert node.distinct is True

    def test_between_node(self):
        """BetweenNode for x BETWEEN a AND b."""
        expr = ColumnRef(column_name="age")
        low = LiteralNode(value=18, data_type=DataType.INT)
        high = LiteralNode(value=65, data_type=DataType.INT)
        node = BetweenNode(expr=expr, low=low, high=high)
        assert node.expr == expr
        assert node.low == low
        assert node.high == high
        assert node.negated is False

    def test_between_node_negated(self):
        """BetweenNode for x NOT BETWEEN a AND b."""
        expr = ColumnRef(column_name="age")
        low = LiteralNode(value=18, data_type=DataType.INT)
        high = LiteralNode(value=65, data_type=DataType.INT)
        node = BetweenNode(expr=expr, low=low, high=high, negated=True)
        assert node.negated is True

    def test_in_list_node(self):
        """InListNode for x IN (1, 2, 3)."""
        expr = ColumnRef(column_name="status")
        values = [
            LiteralNode(value="active", data_type=DataType.TEXT),
            LiteralNode(value="pending", data_type=DataType.TEXT),
        ]
        node = InListNode(expr=expr, values=values)
        assert node.expr == expr
        assert len(node.values) == 2
        assert node.negated is False

    def test_in_list_node_negated(self):
        """InListNode for x NOT IN (1, 2, 3)."""
        expr = ColumnRef(column_name="status")
        values = [LiteralNode(value="deleted", data_type=DataType.TEXT)]
        node = InListNode(expr=expr, values=values, negated=True)
        assert node.negated is True

    def test_case_expression(self):
        """CaseExpression for CASE WHEN ... THEN ... ELSE ... END."""
        when_cond = BinaryOpNode(
            left=ColumnRef(column_name="age"),
            operator=BinaryOperator.LT,
            right=LiteralNode(value=18, data_type=DataType.INT)
        )
        when_result = LiteralNode(value="minor", data_type=DataType.TEXT)
        else_result = LiteralNode(value="adult", data_type=DataType.TEXT)
        node = CaseExpression(
            when_clauses=[(when_cond, when_result)],
            else_result=else_result
        )
        assert len(node.when_clauses) == 1
        assert node.else_result == else_result

    def test_case_expression_no_else(self):
        """CaseExpression without ELSE clause."""
        when_cond = ColumnRef(column_name="active")
        when_result = LiteralNode(value="yes", data_type=DataType.TEXT)
        node = CaseExpression(when_clauses=[(when_cond, when_result)])
        assert node.else_result is None


# =============================================================================
# Subquery Node Tests
# =============================================================================

class TestSubqueryNodes:
    """Tests for subquery nodes."""

    def test_subquery_node_scalar(self):
        """SubqueryNode for scalar subquery."""
        # Create a simple SelectNode
        select = SelectNode(columns=[SelectColumn(expression=ColumnRef(column_name="id"))])
        node = SubqueryNode(query=select, subquery_type="scalar")
        assert node.query == select
        assert node.subquery_type == "scalar"

    def test_subquery_node_exists(self):
        """SubqueryNode for EXISTS."""
        select = SelectNode(columns=[SelectColumn(expression=StarColumn())])
        node = SubqueryNode(query=select, subquery_type="exists")
        assert node.subquery_type == "exists"

    def test_exists_node(self):
        """ExistsNode for EXISTS subquery."""
        select = SelectNode(columns=[SelectColumn(expression=StarColumn())])
        subquery = SubqueryNode(query=select, subquery_type="exists")
        node = ExistsNode(subquery=subquery)
        assert node.subquery == subquery
        assert node.negated is False

    def test_exists_node_negated(self):
        """ExistsNode for NOT EXISTS."""
        select = SelectNode(columns=[SelectColumn(expression=StarColumn())])
        subquery = SubqueryNode(query=select, subquery_type="exists")
        node = ExistsNode(subquery=subquery, negated=True)
        assert node.negated is True


# =============================================================================
# Select Structure Tests
# =============================================================================

class TestSelectStructures:
    """Tests for SELECT-related structures."""

    def test_select_column_simple(self):
        """SelectColumn with simple expression."""
        expr = ColumnRef(column_name="id")
        node = SelectColumn(expression=expr)
        assert node.expression == expr
        assert node.alias is None

    def test_select_column_with_alias(self):
        """SelectColumn with alias."""
        expr = ColumnRef(column_name="user_id")
        node = SelectColumn(expression=expr, alias="uid")
        assert node.alias == "uid"

    def test_join_clause_inner(self):
        """JoinClause for INNER JOIN."""
        table = TableRef(table_name="orders")
        condition = BinaryOpNode(
            left=ColumnRef(column_name="id", table_alias="users"),
            operator=BinaryOperator.EQ,
            right=ColumnRef(column_name="user_id", table_alias="orders")
        )
        node = JoinClause(join_type=JoinType.INNER, table=table, condition=condition)
        assert node.join_type == JoinType.INNER
        assert node.table == table
        assert node.condition == condition

    def test_join_clause_left(self):
        """JoinClause for LEFT JOIN."""
        table = TableRef(table_name="orders", alias="o")
        node = JoinClause(join_type=JoinType.LEFT, table=table)
        assert node.join_type == JoinType.LEFT

    def test_join_clause_cross(self):
        """JoinClause for CROSS JOIN (no condition)."""
        table = TableRef(table_name="products")
        node = JoinClause(join_type=JoinType.CROSS, table=table)
        assert node.join_type == JoinType.CROSS
        assert node.condition is None

    def test_from_clause_simple(self):
        """FromClause with single table."""
        table = TableRef(table_name="users")
        node = FromClause(table=table)
        assert node.table == table
        assert node.joins == []

    def test_from_clause_with_joins(self):
        """FromClause with JOINs."""
        table = TableRef(table_name="users")
        join1 = JoinClause(join_type=JoinType.INNER, table=TableRef(table_name="orders"))
        node = FromClause(table=table, joins=[join1])
        assert len(node.joins) == 1

    def test_order_by_item_asc(self):
        """OrderByItem ascending."""
        expr = ColumnRef(column_name="name")
        node = OrderByItem(expression=expr)
        assert node.expression == expr
        assert node.ascending is True

    def test_order_by_item_desc(self):
        """OrderByItem descending."""
        expr = ColumnRef(column_name="created_at")
        node = OrderByItem(expression=expr, ascending=False)
        assert node.ascending is False


# =============================================================================
# Statement Node Tests
# =============================================================================

class TestStatementNodes:
    """Tests for statement nodes."""

    def test_select_node_minimal(self):
        """SelectNode with minimal fields."""
        columns = [SelectColumn(expression=StarColumn())]
        node = SelectNode(columns=columns)
        assert node.columns == columns
        assert node.from_clause is None
        assert node.where is None
        assert node.group_by == []
        assert node.having is None
        assert node.order_by == []
        assert node.limit is None
        assert node.offset is None
        assert node.distinct is False

    def test_select_node_full(self):
        """SelectNode with all fields."""
        columns = [SelectColumn(expression=ColumnRef(column_name="id"))]
        from_clause = FromClause(table=TableRef(table_name="users"))
        where = BinaryOpNode(
            left=ColumnRef(column_name="active"),
            operator=BinaryOperator.EQ,
            right=LiteralNode(value=True, data_type=DataType.BOOL)
        )
        group_by = [ColumnRef(column_name="category")]
        having = BinaryOpNode(
            left=AggregateNode(agg_type=AggregateType.COUNT),
            operator=BinaryOperator.GT,
            right=LiteralNode(value=10, data_type=DataType.INT)
        )
        order_by = [OrderByItem(expression=ColumnRef(column_name="name"))]
        
        node = SelectNode(
            columns=columns,
            from_clause=from_clause,
            where=where,
            group_by=group_by,
            having=having,
            order_by=order_by,
            limit=100,
            offset=10,
            distinct=True
        )
        assert node.from_clause == from_clause
        assert node.where == where
        assert len(node.group_by) == 1
        assert node.having == having
        assert len(node.order_by) == 1
        assert node.limit == 100
        assert node.offset == 10
        assert node.distinct is True

    def test_insert_node_values(self):
        """InsertNode with VALUES."""
        node = InsertNode(
            table_name="users",
            columns=["id", "name"],
            values=[
                [LiteralNode(value=1, data_type=DataType.INT), 
                 LiteralNode(value="John", data_type=DataType.TEXT)]
            ]
        )
        assert node.table_name == "users"
        assert node.columns == ["id", "name"]
        assert len(node.values) == 1
        assert node.select is None

    def test_insert_node_select(self):
        """InsertNode with SELECT."""
        select = SelectNode(columns=[SelectColumn(expression=StarColumn())])
        node = InsertNode(table_name="users_backup", select=select)
        assert node.select == select
        assert node.values == []

    def test_update_node(self):
        """UpdateNode."""
        where = BinaryOpNode(
            left=ColumnRef(column_name="id"),
            operator=BinaryOperator.EQ,
            right=LiteralNode(value=1, data_type=DataType.INT)
        )
        assignments = {
            "name": LiteralNode(value="Jane", data_type=DataType.TEXT),
            "active": LiteralNode(value=True, data_type=DataType.BOOL)
        }
        node = UpdateNode(table_name="users", assignments=assignments, where=where)
        assert node.table_name == "users"
        assert "name" in node.assignments
        assert node.where == where

    def test_update_node_no_where(self):
        """UpdateNode without WHERE (updates all rows)."""
        assignments = {"active": LiteralNode(value=False, data_type=DataType.BOOL)}
        node = UpdateNode(table_name="users", assignments=assignments)
        assert node.where is None

    def test_delete_node(self):
        """DeleteNode."""
        where = BinaryOpNode(
            left=ColumnRef(column_name="id"),
            operator=BinaryOperator.EQ,
            right=LiteralNode(value=1, data_type=DataType.INT)
        )
        node = DeleteNode(table_name="users", where=where)
        assert node.table_name == "users"
        assert node.where == where

    def test_delete_node_no_where(self):
        """DeleteNode without WHERE (deletes all rows)."""
        node = DeleteNode(table_name="logs")
        assert node.where is None


# =============================================================================
# DDL Node Tests
# =============================================================================

class TestDDLNodes:
    """Tests for DDL nodes."""

    def test_column_def(self):
        """ColumnDef with all fields."""
        node = ColumnDef(
            name="id",
            data_type=DataType.INT,
            nullable=False,
            primary_key=True,
            unique=True
        )
        assert node.name == "id"
        assert node.data_type == DataType.INT
        assert node.nullable is False
        assert node.primary_key is True
        assert node.unique is True
        assert node.default is None

    def test_column_def_with_default(self):
        """ColumnDef with default value."""
        default = LiteralNode(value="guest", data_type=DataType.TEXT)
        node = ColumnDef(name="role", data_type=DataType.TEXT, default=default)
        assert node.default == default

    def test_create_table_node(self):
        """CreateTableNode."""
        columns = [
            ColumnDef(name="id", data_type=DataType.INT, primary_key=True),
            ColumnDef(name="name", data_type=DataType.TEXT, nullable=False),
        ]
        node = CreateTableNode(table_name="users", columns=columns)
        assert node.table_name == "users"
        assert len(node.columns) == 2
        assert node.if_not_exists is False

    def test_create_table_node_if_not_exists(self):
        """CreateTableNode with IF NOT EXISTS."""
        node = CreateTableNode(table_name="users", if_not_exists=True)
        assert node.if_not_exists is True

    def test_create_index_node(self):
        """CreateIndexNode."""
        columns = [ColumnRef(column_name="email")]
        node = CreateIndexNode(
            index_name="idx_email",
            table_name="users",
            columns=columns,
            unique=True
        )
        assert node.index_name == "idx_email"
        assert node.table_name == "users"
        assert node.unique is True
        assert node.if_not_exists is False

    def test_create_index_node_composite(self):
        """CreateIndexNode for composite index."""
        columns = [
            ColumnRef(column_name="last_name"),
            ColumnRef(column_name="first_name"),
        ]
        node = CreateIndexNode(
            index_name="idx_name",
            table_name="users",
            columns=columns
        )
        assert len(node.columns) == 2

    def test_drop_table_node(self):
        """DropTableNode."""
        node = DropTableNode(table_name="users")
        assert node.table_name == "users"
        assert node.if_exists is False

    def test_drop_table_node_if_exists(self):
        """DropTableNode with IF EXISTS."""
        node = DropTableNode(table_name="users", if_exists=True)
        assert node.if_exists is True

    def test_drop_index_node(self):
        """DropIndexNode."""
        node = DropIndexNode(index_name="idx_email")
        assert node.index_name == "idx_email"
        assert node.if_exists is False


# =============================================================================
# Utility Node Tests
# =============================================================================

class TestUtilityNodes:
    """Tests for utility nodes."""

    def test_transaction_node_begin(self):
        """TransactionNode for BEGIN."""
        node = TransactionNode(command="BEGIN")
        assert node.command == "BEGIN"
        assert node.isolation_level is None

    def test_transaction_node_begin_with_isolation(self):
        """TransactionNode for BEGIN with isolation level."""
        node = TransactionNode(command="BEGIN", isolation_level="READ COMMITTED")
        assert node.isolation_level == "READ COMMITTED"

    def test_transaction_node_commit(self):
        """TransactionNode for COMMIT."""
        node = TransactionNode(command="COMMIT")
        assert node.command == "COMMIT"

    def test_transaction_node_rollback(self):
        """TransactionNode for ROLLBACK."""
        node = TransactionNode(command="ROLLBACK")
        assert node.command == "ROLLBACK"

    def test_explain_node(self):
        """ExplainNode."""
        select = SelectNode(columns=[SelectColumn(expression=StarColumn())])
        node = ExplainNode(query=select)
        assert node.query == select
        assert node.analyze is False

    def test_explain_node_analyze(self):
        """ExplainNode with ANALYZE."""
        select = SelectNode(columns=[SelectColumn(expression=StarColumn())])
        node = ExplainNode(query=select, analyze=True)
        assert node.analyze is True


# =============================================================================
# Node Hierarchy Tests
# =============================================================================

class TestNodeHierarchy:
    """Tests for node class hierarchy."""

    def test_expression_nodes_inherit_from_expression_node(self):
        """All expression nodes inherit from ExpressionNode."""
        assert issubclass(LiteralNode, ExpressionNode)
        assert issubclass(ColumnRef, ExpressionNode)
        assert issubclass(BinaryOpNode, ExpressionNode)
        assert issubclass(UnaryOpNode, ExpressionNode)
        assert issubclass(FunctionCall, ExpressionNode)
        assert issubclass(AggregateNode, ExpressionNode)
        assert issubclass(BetweenNode, ExpressionNode)
        assert issubclass(InListNode, ExpressionNode)
        assert issubclass(CaseExpression, ExpressionNode)
        assert issubclass(SubqueryNode, ExpressionNode)
        assert issubclass(ExistsNode, ExpressionNode)
        assert issubclass(StarColumn, ExpressionNode)

    def test_statement_nodes_inherit_from_ast_node(self):
        """All statement nodes inherit from ASTNode."""
        assert issubclass(SelectNode, ASTNode)
        assert issubclass(InsertNode, ASTNode)
        assert issubclass(UpdateNode, ASTNode)
        assert issubclass(DeleteNode, ASTNode)
        assert issubclass(CreateTableNode, ASTNode)
        assert issubclass(CreateIndexNode, ASTNode)
        assert issubclass(DropTableNode, ASTNode)
        assert issubclass(DropIndexNode, ASTNode)
        assert issubclass(TransactionNode, ASTNode)
        assert issubclass(ExplainNode, ASTNode)

    def test_structure_nodes_inherit_from_ast_node(self):
        """All structure nodes inherit from ASTNode."""
        assert issubclass(SelectColumn, ASTNode)
        assert issubclass(JoinClause, ASTNode)
        assert issubclass(FromClause, ASTNode)
        assert issubclass(OrderByItem, ASTNode)
        assert issubclass(TableRef, ASTNode)
        assert issubclass(ColumnDef, ASTNode)


# =============================================================================
# Dataclass Behavior Tests
# =============================================================================

class TestDataclassBehavior:
    """Tests for dataclass behavior of nodes."""

    def test_literal_node_equality(self):
        """LiteralNodes with same values are equal."""
        node1 = LiteralNode(value=42, data_type=DataType.INT)
        node2 = LiteralNode(value=42, data_type=DataType.INT)
        assert node1 == node2

    def test_literal_node_inequality(self):
        """LiteralNodes with different values are not equal."""
        node1 = LiteralNode(value=42, data_type=DataType.INT)
        node2 = LiteralNode(value=43, data_type=DataType.INT)
        assert node1 != node2

    def test_column_ref_equality(self):
        """ColumnRefs with same values are equal."""
        node1 = ColumnRef(column_name="id", table_alias="users")
        node2 = ColumnRef(column_name="id", table_alias="users")
        assert node1 == node2

    def test_binary_op_node_nested_equality(self):
        """BinaryOpNodes with nested nodes are compared correctly."""
        left1 = LiteralNode(value=1, data_type=DataType.INT)
        right1 = LiteralNode(value=2, data_type=DataType.INT)
        node1 = BinaryOpNode(left=left1, operator=BinaryOperator.LT, right=right1)
        
        left2 = LiteralNode(value=1, data_type=DataType.INT)
        right2 = LiteralNode(value=2, data_type=DataType.INT)
        node2 = BinaryOpNode(left=left2, operator=BinaryOperator.LT, right=right2)
        
        assert node1 == node2

    def test_node_repr(self):
        """Nodes have meaningful repr."""
        node = LiteralNode(value=42, data_type=DataType.INT)
        repr_str = repr(node)
        assert "LiteralNode" in repr_str
        assert "42" in repr_str


# =============================================================================
# Run Tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])