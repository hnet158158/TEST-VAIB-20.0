# START_MODULE_CONTRACT
# Module: tests.test_ast
# Intent: Unit tests для AST nodes - проверка структуры и атрибутов.
# END_MODULE_CONTRACT

import unittest

from mini_db.ast.nodes import (
    ASTNode,
    StatementNode,
    ExpressionNode,
    ColumnDef,
    CreateTableNode,
    InsertNode,
    UpdateNode,
    DeleteNode,
    SelectNode,
    CreateIndexNode,
    SaveNode,
    LoadNode,
    ExitNode,
    ComparisonNode,
    LogicalNode,
    IdentifierNode,
    LiteralNode,
)


class TestColumnDef(unittest.TestCase):
    """Тесты ColumnDef"""
    
    def test_column_def_basic(self):
        col = ColumnDef(name="id", data_type="INT")
        self.assertEqual(col.name, "id")
        self.assertEqual(col.data_type, "INT")
        self.assertEqual(col.unique, False)
    
    def test_column_def_unique(self):
        col = ColumnDef(name="email", data_type="TEXT", unique=True)
        self.assertEqual(col.name, "email")
        self.assertEqual(col.data_type, "TEXT")
        self.assertEqual(col.unique, True)
    
    def test_column_def_types(self):
        int_col = ColumnDef(name="count", data_type="INT")
        text_col = ColumnDef(name="name", data_type="TEXT")
        bool_col = ColumnDef(name="active", data_type="BOOL")
        
        self.assertEqual(int_col.data_type, "INT")
        self.assertEqual(text_col.data_type, "TEXT")
        self.assertEqual(bool_col.data_type, "BOOL")


class TestASTNodeHierarchy(unittest.TestCase):
    """Тесты иерархии AST узлов"""
    
    def test_statement_node_is_ast_node(self):
        self.assertTrue(issubclass(StatementNode, ASTNode))
    
    def test_expression_node_is_ast_node(self):
        self.assertTrue(issubclass(ExpressionNode, ASTNode))
    
    def test_create_table_is_statement(self):
        self.assertTrue(issubclass(CreateTableNode, StatementNode))
    
    def test_insert_is_statement(self):
        self.assertTrue(issubclass(InsertNode, StatementNode))
    
    def test_update_is_statement(self):
        self.assertTrue(issubclass(UpdateNode, StatementNode))
    
    def test_delete_is_statement(self):
        self.assertTrue(issubclass(DeleteNode, StatementNode))
    
    def test_select_is_statement(self):
        self.assertTrue(issubclass(SelectNode, StatementNode))
    
    def test_comparison_is_expression(self):
        self.assertTrue(issubclass(ComparisonNode, ExpressionNode))
    
    def test_logical_is_expression(self):
        self.assertTrue(issubclass(LogicalNode, ExpressionNode))
    
    def test_identifier_is_expression(self):
        self.assertTrue(issubclass(IdentifierNode, ExpressionNode))
    
    def test_literal_is_expression(self):
        self.assertTrue(issubclass(LiteralNode, ExpressionNode))


class TestDDLNodes(unittest.TestCase):
    """Тесты DDL узлов"""
    
    def test_create_table_node(self):
        columns = [
            ColumnDef(name="id", data_type="INT", unique=True),
            ColumnDef(name="name", data_type="TEXT"),
        ]
        node = CreateTableNode(name="users", columns=columns)
        
        self.assertEqual(node.name, "users")
        self.assertEqual(len(node.columns), 2)
        self.assertEqual(node.columns[0].name, "id")
        self.assertEqual(node.columns[0].unique, True)
        self.assertEqual(node.columns[1].name, "name")
        self.assertEqual(node.columns[1].unique, False)
    
    def test_create_index_node(self):
        node = CreateIndexNode(name="idx_email", table="users", column="email")
        
        self.assertEqual(node.name, "idx_email")
        self.assertEqual(node.table, "users")
        self.assertEqual(node.column, "email")


class TestDMLNodes(unittest.TestCase):
    """Тесты DML узлов"""
    
    def test_insert_node(self):
        node = InsertNode(
            table="users",
            columns=["id", "name"],
            values=[1, "John"]
        )
        
        self.assertEqual(node.table, "users")
        self.assertEqual(node.columns, ["id", "name"])
        self.assertEqual(node.values, [1, "John"])
    
    def test_update_node_with_where(self):
        where = ComparisonNode(
            left=IdentifierNode(name="id"),
            op="=",
            right=LiteralNode(value=1)
        )
        node = UpdateNode(
            table="users",
            assignments={"name": "Jane"},
            where=where
        )
        
        self.assertEqual(node.table, "users")
        self.assertEqual(node.assignments, {"name": "Jane"})
        self.assertIsNotNone(node.where)
        self.assertIsInstance(node.where, ComparisonNode)
    
    def test_update_node_without_where(self):
        node = UpdateNode(table="users", assignments={"active": True})
        
        self.assertEqual(node.table, "users")
        self.assertEqual(node.assignments, {"active": True})
        self.assertIsNone(node.where)
    
    def test_delete_node_with_where(self):
        where = ComparisonNode(
            left=IdentifierNode(name="id"),
            op="=",
            right=LiteralNode(value=1)
        )
        node = DeleteNode(table="users", where=where)
        
        self.assertEqual(node.table, "users")
        self.assertIsNotNone(node.where)
    
    def test_delete_node_without_where(self):
        node = DeleteNode(table="users")
        
        self.assertEqual(node.table, "users")
        self.assertIsNone(node.where)
    
    def test_select_node_all_columns(self):
        node = SelectNode(table="users")
        
        self.assertEqual(node.table, "users")
        self.assertIsNone(node.columns)  # SELECT *
        self.assertIsNone(node.where)
    
    def test_select_node_specific_columns(self):
        node = SelectNode(table="users", columns=["id", "name"])
        
        self.assertEqual(node.table, "users")
        self.assertEqual(node.columns, ["id", "name"])
    
    def test_select_node_with_where(self):
        where = ComparisonNode(
            left=IdentifierNode(name="id"),
            op=">",
            right=LiteralNode(value=10)
        )
        node = SelectNode(table="users", columns=["id"], where=where)
        
        self.assertEqual(node.table, "users")
        self.assertEqual(node.columns, ["id"])
        self.assertIsNotNone(node.where)


class TestSystemNodes(unittest.TestCase):
    """Тесты системных узлов"""
    
    def test_save_node(self):
        node = SaveNode(filepath="/path/to/db.json")
        
        self.assertEqual(node.filepath, "/path/to/db.json")
    
    def test_load_node(self):
        node = LoadNode(filepath="/path/to/db.json")
        
        self.assertEqual(node.filepath, "/path/to/db.json")
    
    def test_exit_node(self):
        node = ExitNode()
        
        # ExitNode has no attributes
        self.assertIsInstance(node, StatementNode)


class TestExpressionNodes(unittest.TestCase):
    """Тесты узлов выражений"""
    
    def test_comparison_node(self):
        left = IdentifierNode(name="id")
        right = LiteralNode(value=10)
        node = ComparisonNode(left=left, op=">", right=right)
        
        self.assertIsInstance(node.left, IdentifierNode)
        self.assertEqual(node.op, ">")
        self.assertIsInstance(node.right, LiteralNode)
        self.assertEqual(node.right.value, 10)
    
    def test_comparison_operators(self):
        ops = ["=", "!=", "<", ">"]
        for op in ops:
            node = ComparisonNode(
                left=IdentifierNode(name="col"),
                op=op,
                right=LiteralNode(value=1)
            )
            self.assertEqual(node.op, op)
    
    def test_logical_node_and(self):
        left = ComparisonNode(
            left=IdentifierNode(name="a"),
            op="=",
            right=LiteralNode(value=1)
        )
        right = ComparisonNode(
            left=IdentifierNode(name="b"),
            op="=",
            right=LiteralNode(value=2)
        )
        node = LogicalNode(left=left, op="AND", right=right)
        
        self.assertEqual(node.op, "AND")
        self.assertIsInstance(node.left, ComparisonNode)
        self.assertIsInstance(node.right, ComparisonNode)
    
    def test_logical_node_or(self):
        node = LogicalNode(
            left=IdentifierNode(name="a"),
            op="OR",
            right=IdentifierNode(name="b")
        )
        
        self.assertEqual(node.op, "OR")
    
    def test_identifier_node(self):
        node = IdentifierNode(name="column_name")
        
        self.assertEqual(node.name, "column_name")
    
    def test_literal_node_int(self):
        node = LiteralNode(value=42)
        
        self.assertEqual(node.value, 42)
        self.assertIsInstance(node.value, int)
    
    def test_literal_node_string(self):
        node = LiteralNode(value="hello")
        
        self.assertEqual(node.value, "hello")
        self.assertIsInstance(node.value, str)
    
    def test_literal_node_bool(self):
        node_true = LiteralNode(value=True)
        node_false = LiteralNode(value=False)
        
        self.assertEqual(node_true.value, True)
        self.assertEqual(node_false.value, False)
    
    def test_literal_node_null(self):
        node = LiteralNode(value=None)
        
        self.assertIsNone(node.value)


class TestNestedExpressions(unittest.TestCase):
    """Тесты вложенных выражений"""
    
    def test_complex_where(self):
        # (col1 > 10 OR col2 = 'test') AND col3 != true
        or_expr = LogicalNode(
            left=ComparisonNode(
                left=IdentifierNode(name="col1"),
                op=">",
                right=LiteralNode(value=10)
            ),
            op="OR",
            right=ComparisonNode(
                left=IdentifierNode(name="col2"),
                op="=",
                right=LiteralNode(value="test")
            )
        )
        
        and_expr = LogicalNode(
            left=or_expr,
            op="AND",
            right=ComparisonNode(
                left=IdentifierNode(name="col3"),
                op="!=",
                right=LiteralNode(value=True)
            )
        )
        
        # Verify structure
        self.assertEqual(and_expr.op, "AND")
        self.assertIsInstance(and_expr.left, LogicalNode)
        self.assertEqual(and_expr.left.op, "OR")
    
    def test_deeply_nested(self):
        # a = 1 AND (b = 2 OR (c = 3 AND d = 4))
        inner_and = LogicalNode(
            left=ComparisonNode(
                left=IdentifierNode(name="c"),
                op="=",
                right=LiteralNode(value=3)
            ),
            op="AND",
            right=ComparisonNode(
                left=IdentifierNode(name="d"),
                op="=",
                right=LiteralNode(value=4)
            )
        )
        
        or_expr = LogicalNode(
            left=ComparisonNode(
                left=IdentifierNode(name="b"),
                op="=",
                right=LiteralNode(value=2)
            ),
            op="OR",
            right=inner_and
        )
        
        outer_and = LogicalNode(
            left=ComparisonNode(
                left=IdentifierNode(name="a"),
                op="=",
                right=LiteralNode(value=1)
            ),
            op="AND",
            right=or_expr
        )
        
        # Verify deep nesting
        self.assertIsInstance(outer_and.right, LogicalNode)
        self.assertIsInstance(outer_and.right.right, LogicalNode)


if __name__ == "__main__":
    unittest.main()