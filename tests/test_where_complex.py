# Tests for complex WHERE conditions
# Phase 3: DQL + WHERE - CHECKPOINT #1
# Tests for: (col1 > 10 OR col2 = 'test') AND col3 != true

import pytest

from mini_db.parser.parser import Parser, ParseError
from mini_db.ast.nodes import (
    SelectNode,
    IdentifierNode,
    LiteralNode,
    ComparisonNode,
    LogicalNode,
)
from mini_db.executor.executor import Executor, ExecutionResult
from mini_db.storage.database import Database
from mini_db.ast.nodes import ColumnDef


# START_BLOCK_CHECKPOINT1
class TestCheckpoint1ComplexWhere:
    """
    CHECKPOINT #1: Парсер строит корректное AST для
    (col1 > 10 OR col2 = 'test') AND col3 != true
    """
    
    def test_checkpoint1_parser_ast_structure(self):
        """
        Verify AST structure for:
        (col1 > 10 OR col2 = 'test') AND col3 != true
        """
        parser = Parser()
        query = "SELECT * FROM t WHERE (col1 > 10 OR col2 = 'test') AND col3 != true"
        ast = parser.parse(query)
        
        # Verify SELECT node
        assert isinstance(ast, SelectNode)
        assert ast.table == "t"
        assert ast.columns is None
        
        # Verify WHERE is LogicalNode with AND
        where = ast.where
        assert isinstance(where, LogicalNode)
        assert where.op == "AND"
        
        # Left side: (col1 > 10 OR col2 = 'test')
        left = where.left
        assert isinstance(left, LogicalNode)
        assert left.op == "OR"
        
        # Left of OR: col1 > 10
        left_or = left.left
        assert isinstance(left_or, ComparisonNode)
        assert left_or.op == ">"
        assert isinstance(left_or.left, IdentifierNode)
        assert left_or.left.name == "col1"
        assert isinstance(left_or.right, LiteralNode)
        assert left_or.right.value == 10
        
        # Right of OR: col2 = 'test'
        right_or = left.right
        assert isinstance(right_or, ComparisonNode)
        assert right_or.op == "="
        assert isinstance(right_or.left, IdentifierNode)
        assert right_or.left.name == "col2"
        assert isinstance(right_or.right, LiteralNode)
        assert right_or.right.value == "test"
        
        # Right side of AND: col3 != true
        right = where.right
        assert isinstance(right, ComparisonNode)
        assert right.op == "!="
        assert isinstance(right.left, IdentifierNode)
        assert right.left.name == "col3"
        assert isinstance(right.right, LiteralNode)
        assert right.right.value is True
    
    def test_checkpoint1_executor_evaluation(self):
        """
        Verify executor evaluates complex WHERE correctly.
        """
        db = Database()
        executor = Executor()
        
        # Create table
        db.create_table("t", [
            ColumnDef(name="col1", data_type="INT"),
            ColumnDef(name="col2", data_type="TEXT"),
            ColumnDef(name="col3", data_type="BOOL"),
        ])
        
        # Insert test rows
        table = db.get_table("t")
        table.insert({"col1": 15, "col2": "other", "col3": False})  # Row 1: matches
        table.insert({"col1": 5, "col2": "test", "col3": False})     # Row 2: matches
        table.insert({"col1": 15, "col2": "other", "col3": True})    # Row 3: no match (col3 = true)
        table.insert({"col1": 5, "col2": "other", "col3": False})    # Row 4: no match
        
        # Execute query
        parser = Parser()
        ast = parser.parse(
            "SELECT * FROM t WHERE (col1 > 10 OR col2 = 'test') AND col3 != true"
        )
        result = executor.execute(ast, db)
        
        assert result.success
        assert len(result.data) == 2
# END_BLOCK_CHECKPOINT1


# START_BLOCK_OPERATOR_PRECEDENCE
class TestOperatorPrecedence:
    """Tests for operator precedence: AND before OR."""
    
    def test_and_before_or_no_parens(self):
        """
        a OR b AND c should parse as: a OR (b AND c)
        """
        parser = Parser()
        ast = parser.parse("SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3")
        
        where = ast.where
        assert isinstance(where, LogicalNode)
        assert where.op == "OR"
        
        # Right should be AND due to precedence
        assert isinstance(where.right, LogicalNode)
        assert where.right.op == "AND"
    
    def test_and_before_or_multiple(self):
        """
        a AND b OR c AND d should parse as: (a AND b) OR (c AND d)
        """
        parser = Parser()
        ast = parser.parse("SELECT * FROM t WHERE a = 1 AND b = 2 OR c = 3 AND d = 4")
        
        where = ast.where
        assert isinstance(where, LogicalNode)
        assert where.op == "OR"
        
        # Both sides should be AND
        assert isinstance(where.left, LogicalNode)
        assert where.left.op == "AND"
        assert isinstance(where.right, LogicalNode)
        assert where.right.op == "AND"
    
    def test_parens_override_precedence(self):
        """
        (a OR b) AND c should parse as: (a OR b) AND c
        """
        parser = Parser()
        ast = parser.parse("SELECT * FROM t WHERE (a = 1 OR b = 2) AND c = 3")
        
        where = ast.where
        assert isinstance(where, LogicalNode)
        assert where.op == "AND"
        
        # Left should be OR due to parens
        assert isinstance(where.left, LogicalNode)
        assert where.left.op == "OR"
# END_BLOCK_OPERATOR_PRECEDENCE


# START_BLOCK_COMPLEX_EXPRESSIONS
class TestComplexExpressions:
    """Tests for deeply nested expressions."""
    
    def test_deeply_nested_or(self):
        """a OR b OR c OR d"""
        parser = Parser()
        ast = parser.parse("SELECT * FROM t WHERE a = 1 OR b = 2 OR c = 3 OR d = 4")
        
        where = ast.where
        assert isinstance(where, LogicalNode)
        assert where.op == "OR"
        
        # Left should be nested OR
        assert isinstance(where.left, LogicalNode)
        assert where.left.op == "OR"
    
    def test_deeply_nested_and(self):
        """a AND b AND c AND d"""
        parser = Parser()
        ast = parser.parse("SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3 AND d = 4")
        
        where = ast.where
        assert isinstance(where, LogicalNode)
        assert where.op == "AND"
    
    def test_mixed_nested(self):
        """((a AND b) OR (c AND d)) AND e"""
        parser = Parser()
        ast = parser.parse(
            "SELECT * FROM t WHERE (a = 1 AND b = 2 OR c = 3 AND d = 4) AND e = 5"
        )
        
        where = ast.where
        assert isinstance(where, LogicalNode)
        assert where.op == "AND"
        
        # Left should be OR
        assert isinstance(where.left, LogicalNode)
        assert where.left.op == "OR"
    
    def test_all_comparison_operators(self):
        """Test all comparison operators in one query."""
        parser = Parser()
        ast = parser.parse(
            "SELECT * FROM t WHERE a = 1 AND b != 2 AND c < 3 AND d > 4"
        )
        
        where = ast.where
        assert isinstance(where, LogicalNode)
        
        # Traverse and verify all comparisons
        comparisons = []
        node = where
        while isinstance(node, LogicalNode):
            comparisons.append(node.right)
            node = node.left
        comparisons.append(node)
        
        ops = [c.op for c in comparisons if isinstance(c, ComparisonNode)]
        assert "=" in ops
        assert "!=" in ops
        assert "<" in ops
        assert ">" in ops
# END_BLOCK_COMPLEX_EXPRESSIONS


# START_BLOCK_EXECUTION_COMPLEX
class TestComplexWhereExecution:
    """Tests for executing complex WHERE queries."""
    
    @pytest.fixture
    def setup_db(self):
        """Create test database with sample data."""
        db = Database()
        db.create_table("products", [
            ColumnDef(name="id", data_type="INT"),
            ColumnDef(name="name", data_type="TEXT"),
            ColumnDef(name="price", data_type="INT"),
            ColumnDef(name="stock", data_type="INT"),
            ColumnDef(name="active", data_type="BOOL"),
        ])
        
        table = db.get_table("products")
        table.insert({"id": 1, "name": "Widget", "price": 100, "stock": 50, "active": True})
        table.insert({"id": 2, "name": "Gadget", "price": 200, "stock": 5, "active": True})
        table.insert({"id": 3, "name": "Doohickey", "price": 50, "stock": 100, "active": False})
        table.insert({"id": 4, "name": "Thingamajig", "price": 150, "stock": 0, "active": True})
        table.insert({"id": 5, "name": "Whatchamacallit", "price": 300, "stock": 25, "active": False})
        
        return db
    
    def test_complex_query_1(self, setup_db):
        """price > 100 AND stock > 0"""
        db = setup_db
        parser = Parser()
        executor = Executor()
        
        ast = parser.parse("SELECT * FROM products WHERE price > 100 AND stock > 0")
        result = executor.execute(ast, db)
        
        assert result.success
        assert len(result.data) == 2  # Gadget, Whatchamacallit
    
    def test_complex_query_2(self, setup_db):
        """(price < 100 OR price > 200) AND active = true"""
        db = setup_db
        parser = Parser()
        executor = Executor()
        
        ast = parser.parse(
            "SELECT * FROM products WHERE (price < 100 OR price > 200) AND active = true"
        )
        result = executor.execute(ast, db)
        
        assert result.success
        assert len(result.data) == 0  # Doohickey is inactive, Whatchamacallit is inactive
    
    def test_complex_query_3(self, setup_db):
        """active = true AND (stock = 0 OR price > 150)"""
        db = setup_db
        parser = Parser()
        executor = Executor()
        
        ast = parser.parse(
            "SELECT * FROM products WHERE active = true AND (stock = 0 OR price > 150)"
        )
        result = executor.execute(ast, db)
        
        assert result.success
        assert len(result.data) == 2  # Gadget, Thingamajig
    
    def test_or_only(self, setup_db):
        """price < 100 OR stock > 50"""
        db = setup_db
        parser = Parser()
        executor = Executor()
        
        ast = parser.parse("SELECT * FROM products WHERE price < 100 OR stock > 50")
        result = executor.execute(ast, db)
        
        assert result.success
        # Widget: price=100 (not < 100), stock=50 (not > 50) -> False
        # Doohickey: price=50 (< 100) -> True
        assert len(result.data) == 1  # Only Doohickey matches
# END_BLOCK_EXECUTION_COMPLEX