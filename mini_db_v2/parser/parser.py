# START_MODULE_CONTRACT
# Module: mini_db_v2.parser.parser
# Intent: Recursive descent SQL parser для DDL и DML команд.
# Dependencies: typing, mini_db_v2.parser.lexer, mini_db_v2.ast.nodes
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: Parser, ParseError, parse_sql
# END_MODULE_MAP

from __future__ import annotations
from typing import Optional, Union
from mini_db_v2.parser.lexer import Token, TokenType, Lexer
from mini_db_v2.ast.nodes import (
    ASTNode, SelectNode, InsertNode, UpdateNode, DeleteNode,
    CreateTableNode, CreateIndexNode, DropTableNode, DropIndexNode,
    AnalyzeTableNode, ExplainNode, TransactionNode,
    ColumnDef, ColumnRef, TableRef, FromClause, JoinClause,
    SelectColumn, OrderByItem, ExpressionNode, LiteralNode,
    BinaryOpNode, UnaryOpNode, BinaryOperator, UnaryOperator,
    DataType, JoinType, StarColumn, BetweenNode, InListNode,
    AggregateNode, AggregateType, SubqueryNode, ExistsNode,
    CaseExpression, FunctionCall
)


# =============================================================================
# START_BLOCK_ERRORS
# =============================================================================

class ParseError(Exception):
    """
    [START_CONTRACT_PARSE_ERROR]
    Intent: Ошибка парсинга SQL с позицией.
    Input: message - описание; token - токен на котором произошла ошибка.
    Output: Исключение для обработки.
    [END_CONTRACT_PARSE_ERROR]
    """
    def __init__(self, message: str, token: Optional[Token] = None):
        if token:
            msg = f"Parse error at line {token.line}, column {token.column}: {message}"
        else:
            msg = f"Parse error: {message}"
        super().__init__(msg)
        self.token = token

# END_BLOCK_ERRORS


# =============================================================================
# START_BLOCK_PARSER
# =============================================================================

class Parser:
    """
    [START_CONTRACT_PARSER]
    Intent: Recursive descent SQL parser для всех типов команд.
    Input: SQL строка или список токенов.
    Output: AST дерево для выполнения.
    [END_CONTRACT_PARSER]
    """
    
    def __init__(self, tokens: Optional[list[Token]] = None, sql: Optional[str] = None):
        """
        [START_CONTRACT_PARSER_INIT]
        Intent: Инициализация парсера с токенами или SQL строкой.
        Input: tokens - список токенов; sql - SQL строка (альтернатива).
        Output: Готовый к парсингу парсер.
        [END_CONTRACT_PARSER_INIT]
        """
        if tokens is not None:
            self.tokens = tokens
        elif sql is not None:
            self.tokens = Lexer(sql).tokenize()
        else:
            self.tokens = []
        
        self.pos = 0
    
    @property
    def current(self) -> Token:
        """Возвращает текущий токен."""
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return self.tokens[-1]  # EOF
    
    @property
    def peek(self) -> Optional[Token]:
        """Смотрит на следующий токен."""
        if self.pos + 1 < len(self.tokens):
            return self.tokens[self.pos + 1]
        return None
    
    def advance(self) -> Token:
        """Переходит к следующему токену, возвращает текущий."""
        token = self.current
        self.pos += 1
        return token
    
    def expect(self, token_type: TokenType) -> Token:
        """Ожидает токен определённого типа."""
        if self.current.type != token_type:
            raise ParseError(
                f"Expected {token_type.name}, got {self.current.type.name}",
                self.current
            )
        return self.advance()
    
    def match(self, *token_types: TokenType) -> bool:
        """Проверяет, соответствует ли текущий токен одному из типов."""
        return self.current.type in token_types
    
    def parse(self) -> ASTNode:
        """
        [START_CONTRACT_PARSE]
        Intent: Парсит SQL команду и возвращает AST.
        Input: Токены переданные в __init__.
        Output: ASTNode соответствующего типа.
        [END_CONTRACT_PARSE]
        """
        if self.match(TokenType.SELECT):
            ast = self.parse_select()
        elif self.match(TokenType.INSERT):
            ast = self.parse_insert()
        elif self.match(TokenType.UPDATE):
            ast = self.parse_update()
        elif self.match(TokenType.DELETE):
            ast = self.parse_delete()
        elif self.match(TokenType.CREATE):
            ast = self.parse_create()
        elif self.match(TokenType.DROP):
            ast = self.parse_drop()
        elif self.match(TokenType.ANALYZE):
            ast = self.parse_analyze_table()
        elif self.match(TokenType.EXPLAIN):
            ast = self.parse_explain()
        elif self.match(TokenType.BEGIN):
            ast = self.parse_begin()
        elif self.match(TokenType.COMMIT):
            ast = self.parse_commit()
        elif self.match(TokenType.ROLLBACK):
            ast = self.parse_rollback()
        else:
            raise ParseError(f"Unexpected token: {self.current.value}", self.current)
        
        # Check for unexpected tokens after valid command
        if not self.match(TokenType.EOF):
            raise ParseError(f"Unexpected token: {self.current.value}", self.current)
        
        return ast
    
    # =========================================================================
    # SELECT
    # =========================================================================
    
    def parse_select(self) -> SelectNode:
        """Парсит SELECT запрос."""
        self.expect(TokenType.SELECT)
        
        # DISTINCT
        distinct = False
        if self.match(TokenType.DISTINCT):
            self.advance()
            distinct = True
        
        # Columns
        columns = self.parse_select_columns()
        
        # FROM
        from_clause = None
        if self.match(TokenType.FROM):
            self.advance()
            from_clause = self.parse_from_clause()
        
        # WHERE
        where = None
        if self.match(TokenType.WHERE):
            self.advance()
            where = self.parse_expression()
        
        # GROUP BY
        group_by = []
        if self.match(TokenType.GROUP):
            self.advance()
            self.expect(TokenType.BY)
            group_by = self.parse_column_refs()
        
        # HAVING
        having = None
        if self.match(TokenType.HAVING):
            self.advance()
            having = self.parse_expression()
        
        # ORDER BY
        order_by = []
        if self.match(TokenType.ORDER):
            self.advance()
            self.expect(TokenType.BY)
            order_by = self.parse_order_by()
        
        # LIMIT
        limit = None
        if self.match(TokenType.LIMIT):
            self.advance()
            limit = int(self.expect(TokenType.INTEGER).value)
        
        # OFFSET
        offset = None
        if self.match(TokenType.OFFSET):
            self.advance()
            offset = int(self.expect(TokenType.INTEGER).value)
        
        return SelectNode(
            columns=columns,
            from_clause=from_clause,
            where=where,
            group_by=group_by,
            having=having,
            order_by=order_by,
            limit=limit,
            offset=offset,
            distinct=distinct
        )
    
    def parse_select_columns(self) -> list[SelectColumn]:
        """Парсит список колонок в SELECT."""
        columns = []
        
        while True:
            if self.match(TokenType.STAR):
                self.advance()
                columns.append(SelectColumn(expression=StarColumn()))
            elif self.match(TokenType.IDENTIFIER) and self.peek and self.peek.type == TokenType.DOT:
                # table.* or table.column
                table = self.advance().value
                self.expect(TokenType.DOT)
                if self.match(TokenType.STAR):
                    self.advance()
                    columns.append(SelectColumn(expression=StarColumn(table_alias=table)))
                else:
                    col_name = self.expect(TokenType.IDENTIFIER).value
                    alias = self._parse_optional_alias()
                    columns.append(SelectColumn(
                        expression=ColumnRef(column_name=col_name, table_alias=table),
                        alias=alias
                    ))
            else:
                expr = self.parse_expression()
                alias = self._parse_optional_alias()
                columns.append(SelectColumn(expression=expr, alias=alias))
            
            if not self.match(TokenType.COMMA):
                break
            self.advance()
        
        return columns
    
    def _parse_optional_alias(self) -> Optional[str]:
        """Парсит опциональный алиас колонки."""
        if self.match(TokenType.AS):
            self.advance()
            return self.expect(TokenType.IDENTIFIER).value
        elif self.match(TokenType.IDENTIFIER):
            # Alias without AS
            next_types = {TokenType.COMMA, TokenType.FROM, TokenType.WHERE,
                          TokenType.GROUP, TokenType.ORDER, TokenType.LIMIT,
                          TokenType.EOF, TokenType.RPAREN}
            if self.peek and self.peek.type in next_types:
                return self.advance().value
        return None
    
    def parse_from_clause(self) -> FromClause:
        """
        [START_CONTRACT_PARSE_FROM_CLAUSE]
        Intent: Парсит FROM предложение с JOINs и implicit join syntax.
        Input: FROM table1 [alias] [, table2 [alias] ...] [JOIN ...].
        Output: FromClause с таблицей и списком JOINs.
        Note: Поддерживает implicit join: FROM t1, t2 WHERE t1.id = t2.id
        [END_CONTRACT_PARSE_FROM_CLAUSE]
        """
        table = self.parse_table_ref()
        joins = []
        
        # Handle implicit join syntax: FROM t1, t2, t3
        while self.match(TokenType.COMMA):
            self.advance()
            # Parse next table as CROSS JOIN (will be converted to INNER JOIN
            # if WHERE contains join condition)
            next_table = self.parse_table_ref()
            joins.append(JoinClause(
                join_type=JoinType.CROSS,  # Will be optimized later
                table=next_table,
                condition=None
            ))
        
        # Handle explicit JOIN syntax
        while self.match(TokenType.JOIN, TokenType.INNER, TokenType.LEFT,
                         TokenType.RIGHT, TokenType.FULL, TokenType.CROSS):
            joins.append(self.parse_join())
        
        return FromClause(table=table, joins=joins)
    
    def parse_table_ref(self) -> TableRef:
        """Парсит ссылку на таблицу."""
        name = self.expect(TokenType.IDENTIFIER).value
        alias = None
        
        if self.match(TokenType.AS):
            self.advance()
            alias = self.expect(TokenType.IDENTIFIER).value
        elif self.match(TokenType.IDENTIFIER):
            # Alias without AS - check if it's not a keyword
            if self.current.type == TokenType.IDENTIFIER:
                alias = self.advance().value
        
        return TableRef(table_name=name, alias=alias)
    
    def parse_join(self) -> JoinClause:
        """Парсит JOIN предложение."""
        join_type = JoinType.INNER
        
        if self.match(TokenType.INNER):
            self.advance()
            join_type = JoinType.INNER
        elif self.match(TokenType.LEFT):
            self.advance()
            if self.match(TokenType.OUTER):
                self.advance()
            join_type = JoinType.LEFT
        elif self.match(TokenType.RIGHT):
            self.advance()
            if self.match(TokenType.OUTER):
                self.advance()
            join_type = JoinType.RIGHT
        elif self.match(TokenType.FULL):
            self.advance()
            if self.match(TokenType.OUTER):
                self.advance()
            join_type = JoinType.FULL
        elif self.match(TokenType.CROSS):
            self.advance()
            join_type = JoinType.CROSS
        
        self.expect(TokenType.JOIN)
        table = self.parse_table_ref()
        
        condition = None
        if self.match(TokenType.ON):
            self.advance()
            condition = self.parse_expression()
        
        return JoinClause(join_type=join_type, table=table, condition=condition)
    
    def parse_column_refs(self) -> list[ColumnRef]:
        """Парсит список ссылок на колонки."""
        refs = []
        while True:
            refs.append(self.parse_column_ref())
            if not self.match(TokenType.COMMA):
                break
            self.advance()
        return refs
    
    def parse_column_ref(self) -> ColumnRef:
        """Парсит ссылку на колонку."""
        name = self.expect(TokenType.IDENTIFIER).value
        table_alias = None
        
        if self.match(TokenType.DOT):
            self.advance()
            table_alias = name
            name = self.expect(TokenType.IDENTIFIER).value
        
        return ColumnRef(column_name=name, table_alias=table_alias)
    
    def parse_order_by(self) -> list[OrderByItem]:
        """Парсит ORDER BY предложение."""
        items = []
        while True:
            expr = self.parse_expression()
            ascending = True
            
            if self.match(TokenType.ASC):
                self.advance()
            elif self.match(TokenType.DESC):
                self.advance()
                ascending = False
            
            items.append(OrderByItem(expression=expr, ascending=ascending))
            
            if not self.match(TokenType.COMMA):
                break
            self.advance()
        
        return items
    
    # =========================================================================
    # INSERT
    # =========================================================================
    
    def parse_insert(self) -> InsertNode:
        """Парсит INSERT запрос."""
        self.expect(TokenType.INSERT)
        self.expect(TokenType.INTO)
        
        table_name = self.expect(TokenType.IDENTIFIER).value
        
        # Columns
        columns = []
        if self.match(TokenType.LPAREN):
            self.advance()
            while not self.match(TokenType.RPAREN):
                columns.append(self.expect(TokenType.IDENTIFIER).value)
                if not self.match(TokenType.COMMA):
                    break
                self.advance()
            self.expect(TokenType.RPAREN)
        
        # VALUES
        values = []
        if self.match(TokenType.VALUES):
            self.advance()
            values = self.parse_values_list()
        
        return InsertNode(table_name=table_name, columns=columns, values=values)
    
    def parse_values_list(self) -> list[list[ExpressionNode]]:
        """Парсит список VALUES."""
        values = []
        while True:
            self.expect(TokenType.LPAREN)
            row_values = []
            while not self.match(TokenType.RPAREN):
                row_values.append(self.parse_expression())
                if not self.match(TokenType.COMMA):
                    break
                self.advance()
            self.expect(TokenType.RPAREN)
            values.append(row_values)
            
            if not self.match(TokenType.COMMA):
                break
            self.advance()
        
        return values
    
    # =========================================================================
    # UPDATE
    # =========================================================================
    
    def parse_update(self) -> UpdateNode:
        """Парсит UPDATE запрос."""
        self.expect(TokenType.UPDATE)
        table_name = self.expect(TokenType.IDENTIFIER).value
        self.expect(TokenType.SET)
        
        # Assignments
        assignments = {}
        while True:
            col = self.expect(TokenType.IDENTIFIER).value
            self.expect(TokenType.EQ)
            expr = self.parse_expression()
            assignments[col] = expr
            
            if not self.match(TokenType.COMMA):
                break
            self.advance()
        
        # WHERE
        where = None
        if self.match(TokenType.WHERE):
            self.advance()
            where = self.parse_expression()
        
        return UpdateNode(table_name=table_name, assignments=assignments, where=where)
    
    # =========================================================================
    # DELETE
    # =========================================================================
    
    def parse_delete(self) -> DeleteNode:
        """Парсит DELETE запрос."""
        self.expect(TokenType.DELETE)
        self.expect(TokenType.FROM)
        
        table_name = self.expect(TokenType.IDENTIFIER).value
        
        where = None
        if self.match(TokenType.WHERE):
            self.advance()
            where = self.parse_expression()
        
        return DeleteNode(table_name=table_name, where=where)
    
    # =========================================================================
    # CREATE
    # =========================================================================
    
    def parse_create(self) -> Union[CreateTableNode, CreateIndexNode]:
        """Парсит CREATE TABLE или CREATE INDEX."""
        self.expect(TokenType.CREATE)
        
        if self.match(TokenType.TABLE):
            return self.parse_create_table()
        elif self.match(TokenType.INDEX):
            return self.parse_create_index()
        elif self.match(TokenType.UNIQUE):
            self.advance()
            # INDEX will be consumed by parse_create_index
            return self.parse_create_index(unique=True)
        else:
            raise ParseError(f"Expected TABLE or INDEX, got {self.current.value}", self.current)
    
    def parse_create_table(self) -> CreateTableNode:
        """Парсит CREATE TABLE."""
        self.expect(TokenType.TABLE)
        
        if_not_exists = False
        if self.match(TokenType.IF):
            self.advance()
            self.expect(TokenType.NOT)
            self.expect(TokenType.EXISTS)
            if_not_exists = True
        
        table_name = self.expect(TokenType.IDENTIFIER).value
        self.expect(TokenType.LPAREN)
        
        columns = []
        while not self.match(TokenType.RPAREN):
            columns.append(self.parse_column_def())
            if not self.match(TokenType.COMMA):
                break
            self.advance()
        
        self.expect(TokenType.RPAREN)
        
        return CreateTableNode(table_name=table_name, columns=columns, if_not_exists=if_not_exists)
    
    def parse_column_def(self) -> ColumnDef:
        """Парсит определение колонки."""
        name = self.expect(TokenType.IDENTIFIER).value
        data_type = self.parse_data_type()
        
        nullable = True
        primary_key = False
        unique = False
        
        while self.match(TokenType.PRIMARY, TokenType.UNIQUE, TokenType.NOT, TokenType.NULL):
            if self.match(TokenType.PRIMARY):
                self.advance()
                self.expect(TokenType.KEY)
                primary_key = True
                nullable = False
            elif self.match(TokenType.UNIQUE):
                self.advance()
                unique = True
            elif self.match(TokenType.NOT):
                self.advance()
                self.expect(TokenType.NULL)
                nullable = False
            elif self.match(TokenType.NULL):
                self.advance()
                nullable = True
        
        return ColumnDef(
            name=name,
            data_type=data_type,
            nullable=nullable,
            primary_key=primary_key,
            unique=unique
        )
    
    def parse_data_type(self) -> DataType:
        """Парсит тип данных."""
        if self.match(TokenType.INT):
            self.advance()
            return DataType.INT
        elif self.match(TokenType.TEXT):
            self.advance()
            return DataType.TEXT
        elif self.match(TokenType.REAL_KW):
            self.advance()
            return DataType.REAL
        elif self.match(TokenType.BOOL):
            self.advance()
            return DataType.BOOL
        else:
            raise ParseError(f"Expected data type, got {self.current.value}", self.current)
    
    def parse_create_index(self, unique: bool = False) -> CreateIndexNode:
        """Парсит CREATE INDEX."""
        self.expect(TokenType.INDEX)
        
        if_not_exists = False
        if self.match(TokenType.IF):
            self.advance()
            self.expect(TokenType.NOT)
            self.expect(TokenType.EXISTS)
            if_not_exists = True
        
        index_name = self.expect(TokenType.IDENTIFIER).value
        self.expect(TokenType.ON)
        table_name = self.expect(TokenType.IDENTIFIER).value
        self.expect(TokenType.LPAREN)
        
        columns = []
        while not self.match(TokenType.RPAREN):
            columns.append(self.parse_column_ref())
            if not self.match(TokenType.COMMA):
                break
            self.advance()
        
        self.expect(TokenType.RPAREN)
        
        return CreateIndexNode(
            index_name=index_name,
            table_name=table_name,
            columns=columns,
            unique=unique,
            if_not_exists=if_not_exists
        )
    
    # =========================================================================
    # DROP
    # =========================================================================
    
    def parse_drop(self) -> Union[DropTableNode, DropIndexNode]:
        """Парсит DROP TABLE или DROP INDEX."""
        self.expect(TokenType.DROP)
        
        if self.match(TokenType.TABLE):
            return self.parse_drop_table()
        elif self.match(TokenType.INDEX):
            return self.parse_drop_index()
        else:
            raise ParseError(f"Expected TABLE or INDEX, got {self.current.value}", self.current)
    
    def parse_drop_table(self) -> DropTableNode:
        """Парсит DROP TABLE."""
        self.expect(TokenType.TABLE)
        
        if_exists = False
        if self.match(TokenType.IF):
            self.advance()
            self.expect(TokenType.EXISTS)
            if_exists = True
        
        table_name = self.expect(TokenType.IDENTIFIER).value
        
        return DropTableNode(table_name=table_name, if_exists=if_exists)
    
    def parse_drop_index(self) -> DropIndexNode:
        """Парсит DROP INDEX."""
        self.expect(TokenType.INDEX)
        
        if_exists = False
        if self.match(TokenType.IF):
            self.advance()
            self.expect(TokenType.EXISTS)
            if_exists = True
        
        index_name = self.expect(TokenType.IDENTIFIER).value
        
        return DropIndexNode(index_name=index_name, if_exists=if_exists)
    
    # =========================================================================
    # ANALYZE TABLE
    # =========================================================================
    
    def parse_analyze_table(self) -> AnalyzeTableNode:
        """Парсит ANALYZE TABLE."""
        self.expect(TokenType.ANALYZE)
        self.expect(TokenType.TABLE)
        
        table_name = self.expect(TokenType.IDENTIFIER).value
        
        return AnalyzeTableNode(table_name=table_name)
    
    # =========================================================================
    # EXPLAIN
    # =========================================================================
    
    def parse_explain(self) -> ExplainNode:
        """
        Парсит EXPLAIN [ANALYZE] SELECT ...
        
        EXPLAIN SELECT * FROM t1 JOIN t2 ON t1.id = t2.id;
        EXPLAIN ANALYZE SELECT * FROM t1 JOIN t2 ON t1.id = t2.id;
        """
        self.expect(TokenType.EXPLAIN)
        
        # Check for ANALYZE keyword
        analyze = False
        if self.match(TokenType.ANALYZE):
            self.advance()
            analyze = True
        
        # Parse the query (only SELECT supported for now)
        if not self.match(TokenType.SELECT):
            raise ParseError(
                f"EXPLAIN only supports SELECT queries, got {self.current.value}",
                self.current
            )
        
        query = self.parse_select()
        
        return ExplainNode(query=query, analyze=analyze)
    
    # =========================================================================
    # EXPRESSIONS
    # =========================================================================
    
    def parse_expression(self) -> ExpressionNode:
        """Парсит выражение (entry point)."""
        return self.parse_or_expression()
    
    def parse_or_expression(self) -> ExpressionNode:
        """Парсит OR выражение."""
        left = self.parse_and_expression()
        
        while self.match(TokenType.OR):
            self.advance()
            right = self.parse_and_expression()
            left = BinaryOpNode(left=left, operator=BinaryOperator.OR, right=right)
        
        return left
    
    def parse_and_expression(self) -> ExpressionNode:
        """Парсит AND выражение."""
        left = self.parse_not_expression()
        
        while self.match(TokenType.AND):
            self.advance()
            right = self.parse_not_expression()
            left = BinaryOpNode(left=left, operator=BinaryOperator.AND, right=right)
        
        return left
    
    def parse_not_expression(self) -> ExpressionNode:
        """Парсит NOT выражение."""
        if self.match(TokenType.NOT):
            self.advance()
            operand = self.parse_not_expression()
            return UnaryOpNode(operand=operand, operator=UnaryOperator.NOT)
        
        return self.parse_comparison_expression()
    
    def parse_comparison_expression(self) -> ExpressionNode:
        """Парсит сравнение."""
        left = self.parse_additive_expression()
        
        # Comparison operators
        if self.match(TokenType.EQ):
            self.advance()
            right = self.parse_additive_expression()
            return BinaryOpNode(left=left, operator=BinaryOperator.EQ, right=right)
        elif self.match(TokenType.NE):
            self.advance()
            right = self.parse_additive_expression()
            return BinaryOpNode(left=left, operator=BinaryOperator.NE, right=right)
        elif self.match(TokenType.LT):
            self.advance()
            right = self.parse_additive_expression()
            return BinaryOpNode(left=left, operator=BinaryOperator.LT, right=right)
        elif self.match(TokenType.LE):
            self.advance()
            right = self.parse_additive_expression()
            return BinaryOpNode(left=left, operator=BinaryOperator.LE, right=right)
        elif self.match(TokenType.GT):
            self.advance()
            right = self.parse_additive_expression()
            return BinaryOpNode(left=left, operator=BinaryOperator.GT, right=right)
        elif self.match(TokenType.GE):
            self.advance()
            right = self.parse_additive_expression()
            return BinaryOpNode(left=left, operator=BinaryOperator.GE, right=right)
        elif self.match(TokenType.IS):
            self.advance()
            if self.match(TokenType.NOT):
                self.advance()
                self.expect(TokenType.NULL)
                return UnaryOpNode(operand=left, operator=UnaryOperator.IS_NOT_NULL)
            else:
                self.expect(TokenType.NULL)
                return UnaryOpNode(operand=left, operator=UnaryOperator.IS_NULL)
        elif self.match(TokenType.BETWEEN):
            self.advance()
            low = self.parse_additive_expression()
            self.expect(TokenType.AND)
            high = self.parse_additive_expression()
            return BetweenNode(expr=left, low=low, high=high)
        elif self.match(TokenType.IN):
            self.advance()
            self.expect(TokenType.LPAREN)
            if self.match(TokenType.SELECT):
                # Subquery
                subquery = self.parse_select()
                self.expect(TokenType.RPAREN)
                return BinaryOpNode(
                    left=left,
                    operator=BinaryOperator.IN,
                    right=SubqueryNode(query=subquery, subquery_type="in")
                )
            else:
                # Value list
                values = []
                while not self.match(TokenType.RPAREN):
                    values.append(self.parse_expression())
                    if not self.match(TokenType.COMMA):
                        break
                    self.advance()
                self.expect(TokenType.RPAREN)
                return InListNode(expr=left, values=values)
        elif self.match(TokenType.LIKE):
            self.advance()
            right = self.parse_additive_expression()
            return BinaryOpNode(left=left, operator=BinaryOperator.LIKE, right=right)
        
        return left
    
    def parse_additive_expression(self) -> ExpressionNode:
        """Парсит сложение/вычитание."""
        left = self.parse_multiplicative_expression()
        
        while self.match(TokenType.PLUS, TokenType.MINUS):
            op = BinaryOperator.ADD if self.current.type == TokenType.PLUS else BinaryOperator.SUB
            self.advance()
            right = self.parse_multiplicative_expression()
            left = BinaryOpNode(left=left, operator=op, right=right)
        
        return left
    
    def parse_multiplicative_expression(self) -> ExpressionNode:
        """Парсит умножение/деление."""
        left = self.parse_unary_expression()
        
        while self.match(TokenType.STAR, TokenType.SLASH, TokenType.PERCENT):
            if self.current.type == TokenType.STAR:
                op = BinaryOperator.MUL
            elif self.current.type == TokenType.SLASH:
                op = BinaryOperator.DIV
            else:
                op = BinaryOperator.MOD
            self.advance()
            right = self.parse_unary_expression()
            left = BinaryOpNode(left=left, operator=op, right=right)
        
        return left
    
    def parse_unary_expression(self) -> ExpressionNode:
        """Парсит унарное выражение."""
        if self.match(TokenType.MINUS):
            self.advance()
            operand = self.parse_unary_expression()
            return UnaryOpNode(operand=operand, operator=UnaryOperator.NEG)
        
        return self.parse_primary_expression()
    
    def parse_primary_expression(self) -> ExpressionNode:
        """
        [START_CONTRACT_PARSE_PRIMARY_EXPRESSION]
        Intent: Парсит первичное выражение (литералы, функции, CASE, CAST, COALESCE).
        Output: ExpressionNode соответствующего типа.
        [END_CONTRACT_PARSE_PRIMARY_EXPRESSION]
        """
        # Parenthesized expression or subquery
        if self.match(TokenType.LPAREN):
            self.advance()
            if self.match(TokenType.SELECT):
                subquery = self.parse_select()
                self.expect(TokenType.RPAREN)
                return SubqueryNode(query=subquery)
            else:
                expr = self.parse_expression()
                self.expect(TokenType.RPAREN)
                return expr
        
        # Literals
        if self.match(TokenType.INTEGER):
            value = int(self.advance().value)
            return LiteralNode(value=value, data_type=DataType.INT)
        
        if self.match(TokenType.REAL):
            value = float(self.advance().value)
            return LiteralNode(value=value, data_type=DataType.REAL)
        
        if self.match(TokenType.STRING):
            value = self.advance().value
            return LiteralNode(value=value, data_type=DataType.TEXT)
        
        if self.match(TokenType.NULL):
            self.advance()
            return LiteralNode(value=None, data_type=DataType.NULL)
        
        if self.match(TokenType.BOOLEAN):
            value = self.advance().value == "TRUE"
            return LiteralNode(value=value, data_type=DataType.BOOL)
        
        # CASE expression
        if self.match(TokenType.CASE):
            return self.parse_case_expression()
        
        # CAST function
        if self.match(TokenType.CAST):
            return self.parse_cast_function()
        
        # COALESCE function
        if self.match(TokenType.COALESCE):
            return self.parse_coalesce_function()
        
        # Aggregate functions
        if self.match(TokenType.COUNT, TokenType.SUM, TokenType.AVG, TokenType.MIN, TokenType.MAX):
            return self.parse_aggregate()
        
        # EXISTS
        if self.match(TokenType.EXISTS):
            self.advance()
            self.expect(TokenType.LPAREN)
            subquery = self.parse_select()
            self.expect(TokenType.RPAREN)
            return ExistsNode(subquery=SubqueryNode(query=subquery, subquery_type="exists"))
        
        # Column reference or function
        if self.match(TokenType.IDENTIFIER):
            name = self.advance().value
            
            # Function call
            if self.match(TokenType.LPAREN):
                self.advance()
                args = []
                while not self.match(TokenType.RPAREN):
                    args.append(self.parse_expression())
                    if not self.match(TokenType.COMMA):
                        break
                    self.advance()
                self.expect(TokenType.RPAREN)
                return FunctionCall(name=name.upper(), args=args)
            
            # table.column
            if self.match(TokenType.DOT):
                self.advance()
                col_name = self.expect(TokenType.IDENTIFIER).value
                return ColumnRef(column_name=col_name, table_alias=name)
            
            return ColumnRef(column_name=name)
        
        # Star
        if self.match(TokenType.STAR):
            self.advance()
            return StarColumn()
        
        raise ParseError(f"Unexpected token in expression: {self.current.value}", self.current)
    
    def parse_case_expression(self) -> CaseExpression:
        """
        [START_CONTRACT_PARSE_CASE]
        Intent: Парсит CASE WHEN ... THEN ... ELSE ... END выражение.
        Input: CASE WHEN condition THEN result [WHEN ...] [ELSE result] END.
        Output: CaseExpression узел.
        [END_CONTRACT_PARSE_CASE]
        """
        self.expect(TokenType.CASE)
        
        when_clauses = []
        while self.match(TokenType.WHEN):
            self.advance()
            condition = self.parse_expression()
            self.expect(TokenType.THEN)
            result = self.parse_expression()
            when_clauses.append((condition, result))
        
        else_result = None
        if self.match(TokenType.ELSE):
            self.advance()
            else_result = self.parse_expression()
        
        self.expect(TokenType.END)
        
        return CaseExpression(when_clauses=when_clauses, else_result=else_result)
    
    def parse_cast_function(self) -> FunctionCall:
        """
        [START_CONTRACT_PARSE_CAST]
        Intent: Парсит CAST(expr AS type) функцию.
        Input: CAST(expression AS data_type).
        Output: FunctionCall узел с именем CAST.
        [END_CONTRACT_PARSE_CAST]
        """
        self.expect(TokenType.CAST)
        self.expect(TokenType.LPAREN)
        
        expr = self.parse_expression()
        
        self.expect(TokenType.AS)
        target_type = self.parse_data_type()
        
        self.expect(TokenType.RPAREN)
        
        # Return as FunctionCall with type as string arg
        return FunctionCall(
            name="CAST",
            args=[expr, LiteralNode(value=target_type.name, data_type=DataType.TEXT)]
        )
    
    def parse_coalesce_function(self) -> FunctionCall:
        """
        [START_CONTRACT_PARSE_COALESCE]
        Intent: Парсит COALESCE(val1, val2, ...) функцию.
        Input: COALESCE(expression [, expression ...]).
        Output: FunctionCall узел с именем COALESCE.
        [END_CONTRACT_PARSE_COALESCE]
        """
        self.expect(TokenType.COALESCE)
        self.expect(TokenType.LPAREN)
        
        args = []
        while not self.match(TokenType.RPAREN):
            args.append(self.parse_expression())
            if not self.match(TokenType.COMMA):
                break
            self.advance()
        
        self.expect(TokenType.RPAREN)
        
        return FunctionCall(name="COALESCE", args=args)
    
    def parse_aggregate(self) -> AggregateNode:
        """Парсит агрегатную функцию."""
        agg_map = {
            TokenType.COUNT: AggregateType.COUNT,
            TokenType.SUM: AggregateType.SUM,
            TokenType.AVG: AggregateType.AVG,
            TokenType.MIN: AggregateType.MIN,
            TokenType.MAX: AggregateType.MAX,
        }
        
        agg_type = agg_map[self.current.type]
        self.advance()
        self.expect(TokenType.LPAREN)
        
        distinct = False
        if self.match(TokenType.DISTINCT):
            self.advance()
            distinct = True
        
        arg = None
        if self.match(TokenType.STAR):
            self.advance()
        else:
            arg = self.parse_expression()
        
        self.expect(TokenType.RPAREN)
        
        return AggregateNode(agg_type=agg_type, arg=arg, distinct=distinct)
    
    # =========================================================================
    # TRANSACTION COMMANDS
    # =========================================================================
    
    def parse_begin(self) -> TransactionNode:
        """
        [START_CONTRACT_PARSE_BEGIN]
        Intent: Парсит BEGIN [TRANSACTION] [ISOLATION LEVEL ...].
        Input: BEGIN [TRANSACTION] [ISOLATION LEVEL READ COMMITTED|REPEATABLE READ].
        Output: TransactionNode с isolation_level.
        [END_CONTRACT_PARSE_BEGIN]
        """
        self.expect(TokenType.BEGIN)
        
        # Optional TRANSACTION keyword
        if self.match(TokenType.TRANSACTION):
            self.advance()
        
        isolation_level = None
        
        # Optional ISOLATION LEVEL clause
        if self.match(TokenType.ISOLATION):
            self.advance()
            self.expect(TokenType.LEVEL)
            
            if self.match(TokenType.READ):
                self.advance()
                if self.match(TokenType.COMMITTED):
                    self.advance()
                    isolation_level = "READ COMMITTED"
                elif self.match(TokenType.REPEATABLE):
                    self.advance()
                    self.expect(TokenType.READ)
                    isolation_level = "REPEATABLE READ"
                else:
                    raise ParseError(
                        f"Expected COMMITTED or REPEATABLE READ, got {self.current.value}",
                        self.current
                    )
            else:
                raise ParseError(
                    f"Expected READ after ISOLATION LEVEL, got {self.current.value}",
                    self.current
                )
        
        return TransactionNode(command="BEGIN", isolation_level=isolation_level)
    
    def parse_commit(self) -> TransactionNode:
        """
        [START_CONTRACT_PARSE_COMMIT]
        Intent: Парсит COMMIT [TRANSACTION].
        Input: COMMIT [TRANSACTION].
        Output: TransactionNode для COMMIT.
        [END_CONTRACT_PARSE_COMMIT]
        """
        self.expect(TokenType.COMMIT)
        
        # Optional TRANSACTION keyword
        if self.match(TokenType.TRANSACTION):
            self.advance()
        
        return TransactionNode(command="COMMIT")
    
    def parse_rollback(self) -> TransactionNode:
        """
        [START_CONTRACT_PARSE_ROLLBACK]
        Intent: Парсит ROLLBACK [TRANSACTION].
        Input: ROLLBACK [TRANSACTION].
        Output: TransactionNode для ROLLBACK.
        [END_CONTRACT_PARSE_ROLLBACK]
        """
        self.expect(TokenType.ROLLBACK)
        
        # Optional TRANSACTION keyword
        if self.match(TokenType.TRANSACTION):
            self.advance()
        
        return TransactionNode(command="ROLLBACK")


# END_BLOCK_PARSER


# =============================================================================
# START_BLOCK_HELPERS
# =============================================================================

def parse_sql(sql: str) -> ASTNode:
    """
    [START_CONTRACT_PARSE_SQL]
    Intent: Утилитарная функция для парсинга SQL строки.
    Input: sql - SQL запрос.
    Output: ASTNode для выполнения.
    [END_CONTRACT_PARSE_SQL]
    """
    parser = Parser(sql=sql)
    return parser.parse()

# END_BLOCK_HELPERS