# START_MODULE_CONTRACT
# Module: mini_db.parser.parser
# Intent: Recursive descent parser для SQL-подобного синтаксиса.
#         Преобразует токены от Lexer в AST-узлы.
# Constraints: Запрещено использовать regex для всего запроса целиком.
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports:
#   - ParseError: исключение при ошибке парсинга
#   - Parser: класс с методом parse(query: str) -> ASTNode
# END_MODULE_MAP

from __future__ import annotations

from typing import Any, Optional

from mini_db.ast.nodes import (
    ASTNode,
    ColumnDef,
    ComparisonNode,
    CreateIndexNode,
    CreateTableNode,
    DeleteNode,
    ExitNode,
    ExpressionNode,
    IdentifierNode,
    InsertNode,
    LiteralNode,
    LoadNode,
    LogicalNode,
    SaveNode,
    SelectNode,
    StatementNode,
    UpdateNode,
)
from mini_db.parser.lexer import Lexer, LexerError, Token, TokenType


# START_BLOCK_PARSE_ERROR
class ParseError(Exception):
    """
    [START_CONTRACT_PARSE_ERROR]
    Intent: Исключение при ошибке парсинга с информативным сообщением.
    Input: message - описание ошибки.
    Output: Exception для обработки в REPL.
    [END_CONTRACT_PARSE_ERROR]
    """
    pass
# END_BLOCK_PARSE_ERROR


# START_BLOCK_PARSER
class Parser:
    """
    [START_CONTRACT_PARSER]
    Intent: Recursive descent parser для SQL-операторов CREATE TABLE и INSERT.
    Input: query - строка SQL-запроса.
    Output: ASTNode (StatementNode) или выбрасывает ParseError.
    [END_CONTRACT_PARSER]
    """
    
    def __init__(self):
        self.lexer = Lexer()
        self.tokens: list[Token] = []
        self.pos: int = 0
    
    def parse(self, query: str) -> StatementNode:
        """
        [START_CONTRACT_PARSE]
        Intent: Разобрать SQL-запрос в AST.
        Input: query - непустая строка SQL-запроса.
        Output: StatementNode (CreateTableNode, InsertNode, etc.) или ParseError.
        [END_CONTRACT_PARSE]
        """
        # Tokenize
        try:
            self.tokens = self.lexer.tokenize(query)
        except LexerError as e:
            raise ParseError(f"Lexer error: {e}")
        
        self.pos = 0
        
        # Parse statement
        stmt = self._parse_statement()
        
        # Expect EOF or semicolon
        if not self._is_at_end():
            token = self._current()
            if token.type != TokenType.SEMICOLON:
                raise ParseError(f"Unexpected token '{token.value}' after statement")
        
        return stmt
    
    def _current(self) -> Token:
        # CONTRACT: Текущий токен или EOF.
        if self._is_at_end():
            return self.tokens[-1]  # EOF
        return self.tokens[self.pos]
    
    def _peek(self, offset: int = 1) -> Token:
        # CONTRACT: Токен на offset позиций вперёд.
        pos = self.pos + offset
        if pos >= len(self.tokens):
            return self.tokens[-1]  # EOF
        return self.tokens[pos]
    
    def _is_at_end(self) -> bool:
        # CONTRACT: Проверка достижения конца токенов.
        return self.pos >= len(self.tokens) or self.tokens[self.pos].type == TokenType.EOF
    
    def _advance(self) -> Token:
        # CONTRACT: Вернуть текущий токен и сдвинуть позицию.
        token = self._current()
        self.pos += 1
        return token
    
    def _match(self, *types: TokenType) -> bool:
        # CONTRACT: Проверить, что текущий токен имеет один из указанных типов.
        return self._current().type in types
    
    def _expect(self, token_type: TokenType, message: str) -> Token:
        # CONTRACT: Ожидать токен определённого типа, иначе ошибка.
        if self._current().type != token_type:
            raise ParseError(f"{message}, got '{self._current().value}'")
        return self._advance()
    
    def _parse_statement(self) -> StatementNode:
        """
        [START_CONTRACT_PARSE_STATEMENT]
        Intent: Определить тип оператора и вызвать соответствующий парсер.
        Output: CreateTableNode, InsertNode, SelectNode, UpdateNode, DeleteNode, SaveNode, LoadNode, ExitNode.
        [END_CONTRACT_PARSE_STATEMENT]
        """
        if self._match(TokenType.CREATE):
            return self._parse_create()
        if self._match(TokenType.INSERT):
            return self._parse_insert()
        if self._match(TokenType.SELECT):
            return self._parse_select()
        if self._match(TokenType.UPDATE):
            return self._parse_update()
        if self._match(TokenType.DELETE):
            return self._parse_delete()
        if self._match(TokenType.SAVE):
            return self._parse_save()
        if self._match(TokenType.LOAD):
            return self._parse_load()
        if self._match(TokenType.EXIT):
            return self._parse_exit()
        
        raise ParseError(f"Unknown statement: '{self._current().value}'")
    
    # ==================== CREATE TABLE ====================
    
    def _parse_create(self) -> StatementNode:
        """
        [START_CONTRACT_PARSE_CREATE]
        Intent: Разобрать CREATE TABLE или CREATE INDEX.
        Output: CreateTableNode или CreateIndexNode.
        [END_CONTRACT_PARSE_CREATE]
        """
        self._advance()  # consume CREATE
        
        if self._match(TokenType.TABLE):
            return self._parse_create_table()
        
        if self._match(TokenType.INDEX):
            return self._parse_create_index()
        
        raise ParseError("Expected TABLE or INDEX after CREATE")
    
    def _parse_create_index(self) -> CreateIndexNode:
        """
        [START_CONTRACT_PARSE_CREATE_INDEX]
        Intent: Разобрать CREATE INDEX idx_name ON table_name (col1).
        Output: CreateIndexNode с именем индекса, таблицей и колонкой.
        [END_CONTRACT_PARSE_CREATE_INDEX]
        """
        self._advance()  # consume INDEX
        
        # Index name
        name_token = self._expect(TokenType.IDENTIFIER, "Expected index name")
        index_name = name_token.value
        
        # ON keyword
        self._expect(TokenType.ON, "Expected ON after index name")
        
        # Table name
        table_token = self._expect(TokenType.IDENTIFIER, "Expected table name")
        table_name = table_token.value
        
        # Opening parenthesis
        self._expect(TokenType.LPAREN, "Expected '(' after table name")
        
        # Column name (single column for now)
        col_token = self._expect(TokenType.IDENTIFIER, "Expected column name")
        column_name = col_token.value
        
        # Closing parenthesis
        self._expect(TokenType.RPAREN, "Expected ')' after column name")
        
        return CreateIndexNode(name=index_name, table=table_name, column=column_name)
    
    def _parse_create_table(self) -> CreateTableNode:
        """
        [START_CONTRACT_PARSE_CREATE_TABLE]
        Intent: Разобрать CREATE TABLE name (col1 type, col2 type UNIQUE, ...).
        Output: CreateTableNode с именем и списком ColumnDef.
        [END_CONTRACT_PARSE_CREATE_TABLE]
        """
        self._advance()  # consume TABLE
        
        # Table name
        name_token = self._expect(TokenType.IDENTIFIER, "Expected table name")
        table_name = name_token.value
        
        # Opening parenthesis
        self._expect(TokenType.LPAREN, "Expected '(' after table name")
        
        # Parse columns
        columns = self._parse_column_definitions()
        
        # Closing parenthesis
        self._expect(TokenType.RPAREN, "Expected ')' after column definitions")
        
        return CreateTableNode(name=table_name, columns=columns)
    
    def _parse_column_definitions(self) -> list[ColumnDef]:
        """
        [START_CONTRACT_PARSE_COLUMN_DEFS]
        Intent: Разобрать список определений колонок через запятую.
        Output: list[ColumnDef] с валидированными колонками.
        [END_CONTRACT_PARSE_COLUMN_DEFS]
        """
        columns: list[ColumnDef] = []
        
        # First column is required
        columns.append(self._parse_column_def())
        
        # Additional columns
        while self._match(TokenType.COMMA):
            self._advance()  # consume comma
            columns.append(self._parse_column_def())
        
        return columns
    
    def _parse_column_def(self) -> ColumnDef:
        """
        [START_CONTRACT_PARSE_COLUMN_DEF]
        Intent: Разобрать определение одной колонки: name type [UNIQUE].
        Output: ColumnDef с именем, типом и флагом unique.
        [END_CONTRACT_PARSE_COLUMN_DEF]
        """
        # Column name
        name_token = self._expect(TokenType.IDENTIFIER, "Expected column name")
        col_name = name_token.value
        
        # Data type
        if not self._match(TokenType.INT, TokenType.TEXT, TokenType.BOOL):
            raise ParseError(
                f"Expected data type (INT, TEXT, BOOL), got '{self._current().value}'"
            )
        type_token = self._advance()
        data_type = type_token.value
        
        # Optional UNIQUE
        unique = False
        if self._match(TokenType.UNIQUE):
            self._advance()
            unique = True
        
        return ColumnDef(name=col_name, data_type=data_type, unique=unique)
    
    # ==================== INSERT ====================
    
    def _parse_insert(self) -> InsertNode:
        """
        [START_CONTRACT_PARSE_INSERT]
        Intent: Разобрать INSERT INTO table (col1, col2) VALUES (val1, val2).
        Output: InsertNode с таблицей, колонками и значениями.
        [END_CONTRACT_PARSE_INSERT]
        """
        self._advance()  # consume INSERT
        
        # INTO keyword
        self._expect(TokenType.INTO, "Expected INTO after INSERT")
        
        # Table name
        name_token = self._expect(TokenType.IDENTIFIER, "Expected table name")
        table_name = name_token.value
        
        # Column list (optional)
        columns: Optional[list[str]] = None
        if self._match(TokenType.LPAREN):
            columns = self._parse_column_list()
        
        # VALUES keyword
        self._expect(TokenType.VALUES, "Expected VALUES")
        
        # Value list
        values = self._parse_value_list()
        
        # Validate columns and values match
        if columns is not None and len(columns) != len(values):
            raise ParseError(
                f"Column count ({len(columns)}) does not match value count ({len(values)})"
            )
        
        return InsertNode(
            table=table_name,
            columns=columns if columns else [],
            values=values
        )
    
    def _parse_column_list(self) -> list[str]:
        """
        [START_CONTRACT_PARSE_COLUMN_LIST]
        Intent: Разобрать список имён колонок в скобках: (col1, col2, ...).
        Output: list[str] с именами колонок.
        [END_CONTRACT_PARSE_COLUMN_LIST]
        """
        self._expect(TokenType.LPAREN, "Expected '('")
        
        columns: list[str] = []
        
        # First column
        name_token = self._expect(TokenType.IDENTIFIER, "Expected column name")
        columns.append(name_token.value)
        
        # Additional columns
        while self._match(TokenType.COMMA):
            self._advance()
            name_token = self._expect(TokenType.IDENTIFIER, "Expected column name")
            columns.append(name_token.value)
        
        self._expect(TokenType.RPAREN, "Expected ')'")
        
        return columns
    
    def _parse_value_list(self) -> list[Any]:
        """
        [START_CONTRACT_PARSE_VALUE_LIST]
        Intent: Разобрать список значений в скобках: (1, 'text', true).
        Output: list[Any] с Python-значениями (int, str, bool, None).
        [END_CONTRACT_PARSE_VALUE_LIST]
        """
        self._expect(TokenType.LPAREN, "Expected '('")
        
        values: list[Any] = []
        
        # First value
        values.append(self._parse_literal())
        
        # Additional values
        while self._match(TokenType.COMMA):
            self._advance()
            values.append(self._parse_literal())
        
        self._expect(TokenType.RPAREN, "Expected ')'")
        
        return values
    
    def _parse_literal(self) -> Any:
        """
        [START_CONTRACT_PARSE_LITERAL]
        Intent: Разобрать литеральное значение: число, строка, true/false/null.
        Output: Python-значение (int, str, bool, None).
        [END_CONTRACT_PARSE_LITERAL]
        """
        token = self._current()
        
        if token.type == TokenType.NUMBER:
            self._advance()
            return token.value
        
        if token.type == TokenType.STRING:
            self._advance()
            return token.value
        
        if token.type == TokenType.TRUE:
            self._advance()
            return True
        
        if token.type == TokenType.FALSE:
            self._advance()
            return False
        
        if token.type == TokenType.NULL:
            self._advance()
            return None
        
        raise ParseError(f"Expected literal value, got '{token.value}'")
    
    # ==================== SELECT ====================
    
    def _parse_select(self) -> SelectNode:
        """
        [START_CONTRACT_PARSE_SELECT]
        Intent: Разобрать SELECT [col1, col2 | *] FROM table [WHERE expr].
        Output: SelectNode с таблицей, колонками (None = *) и опциональным WHERE.
        [END_CONTRACT_PARSE_SELECT]
        """
        self._advance()  # consume SELECT
        
        # Parse column list or *
        columns: Optional[list[str]] = None
        if self._match(TokenType.STAR):
            self._advance()
            columns = None  # SELECT *
        else:
            columns = self._parse_select_columns()
        
        # FROM keyword
        self._expect(TokenType.FROM, "Expected FROM")
        
        # Table name
        name_token = self._expect(TokenType.IDENTIFIER, "Expected table name")
        table_name = name_token.value
        
        # Optional WHERE clause
        where: Optional[ExpressionNode] = None
        if self._match(TokenType.WHERE):
            self._advance()
            where = self._parse_expression()
        
        return SelectNode(table=table_name, columns=columns, where=where)
    
    def _parse_select_columns(self) -> list[str]:
        """
        [START_CONTRACT_PARSE_SELECT_COLUMNS]
        Intent: Разобрать список колонок для SELECT: col1, col2, ...
        Output: list[str] с именами колонок.
        [END_CONTRACT_PARSE_SELECT_COLUMNS]
        """
        columns: list[str] = []
        
        # First column
        name_token = self._expect(TokenType.IDENTIFIER, "Expected column name")
        columns.append(name_token.value)
        
        # Additional columns
        while self._match(TokenType.COMMA):
            self._advance()
            name_token = self._expect(TokenType.IDENTIFIER, "Expected column name")
            columns.append(name_token.value)
        
        return columns
    
    # ==================== UPDATE ====================
    
    def _parse_update(self) -> UpdateNode:
        """
        [START_CONTRACT_PARSE_UPDATE]
        Intent: Разобрать UPDATE table SET col1 = val1, col2 = val2 [WHERE expr].
        Output: UpdateNode с таблицей, assignments dict и опциональным WHERE.
        [END_CONTRACT_PARSE_UPDATE]
        """
        self._advance()  # consume UPDATE
        
        # Table name
        name_token = self._expect(TokenType.IDENTIFIER, "Expected table name")
        table_name = name_token.value
        
        # SET keyword
        self._expect(TokenType.SET, "Expected SET")
        
        # Parse assignments
        assignments = self._parse_assignments()
        
        # Optional WHERE clause
        where: Optional[ExpressionNode] = None
        if self._match(TokenType.WHERE):
            self._advance()
            where = self._parse_expression()
        
        return UpdateNode(table=table_name, assignments=assignments, where=where)
    
    def _parse_assignments(self) -> dict[str, Any]:
        """
        [START_CONTRACT_PARSE_ASSIGNMENTS]
        Intent: Разобрать список присваиваний: col1 = val1, col2 = val2, ...
        Output: dict[str, Any] с именами колонок и значениями.
        [END_CONTRACT_PARSE_ASSIGNMENTS]
        """
        assignments: dict[str, Any] = {}
        
        # First assignment
        col_name, value = self._parse_assignment()
        assignments[col_name] = value
        
        # Additional assignments
        while self._match(TokenType.COMMA):
            self._advance()
            col_name, value = self._parse_assignment()
            assignments[col_name] = value
        
        return assignments
    
    def _parse_assignment(self) -> tuple[str, Any]:
        """
        [START_CONTRACT_PARSE_ASSIGNMENT]
        Intent: Разобрать одно присваивание: col = value.
        Output: tuple (column_name, value).
        [END_CONTRACT_PARSE_ASSIGNMENT]
        """
        # Column name
        name_token = self._expect(TokenType.IDENTIFIER, "Expected column name")
        col_name = name_token.value
        
        # = operator
        self._expect(TokenType.EQ, "Expected '='")
        
        # Value (literal)
        value = self._parse_literal()
        
        return (col_name, value)
    
    # ==================== DELETE ====================
    
    def _parse_delete(self) -> DeleteNode:
        """
        [START_CONTRACT_PARSE_DELETE]
        Intent: Разобрать DELETE FROM table [WHERE expr].
        Output: DeleteNode с таблицей и опциональным WHERE.
        [END_CONTRACT_PARSE_DELETE]
        """
        self._advance()  # consume DELETE
        
        # FROM keyword
        self._expect(TokenType.FROM, "Expected FROM")
        
        # Table name
        name_token = self._expect(TokenType.IDENTIFIER, "Expected table name")
        table_name = name_token.value
        
        # Optional WHERE clause
        where: Optional[ExpressionNode] = None
        if self._match(TokenType.WHERE):
            self._advance()
            where = self._parse_expression()
        
        return DeleteNode(table=table_name, where=where)
    
    # ==================== SAVE ====================
    
    def _parse_save(self) -> SaveNode:
        """
        [START_CONTRACT_PARSE_SAVE]
        Intent: Разобрать SAVE 'filepath'; - сохранение базы в JSON-файл.
        Output: SaveNode с путём к файлу.
        [END_CONTRACT_PARSE_SAVE]
        """
        self._advance()  # consume SAVE
        
        # File path (string literal)
        if not self._match(TokenType.STRING):
            raise ParseError("Expected file path string after SAVE")
        
        filepath_token = self._advance()
        filepath = filepath_token.value
        
        return SaveNode(filepath=filepath)
    
    # ==================== LOAD ====================
    
    def _parse_load(self) -> LoadNode:
        """
        [START_CONTRACT_PARSE_LOAD]
        Intent: Разобрать LOAD 'filepath'; - загрузка базы из JSON-файла.
        Output: LoadNode с путём к файлу.
        [END_CONTRACT_PARSE_LOAD]
        """
        self._advance()  # consume LOAD
        
        # File path (string literal)
        if not self._match(TokenType.STRING):
            raise ParseError("Expected file path string after LOAD")
        
        filepath_token = self._advance()
        filepath = filepath_token.value
        
        return LoadNode(filepath=filepath)
    
    # ==================== EXIT ====================
    
    def _parse_exit(self) -> ExitNode:
        """
        [START_CONTRACT_PARSE_EXIT]
        Intent: Разобрать EXIT; - завершение работы REPL.
        Output: ExitNode - маркер выхода.
        [END_CONTRACT_PARSE_EXIT]
        """
        self._advance()  # consume EXIT
        return ExitNode()
    
    # ==================== EXPRESSION PARSING ====================
    
    def _parse_expression(self) -> ExpressionNode:
        """
        [START_CONTRACT_PARSE_EXPRESSION]
        Intent: Разобрать выражение WHERE с приоритетом: OR < AND < comparison.
        Output: ExpressionNode (LogicalNode, ComparisonNode, IdentifierNode, LiteralNode).
        [END_CONTRACT_PARSE_EXPRESSION]
        """
        return self._parse_or()
    
    def _parse_or(self) -> ExpressionNode:
        """
        [START_CONTRACT_PARSE_OR]
        Intent: Разобрать OR-выражения (низший приоритет).
        Output: LogicalNode с op="OR" или результат _parse_and.
        [END_CONTRACT_PARSE_OR]
        """
        left = self._parse_and()
        
        while self._match(TokenType.OR):
            self._advance()
            right = self._parse_and()
            left = LogicalNode(left=left, op="OR", right=right)
        
        return left
    
    def _parse_and(self) -> ExpressionNode:
        """
        [START_CONTRACT_PARSE_AND]
        Intent: Разобрать AND-выражения (средний приоритет).
        Output: LogicalNode с op="AND" или результат _parse_comparison.
        [END_CONTRACT_PARSE_AND]
        """
        left = self._parse_comparison()
        
        while self._match(TokenType.AND):
            self._advance()
            right = self._parse_comparison()
            left = LogicalNode(left=left, op="AND", right=right)
        
        return left
    
    def _parse_comparison(self) -> ExpressionNode:
        """
        [START_CONTRACT_PARSE_COMPARISON]
        Intent: Разобрать сравнение: expr =|!=|<|> expr.
        Output: ComparisonNode или результат _parse_primary.
        [END_CONTRACT_PARSE_COMPARISON]
        """
        left = self._parse_primary()
        
        if self._match(TokenType.EQ, TokenType.NEQ, TokenType.LT, TokenType.GT):
            op_token = self._advance()
            op = op_token.value
            right = self._parse_primary()
            return ComparisonNode(left=left, op=op, right=right)
        
        return left
    
    def _parse_primary(self) -> ExpressionNode:
        """
        [START_CONTRACT_PARSE_PRIMARY]
        Intent: Разобрать первичное выражение: литерал, идентификатор или (expr).
        Output: LiteralNode, IdentifierNode, или вложенное ExpressionNode.
        [END_CONTRACT_PARSE_PRIMARY]
        """
        token = self._current()
        
        # Parenthesized expression
        if self._match(TokenType.LPAREN):
            self._advance()
            expr = self._parse_expression()
            self._expect(TokenType.RPAREN, "Expected ')'")
            return expr
        
        # Literal values
        if token.type == TokenType.NUMBER:
            self._advance()
            return LiteralNode(value=token.value)
        
        if token.type == TokenType.STRING:
            self._advance()
            return LiteralNode(value=token.value)
        
        if token.type == TokenType.TRUE:
            self._advance()
            return LiteralNode(value=True)
        
        if token.type == TokenType.FALSE:
            self._advance()
            return LiteralNode(value=False)
        
        if token.type == TokenType.NULL:
            self._advance()
            return LiteralNode(value=None)
        
        # Identifier (column name)
        if token.type == TokenType.IDENTIFIER:
            self._advance()
            return IdentifierNode(name=token.value)
        
        raise ParseError(f"Expected expression, got '{token.value}'")
# END_BLOCK_PARSER