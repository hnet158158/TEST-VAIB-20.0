# START_MODULE_CONTRACT
# Module: mini_db_v2.parser.lexer
# Intent: SQL токенизация для расширенного синтаксиса (JOIN, GROUP BY, HAVING, subqueries).
# Dependencies: dataclasses, typing, enum, re
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: Token, TokenType, Lexer, LexerError
# END_MODULE_MAP

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from enum import Enum, auto
import re


# =============================================================================
# START_BLOCK_TOKEN_TYPES
# =============================================================================

class TokenType(Enum):
    """Типы токенов SQL."""
    # Literals
    INTEGER = auto()
    REAL = auto()
    STRING = auto()
    NULL = auto()
    BOOLEAN = auto()
    IDENTIFIER = auto()
    
    # Keywords
    SELECT = auto()
    FROM = auto()
    WHERE = auto()
    AND = auto()
    OR = auto()
    NOT = auto()
    INSERT = auto()
    INTO = auto()
    VALUES = auto()
    UPDATE = auto()
    SET = auto()
    DELETE = auto()
    CREATE = auto()
    DROP = auto()
    TABLE = auto()
    INDEX = auto()
    PRIMARY = auto()
    KEY = auto()
    UNIQUE = auto()
    NULL_KW = auto()  # NULL keyword (vs NULL literal)
    NOT_NULL = auto()
    DEFAULT = auto()
    INT = auto()
    TEXT = auto()
    REAL_KW = auto()
    BOOL = auto()
    
    # JOIN keywords
    JOIN = auto()
    INNER = auto()
    LEFT = auto()
    RIGHT = auto()
    FULL = auto()
    OUTER = auto()
    CROSS = auto()
    ON = auto()
    
    # GROUP BY / HAVING
    GROUP = auto()
    BY = auto()
    HAVING = auto()
    
    # ORDER BY / LIMIT
    ORDER = auto()
    ASC = auto()
    DESC = auto()
    LIMIT = auto()
    OFFSET = auto()
    
    # Aggregates
    COUNT = auto()
    SUM = auto()
    AVG = auto()
    MIN = auto()
    MAX = auto()
    DISTINCT = auto()
    
    # Subquery / IN / EXISTS
    IN = auto()
    EXISTS = auto()
    BETWEEN = auto()
    LIKE = auto()
    IS = auto()
    AS = auto()
    CASE = auto()
    WHEN = auto()
    THEN = auto()
    ELSE = auto()
    END = auto()
    CAST = auto()
    COALESCE = auto()
    
    # Transaction
    BEGIN = auto()
    COMMIT = auto()
    ROLLBACK = auto()
    TRANSACTION = auto()
    ISOLATION = auto()
    LEVEL = auto()
    READ = auto()
    COMMITTED = auto()
    REPEATABLE = auto()
    
    # EXPLAIN
    EXPLAIN = auto()
    ANALYZE = auto()
    
    # IF EXISTS / IF NOT EXISTS
    IF = auto()
    
    # Operators
    PLUS = auto()       # +
    MINUS = auto()      # -
    STAR = auto()       # *
    SLASH = auto()      # /
    PERCENT = auto()    # %
    EQ = auto()         # =
    NE = auto()         # != or <>
    LT = auto()         # <
    LE = auto()         # <=
    GT = auto()         # >
    GE = auto()         # >=
    
    # Punctuation
    LPAREN = auto()     # (
    RPAREN = auto()     # )
    COMMA = auto()      # ,
    DOT = auto()        # .
    SEMICOLON = auto()  # ;
    
    # Special
    EOF = auto()
    UNKNOWN = auto()


# END_BLOCK_TOKEN_TYPES


# =============================================================================
# START_BLOCK_TOKEN
# =============================================================================

@dataclass
class Token:
    """
    [START_CONTRACT_TOKEN]
    Intent: Токен SQL с типом, значением и позицией.
    Input: type - тип токена; value - текстовое значение; line, column - позиция.
    Output: Структура для парсера.
    [END_CONTRACT_TOKEN]
    """
    type: TokenType
    value: str
    line: int
    column: int

# END_BLOCK_TOKEN


# =============================================================================
# START_BLOCK_LEXER_ERROR
# =============================================================================

class LexerError(Exception):
    """
    [START_CONTRACT_LEXER_ERROR]
    Intent: Ошибка токенизации с позицией.
    Input: message - описание; line, column - позиция ошибки.
    Output: Исключение для обработки.
    [END_CONTRACT_LEXER_ERROR]
    """
    def __init__(self, message: str, line: int, column: int):
        super().__init__(f"Lexer error at line {line}, column {column}: {message}")
        self.line = line
        self.column = column

# END_BLOCK_LEXER_ERROR


# =============================================================================
# START_BLOCK_KEYWORDS
# =============================================================================

KEYWORDS: dict[str, TokenType] = {
    # Basic
    "SELECT": TokenType.SELECT,
    "FROM": TokenType.FROM,
    "WHERE": TokenType.WHERE,
    "AND": TokenType.AND,
    "OR": TokenType.OR,
    "NOT": TokenType.NOT,
    "INSERT": TokenType.INSERT,
    "INTO": TokenType.INTO,
    "VALUES": TokenType.VALUES,
    "UPDATE": TokenType.UPDATE,
    "SET": TokenType.SET,
    "DELETE": TokenType.DELETE,
    "CREATE": TokenType.CREATE,
    "DROP": TokenType.DROP,
    "TABLE": TokenType.TABLE,
    "INDEX": TokenType.INDEX,
    "PRIMARY": TokenType.PRIMARY,
    "KEY": TokenType.KEY,
    "UNIQUE": TokenType.UNIQUE,
    "DEFAULT": TokenType.DEFAULT,
    "INT": TokenType.INT,
    "INTEGER": TokenType.INT,
    "TEXT": TokenType.TEXT,
    "VARCHAR": TokenType.TEXT,
    "STRING": TokenType.TEXT,
    "REAL": TokenType.REAL_KW,
    "FLOAT": TokenType.REAL_KW,
    "DOUBLE": TokenType.REAL_KW,
    "BOOL": TokenType.BOOL,
    "BOOLEAN": TokenType.BOOL,
    
    # JOIN
    "JOIN": TokenType.JOIN,
    "INNER": TokenType.INNER,
    "LEFT": TokenType.LEFT,
    "RIGHT": TokenType.RIGHT,
    "FULL": TokenType.FULL,
    "OUTER": TokenType.OUTER,
    "CROSS": TokenType.CROSS,
    "ON": TokenType.ON,
    
    # GROUP BY / HAVING
    "GROUP": TokenType.GROUP,
    "BY": TokenType.BY,
    "HAVING": TokenType.HAVING,
    
    # ORDER BY / LIMIT
    "ORDER": TokenType.ORDER,
    "ASC": TokenType.ASC,
    "DESC": TokenType.DESC,
    "LIMIT": TokenType.LIMIT,
    "OFFSET": TokenType.OFFSET,
    
    # Aggregates
    "COUNT": TokenType.COUNT,
    "SUM": TokenType.SUM,
    "AVG": TokenType.AVG,
    "MIN": TokenType.MIN,
    "MAX": TokenType.MAX,
    "DISTINCT": TokenType.DISTINCT,
    
    # Subquery / IN / EXISTS
    "IN": TokenType.IN,
    "EXISTS": TokenType.EXISTS,
    "BETWEEN": TokenType.BETWEEN,
    "LIKE": TokenType.LIKE,
    "IS": TokenType.IS,
    "AS": TokenType.AS,
    "CASE": TokenType.CASE,
    "WHEN": TokenType.WHEN,
    "THEN": TokenType.THEN,
    "ELSE": TokenType.ELSE,
    "END": TokenType.END,
    "CAST": TokenType.CAST,
    "COALESCE": TokenType.COALESCE,
    
    # Transaction
    "BEGIN": TokenType.BEGIN,
    "COMMIT": TokenType.COMMIT,
    "ROLLBACK": TokenType.ROLLBACK,
    "TRANSACTION": TokenType.TRANSACTION,
    "ISOLATION": TokenType.ISOLATION,
    "LEVEL": TokenType.LEVEL,
    "READ": TokenType.READ,
    "COMMITTED": TokenType.COMMITTED,
    "REPEATABLE": TokenType.REPEATABLE,
    
    # EXPLAIN
    "EXPLAIN": TokenType.EXPLAIN,
    "ANALYZE": TokenType.ANALYZE,
    
    # IF
    "IF": TokenType.IF,
    
    # NULL / TRUE / FALSE
    "NULL": TokenType.NULL,
    "TRUE": TokenType.BOOLEAN,
    "FALSE": TokenType.BOOLEAN,
}

# END_BLOCK_KEYWORDS


# =============================================================================
# START_BLOCK_LEXER
# =============================================================================

class Lexer:
    """
    [START_CONTRACT_LEXER]
    Intent: Токенизация SQL запросов с поддержкой полного синтаксиса.
    Input: SQL строка.
    Output: Список токенов.
    [END_CONTRACT_LEXER]
    """
    
    def __init__(self, text: str):
        """
        [START_CONTRACT_LEXER_INIT]
        Intent: Инициализация лексера с исходным текстом.
        Input: text - SQL запрос.
        Output: Готовый к токенизации лексер.
        [END_CONTRACT_LEXER_INIT]
        """
        self.text = text
        self.pos = 0
        self.line = 1
        self.column = 1
        self.tokens: list[Token] = []
    
    def tokenize(self) -> list[Token]:
        """
        [START_CONTRACT_TOKENIZE]
        Intent: Токенизация всего SQL текста.
        Input: Исходный текст переданный в __init__.
        Output: Список токенов включая EOF.
        [END_CONTRACT_TOKENIZE]
        """
        while self.pos < len(self.text):
            self._skip_whitespace()
            if self.pos >= len(self.text):
                break
            
            char = self.text[self.pos]
            
            # Comments
            if char == '-' and self._peek(1) == '-':
                self._skip_line_comment()
                continue
            if char == '/' and self._peek(1) == '*':
                self._skip_block_comment()
                continue
            
            # String literals
            if char in ('"', "'"):
                self._read_string(char)
                continue
            
            # Numbers
            if char.isdigit() or (char == '-' and self._peek(1) and self._peek(1).isdigit()):
                self._read_number()
                continue
            
            # Identifiers and keywords
            if char.isalpha() or char == '_':
                self._read_identifier()
                continue
            
            # Operators and punctuation
            self._read_operator()
        
        self.tokens.append(Token(TokenType.EOF, "", self.line, self.column))
        return self.tokens
    
    def _peek(self, offset: int = 0) -> Optional[str]:
        """Смотрит на символ смещения без продвижения."""
        pos = self.pos + offset
        if pos < len(self.text):
            return self.text[pos]
        return None
    
    def _advance(self) -> str:
        """Продвигается на один символ, возвращает текущий."""
        char = self.text[self.pos]
        self.pos += 1
        if char == '\n':
            self.line += 1
            self.column = 1
        else:
            self.column += 1
        return char
    
    def _skip_whitespace(self) -> None:
        """Пропускает пробельные символы."""
        while self.pos < len(self.text) and self.text[self.pos].isspace():
            self._advance()
    
    def _skip_line_comment(self) -> None:
        """Пропускает однострочный комментарий --."""
        while self.pos < len(self.text) and self.text[self.pos] != '\n':
            self._advance()
    
    def _skip_block_comment(self) -> None:
        """Пропускает блочный комментарий /* */."""
        self._advance()  # /
        self._advance()  # *
        while self.pos < len(self.text) - 1:
            if self.text[self.pos] == '*' and self.text[self.pos + 1] == '/':
                self._advance()
                self._advance()
                return
            self._advance()
    
    def _read_string(self, quote: str) -> None:
        """Читает строковый литерал."""
        start_line = self.line
        start_col = self.column
        self._advance()  # opening quote
        
        value = []
        while self.pos < len(self.text):
            char = self.text[self.pos]
            if char == quote:
                # Check for escaped quote
                if self._peek(1) == quote:
                    value.append(quote)
                    self._advance()
                    self._advance()
                else:
                    self._advance()  # closing quote
                    break
            else:
                value.append(self._advance())
        
        self.tokens.append(Token(
            TokenType.STRING,
            ''.join(value),
            start_line,
            start_col
        ))
    
    def _read_number(self) -> None:
        """Читает числовой литерал (целый или вещественный)."""
        start_line = self.line
        start_col = self.column
        value = []
        is_real = False
        
        # Optional sign
        if self._peek() == '-':
            value.append(self._advance())
        
        # Integer part
        while self.pos < len(self.text) and self.text[self.pos].isdigit():
            value.append(self._advance())
        
        # Decimal part
        if self._peek() == '.' and self._peek(1) and self._peek(1).isdigit():
            is_real = True
            value.append(self._advance())  # .
            while self.pos < len(self.text) and self.text[self.pos].isdigit():
                value.append(self._advance())
        
        # Exponent
        if self._peek() in ('e', 'E'):
            is_real = True
            value.append(self._advance())
            if self._peek() in ('+', '-'):
                value.append(self._advance())
            while self.pos < len(self.text) and self.text[self.pos].isdigit():
                value.append(self._advance())
        
        token_type = TokenType.REAL if is_real else TokenType.INTEGER
        self.tokens.append(Token(token_type, ''.join(value), start_line, start_col))
    
    def _read_identifier(self) -> None:
        """Читает идентификатор или ключевое слово."""
        start_line = self.line
        start_col = self.column
        value = []
        
        while self.pos < len(self.text):
            char = self.text[self.pos]
            if char.isalnum() or char == '_':
                value.append(self._advance())
            else:
                break
        
        name = ''.join(value)
        upper_name = name.upper()
        
        # Check for keywords
        if upper_name in KEYWORDS:
            token_type = KEYWORDS[upper_name]
            # Special handling for TRUE/FALSE
            if token_type == TokenType.BOOLEAN:
                self.tokens.append(Token(TokenType.BOOLEAN, upper_name, start_line, start_col))
            elif token_type == TokenType.NULL:
                self.tokens.append(Token(TokenType.NULL, "NULL", start_line, start_col))
            else:
                self.tokens.append(Token(token_type, name, start_line, start_col))
        else:
            self.tokens.append(Token(TokenType.IDENTIFIER, name, start_line, start_col))
    
    def _read_operator(self) -> None:
        """Читает оператор или пунктуацию."""
        start_line = self.line
        start_col = self.column
        char = self._advance()
        
        operators = {
            '+': TokenType.PLUS,
            '-': TokenType.MINUS,
            '*': TokenType.STAR,
            '/': TokenType.SLASH,
            '%': TokenType.PERCENT,
            '=': TokenType.EQ,
            '(': TokenType.LPAREN,
            ')': TokenType.RPAREN,
            ',': TokenType.COMMA,
            '.': TokenType.DOT,
            ';': TokenType.SEMICOLON,
        }
        
        if char in operators:
            self.tokens.append(Token(operators[char], char, start_line, start_col))
        elif char == '<':
            if self._peek() == '=':
                self._advance()
                self.tokens.append(Token(TokenType.LE, "<=", start_line, start_col))
            elif self._peek() == '>':
                self._advance()
                self.tokens.append(Token(TokenType.NE, "<>", start_line, start_col))
            else:
                self.tokens.append(Token(TokenType.LT, "<", start_line, start_col))
        elif char == '>':
            if self._peek() == '=':
                self._advance()
                self.tokens.append(Token(TokenType.GE, ">=", start_line, start_col))
            else:
                self.tokens.append(Token(TokenType.GT, ">", start_line, start_col))
        elif char == '!':
            if self._peek() == '=':
                self._advance()
                self.tokens.append(Token(TokenType.NE, "!=", start_line, start_col))
            else:
                self.tokens.append(Token(TokenType.UNKNOWN, "!", start_line, start_col))
        else:
            self.tokens.append(Token(TokenType.UNKNOWN, char, start_line, start_col))

# END_BLOCK_LEXER


def tokenize(sql: str) -> list[Token]:
    """
    [START_CONTRACT_TOKENIZE_FUNC]
    Intent: Утилитарная функция для быстрой токенизации.
    Input: sql - SQL запрос.
    Output: Список токенов.
    [END_CONTRACT_TOKENIZE_FUNC]
    """
    lexer = Lexer(sql)
    return lexer.tokenize()