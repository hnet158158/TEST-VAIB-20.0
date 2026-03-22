# START_MODULE_CONTRACT
# Module: mini_db.parser.lexer
# Intent: Токенизация SQL-подобного синтаксиса без использования regex.
#         Character-by-character scanning с lookahead.
# Constraints: Запрещено использовать regex для всего запроса целиком.
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports:
#   - TokenType: enum типов токенов
#   - Token: dataclass токена (type, value, position)
#   - LexerError: исключение при ошибке токенизации
#   - Lexer: класс с методом tokenize(query: str) -> list[Token]
# END_MODULE_MAP

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Any


# START_BLOCK_TOKEN_TYPE
class TokenType(Enum):
    """
    [START_CONTRACT_TOKEN_TYPE]
    Intent: Перечисление всех типов токенов SQL-синтаксиса.
    Output: Enum с категориями: Keywords, Types, Literals, Operators, Punctuation.
    [END_CONTRACT_TOKEN_TYPE]
    """
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
    ON = auto()
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
# END_BLOCK_TOKEN_TYPE


# START_BLOCK_TOKEN
@dataclass
class Token:
    """
    [START_CONTRACT_TOKEN]
    Intent: Представление токена с типом, значением и позицией в исходном тексте.
    Input: type - TokenType; value - примитивный тип; position - индекс в строке.
    Output: Immutable dataclass для использования в Parser.
    [END_CONTRACT_TOKEN]
    """
    type: TokenType
    value: Any
    position: int
# END_BLOCK_TOKEN


# START_BLOCK_LEXER_ERROR
class LexerError(Exception):
    """
    [START_CONTRACT_LEXER_ERROR]
    Intent: Исключение при ошибке токенизации с позицией символа.
    Input: message - описание ошибки; position - индекс проблемного символа.
    Output: Exception с информативным сообщением для REPL.
    [END_CONTRACT_LEXER_ERROR]
    """
    def __init__(self, message: str, position: int):
        self.position = position
        super().__init__(f"{message} at position {position}")
# END_BLOCK_LEXER_ERROR


# START_BLOCK_LEXER
class Lexer:
    """
    [START_CONTRACT_LEXER]
    Intent: Токенизация SQL-запросов character-by-character без regex.
    Input: query - строка SQL-запроса.
    Output: list[Token] с токенами от начала до EOF, или выбрасывает LexerError.
    [END_CONTRACT_LEXER]
    """
    
    # Keywords mapping (case-insensitive)
    KEYWORDS: dict[str, TokenType] = {
        "SELECT": TokenType.SELECT,
        "FROM": TokenType.FROM,
        "WHERE": TokenType.WHERE,
        "INSERT": TokenType.INSERT,
        "INTO": TokenType.INTO,
        "VALUES": TokenType.VALUES,
        "UPDATE": TokenType.UPDATE,
        "SET": TokenType.SET,
        "DELETE": TokenType.DELETE,
        "CREATE": TokenType.CREATE,
        "TABLE": TokenType.TABLE,
        "INDEX": TokenType.INDEX,
        "ON": TokenType.ON,
        "UNIQUE": TokenType.UNIQUE,
        "SAVE": TokenType.SAVE,
        "LOAD": TokenType.LOAD,
        "EXIT": TokenType.EXIT,
        "INT": TokenType.INT,
        "TEXT": TokenType.TEXT,
        "BOOL": TokenType.BOOL,
        "TRUE": TokenType.TRUE,
        "FALSE": TokenType.FALSE,
        "NULL": TokenType.NULL,
        "AND": TokenType.AND,
        "OR": TokenType.OR,
    }
    
    def __init__(self):
        self.query: str = ""
        self.pos: int = 0
        self.length: int = 0
    
    def tokenize(self, query: str) -> list[Token]:
        """
        [START_CONTRACT_TOKENIZE]
        Intent: Преобразовать SQL-строку в список токенов.
        Input: query - непустая строка SQL-запроса.
        Output: list[Token] с токенами, последний токен - EOF.
        [END_CONTRACT_TOKENIZE]
        """
        self.query = query
        self.pos = 0
        self.length = len(query)
        
        tokens: list[Token] = []
        
        while not self._is_at_end():
            token = self._scan_token()
            if token is not None:
                tokens.append(token)
        
        tokens.append(Token(TokenType.EOF, None, self.pos))
        return tokens
    
    def _is_at_end(self) -> bool:
        # CONTRACT: Проверка достижения конца строки.
        return self.pos >= self.length
    
    def _current_char(self) -> str:
        # CONTRACT: Текущий символ или '\0' если конец.
        if self._is_at_end():
            return '\0'
        return self.query[self.pos]
    
    def _peek_char(self, offset: int = 1) -> str:
        # CONTRACT: Символ на offset позиций вперёд или '\0'.
        pos = self.pos + offset
        if pos >= self.length:
            return '\0'
        return self.query[pos]
    
    def _advance(self) -> str:
        # CONTRACT: Вернуть текущий символ и сдвинуть позицию.
        char = self._current_char()
        self.pos += 1
        return char
    
    def _skip_whitespace(self) -> None:
        # CONTRACT: Пропустить пробельные символы.
        while self._current_char().isspace():
            self._advance()
    
    def _scan_token(self) -> Token | None:
        """
        [START_CONTRACT_SCAN_TOKEN]
        Intent: Сканировать один токен из текущей позиции.
        Output: Token или None (для whitespace/comments), или выбрасывает LexerError.
        [END_CONTRACT_SCAN_TOKEN]
        """
        self._skip_whitespace()
        
        if self._is_at_end():
            return None
        
        start_pos = self.pos
        char = self._current_char()
        
        # Single-character tokens
        if char == '(':
            self._advance()
            return Token(TokenType.LPAREN, '(', start_pos)
        if char == ')':
            self._advance()
            return Token(TokenType.RPAREN, ')', start_pos)
        if char == ',':
            self._advance()
            return Token(TokenType.COMMA, ',', start_pos)
        if char == ';':
            self._advance()
            return Token(TokenType.SEMICOLON, ';', start_pos)
        if char == '*':
            self._advance()
            return Token(TokenType.STAR, '*', start_pos)
        
        # Two-character operators
        if char == '!' and self._peek_char() == '=':
            self._advance()
            self._advance()
            return Token(TokenType.NEQ, '!=', start_pos)
        if char == '=':
            self._advance()
            return Token(TokenType.EQ, '=', start_pos)
        if char == '<':
            self._advance()
            return Token(TokenType.LT, '<', start_pos)
        if char == '>':
            self._advance()
            return Token(TokenType.GT, '>', start_pos)
        
        # String literal
        if char == "'" or char == '"':
            return self._scan_string(char, start_pos)
        
        # Number literal
        if char.isdigit() or (char == '-' and self._peek_char().isdigit()):
            return self._scan_number(start_pos)
        
        # Identifier or keyword
        if char.isalpha() or char == '_':
            return self._scan_identifier(start_pos)
        
        # Unknown character
        raise LexerError(f"Unexpected character '{char}'", start_pos)
    
    def _scan_string(self, quote: str, start_pos: int) -> Token:
        """
        [START_CONTRACT_SCAN_STRING]
        Intent: Сканировать строковый литерал в кавычках.
        Input: quote - открывающая кавычка (' или "); start_pos - позиция начала.
        Output: Token(TokenType.STRING, str, start_pos) или LexerError.
        [END_CONTRACT_SCAN_STRING]
        """
        self._advance()  # Skip opening quote
        
        value_chars: list[str] = []
        
        while not self._is_at_end():
            char = self._current_char()
            
            if char == quote:
                # Check for escaped quote
                if self._peek_char() == quote:
                    value_chars.append(quote)
                    self._advance()
                    self._advance()
                else:
                    self._advance()  # Skip closing quote
                    return Token(TokenType.STRING, ''.join(value_chars), start_pos)
            else:
                value_chars.append(char)
                self._advance()
        
        raise LexerError("Unterminated string", start_pos)
    
    def _scan_number(self, start_pos: int) -> Token:
        """
        [START_CONTRACT_SCAN_NUMBER]
        Intent: Сканировать числовой литерал (целое число).
        Input: start_pos - позиция начала числа.
        Output: Token(TokenType.NUMBER, int, start_pos) или LexerError.
        [END_CONTRACT_SCAN_NUMBER]
        """
        value_chars: list[str] = []
        
        # Handle negative sign
        if self._current_char() == '-':
            value_chars.append('-')
            self._advance()
        
        while not self._is_at_end() and self._current_char().isdigit():
            value_chars.append(self._current_char())
            self._advance()
        
        value_str = ''.join(value_chars)
        try:
            value = int(value_str)
        except ValueError:
            raise LexerError(f"Invalid number '{value_str}'", start_pos)
        
        return Token(TokenType.NUMBER, value, start_pos)
    
    def _scan_identifier(self, start_pos: int) -> Token:
        """
        [START_CONTRACT_SCAN_IDENTIFIER]
        Intent: Сканировать идентификатор или ключевое слово.
        Input: start_pos - позиция начала идентификатора.
        Output: Token с соответствующим типом (keyword или IDENTIFIER).
        [END_CONTRACT_SCAN_IDENTIFIER]
        """
        value_chars: list[str] = []
        
        while not self._is_at_end():
            char = self._current_char()
            if char.isalnum() or char == '_':
                value_chars.append(char)
                self._advance()
            else:
                break
        
        value = ''.join(value_chars)
        upper_value = value.upper()
        
        # Check if it's a keyword
        if upper_value in self.KEYWORDS:
            return Token(self.KEYWORDS[upper_value], upper_value, start_pos)
        
        return Token(TokenType.IDENTIFIER, value, start_pos)
# END_BLOCK_LEXER