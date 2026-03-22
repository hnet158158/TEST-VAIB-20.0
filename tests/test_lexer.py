# START_MODULE_CONTRACT
# Module: tests.test_lexer
# Intent: Unit tests для Lexer - токенизация SQL-подобного синтаксиса.
# END_MODULE_CONTRACT

import unittest

from mini_db.parser.lexer import Lexer, Token, TokenType, LexerError


class TestLexerKeywords(unittest.TestCase):
    """Тесты токенизации ключевых слов"""
    
    def setUp(self):
        self.lexer = Lexer()
    
    def test_select_keyword(self):
        tokens = self.lexer.tokenize("SELECT")
        self.assertEqual(len(tokens), 2)  # SELECT + EOF
        self.assertEqual(tokens[0].type, TokenType.SELECT)
        self.assertEqual(tokens[0].value, "SELECT")
    
    def test_from_keyword(self):
        tokens = self.lexer.tokenize("FROM")
        self.assertEqual(tokens[0].type, TokenType.FROM)
    
    def test_where_keyword(self):
        tokens = self.lexer.tokenize("WHERE")
        self.assertEqual(tokens[0].type, TokenType.WHERE)
    
    def test_insert_keyword(self):
        tokens = self.lexer.tokenize("INSERT")
        self.assertEqual(tokens[0].type, TokenType.INSERT)
    
    def test_update_keyword(self):
        tokens = self.lexer.tokenize("UPDATE")
        self.assertEqual(tokens[0].type, TokenType.UPDATE)
    
    def test_delete_keyword(self):
        tokens = self.lexer.tokenize("DELETE")
        self.assertEqual(tokens[0].type, TokenType.DELETE)
    
    def test_create_keyword(self):
        tokens = self.lexer.tokenize("CREATE")
        self.assertEqual(tokens[0].type, TokenType.CREATE)
    
    def test_table_keyword(self):
        tokens = self.lexer.tokenize("TABLE")
        self.assertEqual(tokens[0].type, TokenType.TABLE)
    
    def test_case_insensitive_keywords(self):
        tokens = self.lexer.tokenize("select")
        self.assertEqual(tokens[0].type, TokenType.SELECT)
        self.assertEqual(tokens[0].value, "SELECT")
        
        tokens = self.lexer.tokenize("SeLeCt")
        self.assertEqual(tokens[0].type, TokenType.SELECT)


class TestLexerTypes(unittest.TestCase):
    """Тесты токенизации типов данных"""
    
    def setUp(self):
        self.lexer = Lexer()
    
    def test_int_type(self):
        tokens = self.lexer.tokenize("INT")
        self.assertEqual(tokens[0].type, TokenType.INT)
    
    def test_text_type(self):
        tokens = self.lexer.tokenize("TEXT")
        self.assertEqual(tokens[0].type, TokenType.TEXT)
    
    def test_bool_type(self):
        tokens = self.lexer.tokenize("BOOL")
        self.assertEqual(tokens[0].type, TokenType.BOOL)


class TestLexerLiterals(unittest.TestCase):
    """Тесты токенизации литералов"""
    
    def setUp(self):
        self.lexer = Lexer()
    
    def test_identifier(self):
        tokens = self.lexer.tokenize("my_table")
        self.assertEqual(tokens[0].type, TokenType.IDENTIFIER)
        self.assertEqual(tokens[0].value, "my_table")
    
    def test_identifier_with_underscore(self):
        tokens = self.lexer.tokenize("_private_table")
        self.assertEqual(tokens[0].type, TokenType.IDENTIFIER)
        self.assertEqual(tokens[0].value, "_private_table")
    
    def test_identifier_with_numbers(self):
        tokens = self.lexer.tokenize("table123")
        self.assertEqual(tokens[0].type, TokenType.IDENTIFIER)
        self.assertEqual(tokens[0].value, "table123")
    
    def test_string_single_quote(self):
        tokens = self.lexer.tokenize("'hello world'")
        self.assertEqual(tokens[0].type, TokenType.STRING)
        self.assertEqual(tokens[0].value, "hello world")
    
    def test_string_double_quote(self):
        tokens = self.lexer.tokenize('"test"')
        self.assertEqual(tokens[0].type, TokenType.STRING)
        self.assertEqual(tokens[0].value, "test")
    
    def test_string_empty(self):
        tokens = self.lexer.tokenize("''")
        self.assertEqual(tokens[0].type, TokenType.STRING)
        self.assertEqual(tokens[0].value, "")
    
    def test_number_integer(self):
        tokens = self.lexer.tokenize("42")
        self.assertEqual(tokens[0].type, TokenType.NUMBER)
        self.assertEqual(tokens[0].value, 42)
    
    def test_number_zero(self):
        tokens = self.lexer.tokenize("0")
        self.assertEqual(tokens[0].type, TokenType.NUMBER)
        self.assertEqual(tokens[0].value, 0)
    
    def test_number_negative(self):
        tokens = self.lexer.tokenize("-10")
        self.assertEqual(tokens[0].type, TokenType.NUMBER)
        self.assertEqual(tokens[0].value, -10)
    
    def test_true_literal(self):
        tokens = self.lexer.tokenize("TRUE")
        self.assertEqual(tokens[0].type, TokenType.TRUE)
    
    def test_false_literal(self):
        tokens = self.lexer.tokenize("FALSE")
        self.assertEqual(tokens[0].type, TokenType.FALSE)
    
    def test_null_literal(self):
        tokens = self.lexer.tokenize("NULL")
        self.assertEqual(tokens[0].type, TokenType.NULL)


class TestLexerOperators(unittest.TestCase):
    """Тесты токенизации операторов"""
    
    def setUp(self):
        self.lexer = Lexer()
    
    def test_equals(self):
        tokens = self.lexer.tokenize("=")
        self.assertEqual(tokens[0].type, TokenType.EQ)
    
    def test_not_equals(self):
        tokens = self.lexer.tokenize("!=")
        self.assertEqual(tokens[0].type, TokenType.NEQ)
    
    def test_less_than(self):
        tokens = self.lexer.tokenize("<")
        self.assertEqual(tokens[0].type, TokenType.LT)
    
    def test_greater_than(self):
        tokens = self.lexer.tokenize(">")
        self.assertEqual(tokens[0].type, TokenType.GT)
    
    def test_and_operator(self):
        tokens = self.lexer.tokenize("AND")
        self.assertEqual(tokens[0].type, TokenType.AND)
    
    def test_or_operator(self):
        tokens = self.lexer.tokenize("OR")
        self.assertEqual(tokens[0].type, TokenType.OR)


class TestLexerPunctuation(unittest.TestCase):
    """Тесты токенизации пунктуации"""
    
    def setUp(self):
        self.lexer = Lexer()
    
    def test_left_paren(self):
        tokens = self.lexer.tokenize("(")
        self.assertEqual(tokens[0].type, TokenType.LPAREN)
    
    def test_right_paren(self):
        tokens = self.lexer.tokenize(")")
        self.assertEqual(tokens[0].type, TokenType.RPAREN)
    
    def test_comma(self):
        tokens = self.lexer.tokenize(",")
        self.assertEqual(tokens[0].type, TokenType.COMMA)
    
    def test_semicolon(self):
        tokens = self.lexer.tokenize(";")
        self.assertEqual(tokens[0].type, TokenType.SEMICOLON)
    
    def test_star(self):
        tokens = self.lexer.tokenize("*")
        self.assertEqual(tokens[0].type, TokenType.STAR)


class TestLexerComplexQueries(unittest.TestCase):
    """Тесты токенизации сложных запросов"""
    
    def setUp(self):
        self.lexer = Lexer()
    
    def test_create_table_query(self):
        tokens = self.lexer.tokenize("CREATE TABLE users (id INT, name TEXT)")
        self.assertEqual(tokens[0].type, TokenType.CREATE)
        self.assertEqual(tokens[1].type, TokenType.TABLE)
        self.assertEqual(tokens[2].type, TokenType.IDENTIFIER)
        self.assertEqual(tokens[2].value, "users")
        self.assertEqual(tokens[3].type, TokenType.LPAREN)
        self.assertEqual(tokens[4].type, TokenType.IDENTIFIER)
        self.assertEqual(tokens[4].value, "id")
        self.assertEqual(tokens[5].type, TokenType.INT)
        self.assertEqual(tokens[6].type, TokenType.COMMA)
        self.assertEqual(tokens[7].type, TokenType.IDENTIFIER)
        self.assertEqual(tokens[7].value, "name")
        self.assertEqual(tokens[8].type, TokenType.TEXT)
        self.assertEqual(tokens[9].type, TokenType.RPAREN)
    
    def test_insert_query(self):
        tokens = self.lexer.tokenize("INSERT INTO users (id, name) VALUES (1, 'John')")
        self.assertEqual(tokens[0].type, TokenType.INSERT)
        self.assertEqual(tokens[1].type, TokenType.INTO)
        self.assertEqual(tokens[2].type, TokenType.IDENTIFIER)
        # tokens: INSERT(0), INTO(1), users(2), ((3), id(4), ,(5), name(6), )(7), VALUES(8), ((9), 1(10), ,(11), 'John'(12), )(13)
        self.assertEqual(tokens[8].type, TokenType.VALUES)
        self.assertEqual(tokens[10].type, TokenType.NUMBER)
        self.assertEqual(tokens[10].value, 1)
        self.assertEqual(tokens[12].type, TokenType.STRING)
        self.assertEqual(tokens[12].value, "John")
    
    def test_select_query(self):
        tokens = self.lexer.tokenize("SELECT * FROM users WHERE id = 1")
        self.assertEqual(tokens[0].type, TokenType.SELECT)
        self.assertEqual(tokens[1].type, TokenType.STAR)
        self.assertEqual(tokens[2].type, TokenType.FROM)
        self.assertEqual(tokens[4].type, TokenType.WHERE)
        self.assertEqual(tokens[6].type, TokenType.EQ)
        self.assertEqual(tokens[7].type, TokenType.NUMBER)
        self.assertEqual(tokens[7].value, 1)
    
    def test_update_query(self):
        tokens = self.lexer.tokenize("UPDATE users SET name = 'Jane' WHERE id = 1")
        self.assertEqual(tokens[0].type, TokenType.UPDATE)
        self.assertEqual(tokens[2].type, TokenType.SET)
        self.assertEqual(tokens[4].type, TokenType.EQ)
        self.assertEqual(tokens[5].type, TokenType.STRING)
    
    def test_delete_query(self):
        tokens = self.lexer.tokenize("DELETE FROM users WHERE id = 1")
        self.assertEqual(tokens[0].type, TokenType.DELETE)
        self.assertEqual(tokens[1].type, TokenType.FROM)
        # tokens: DELETE(0), FROM(1), users(2), WHERE(3), id(4), =(5), 1(6)
        self.assertEqual(tokens[3].type, TokenType.WHERE)
    
    def test_complex_where(self):
        tokens = self.lexer.tokenize("(col1 > 10 OR col2 = 'test') AND col3 != true")
        # tokens: ((0), col1(1), >(2), 10(3), OR(4), col2(5), =(6), 'test'(7), )(8), AND(9), col3(10), !=(11), true(12)
        self.assertEqual(tokens[0].type, TokenType.LPAREN)
        self.assertEqual(tokens[1].type, TokenType.IDENTIFIER)
        self.assertEqual(tokens[2].type, TokenType.GT)
        self.assertEqual(tokens[3].type, TokenType.NUMBER)
        self.assertEqual(tokens[4].type, TokenType.OR)
        self.assertEqual(tokens[8].type, TokenType.RPAREN)
        self.assertEqual(tokens[9].type, TokenType.AND)
        self.assertEqual(tokens[11].type, TokenType.NEQ)
        self.assertEqual(tokens[12].type, TokenType.TRUE)


class TestLexerErrors(unittest.TestCase):
    """Тесты обработки ошибок"""
    
    def setUp(self):
        self.lexer = Lexer()
    
    def test_unexpected_character(self):
        with self.assertRaises(LexerError) as context:
            self.lexer.tokenize("@")
        self.assertIn("Unexpected character", str(context.exception))
    
    def test_unterminated_string(self):
        with self.assertRaises(LexerError) as context:
            self.lexer.tokenize("'unterminated")
        self.assertIn("Unterminated string", str(context.exception))
    
    def test_error_position(self):
        with self.assertRaises(LexerError) as context:
            self.lexer.tokenize("SELECT @ FROM")
        self.assertEqual(context.exception.position, 7)


class TestLexerEOF(unittest.TestCase):
    """Тесты токена EOF"""
    
    def setUp(self):
        self.lexer = Lexer()
    
    def test_eof_token_added(self):
        tokens = self.lexer.tokenize("SELECT")
        self.assertEqual(tokens[-1].type, TokenType.EOF)
    
    def test_empty_query(self):
        tokens = self.lexer.tokenize("")
        self.assertEqual(len(tokens), 1)
        self.assertEqual(tokens[0].type, TokenType.EOF)
    
    def test_whitespace_only(self):
        tokens = self.lexer.tokenize("   \t\n  ")
        self.assertEqual(len(tokens), 1)
        self.assertEqual(tokens[0].type, TokenType.EOF)


class TestLexerWhitespace(unittest.TestCase):
    """Тесты обработки пробелов"""
    
    def setUp(self):
        self.lexer = Lexer()
    
    def test_multiple_spaces(self):
        tokens = self.lexer.tokenize("SELECT    FROM")
        self.assertEqual(len(tokens), 3)  # SELECT, FROM, EOF
        self.assertEqual(tokens[0].type, TokenType.SELECT)
        self.assertEqual(tokens[1].type, TokenType.FROM)
    
    def test_newlines(self):
        tokens = self.lexer.tokenize("SELECT\nFROM\nWHERE")
        self.assertEqual(len(tokens), 4)  # SELECT, FROM, WHERE, EOF
    
    def test_tabs(self):
        tokens = self.lexer.tokenize("SELECT\tFROM")
        self.assertEqual(len(tokens), 3)


if __name__ == "__main__":
    unittest.main()