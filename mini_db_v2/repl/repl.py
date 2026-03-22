# START_MODULE_CONTRACT
# Module: mini_db_v2.repl.repl
# Intent: Read-Eval-Print Loop для интерактивной работы с mini_db_v2.
# Dependencies: typing, sys, mini_db_v2.parser, mini_db_v2.executor, mini_db_v2.storage
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: REPL
# END_MODULE_MAP

from __future__ import annotations
from typing import TYPE_CHECKING, Optional, Any
import sys
import time

if TYPE_CHECKING:
    from mini_db_v2.storage.database import Database
    from mini_db_v2.executor.executor import Executor, ExecutionResult

from mini_db_v2.parser.parser import Parser, ParseError
from mini_db_v2.parser.lexer import LexerError
from mini_db_v2.executor.executor import Executor, ExecutorError
from mini_db_v2.repl.commands import CommandHandler


# =============================================================================
# START_BLOCK_REPL
# =============================================================================

class REPL:
    """
    [START_CONTRACT_REPL]
    Intent: Интерактивный Read-Eval-Print Loop для mini_db_v2.
    Input: database - база данных для работы.
    Output: Интерактивный интерфейс для SQL команд.
    Note: Graceful error handling - никаких Python Traceback пользователю.
    [END_CONTRACT_REPL]
    """
    
    PROMPT = "mini_db> "
    CONTINUE_PROMPT = "      ...> "
    WELCOME = """
+===============================================================+
|              MINI_DB_V2 - Production-Grade Database           |
|                     Version 2.0.0                             |
+===============================================================+
|  Type .help for available commands                            |
|  End SQL statements with ;                                    |
|  Multi-line input supported                                   |
+===============================================================+
"""
    
    def __init__(self, database: Database):
        """
        [START_CONTRACT_REPL_INIT]
        Intent: Инициализация REPL с базой данных.
        Input: database - база данных для работы.
        Output: Готовый к работе REPL.
        [END_CONTRACT_REPL_INIT]
        """
        self._database = database
        self._executor = Executor(database)
        self._command_handler = CommandHandler(database)
        self._running = True
        self._buffer: list[str] = []
    
    def run(self) -> None:
        """
        [START_CONTRACT_RUN]
        Intent: Запускает главный цикл REPL.
        Output: Интерактивный интерфейс до команды .quit/.exit.
        Note: Все ошибки перехватываются и выводятся в дружественном формате.
        [END_CONTRACT_RUN]
        """
        print(self.WELCOME)
        
        while self._running:
            try:
                self._iteration()
            except KeyboardInterrupt:
                print("\nInterrupted. Type .quit to exit.")
                self._buffer = []
            except EOFError:
                print("\nGoodbye!")
                break
    
    def _iteration(self) -> None:
        """Одна итерация REPL цикла."""
        # Determine prompt
        prompt = self.CONTINUE_PROMPT if self._buffer else self.PROMPT
        
        # Read input
        try:
            line = input(prompt)
        except EOFError:
            raise
        
        # Check for empty input
        if not line.strip():
            if not self._buffer:
                return
            # Continue collecting input
            return
        
        # Check for dot commands (only at start of statement)
        if line.strip().startswith('.') and not self._buffer:
            result = self._command_handler.handle(line)
            if result is None:
                # .quit or .exit
                print("Goodbye!")
                self._running = False
            else:
                print(result)
            return
        
        # Add to buffer
        self._buffer.append(line)
        
        # Check for statement terminator
        if not self._is_complete():
            return
        
        # Execute the statement
        sql = ' '.join(self._buffer)
        self._buffer = []
        
        output = self.execute(sql)
        print(output)
    
    def _is_complete(self) -> bool:
        """
        [START_CONTRACT_IS_COMPLETE]
        Intent: Проверяет, завершён ли SQL оператор.
        Output: True если оператор завершён (; или одна строка без продолжения).
        [END_CONTRACT_IS_COMPLETE]
        """
        if not self._buffer:
            return False
        
        joined = ' '.join(self._buffer)
        
        # Check for semicolon
        if ';' in joined:
            return True
        
        # Check for incomplete constructs
        open_parens = joined.count('(') - joined.count(')')
        if open_parens > 0:
            return False
        
        # Single line without semicolon - execute anyway for simple commands
        # This allows: SELECT 1, INSERT INTO t VALUES (1, 2)
        # But requires ; for multi-line
        return len(self._buffer) == 1
    
    def execute(self, sql: str) -> str:
        """
        [START_CONTRACT_EXECUTE]
        Intent: Выполняет SQL и возвращает форматированный результат.
        Input: sql - SQL запрос.
        Output: Форматированный результат или сообщение об ошибке.
        Note: Все ошибки перехватываются, Python Traceback не выводится.
        [END_CONTRACT_EXECUTE]
        """
        start_time = time.time()
        
        try:
            # Parse
            parser = Parser(sql=sql)
            ast = parser.parse()
            
            # Execute
            result = self._executor.execute(ast)
            
            # Format output
            output = self.format_result(result)
            
            # Add timing if enabled
            if self._command_handler.timer_enabled:
                elapsed = (time.time() - start_time) * 1000
                output += f"\nTime: {elapsed:.3f} ms"
            
            return output
            
        except LexerError as e:
            return f"Syntax error: {e}"
        except ParseError as e:
            return f"Syntax error: {e}"
        except ExecutorError as e:
            return f"Error: {e}"
        except Exception as e:
            # Catch-all for unexpected errors (should not happen in production)
            return f"Internal error: {e}"
    
    def format_result(self, result: ExecutionResult) -> str:
        """
        [START_CONTRACT_FORMAT_RESULT]
        Intent: Форматирует результат выполнения в таблицу.
        Input: result - результат выполнения SQL.
        Output: Форматированная ASCII таблица или сообщение.
        [END_CONTRACT_FORMAT_RESULT]
        """
        if not result.success:
            return f"Error: {result.message}"
        
        # No data returned
        if not result.rows:
            return result.message or "OK"
        
        # Format as table
        return self._format_table(result.rows, result.columns)
    
    def _format_table(self, rows: list[dict], columns: list[str]) -> str:
        """
        [START_CONTRACT_FORMAT_TABLE]
        Intent: Форматирует строки как ASCII таблицу.
        Input: rows - данные; columns - имена колонок.
        Output: ASCII таблица с рамками.
        [END_CONTRACT_FORMAT_TABLE]
        """
        if not rows or not columns:
            return "(empty result)"
        
        # Calculate column widths
        widths = {}
        for col in columns:
            widths[col] = len(str(col))
            for row in rows:
                val = row.get(col)
                val_str = self._format_value(val)
                widths[col] = max(widths[col], len(val_str))
        
        # Limit column width
        max_width = 50
        for col in widths:
            widths[col] = min(widths[col], max_width)
        
        # Build separator
        sep_parts = ["+"]
        for col in columns:
            sep_parts.append("-" * (widths[col] + 2))
            sep_parts.append("+")
        separator = "".join(sep_parts)
        
        # Build header
        header_parts = ["|"]
        for col in columns:
            header_parts.append(f" {str(col):<{widths[col]}} ")
            header_parts.append("|")
        header = "".join(header_parts)
        
        # Build rows
        lines = [separator, header, separator]
        for row in rows:
            row_parts = ["|"]
            for col in columns:
                val = row.get(col)
                val_str = self._format_value(val)
                if len(val_str) > max_width:
                    val_str = val_str[:max_width-3] + "..."
                row_parts.append(f" {val_str:<{widths[col]}} ")
                row_parts.append("|")
            lines.append("".join(row_parts))
        lines.append(separator)
        
        # Add row count
        lines.append(f"({len(rows)} row{'s' if len(rows) != 1 else ''})")
        
        return "\n".join(lines)
    
    def _format_value(self, value: Any) -> str:
        """Форматирует значение для вывода."""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, float):
            return f"{value:.6g}"
        return str(value)

# END_BLOCK_REPL


# =============================================================================
# START_BLOCK_HELPERS
# =============================================================================

def start_repl(database: Database) -> None:
    """
    [START_CONTRACT_START_REPL]
    Intent: Запускает REPL с заданной БД.
    Input: database - база данных.
    Output: Интерактивный REPL.
    [END_CONTRACT_START_REPL]
    """
    repl = REPL(database)
    repl.run()

# END_BLOCK_HELPERS