# START_MODULE_CONTRACT
# Module: mini_db.repl.repl
# Intent: Read-Eval-Print Loop для интерактивной работы с базой данных.
#         Graceful error handling - никаких Python Traceback при ошибках.
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports:
#   - REPL: класс с методами run(), process()
# END_MODULE_MAP

from __future__ import annotations

from typing import Optional

from mini_db.ast.nodes import ExitNode
from mini_db.executor.executor import Executor
from mini_db.parser.parser import ParseError, Parser
from mini_db.storage.database import Database


# START_BLOCK_REPL
class REPL:
    """
    [START_CONTRACT_REPL]
    Intent: Read-Eval-Print Loop для интерактивной работы с базой данных.
            Все ошибки перехватываются и выводятся в понятном формате.
    Output: Интерактивный интерфейс для SQL-команд.
    [END_CONTRACT_REPL]
    """
    
    def __init__(self):
        self.db = Database()
        self.parser = Parser()
        self.executor = Executor()
        self.running = True
    
    def run(self) -> None:
        """
        [START_CONTRACT_RUN]
        Intent: Запустить интерактивный REPL loop.
        Output: Читает команды из stdin, выполняет, выводит результат.
                Завершается при команде EXIT или EOF.
        [END_CONTRACT_RUN]
        """
        print("mini_db - In-memory SQL Database")
        print("Type 'EXIT;' to quit")
        print()
        
        while self.running:
            try:
                # Read input
                line = input("mini_db> ")
                
                # Skip empty lines
                if not line.strip():
                    continue
                
                # Process command
                output = self.process(line)
                
                # Print result
                if output:
                    print(output)
                    print()
                
            except EOFError:
                # Ctrl+D
                print("Goodbye!")
                self.running = False
            except KeyboardInterrupt:
                # Ctrl+C
                print()
                print("Interrupted. Type 'EXIT;' to quit.")
    
    def process(self, query: str) -> str:
        """
        [START_CONTRACT_PROCESS]
        Intent: Обработать SQL-запрос и вернуть результат.
        Input: query - строка SQL-запроса.
        Output: Строка с результатом или сообщением об ошибке.
                Никогда не выбрасывает исключения - возвращает "Syntax error: ..." или "Error: ...".
        [END_CONTRACT_PROCESS]
        """
        try:
            # Parse query
            ast = self.parser.parse(query)
            
            # Check for EXIT command
            if isinstance(ast, ExitNode):
                self.running = False
                return "Goodbye!"
            
            # Execute query
            result = self.executor.execute(ast, db=self.db)
            
            # Format output
            if result.success:
                if result.data is not None:
                    return self._format_data(result.data)
                return result.message
            else:
                return f"Error: {result.error}"
        
        except ParseError as e:
            return f"Syntax error: {e}"
        except Exception as e:
            # Catch-all for unexpected errors (should not happen in production)
            return f"Error: {e}"
    
    def _format_data(self, data: list[dict]) -> str:
        """
        [START_CONTRACT_FORMAT_DATA]
        Intent: Форматировать данные SELECT для вывода в таблицу.
        Input: data - list[dict] с результатами запроса.
        Output: Строка с форматированной таблицей.
        [END_CONTRACT_FORMAT_DATA]
        """
        if not data:
            return "0 row(s)"
        
        # Get column names from first row
        columns = list(data[0].keys())
        
        # Calculate column widths
        widths = {}
        for col in columns:
            widths[col] = len(col)
            for row in data:
                val = row.get(col)
                val_str = self._format_value(val)
                widths[col] = max(widths[col], len(val_str))
        
        # Build header
        header_parts = []
        for col in columns:
            header_parts.append(col.ljust(widths[col]))
        header = " | ".join(header_parts)
        
        # Build separator
        separator_parts = []
        for col in columns:
            separator_parts.append("-" * widths[col])
        separator = "-+-".join(separator_parts)
        
        # Build rows
        rows = []
        for row in data:
            row_parts = []
            for col in columns:
                val = row.get(col)
                val_str = self._format_value(val)
                row_parts.append(val_str.ljust(widths[col]))
            rows.append(" | ".join(row_parts))
        
        # Combine all parts
        result = [header, separator] + rows
        result.append(f"{len(data)} row(s)")
        
        return "\n".join(result)
    
    def _format_value(self, value: Optional[object]) -> str:
        """
        [START_CONTRACT_FORMAT_VALUE]
        Intent: Форматировать значение для вывода.
        Input: value - примитивный тип или None.
        Output: Строка "NULL" для None, иначе строковое представление.
        [END_CONTRACT_FORMAT_VALUE]
        """
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, str):
            return f"'{value}'"
        return str(value)
# END_BLOCK_REPL