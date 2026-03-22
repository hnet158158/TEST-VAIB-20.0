# START_MODULE_CONTRACT
# Module: mini_db_v2.__main__
# Intent: Entry point для python -m mini_db_v2.
# Dependencies: mini_db_v2.storage, mini_db_v2.repl
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: main (entry point)
# END_MODULE_MAP

"""
Entry point для mini_db_v2.

Usage:
    python -m mini_db_v2
    python -m mini_db_v2 --help
    python -m mini_db_v2 --file script.sql
"""

import sys
from mini_db_v2.storage.database import Database
from mini_db_v2.repl import REPL


def main() -> int:
    """
    [START_CONTRACT_MAIN]
    Intent: Главный entry point для mini_db_v2.
    Output: Код возврата (0 = успех).
    Note: Запускает интерактивный REPL.
    [END_CONTRACT_MAIN]
    """
    # Parse arguments
    args = sys.argv[1:]
    
    if "--help" in args or "-h" in args:
        print(__doc__)
        return 0
    
    if "--version" in args or "-v" in args:
        print("mini_db_v2 version 2.0.0")
        return 0
    
    # Create in-memory database
    database = Database(name="default")
    
    # Check for script file
    if "--file" in args or "-f" in args:
        idx = args.index("--file") if "--file" in args else args.index("-f")
        if idx + 1 < len(args):
            script_path = args[idx + 1]
            return run_script(database, script_path)
        else:
            print("Error: --file requires a file path")
            return 1
    
    # Start interactive REPL
    repl = REPL(database)
    repl.run()
    
    return 0


def run_script(database: Database, script_path: str) -> int:
    """
    [START_CONTRACT_RUN_SCRIPT]
    Intent: Выполняет SQL скрипт из файла.
    Input: database - БД; script_path - путь к файлу.
    Output: Код возврата (0 = успех).
    [END_CONTRACT_RUN_SCRIPT]
    """
    try:
        with open(script_path, 'r', encoding='utf-8') as f:
            script = f.read()
    except FileNotFoundError:
        print(f"Error: File not found: {script_path}")
        return 1
    except IOError as e:
        print(f"Error reading file: {e}")
        return 1
    
    # Split by semicolon and execute each statement
    from mini_db_v2.parser.parser import Parser
    from mini_db_v2.executor.executor import Executor
    from mini_db_v2.parser.lexer import LexerError
    from mini_db_v2.parser.parser import ParseError
    from mini_db_v2.executor.executor import ExecutorError
    
    executor = Executor(database)
    
    # Simple split by semicolon (doesn't handle semicolons in strings)
    statements = [s.strip() for s in script.split(';') if s.strip()]
    
    for stmt in statements:
        try:
            parser = Parser(sql=stmt)
            ast = parser.parse()
            result = executor.execute(ast)
            if result.rows:
                print(format_rows(result.rows, result.columns))
            elif result.message:
                print(result.message)
        except (LexerError, ParseError) as e:
            print(f"Syntax error: {e}")
            return 1
        except ExecutorError as e:
            print(f"Error: {e}")
            return 1
    
    return 0


def format_rows(rows: list[dict], columns: list[str]) -> str:
    """Форматирует строки как таблицу."""
    if not rows:
        return "(no rows)"
    
    # Calculate widths
    widths = {col: len(str(col)) for col in columns}
    for row in rows:
        for col in columns:
            val = row.get(col)
            val_str = "NULL" if val is None else str(val)
            widths[col] = max(widths[col], len(val_str))
    
    # Build output
    lines = []
    header = " | ".join(str(col).ljust(widths[col]) for col in columns)
    lines.append(header)
    lines.append("-" * len(header))
    for row in rows:
        vals = []
        for col in columns:
            val = row.get(col)
            val_str = "NULL" if val is None else str(val)
            vals.append(val_str.ljust(widths[col]))
        lines.append(" | ".join(vals))
    
    return "\n".join(lines)


if __name__ == "__main__":
    sys.exit(main())