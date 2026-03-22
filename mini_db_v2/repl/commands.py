# START_MODULE_CONTRACT
# Module: mini_db_v2.repl.commands
# Intent: REPL commands - обработка dot-команд (.help, .tables, etc.)
# Dependencies: typing, enum
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: CommandHandler, REPLCommand
# END_MODULE_MAP

from __future__ import annotations
from typing import TYPE_CHECKING, Optional
from enum import Enum, auto

if TYPE_CHECKING:
    from mini_db_v2.storage.database import Database
    from mini_db_v2.storage.table import Table


# =============================================================================
# START_BLOCK_ENUMS
# =============================================================================

class REPLCommand(Enum):
    """Типы REPL команд."""
    HELP = auto()
    TABLES = auto()
    SCHEMA = auto()
    INDICES = auto()
    EXPLAIN = auto()
    QUIT = auto()
    EXIT = auto()
    TIMER = auto()
    UNKNOWN = auto()

# END_BLOCK_ENUMS


# =============================================================================
# START_BLOCK_HANDLER
# =============================================================================

class CommandHandler:
    """
    [START_CONTRACT_COMMAND_HANDLER]
    Intent: Обработчик REPL dot-команд.
    Input: command - имя команды; args - аргументы; database - БД.
    Output: Строка с результатом команды.
    Note: Поддерживает .help, .tables, .schema, .indices, .quit, .exit, .timer.
    [END_CONTRACT_COMMAND_HANDLER]
    """
    
    def __init__(self, database: Optional[Database] = None):
        """
        [START_CONTRACT_COMMAND_HANDLER_INIT]
        Intent: Инициализация handler с опциональной БД.
        Input: database - база данных (может быть установлена позже).
        Output: Готовый handler.
        [END_CONTRACT_COMMAND_HANDLER_INIT]
        """
        self._database = database
        self._timer_enabled = False
    
    @property
    def database(self) -> Optional[Database]:
        """Возвращает текущую БД."""
        return self._database
    
    @database.setter
    def database(self, db: Database) -> None:
        """Устанавливает БД."""
        self._database = db
    
    @property
    def timer_enabled(self) -> bool:
        """Возвращает состояние таймера."""
        return self._timer_enabled
    
    def parse_command(self, line: str) -> tuple[REPLCommand, list[str]]:
        """
        [START_CONTRACT_PARSE_COMMAND]
        Intent: Парсит строку и возвращает команду с аргументами.
        Input: line - строка ввода (например, ".tables" или ".schema users").
        Output: Кортеж (REPLCommand, args).
        [END_CONTRACT_PARSE_COMMAND]
        """
        line = line.strip()
        if not line.startswith('.'):
            return REPLCommand.UNKNOWN, []
        
        parts = line[1:].split()
        if not parts:
            return REPLCommand.UNKNOWN, []
        
        cmd_str = parts[0].upper()
        args = parts[1:]
        
        cmd_map = {
            'HELP': REPLCommand.HELP,
            'TABLES': REPLCommand.TABLES,
            'SCHEMA': REPLCommand.SCHEMA,
            'INDICES': REPLCommand.INDICES,
            'INDEXES': REPLCommand.INDICES,  # alias
            'EXPLAIN': REPLCommand.EXPLAIN,
            'QUIT': REPLCommand.QUIT,
            'EXIT': REPLCommand.EXIT,
            'TIMER': REPLCommand.TIMER,
        }
        
        return cmd_map.get(cmd_str, REPLCommand.UNKNOWN), args
    
    def handle(self, line: str) -> Optional[str]:
        """
        [START_CONTRACT_HANDLE]
        Intent: Обрабатывает dot-команду и возвращает результат.
        Input: line - строка ввода.
        Output: Результат команды или None для quit/exit.
        [END_CONTRACT_HANDLE]
        """
        cmd, args = self.parse_command(line)
        
        if cmd == REPLCommand.HELP:
            return self.handle_help()
        elif cmd == REPLCommand.TABLES:
            return self.handle_tables()
        elif cmd == REPLCommand.SCHEMA:
            return self.handle_schema(args[0] if args else None)
        elif cmd == REPLCommand.INDICES:
            return self.handle_indices(args[0] if args else None)
        elif cmd == REPLCommand.QUIT or cmd == REPLCommand.EXIT:
            return None  # Signal to exit
        elif cmd == REPLCommand.TIMER:
            return self.handle_timer(args[0] if args else None)
        else:
            return f"Unknown command: {line}\nType .help for available commands."
    
    def handle_help(self) -> str:
        """
        [START_CONTRACT_HANDLE_HELP]
        Intent: Возвращает справку по командам.
        Output: Многострочная строка со справкой.
        [END_CONTRACT_HANDLE_HELP]
        """
        return """
╔═══════════════════════════════════════════════════════════════╗
║                    MINI_DB_V2 REPL HELP                       ║
╠═══════════════════════════════════════════════════════════════╣
║  .help          Show this help message                        ║
║  .tables        List all tables in the database               ║
║  .schema [tbl]  Show schema for table (or all tables)         ║
║  .indices [tbl] Show indices for table (or all tables)        ║
║  .timer on/off  Enable/disable query timing                   ║
║  .quit / .exit  Exit the REPL                                 ║
╠═══════════════════════════════════════════════════════════════╣
║  SQL Commands:                                                ║
║    SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE   ║
║    CREATE INDEX, DROP INDEX, ANALYZE TABLE, EXPLAIN           ║
║    BEGIN, COMMIT, ROLLBACK                                    ║
╠═══════════════════════════════════════════════════════════════╣
║  Features:                                                    ║
║    - Multi-line input (end with ;)                            ║
║    - B-tree indexes for range queries                         ║
║    - MVCC for concurrent transactions                         ║
║    - WAL for crash recovery                                   ║
╚═══════════════════════════════════════════════════════════════╝
"""
    
    def handle_tables(self) -> str:
        """
        [START_CONTRACT_HANDLE_TABLES]
        Intent: Возвращает список таблиц в БД.
        Output: Форматированный список таблиц.
        [END_CONTRACT_HANDLE_TABLES]
        """
        if self._database is None:
            return "No database connected."
        
        tables = self._database.tables
        if not tables:
            return "No tables found."
        
        result = "Tables:\n"
        for name in tables:
            table = self._database.get_table(name)
            if table:
                row_count = len(list(table.select()))
                result += f"  {name} ({row_count} rows)\n"
        return result.rstrip()
    
    def handle_schema(self, table_name: Optional[str] = None) -> str:
        """
        [START_CONTRACT_HANDLE_SCHEMA]
        Intent: Возвращает схему таблицы или всех таблиц.
        Input: table_name - имя таблицы или None для всех.
        Output: Форматированная схема.
        [END_CONTRACT_HANDLE_SCHEMA]
        """
        if self._database is None:
            return "No database connected."
        
        if table_name:
            return self._format_table_schema(table_name)
        
        # All tables
        tables = self._database.tables
        if not tables:
            return "No tables found."
        
        result = ""
        for name in tables:
            result += self._format_table_schema(name) + "\n"
        return result.rstrip()
    
    def _format_table_schema(self, table_name: str) -> str:
        """Форматирует схему одной таблицы."""
        table = self._database.get_table(table_name)
        if table is None:
            return f"Table '{table_name}' not found."
        
        result = f"CREATE TABLE {table_name} (\n"
        cols = []
        for col_name, col_def in table.columns.items():
            col_str = f"  {col_name} {col_def.data_type.name}"
            if col_def.primary_key:
                col_str += " PRIMARY KEY"
            if not col_def.nullable:
                col_str += " NOT NULL"
            if col_def.unique:
                col_str += " UNIQUE"
            cols.append(col_str)
        result += ",\n".join(cols)
        result += "\n);"
        return result
    
    def handle_indices(self, table_name: Optional[str] = None) -> str:
        """
        [START_CONTRACT_HANDLE_INDICES]
        Intent: Возвращает индексы таблицы или всех таблиц.
        Input: table_name - имя таблицы или None для всех.
        Output: Форматированный список индексов.
        [END_CONTRACT_HANDLE_INDICES]
        """
        if self._database is None:
            return "No database connected."
        
        # Note: Index information is stored in Executor, not in Database
        # This is a simplified version that shows the concept
        if table_name:
            return self._format_table_indices(table_name)
        
        tables = self._database.tables
        if not tables:
            return "No tables found."
        
        result = "Indices:\n"
        for name in tables:
            idx_info = self._format_table_indices(name)
            if "No indices" not in idx_info:
                result += idx_info + "\n"
        return result.rstrip() if "No indices" not in result else "No indices found."
    
    def _format_table_indices(self, table_name: str) -> str:
        """Форматирует индексы одной таблицы."""
        table = self._database.get_table(table_name)
        if table is None:
            return f"Table '{table_name}' not found."
        
        # Check for unique constraints (which create implicit indices)
        indices = []
        for col_name, col_def in table.columns.items():
            if col_def.unique or col_def.primary_key:
                idx_type = "UNIQUE" if col_def.unique else "PRIMARY"
                indices.append(f"  {table_name}_{col_name}_idx ({col_name}) [{idx_type}]")
        
        if not indices:
            return f"No indices on '{table_name}'."
        
        return f"'{table_name}':\n" + "\n".join(indices)
    
    def handle_timer(self, arg: Optional[str] = None) -> str:
        """
        [START_CONTRACT_HANDLE_TIMER]
        Intent: Управляет таймером выполнения запросов.
        Input: arg - "on" или "off".
        Output: Статус таймера.
        [END_CONTRACT_HANDLE_TIMER]
        """
        if arg is None:
            status = "ON" if self._timer_enabled else "OFF"
            return f"Timer is {status}."
        
        arg = arg.lower()
        if arg == "on":
            self._timer_enabled = True
            return "Timer enabled."
        elif arg == "off":
            self._timer_enabled = False
            return "Timer disabled."
        else:
            return f"Usage: .timer on|off (got: {arg})"

# END_BLOCK_HANDLER