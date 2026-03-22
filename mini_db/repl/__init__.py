# START_MODULE_CONTRACT
# Module: mini_db.repl
# Intent: Read-Eval-Print Loop для интерактивной работы с базой данных.
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports:
#   - REPL: класс с методами run(), process()
# END_MODULE_MAP

from mini_db.repl.repl import REPL

__all__ = ["REPL"]