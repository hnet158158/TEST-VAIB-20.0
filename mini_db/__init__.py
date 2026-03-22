# START_MODULE_CONTRACT
# Module: mini_db
# Intent: In-memory СУБД для VAIB Stress-Test Benchmark.
#         Предоставляет SQL-подобный интерфейс без сторонних зависимостей.
# Version: 1.0
# END_MODULE_CONTRACT

"""
mini_db - In-memory СУБД на чистом Python 3.11+

Использование:
    python -m mini_db
    
Команды:
    CREATE TABLE, INSERT, SELECT, UPDATE, DELETE
    CREATE INDEX, SAVE, LOAD, EXIT
"""

__version__ = "1.0.0"