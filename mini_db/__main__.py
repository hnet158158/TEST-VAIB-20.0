# START_MODULE_CONTRACT
# Module: mini_db.__main__
# Intent: Entry point для запуска REPL через python -m mini_db.
# END_MODULE_CONTRACT

from mini_db.repl import REPL


def main() -> None:
    """
    [START_CONTRACT_MAIN]
    Intent: Запустить REPL для интерактивной работы с базой данных.
    Output: Интерактивный интерфейс для SQL-команд.
    [END_CONTRACT_MAIN]
    """
    repl = REPL()
    repl.run()


if __name__ == "__main__":
    main()