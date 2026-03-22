# START_MODULE_CONTRACT
# Module: mini_db_v2.executor
# Intent: SQL executor module - выполнение DDL и DML команд с JOIN, агрегацией и subqueries.
# Dependencies: mini_db_v2.ast, mini_db_v2.storage
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: Executor, ExecutionResult, ExecutorError, JoinExecutor, MultiJoinExecutor,
#          AggregateExecutor, HashAggregator, DistinctExecutor, SubqueryExecutor
# END_MODULE_MAP

from mini_db_v2.executor.executor import (
    Executor,
    ExecutionResult,
    ExecutorError,
    TableNotFoundError,
    ColumnNotFoundError,
    DuplicateIndexError,
    create_executor
)
from mini_db_v2.executor.joins import (
    JoinExecutor,
    MultiJoinExecutor,
    JoinResult
)
from mini_db_v2.executor.aggregates import (
    AggregateExecutor,
    AggregateResult,
    GroupResult,
    HashAggregator,
    DistinctExecutor,
    AggregateFunctions
)
from mini_db_v2.executor.subqueries import (
    SubqueryExecutor,
    SubqueryContext,
    SubqueryError,
    ScalarSubqueryError,
    CorrelatedSubqueryError
)

__all__ = [
    "Executor",
    "ExecutionResult",
    "ExecutorError",
    "TableNotFoundError",
    "ColumnNotFoundError",
    "DuplicateIndexError",
    "create_executor",
    "JoinExecutor",
    "MultiJoinExecutor",
    "JoinResult",
    "AggregateExecutor",
    "AggregateResult",
    "GroupResult",
    "HashAggregator",
    "DistinctExecutor",
    "AggregateFunctions",
    "SubqueryExecutor",
    "SubqueryContext",
    "SubqueryError",
    "ScalarSubqueryError",
    "CorrelatedSubqueryError"
]