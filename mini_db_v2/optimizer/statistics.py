# START_MODULE_CONTRACT
# Module: mini_db_v2.optimizer.statistics
# Intent: Сбор и хранение статистики таблиц и колонок для cost-based optimizer.
# Dependencies: dataclasses, typing, datetime, threading
# END_MODULE_CONTRACT

# START_MODULE_MAP
# Exports: Statistics, TableStats, ColumnStats, HistogramBucket, StatisticsManager
# END_MODULE_MAP

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional
from datetime import datetime
import threading


# =============================================================================
# START_BLOCK_DATA_CLASSES
# =============================================================================

@dataclass
class TableStats:
    """
    [START_CONTRACT_TABLE_STATS]
    Intent: Статистика таблицы для cost estimation.
    Input: row_count - количество строк; page_count - количество страниц.
    Output: Структура для хранения статистики таблицы.
    [END_CONTRACT_TABLE_STATS]
    """
    row_count: int = 0
    page_count: int = 0
    last_analyze: Optional[datetime] = None
    table_name: str = ""
    
    # Estimated sizes
    avg_row_size: int = 0  # bytes


@dataclass
class HistogramBucket:
    """
    [START_CONTRACT_HISTOGRAM_BUCKET]
    Intent: Bucket гистограммы для оценки selectivity.
    Input: lower_bound, upper_bound - границы; count - количество значений.
    Output: Структура для хранения bucket гистограммы.
    [END_CONTRACT_HISTOGRAM_BUCKET]
    """
    lower_bound: Any
    upper_bound: Any
    count: int = 0
    distinct_count: int = 0  # distinct values in bucket


@dataclass
class ColumnStats:
    """
    [START_CONTRACT_COLUMN_STATS]
    Intent: Статистика колонки для selectivity estimation.
    Input: distinct_values - уникальные значения; null_count - NULL значения.
    Output: Структура для хранения статистики колонки.
    [END_CONTRACT_COLUMN_STATS]
    """
    column_name: str = ""
    distinct_values: int = 0
    null_count: int = 0
    null_fraction: float = 0.0
    
    # Min/max values for range estimation
    min_value: Any = None
    max_value: Any = None
    
    # Histogram for range selectivity (equi-depth)
    histogram: list[HistogramBucket] = field(default_factory=list)
    
    # Most common values (MCV)
    most_common_values: list[Any] = field(default_factory=list)
    most_common_freqs: list[float] = field(default_factory=list)
    
    # Correlation with physical order (-1 to 1)
    correlation: float = 0.0


# END_BLOCK_DATA_CLASSES


# =============================================================================
# START_BLOCK_STATISTICS
# =============================================================================

class Statistics:
    """
    [START_CONTRACT_STATISTICS]
    Intent: Хранилище статистики для всех таблиц базы данных.
    Input: table_stats - статистика таблиц; column_stats - статистика колонок.
    Output: API для доступа к статистике.
    [END_CONTRACT_STATISTICS]
    """
    
    def __init__(self):
        """Инициализация пустого хранилища статистики."""
        self._table_stats: dict[str, TableStats] = {}
        self._column_stats: dict[str, ColumnStats] = {}  # key: "table.column"
        self._lock = threading.RLock()
    
    def get_table_stats(self, table_name: str) -> Optional[TableStats]:
        """Возвращает статистику таблицы или None."""
        with self._lock:
            return self._table_stats.get(table_name)
    
    def get_column_stats(self, table_name: str, column_name: str) -> Optional[ColumnStats]:
        """Возвращает статистику колонки или None."""
        with self._lock:
            key = f"{table_name}.{column_name}"
            return self._column_stats.get(key)
    
    def set_table_stats(self, table_name: str, stats: TableStats) -> None:
        """Устанавливает статистику таблицы."""
        with self._lock:
            stats.table_name = table_name
            self._table_stats[table_name] = stats
    
    def set_column_stats(self, table_name: str, column_name: str, 
                         stats: ColumnStats) -> None:
        """Устанавливает статистику колонки."""
        with self._lock:
            key = f"{table_name}.{column_name}"
            stats.column_name = column_name
            self._column_stats[key] = stats
    
    def drop_table_stats(self, table_name: str) -> None:
        """Удаляет статистику таблицы и её колонок."""
        with self._lock:
            if table_name in self._table_stats:
                del self._table_stats[table_name]
            
            # Remove column stats
            prefix = f"{table_name}."
            keys_to_remove = [k for k in self._column_stats if k.startswith(prefix)]
            for key in keys_to_remove:
                del self._column_stats[key]
    
    def estimate_selectivity(self, table_name: str, column_name: str,
                            operator: str, value: Any) -> float:
        """
        [START_CONTRACT_ESTIMATE_SELECTIVITY]
        Intent: Оценить selectivity условия column op value.
        Input: table_name, column_name - колонка; operator - оператор; value - значение.
        Output: Оценка selectivity (0.0 - 1.0).
        [END_CONTRACT_ESTIMATE_SELECTIVITY]
        """
        col_stats = self.get_column_stats(table_name, column_name)
        table_stats = self.get_table_stats(table_name)
        
        if col_stats is None or table_stats is None:
            return 0.1  # Default selectivity
        
        if operator == "=":
            return self._estimate_eq_selectivity(col_stats, value)
        elif operator in ("<", "<="):
            return self._estimate_range_selectivity(col_stats, value, True)
        elif operator in (">", ">="):
            return self._estimate_range_selectivity(col_stats, value, False)
        elif operator == "BETWEEN":
            return 0.3  # Default for BETWEEN
        
        return 0.1  # Default
    
    def _estimate_eq_selectivity(self, stats: ColumnStats, value: Any) -> float:
        """Оценка selectivity для equality condition."""
        # Check MCV first
        for i, mcv in enumerate(stats.most_common_values):
            if mcv == value:
                return stats.most_common_freqs[i] if i < len(stats.most_common_freqs) else 0.1
        
        # Use distinct values estimate
        if stats.distinct_values > 0:
            return 1.0 / stats.distinct_values
        
        return 0.1
    
    def _estimate_range_selectivity(self, stats: ColumnStats, value: Any,
                                    is_lower: bool) -> float:
        """Оценка selectivity для range condition."""
        if stats.min_value is None or stats.max_value is None:
            return 0.3
        
        # Use histogram if available
        if stats.histogram:
            return self._estimate_from_histogram(stats.histogram, value, is_lower)
        
        # Linear interpolation
        try:
            range_size = self._get_range_size(stats.min_value, stats.max_value)
            if range_size == 0:
                return 0.5
            
            if is_lower:
                # col < value
                if value <= stats.min_value:
                    return 0.0
                if value >= stats.max_value:
                    return 1.0
                pos = self._get_range_size(stats.min_value, value)
                return pos / range_size
            else:
                # col > value
                if value >= stats.max_value:
                    return 0.0
                if value <= stats.min_value:
                    return 1.0
                pos = self._get_range_size(value, stats.max_value)
                return pos / range_size
        except (TypeError, ValueError):
            return 0.3
    
    def _estimate_from_histogram(self, histogram: list[HistogramBucket],
                                  value: Any, is_lower: bool) -> float:
        """Оценка selectivity из гистограммы."""
        total_count = sum(b.count for b in histogram)
        if total_count == 0:
            return 0.3
        
        cumulative = 0
        for bucket in histogram:
            if is_lower:
                if value <= bucket.lower_bound:
                    return cumulative / total_count
                if value <= bucket.upper_bound:
                    # Interpolate within bucket
                    bucket_frac = (value - bucket.lower_bound) / \
                                  (bucket.upper_bound - bucket.lower_bound + 1)
                    return (cumulative + bucket.count * bucket_frac) / total_count
            else:
                if value < bucket.lower_bound:
                    return cumulative / total_count
                if value < bucket.upper_bound:
                    bucket_frac = (bucket.upper_bound - value) / \
                                  (bucket.upper_bound - bucket.lower_bound + 1)
                    return (cumulative + bucket.count * bucket_frac) / total_count
            
            cumulative += bucket.count
        
        return 1.0 if is_lower else 0.0
    
    def _get_range_size(self, min_val: Any, max_val: Any) -> float:
        """Вычисляет размер диапазона."""
        if isinstance(min_val, (int, float)) and isinstance(max_val, (int, float)):
            return float(max_val - min_val)
        return 1.0


# END_BLOCK_STATISTICS


# =============================================================================
# START_BLOCK_STATISTICS_MANAGER
# =============================================================================

class StatisticsManager:
    """
    [START_CONTRACT_STATISTICS_MANAGER]
    Intent: Управление сбором статистики для таблиц базы данных.
    Input: database - база данных для анализа.
    Output: API для ANALYZE TABLE команд.
    [END_CONTRACT_STATISTICS_MANAGER]
    """
    
    def __init__(self, statistics: Statistics):
        """
        [START_CONTRACT_SM_INIT]
        Intent: Инициализация менеджера статистики.
        Input: statistics - хранилище статистики.
        Output: Готовый к работе менеджер.
        [END_CONTRACT_SM_INIT]
        """
        self._statistics = statistics
        self._histogram_buckets = 100  # Number of histogram buckets
    
    def analyze_table(self, table) -> TableStats:
        """
        [START_CONTRACT_ANALYZE_TABLE]
        Intent: Собрать статистику по таблице.
        Input: table - объект Table для анализа.
        Output: TableStats с собранной статистикой.
        [END_CONTRACT_ANALYZE_TABLE]
        """
        rows = table.select()
        row_count = len(rows)
        
        # Estimate page count (assuming 4KB pages)
        avg_row_size = self._estimate_row_size(table, rows)
        page_count = max(1, (row_count * avg_row_size) // 4096)
        
        table_stats = TableStats(
            row_count=row_count,
            page_count=page_count,
            last_analyze=datetime.now(),
            avg_row_size=avg_row_size
        )
        
        self._statistics.set_table_stats(table.name, table_stats)
        
        # Analyze each column
        for col_name in table.column_names:
            col_stats = self._analyze_column(table.name, col_name, rows)
            self._statistics.set_column_stats(table.name, col_name, col_stats)
        
        return table_stats
    
    def _analyze_column(self, table_name: str, column_name: str,
                        rows: list) -> ColumnStats:
        """Анализирует колонку и собирает статистику."""
        values = []
        null_count = 0
        
        for row in rows:
            val = row.data.get(column_name)
            if val is None:
                null_count += 1
            else:
                values.append(val)
        
        # Distinct values
        distinct_values = len(set(values))
        
        # Min/max
        min_value = None
        max_value = None
        if values:
            try:
                min_value = min(values)
                max_value = max(values)
            except TypeError:
                pass  # Incomparable types
        
        # Build histogram
        histogram = self._build_histogram(values)
        
        # MCV (Most Common Values)
        mcv, mcf = self._compute_mcv(values)
        
        # Null fraction
        null_fraction = null_count / len(rows) if rows else 0.0
        
        return ColumnStats(
            column_name=column_name,
            distinct_values=distinct_values,
            null_count=null_count,
            null_fraction=null_fraction,
            min_value=min_value,
            max_value=max_value,
            histogram=histogram,
            most_common_values=mcv,
            most_common_freqs=mcf
        )
    
    def _estimate_row_size(self, table, rows: list) -> int:
        """Оценивает средний размер строки в байтах."""
        if not rows:
            return 0
        
        total_size = 0
        sample_size = min(100, len(rows))
        
        for i in range(sample_size):
            row = rows[i]
            for col_name, value in row.data.items():
                total_size += self._estimate_value_size(value)
        
        return total_size // sample_size if sample_size > 0 else 0
    
    def _estimate_value_size(self, value: Any) -> int:
        """Оценивает размер значения в байтах."""
        if value is None:
            return 1
        if isinstance(value, bool):
            return 1
        if isinstance(value, int):
            return 8
        if isinstance(value, float):
            return 8
        if isinstance(value, str):
            return len(value) + 4  # Length prefix
        return 8  # Default
    
    def _build_histogram(self, values: list) -> list[HistogramBucket]:
        """Строит equi-depth гистограмму."""
        if not values:
            return []
        
        # Sort values
        try:
            sorted_values = sorted(values)
        except TypeError:
            return []  # Incomparable types
        
        n = len(sorted_values)
        bucket_size = max(1, n // self._histogram_buckets)
        histogram = []
        
        for i in range(0, n, bucket_size):
            end = min(i + bucket_size, n)
            bucket_values = sorted_values[i:end]
            
            if bucket_values:
                bucket = HistogramBucket(
                    lower_bound=bucket_values[0],
                    upper_bound=bucket_values[-1],
                    count=len(bucket_values),
                    distinct_count=len(set(bucket_values))
                )
                histogram.append(bucket)
        
        return histogram
    
    def _compute_mcv(self, values: list, max_mcv: int = 10) -> tuple[list, list]:
        """Вычисляет Most Common Values и их частоты."""
        if not values:
            return [], []
        
        # Count frequencies
        freq = {}
        for v in values:
            freq[v] = freq.get(v, 0) + 1
        
        # Sort by frequency
        sorted_items = sorted(freq.items(), key=lambda x: -x[1])
        
        # Take top MCV
        mcv = []
        mcf = []
        total = len(values)
        
        for value, count in sorted_items[:max_mcv]:
            mcv.append(value)
            mcf.append(count / total if total > 0 else 0.0)
        
        return mcv, mcf


# END_BLOCK_STATISTICS_MANAGER