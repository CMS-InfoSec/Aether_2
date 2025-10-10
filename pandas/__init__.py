"""Lightweight pandas compatibility shim used in test environments.

This module intentionally implements only the tiny subset of the pandas API
that the reporting stack exercises inside this repository.  It is **not** a
general replacement for pandas, but it provides enough behaviour for the
regression suite to execute when the real dependency is unavailable.  The
implementation focuses on ergonomics rather than performance – all
operations operate on Python lists and dictionaries.
"""

from __future__ import annotations

from collections import defaultdict
import json
from datetime import date, datetime, timedelta, timezone
import math
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Sequence

__all__ = [
    "DataFrame",
    "Series",
    "NA",
    "merge",
    "date_range",
    "to_datetime",
    "to_numeric",
    "isna",
    "read_parquet",
]


NA = object()


def _is_na(value: Any) -> bool:
    return value is None or value is NA


def _ensure_list(values: Iterable[Any]) -> List[Any]:
    if isinstance(values, list):
        return values
    return list(values)


class _SeriesImpl:
    """Minimal Series implementation backed by a Python list."""

    def __init__(
        self,
        data: Iterable[Any] | None = None,
        *,
        index: Sequence[Any] | None = None,
        dtype: type | str | None = None,
    ) -> None:
        values = [] if data is None else _ensure_list(data)
        if dtype is not None:
            values = [self._coerce_dtype(value, dtype) for value in values]
        self._data: List[Any] = values
        self.index: List[Any] = list(index) if index is not None else list(range(len(values)))

    # ------------------------------------------------------------------
    # Representation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _coerce_dtype(value: Any, dtype: type | str) -> Any:
        if _is_na(value):
            return 0.0 if dtype in (float, "float") else value
        if dtype in (float, "float"):
            try:
                return float(value)
            except (TypeError, ValueError):
                return 0.0
        if dtype in (int, "int"):
            try:
                return int(value)
            except (TypeError, ValueError):
                return 0
        if dtype in (str, "str"):
            return "" if value is None else str(value)
        return value

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self._data)

    def __iter__(self) -> Iterator[Any]:  # pragma: no cover - trivial
        return iter(self._data)

    def __getitem__(self, idx: int | str) -> Any:
        if isinstance(idx, int):
            return self._data[idx]
        if isinstance(idx, str):
            try:
                position = self.index.index(idx)
            except ValueError:
                raise KeyError(idx) from None
            return self._data[position]
        raise TypeError("Invalid index type")

    def __setitem__(self, idx: int | str, value: Any) -> None:
        if isinstance(idx, int):
            self._data[idx] = value
            return
        if isinstance(idx, str):
            try:
                position = self.index.index(idx)
            except ValueError:
                raise KeyError(idx) from None
            self._data[position] = value
            return
        raise TypeError("Invalid index type")

    # ------------------------------------------------------------------
    # Numeric helpers
    # ------------------------------------------------------------------

    def _binary_op(self, other: Any, func: Callable[[Any, Any], Any]) -> "_SeriesImpl":
        if isinstance(other, _SeriesImpl):
            result = [func(a, b) for a, b in zip(self._data, other._data)]
        else:
            result = [func(a, other) for a in self._data]
        return _SeriesImpl(result, index=self.index)

    def __add__(self, other: Any) -> "_SeriesImpl":
        return self._binary_op(other, lambda a, b: (a or 0) + (b or 0))

    __radd__ = __add__

    def __sub__(self, other: Any) -> "_SeriesImpl":
        return self._binary_op(other, lambda a, b: (a or 0) - (b or 0))

    def __mul__(self, other: Any) -> "_SeriesImpl":
        return self._binary_op(other, lambda a, b: (a or 0) * (b or 0))

    __rmul__ = __mul__

    def __truediv__(self, other: Any) -> "_SeriesImpl":
        def _divide(a: Any, b: Any) -> Any:
            if isinstance(b, _SeriesImpl):  # pragma: no cover - handled by _binary_op
                raise TypeError
            if b in (0, 0.0, None, NA):
                return None
            return (a or 0) / b

        if isinstance(other, _SeriesImpl):
            result: List[Any] = []
            for a, b in zip(self._data, other._data):
                if b in (0, 0.0, None, NA):
                    result.append(None)
                else:
                    result.append((a or 0) / b)
            return _SeriesImpl(result, index=self.index)
        return self._binary_op(other, _divide)

    def __rtruediv__(self, other: Any) -> "_SeriesImpl":
        result = []
        for value in self._data:
            if value in (0, 0.0, None, NA):
                result.append(None)
            else:
                result.append((other or 0) / value)
        return _SeriesImpl(result, index=self.index)

    def __eq__(self, other: Any) -> "_SeriesImpl":  # pragma: no cover - simple
        return _SeriesImpl([value == other for value in self._data], index=self.index)

    def __ne__(self, other: Any) -> "_SeriesImpl":  # pragma: no cover - simple
        return _SeriesImpl([value != other for value in self._data], index=self.index)

    def _compare(self, other: Any, op: Callable[[Any, Any], bool]) -> "_SeriesImpl":
        if isinstance(other, _SeriesImpl):
            comparisons = [op(a, b) for a, b in zip(self._data, other._data)]
        else:
            comparisons = [False if _is_na(a) else op(a, other) for a in self._data]
        return _SeriesImpl(comparisons, index=self.index)

    def __lt__(self, other: Any) -> "_SeriesImpl":  # pragma: no cover - simple
        return self._compare(other, lambda a, b: a < b)

    def __le__(self, other: Any) -> "_SeriesImpl":  # pragma: no cover - simple
        return self._compare(other, lambda a, b: a <= b)

    def __gt__(self, other: Any) -> "_SeriesImpl":  # pragma: no cover - simple
        return self._compare(other, lambda a, b: a > b)

    def __ge__(self, other: Any) -> "_SeriesImpl":  # pragma: no cover - simple
        return self._compare(other, lambda a, b: a >= b)

    def __and__(self, other: Any) -> "_SeriesImpl":  # pragma: no cover - simple
        if isinstance(other, _SeriesImpl):
            return _SeriesImpl([bool(a) and bool(b) for a, b in zip(self._data, other._data)], index=self.index)
        return _SeriesImpl([bool(a) and bool(other) for a in self._data], index=self.index)

    def __or__(self, other: Any) -> "_SeriesImpl":  # pragma: no cover - simple
        if isinstance(other, _SeriesImpl):
            return _SeriesImpl([bool(a) or bool(b) for a, b in zip(self._data, other._data)], index=self.index)
        return _SeriesImpl([bool(a) or bool(other) for a in self._data], index=self.index)

    # ------------------------------------------------------------------
    # Pandas-like helpers
    # ------------------------------------------------------------------

    def astype(self, dtype: type | str) -> "_SeriesImpl":
        return _SeriesImpl([self._coerce_dtype(value, dtype) for value in self._data], index=self.index)

    def fillna(self, value: Any) -> "_SeriesImpl":
        if isinstance(value, _SeriesImpl):
            filled = [
                replacement if _is_na(original) else original
                for original, replacement in zip(self._data, value._data)
            ]
        else:
            filled = [value if _is_na(original) else original for original in self._data]
        return _SeriesImpl(filled, index=self.index)

    def replace(self, mapping: Mapping[Any, Any]) -> "_SeriesImpl":
        replaced = [mapping.get(value, value) for value in self._data]
        return _SeriesImpl(replaced, index=self.index)

    def abs(self) -> "_SeriesImpl":
        return _SeriesImpl(
            [abs(value) if value is not None else None for value in self._data], index=self.index
        )

    def sum(self) -> float:
        total = 0.0
        for value in self._data:
            if isinstance(value, bool):
                total += 1.0 if value else 0.0
            elif not _is_na(value):
                total += float(value)
        return total

    def count(self) -> int:
        return sum(0 if _is_na(value) else 1 for value in self._data)

    def nunique(self) -> int:
        return len({value for value in self._data if not _is_na(value)})

    def min(self) -> Any:  # pragma: no cover - simple
        values = [value for value in self._data if not _is_na(value)]
        return min(values) if values else None

    def max(self) -> Any:  # pragma: no cover - simple
        values = [value for value in self._data if not _is_na(value)]
        return max(values) if values else None

    def all(self) -> bool:  # pragma: no cover - simple
        return all(bool(value) for value in self._data)

    def std(self) -> float:
        values = [float(value) for value in self._data if not _is_na(value)]
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
        return variance ** 0.5

    def pct_change(self) -> "_SeriesImpl":
        changes: List[Any] = [None]
        if not self._data:
            return _SeriesImpl(changes[:1], index=self.index)
        prev = self._data[0]
        for value in self._data[1:]:
            if prev in (None, 0, 0.0):
                changes.append(None)
            else:
                changes.append((value - prev) / prev)
            prev = value
        return _SeriesImpl(changes, index=self.index)

    def dropna(self) -> "_SeriesImpl":  # pragma: no cover - simple
        return _SeriesImpl([value for value in self._data if not _is_na(value)])

    def apply(self, func: Callable[[Any], Any]) -> "_SeriesImpl":
        return _SeriesImpl([func(value) for value in self._data], index=self.index)

    def isna(self) -> "_SeriesImpl":  # pragma: no cover - simple
        return _SeriesImpl([_is_na(value) for value in self._data], index=self.index)

    def tolist(self) -> List[Any]:
        return list(self._data)

    def to_numpy(self) -> List[Any]:  # pragma: no cover - simple alias
        return list(self._data)

    def copy(self) -> "_SeriesImpl":  # pragma: no cover - simple
        return _SeriesImpl(list(self._data), index=self.index)

    def drop_duplicates(self) -> "_SeriesImpl":
        seen: List[Any] = []
        unique: List[Any] = []
        for value in self._data:
            if value not in seen:
                seen.append(value)
                unique.append(value)
        return _SeriesImpl(unique)

    def quantile(self, q: float) -> Any:
        values = [float(value) for value in self._data if not _is_na(value)]
        if not values:
            return None
        values.sort()
        if q <= 0:
            return values[0]
        if q >= 1:
            return values[-1]
        position = (len(values) - 1) * q
        lower = math.floor(position)
        upper = math.ceil(position)
        if lower == upper:
            return values[lower]
        fraction = position - lower
        return values[lower] + (values[upper] - values[lower]) * fraction

    def between(self, left: float, right: float, inclusive: str = "both") -> "_SeriesImpl":
        results = []
        for value in self._data:
            if _is_na(value):
                results.append(False)
                continue
            lower_ok = value >= left if inclusive in {"both", "right"} else value > left
            upper_ok = value <= right if inclusive in {"both", "left"} else value < right
            results.append(lower_ok and upper_ok)
        return _SeriesImpl(results, index=self.index)

    def clip(self, lower: float | None = None, upper: float | None = None) -> "_SeriesImpl":
        clipped: List[Any] = []
        for value in self._data:
            if _is_na(value):
                clipped.append(value)
                continue
            new_value = value
            if lower is not None and value < lower:
                new_value = lower
            if upper is not None and new_value > upper:
                new_value = upper
            clipped.append(new_value)
        return _SeriesImpl(clipped, index=self.index)

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def dt(self) -> "_DateTimeAccessor":
        return _DateTimeAccessor(self)

    @property
    def str(self) -> "_StringAccessor":  # pragma: no cover - simple
        return _StringAccessor(self)


class _StringAccessor:
    def __init__(self, series: _SeriesImpl) -> None:
        self._series = series

    def lower(self) -> _SeriesImpl:
        return _SeriesImpl(
            [value.lower() if isinstance(value, str) else value for value in self._series._data],
            index=self._series.index,
        )


class _DateTimeAccessor:
    def __init__(self, series: _SeriesImpl) -> None:
        self._series = series

    def tz_localize(self, tz: Any) -> _SeriesImpl:
        if tz is not None:  # pragma: no cover - the shim only supports UTC removal
            raise TypeError("Only tz_localize(None) is supported in the shim")

        naive_detected = any(
            (value is not None and getattr(value, "tzinfo", None) is None)
            for value in self._series._data
        )
        if naive_detected:
            raise TypeError("tz_localize is not supported on naive datetimes")
        localized = [
            value.replace(tzinfo=None) if value is not None else None
            for value in self._series._data
        ]
        return _SeriesImpl(localized, index=self._series.index)

    @property
    def date(self) -> _SeriesImpl:
        return _SeriesImpl(
            [value.date() if hasattr(value, "date") else value for value in self._series._data],
            index=self._series.index,
        )


class _DataFrameImpl:
    """Simplified DataFrame implementation built on top of Series objects."""

    def __init__(
        self,
        data: Iterable[Mapping[str, Any]] | Mapping[str, Sequence[Any]] | None = None,
        columns: Sequence[str] | None = None,
    ) -> None:
        rows: List[Dict[str, Any]]
        if data is None:
            rows = []
        elif isinstance(data, Mapping):
            columns = list(data.keys()) if columns is None else list(columns)
            values = [list(data[column]) for column in columns]
            rows = [dict(zip(columns, combination)) for combination in zip(*values)]
        else:
            rows = [dict(row) for row in data]
        self._rows: List[Dict[str, Any]] = rows
        if columns is None:
            column_set = []
            for row in rows:
                for key in row.keys():
                    if key not in column_set:
                        column_set.append(key)
            self.columns: List[str] = column_set
        else:
            self.columns = list(columns)
        for row in self._rows:
            for column in self.columns:
                row.setdefault(column, None)

    # ------------------------------------------------------------------
    # Convenience properties
    # ------------------------------------------------------------------

    @property
    def empty(self) -> bool:  # pragma: no cover - trivial
        return not self._rows

    @property
    def index(self) -> List[int]:  # pragma: no cover - simple
        return list(range(len(self._rows)))

    def copy(self) -> "DataFrame":  # pragma: no cover - simple
        return DataFrame([dict(row) for row in self._rows], columns=self.columns)

    def to_dict(self, orient: str = "records") -> List[Dict[str, Any]]:  # pragma: no cover - simple
        if orient != "records":
            raise ValueError("Only orient='records' is supported in the shim")
        return [dict(row) for row in self._rows]

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self._rows)

    # ------------------------------------------------------------------
    # Column access helpers
    # ------------------------------------------------------------------

    def _column_values(self, column: str) -> List[Any]:
        return [row.get(column) for row in self._rows]

    def __getitem__(self, key: str | Sequence[str]) -> _SeriesImpl | "_DataFrameImpl":
        if isinstance(key, str):
            return _SeriesImpl(self._column_values(key), index=self.index)
        selected_rows = [
            {column: row.get(column) for column in key}
            for row in self._rows
        ]
        return _DataFrameImpl(selected_rows, columns=list(key))

    def get(self, key: str, default: Any = None) -> _SeriesImpl:
        if key in self.columns:
            return _SeriesImpl(self._column_values(key), index=self.index)
        fill_value = default if default is not None else None
        return _SeriesImpl([fill_value for _ in self._rows], index=self.index)

    def __setitem__(self, key: str, value: Iterable[Any] | _SeriesImpl | Any) -> None:
        values = (
            list(value._data) if isinstance(value, _SeriesImpl) else _ensure_list(value)
            if isinstance(value, Iterable) and not isinstance(value, (str, bytes))
            else [value] * len(self._rows)
        )
        if len(values) != len(self._rows):
            raise ValueError("Column assignment length must match DataFrame length")
        if key not in self.columns:
            self.columns.append(key)
        for row, cell in zip(self._rows, values):
            row[key] = cell

    # ------------------------------------------------------------------
    # Indexing helpers
    # ------------------------------------------------------------------

    @property
    def loc(self) -> "_LocIndexer":
        return _LocIndexer(self)

    @property
    def iloc(self) -> "_ILocIndexer":
        return _ILocIndexer(self)

    # ------------------------------------------------------------------
    # Operations
    # ------------------------------------------------------------------

    def groupby(self, keys: Sequence[str], dropna: bool = True) -> "_GroupBy":
        return _GroupBy(self, keys, dropna=dropna)

    def reset_index(self) -> "_DataFrameImpl":  # pragma: no cover - simple
        return self.copy()

    def sort_values(self, by: Sequence[str], inplace: bool = False) -> "_DataFrameImpl":
        sorted_rows = sorted(self._rows, key=lambda row: tuple(row.get(column) for column in by))
        if inplace:
            self._rows = [dict(row) for row in sorted_rows]
            return self
        return _DataFrameImpl(sorted_rows, columns=self.columns)

    def fillna(self, value: Any | Mapping[str, Any]) -> "_DataFrameImpl":
        if isinstance(value, Mapping):
            rows = []
            for row in self._rows:
                filled = dict(row)
                for column, replacement in value.items():
                    if _is_na(filled.get(column)):
                        filled[column] = replacement
                rows.append(filled)
        else:
            rows = [
                {column: (value if _is_na(cell) else cell) for column, cell in row.items()}
                for row in self._rows
            ]
        return _DataFrameImpl(rows, columns=self.columns)

    def apply(self, func: Callable[[Mapping[str, Any]], Any], axis: int = 0) -> _SeriesImpl:
        if axis != 1:
            raise ValueError("Only axis=1 is supported in the shim")
        results = [func(row) for row in self._rows]
        return _SeriesImpl(results, index=self.index)

    def to_parquet(self, buffer: Any, index: bool = False) -> bytes:
        data = json.dumps(self.to_dict("records")).encode("utf-8")
        if hasattr(buffer, "write"):
            buffer.write(data)
            return data
        return data


class _LocIndexer:
    def __init__(self, frame: _DataFrameImpl) -> None:
        self._frame = frame

    def __getitem__(self, key: Any) -> _DataFrameImpl:
        if isinstance(key, tuple):
            rows_selector, column_selector = key
        else:
            rows_selector, column_selector = key, None

        selected_rows = self._select_rows(rows_selector)
        if column_selector is None or column_selector == slice(None):
            return _DataFrameImpl(selected_rows, columns=self._frame.columns)
        if isinstance(column_selector, str):
            values = [row.get(column_selector) for row in selected_rows]
            if len(values) == 1:
                return values[0]
            return _SeriesImpl(values)
        if isinstance(column_selector, Sequence):
            return _DataFrameImpl(
                [{column: row.get(column) for column in column_selector} for row in selected_rows],
                columns=list(column_selector),
            )
        raise TypeError("Unsupported column selector")

    def __setitem__(self, key: Any, value: Iterable[Any] | _SeriesImpl) -> None:
        if not isinstance(key, tuple) or len(key) != 2:
            raise TypeError("Assignment requires row and column selectors")
        row_selector, column_selector = key
        indices = self._row_indices(row_selector)
        if isinstance(column_selector, str):
            values = (
                list(value._data)
                if isinstance(value, _SeriesImpl)
                else _ensure_list(value)
            )
            if len(values) != len(indices):
                raise ValueError("Assignment length mismatch")
            if column_selector not in self._frame.columns:
                self._frame.columns.append(column_selector)
            for idx, cell in zip(indices, values):
                self._frame._rows[idx][column_selector] = cell
            return
        raise TypeError("Only single-column assignments are supported")

    def _select_rows(self, selector: Any) -> List[Dict[str, Any]]:
        if selector is None or selector == slice(None):
            return [dict(row) for row in self._frame._rows]
        if isinstance(selector, int):
            if selector < 0:
                selector += len(self._frame._rows)
            if selector < 0 or selector >= len(self._frame._rows):
                raise IndexError("Row index out of range")
            return [dict(self._frame._rows[selector])]
        if isinstance(selector, list):
            if selector and isinstance(selector[0], bool):
                return [dict(row) for row, flag in zip(self._frame._rows, selector) if flag]
            return [dict(self._frame._rows[idx]) for idx in selector]
        if isinstance(selector, _SeriesImpl):
            return [
                dict(row)
                for row, flag in zip(self._frame._rows, selector._data)
                if bool(flag)
            ]
        raise TypeError("Unsupported row selector")

    def _row_indices(self, selector: Any) -> List[int]:
        if selector == slice(None):
            return list(range(len(self._frame._rows)))
        if isinstance(selector, list) and selector and isinstance(selector[0], bool):
            return [idx for idx, flag in enumerate(selector) if flag]
        if isinstance(selector, _SeriesImpl):
            return [idx for idx, flag in enumerate(selector._data) if bool(flag)]
        raise TypeError("Unsupported row selector for assignment")


class _ILocIndexer:
    def __init__(self, frame: _DataFrameImpl) -> None:
        self._frame = frame

    def __getitem__(self, idx: int) -> _SeriesImpl:
        row = self._frame._rows[idx]
        return _SeriesImpl(
            [row.get(column) for column in self._frame.columns],
            index=self._frame.columns,
        )


class _GroupBy:
    def __init__(self, frame: _DataFrameImpl, keys: Sequence[str], dropna: bool) -> None:
        self._frame = frame
        self._keys = list(keys)
        self._dropna = dropna

    def agg(
        self,
        spec: Mapping[str, tuple[str, Any]] | None = None,
        **named: tuple[str, Any],
    ) -> _DataFrameImpl:
        groups: Dict[tuple[Any, ...], List[Dict[str, Any]]] = defaultdict(list)
        for row in self._frame._rows:
            key = tuple(row.get(column) for column in self._keys)
            if self._dropna and any(_is_na(value) for value in key):
                continue
            groups[key].append(row)

        results: List[Dict[str, Any]] = []
        combined_spec: Dict[str, tuple[str, Any]] = {}
        if spec:
            combined_spec.update(spec)
        for key_name, reducer in named.items():
            combined_spec[key_name] = reducer

        for key, rows in groups.items():
            result = {column: value for column, value in zip(self._keys, key)}
            for out_column, (source_column, reducer) in combined_spec.items():
                values = [row.get(source_column) for row in rows]
                series = _SeriesImpl(values)
                if callable(reducer):
                    result[out_column] = reducer(series)
                else:
                    result[out_column] = self._reduce(series, reducer)
            results.append(result)
        return _DataFrameImpl(results)

    @staticmethod
    def _reduce(series: _SeriesImpl, reducer: str) -> Any:
        reducer = reducer.lower()
        if reducer == "sum":
            return series.sum()
        if reducer == "count":
            return series.count()
        if reducer == "max":
            return series.max()
        if reducer == "min":
            return series.min()
        if reducer == "first":
            for value in series:
                if not _is_na(value):
                    return value
            return None
        if reducer == "last":
            for value in reversed(series.tolist()):
                if not _is_na(value):
                    return value
            return None
        if reducer == "nunique":
            return series.nunique()
        raise ValueError(f"Unsupported aggregator: {reducer}")


def Series(
    data: Iterable[Any] | None = None,
    *,
    index: Sequence[Any] | None = None,
    dtype: type | str | None = None,
) -> _SeriesImpl:
    return _SeriesImpl(data, index=index, dtype=dtype)


def merge(
    left: _DataFrameImpl,
    right: _DataFrameImpl,
    *,
    on: Sequence[str],
    how: str = "inner",
) -> _DataFrameImpl:
    on = list(on)
    right_map: Dict[tuple[Any, ...], List[Dict[str, Any]]] = defaultdict(list)
    for row in right._rows:
        key = tuple(row.get(column) for column in on)
        right_map[key].append(row)

    rows: List[Dict[str, Any]] = []
    matched_keys: set[tuple[Any, ...]] = set()
    for left_row in left._rows:
        key = tuple(left_row.get(column) for column in on)
        matches = right_map.get(key)
        if matches:
            matched_keys.add(key)
            for match in matches:
                merged = dict(left_row)
                for column, value in match.items():
                    if column not in on:
                        merged[column] = value
                rows.append(merged)
        else:
            merged = dict(left_row)
            for column in right.columns:
                if column not in on and column not in merged:
                    merged[column] = None
            rows.append(merged)

    if how == "outer":
        for key, matches in right_map.items():
            if key in matched_keys:
                continue
            for match in matches:
                merged = {column: match.get(column) for column in right.columns}
                for column in left.columns:
                    if column not in merged:
                        merged[column] = None
                rows.append(merged)
    return _DataFrameImpl(rows)


def to_datetime(values: Iterable[Any], utc: bool | None = None) -> _SeriesImpl:
    converted: List[Any] = []
    for value in values:
        if isinstance(value, datetime):
            converted.append(value)
            continue
        if value is None:
            converted.append(None)
            continue
        if isinstance(value, date):
            converted.append(datetime.combine(value, datetime.min.time()))
            continue
        if isinstance(value, str):
            normalised = value.replace("Z", "+00:00")
            converted.append(datetime.fromisoformat(normalised))
            continue
        converted.append(value)

    if utc:
        coerced: List[Any] = []
        for value in converted:
            if isinstance(value, datetime):
                if value.tzinfo is None:
                    coerced.append(value.replace(tzinfo=timezone.utc))
                else:
                    coerced.append(value.astimezone(timezone.utc))
            else:
                coerced.append(value)
        converted = coerced
    return _SeriesImpl(converted)


def to_numeric(values: Iterable[Any], errors: str = "raise") -> _SeriesImpl:
    converted: List[Any] = []
    for value in values:
        if _is_na(value):
            converted.append(None)
            continue
        try:
            converted.append(float(value))
        except (TypeError, ValueError):
            if errors == "coerce":
                converted.append(None)
            else:  # pragma: no cover - defensive branch
                raise
    return _SeriesImpl(converted)


def isna(value: Any) -> bool:
    return _is_na(value)


def _parse_frequency(freq: str) -> timedelta:
    if not freq:
        raise ValueError("Frequency string must not be empty")
    freq = freq.strip().lower()
    number_part = ""
    unit_part = ""
    for char in freq:
        if char.isdigit():
            number_part += char
        else:
            unit_part += char
    amount = int(number_part or 1)
    unit_map = {
        "s": timedelta(seconds=1),
        "sec": timedelta(seconds=1),
        "m": timedelta(minutes=1),
        "min": timedelta(minutes=1),
        "h": timedelta(hours=1),
        "hour": timedelta(hours=1),
        "d": timedelta(days=1),
        "day": timedelta(days=1),
    }
    if unit_part not in unit_map:
        raise ValueError(f"Unsupported frequency '{freq}'")
    return unit_map[unit_part] * amount


def date_range(start: datetime, periods: int, freq: str) -> _SeriesImpl:
    if not isinstance(start, datetime):
        raise TypeError("start must be a datetime instance")
    if periods < 0:
        raise ValueError("periods must be non-negative")
    delta = _parse_frequency(freq)
    values = [start + i * delta for i in range(periods)]
    return _SeriesImpl(values)


# Public constructors mirroring pandas naming.
def DataFrame(
    data: Iterable[Mapping[str, Any]] | Mapping[str, Sequence[Any]] | None = None,
    columns: Sequence[str] | None = None,
) -> _DataFrameImpl:
    return _DataFrameImpl(data, columns=columns)


def read_parquet(path: Any) -> _DataFrameImpl:
    if hasattr(path, "read_text"):
        raw = path.read_text(encoding="utf-8")  # type: ignore[attr-defined]
    else:
        with open(path, "r", encoding="utf-8") as handle:
            raw = handle.read()
    data = json.loads(raw) if raw else []
    if isinstance(data, list):
        return _DataFrameImpl(data)
    raise ValueError("Unsupported parquet payload")
