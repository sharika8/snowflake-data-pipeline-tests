"""
src/validators/data_quality.py — Data quality assertion library
"""
from __future__ import annotations
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
import pandas as pd

@dataclass
class DQResult:
    check: str; passed: bool; message: str; details: dict = field(default_factory=dict)
    def __str__(self): return f"[{'PASS' if self.passed else 'FAIL'}] {self.check}: {self.message}"

class DQAssertionError(AssertionError):
    def __init__(self, results: list[DQResult]) -> None:
        failures = [r for r in results if not r.passed]
        super().__init__("\n" + "\n".join(str(r) for r in failures))
        self.results = results; self.failures = failures

class DataQualityValidator:
    """Fluent DQ validator. Chain checks then call .assert_all()."""
    def __init__(self, df: pd.DataFrame, label: str = "dataset") -> None:
        self.df = df; self.label = label; self._results: list[DQResult] = []

    def has_columns(self, *columns: str) -> "DataQualityValidator":
        missing = [c for c in columns if c not in self.df.columns]
        self._results.append(DQResult(f"has_columns({','.join(columns)})", not missing,
            "All required columns present" if not missing else f"Missing: {missing}"))
        return self

    def not_null(self, *columns: str) -> "DataQualityValidator":
        for col in columns:
            if col not in self.df.columns:
                self._results.append(DQResult(f"not_null({col})", False, f"Column '{col}' not found")); continue
            n = int(self.df[col].isna().sum())
            self._results.append(DQResult(f"not_null({col})", n == 0,
                "No nulls" if n == 0 else f"{n} null values found", {"null_count": n}))
        return self

    def null_rate(self, column: str, max_rate: float = 0.05) -> "DataQualityValidator":
        if column not in self.df.columns:
            self._results.append(DQResult(f"null_rate({column})", False, "Column not found")); return self
        rate = self.df[column].isna().mean()
        self._results.append(DQResult(f"null_rate({column},max={max_rate:.0%})", rate <= max_rate,
            f"Null rate {rate:.2%} {'≤' if rate <= max_rate else '>'} {max_rate:.0%}", {"null_rate": round(rate,4)}))
        return self

    def unique(self, *columns: str) -> "DataQualityValidator":
        cols = list(columns)
        if any(c not in self.df.columns for c in cols):
            self._results.append(DQResult(f"unique({','.join(cols)})", False, "Column not found")); return self
        n = int(self.df.duplicated(subset=cols).sum())
        self._results.append(DQResult(f"unique({','.join(cols)})", n == 0,
            "All values unique" if n == 0 else f"{n} duplicate rows", {"duplicate_count": n}))
        return self

    def row_count(self, exact: int | None = None, min: int | None = None, max: int | None = None) -> "DataQualityValidator":
        n = len(self.df)
        if exact is not None: passed, msg = n == exact, f"Row count {n} == {exact}" if n == exact else f"Expected {exact}, got {n}"
        elif min is not None and max is not None: passed, msg = min <= n <= max, f"Row count {n} in [{min},{max}]" if min<=n<=max else f"Row count {n} outside [{min},{max}]"
        elif min is not None: passed, msg = n >= min, f"Row count {n} >= {min}" if n>=min else f"Row count {n} < {min}"
        elif max is not None: passed, msg = n <= max, f"Row count {n} <= {max}" if n<=max else f"Row count {n} > {max}"
        else: passed, msg = n > 0, f"Row count {n} > 0" if n>0 else "Dataset is empty"
        self._results.append(DQResult("row_count", passed, msg, {"row_count": n}))
        return self

    def value_range(self, column: str, min_val: Any = None, max_val: Any = None) -> "DataQualityValidator":
        if column not in self.df.columns:
            self._results.append(DQResult(f"value_range({column})", False, "Column not found")); return self
        s = self.df[column].dropna(); v = 0
        if min_val is not None: v += int((s < min_val).sum())
        if max_val is not None: v += int((s > max_val).sum())
        self._results.append(DQResult(f"value_range({column},min={min_val},max={max_val})", v == 0,
            "All values in range" if v == 0 else f"{v} out-of-range values",
            {"violations": v, "actual_min": float(s.min()), "actual_max": float(s.max())}))
        return self

    def allowed_values(self, column: str, values: list) -> "DataQualityValidator":
        if column not in self.df.columns:
            self._results.append(DQResult(f"allowed_values({column})", False, "Column not found")); return self
        invalid = self.df[column].dropna()[~self.df[column].dropna().isin(values)]
        self._results.append(DQResult(f"allowed_values({column})", len(invalid) == 0,
            "All values in allowed set" if not len(invalid) else f"Invalid values: {sorted(str(v) for v in invalid.unique())[:5]}",
            {"invalid_count": len(invalid)}))
        return self

    def matches_pattern(self, column: str, pattern: str) -> "DataQualityValidator":
        if column not in self.df.columns:
            self._results.append(DQResult(f"matches_pattern({column})", False, "Column not found")); return self
        n = int((~self.df[column].dropna().astype(str).str.match(pattern, na=False)).sum())
        self._results.append(DQResult(f"matches_pattern({column})", n == 0,
            "All values match pattern" if n == 0 else f"{n} values don't match"))
        return self

    def is_email(self, column: str) -> "DataQualityValidator":
        return self.matches_pattern(column, r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

    def is_date(self, column: str, fmt: str = "%Y-%m-%d") -> "DataQualityValidator":
        if column not in self.df.columns:
            self._results.append(DQResult(f"is_date({column})", False, "Column not found")); return self
        invalid = sum(1 for v in self.df[column].dropna() if not self._try_parse_date(str(v)[:10], fmt))
        self._results.append(DQResult(f"is_date({column})", invalid == 0,
            "All values are valid dates" if invalid == 0 else f"{invalid} invalid date values"))
        return self

    @staticmethod
    def _try_parse_date(val, fmt):
        try: datetime.strptime(val, fmt); return True
        except: return False

    def referential_integrity(self, column: str, reference_df: pd.DataFrame, ref_column: str) -> "DataQualityValidator":
        if column not in self.df.columns:
            self._results.append(DQResult(f"referential_integrity({column})", False, "Column not found")); return self
        orphans = self.df[column].dropna()[~self.df[column].dropna().isin(reference_df[ref_column])]
        self._results.append(DQResult(f"referential_integrity({column}→{ref_column})", len(orphans) == 0,
            "All references valid" if not len(orphans) else f"{len(orphans)} orphaned references",
            {"orphan_count": len(orphans)}))
        return self

    def schema_matches(self, expected: dict[str, type]) -> "DataQualityValidator":
        missing = [c for c in expected if c not in self.df.columns]
        mismatches = [f"{c}: expected {t}, got {self.df[c].dtype}" for c,t in expected.items() if c in self.df.columns and not pd.api.types.is_dtype_equal(self.df[c].dtype, t)]
        passed = not missing and not mismatches
        self._results.append(DQResult("schema_matches", passed, "Schema matches" if passed else f"Missing:{missing} Mismatches:{mismatches}"))
        return self

    def is_fresh(self, column: str, max_age_hours: float = 24) -> "DataQualityValidator":
        if column not in self.df.columns:
            self._results.append(DQResult(f"is_fresh({column})", False, "Column not found")); return self
        try:
            latest = pd.to_datetime(self.df[column]).max()
            age = (pd.Timestamp.now() - latest).total_seconds() / 3600
            self._results.append(DQResult(f"is_fresh({column},max={max_age_hours}h)", age <= max_age_hours,
                f"Latest record is {age:.1f}h old", {"age_hours": round(age,2)}))
        except Exception as e:
            self._results.append(DQResult(f"is_fresh({column})", False, f"Error: {e}"))
        return self

    def get_results(self) -> list[DQResult]: return self._results

    def assert_all(self) -> "DataQualityValidator":
        if any(not r.passed for r in self._results): raise DQAssertionError(self._results)
        return self

    def summary(self) -> str:
        passed = sum(1 for r in self._results if r.passed)
        return f"DQ Summary [{self.label}]: {passed}/{len(self._results)} checks passed\n" + "\n".join(f"  {r}" for r in self._results)
