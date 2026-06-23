"""
src/validators/schema_validator.py — Schema drift detection
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any

@dataclass
class ColumnDiff:
    column: str; issue: str; expected: Any = None; actual: Any = None

@dataclass
class SchemaDiffResult:
    table: str; drifted: bool; diffs: list[ColumnDiff]
    def __str__(self):
        if not self.drifted: return f"[PASS] {self.table}: no schema drift"
        lines = [f"[FAIL] {self.table}: {len(self.diffs)} drift(s)"]
        for d in self.diffs: lines.append(f"  {d.issue.upper()} {d.column}: expected={d.expected!r}, actual={d.actual!r}")
        return "\n".join(lines)

class SchemaValidator:
    """Validates Snowflake table schemas against expected definitions."""
    TYPE_ALIASES = {"TEXT":"VARCHAR","STRING":"VARCHAR","INT":"NUMBER","INTEGER":"NUMBER","BIGINT":"NUMBER",
                    "FLOAT":"FLOAT","DOUBLE":"FLOAT","BOOL":"BOOLEAN","DATETIME":"TIMESTAMP_NTZ","TIMESTAMP":"TIMESTAMP_NTZ"}

    def __init__(self, connector) -> None:
        self.connector = connector

    def _normalise_type(self, t: str) -> str:
        t = t.upper().split("(")[0].strip()
        return self.TYPE_ALIASES.get(t, t)

    def get_live_schema(self, table: str, schema: str | None = None) -> dict[str, dict]:
        rows = self.connector.get_table_schema(table, schema)
        return {row["COLUMN_NAME"].upper(): {
            "type": self._normalise_type(row.get("DATA_TYPE","")),
            "nullable": row.get("IS_NULLABLE","YES").upper() == "YES"
        } for row in rows}

    def compare(self, table: str, expected: dict[str, dict], schema: str | None = None) -> SchemaDiffResult:
        live = self.get_live_schema(table, schema)
        exp = {k.upper(): v for k, v in expected.items()}
        diffs: list[ColumnDiff] = []
        for col, ep in exp.items():
            if col not in live:
                diffs.append(ColumnDiff(col, "missing", ep, None)); continue
            lp = live[col]
            exp_type = self._normalise_type(ep.get("type",""))
            if exp_type and exp_type != lp["type"]:
                diffs.append(ColumnDiff(col, "type_changed", exp_type, lp["type"]))
            if "nullable" in ep and ep["nullable"] != lp["nullable"]:
                diffs.append(ColumnDiff(col, "nullable_changed", ep["nullable"], lp["nullable"]))
        for col in live:
            if col not in exp:
                diffs.append(ColumnDiff(col, "added", None, live[col]))
        return SchemaDiffResult(table=table, drifted=bool(diffs), diffs=diffs)

    def assert_no_drift(self, table: str, expected: dict[str, dict], schema: str | None = None) -> SchemaDiffResult:
        result = self.compare(table, expected, schema)
        assert not result.drifted, str(result)
        return result

    def assert_columns_exist(self, table: str, *columns: str) -> None:
        live = self.get_live_schema(table)
        missing = [c.upper() for c in columns if c.upper() not in live]
        assert not missing, f"Table {table} missing columns: {missing}"
