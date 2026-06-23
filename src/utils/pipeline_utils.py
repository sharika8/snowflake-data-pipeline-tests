"""
src/utils/pipeline_utils.py — Pipeline testing utilities
"""
from __future__ import annotations
import hashlib, logging, time
from typing import Any
import pandas as pd
import requests
from faker import Faker

log = logging.getLogger(__name__)
fake = Faker()

def assert_row_count_match(source_count: int, target_count: int, tolerance_pct: float = 0.0, label: str = "source→target") -> None:
    """Assert row counts match within a tolerance percentage."""
    if source_count == 0 and target_count == 0: return
    if source_count == 0:
        raise AssertionError(f"[{label}] Source is empty but target has {target_count} rows")
    diff_pct = abs(source_count - target_count) / source_count
    assert diff_pct <= tolerance_pct, (
        f"[{label}] Row count mismatch: source={source_count}, target={target_count}, "
        f"diff={diff_pct:.2%} exceeds tolerance {tolerance_pct:.2%}"
    )

def compute_row_hash(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    """Compute MD5 hash per row for reconciliation."""
    combined = df[columns].astype(str).agg("||".join, axis=1)
    return combined.map(lambda s: hashlib.md5(s.encode()).hexdigest())

def reconcile_datasets(source_df: pd.DataFrame, target_df: pd.DataFrame, key_columns: list[str], compare_columns: list[str] | None = None) -> dict:
    """Reconcile two DataFrames by key columns. Returns summary dict."""
    compare_columns = compare_columns or [c for c in source_df.columns if c not in key_columns]
    src = source_df.copy(); tgt = target_df.copy()
    for col in key_columns:
        src[col] = src[col].astype(str); tgt[col] = tgt[col].astype(str)
    src.set_index(key_columns, inplace=True); tgt.set_index(key_columns, inplace=True)
    src_keys = set(src.index.tolist()); tgt_keys = set(tgt.index.tolist())
    missing = src_keys - tgt_keys; extra = tgt_keys - src_keys; common = src_keys & tgt_keys
    changed = 0
    for key in common:
        sr = src.loc[key]; tr = tgt.loc[key]
        for col in compare_columns:
            if col in sr.index and col in tr.index and str(sr[col]) != str(tr[col]):
                changed += 1; break
    return {
        "total_source": len(src), "total_target": len(tgt),
        "matching": len(common) - changed, "missing_in_target": len(missing),
        "extra_in_target": len(extra), "changed": changed,
        "match_rate": round((len(common) - changed) / len(src), 4) if len(src) > 0 else 1.0,
    }

class AirflowClient:
    """Lightweight Airflow REST API client for pipeline tests."""
    def __init__(self, base_url: str, username: str = "airflow", password: str = "airflow") -> None:
        self.base_url = base_url.rstrip("/"); self.session = requests.Session()
        self.session.auth = (username, password); self.session.headers["Content-Type"] = "application/json"

    def _get(self, path: str) -> dict:
        r = self.session.get(f"{self.base_url}/api/v1{path}", timeout=15); r.raise_for_status(); return r.json()
    def _post(self, path: str, data: dict) -> dict:
        r = self.session.post(f"{self.base_url}/api/v1{path}", json=data, timeout=15); r.raise_for_status(); return r.json()
    def get_dag(self, dag_id: str) -> dict: return self._get(f"/dags/{dag_id}")
    def trigger_dag(self, dag_id: str, conf: dict | None = None) -> dict: return self._post(f"/dags/{dag_id}/dagRuns", {"conf": conf or {}})
    def get_dag_run(self, dag_id: str, run_id: str) -> dict: return self._get(f"/dags/{dag_id}/dagRuns/{run_id}")
    def wait_for_dag_run(self, dag_id: str, run_id: str, timeout_seconds: int = 300, poll_interval: int = 10) -> str:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            state = self.get_dag_run(dag_id, run_id).get("state","")
            if state in ("success","failed"): return state
            time.sleep(poll_interval)
        raise TimeoutError(f"DAG {dag_id}/{run_id} did not finish within {timeout_seconds}s")
    def assert_dag_success(self, dag_id: str, run_id: str) -> None:
        state = self.wait_for_dag_run(dag_id, run_id)
        assert state == "success", f"DAG {dag_id} run {run_id} state: {state}"

def generate_user_records(n: int, seed: int | None = None) -> list[dict]:
    if seed is not None: Faker.seed(seed)
    return [{"USER_ID": fake.uuid4(), "USERNAME": fake.user_name(), "EMAIL": fake.email(),
             "FIRST_NAME": fake.first_name(), "LAST_NAME": fake.last_name(),
             "COUNTRY": fake.country_code(), "CREATED_AT": fake.date_time_between("-2y","now").isoformat(),
             "IS_ACTIVE": fake.boolean(80)} for _ in range(n)]

def generate_order_records(n: int, user_ids: list, seed: int | None = None) -> list[dict]:
    if seed is not None: Faker.seed(seed)
    statuses = ["PENDING","CONFIRMED","SHIPPED","DELIVERED","CANCELLED"]
    return [{"ORDER_ID": fake.uuid4(), "USER_ID": fake.random_element(user_ids),
             "QUANTITY": fake.random_int(1,10), "UNIT_PRICE": round(fake.random_number(digits=3) + fake.random.random(), 2),
             "STATUS": fake.random_element(statuses), "ORDER_DATE": fake.date_between("-1y","today").isoformat(),
             "CURRENCY": fake.currency_code()} for _ in range(n)]

def generate_event_records(n: int, user_ids: list, seed: int | None = None) -> list[dict]:
    if seed is not None: Faker.seed(seed)
    event_types = ["PAGE_VIEW","CLICK","PURCHASE","SIGNUP","LOGOUT","ERROR"]
    return [{"EVENT_ID": fake.uuid4(), "USER_ID": fake.random_element(user_ids),
             "EVENT_TYPE": fake.random_element(event_types), "PAGE": fake.uri_path(),
             "SESSION_ID": fake.uuid4(), "TIMESTAMP": fake.date_time_between("-30d","now").isoformat()} for _ in range(n)]
