"""
src/connectors/snowflake_connector.py — Snowflake connection manager
"""
from __future__ import annotations
import os, time, logging
from contextlib import contextmanager
from typing import Any, Generator
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

log = logging.getLogger(__name__)

class SnowflakeConnectionError(Exception): pass
class SnowflakeQueryError(Exception): pass

class SnowflakeConnector:
    """Manages Snowflake connections. Reads credentials from environment variables."""
    def __init__(self, config: dict | None = None) -> None:
        self.config = config or self._from_env()
        self._conn = None

    @staticmethod
    def _from_env() -> dict:
        required = ["SNOWFLAKE_ACCOUNT","SNOWFLAKE_USER","SNOWFLAKE_PASSWORD","SNOWFLAKE_DATABASE","SNOWFLAKE_SCHEMA","SNOWFLAKE_WAREHOUSE"]
        missing = [k for k in required if not os.getenv(k)]
        if missing:
            raise SnowflakeConnectionError(f"Missing environment variables: {missing}")
        cfg = {k.replace("SNOWFLAKE_","").lower(): os.environ[k] for k in required}
        if os.getenv("SNOWFLAKE_ROLE"):
            cfg["role"] = os.environ["SNOWFLAKE_ROLE"]
        return cfg

    def connect(self) -> None:
        try:
            import snowflake.connector
            self._conn = snowflake.connector.connect(**self.config)
            log.info("Connected to Snowflake: account=%s db=%s", self.config.get("account"), self.config.get("database"))
        except Exception as exc:
            raise SnowflakeConnectionError(f"Failed to connect: {exc}") from exc

    def disconnect(self) -> None:
        if self._conn:
            try: self._conn.close()
            except Exception as e: log.warning("Error closing connection: %s", e)
            finally: self._conn = None

    @contextmanager
    def connection(self) -> Generator:
        self.connect()
        try: yield self
        finally: self.disconnect()

    def is_connected(self) -> bool:
        return self._conn is not None and not self._conn.is_closed()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)
    def execute(self, sql: str, params: tuple | None = None) -> list[dict]:
        if not self.is_connected():
            raise SnowflakeConnectionError("Not connected. Call connect() first.")
        try:
            import snowflake.connector
            with self._conn.cursor(snowflake.connector.DictCursor) as cur:
                start = time.perf_counter()
                cur.execute(sql, params)
                rows = cur.fetchall()
                log.info("Query returned %d rows in %.3fs", len(rows), time.perf_counter() - start)
                return rows
        except Exception as exc:
            raise SnowflakeQueryError(f"Query failed: {exc}") from exc

    def execute_df(self, sql: str, params: tuple | None = None) -> pd.DataFrame:
        return pd.DataFrame(self.execute(sql, params))

    def execute_scalar(self, sql: str, params: tuple | None = None) -> Any:
        rows = self.execute(sql, params)
        if not rows: return None
        return next(iter(rows[0].values()))

    def execute_many(self, sql: str, data: list[tuple]) -> int:
        if not self.is_connected():
            raise SnowflakeConnectionError("Not connected.")
        with self._conn.cursor() as cur:
            cur.executemany(sql, data)
            return cur.rowcount

    def table_exists(self, table: str, schema: str | None = None) -> bool:
        schema = schema or self.config.get("schema", "")
        sql = "SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=UPPER(%s) AND TABLE_NAME=UPPER(%s)"
        return int(self.execute_scalar(sql, (schema, table)) or 0) > 0

    def get_row_count(self, table: str, where: str = "") -> int:
        sql = f"SELECT COUNT(1) FROM {table}" + (f" WHERE {where}" if where else "")
        return int(self.execute_scalar(sql) or 0)

    def get_column_names(self, table: str) -> list[str]:
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table} LIMIT 0")
            return [d[0] for d in cur.description]

    def get_table_schema(self, table: str, schema: str | None = None) -> list[dict]:
        schema = schema or self.config.get("schema", "")
        sql = """SELECT COLUMN_NAME,DATA_TYPE,IS_NULLABLE,CHARACTER_MAXIMUM_LENGTH
                 FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_SCHEMA=UPPER(%s) AND TABLE_NAME=UPPER(%s)
                 ORDER BY ORDINAL_POSITION"""
        return self.execute(sql, (schema, table))

    def begin_transaction(self) -> None: self.execute("BEGIN")
    def commit(self) -> None: self.execute("COMMIT")
    def rollback(self) -> None: self.execute("ROLLBACK")
