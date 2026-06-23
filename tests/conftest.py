"""tests/conftest.py — Shared pytest fixtures"""
from __future__ import annotations
import os, logging, pytest
import pandas as pd

log = logging.getLogger(__name__)

SNOWFLAKE_AVAILABLE = all(os.getenv(v) for v in [
    "SNOWFLAKE_ACCOUNT","SNOWFLAKE_USER","SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_DATABASE","SNOWFLAKE_SCHEMA","SNOWFLAKE_WAREHOUSE"])
AIRFLOW_AVAILABLE = bool(os.getenv("AIRFLOW_BASE_URL"))

requires_snowflake = pytest.mark.skipif(not SNOWFLAKE_AVAILABLE, reason="Snowflake credentials not configured")
requires_airflow   = pytest.mark.skipif(not AIRFLOW_AVAILABLE,   reason="Airflow not configured")

@pytest.fixture(scope="session")
def sf_connector():
    if not SNOWFLAKE_AVAILABLE: pytest.skip("Snowflake credentials not configured")
    from src.connectors.snowflake_connector import SnowflakeConnector
    c = SnowflakeConnector(); c.connect(); yield c; c.disconnect()

@pytest.fixture(scope="function")
def sf_transaction(sf_connector):
    sf_connector.begin_transaction(); yield sf_connector; sf_connector.rollback()

@pytest.fixture(scope="session")
def schema_validator(sf_connector):
    from src.validators.schema_validator import SchemaValidator
    return SchemaValidator(sf_connector)

@pytest.fixture
def mock_users_df():
    return pd.DataFrame({
        "USER_ID":    ["u1","u2","u3","u4","u5"],
        "USERNAME":   ["alice","bob","carol","dave","eve"],
        "EMAIL":      ["alice@test.com","bob@test.com","carol@test.com","dave@test.com","eve@test.com"],
        "COUNTRY":    ["GB","US","DE","FR","GB"],
        "IS_ACTIVE":  [True,True,True,False,True],
        "CREATED_AT": ["2024-01-01","2024-02-15","2024-03-10","2024-04-01","2024-05-20"],
    })

@pytest.fixture
def mock_orders_df():
    return pd.DataFrame({
        "ORDER_ID":   ["o1","o2","o3","o4"],
        "USER_ID":    ["u1","u2","u1","u3"],
        "QUANTITY":   [2,1,5,3],
        "UNIT_PRICE": [29.99,49.00,9.99,199.00],
        "STATUS":     ["DELIVERED","SHIPPED","DELIVERED","PENDING"],
        "ORDER_DATE": ["2024-06-01","2024-06-10","2024-06-15","2024-06-20"],
        "CURRENCY":   ["GBP","USD","GBP","EUR"],
    })

@pytest.fixture
def mock_events_df():
    return pd.DataFrame({
        "EVENT_ID":   ["e1","e2","e3","e4","e5"],
        "USER_ID":    ["u1","u2","u1","u3","u4"],
        "EVENT_TYPE": ["PAGE_VIEW","CLICK","PURCHASE","PAGE_VIEW","SIGNUP"],
        "PAGE":       ["/home","/product/1","/checkout","/home","/register"],
        "SESSION_ID": ["s1","s2","s1","s3","s4"],
        "TIMESTAMP":  ["2024-06-20 10:00:00","2024-06-20 10:05:00","2024-06-20 10:10:00","2024-06-20 11:00:00","2024-06-20 11:30:00"],
    })

@pytest.fixture
def mock_snowflake_connector(mock_users_df, mock_orders_df):
    class MockConnector:
        def execute(self, sql, params=None): return []
        def execute_df(self, sql, params=None):
            if "USER" in sql.upper(): return mock_users_df
            if "ORDER" in sql.upper(): return mock_orders_df
            return pd.DataFrame()
        def execute_scalar(self, sql, params=None):
            if "USER" in sql.upper(): return len(mock_users_df)
            if "ORDER" in sql.upper(): return len(mock_orders_df)
            return 0
        def get_row_count(self, table, where=""): return 5
        def table_exists(self, table, schema=None): return True
        def get_column_names(self, table): return ["USER_ID","USERNAME","EMAIL","COUNTRY","IS_ACTIVE","CREATED_AT"]
        def get_table_schema(self, table, schema=None):
            return [
                {"COLUMN_NAME":"USER_ID",    "DATA_TYPE":"VARCHAR",   "IS_NULLABLE":"NO"},
                {"COLUMN_NAME":"USERNAME",   "DATA_TYPE":"VARCHAR",   "IS_NULLABLE":"NO"},
                {"COLUMN_NAME":"EMAIL",      "DATA_TYPE":"VARCHAR",   "IS_NULLABLE":"NO"},
                {"COLUMN_NAME":"COUNTRY",    "DATA_TYPE":"VARCHAR",   "IS_NULLABLE":"YES"},
                {"COLUMN_NAME":"IS_ACTIVE",  "DATA_TYPE":"BOOLEAN",   "IS_NULLABLE":"YES"},
                {"COLUMN_NAME":"CREATED_AT", "DATA_TYPE":"TIMESTAMP", "IS_NULLABLE":"YES"},
            ]
    return MockConnector()

@pytest.fixture
def dq_validator(mock_users_df):
    from src.validators.data_quality import DataQualityValidator
    return DataQualityValidator(mock_users_df, label="mock_users")

@pytest.fixture(scope="session", autouse=True)
def configure_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s", datefmt="%H:%M:%S")
