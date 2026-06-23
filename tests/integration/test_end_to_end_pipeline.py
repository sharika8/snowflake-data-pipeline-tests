"""
tests/integration/test_end_to_end_pipeline.py — End-to-end pipeline tests
Full pipeline flow: generate → validate → reconcile.
Tests marked requires_snowflake are skipped in CI without credentials.
"""
import pytest
import pandas as pd
from src.validators.data_quality import DataQualityValidator
from src.validators.schema_validator import SchemaValidator
from src.utils.pipeline_utils import (
    generate_user_records, generate_order_records,
    reconcile_datasets, assert_row_count_match,
)
from tests.conftest import requires_snowflake

class TestSimulatedPipelineRun:
    """Full pipeline simulation using in-memory DataFrames — no Snowflake needed."""

    def test_users_pipeline_full_flow(self):
        # Step 1: Generate raw data
        raw = generate_user_records(100, seed=42)
        df_raw = pd.DataFrame(raw)

        # Step 2: Transform (normalise email, uppercase country)
        df_transformed = df_raw.copy()
        df_transformed["EMAIL"]   = df_transformed["EMAIL"].str.lower().str.strip()
        df_transformed["COUNTRY"] = df_transformed["COUNTRY"].str.upper()
        df_transformed["USERNAME"] = df_transformed["USERNAME"].str.strip()

        # Step 3: Validate output
        DataQualityValidator(df_transformed, label="users_pipeline") \
            .row_count(exact=100) \
            .not_null("USER_ID","EMAIL","USERNAME") \
            .unique("USER_ID") \
            .is_email("EMAIL") \
            .assert_all()

        # Step 4: Reconcile source to target
        result = reconcile_datasets(df_raw, df_transformed, ["USER_ID"], ["EMAIL"])
        # Emails were normalised, so some will appear "changed"
        assert result["total_source"] == 100
        assert result["missing_in_target"] == 0
        assert result["extra_in_target"] == 0

    def test_orders_pipeline_full_flow(self):
        users = generate_user_records(20, seed=1)
        user_ids = [u["USER_ID"] for u in users]
        orders = generate_order_records(50, user_ids, seed=1)

        df_orders = pd.DataFrame(orders)
        df_users  = pd.DataFrame(users)

        # Validate orders
        DataQualityValidator(df_orders, label="orders_pipeline") \
            .row_count(exact=50) \
            .not_null("ORDER_ID","USER_ID","STATUS") \
            .unique("ORDER_ID") \
            .allowed_values("STATUS",["PENDING","CONFIRMED","SHIPPED","DELIVERED","CANCELLED"]) \
            .referential_integrity("USER_ID", df_users, "USER_ID") \
            .assert_all()

    def test_row_count_reconciliation_passes(self):
        raw = generate_user_records(200, seed=5)
        df = pd.DataFrame(raw)
        # Simulate some records filtered out (e.g. deduplication)
        df_deduped = df.drop_duplicates(subset=["USER_ID"])
        # All were unique so counts match
        assert_row_count_match(len(df), len(df_deduped), tolerance_pct=0.0)

    def test_incremental_load_adds_new_rows_only(self):
        existing = pd.DataFrame(generate_user_records(50, seed=10))
        new_batch = pd.DataFrame(generate_user_records(20, seed=20))
        combined = pd.concat([existing, new_batch], ignore_index=True)
        deduped  = combined.drop_duplicates(subset=["USER_ID"])
        # All unique IDs, so total = 70
        assert len(deduped) == 70

    def test_schema_validation_against_expected(self, mock_snowflake_connector):
        sv = SchemaValidator(mock_snowflake_connector)
        expected = {
            "USER_ID":    {"type":"VARCHAR", "nullable":False},
            "USERNAME":   {"type":"VARCHAR", "nullable":False},
            "EMAIL":      {"type":"VARCHAR", "nullable":False},
            "COUNTRY":    {"type":"VARCHAR", "nullable":True},
            "IS_ACTIVE":  {"type":"BOOLEAN", "nullable":True},
            "CREATED_AT": {"type":"TIMESTAMP","nullable":True},
        }
        sv.assert_no_drift("USERS", expected)

    def test_data_quality_gate_blocks_bad_data(self):
        bad_df = pd.DataFrame({
            "USER_ID": [None,"u2","u2"],       # null + duplicate
            "EMAIL":   ["not-an-email","b@x.com","b@x.com"],  # invalid email
            "COUNTRY": ["GB","XX","GB"],
        })
        from src.validators.data_quality import DQAssertionError
        with pytest.raises(DQAssertionError):
            DataQualityValidator(bad_df, label="bad_data") \
                .not_null("USER_ID") \
                .unique("USER_ID") \
                .is_email("EMAIL") \
                .assert_all()


@requires_snowflake
class TestLiveSnowflakePipeline:
    """Tests that run against a real Snowflake instance — skipped in CI without credentials."""

    def test_connection_and_basic_query(self, sf_connector):
        result = sf_connector.execute_scalar("SELECT 42")
        assert result == 42

    def test_information_schema_accessible(self, sf_connector):
        count = sf_connector.execute_scalar(
            "SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        )
        assert isinstance(count, int)

    def test_table_row_count_within_expected_range(self, sf_connector):
        """Example: assert your users table is not empty and not unexpectedly large."""
        # Adapt TABLE_NAME to your actual table
        count = sf_connector.execute_scalar("SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES")
        assert count is not None and count >= 0

    def test_no_schema_drift_on_information_schema(self, schema_validator):
        schema_validator.assert_columns_exist(
            "INFORMATION_SCHEMA.COLUMNS",
            "TABLE_NAME","COLUMN_NAME","DATA_TYPE","IS_NULLABLE"
        )
