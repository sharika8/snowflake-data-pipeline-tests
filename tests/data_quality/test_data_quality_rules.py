"""
tests/data_quality/test_data_quality_rules.py — DQ validator unit tests
All tests use mock DataFrames — no Snowflake connection required.
"""
import pytest
import pandas as pd
from src.validators.data_quality import DataQualityValidator, DQAssertionError

class TestNotNull:
    def test_passes_when_no_nulls(self, mock_users_df):
        DataQualityValidator(mock_users_df).not_null("USER_ID","EMAIL").assert_all()

    def test_fails_when_null_found(self):
        df = pd.DataFrame({"ID":[1,None,3],"NAME":["a","b",None]})
        dq = DataQualityValidator(df).not_null("ID","NAME")
        with pytest.raises(DQAssertionError): dq.assert_all()

    def test_missing_column_fails(self, mock_users_df):
        with pytest.raises(DQAssertionError):
            DataQualityValidator(mock_users_df).not_null("NONEXISTENT").assert_all()

    def test_null_rate_within_threshold(self):
        df = pd.DataFrame({"COL":[1,2,None,4,5,6,7,8,9,10]})
        DataQualityValidator(df).null_rate("COL", max_rate=0.15).assert_all()

    def test_null_rate_exceeds_threshold(self):
        df = pd.DataFrame({"COL":[1,None,None,4,5]})
        with pytest.raises(DQAssertionError):
            DataQualityValidator(df).null_rate("COL", max_rate=0.10).assert_all()

class TestUniqueness:
    def test_unique_column_passes(self, mock_users_df):
        DataQualityValidator(mock_users_df).unique("USER_ID").assert_all()

    def test_duplicate_column_fails(self):
        df = pd.DataFrame({"ID":[1,2,2,3],"NAME":["a","b","b_dup","c"]})
        with pytest.raises(DQAssertionError):
            DataQualityValidator(df).unique("ID").assert_all()

    def test_composite_key_uniqueness_passes(self, mock_orders_df):
        DataQualityValidator(mock_orders_df).unique("ORDER_ID").assert_all()

    def test_composite_key_uniqueness_fails(self):
        df = pd.DataFrame({"USER_ID":["u1","u1","u2"],"ORDER_ID":["o1","o1","o2"]})
        with pytest.raises(DQAssertionError):
            DataQualityValidator(df).unique("USER_ID","ORDER_ID").assert_all()

class TestRowCount:
    def test_exact_count_passes(self, mock_users_df):
        DataQualityValidator(mock_users_df).row_count(exact=5).assert_all()

    def test_exact_count_fails(self, mock_users_df):
        with pytest.raises(DQAssertionError):
            DataQualityValidator(mock_users_df).row_count(exact=10).assert_all()

    def test_min_count_passes(self, mock_users_df):
        DataQualityValidator(mock_users_df).row_count(min=1).assert_all()

    def test_range_count_passes(self, mock_users_df):
        DataQualityValidator(mock_users_df).row_count(min=1, max=100).assert_all()

    def test_empty_fails(self):
        with pytest.raises(DQAssertionError):
            DataQualityValidator(pd.DataFrame({"A":[]})).row_count().assert_all()

class TestValueRange:
    def test_value_range_passes(self, mock_orders_df):
        DataQualityValidator(mock_orders_df).value_range("QUANTITY", min_val=1, max_val=100).assert_all()

    def test_value_range_fails_below_min(self):
        df = pd.DataFrame({"PRICE":[10.0,-5.0,20.0]})
        with pytest.raises(DQAssertionError):
            DataQualityValidator(df).value_range("PRICE", min_val=0).assert_all()

    def test_allowed_values_passes(self, mock_orders_df):
        DataQualityValidator(mock_orders_df).allowed_values("STATUS",["PENDING","CONFIRMED","SHIPPED","DELIVERED","CANCELLED"]).assert_all()

    def test_allowed_values_fails(self, mock_orders_df):
        with pytest.raises(DQAssertionError):
            DataQualityValidator(mock_orders_df).allowed_values("STATUS",["ACTIVE","INACTIVE"]).assert_all()

class TestPatternMatching:
    def test_email_format_passes(self, mock_users_df):
        DataQualityValidator(mock_users_df).is_email("EMAIL").assert_all()

    def test_email_format_fails(self):
        df = pd.DataFrame({"EMAIL":["valid@test.com","not-an-email","also@valid.org"]})
        with pytest.raises(DQAssertionError):
            DataQualityValidator(df).is_email("EMAIL").assert_all()

    def test_date_format_passes(self, mock_users_df):
        DataQualityValidator(mock_users_df).is_date("CREATED_AT").assert_all()

    def test_custom_pattern_passes(self):
        df = pd.DataFrame({"CODE":["GB","US","DE","FR"]})
        DataQualityValidator(df).matches_pattern("CODE", r"^[A-Z]{2}$").assert_all()

class TestReferentialIntegrity:
    def test_integrity_passes(self, mock_users_df, mock_orders_df):
        DataQualityValidator(mock_orders_df).referential_integrity("USER_ID", mock_users_df, "USER_ID").assert_all()

    def test_integrity_fails_with_orphans(self):
        users = pd.DataFrame({"USER_ID":["u1","u2","u3"]})
        orders = pd.DataFrame({"ORDER_ID":["o1","o2"],"USER_ID":["u1","u999"]})
        with pytest.raises(DQAssertionError):
            DataQualityValidator(orders).referential_integrity("USER_ID", users, "USER_ID").assert_all()

class TestChaining:
    def test_all_checks_pass(self, mock_users_df):
        DataQualityValidator(mock_users_df, label="full_check") \
            .has_columns("USER_ID","EMAIL","USERNAME","IS_ACTIVE") \
            .row_count(min=1, max=1000) \
            .not_null("USER_ID","EMAIL") \
            .unique("USER_ID") \
            .is_email("EMAIL") \
            .allowed_values("COUNTRY",["GB","US","DE","FR","AU","CA"]) \
            .assert_all()

    def test_summary_output(self, mock_users_df):
        dq = DataQualityValidator(mock_users_df, label="test_summary")
        dq.not_null("USER_ID").unique("USER_ID")
        assert "test_summary" in dq.summary()
        assert "PASS" in dq.summary()

    def test_get_results_returns_all(self, mock_users_df):
        dq = DataQualityValidator(mock_users_df)
        dq.not_null("USER_ID").unique("EMAIL").row_count(min=1)
        assert len(dq.get_results()) == 3
