"""
tests/snowflake/test_schema_validation.py — Schema drift detection tests
"""
import pytest
import pandas as pd
from src.validators.schema_validator import SchemaValidator, SchemaDiffResult, ColumnDiff

class TestSchemaValidatorUnit:
    """No Snowflake needed — uses mock connector."""
    @pytest.fixture
    def validator(self, mock_snowflake_connector):
        return SchemaValidator(mock_snowflake_connector)

    EXPECTED = {
        "USER_ID":    {"type":"VARCHAR",   "nullable":False},
        "USERNAME":   {"type":"VARCHAR",   "nullable":False},
        "EMAIL":      {"type":"VARCHAR",   "nullable":False},
        "COUNTRY":    {"type":"VARCHAR",   "nullable":True},
        "IS_ACTIVE":  {"type":"BOOLEAN",   "nullable":True},
        "CREATED_AT": {"type":"TIMESTAMP", "nullable":True},
    }

    def test_no_drift_when_matches(self, validator):
        result = validator.compare("USERS", self.EXPECTED)
        assert not result.drifted and result.diffs == []

    def test_drift_detected_for_missing_column(self, validator):
        schema = {**self.EXPECTED, "PHONE_NUMBER": {"type":"VARCHAR"}}
        result = validator.compare("USERS", schema)
        assert result.drifted
        assert any(d.column == "PHONE_NUMBER" and d.issue == "missing" for d in result.diffs)

    def test_drift_detected_for_extra_column(self, validator):
        schema = {k:v for k,v in self.EXPECTED.items() if k != "COUNTRY"}
        result = validator.compare("USERS", schema)
        assert result.drifted
        assert any(d.column == "COUNTRY" and d.issue == "added" for d in result.diffs)

    def test_str_no_drift(self, validator):
        assert "[PASS]" in str(validator.compare("USERS", self.EXPECTED))

    def test_str_with_drift(self, validator):
        assert "[FAIL]" in str(validator.compare("USERS", {"NONEXISTENT":{"type":"VARCHAR"}}))

    def test_assert_no_drift_passes(self, validator):
        validator.assert_no_drift("USERS", self.EXPECTED)

    def test_assert_no_drift_raises(self, validator):
        with pytest.raises(AssertionError):
            validator.assert_no_drift("USERS", {"NONEXISTENT":{"type":"NUMBER"}})

    def test_assert_columns_exist_passes(self, validator):
        validator.assert_columns_exist("USERS", "USER_ID", "EMAIL")

    def test_assert_columns_exist_raises(self, validator):
        with pytest.raises(AssertionError) as exc:
            validator.assert_columns_exist("USERS", "NONEXISTENT_COL")
        assert "missing" in str(exc.value).lower()

class TestTypeNormalisation:
    def setup_method(self):
        self.sv = SchemaValidator(None)

    def test_text_to_varchar(self):
        assert self.sv._normalise_type("TEXT") == "VARCHAR"
        assert self.sv._normalise_type("STRING") == "VARCHAR"

    def test_int_to_number(self):
        assert self.sv._normalise_type("INT") == "NUMBER"
        assert self.sv._normalise_type("BIGINT") == "NUMBER"

    def test_timestamp_normalised(self):
        assert self.sv._normalise_type("TIMESTAMP") == "TIMESTAMP_NTZ"

    def test_length_stripped(self):
        assert self.sv._normalise_type("VARCHAR(255)") == "VARCHAR"
        assert self.sv._normalise_type("NUMBER(38,0)") == "NUMBER"

from tests.conftest import requires_snowflake

@requires_snowflake
class TestSchemaValidatorLive:
    def test_information_schema_has_columns(self, schema_validator):
        schema_validator.assert_columns_exist("INFORMATION_SCHEMA.TABLES","TABLE_NAME","TABLE_SCHEMA","TABLE_TYPE")
