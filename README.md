# ❄️ Snowflake & Data Pipeline Testing Framework

Enterprise test framework for Snowflake data pipelines — data quality validation, schema drift detection, ETL transformation testing, row-level reconciliation, and Airflow DAG integration. Unit and integration tests run in CI without Snowflake credentials; live tests activate via GitHub Secrets.

[![CI](https://github.com/sharika8/snowflake-data-pipeline-tests/actions/workflows/tests.yml/badge.svg)](https://github.com/sharika8/snowflake-data-pipeline-tests/actions/workflows/tests.yml)
[![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)](https://python.org)
[![Snowflake](https://img.shields.io/badge/Snowflake-Connector-29B5E8?logo=snowflake)](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## ✨ Features

| Feature | Detail |
|---|---|
| **Data Quality Validator** | Fluent API: `not_null`, `unique`, `value_range`, `is_email`, `referential_integrity`, `is_fresh` |
| **Schema Drift Detection** | Compares live Snowflake DDL to expected schema — type normalisation, nullable checks |
| **ETL Transform Tests** | In-memory DataFrame tests — email normalisation, title-case, numeric coercion |
| **Row Reconciliation** | `reconcile_datasets()` — matching, missing, extra, changed row counts |
| **Airflow Integration** | `AirflowClient` — trigger DAGs, poll status, assert success |
| **Data Generation** | `generate_user/order/event_records()` — seeded, reproducible test data |
| **60+ tests** | DQ rules, schema, pipeline, ETL transforms, fixtures, integration — all passing |
| **CI without creds** | Unit + integration tests run on every push; Snowflake tests opt-in via Secrets |

---

## 📁 Project Structure

```
snowflake-data-pipeline-tests/
├── src/
│   ├── connectors/
│   │   └── snowflake_connector.py    # Connection manager: execute, execute_df, retry
│   ├── validators/
│   │   ├── data_quality.py           # DataQualityValidator — fluent DQ rule engine
│   │   └── schema_validator.py       # SchemaValidator — drift detection
│   └── utils/
│       └── pipeline_utils.py         # Reconciliation, AirflowClient, data generators
├── tests/
│   ├── data_quality/                 # 30 DQ validator unit tests
│   ├── snowflake/                    # Connectivity + schema tests (skipped without creds)
│   ├── pipeline/                     # Reconciliation + ETL transform tests
│   ├── fixtures/                     # Data generation factory validation
│   └── integration/                  # End-to-end pipeline simulation
├── config/environments.py            # ENV-aware config loader
├── pytest.ini
└── .github/workflows/tests.yml       # CI: unit + integration always; Snowflake optional
```

---

## 🚀 Quick Start

```bash
# Install (no Snowflake credentials needed for most tests)
pip install pandas pytest pytest-html faker tenacity pydantic python-dotenv requests

# Run all tests (Snowflake tests auto-skipped without credentials)
pytest tests/ -v

# Run only unit tests (always fast)
pytest tests/data_quality/ tests/pipeline/ tests/fixtures/ -v

# Run with Snowflake (set env vars first)
export SNOWFLAKE_ACCOUNT=your-account
export SNOWFLAKE_USER=your-user
export SNOWFLAKE_PASSWORD=your-password
export SNOWFLAKE_DATABASE=your-db
export SNOWFLAKE_SCHEMA=your-schema
export SNOWFLAKE_WAREHOUSE=your-warehouse
pytest tests/ -v
```

---

## 💡 Usage Examples

```python
from src.validators.data_quality import DataQualityValidator

# Fluent DQ validation chain
DataQualityValidator(df, label="users") \
    .has_columns("USER_ID", "EMAIL", "CREATED_AT") \
    .row_count(min=1, max=1_000_000) \
    .not_null("USER_ID", "EMAIL") \
    .unique("USER_ID") \
    .is_email("EMAIL") \
    .allowed_values("STATUS", ["ACTIVE", "INACTIVE"]) \
    .is_fresh("CREATED_AT", max_age_hours=24) \
    .assert_all()

# Schema drift detection
from src.validators.schema_validator import SchemaValidator
validator = SchemaValidator(connector)
validator.assert_no_drift("USERS", {
    "USER_ID": {"type": "VARCHAR", "nullable": False},
    "EMAIL":   {"type": "VARCHAR", "nullable": False},
})

# Row reconciliation
from src.utils.pipeline_utils import reconcile_datasets
result = reconcile_datasets(source_df, target_df, key_columns=["USER_ID"])
# Returns: {matching, missing_in_target, extra_in_target, changed, match_rate}
```

---

## 🤖 CI Pipeline

| Job | Trigger | Snowflake? | What it does |
|---|---|---|---|
| `unit-tests` | every push | ❌ No | DQ rules, reconciliation, ETL, fixtures |
| `integration-tests` | after unit | ❌ No | End-to-end pipeline simulation |
| `snowflake-tests` | Secrets set / manual | ✅ Yes | Live connectivity, schema, row counts |

---

## 🔗 Related Repos

| Repo | Description |
|---|---|
| [test-data-management](https://github.com/sharika8/test-data-management) | Test data factories + seeders |
| [enterprise-qa-framework](https://github.com/sharika8/enterprise-qa-framework) | Python + Playwright UI + API |
| [k6-performance-framework](https://github.com/sharika8/k6-performance-framework) | Performance testing |

---

## 📜 Licence
MIT