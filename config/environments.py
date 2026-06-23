"""config/environments.py - Environment-aware configuration loader"""
from __future__ import annotations
import os
from dataclasses import dataclass
from dotenv import load_dotenv
from pathlib import Path

_env_file = Path(__file__).parent.parent / f".env.{os.getenv('ENV', 'development')}"
if _env_file.exists():
    load_dotenv(_env_file)

@dataclass(frozen=True)
class SnowflakeConfig:
    account: str; user: str; password: str
    database: str; schema: str; warehouse: str; role: str = ""

@dataclass(frozen=True)
class AirflowConfig:
    base_url: str = "http://localhost:8080"
    username: str = "airflow"; password: str = "airflow"

@dataclass(frozen=True)
class PipelineConfig:
    env: str; snowflake: SnowflakeConfig; airflow: AirflowConfig
    test_schema: str = "TEST_SCHEMA"

def get_config() -> PipelineConfig:
    sf = SnowflakeConfig(
        account=os.getenv("SNOWFLAKE_ACCOUNT",""), user=os.getenv("SNOWFLAKE_USER",""),
        password=os.getenv("SNOWFLAKE_PASSWORD",""), database=os.getenv("SNOWFLAKE_DATABASE",""),
        schema=os.getenv("SNOWFLAKE_SCHEMA",""), warehouse=os.getenv("SNOWFLAKE_WAREHOUSE",""),
        role=os.getenv("SNOWFLAKE_ROLE",""),
    )
    af = AirflowConfig(
        base_url=os.getenv("AIRFLOW_BASE_URL","http://localhost:8080"),
        username=os.getenv("AIRFLOW_USERNAME","airflow"),
        password=os.getenv("AIRFLOW_PASSWORD","airflow"),
    )
    return PipelineConfig(env=os.getenv("ENV","development"), snowflake=sf, airflow=af,
                          test_schema=os.getenv("SNOWFLAKE_TEST_SCHEMA","TEST_SCHEMA"))

CONFIG = get_config()
