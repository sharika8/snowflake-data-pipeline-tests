"""
tests/fixtures/test_data_generation.py — Test data factory validation
Ensures generated data meets the format and quality requirements.
"""
import pytest
import pandas as pd
from src.utils.pipeline_utils import generate_user_records, generate_order_records, generate_event_records
from src.validators.data_quality import DataQualityValidator

class TestUserDataGeneration:
    def test_generates_correct_count(self):
        users = generate_user_records(50, seed=42)
        assert len(users) == 50

    def test_all_required_fields_present(self):
        users = generate_user_records(10, seed=42)
        required = ["USER_ID","USERNAME","EMAIL","FIRST_NAME","LAST_NAME","COUNTRY","CREATED_AT","IS_ACTIVE"]
        for user in users:
            for field in required:
                assert field in user, f"Missing field: {field}"

    def test_emails_are_valid_format(self):
        users = generate_user_records(20, seed=42)
        df = pd.DataFrame(users)
        DataQualityValidator(df).is_email("EMAIL").assert_all()

    def test_user_ids_are_unique(self):
        users = generate_user_records(100, seed=42)
        ids = [u["USER_ID"] for u in users]
        assert len(ids) == len(set(ids))

    def test_seeded_generation_is_reproducible(self):
        batch1 = generate_user_records(5, seed=99)
        batch2 = generate_user_records(5, seed=99)
        assert [u["USER_ID"] for u in batch1] == [u["USER_ID"] for u in batch2]

    def test_is_active_is_boolean(self):
        users = generate_user_records(20, seed=42)
        assert all(isinstance(u["IS_ACTIVE"], bool) for u in users)

    def test_country_is_2_char_code(self):
        users = generate_user_records(30, seed=42)
        assert all(len(u["COUNTRY"]) == 2 for u in users)

    def test_dq_full_validation(self):
        df = pd.DataFrame(generate_user_records(50, seed=42))
        DataQualityValidator(df, label="generated_users") \
            .row_count(exact=50) \
            .not_null("USER_ID","EMAIL","FIRST_NAME") \
            .unique("USER_ID") \
            .is_email("EMAIL") \
            .assert_all()


class TestOrderDataGeneration:
    def test_generates_correct_count(self):
        users = generate_user_records(10, seed=1)
        user_ids = [u["USER_ID"] for u in users]
        orders = generate_order_records(25, user_ids, seed=1)
        assert len(orders) == 25

    def test_all_user_ids_are_valid(self):
        users = generate_user_records(5, seed=1)
        user_ids = [u["USER_ID"] for u in users]
        orders = generate_order_records(20, user_ids, seed=1)
        assert all(o["USER_ID"] in user_ids for o in orders)

    def test_quantities_are_positive(self):
        users = generate_user_records(5, seed=1)
        user_ids = [u["USER_ID"] for u in users]
        orders = generate_order_records(20, user_ids, seed=1)
        assert all(o["QUANTITY"] >= 1 for o in orders)

    def test_status_is_valid(self):
        users = generate_user_records(5, seed=1)
        user_ids = [u["USER_ID"] for u in users]
        orders = generate_order_records(30, user_ids, seed=1)
        df = pd.DataFrame(orders)
        DataQualityValidator(df).allowed_values("STATUS",["PENDING","CONFIRMED","SHIPPED","DELIVERED","CANCELLED"]).assert_all()

    def test_referential_integrity_with_users(self):
        users_df = pd.DataFrame(generate_user_records(10, seed=2))
        user_ids = users_df["USER_ID"].tolist()
        orders_df = pd.DataFrame(generate_order_records(30, user_ids, seed=2))
        DataQualityValidator(orders_df).referential_integrity("USER_ID", users_df, "USER_ID").assert_all()


class TestEventDataGeneration:
    def test_generates_correct_count(self):
        user_ids = [f"u{i}" for i in range(10)]
        events = generate_event_records(100, user_ids, seed=42)
        assert len(events) == 100

    def test_event_types_are_valid(self):
        user_ids = [f"u{i}" for i in range(5)]
        events = generate_event_records(50, user_ids, seed=42)
        df = pd.DataFrame(events)
        DataQualityValidator(df).allowed_values("EVENT_TYPE",["PAGE_VIEW","CLICK","PURCHASE","SIGNUP","LOGOUT","ERROR"]).assert_all()

    def test_event_ids_are_unique(self):
        user_ids = [f"u{i}" for i in range(5)]
        events = generate_event_records(50, user_ids, seed=42)
        ids = [e["EVENT_ID"] for e in events]
        assert len(ids) == len(set(ids))
