"""
tests/pipeline/test_etl_transformations.py — ETL business logic transformation tests
Tests transformations that happen in your pipeline without needing Snowflake.
"""
import pytest
import pandas as pd
from datetime import datetime, date

class TestUserTransformations:
    """Tests for the users ETL layer transformations."""
    @pytest.fixture
    def raw_users(self):
        return pd.DataFrame({
            "user_id":    ["U-001","U-002","U-003","U-004"],
            "email":      ["ALICE@TEST.COM","bob@test.com","carol@test.com","  dave@test.com  "],
            "first_name": ["alice","BOB","Carol","dave"],
            "last_name":  ["SMITH","jones","WILLIAMS","Brown"],
            "status":     ["active","ACTIVE","inactive","Active"],
            "created_at": ["2024-01-15","2024-02-20","2024-03-10","2024-04-05"],
            "revenue":    ["1000.50","invalid","2500.00","750"],
        })

    def _transform_users(self, df):
        out = df.copy()
        out["email"]      = out["email"].str.strip().str.lower()
        out["first_name"] = out["first_name"].str.strip().str.title()
        out["last_name"]  = out["last_name"].str.strip().str.title()
        out["status"]     = out["status"].str.strip().str.upper()
        out["created_at"] = pd.to_datetime(out["created_at"], errors="coerce")
        out["revenue"]    = pd.to_numeric(out["revenue"], errors="coerce")
        return out

    def test_email_lowercased(self, raw_users):
        out = self._transform_users(raw_users)
        assert all(e == e.lower() for e in out["email"].dropna())

    def test_email_whitespace_stripped(self, raw_users):
        out = self._transform_users(raw_users)
        assert all(e == e.strip() for e in out["email"].dropna())

    def test_name_title_cased(self, raw_users):
        out = self._transform_users(raw_users)
        assert out["first_name"].iloc[0] == "Alice"
        assert out["last_name"].iloc[1]  == "Jones"

    def test_status_upper_cased(self, raw_users):
        out = self._transform_users(raw_users)
        assert all(s == s.upper() for s in out["status"].dropna())

    def test_created_at_is_datetime(self, raw_users):
        out = self._transform_users(raw_users)
        assert pd.api.types.is_datetime64_any_dtype(out["created_at"])

    def test_invalid_revenue_becomes_null(self, raw_users):
        out = self._transform_users(raw_users)
        assert pd.isna(out["revenue"].iloc[1])

    def test_valid_revenue_converted(self, raw_users):
        out = self._transform_users(raw_users)
        assert out["revenue"].iloc[0] == 1000.50
        assert out["revenue"].iloc[2] == 2500.00


class TestOrderTransformations:
    """Tests for order ETL layer transformations."""
    @pytest.fixture
    def raw_orders(self):
        return pd.DataFrame({
            "order_id":   ["ORD-001","ORD-002","ORD-003","ORD-004"],
            "user_id":    ["U-001","U-002","U-003","U-001"],
            "quantity":   [2,5,1,3],
            "unit_price": [10.00,20.00,50.00,15.00],
            "discount":   [0.10,0.0,0.25,0.0],
            "currency":   ["gbp","USD","GBP","usd"],
            "status":     ["shipped","PENDING","delivered","cancelled"],
        })

    def _transform_orders(self, df):
        out = df.copy()
        out["currency"]    = out["currency"].str.upper()
        out["status"]      = out["status"].str.upper()
        out["gross_total"] = out["quantity"] * out["unit_price"]
        out["net_total"]   = out["gross_total"] * (1 - out["discount"])
        return out

    def test_currency_uppercased(self, raw_orders):
        out = self._transform_orders(raw_orders)
        assert all(c == c.upper() for c in out["currency"])

    def test_gross_total_calculated(self, raw_orders):
        out = self._transform_orders(raw_orders)
        assert out["gross_total"].iloc[0] == 20.00  # 2 * 10
        assert out["gross_total"].iloc[1] == 100.00 # 5 * 20

    def test_net_total_applies_discount(self, raw_orders):
        out = self._transform_orders(raw_orders)
        assert abs(out["net_total"].iloc[0] - 18.00) < 0.01  # 20 * 0.9
        assert abs(out["net_total"].iloc[2] - 37.50) < 0.01  # 50 * 0.75

    def test_no_negative_totals(self, raw_orders):
        out = self._transform_orders(raw_orders)
        assert (out["net_total"] >= 0).all()


class TestAggregationLogic:
    """Tests for aggregation transformations (user order summaries, etc.)."""
    @pytest.fixture
    def orders_with_totals(self):
        return pd.DataFrame({
            "user_id":  ["u1","u1","u2","u2","u2","u3"],
            "order_id": ["o1","o2","o3","o4","o5","o6"],
            "net_total":  [100.0,200.0,50.0,75.0,25.0,500.0],
            "status":   ["DELIVERED","DELIVERED","DELIVERED","CANCELLED","DELIVERED","SHIPPED"],
        })

    def _aggregate_by_user(self, df):
        delivered = df[df["status"] == "DELIVERED"]
        return delivered.groupby("user_id").agg(
            order_count  = ("order_id","count"),
            total_revenue = ("net_total","sum"),
            avg_order_value = ("net_total","mean"),
        ).reset_index()

    def test_user_order_count(self, orders_with_totals):
        agg = self._aggregate_by_user(orders_with_totals)
        u1 = agg[agg["user_id"]=="u1"].iloc[0]
        assert u1["order_count"] == 2

    def test_user_total_revenue(self, orders_with_totals):
        agg = self._aggregate_by_user(orders_with_totals)
        u1 = agg[agg["user_id"]=="u1"].iloc[0]
        assert u1["total_revenue"] == 300.0

    def test_cancelled_orders_excluded(self, orders_with_totals):
        agg = self._aggregate_by_user(orders_with_totals)
        u2 = agg[agg["user_id"]=="u2"].iloc[0]
        assert u2["order_count"] == 2  # o3 and o5 only (o4 cancelled)
        assert u2["total_revenue"] == 75.0

    def test_avg_order_value(self, orders_with_totals):
        agg = self._aggregate_by_user(orders_with_totals)
        u1 = agg[agg["user_id"]=="u1"].iloc[0]
        assert u1["avg_order_value"] == 150.0


class TestDataDeduplication:
    def test_dedup_keeps_latest_record(self):
        df = pd.DataFrame({
            "id":         [1,1,1,2,2],
            "status":     ["PENDING","PROCESSING","DELIVERED","PENDING","SHIPPED"],
            "updated_at": pd.to_datetime(["2024-01-01","2024-01-02","2024-01-03","2024-01-01","2024-01-02"]),
        })
        deduped = df.sort_values("updated_at").groupby("id").last().reset_index()
        assert len(deduped) == 2
        assert deduped[deduped["id"]==1]["status"].values[0] == "DELIVERED"
        assert deduped[deduped["id"]==2]["status"].values[0] == "SHIPPED"

    def test_dedup_row_count(self):
        df = pd.DataFrame({"key":["a","b","a","c","b","a"],"val":[1,2,3,4,5,6]})
        deduped = df.drop_duplicates(subset=["key"], keep="last")
        assert len(deduped) == 3

    def test_exact_duplicate_removal(self):
        df = pd.DataFrame({"A":[1,1,2,3],"B":["x","x","y","z"]})
        deduped = df.drop_duplicates()
        assert len(deduped) == 3


class TestIncrementalLoadLogic:
    """Tests for incremental load watermark logic."""
    def test_new_records_only(self):
        existing = pd.DataFrame({"id":[1,2,3],"updated_at":pd.to_datetime(["2024-01-01","2024-01-02","2024-01-03"])})
        incoming = pd.DataFrame({"id":[2,3,4,5],"updated_at":pd.to_datetime(["2024-01-04","2024-01-05","2024-01-06","2024-01-07"])})
        watermark = existing["updated_at"].max()
        new_records = incoming[incoming["updated_at"] > watermark]
        assert len(new_records) == 4  # all incoming are after watermark

    def test_watermark_excludes_older(self):
        watermark = pd.Timestamp("2024-06-01")
        incoming = pd.DataFrame({"id":[1,2,3],"updated_at":pd.to_datetime(["2024-05-30","2024-06-01","2024-06-02"])})
        new_records = incoming[incoming["updated_at"] > watermark]
        assert len(new_records) == 1 and new_records["id"].iloc[0] == 3
