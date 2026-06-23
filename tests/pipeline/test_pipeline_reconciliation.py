"""
tests/pipeline/test_pipeline_reconciliation.py — Row count and data reconciliation tests
"""
import pytest
import pandas as pd
from src.utils.pipeline_utils import assert_row_count_match, reconcile_datasets, compute_row_hash

class TestRowCountReconciliation:
    def test_exact_match_passes(self): assert_row_count_match(100, 100)
    def test_within_tolerance_passes(self): assert_row_count_match(1000, 990, tolerance_pct=0.02)
    def test_exact_required_fails(self):
        with pytest.raises(AssertionError): assert_row_count_match(100, 95, tolerance_pct=0.0)
    def test_exceeds_tolerance_fails(self):
        with pytest.raises(AssertionError): assert_row_count_match(1000, 800, tolerance_pct=0.10)
    def test_both_zero_passes(self): assert_row_count_match(0, 0)
    def test_empty_source_fails(self):
        with pytest.raises(AssertionError): assert_row_count_match(0, 100)

class TestRowHashing:
    def test_hash_reproducible(self):
        df = pd.DataFrame({"A":[1,2,3],"B":["x","y","z"]})
        assert compute_row_hash(df,["A","B"]).equals(compute_row_hash(df,["A","B"]))
    def test_different_data_different_hash(self):
        df1 = pd.DataFrame({"ID":[1],"VAL":["hello"]})
        df2 = pd.DataFrame({"ID":[1],"VAL":["world"]})
        assert compute_row_hash(df1,["ID","VAL"]).iloc[0] != compute_row_hash(df2,["ID","VAL"]).iloc[0]
    def test_hash_is_32_chars(self):
        df = pd.DataFrame({"K":["val"]})
        assert all(len(v)==32 for v in compute_row_hash(df,["K"]))

class TestReconciliation:
    @pytest.fixture
    def source(self):
        return pd.DataFrame({"USER_ID":["u1","u2","u3","u4"],"EMAIL":["a@x.com","b@x.com","c@x.com","d@x.com"],"STATUS":["ACTIVE","ACTIVE","INACTIVE","ACTIVE"]})
    def test_exact_match(self, source):
        r = reconcile_datasets(source, source.copy(), ["USER_ID"])
        assert r["matching"]==4 and r["missing_in_target"]==0 and r["changed"]==0 and r["match_rate"]==1.0
    def test_missing_row_detected(self, source):
        r = reconcile_datasets(source, source.iloc[:-1].copy(), ["USER_ID"])
        assert r["missing_in_target"]==1
    def test_extra_row_detected(self, source):
        extra = pd.concat([source, pd.DataFrame({"USER_ID":["u99"],"EMAIL":["x@x.com"],"STATUS":["ACTIVE"]})], ignore_index=True)
        r = reconcile_datasets(source, extra, ["USER_ID"])
        assert r["extra_in_target"]==1
    def test_changed_value_detected(self, source):
        tgt = source.copy(); tgt.loc[0,"STATUS"] = "INACTIVE"
        r = reconcile_datasets(source, tgt, ["USER_ID"], ["STATUS"])
        assert r["changed"]==1
