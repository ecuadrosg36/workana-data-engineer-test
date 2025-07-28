import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


import pytest
import pandas as pd
from etl.transform import transform_transactions
from pathlib import Path

def test_transform_valid_csv(tmp_path):
    sample_csv = tmp_path / "sample.csv"
    sample_csv.write_text(
        "order_id,user_id,amount,ts,status\n"
        "1,100,29.99,2025-07-01T10:00:00,completed\n"
        "2,101,49.99,2025-07-01T12:00:00,failed\n"
    )
    df = transform_transactions(sample_csv)
    assert not df.empty
    assert df.shape[0] == 2
    assert set(df.columns) == {"order_id", "user_id", "amount", "ts", "status"}
