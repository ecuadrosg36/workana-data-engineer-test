import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


import sqlite3
import pandas as pd
from etl.load import load_dataframe_to_sqlite, validate_table_not_empty

def test_load_dataframe_to_sqlite(tmp_path):
    db_path = tmp_path / "test.db"
    df = pd.DataFrame({
        "order_id": [1, 2],
        "user_id": [100, 101],
        "amount": [29.99, 49.99],
        "ts": pd.to_datetime(["2025-07-01T10:00:00", "2025-07-01T12:00:00"]),
        "status": ["COMPLETED", "FAILED"],
    })
    load_dataframe_to_sqlite(df, str(db_path), "transactions", "replace", chunksize=1000)

    count = validate_table_not_empty(str(db_path), "transactions")
    assert count == 2

    with sqlite3.connect(str(db_path)) as conn:
        result = pd.read_sql("SELECT COUNT(*) as total FROM transactions", conn)
        assert result["total"].iloc[0] == 2
