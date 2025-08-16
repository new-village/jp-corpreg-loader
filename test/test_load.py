"""pytest-based tests for jpcorpreg.load and helpers"""
import os
import contextlib
import pandas as pd
import pytest
import jpcorpreg


def test_load_shimane_df(shimane_df, expected_columns):
    """Live test: fetch Shimane data as DataFrame and validate shape/columns."""
    assert isinstance(shimane_df, pd.DataFrame)
    assert list(shimane_df.columns) == expected_columns
    assert len(shimane_df) > 20000


def test_load_shimane_parquet(expected_columns):
    """Optional slow test: download Shimane and persist to parquet, then validate."""
    parquet_path = jpcorpreg.load(prefecture="Shimane", format="parquet")
    try:
        assert isinstance(parquet_path, str) and parquet_path.endswith(".parquet")
        assert os.path.exists(parquet_path)
        df = pd.read_parquet(parquet_path)
        assert list(df.columns) == expected_columns
        assert len(df) > 20000
    finally:
        # Best-effort cleanup of the parquet file
        with contextlib.suppress(FileNotFoundError, OSError):
            os.remove(parquet_path)


def test_read_csv_sample(expected_columns):
    """Read a small sample CSV and validate contents."""
    result = jpcorpreg.read_csv("./test/data/31_tottori_test_20240329.csv")
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == expected_columns
    assert len(result) == 5
    assert result.iloc[0]["corporate_number"] == "1000013050238"
    assert result.iloc[1]["name"] == "島田商事株式会社"


def test_invalid_prefecture_raises():
    """Invalid prefecture should raise SystemExit from loader."""
    with pytest.raises(SystemExit):
        jpcorpreg.load(prefecture="Atlantis")
