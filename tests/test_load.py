"""pytest-based tests for jpcorpreg client (zenken downloads)"""
import os
import contextlib
import pandas as pd
import pytest
from jpcorpreg.client import CorporateRegistryClient


def test_download_shimane_df(shimane_df, expected_columns):
    """Live test: fetch Shimane data as DataFrame and validate shape/columns."""
    assert isinstance(shimane_df, pd.DataFrame)
    assert list(shimane_df.columns) == expected_columns
    assert len(shimane_df) > 20000


def test_download_shimane_parquet(client, expected_columns):
    """Optional slow test: download Shimane and persist to parquet, then validate."""
    parquet_out_dir = client.fetch(prefecture="Shimane", format="parquet")
    try:
        assert isinstance(parquet_out_dir, str)
        assert os.path.exists(parquet_out_dir)
        # Using pyarrow dataset or pandas direct read
        # In partitioned directories, read_parquet on the folder works
        df = pd.read_parquet(parquet_out_dir)
        assert list(df.columns) == expected_columns
        assert len(df) > 20000
    finally:
        import shutil
        with contextlib.suppress(FileNotFoundError, OSError):
            shutil.rmtree(parquet_out_dir)


def test_invalid_prefecture_raises(client):
    """Invalid prefecture should raise ValueError from loader."""
    with pytest.raises(ValueError, match="Unexpected Prefecture"):
        client.fetch(prefecture="Atlantis")
