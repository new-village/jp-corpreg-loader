import pytest
import pandas as pd
import tempfile
import os
from jpcorpreg.load import SabunLoader, load_diff

def test_load_diff_df():
    df = load_diff(format="df")
    assert isinstance(df, pd.DataFrame)
    assert not df.empty

def test_load_diff_parquet():
    parquet_path = load_diff(format="parquet")
    assert isinstance(parquet_path, str)
    assert parquet_path.endswith(".parquet")
    assert os.path.exists(parquet_path)
    # Check that it can be read
    df = pd.read_parquet(parquet_path)
    assert not df.empty
    os.remove(parquet_path)

def test_load_diff_invalid_date():
    with pytest.raises(ValueError, match="No sabun data found for the date"):
        load_diff(date="19990101")  # very old date should not be there

def test_sabun_loader_date_parsing():
    loader = SabunLoader()  # Won't crash on init if site is up
    assert loader._convert_japanese_date("令和8年2月20日") == "20260220"
    assert loader._convert_japanese_date("令和元年5月1日") == "20190501"
    assert loader._convert_japanese_date("平成31年4月30日") == "20190430"
    
