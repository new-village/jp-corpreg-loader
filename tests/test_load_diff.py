import pytest
import pandas as pd
import shutil
import os
from jpcorpreg.client import CorporateRegistryClient

def test_download_diff_df(client):
    df = client.download_diff(format="df")
    assert isinstance(df, pd.DataFrame)
    assert not df.empty

def test_download_diff_parquet(client):
    parquet_out_dir = client.download_diff(format="parquet")
    assert isinstance(parquet_out_dir, str)
    assert os.path.exists(parquet_out_dir)
    
    # Check that it can be read
    df = pd.read_parquet(parquet_out_dir)
    assert not df.empty
    
    # Clean up output dir
    shutil.rmtree(parquet_out_dir, ignore_errors=True)

def test_download_diff_invalid_date(client):
    with pytest.raises(ValueError, match="No sabun data found for the date"):
        client.download_diff(date="19990101")  # very old date should not be there

def test_sabun_date_parsing(client):
    assert client._convert_japanese_date("令和8年2月20日") == "20260220"
    assert client._convert_japanese_date("令和元年5月1日") == "20190501"
    assert client._convert_japanese_date("平成31年4月30日") == "20190430"
    assert client._convert_japanese_date("不正な文字列") is None
