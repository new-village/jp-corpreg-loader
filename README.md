# jpcorpreg  
[![Test](https://github.com/new-village/jp-corpreg-loader/actions/workflows/test.yaml/badge.svg)](https://github.com/new-village/jp-corpreg-loader/actions/workflows/test.yaml)
![PyPI - Version](https://img.shields.io/pypi/v/jpcorpreg)
  
**jpcorpreg** is a Python library that downloads corporate registry which is published in the [Corporate Number Publication Site](https://www.houjin-bangou.nta.go.jp/en/) as a data frame.
   
  
## Installation  
----------------------
jpcorpreg is available on pip installation.
```sh
$ python -m pip install jpcorpreg
```
  
### GitHub Install
Installing the latest version from GitHub:  
```sh
$ git clone https://github.com/new-village/jpcorpreg
$ cd jpcorpreg
$ pip install -e .
```
    
## Usage
This section demonstrates how to use this library to load and process data from the National Tax Agency's [Corporate Number Publication Site](https://www.houjin-bangou.nta.go.jp/).

Starting with version 2.0.0, jpcorpreg provides a robust object-oriented client (`CorporateRegistryClient`) optimized for reading large datasets and native Parquet partitioning.

### Initializing the Client
First, import and initialize the client:
```python
from jpcorpreg import CorporateRegistryClient
client = CorporateRegistryClient()
```

### Direct Data Loading
To download data for a specific prefecture as a pandas DataFrame, use the `fetch` method. By passing the prefecture name in as an argument, it will perform streaming fetch from the National Tax site:
```python
>>> df = client.fetch("Shimane")
```

To execute the download across all prefectures across Japan, simply leave the parameter empty or pass `"All"`:
```python
>>> df = client.fetch()
```

### Differential Data Loading
If you want to download only the daily differential updates (sabun), use the `fetch_diff` function. By passing a `date` in `YYYYMMDD` format, you can download the diff for that specific date. If no date is provided, the latest available diff is returned.
```python
>>> df = client.fetch_diff("20260220")
```

### Parquet Output and Partitioning
If you prefer to save the downloaded data for data lakes explicitly, pass `format="parquet"`. You can also supply the `partition_cols` argument so that the dataset is written in partitioned directories on disk. The function returns the output base directory path.

**Partitioning Context Notes:**
- For `fetch()` (full wash dataset), use something like `partition_cols=["prefecture_name"]`. Avoid using "update_date" on a full data wash to prevent query fragmentation.
- For `fetch_diff()` (daily diff data), use `partition_cols=["update_date"]` to append daily updates seamlessly into your data lake structure.

```python
>>> # Example: Output differential data partitioned by update_date
>>> out_dir = client.fetch_diff(format="parquet", partition_cols=["update_date"])
```

You can then read the dynamically generated Parquet Dataset efficiently with pandas or PyArrow:
```python
>>> import pandas as pd
>>> df = pd.read_parquet(out_dir)
```
