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

### Direct Data Loading
To download data for a specific prefecture, use the `load` function. By passing the prefecture name as an argument, you can obtain a DataFrame containing data for that prefecture.  
```python
>>> import jpcorpreg
>>> df = jpcorpreg.load("Shimane")
```

To execute the `load` function without argument, data for all prefectures across Japan will be downloaded. 
```python
>>> import jpcorpreg
>>> df = jpcorpreg.load()
```

### Parquet Output
If you prefer to save the downloaded data as a Parquet file instead of returning a DataFrame, pass `format="parquet"`. The function returns the path to the generated `.parquet` file.
```python
>>> import jpcorpreg
>>> parquet_path = jpcorpreg.load("Shimane", format="parquet")
```

You can then read the Parquet file with pandas:
```python
>>> import pandas as pd
>>> df = pd.read_parquet(parquet_path)
```

### CSV Data Loading
If you already have a downloaded CSV file, use the `read_csv` function. By passing the file path as an argument, you can obtain a DataFrame with headers from the CSV data.
```python
>>> import jpcorpreg
>>> df = jpcorpreg.read_csv("path/to/data.csv")
```
