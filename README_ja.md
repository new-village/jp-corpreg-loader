# jpcorpreg  
[![Test](https://github.com/new-village/jp-corpreg-loader/actions/workflows/test.yaml/badge.svg)](https://github.com/new-village/jp-corpreg-loader/actions/workflows/test.yaml)
![PyPI - Version](https://img.shields.io/pypi/v/jpcorpreg)
  
**jpcorpreg** は、国税庁の[法人番号公表サイト](https://www.houjin-bangou.nta.go.jp/)で公開されている法人登記データをDataFrameとしてダウンロードするためのPythonライブラリです。
   
  
## インストール
----------------------
jpcorpreg は pip でインストール可能です。
```sh
$ python -m pip install jpcorpreg
```
  
### GitHubからのインストール
GitHubから最新バージョンをインストールする場合:
```sh
$ git clone https://github.com/new-village/jpcorpreg
$ cd jpcorpreg
$ pip install -e .
```
    
## 使い方
このセクションでは、国税庁の[法人番号公表サイト](https://www.houjin-bangou.nta.go.jp/)からデータを読み込み、処理するためのライブラリの使い方を説明します。

### 直接データ読込
特定の都道府県のデータをダウンロードするには、`load`関数を使用します。都道府県名を引数として渡すことで、その都道府県のデータが含まれるDataFrameを取得できます。
```python
>>> import jpcorpreg
>>> df = jpcorpreg.load("Shimane")
```

引数なしで `load` 関数を実行すると、日本全国のデータがダウンロードされます。
```python
>>> import jpcorpreg
>>> df = jpcorpreg.load()
```

### 差分データの読込
毎日の差分更新データ（日次差分）のみをダウンロードしたい場合は、`load_diff`関数を使用します。`YYYYMMDD`形式の `date` 引数を渡すことで、指定した日付の差分データがダウンロードできます。日付を指定しない場合、最新の差分データが返されます。
```python
>>> import jpcorpreg
>>> df = jpcorpreg.load_diff("20260220")
```

### Parquet出力
ダウンロードしたデータをDataFrameとして返す代わりに、Parquetファイルとして保存したい場合は、`format="parquet"` を指定します。この関数は、生成された `.parquet` ファイルのパスを返します。
```python
>>> import jpcorpreg
>>> parquet_path = jpcorpreg.load("Shimane", format="parquet")
```

または差分データの場合:
```python
>>> import jpcorpreg
>>> parquet_path = jpcorpreg.load_diff(format="parquet")
```

その後、pandasを使ってParquetファイルを読み込むことができます:
```python
>>> import pandas as pd
>>> df = pd.read_parquet(parquet_path)
```

### CSVデータ読込
すでにダウンロード済みのCSVファイルがある場合は、`read_csv`関数を使用します。ファイルパスを引数として渡すことで、CSVデータから適切なヘッダが設定されたDataFrameを取得できます。
```python
>>> import jpcorpreg
>>> df = jpcorpreg.read_csv("path/to/data.csv")
```
