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

バージョン2.0.0より、高速なストリーミングデータ処理とParquet対応に最適化された `CorporateRegistryClient` クラスが提供されました。

### クライアントの使用
はじめにライブラリとクライアントを読み込みます:
```python
from jpcorpreg import CorporateRegistryClient
client = CorporateRegistryClient()
```

### 全件・指定都道府県データの読込
特定の都道府県のデータをダウンロードしてDataFrameで取得するには `fetch` メソッドを使用します:
```python
>>> df = client.fetch("Shimane")
```

日本全国の全データを一括でダウンロードする場合は、引数なし、もしくは `"All"` で実行します:
```python
>>> df = client.fetch()
```

### 差分データの読込
毎日の差分更新データ（日次差分）をダウンロードしたい場合は、`fetch_diff` メソッドを使用します。`YYYYMMDD` 形式の `date` 引数を渡すことで、指定した日付の差分データをダウンロードできます。日付を指定しない場合は、サイト上で公開されている最新の差分データが返されます。
```python
>>> df = client.fetch_diff("20260220")
```

### Parquet出力とパーティショニング (Data Lake向け)
ダウンロードしたデータをDataFrameとして返す代わりに、データレイクなどの分析基盤向けにParquetフォーマットで保存したい場合は、引数に `format="parquet"` を指定します。さらに、`partition_cols` を用いて、データを列ごとに分割されたディレクトリツリーとして強力に書き出すことが可能です。

**パーティショニングに関する注意点:**
- `fetch()` (全件洗い替えデータ) に対しては `prefecture_name` などでのディレクトリ分割を推奨します。巨大な月次洗い替えデータに対し `update_date` を指定するとファイルが数千のチャンクに断片化し、クエリパフォーマンスが悪化する恐れがあります。
- `fetch_diff()` (日次差分データ) に対しては `update_date` によって日々の更新差分を綺麗なディレクトリ構造でストック・追記していく形が非常に適しています。

```python
>>> # 差分データをダウンロードし、更新日(update_date)によってParquetデータセットを作る例
>>> out_dir = client.fetch_diff(format="parquet", partition_cols=["update_date"])
```

その後、pandas等を使ってParquet出力されたディレクトリ郡を一括で高速に読み込むことができます:
```python
>>> import pandas as pd
>>> df = pd.read_parquet(out_dir)
```
