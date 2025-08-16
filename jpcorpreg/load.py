'''load.py
'''
import os
import re
import zipfile
import tempfile

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from bs4 import BeautifulSoup

from jpcorpreg.utility import load_config

# Dictionary mapping Japanese prefecture names to English
PREF_MAP = {
    "全国": "All",
    "北海道": "Hokkaido",
    "青森県": "Aomori",
    "岩手県": "Iwate",
    "宮城県": "Miyagi",
    "秋田県": "Akita",
    "山形県": "Yamagata",
    "福島県": "Fukushima",
    "茨城県": "Ibaraki",
    "栃木県": "Tochigi",
    "群馬県": "Gunma",
    "埼玉県": "Saitama",
    "千葉県": "Chiba",
    "東京都": "Tokyo",
    "神奈川県": "Kanagawa",
    "新潟県": "Niigata",
    "富山県": "Toyama",
    "石川県": "Ishikawa",
    "福井県": "Fukui",
    "山梨県": "Yamanashi",
    "長野県": "Nagano",
    "岐阜県": "Gifu",
    "静岡県": "Shizuoka",
    "愛知県": "Aichi",
    "三重県": "Mie",
    "滋賀県": "Shiga",
    "京都府": "Kyoto",
    "大阪府": "Osaka",
    "兵庫県": "Hyogo",
    "奈良県": "Nara",
    "和歌山県": "Wakayama",
    "鳥取県": "Tottori",
    "島根県": "Shimane",
    "岡山県": "Okayama",
    "広島県": "Hiroshima",
    "山口県": "Yamaguchi",
    "徳島県": "Tokushima",
    "香川県": "Kagawa",
    "愛媛県": "Ehime",
    "高知県": "Kochi",
    "福岡県": "Fukuoka",
    "佐賀県": "Saga",
    "長崎県": "Nagasaki",
    "熊本県": "Kumamoto",
    "大分県": "Oita",
    "宮崎県": "Miyazaki",
    "鹿児島県": "Kagoshima",
    "沖縄県": "Okinawa",
    "国外": "Other"
}

def load(prefecture: str = "All", format: str = "df"):
    """Loads data for a specified prefecture.

    Args:
        prefecture (str): The name of the prefecture to load data for. Defaults to "All".

    Returns:
        DataFrame: A DataFrame containing the loaded data.
    """
    loader = ZipLoader()

    if format == "df":
        return loader.to_df(prefecture)
    elif format == "parquet":
        return loader.to_parquet(prefecture)
    else:
        raise ValueError("Invalid format. Use 'df' or 'parquet'.")

def read_csv(file_path: str) -> pd.DataFrame:
    """Reads a CSV file from a specified path.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        DataFrame: A DataFrame containing the CSV data.
    """
    header = load_config("header")
    return pd.read_csv(file_path, encoding='utf-8', header=None, names=header, dtype='object')

class ZipLoader():
    """Handles the loading and processing of zip files from a specified URL."""
    def __init__(self):
        self.url = "https://www.houjin-bangou.nta.go.jp/download/zenken/"

        try:
            response = requests.get(self.url, timeout=(3.0, 60.0))
            soup = BeautifulSoup(response.text, "html.parser")
        except requests.exceptions.RequestException as exp:
            raise SystemExit(f"Request to {self.url} has been failure") from exp
        
        key = "jp.go.nta.houjin_bangou.framework.web.common.CNSFWTokenProcessor.request.token"
        self.payload = {key: self._load_token(soup, key), "event": "download"}
        self.pref_list = self._fetch_file_ids(soup)

    def to_parquet(self, prefecture: str) -> str:
        csv_path = self._zip_load(prefecture)
        return self._convert_csv_2_parquet(csv_path)

    def to_df(self, prefecture: str) -> pd.DataFrame:
        csv_path = self._zip_load(prefecture)
        return self._convert_csv_2_df(csv_path)

    def _zip_load(self, prefecture) -> str:
        """Loads and processes a zip file from the server using a file ID.

        Args:
            file_id (str): The file ID to request the zip file.

        Returns:
            DataFrame: A DataFrame containing the data from the zip file.
        """
        try:
            file_id = self.pref_list[prefecture.capitalize()]
        except KeyError as exp:
            raise SystemExit(f"Unexpected Key Value: {prefecture}") from exp

        zip_path = self._download_zip(file_id)
        return self._uncompress_file(zip_path)

    def _load_token(self, soup, key) -> str:
        """Loads a security token from the server for requests.

        Args:
            url (str): The URL to load the token from.
            key (str): The key name of the token to retrieve.

        Returns:
            str: The token as a string.

        Raises:
            SystemExit: If the request fails.
        """
        return soup.find("input", {"name": key, "type": "hidden"})["value"]

    def _download_zip(self, file_id) -> str:
        """Downloads a zip file from the server using the specified file ID.

        Args:
            file_id (str): The file ID to use for the download.

        Returns:
            bytes: The content of the zip file as bytes.

        Raises:
            SystemExit: If the request fails or the server responds with an error.
        """
        self.payload["selDlFileNo"] = file_id

        try:
            with requests.post(self.url, params=self.payload, stream=True, timeout=(3.0, 120.0)) as res:
                res.raise_for_status()
                tmp = tempfile.NamedTemporaryFile(prefix="jpnzip_", suffix=".zip", delete=False)
                with tmp as f:
                    for chunk in res.iter_content(chunk_size=8 * 1024 * 1024):
                        if chunk:
                            f.write(chunk)
                return tmp.name
        except requests.exceptions.RequestException as exp:
            raise SystemExit("Request to download ZIP failed") from exp

    def _uncompress_file(self, zip_path: str) -> str:
        """Uncompresses the zip file content and extracts the CSV file.

        Args:
            content (bytes): The content of the zip file as bytes.

        Returns:
            StringIO: The CSV file content as a StringIO object.

        Raises:
            zipfile.BadZipFile: If the content is not a valid zip file.
        """
        try:
            with zipfile.ZipFile(zip_path) as zf:
                # Enumerate candidate CSV files
                members = [n for n in zf.namelist() if re.search(r"\.csv$", n, re.IGNORECASE)]
                if not members:
                    raise zipfile.BadZipFile("No CSV found in ZIP.")
                target = members[0]

                # Save CSV file to temporary file
                tmp_csv = tempfile.NamedTemporaryFile(prefix="jpncsv_", suffix=".csv", delete=False)
                with zf.open(target, "r") as src, open(tmp_csv.name, "wb") as dst:
                    while True:
                        chunk = src.read(8 * 1024 * 1024)
                        if not chunk:
                            break
                        dst.write(chunk)
                return tmp_csv.name
        except zipfile.BadZipFile:
            raise

    def _convert_csv_2_df(self, csv_path: str) -> pd.DataFrame:
        """Converts a CSV file to a DataFrame using predefined headers.

        Args:
            csv_path (str): The path to the CSV file.

        Returns:
            DataFrame: A DataFrame created from the CSV file.
        """
        header = load_config("header")
        return pd.read_csv(csv_path, encoding='utf-8', header=None, names=header, dtype='object')
    
    def _convert_csv_2_parquet(self, csv_path: str) -> str:
        """Converts a CSV file to a Parquet file.

        Args:
            csv_path (str): The path to the CSV file.

        Returns:
            str: The path to the created Parquet file.
        """
        header = load_config("header")
        parquet_path = csv_path.replace(".csv", ".parquet")

        # Create writer first and write chunk by chunk
        writer = None
        try:
            for chunk in pd.read_csv(
                csv_path,
                encoding="utf-8",
                header=None,
                names=header,
                dtype="object",
                chunksize=200_000,   # メモリに合わせて調整
            ):
                table = pa.Table.from_pandas(chunk, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(parquet_path, table.schema)
                writer.write_table(table)
        finally:
            if writer:
                writer.close()

        return parquet_path

    def _fetch_file_ids(self, soup) -> dict:
        """
        Extracts file IDs from the parsed HTML of a webpage.
        
        Args:
        soup (BeautifulSoup): The BeautifulSoup object containing the parsed HTML.
        
        Returns:
        dict: A dictionary mapping region names to their corresponding file IDs.
        """

        unicode_table = soup.find('div', class_='inBox21').find_all('div', class_='tbl02')[1]
        rows = unicode_table.find_all('dl')
        region_file_ids = {}
        for row in rows:
            region_name_jp = row.find('dt', class_='mb05').text.strip()
            region_name = PREF_MAP.get(region_name_jp, region_name_jp)  # Convert Japanese to English if possible
            file_id = re.search(r'\d{5}', row.find('a').get('onclick')).group()  # Extract file ID using regex
            region_file_ids[region_name] = file_id
        return region_file_ids
