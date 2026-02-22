import os
import re
import tempfile
import requests
from bs4 import BeautifulSoup
from typing import Optional, List, Union
import pandas as pd

from jpcorpreg.writer import CorporateDataWriter

# Dictionary mapping Japanese prefecture names to English
PREF_MAP = {
    "全国": "All",
    "北海道": "Hokkaido", "青森県": "Aomori", "岩手県": "Iwate", "宮城県": "Miyagi",
    "秋田県": "Akita", "山形県": "Yamagata", "福島県": "Fukushima", "茨城県": "Ibaraki",
    "栃木県": "Tochigi", "群馬県": "Gunma", "埼玉県": "Saitama", "千葉県": "Chiba",
    "東京都": "Tokyo", "神奈川県": "Kanagawa", "新潟県": "Niigata", "富山県": "Toyama",
    "石川県": "Ishikawa", "福井県": "Fukui", "山梨県": "Yamanashi", "長野県": "Nagano",
    "岐阜県": "Gifu", "静岡県": "Shizuoka", "愛知県": "Aichi", "三重県": "Mie",
    "滋賀県": "Shiga", "京都府": "Kyoto", "大阪府": "Osaka", "兵庫県": "Hyogo",
    "奈良県": "Nara", "和歌山県": "Wakayama", "鳥取県": "Tottori", "島根県": "Shimane",
    "岡山県": "Okayama", "広島県": "Hiroshima", "山口県": "Yamaguchi", "徳島県": "Tokushima",
    "香川県": "Kagawa", "愛媛県": "Ehime", "高知県": "Kochi", "福岡県": "Fukuoka",
    "佐賀県": "Saga", "長崎県": "Nagasaki", "熊本県": "Kumamoto", "大分県": "Oita",
    "宮崎県": "Miyazaki", "鹿児島県": "Kagoshima", "沖縄県": "Okinawa", "国外": "Other"
}

class CorporateRegistryClient:
    """
    A modern, object-oriented client for downloading and streaming national corporate registry data
    from the National Tax Agency's Corporate Number Publication Site in Japan.
    """
    
    ZENKEN_URL = "https://www.houjin-bangou.nta.go.jp/download/zenken/"
    SABUN_URL = "https://www.houjin-bangou.nta.go.jp/download/sabun/"

    def __init__(self):
        self.session = requests.Session()
        self.writer = CorporateDataWriter()

    def download_all(
        self, 
        format: str = "df", 
        output_dir: Optional[str] = None, 
        partition_cols: Optional[List[str]] = None
    ) -> Union[pd.DataFrame, str]:
        """
        Downloads corporate registry data for all prefectures.
        """
        return self._download_and_process(
            prefecture="All", 
            format_type=format, 
            output_dir=output_dir, 
            partition_cols=partition_cols
        )

    def download_prefecture(
        self, 
        prefecture: str, 
        format: str = "df", 
        output_dir: Optional[str] = None, 
        partition_cols: Optional[List[str]] = None
    ) -> Union[pd.DataFrame, str]:
        """
        Downloads corporate registry data for a specific prefecture.
        """
        return self._download_and_process(
            prefecture=prefecture, 
            format_type=format, 
            output_dir=output_dir, 
            partition_cols=partition_cols
        )

    def download_diff(
        self, 
        date: Optional[str] = None, 
        format: str = "df", 
        output_dir: Optional[str] = None, 
        partition_cols: Optional[List[str]] = None
    ) -> Union[pd.DataFrame, str]:
        """
        Downloads differential (sabun) corporate registry update data.
        If `date` is provided (YYYYMMDD), past differential data is requested.
        Otherwise, the latest differential is fetched.
        """
        soup, token = self._get_page_and_token(self.SABUN_URL + "index.html")
        file_id = self._fetch_sabun_file_id(soup, date)
        
        payload = {
            "jp.go.nta.houjin_bangou.framework.web.common.CNSFWTokenProcessor.request.token": token,
            "event": "download",
            "selDlFileNo": file_id
        }

        zip_path = self._download_zip(self.SABUN_URL, payload)
        
        try:
            return self.writer.uncompress_and_parse(
                zip_path=zip_path, 
                format_type=format, 
                output_dir=output_dir, 
                partition_cols=partition_cols
            )
        finally:
            if os.path.exists(zip_path):
                os.remove(zip_path)

    # --- Internal helpers ---

    def _download_and_process(
        self, 
        prefecture: str, 
        format_type: str, 
        output_dir: Optional[str], 
        partition_cols: Optional[List[str]]
    ) -> Union[pd.DataFrame, str]:
        """Executes the pipeline for zenken (full volume) downloads."""
        soup, token = self._get_page_and_token(self.ZENKEN_URL)
        pref_list = self._fetch_zenken_file_ids(soup)

        capitalized_pref = prefecture.capitalize()
        if capitalized_pref not in pref_list:
            raise ValueError(f"Unexpected Prefecture or Region: {prefecture}")

        file_id = pref_list[capitalized_pref]

        payload = {
            "jp.go.nta.houjin_bangou.framework.web.common.CNSFWTokenProcessor.request.token": token,
            "event": "download",
            "selDlFileNo": file_id
        }

        zip_path = self._download_zip(self.ZENKEN_URL, payload)
        
        try:
            return self.writer.uncompress_and_parse(
                zip_path=zip_path, 
                format_type=format_type, 
                output_dir=output_dir, 
                partition_cols=partition_cols
            )
        finally:
            if os.path.exists(zip_path):
                os.remove(zip_path)

    def _get_page_and_token(self, url: str) -> tuple[BeautifulSoup, str]:
        """Fetches the page and extracts the hidden CSRF security token."""
        try:
            resp = self.session.get(url, timeout=(3.0, 60.0))
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
        except requests.exceptions.RequestException as e:
            raise SystemExit(f"Request to {url} failed: {e}") from e

        key = "jp.go.nta.houjin_bangou.framework.web.common.CNSFWTokenProcessor.request.token"
        token_input = soup.find("input", {"name": key, "type": "hidden"})
        if not token_input:
            raise ValueError(f"Security token '{key}' not found on {url}")
        
        return soup, token_input["value"]

    def _fetch_zenken_file_ids(self, soup: BeautifulSoup) -> dict:
        """Parses the HTML to map prefectures to their download file IDs."""
        unicode_table = soup.find('div', class_='inBox21').find_all('div', class_='tbl02')[1]
        rows = unicode_table.find_all('dl')
        region_file_ids = {}
        for row in rows:
            region_name_jp = row.find('dt', class_='mb05').text.strip()
            # Default to original Japanese if not found in map
            region_name = PREF_MAP.get(region_name_jp, region_name_jp)  
            
            a_tag = row.find('a')
            if a_tag and a_tag.has_attr('onclick'):
                match = re.search(r'\d{5}', a_tag['onclick'])
                if match:
                    region_file_ids[region_name] = match.group()
        return region_file_ids

    def _convert_japanese_date(self, raw_date: str) -> Optional[str]:
        """Converts '令和8年2月20日' to '20260220'."""
        match_reiwa = re.search(r'令和(\d+|元)年(\d+)月(\d+)日', raw_date)
        if match_reiwa:
            year_val = 1 if match_reiwa.group(1) == '元' else int(match_reiwa.group(1))
            return f"{year_val + 2018:04d}{int(match_reiwa.group(2)):02d}{int(match_reiwa.group(3)):02d}"
            
        match_heisei = re.search(r'平成(\d+|元)年(\d+)月(\d+)日', raw_date)
        if match_heisei:
            year_val = 1 if match_heisei.group(1) == '元' else int(match_heisei.group(1))
            return f"{year_val + 1988:04d}{int(match_heisei.group(2)):02d}{int(match_heisei.group(3)):02d}"
            
        return None

    def _fetch_sabun_file_id(self, soup: BeautifulSoup, target_date: Optional[str]) -> str:
        """Parses the sabun HTML to find the file ID for a specific date or the latest one."""
        header = soup.find(id="csv-unicode")
        if header:
            table_div = header.find_next('div', class_='tbl03')
            if table_div and table_div.find('table'):
                tbody = table_div.find('table').find('tbody')
                if tbody:
                    for tr in tbody.find_all('tr'):
                        th = tr.find('th')
                        td = tr.find('td')
                        if th and td:
                            parsed_date = self._convert_japanese_date(th.text.strip())
                            
                            a_tag = td.find('a')
                            if a_tag and a_tag.has_attr('onclick'):
                                match_id = re.search(r'\d{5}', a_tag['onclick'])
                                if match_id:
                                    file_id = match_id.group()
                                    if target_date is None or parsed_date == target_date:
                                        return file_id

        if target_date is None:
            raise ValueError("Failed to retrieve the latest sabun file ID.")
        else:
            raise ValueError(f"No sabun data found for the date: {target_date}")

    def _download_zip(self, base_url: str, payload: dict) -> str:
        """Downloads a zip stream and writes to a temporary file."""
        try:
            with self.session.post(base_url, params=payload, stream=True, timeout=(3.0, 120.0)) as res:
                res.raise_for_status()
                tmp = tempfile.NamedTemporaryFile(prefix="jpnzip_v2_", suffix=".zip", delete=False)
                with tmp as f:
                    for chunk in res.iter_content(chunk_size=8 * 1024 * 1024):
                        if chunk:
                            f.write(chunk)
                return tmp.name
        except requests.exceptions.RequestException as exp:
            raise SystemExit("Request to download ZIP failed") from exp
