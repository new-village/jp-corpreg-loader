import os
import tempfile
import zipfile
import re
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.csv as pacsv
from typing import List, Optional, Union

from jpcorpreg.utility import load_config

class CorporateDataWriter:
    """Handles parsing and writing of corporate registry CSVs to DataFrame or Parquet dataset."""
    
    def __init__(self):
        self.headers = load_config("header")
        # Ensure schema typing to string to prevent inference mismatches across chunks
        self.schema = pa.schema([pa.field(col, pa.string()) for col in self.headers])

    def uncompress_and_parse(
        self, 
        zip_path: str, 
        format_type: str = "df", 
        output_dir: Optional[str] = None, 
        partition_cols: Optional[List[str]] = None
    ) -> Union[pd.DataFrame, str]:
        """
        Uncompresses a zip file, streams the CSV content without loading everything in memory,
        and either builds a DataFrame or writes out a Parquet dataset (potentially partitioned).
        """
        try:
            with zipfile.ZipFile(zip_path) as zf:
                # Find CSV target
                members = [n for n in zf.namelist() if re.search(r"\.csv$", n, re.IGNORECASE)]
                if not members:
                    raise zipfile.BadZipFile("No CSV found in ZIP.")
                target = members[0]

                # Extract CSV to temp file for streaming
                tmp_csv = tempfile.NamedTemporaryFile(prefix="jpn_csv_v2_", suffix=".csv", delete=False)
                with zf.open(target, "r") as src, open(tmp_csv.name, "wb") as dst:
                    while True:
                        chunk = src.read(8 * 1024 * 1024)
                        if not chunk:
                            break
                        dst.write(chunk)
                
                csv_file_path = tmp_csv.name

        except zipfile.BadZipFile:
            raise

        try:
            if format_type == "df":
                return self._parse_to_df(csv_file_path)
            elif format_type == "parquet":
                if not output_dir:
                    # Provide a default temp directory if not specified
                    output_dir = tempfile.mkdtemp(prefix="jpn_parquet_v2_")
                return self._parse_to_parquet_dataset(csv_file_path, output_dir, partition_cols)
            else:
                raise ValueError("Invalid format. Use 'df' or 'parquet'.")
        finally:
            if os.path.exists(csv_file_path):
                os.remove(csv_file_path)

    def _parse_to_df(self, csv_file_path: str) -> pd.DataFrame:
        """Reads CSV entirely into a pandas DataFrame using pyarrow for speed."""
        read_options = pacsv.ReadOptions(column_names=self.headers, encoding="utf8")
        parse_options = pacsv.ParseOptions()
        # Convert all to string to match v1 behavior string mappings
        convert_options = pacsv.ConvertOptions(column_types=self.schema)
        
        table = pacsv.read_csv(
            csv_file_path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options
        )
        return table.to_pandas()

    def _parse_to_parquet_dataset(
        self, 
        csv_file_path: str, 
        output_dir: str, 
        partition_cols: Optional[List[str]] = None
    ) -> str:
        """Reads CSV in streaming fashion (chunks) and writes using PyArrow Dataset API."""
        read_options = pacsv.ReadOptions(column_names=self.headers, encoding="utf8")
        parse_options = pacsv.ParseOptions()
        convert_options = pacsv.ConvertOptions(column_types=self.schema)
        
        # We use PyArrow's streaming CSV reader to handle massive files smoothly
        with pacsv.open_csv(
            csv_file_path, 
            read_options=read_options, 
            parse_options=parse_options, 
            convert_options=convert_options
        ) as reader:
            
            # Using Dataset API to write streams directly to partitioned or flat directories.
            ds.write_dataset(
                reader, 
                base_dir=output_dir, 
                format="parquet",
                partitioning=partition_cols if partition_cols else None,
                existing_data_behavior="overwrite_or_ignore",
                max_open_files=1024,
                max_rows_per_file=1_000_000,
                min_rows_per_group=100_000,
                max_rows_per_group=1_000_000
            )

        return output_dir
