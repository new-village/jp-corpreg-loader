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
        if format_type not in ("df", "parquet"):
            raise ValueError("Invalid format. Use 'df' or 'parquet'.")

        with zipfile.ZipFile(zip_path) as zf:
            # Find CSV target
            members = [n for n in zf.namelist() if re.search(r"\.csv$", n, re.IGNORECASE)]
            if not members:
                raise zipfile.BadZipFile("No CSV found in ZIP.")
            target = members[0]

            with zf.open(target, "r") as src:
                if format_type == "df":
                    return self._parse_to_df(src)
                elif format_type == "parquet":
                    if not output_dir:
                        output_dir = tempfile.mkdtemp(prefix="jpn_parquet_v2_")
                    return self._parse_to_parquet_dataset(src, output_dir, partition_cols)

    def _parse_to_df(self, src) -> pd.DataFrame:
        """Reads CSV stream in chunks to build a pandas DataFrame securely."""
        chunks = []
        # Need dtype=str to avoid mixed type warnings and match v1 behavior string mappings
        kwargs = {"names": self.headers, "dtype": str, "encoding": "utf-8"}
        
        # Read in chunks to avoid single massive allocation spike
        for chunk in pd.read_csv(src, chunksize=100000, **kwargs):
            chunks.append(chunk)
            
        return pd.concat(chunks, ignore_index=True)

    def _parse_to_parquet_dataset(
        self, 
        src, 
        output_dir: str, 
        partition_cols: Optional[List[str]] = None
    ) -> str:
        """Reads CSV stream in chunks and writes using PyArrow Dataset API."""
        
        kwargs = {"names": self.headers, "dtype": str, "encoding": "utf-8"}
        
        # We can't use pacsv directly on a zip stream reliably due to seek/tell limitations in pyarrow vs zipfile.
        # Instead, we read chunk streams with Pandas, convert to PyArrow tables, and write to Dataset.
        # This keeps the memory footprint bounded.
        
        # Note: writing to partitioned datasets piece by piece efficiently is tricky without pulling all to memory or 
        # risking lots of small files. PyArrow dataset scanner + write_dataset is ideal but requires pacsv.
        # Since pacsv.open_csv doesn't natively support Python file-like zip uncompressed streams well 
        # (raises ArrowInvalid or ArrowNotImplementedError for seeking), we use chunked ParquetWriter manually.
        
        # We'll use ds.write_dataset per chunk, but `existing_data_behavior="overwrite_or_ignore"` 
        # is required. Actually, writing chunks one-by-one with identical partition columns might create 
        # multiple files per partition per chunk. That's acceptable for Parquet directory structures.
        
        for chunk in pd.read_csv(src, chunksize=100000, **kwargs):
            table = pa.Table.from_pandas(chunk, schema=self.schema)
            # using dataset API to write this chunk
            ds.write_dataset(
                table, 
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
