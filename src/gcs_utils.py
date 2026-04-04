# src/gcs_utils.py
# Reusable GCS helpers — import anywhere

import pandas as pd
from google.cloud import storage
import os

from src.config import BUCKET_NAME

_client = storage.Client()

def upload_df(df: pd.DataFrame, layer: str, name: str, fmt: str = "parquet"):
    """Upload a DataFrame to GCS. fmt = parquet or csv."""
    os.makedirs("/tmp/wpa", exist_ok=True)
    local = f"/tmp/wpa/{name}.{fmt}"
    if fmt == "parquet":
        df.to_parquet(local, index=False)
    else:
        df.to_csv(local, index=False)
    blob_path = f"{layer}/{name}.{fmt}"
    _client.bucket(BUCKET_NAME).blob(blob_path).upload_from_filename(local)
    print(f"  ✅ {blob_path} uploaded to GCS")

def read_df(layer: str, name: str, fmt: str = "parquet") -> pd.DataFrame:
    """Read a DataFrame from GCS."""
    path = f"gs://{BUCKET_NAME}/{layer}/{name}.{fmt}"
    opts = {"token": "google_default"}
    if fmt == "parquet":
        return pd.read_parquet(path, storage_options=opts)
    return pd.read_csv(path, storage_options=opts)
