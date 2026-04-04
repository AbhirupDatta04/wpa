import os
import pandas as pd
from config.settings import BUCKET_NAME
from src.utils.gcs_client import get_bucket
from src.utils.logger import get_logger

logger = get_logger(__name__)
LOCAL_TMP = "/tmp/wpa"

def upload_df(df: pd.DataFrame, layer: str, name: str, fmt: str = "parquet") -> None:
    os.makedirs(LOCAL_TMP, exist_ok=True)
    local = f"{LOCAL_TMP}/{name}.{fmt}"
    if fmt == "parquet":
        df.to_parquet(local, index=False)
    elif fmt == "csv":
        df.to_csv(local, index=False)
    else:
        raise ValueError(f"Unsupported format: {fmt}")
    get_bucket().blob(f"{layer}/{name}.{fmt}").upload_from_filename(local)
    logger.info(f"Uploaded {layer}/{name}.{fmt} ({len(df):,} rows)")

def read_df(layer: str, name: str, fmt: str = "parquet") -> pd.DataFrame:
    path = f"gs://{BUCKET_NAME}/{layer}/{name}.{fmt}"
    opts = {"token": "google_default"}
    if fmt == "parquet":
        return pd.read_parquet(path, storage_options=opts)
    elif fmt == "csv":
        return pd.read_csv(path, storage_options=opts)
    else:
        raise ValueError(f"Unsupported format: {fmt}")
