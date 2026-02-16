import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """공유 설정."""

    s3_bucket: str = os.environ.get("S3_BUCKET", "fraud-detection-data")
    s3_raw_data_prefix: str = os.environ.get("S3_RAW_DATA_PREFIX", "raw/")
    s3_features_prefix: str = os.environ.get("S3_FEATURES_PREFIX", "features/")
    mlflow_tracking_uri: str = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
