from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """공유 설정."""

    s3_bucket: str = "fraud-detection-data"
    s3_raw_data_prefix: str = "raw/"
    s3_features_prefix: str = "features/"
    mlflow_tracking_uri: str = "http://localhost:5000"
