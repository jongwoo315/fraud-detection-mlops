"""S3 기반 Kaggle 거래 데이터 저장소."""

from __future__ import annotations

import csv
from collections.abc import Iterable, Iterator
from pathlib import Path

import boto3

from services.data_pipeline.domain.models import Feature, RawTransaction
from services.data_pipeline.domain.repositories import TransactionRepository
from services.data_pipeline.infrastructure.csv_parser import KaggleCsvParser


class S3TransactionRepository(TransactionRepository):
    """S3에서 Kaggle CSV 거래 데이터를 로드하는 저장소."""

    def __init__(self, bucket: str, s3_client=None) -> None:
        self._bucket = bucket
        self._s3 = s3_client or boto3.client("s3")
        self._parser = KaggleCsvParser()

    def load_raw_transactions(self, source: str) -> Iterator[RawTransaction]:
        response = self._s3.get_object(Bucket=self._bucket, Key=source)
        lines = (line.decode("utf-8") for line in response["Body"].iter_lines())
        reader = csv.DictReader(lines)
        for idx, row in enumerate(reader):
            yield self._parser.parse_row(row, row_index=idx)

    def save_features(self, features: Iterable[Feature], destination: str) -> Path:
        raise NotImplementedError("DEV-39에서 구현")
