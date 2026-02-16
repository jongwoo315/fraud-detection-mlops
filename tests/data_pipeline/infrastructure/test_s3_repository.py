import csv
import io

import boto3
import pytest
from moto import mock_aws

from services.data_pipeline.infrastructure.s3_repository import S3TransactionRepository

_KAGGLE_FIELDNAMES = ["Time", *(f"V{i}" for i in range(1, 29)), "Amount", "Class"]


def _make_kaggle_row(**overrides) -> dict:
    row = {"Time": "0.0", "Amount": "149.62", "Class": "0"}
    for i in range(1, 29):
        row[f"V{i}"] = "0.0"
    row.update(overrides)
    return row


def _upload_csv(bucket: str, key: str, rows: list[dict]) -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=bucket)
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=_KAGGLE_FIELDNAMES)
    writer.writeheader()
    writer.writerows(rows)
    s3.put_object(Bucket=bucket, Key=key, Body=output.getvalue())


class TestS3TransactionRepository:
    @mock_aws
    def test_load_raw_transactions(self):
        _upload_csv("test-bucket", "raw/creditcard.csv", [
            _make_kaggle_row(Time="0.0", Amount="149.62", Class="0"),
            _make_kaggle_row(Time="1.0", Amount="2.69", Class="1"),
        ])
        repo = S3TransactionRepository(bucket="test-bucket")
        transactions = list(repo.load_raw_transactions("raw/creditcard.csv"))
        assert len(transactions) == 2
        assert transactions[0].transaction_id == "txn_000000"
        assert transactions[1].is_fraud is True

    @mock_aws
    def test_load_empty_csv(self):
        _upload_csv("test-bucket", "raw/empty.csv", [])
        repo = S3TransactionRepository(bucket="test-bucket")
        transactions = list(repo.load_raw_transactions("raw/empty.csv"))
        assert len(transactions) == 0

    @mock_aws
    def test_load_nonexistent_key_raises(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")
        repo = S3TransactionRepository(bucket="test-bucket")
        with pytest.raises(Exception):
            list(repo.load_raw_transactions("nonexistent.csv"))
