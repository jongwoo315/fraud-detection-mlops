"""로컬 파일시스템 기반 거래 데이터 저장소."""

from __future__ import annotations

import csv
import json
from collections.abc import Iterable, Iterator
from pathlib import Path

from services.data_pipeline.domain.models import Feature, RawTransaction, ValidationReport
from services.data_pipeline.domain.repositories import (
    TransactionRepository,
    ValidationReportRepository,
)
from services.data_pipeline.infrastructure.csv_parser import KaggleCsvParser


class LocalFileTransactionRepository(TransactionRepository):
    """로컬 CSV 파일에서 Kaggle 거래 데이터를 로드하는 저장소."""

    def __init__(self) -> None:
        self._parser = KaggleCsvParser()

    def load_raw_transactions(self, source: str) -> Iterator[RawTransaction]:
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {source}")

        with open(path, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                yield self._parser.parse_row(row, row_index=idx)

    def save_features(self, features: Iterable[Feature], destination: str) -> Path:
        """엔지니어링된 피처를 CSV 파일로 저장한다."""
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)

        fieldnames = [
            "transaction_id",
            "amount",
            "hour_of_day",
            "day_of_week",
            "amount_bin",
            "is_fraud",
            "is_weekend",
        ]

        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for feature in features:
                writer.writerow({
                    "transaction_id": feature.transaction_id,
                    "amount": str(feature.amount),
                    "hour_of_day": feature.hour_of_day,
                    "day_of_week": feature.day_of_week,
                    "amount_bin": feature.amount_bin,
                    "is_fraud": feature.is_fraud,
                    "is_weekend": feature.is_weekend,
                })

        return path


class LocalFileValidationReportRepository(ValidationReportRepository):
    """로컬 JSON 파일로 검증 리포트를 저장하는 저장소."""

    def save_report(self, report: ValidationReport, destination: str) -> Path:
        """검증 리포트를 JSON 파일로 저장한다."""
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)

        data = {
            "total_records": report.total_records,
            "valid_records": report.valid_records,
            "error_rate": report.error_rate,
            "is_valid": report.is_valid,
            "errors": [
                {
                    "field": error.field,
                    "message": error.message,
                    "record_index": error.record_index,
                }
                for error in report.errors
            ],
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        return path
