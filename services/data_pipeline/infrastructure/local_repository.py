"""로컬 파일시스템 기반 거래 데이터 저장소."""

from __future__ import annotations

import csv
from collections.abc import Iterable
from pathlib import Path

from services.data_pipeline.domain.models import Feature, RawTransaction
from services.data_pipeline.domain.repositories import TransactionRepository
from services.data_pipeline.infrastructure.csv_parser import KaggleCsvParser


class LocalFileTransactionRepository(TransactionRepository):
    """로컬 CSV 파일에서 Kaggle 거래 데이터를 로드하는 저장소."""

    def __init__(self) -> None:
        self._parser = KaggleCsvParser()

    def load_raw_transactions(self, source: str) -> Iterable[RawTransaction]:
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {source}")

        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader):
                yield self._parser.parse_row(row, row_index=idx)

    def save_features(self, features: Iterable[Feature], destination: str) -> Path:
        raise NotImplementedError("DEV-39에서 구현")
