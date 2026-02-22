import csv
import json
from decimal import Decimal

import pytest

from services.data_pipeline.domain.models import (
    Feature,
    ValidationError,
    ValidationReport,
)
from services.data_pipeline.infrastructure.local_repository import (
    LocalFileTransactionRepository,
    LocalFileValidationReportRepository,
)
from tests.data_pipeline.infrastructure.conftest import make_kaggle_row


class TestLocalFileTransactionRepository:
    def test_load_raw_transactions(self, kaggle_csv):
        csv_path = kaggle_csv([
            make_kaggle_row(Time="0.0", Amount="149.62", Class="0"),
            make_kaggle_row(Time="1.0", Amount="2.69", Class="0"),
            make_kaggle_row(Time="2.0", Amount="378.66", Class="1"),
        ])
        repo = LocalFileTransactionRepository()
        transactions = list(repo.load_raw_transactions(str(csv_path)))
        assert len(transactions) == 3
        assert transactions[0].transaction_id == "txn_000000"
        assert transactions[2].is_fraud is True

    def test_load_empty_csv(self, kaggle_csv):
        csv_path = kaggle_csv([])
        repo = LocalFileTransactionRepository()
        transactions = list(repo.load_raw_transactions(str(csv_path)))
        assert len(transactions) == 0

    def test_load_nonexistent_file_raises(self):
        repo = LocalFileTransactionRepository()
        with pytest.raises(FileNotFoundError):
            list(repo.load_raw_transactions("/nonexistent/path.csv"))


class TestSaveFeatures:
    """save_features 메서드 테스트."""

    @pytest.fixture()
    def sample_features(self) -> list[Feature]:
        return [
            Feature(
                transaction_id="txn_000000",
                amount=Decimal("149.62"),
                hour_of_day=0,
                day_of_week=1,
                amount_bin="high",
                is_fraud=False,
            ),
            Feature(
                transaction_id="txn_000001",
                amount=Decimal("2.69"),
                hour_of_day=12,
                day_of_week=5,
                amount_bin="low",
                is_fraud=True,
            ),
        ]

    def test_save_features_creates_csv(self, tmp_path, sample_features):
        """피처 리스트를 저장하면 CSV 파일이 생성된다."""
        dest = tmp_path / "features.csv"
        repo = LocalFileTransactionRepository()

        result = repo.save_features(sample_features, str(dest))

        assert result == dest
        assert dest.exists()

    def test_save_features_csv_headers_and_data(self, tmp_path, sample_features):
        """저장된 CSV의 헤더와 데이터가 Feature 필드와 일치한다."""
        dest = tmp_path / "features.csv"
        repo = LocalFileTransactionRepository()
        repo.save_features(sample_features, str(dest))

        with open(dest, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            assert reader.fieldnames == [
                "transaction_id",
                "amount",
                "hour_of_day",
                "day_of_week",
                "amount_bin",
                "is_fraud",
                "is_weekend",
            ]
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["transaction_id"] == "txn_000000"
        assert rows[0]["amount"] == "149.62"
        assert rows[0]["hour_of_day"] == "0"
        assert rows[0]["day_of_week"] == "1"
        assert rows[0]["amount_bin"] == "high"
        assert rows[0]["is_fraud"] == "False"
        assert rows[0]["is_weekend"] == "False"

        # day_of_week=5 → is_weekend=True
        assert rows[1]["is_weekend"] == "True"
        assert rows[1]["is_fraud"] == "True"

    def test_save_features_empty(self, tmp_path):
        """빈 피처 리스트를 저장하면 헤더만 있는 CSV가 생성된다."""
        dest = tmp_path / "empty.csv"
        repo = LocalFileTransactionRepository()
        repo.save_features([], str(dest))

        with open(dest, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 0

    def test_save_features_auto_creates_directory(self, tmp_path, sample_features):
        """존재하지 않는 디렉토리 경로에 저장하면 디렉토리가 자동 생성된다."""
        dest = tmp_path / "nested" / "deep" / "features.csv"
        repo = LocalFileTransactionRepository()

        result = repo.save_features(sample_features, str(dest))

        assert result == dest
        assert dest.exists()


class TestLocalFileValidationReportRepository:
    """LocalFileValidationReportRepository 테스트."""

    def test_save_report_creates_json(self, tmp_path):
        """리포트를 저장하면 JSON 파일이 생성된다."""
        report = ValidationReport(total_records=100, errors=())
        dest = tmp_path / "report.json"
        repo = LocalFileValidationReportRepository()

        result = repo.save_report(report, str(dest))

        assert result == dest
        assert dest.exists()

    def test_save_report_json_structure(self, tmp_path):
        """저장된 JSON의 구조가 올바르다."""
        report = ValidationReport(total_records=100, errors=())
        dest = tmp_path / "report.json"
        repo = LocalFileValidationReportRepository()
        repo.save_report(report, str(dest))

        with open(dest, encoding="utf-8") as f:
            data = json.load(f)

        assert data["total_records"] == 100
        assert data["valid_records"] == 100
        assert data["error_rate"] == 0.0
        assert data["is_valid"] is True
        assert data["errors"] == []

    def test_save_report_auto_creates_directory(self, tmp_path):
        """존재하지 않는 디렉토리 경로에 저장하면 디렉토리가 자동 생성된다."""
        report = ValidationReport(total_records=10, errors=())
        dest = tmp_path / "nested" / "report.json"
        repo = LocalFileValidationReportRepository()

        result = repo.save_report(report, str(dest))

        assert result == dest
        assert dest.exists()

    def test_save_report_with_errors(self, tmp_path):
        """에러가 포함된 리포트가 올바르게 저장된다."""
        errors = (
            ValidationError(field="amount", message="음수 금액", record_index=3),
            ValidationError(field="time", message="유효하지 않은 시간", record_index=7),
        )
        report = ValidationReport(total_records=50, errors=errors)
        dest = tmp_path / "report.json"
        repo = LocalFileValidationReportRepository()
        repo.save_report(report, str(dest))

        with open(dest, encoding="utf-8") as f:
            data = json.load(f)

        assert data["total_records"] == 50
        assert data["valid_records"] == 48
        assert data["error_rate"] == pytest.approx(0.04)
        assert data["is_valid"] is False
        assert len(data["errors"]) == 2
        assert data["errors"][0] == {
            "field": "amount",
            "message": "음수 금액",
            "record_index": 3,
        }
        assert data["errors"][1] == {
            "field": "time",
            "message": "유효하지 않은 시간",
            "record_index": 7,
        }
