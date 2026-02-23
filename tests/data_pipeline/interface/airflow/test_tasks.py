"""load_data / validate_data task callable 테스트."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from tests.data_pipeline.interface.airflow.conftest import make_kaggle_row


class TestLoadData:
    def test_loads_csv_and_returns_metadata(self, kaggle_csv, data_dirs):
        rows = [
            make_kaggle_row(Time="0.0", Amount="149.62", Class="0"),
            make_kaggle_row(Time="1.0", Amount="2.69", Class="1"),
        ]
        source_path = str(kaggle_csv(rows))
        intermediate_dir = str(data_dirs["intermediate"])

        from services.data_pipeline.interface.airflow.tasks import load_data

        result = load_data(source_path=source_path, intermediate_dir=intermediate_dir)

        assert result["record_count"] == 2
        assert result["source_path"] == source_path
        assert Path(result["output_path"]).exists()

    def test_output_file_has_same_row_count(self, kaggle_csv, data_dirs):
        rows = [
            make_kaggle_row(),
            make_kaggle_row(Time="1.0"),
            make_kaggle_row(Time="2.0"),
        ]
        source_path = str(kaggle_csv(rows))
        intermediate_dir = str(data_dirs["intermediate"])

        from services.data_pipeline.interface.airflow.tasks import load_data

        result = load_data(source_path=source_path, intermediate_dir=intermediate_dir)

        with open(result["output_path"], newline="") as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            output_count = sum(1 for _ in reader)
        assert output_count == 3


class TestValidateData:
    def test_validates_and_saves_report(self, kaggle_csv, data_dirs):
        rows = [
            make_kaggle_row(Time="0.0", Amount="149.62", Class="0"),
            make_kaggle_row(Time="1.0", Amount="2.69", Class="1"),
        ]
        source_path = str(kaggle_csv(rows))
        report_dir = str(data_dirs["reports"])
        intermediate_dir = str(data_dirs["intermediate"])

        from services.data_pipeline.interface.airflow.tasks import validate_data

        result = validate_data(
            input_path=source_path,
            report_dir=report_dir,
            intermediate_dir=intermediate_dir,
        )

        assert result["valid_count"] == 2
        assert result["invalid_count"] == 0
        assert Path(result["report_path"]).exists()
        assert Path(result["output_path"]).exists()
        assert result["should_skip"] is False

        # report JSON이 올바르게 저장되었는지 확인
        with open(result["report_path"]) as f:
            report_data = json.load(f)
        assert report_data["total_records"] == 2
        assert report_data["valid_records"] == 2

        # 출력 CSV의 행 수 확인
        with open(result["output_path"], newline="") as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            output_count = sum(1 for _ in reader)
        assert output_count == 2

    def test_skip_when_error_rate_exceeds_threshold(self, kaggle_csv, data_dirs):
        rows = [
            make_kaggle_row(Amount="invalid"),
            make_kaggle_row(Amount="invalid"),
        ]
        source_path = str(kaggle_csv(rows))
        report_dir = str(data_dirs["reports"])
        intermediate_dir = str(data_dirs["intermediate"])

        from services.data_pipeline.interface.airflow.tasks import validate_data

        result = validate_data(
            input_path=source_path,
            report_dir=report_dir,
            intermediate_dir=intermediate_dir,
        )

        assert result["should_skip"] is True
        assert result["valid_count"] == 0
        assert result["invalid_count"] == 2
