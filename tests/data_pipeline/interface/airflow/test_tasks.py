"""load_data task callable 테스트."""

from __future__ import annotations

import csv
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
