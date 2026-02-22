"""Kaggle Credit Card Fraud Detection CSV 파서."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

from services.data_pipeline.domain.models import RawTransaction

_PCA_COLUMNS = tuple(f"V{i}" for i in range(1, 29))


class KaggleCsvParser:
    """Kaggle CSV row를 RawTransaction 도메인 객체로 변환."""

    def parse_row(
        self,
        row: dict[str, str],
        *,
        row_index: int,
    ) -> RawTransaction:
        try:
            amount = Decimal(row["Amount"])
        except InvalidOperation as e:
            raise ValueError(
                f"Row {row_index}: invalid Amount"
                f" '{row['Amount']}'"
            ) from e

        try:
            pca_features = tuple(
                float(row[col]) for col in _PCA_COLUMNS
            )
        except ValueError as e:
            raise ValueError(
                f"Row {row_index}: invalid PCA feature value"
            ) from e

        try:
            time_seconds = float(row["Time"])
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"Row {row_index}: invalid Time '{row.get('Time')}'"
            ) from e

        class_value = row["Class"]
        if class_value not in ("0", "1"):
            raise ValueError(
                f"Row {row_index}: invalid Class '{class_value}', expected '0' or '1'"
            )

        return RawTransaction(
            transaction_id=f"txn_{row_index:06d}",
            time_seconds=time_seconds,
            amount=amount,
            is_fraud=class_value == "1",
            pca_features=pca_features,
        )
