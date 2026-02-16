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

        pca_features = tuple(
            float(row[col]) for col in _PCA_COLUMNS
        )

        return RawTransaction(
            transaction_id=f"txn_{row_index:06d}",
            time_seconds=float(row["Time"]),
            amount=amount,
            is_fraud=row["Class"] == "1",
            pca_features=pca_features,
        )
