"""데이터 검증 로직."""

from __future__ import annotations

import math
from collections.abc import Iterable

from services.data_pipeline.domain.models import (
    PCA_FEATURES_COUNT,
    RawTransaction,
    ValidationError,
    ValidationReport,
)


class TransactionValidator:
    """RawTransaction 스키마 검증기."""

    def validate(
        self,
        transactions: Iterable[RawTransaction],
    ) -> tuple[list[RawTransaction], ValidationReport]:
        """모든 transaction을 검증하고, 유효한 것만 반환 + ValidationReport 동봉."""
        valid: list[RawTransaction] = []
        errors: list[ValidationError] = []
        total = 0

        for idx, txn in enumerate(transactions):
            total += 1
            row_errors = self._validate_one(txn, idx)
            if row_errors:
                errors.extend(row_errors)
            else:
                valid.append(txn)

        report = ValidationReport(
            total_records=total,
            errors=tuple(errors),
        )
        return valid, report

    def _validate_one(
        self,
        txn: RawTransaction,
        index: int,
    ) -> list[ValidationError]:
        errors: list[ValidationError] = []

        if not txn.transaction_id:
            errors.append(
                ValidationError("transaction_id", "empty transaction_id", index)
            )

        if txn.time_seconds < 0:
            errors.append(
                ValidationError("time_seconds", "negative time_seconds", index)
            )

        if txn.amount < 0:
            errors.append(
                ValidationError("amount", "negative amount", index)
            )

        if not isinstance(txn.is_fraud, bool):
            errors.append(
                ValidationError("is_fraud", "invalid is_fraud type", index)
            )

        if len(txn.pca_features) != PCA_FEATURES_COUNT:
            errors.append(
                ValidationError(
                    "pca_features", "pca_features count mismatch", index
                )
            )

        if any(math.isnan(v) or math.isinf(v) for v in txn.pca_features):
            errors.append(
                ValidationError(
                    "pca_features", "pca_features contains NaN or Inf", index
                )
            )

        return errors
