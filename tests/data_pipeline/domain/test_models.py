from datetime import datetime
from decimal import Decimal

from services.data_pipeline.domain.models import RawTransaction


class TestRawTransaction:
    def test_create_raw_transaction(self):
        tx = RawTransaction(
            transaction_id="tx_001",
            amount=Decimal("150.00"),
            timestamp=datetime(2024, 1, 15, 10, 30, 0),
            merchant_id="merchant_001",
            customer_id="customer_001",
            is_fraud=False,
        )
        assert tx.transaction_id == "tx_001"
        assert tx.amount == Decimal("150.00")
        assert tx.is_fraud is False

    def test_raw_transaction_immutable(self):
        tx = RawTransaction(
            transaction_id="tx_001",
            amount=Decimal("100.00"),
            timestamp=datetime(2024, 1, 15, 10, 30, 0),
            merchant_id="m1",
            customer_id="c1",
            is_fraud=False,
        )
        try:
            tx.amount = Decimal("999.99")
            assert False, "Should be frozen"
        except AttributeError:
            pass
