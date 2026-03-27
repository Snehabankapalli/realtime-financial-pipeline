"""
Unit tests for financial transaction transformation logic.
Tests run without a live Spark cluster using pytest + mocks.
"""

import pytest
from unittest.mock import patch, MagicMock

from src.spark.transformations import (
    HIGH_VALUE_THRESHOLD_CENTS,
    _MCC_CATEGORY_MAP,
)


class TestMccCategoryMapping:
    def test_known_mcc_returns_category(self):
        assert _MCC_CATEGORY_MAP["5411"] == "GROCERY"
        assert _MCC_CATEGORY_MAP["5812"] == "RESTAURANT"
        assert _MCC_CATEGORY_MAP["4511"] == "AIRLINE"

    def test_all_mcc_values_are_uppercase(self):
        for category in _MCC_CATEGORY_MAP.values():
            assert category == category.upper(), f"{category} should be uppercase"

    def test_all_mcc_keys_are_four_digits(self):
        for code in _MCC_CATEGORY_MAP.keys():
            assert len(code) == 4 and code.isdigit(), f"{code} is not a 4-digit MCC"


class TestHighValueThreshold:
    def test_threshold_is_positive(self):
        assert HIGH_VALUE_THRESHOLD_CENTS > 0

    def test_threshold_represents_meaningful_amount(self):
        # Threshold should be at least $100
        assert HIGH_VALUE_THRESHOLD_CENTS >= 10_000


class TestTransactionProducerModel:
    def test_transaction_event_create(self):
        from src.producer.transaction_producer import TransactionEvent

        event = TransactionEvent.create(
            account_id="ACC-123",
            amount_cents=5000,
            merchant_id="MERCH-456",
        )

        assert event.account_id == "ACC-123"
        assert event.amount_cents == 5000
        assert event.merchant_id == "MERCH-456"
        assert event.currency == "USD"
        assert event.transaction_id  # must be non-empty UUID
        assert event.event_timestamp  # must be set

    def test_transaction_event_defaults(self):
        from src.producer.transaction_producer import TransactionEvent

        event = TransactionEvent.create(
            account_id="ACC-999",
            amount_cents=0,
            merchant_id="MERCH-001",
        )

        assert event.is_international is False
        assert event.currency == "USD"
        assert event.metadata == {}
