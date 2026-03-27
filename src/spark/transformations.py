"""
Business logic transformations for financial transaction events.
All transformations are pure functions operating on Spark Column expressions.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, BooleanType

# ISO 4217 MCC category mapping (abbreviated)
_MCC_CATEGORY_MAP = {
    "5411": "GROCERY",
    "5812": "RESTAURANT",
    "5541": "GAS_STATION",
    "4111": "TRANSPORTATION",
    "5912": "PHARMACY",
    "5732": "ELECTRONICS",
    "7011": "HOTEL",
    "4511": "AIRLINE",
    "5999": "MISC_RETAIL",
    "6011": "ATM_CASH",
}

_MCC_BROADCAST = None  # Populated at job startup via _broadcast_mcc_map()

HIGH_VALUE_THRESHOLD_CENTS = 100_000  # $1,000.00


def enrich_with_mcc_category(mcc_col):
    """Map MCC code to human-readable category. Returns 'OTHER' for unknown codes."""
    mapping_expr = F.create_map(
        *[item for pair in _MCC_CATEGORY_MAP.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )
    return F.coalesce(mapping_expr[mcc_col], F.lit("OTHER"))


def flag_high_value_transactions(amount_cents_col):
    """Return True if transaction exceeds high-value threshold."""
    return amount_cents_col >= F.lit(HIGH_VALUE_THRESHOLD_CENTS)


def normalize_currency(currency_col):
    """Uppercase and trim currency codes for consistency."""
    return F.upper(F.trim(currency_col))
