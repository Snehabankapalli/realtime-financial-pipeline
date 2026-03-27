"""
PII tokenization for financial transaction data.
Uses HMAC-SHA256 with a rotation-safe secret to produce deterministic, irreversible tokens.
Tokens are consistent for the same input, enabling joins without storing raw PII.
"""

import hashlib
import hmac
import os

from pyspark.sql import functions as F
from pyspark.sql.types import StringType


_SECRET = os.environ.get("PII_TOKENIZATION_SECRET", "").encode("utf-8")
if not _SECRET:
    raise EnvironmentError(
        "PII_TOKENIZATION_SECRET environment variable must be set. "
        "Never hardcode secrets in source code."
    )


def _hmac_token(value: str) -> str:
    """Produce a deterministic HMAC-SHA256 token for a PII value."""
    if not value:
        return ""
    return hmac.new(_SECRET, value.encode("utf-8"), hashlib.sha256).hexdigest()


tokenize_udf = F.udf(_hmac_token, StringType())


def tokenize_account_id(col):
    """Tokenize an account ID column. Returns a Spark Column expression."""
    return tokenize_udf(col)


def tokenize_card_number(col):
    """Tokenize a full card number column. Returns a Spark Column expression."""
    return tokenize_udf(col)
