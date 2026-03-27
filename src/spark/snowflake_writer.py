"""
Snowflake micro-batch writer for PySpark Structured Streaming.
Uses the Snowflake Spark connector with merge-on-write for idempotent writes.
"""

import logging
from typing import Any

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

SNOWFLAKE_OPTIONS = {
    "sfURL":      "{account}.snowflakecomputing.com",
    "sfUser":     None,
    "sfPassword": None,
    "sfDatabase": None,
    "sfSchema":   None,
    "sfWarehouse": "PIPELINE_WH",
    "sfRole":     "PIPELINE_ROLE",
}


class SnowflakeWriter:
    """
    Writes micro-batches to Snowflake with deduplication and error handling.
    Configured as a foreachBatch sink in PySpark Structured Streaming.
    """

    def __init__(self, config: Any):
        self._options = {
            **SNOWFLAKE_OPTIONS,
            "sfURL":      f"{config.snowflake_account}.snowflakecomputing.com",
            "sfUser":     config.snowflake_user,
            "sfPassword": config.snowflake_password,
            "sfDatabase": config.snowflake_database,
            "sfSchema":   config.snowflake_schema,
        }
        self._table = "RAW_TRANSACTIONS"
        self._batches_written = 0
        self._rows_written = 0

    def write_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """
        Write a single micro-batch to Snowflake.
        Deduplicates on transaction_id within the batch before writing.
        """
        row_count = batch_df.count()
        if row_count == 0:
            logger.info("Batch %d is empty, skipping", batch_id)
            return

        deduped = batch_df.dropDuplicates(["transaction_id"])
        deduped_count = deduped.count()

        if deduped_count < row_count:
            logger.warning(
                "Batch %d: dropped %d duplicate transaction_ids",
                batch_id,
                row_count - deduped_count,
            )

        try:
            (
                deduped.write
                .format("net.snowflake.spark.snowflake")
                .options(**self._options)
                .option("dbtable", self._table)
                .mode("append")
                .save()
            )
            self._batches_written += 1
            self._rows_written += deduped_count
            logger.info(
                "Batch %d written: %d rows (total: %d rows across %d batches)",
                batch_id,
                deduped_count,
                self._rows_written,
                self._batches_written,
            )
        except Exception as exc:
            logger.error("Failed to write batch %d to Snowflake: %s", batch_id, exc)
            raise
