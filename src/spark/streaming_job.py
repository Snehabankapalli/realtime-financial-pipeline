"""
PySpark Structured Streaming job: Kafka → Snowflake.
Handles schema evolution, PII tokenization, dead-letter routing, and micro-batch writes.
"""

import logging
from dataclasses import dataclass
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, BooleanType, MapType, TimestampType,
)

from src.spark.pii_tokenizer import tokenize_account_id
from src.spark.snowflake_writer import SnowflakeWriter
from src.spark.transformations import (
    enrich_with_mcc_category,
    flag_high_value_transactions,
    normalize_currency,
)

logger = logging.getLogger(__name__)

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",        StringType(),  nullable=False),
    StructField("account_id",            StringType(),  nullable=False),
    StructField("amount_cents",          LongType(),    nullable=False),
    StructField("currency",              StringType(),  nullable=False),
    StructField("merchant_id",           StringType(),  nullable=False),
    StructField("merchant_category_code",StringType(),  nullable=False),
    StructField("event_type",            StringType(),  nullable=False),
    StructField("event_timestamp",       StringType(),  nullable=False),
    StructField("card_last_four",        StringType(),  nullable=False),
    StructField("is_international",      BooleanType(), nullable=False),
    StructField("metadata",              MapType(StringType(), StringType()), nullable=True),
])


@dataclass
class StreamingConfig:
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str
    schema_registry_url: str
    snowflake_account: str
    snowflake_user: str
    snowflake_password: str
    snowflake_database: str
    snowflake_schema: str
    checkpoint_location: str
    dlq_topic: str = "transactions-dlq"
    max_offsets_per_trigger: int = 50_000
    trigger_interval_seconds: int = 10


def build_spark_session(app_name: str = "financial-streaming-pipeline") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def read_from_kafka(spark: SparkSession, config: StreamingConfig) -> DataFrame:
    """Read raw Kafka stream with backpressure configuration."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.kafka_topic)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
        .option("kafka.group.id", config.kafka_group_id)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_and_validate(raw_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Parse JSON payload, validate schema. Returns (valid_df, invalid_df).
    Invalid records are routed to the dead-letter queue.
    """
    parsed = (
        raw_df
        .select(
            F.col("key").cast(StringType()).alias("kafka_key"),
            F.from_json(
                F.col("value").cast(StringType()),
                TRANSACTION_SCHEMA,
            ).alias("data"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("partition"),
            F.col("offset"),
        )
    )

    valid = (
        parsed
        .filter(F.col("data.transaction_id").isNotNull())
        .select("data.*", "kafka_timestamp", "partition", "offset")
    )

    invalid = (
        parsed
        .filter(F.col("data.transaction_id").isNull())
        .select(
            F.col("kafka_key"),
            F.col("kafka_timestamp"),
            F.lit("PARSE_FAILURE").alias("dlq_reason"),
            F.col("partition"),
            F.col("offset"),
        )
    )

    return valid, invalid


def transform(df: DataFrame) -> DataFrame:
    """Apply business logic transformations."""
    return (
        df
        .withColumn("account_id_token", tokenize_account_id(F.col("account_id")))
        .withColumn("account_id", F.lit("[TOKENIZED]"))
        .withColumn("event_ts", F.to_timestamp("event_timestamp"))
        .withColumn("amount_dollars", F.round(F.col("amount_cents") / 100.0, 2))
        .withColumn("mcc_category", enrich_with_mcc_category(F.col("merchant_category_code")))
        .withColumn("is_high_value", flag_high_value_transactions(F.col("amount_cents")))
        .withColumn("currency", normalize_currency(F.col("currency")))
        .withColumn("ingested_at", F.current_timestamp())
        .drop("event_timestamp")
    )


def run(config: StreamingConfig) -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Starting streaming job. Topic: %s", config.kafka_topic)

    raw_df = read_from_kafka(spark, config)
    valid_df, invalid_df = parse_and_validate(raw_df)
    transformed_df = transform(valid_df)

    writer = SnowflakeWriter(config)

    # Write valid records to Snowflake
    valid_query = (
        transformed_df.writeStream
        .foreachBatch(writer.write_batch)
        .outputMode("append")
        .option("checkpointLocation", f"{config.checkpoint_location}/valid")
        .trigger(processingTime=f"{config.trigger_interval_seconds} seconds")
        .start()
    )

    # Write invalid records to DLQ topic
    dlq_query = (
        invalid_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("topic", config.dlq_topic)
        .option("checkpointLocation", f"{config.checkpoint_location}/dlq")
        .outputMode("append")
        .start()
    )

    logger.info("Streaming queries started. Awaiting termination.")
    spark.streams.awaitAnyTermination()
