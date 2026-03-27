-- RAW_TRANSACTIONS: Landing table for financial transaction events
-- Append-only. No deduplication at this layer.
-- Retention: 90 days. Partitioned by event date for query performance.

CREATE TABLE IF NOT EXISTS FINTECH_DB.RAW.RAW_TRANSACTIONS (
    transaction_id          VARCHAR(36)     NOT NULL,
    account_id_token        VARCHAR(64)     NOT NULL,   -- HMAC-SHA256 token, never raw account ID
    account_id              VARCHAR(20)     NOT NULL,   -- Always '[TOKENIZED]' in this layer
    amount_cents            BIGINT          NOT NULL,
    amount_dollars          DECIMAL(18, 2)  NOT NULL,
    currency                VARCHAR(3)      NOT NULL,
    merchant_id             VARCHAR(50)     NOT NULL,
    merchant_category_code  VARCHAR(4)      NOT NULL,
    mcc_category            VARCHAR(30),
    event_type              VARCHAR(30)     NOT NULL,
    event_ts                TIMESTAMP_NTZ   NOT NULL,
    card_last_four          VARCHAR(4),
    is_international        BOOLEAN         NOT NULL DEFAULT FALSE,
    is_high_value           BOOLEAN         NOT NULL DEFAULT FALSE,
    metadata                VARIANT,
    kafka_partition         INTEGER,
    kafka_offset            BIGINT,
    ingested_at             TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _file_row_number        INTEGER         AUTOINCREMENT,

    CONSTRAINT pk_raw_transactions PRIMARY KEY (transaction_id, ingested_at)
)
CLUSTER BY (DATE_TRUNC('day', event_ts))
DATA_RETENTION_TIME_IN_DAYS = 90
COMMENT = 'Raw financial transaction events from Kafka. Append-only.';


-- Continuous ingestion pipe from S3 landing zone
CREATE PIPE IF NOT EXISTS FINTECH_DB.RAW.TRANSACTIONS_PIPE
    AUTO_INGEST = TRUE
    COMMENT = 'Snowpipe: S3 → RAW_TRANSACTIONS. Triggered by SQS notifications.'
AS
COPY INTO FINTECH_DB.RAW.RAW_TRANSACTIONS (
    transaction_id, account_id_token, account_id,
    amount_cents, amount_dollars, currency,
    merchant_id, merchant_category_code, event_type,
    event_ts, card_last_four, is_international, metadata
)
FROM (
    SELECT
        $1:transaction_id::VARCHAR,
        $1:account_id_token::VARCHAR,
        '[TOKENIZED]',
        $1:amount_cents::BIGINT,
        ROUND($1:amount_cents::BIGINT / 100.0, 2),
        UPPER(TRIM($1:currency::VARCHAR)),
        $1:merchant_id::VARCHAR,
        $1:merchant_category_code::VARCHAR,
        $1:event_type::VARCHAR,
        TO_TIMESTAMP_NTZ($1:event_timestamp::VARCHAR),
        $1:card_last_four::VARCHAR,
        $1:is_international::BOOLEAN,
        PARSE_JSON($1:metadata::VARCHAR)
    FROM @FINTECH_DB.RAW.TRANSACTIONS_STAGE
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE);
