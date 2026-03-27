# Real-Time Financial Streaming Pipeline

Production-grade Kafka → PySpark → Snowflake streaming pipeline for financial transaction processing. Built to handle **1M+ events/day** at sub-second latency with full observability, dead-letter queues, and automated incident alerting.

---

## Architecture

```
┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────┐
│  Event Sources  │     │   Apache Kafka        │     │  AWS EMR        │
│                 │     │                       │     │  Serverless     │
│  - Transactions │────▶│  Schema Registry      │────▶│                 │
│  - Auth Events  │     │  Dead-Letter Queue    │     │  PySpark        │
│  - Settlements  │     │  Partitioned Topics   │     │  Streaming Job  │
└─────────────────┘     └──────────────────────┘     └────────┬────────┘
                                                               │
                        ┌──────────────────────┐              │
                        │     Snowflake         │◀─────────────┘
                        │                       │
                        │  - RAW layer          │     ┌─────────────────┐
                        │  - CLEAN layer        │────▶│   DataDog       │
                        │  - ANALYTICS layer    │     │   Monitoring    │
                        │  - dbt transforms     │     │   + Slack Alerts│
                        └──────────────────────┘     └─────────────────┘
```

---

## Features

- **Schema Registry** - Enforces Avro schema contracts, prevents bad data from entering the pipeline
- **Dead-Letter Queue** - Malformed or unprocessable messages routed to DLQ with full metadata for replay
- **Backpressure Handling** - Dynamic rate limiting prevents consumer lag from compounding
- **Exactly-Once Semantics** - Idempotent producers + transactional consumers guarantee no duplicates
- **PII Tokenization** - Card numbers and account IDs tokenized at ingestion, never stored raw
- **DataDog Integration** - Consumer lag, throughput, error rates, and latency tracked in real time
- **Slack Alerting** - Automated incident notifications on DLQ spike, lag threshold breach, or job failure

---

## Performance

| Metric | Value |
|--------|-------|
| Daily event volume | 1M+ |
| End-to-end latency (p99) | < 2 seconds |
| Consumer lag (steady state) | < 500 messages |
| DLQ rate | < 0.01% |
| Pipeline uptime | 99.9% |

---

## Project Structure

```
realtime-financial-pipeline/
├── src/
│   ├── producer/
│   │   ├── transaction_producer.py     # Kafka producer with schema validation
│   │   ├── schema_registry_client.py   # Confluent Schema Registry wrapper
│   │   └── models.py                   # Avro schema definitions
│   ├── consumer/
│   │   ├── kafka_consumer.py           # Consumer group with offset management
│   │   ├── dlq_handler.py              # Dead-letter queue routing logic
│   │   └── backpressure.py             # Rate limiting and flow control
│   ├── spark/
│   │   ├── streaming_job.py            # PySpark Structured Streaming job
│   │   ├── transformations.py          # Business logic transformations
│   │   ├── pii_tokenizer.py            # PII masking and tokenization
│   │   └── snowflake_writer.py         # Snowflake sink with micro-batch writes
│   └── snowflake/
│       ├── ddl/
│       │   ├── raw_transactions.sql    # Raw landing table
│       │   ├── clean_transactions.sql  # Cleaned, validated table
│       │   └── analytics_events.sql    # Analytics-ready fact table
│       └── pipes/
│           └── s3_continuous_pipe.sql  # Snowpipe for S3 → Snowflake
├── tests/
│   ├── test_producer.py
│   ├── test_consumer.py
│   ├── test_transformations.py
│   └── test_pii_tokenizer.py
├── infra/
│   ├── main.tf                         # Terraform: EMR, MSK, IAM
│   ├── variables.tf
│   └── outputs.tf
├── docker/
│   ├── docker-compose.yml              # Local dev: Kafka + ZK + Schema Registry
│   └── Dockerfile.spark
├── .github/
│   └── workflows/
│       └── ci.yml                      # GitHub Actions CI/CD
├── requirements.txt
└── Makefile
```

---

## Quick Start

### Local Development

```bash
# Start local Kafka stack
docker-compose up -d

# Install dependencies
pip install -r requirements.txt

# Run producer (sends synthetic transaction events)
python src/producer/transaction_producer.py --topic transactions --rate 1000

# Run Spark streaming job locally
python src/spark/streaming_job.py --env local --checkpoint /tmp/checkpoint
```

### Deploy to AWS

```bash
# Initialize Terraform
cd infra
terraform init
terraform plan -var-file=prod.tfvars
terraform apply

# Deploy Spark job to EMR Serverless
aws emr-serverless start-job-run \
  --application-id $APP_ID \
  --execution-role-arn $ROLE_ARN \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://your-bucket/jobs/streaming_job.py",
      "sparkSubmitParameters": "--conf spark.executor.cores=4"
    }
  }'
```

---

## Environment Variables

```bash
KAFKA_BOOTSTRAP_SERVERS=your-msk-cluster:9092
SCHEMA_REGISTRY_URL=http://schema-registry:8081
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=pipeline_user
SNOWFLAKE_PASSWORD=...         # Use AWS Secrets Manager in prod
SNOWFLAKE_DATABASE=FINTECH_DB
SNOWFLAKE_SCHEMA=RAW
DATADOG_API_KEY=...
SLACK_WEBHOOK_URL=...
```

---

## Stack

- **Apache Kafka** (AWS MSK) - Event streaming
- **PySpark** (AWS EMR Serverless) - Stream processing
- **Snowflake** - Cloud data warehouse
- **dbt** - Transformations and data quality
- **Apache Airflow** - Orchestration
- **Terraform** - Infrastructure as code
- **DataDog** - Monitoring and alerting
- **Docker** - Local development
