"""Amazon MWAA DAG for the AI inference showback walkthrough.

Replace the two configuration tokens below with your resource values before
uploading the DAG. Do not put AWS credentials in this file.
"""

from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.duckdb.operators.duckdb import (
    DuckDBExecuteQueryOperator,
)


SOURCE_BUCKET = "__SOURCE_BUCKET__"
TABLE_BUCKET_ARN = "__TABLE_BUCKET_ARN__"

EVENTS_URI = (
    f"s3://{SOURCE_BUCKET}/ai-showback/"
    "inference_events/ingest_date=*/*.parquet"
)
RATES_URI = f"s3://{SOURCE_BUCKET}/ai-showback/reference/model_rates.parquet"
OWNERSHIP_URI = (
    f"s3://{SOURCE_BUCKET}/ai-showback/"
    "reference/application_ownership.parquet"
)
MANIFEST_URI = f"s3://{SOURCE_BUCKET}/ai-showback/manifest.json"

HOOK_PARAMS = {
    "memory_limit": "2GB",
    "threads": 2,
    "temp_directory": "/tmp/duckdb-ai-showback-spill",
}

PUBLISH_SQL = """
INSTALL iceberg;
LOAD iceberg;

ATTACH '{{ params.table_bucket_arn }}' AS s3t
  (TYPE iceberg, ENDPOINT_TYPE s3_tables);

CREATE SCHEMA IF NOT EXISTS s3t.ai_showback;

CREATE OR REPLACE TEMP TABLE raw_events AS
SELECT
    event_id,
    event_ts,
    ingested_at,
    application_id,
    model_id,
    input_tokens,
    output_tokens,
    COALESCE(cached_input_tokens, 0) AS cached_input_tokens,
    latency_ms,
    status_code,
    json_extract_string(context_json, '$.use_case') AS use_case,
    json_extract_string(context_json, '$.environment') AS environment
FROM read_parquet(
    '{{ params.events_uri }}',
    hive_partitioning = true,
    union_by_name = true
);

CREATE OR REPLACE TEMP TABLE latest_events AS
SELECT *
FROM raw_events
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id
    ORDER BY ingested_at DESC
) = 1;

CREATE OR REPLACE TEMP TABLE events_with_rates AS
SELECT
    e.*,
    r.input_credits_per_million,
    r.output_credits_per_million,
    r.cache_read_credits_per_million
FROM latest_events AS e
ASOF LEFT JOIN read_parquet('{{ params.rates_uri }}') AS r
    ON e.model_id = r.model_id
   AND e.event_ts >= r.effective_from;

CREATE OR REPLACE TEMP TABLE costed_batch AS
SELECT
    e.event_id,
    e.event_ts,
    e.ingested_at,
    e.application_id,
    e.model_id,
    o.team_id,
    o.cost_center,
    e.use_case,
    e.environment,
    e.input_tokens,
    e.output_tokens,
    e.cached_input_tokens,
    e.latency_ms,
    e.status_code,
    o.monthly_budget_credits,
    CASE
        WHEN e.status_code <> 200 THEN 0.0
        WHEN e.input_credits_per_million IS NULL THEN NULL
        ELSE (
            (e.input_tokens - e.cached_input_tokens)
                * e.input_credits_per_million
            + e.output_tokens * e.output_credits_per_million
            + e.cached_input_tokens
                * e.cache_read_credits_per_million
        ) / 1000000.0
    END AS cost_credits,
    CASE
        WHEN e.status_code <> 200 THEN 'non_success'
        WHEN e.input_credits_per_million IS NULL THEN 'missing_rate'
        WHEN o.team_id IS NULL THEN 'missing_ownership'
        ELSE 'allocated'
    END AS allocation_status
FROM events_with_rates AS e
ASOF LEFT JOIN read_parquet('{{ params.ownership_uri }}') AS o
    ON e.application_id = o.application_id
   AND e.event_ts >= o.effective_from;

CREATE TABLE IF NOT EXISTS s3t.ai_showback.costed_invocations (
    event_id VARCHAR,
    event_ts TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ,
    application_id VARCHAR,
    model_id VARCHAR,
    team_id VARCHAR,
    cost_center VARCHAR,
    use_case VARCHAR,
    environment VARCHAR,
    input_tokens BIGINT,
    output_tokens BIGINT,
    cached_input_tokens BIGINT,
    latency_ms INTEGER,
    status_code INTEGER,
    monthly_budget_credits DOUBLE,
    cost_credits DOUBLE,
    allocation_status VARCHAR
);

MERGE INTO s3t.ai_showback.costed_invocations AS target
USING costed_batch AS source
ON target.event_id = source.event_id
WHEN MATCHED THEN UPDATE SET
    event_ts = source.event_ts,
    ingested_at = source.ingested_at,
    application_id = source.application_id,
    model_id = source.model_id,
    team_id = source.team_id,
    cost_center = source.cost_center,
    use_case = source.use_case,
    environment = source.environment,
    input_tokens = source.input_tokens,
    output_tokens = source.output_tokens,
    cached_input_tokens = source.cached_input_tokens,
    latency_ms = source.latency_ms,
    status_code = source.status_code,
    monthly_budget_credits = source.monthly_budget_credits,
    cost_credits = source.cost_credits,
    allocation_status = source.allocation_status
WHEN NOT MATCHED THEN INSERT (
    event_id,
    event_ts,
    ingested_at,
    application_id,
    model_id,
    team_id,
    cost_center,
    use_case,
    environment,
    input_tokens,
    output_tokens,
    cached_input_tokens,
    latency_ms,
    status_code,
    monthly_budget_credits,
    cost_credits,
    allocation_status
) VALUES (
    source.event_id,
    source.event_ts,
    source.ingested_at,
    source.application_id,
    source.model_id,
    source.team_id,
    source.cost_center,
    source.use_case,
    source.environment,
    source.input_tokens,
    source.output_tokens,
    source.cached_input_tokens,
    source.latency_ms,
    source.status_code,
    source.monthly_budget_credits,
    source.cost_credits,
    source.allocation_status
);

CREATE OR REPLACE TEMP TABLE daily_batch AS
SELECT
    CAST(event_ts AS DATE) AS usage_date,
    application_id,
    team_id,
    cost_center,
    model_id,
    use_case,
    COUNT(*) AS invocation_count,
    SUM(input_tokens) AS input_tokens,
    SUM(output_tokens) AS output_tokens,
    SUM(cached_input_tokens) AS cached_input_tokens,
    SUM(cost_credits) AS cost_credits,
    AVG(latency_ms) AS average_latency_ms,
    COUNT(*) FILTER (WHERE status_code <> 200) AS failed_invocations,
    COUNT(*) FILTER (
        WHERE allocation_status <> 'allocated'
          AND allocation_status <> 'non_success'
    ) AS unallocated_invocations
FROM costed_batch
GROUP BY ALL;

CREATE TABLE IF NOT EXISTS s3t.ai_showback.daily_application_showback (
    usage_date DATE,
    application_id VARCHAR,
    team_id VARCHAR,
    cost_center VARCHAR,
    model_id VARCHAR,
    use_case VARCHAR,
    invocation_count BIGINT,
    input_tokens BIGINT,
    output_tokens BIGINT,
    cached_input_tokens BIGINT,
    cost_credits DOUBLE,
    average_latency_ms DOUBLE,
    failed_invocations BIGINT,
    unallocated_invocations BIGINT
);

MERGE INTO s3t.ai_showback.daily_application_showback AS target
USING daily_batch AS source
ON target.usage_date = source.usage_date
AND target.application_id = source.application_id
AND target.model_id = source.model_id
AND target.use_case = source.use_case
WHEN MATCHED THEN UPDATE SET
    team_id = source.team_id,
    cost_center = source.cost_center,
    invocation_count = source.invocation_count,
    input_tokens = source.input_tokens,
    output_tokens = source.output_tokens,
    cached_input_tokens = source.cached_input_tokens,
    cost_credits = source.cost_credits,
    average_latency_ms = source.average_latency_ms,
    failed_invocations = source.failed_invocations,
    unallocated_invocations = source.unallocated_invocations
WHEN NOT MATCHED THEN INSERT (
    usage_date,
    application_id,
    team_id,
    cost_center,
    model_id,
    use_case,
    invocation_count,
    input_tokens,
    output_tokens,
    cached_input_tokens,
    cost_credits,
    average_latency_ms,
    failed_invocations,
    unallocated_invocations
) VALUES (
    source.usage_date,
    source.application_id,
    source.team_id,
    source.cost_center,
    source.model_id,
    source.use_case,
    source.invocation_count,
    source.input_tokens,
    source.output_tokens,
    source.cached_input_tokens,
    source.cost_credits,
    source.average_latency_ms,
    source.failed_invocations,
    source.unallocated_invocations
);

CREATE OR REPLACE TEMP TABLE budget_batch AS
SELECT
    CAST(date_trunc('month', event_ts) AS DATE) AS month_start,
    team_id,
    cost_center,
    MAX(monthly_budget_credits) AS monthly_budget_credits,
    SUM(cost_credits) AS consumed_credits,
    100.0 * SUM(cost_credits)
        / NULLIF(MAX(monthly_budget_credits), 0) AS budget_used_percent,
    CASE
        WHEN SUM(cost_credits) > MAX(monthly_budget_credits)
            THEN 'over_budget'
        WHEN SUM(cost_credits) > 0.8 * MAX(monthly_budget_credits)
            THEN 'approaching_budget'
        ELSE 'within_budget'
    END AS budget_status
FROM costed_batch
WHERE team_id IS NOT NULL
GROUP BY ALL;

CREATE TABLE IF NOT EXISTS s3t.ai_showback.team_budget_status (
    month_start DATE,
    team_id VARCHAR,
    cost_center VARCHAR,
    monthly_budget_credits DOUBLE,
    consumed_credits DOUBLE,
    budget_used_percent DOUBLE,
    budget_status VARCHAR
);

MERGE INTO s3t.ai_showback.team_budget_status AS target
USING budget_batch AS source
ON target.month_start = source.month_start
AND target.team_id = source.team_id
AND target.cost_center = source.cost_center
WHEN MATCHED THEN UPDATE SET
    monthly_budget_credits = source.monthly_budget_credits,
    consumed_credits = source.consumed_credits,
    budget_used_percent = source.budget_used_percent,
    budget_status = source.budget_status
WHEN NOT MATCHED THEN INSERT (
    month_start,
    team_id,
    cost_center,
    monthly_budget_credits,
    consumed_credits,
    budget_used_percent,
    budget_status
) VALUES (
    source.month_start,
    source.team_id,
    source.cost_center,
    source.monthly_budget_credits,
    source.consumed_credits,
    source.budget_used_percent,
    source.budget_status
);
"""

VALIDATE_SQL = """
INSTALL iceberg;
LOAD iceberg;

SELECT version() AS duckdb_version;

ATTACH '{{ params.table_bucket_arn }}' AS s3t
  (TYPE iceberg, ENDPOINT_TYPE s3_tables);

CREATE OR REPLACE TEMP TABLE expected_manifest AS
SELECT *
FROM read_json_auto('{{ params.manifest_uri }}');

CREATE OR REPLACE TEMP TABLE validation_metrics AS
SELECT
    COUNT(*) AS actual_unique_events,
    COUNT(DISTINCT event_id) AS actual_distinct_events,
    COUNT(*) FILTER (
        WHERE allocation_status = 'allocated'
    ) AS allocated_events,
    COUNT(*) FILTER (
        WHERE allocation_status = 'missing_rate'
    ) AS missing_rate_events,
    COUNT(*) FILTER (
        WHERE allocation_status = 'missing_ownership'
    ) AS missing_ownership_events,
    COUNT(*) FILTER (
        WHERE allocation_status = 'non_success'
    ) AS non_success_events,
    SUM(cost_credits) AS total_cost_credits
FROM s3t.ai_showback.costed_invocations;

SELECT
    CASE
        WHEN actual_unique_events = actual_distinct_events
            THEN 'PASS'
        ELSE error('Duplicate event_id values found in costed_invocations')
    END AS unique_event_id_check,
    CASE
        WHEN actual_unique_events = (
            SELECT unique_event_ids FROM expected_manifest
        )
            THEN 'PASS'
        ELSE error(
            'Output row count does not match the generator manifest'
        )
    END AS manifest_row_count_check,
    CASE
        WHEN allocated_events > 0
         AND missing_rate_events > 0
         AND missing_ownership_events > 0
         AND non_success_events > 0
            THEN 'PASS'
        ELSE error('One or more expected allocation statuses are absent')
    END AS allocation_coverage_check,
    actual_unique_events,
    allocated_events,
    missing_rate_events,
    missing_ownership_events,
    non_success_events,
    total_cost_credits
FROM validation_metrics;

SELECT
    allocation_status,
    COUNT(*) AS invocation_count,
    SUM(cost_credits) AS cost_credits
FROM s3t.ai_showback.costed_invocations
GROUP BY allocation_status
ORDER BY allocation_status;
"""


with DAG(
    dag_id="duckdb_ai_inference_showback",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["duckdb", "ai-showback", "s3-tables"],
) as dag:
    transform_and_publish = DuckDBExecuteQueryOperator(
        task_id="transform_and_publish_showback",
        sql=PUBLISH_SQL,
        split_statements=True,
        params={
            "events_uri": EVENTS_URI,
            "rates_uri": RATES_URI,
            "ownership_uri": OWNERSHIP_URI,
            "table_bucket_arn": TABLE_BUCKET_ARN,
        },
        hook_params=HOOK_PARAMS,
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=30),
    )

    validate_showback = DuckDBExecuteQueryOperator(
        task_id="validate_showback",
        sql=VALIDATE_SQL,
        split_statements=True,
        return_last=False,
        params={
            "table_bucket_arn": TABLE_BUCKET_ARN,
            "manifest_uri": MANIFEST_URI,
        },
        hook_params=HOOK_PARAMS,
        do_xcom_push=True,
        show_return_value_in_logs=True,
        execution_timeout=timedelta(minutes=10),
    )

    transform_and_publish >> validate_showback
