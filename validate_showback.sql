INSTALL aws;
LOAD aws;
INSTALL iceberg;
LOAD iceberg;
SET memory_limit = '2GB';
SET threads = 1;
SET temp_directory = '/tmp/duckdb-ai-showback-spill';
SET TimeZone = 'UTC';

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

CREATE OR REPLACE TEMP TABLE summary_validation_metrics AS
SELECT
    (SELECT COUNT(*)
     FROM s3t.ai_showback.daily_application_showback)
        AS daily_application_rows,
    (SELECT COUNT(*)
     FROM s3t.ai_showback.team_budget_status)
        AS team_budget_rows,
    (SELECT COUNT(*)
     FROM (
         SELECT
             usage_date,
             application_id,
             team_id,
             cost_center,
             model_id,
             use_case
         FROM s3t.ai_showback.daily_application_showback
         GROUP BY ALL
         HAVING COUNT(*) > 1
     ))
        AS duplicate_daily_keys,
    (SELECT COUNT(*)
     FROM (
         SELECT
             month_start,
             team_id,
             cost_center
         FROM s3t.ai_showback.team_budget_status
         GROUP BY ALL
         HAVING COUNT(*) > 1
     ))
        AS duplicate_budget_keys;

SELECT
    CASE
        WHEN daily_application_rows > 0
            THEN 'PASS'
        ELSE error('daily_application_showback is empty')
    END AS daily_row_count_check,
    CASE
        WHEN team_budget_rows > 0
            THEN 'PASS'
        ELSE error('team_budget_status is empty')
    END AS budget_row_count_check,
    CASE
        WHEN duplicate_daily_keys = 0
            THEN 'PASS'
        ELSE error('Duplicate logical keys found in daily_application_showback')
    END AS daily_key_uniqueness_check,
    CASE
        WHEN duplicate_budget_keys = 0
            THEN 'PASS'
        ELSE error('Duplicate logical keys found in team_budget_status')
    END AS budget_key_uniqueness_check,
    daily_application_rows,
    team_budget_rows,
    duplicate_daily_keys,
    duplicate_budget_keys
FROM summary_validation_metrics;

SELECT
    allocation_status,
    COUNT(*) AS invocation_count,
    SUM(cost_credits) AS cost_credits
FROM s3t.ai_showback.costed_invocations
GROUP BY allocation_status
ORDER BY allocation_status;

SELECT
    'PASS' AS event_id_uniqueness,
    'PASS' AS manifest_row_count,
    'PASS' AS allocation_coverage,
    'PASS' AS daily_key_uniqueness,
    'PASS' AS budget_key_uniqueness,
    actual_unique_events,
    allocated_events,
    missing_rate_events,
    missing_ownership_events,
    non_success_events,
    daily_application_rows,
    team_budget_rows,
    duplicate_daily_keys,
    duplicate_budget_keys
FROM validation_metrics
CROSS JOIN summary_validation_metrics;
