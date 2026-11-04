# Orchestrate DuckDB analytics with Amazon MWAA

This repository contains the sample assets for an AWS walkthrough that uses Amazon Managed Workflows for Apache Airflow (Amazon MWAA) to orchestrate DuckDB analytical SQL transformations. The example processes fictional AI inference telemetry stored in Amazon S3 and publishes Apache Iceberg tables to Amazon S3 Tables.

## Repository contents

- `duckdb_ai_inference_showback.py` — Amazon MWAA DAG with transformation and validation tasks
- `generate_ai_inference_data.py` — deterministic synthetic-data generator
- `generator-requirements.txt` — Python dependency for the data generator
- `publish_showback.sql` — transformation and publication SQL
- `validate_showback.sql` — validation SQL

## Generate the sample data

Install the generator dependency and create the fictional dataset:

```bash
python3 -m pip install -r generator-requirements.txt
python3 generate_ai_inference_data.py \
  --output sample_ai_inference \
  --start-date 2026-09-01 \
  --days 30 \
  --events-per-day 10000 \
  --seed 42
```

Upload the generated files to Amazon S3:

```bash
aws s3 cp sample_ai_inference/ \
  s3://<SOURCE-BUCKET>/ai-showback/ \
  --recursive
```

## Configure and deploy the DAG

Before uploading `duckdb_ai_inference_showback.py` to the DAG folder configured for your Amazon MWAA environment, replace these placeholders:

- `__SOURCE_BUCKET__` with the name of the Amazon S3 source bucket
- `__TABLE_BUCKET_ARN__` with the ARN of the Amazon S3 Tables table bucket

Do not put AWS access keys in the DAG or SQL. The AWS-aware DuckDB hook obtains temporary credentials from the Amazon MWAA execution role.

The standalone SQL files match the SQL embedded in the DAG and are included for review and reuse. Follow the accompanying AWS walkthrough for the complete prerequisites, IAM permissions, and validation steps.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.
