# Orchestrate DuckDB analytics with Amazon MWAA

This repository contains the sample assets for an AWS walkthrough that uses Amazon Managed Workflows for Apache Airflow (Amazon MWAA) to orchestrate DuckDB analytical SQL transformations. The example processes fictional AI inference telemetry stored in Amazon S3 and publishes Apache Iceberg tables to Amazon S3 Tables.

## Repository contents

- `duckdb_ai_inference_showback.py` — Amazon MWAA DAG with transformation and validation tasks
- `generate_ai_inference_data.py` — deterministic synthetic-data generator
- `generator-requirements.txt` — Python dependency for the data generator
- `publish_showback.sql` — transformation and publication SQL
- `validate_showback.sql` — validation SQL
- `duckdb-mwaa-s3-tables-architecture-v2.png` — architecture diagram

## Architecture

![Five-stage architecture of the AI inference showback pipeline. Amazon S3 stores the source Parquet files. Amazon MWAA orchestrates two Airflow tasks in which DuckDB reads, cleans, combines, publishes, and validates the data. The pipeline commits the output to Apache Iceberg tables in Amazon S3 Tables.](duckdb-mwaa-s3-tables-architecture-v2.png)

*Figure 1: Amazon MWAA orchestrates two DuckDB tasks across five stages, from
reading Parquet data in Amazon S3 through publishing and validating Iceberg
tables in Amazon S3 Tables.*

The DAG runs two tasks in sequence. `transform_and_publish_showback` uses DuckDB
to read the source Parquet files, transform and deduplicate the records, and
publish detailed and summary Iceberg tables. `validate_showback` then checks
the published tables for expected counts, uniqueness, and allocation states.
DuckDB runs inside the Amazon MWAA worker; it isn't a separate service.

## Prerequisites and deployment steps

### Before you begin

Before configuring the DAG, make sure you have:

- Python 3 and the AWS CLI installed
- AWS credentials configured on the workstation used to generate and upload
  the sample data
- an Amazon S3 source bucket
- an Amazon S3 Tables table bucket in the same AWS Region as the Amazon MWAA
  environment
- an Amazon MWAA Provisioned environment running Apache Airflow 3.3.1 with
  Python 3.12
- an Amazon MWAA execution role with:
  - read access to the source S3 bucket
  - permissions to create, read, and update tables and table data in the S3
    Tables table bucket
- network connectivity from Amazon MWAA to Amazon S3, Amazon S3 Tables, and
  the official DuckDB extension repository

Amazon MWAA Provisioned environments running Apache Airflow 3.3.1 include
DuckDB, the DuckDB provider, and a `duckdb_default` connection with the
`duckdb_aws` connection type. You don't need to add packages to
`requirements.txt` or create the connection manually. In the Airflow UI,
choose **Admin**, then **Connections**, and confirm that `duckdb_default`
exists. If it doesn't appear, update the Amazon MWAA environment without
changing its settings to apply the latest Amazon MWAA image.

### 1. Generate and upload the sample data

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

Upload the generated files to the `ai-showback/` prefix in the source bucket:

```bash
aws s3 cp sample_ai_inference/ \
  s3://<SOURCE-BUCKET>/ai-showback/ \
  --recursive
```

Save the source bucket name for the DAG configuration.

### 2. Create an Amazon S3 Tables table bucket

Create a table bucket in the same AWS Region as the Amazon MWAA environment,
then retrieve its ARN:

```bash
aws s3tables create-table-bucket \
  --name <TABLE-BUCKET> \
  --region <AWS-REGION> \
  --query arn \
  --output text
```

Save the returned ARN for the DAG configuration. It has this format:
`arn:aws:s3tables:<REGION>:<ACCOUNT-ID>:bucket/<TABLE-BUCKET>`.

If the source or table bucket already exists, don't create another one.
Confirm that the Amazon MWAA execution role has the required access instead.

### 3. Configure and upload the DAG

Open `duckdb_ai_inference_showback.py` and replace:

- `__SOURCE_BUCKET__` with the source S3 bucket name
- `__TABLE_BUCKET_ARN__` with the S3 Tables table bucket ARN

Upload the configured `duckdb_ai_inference_showback.py` file to the Amazon S3
`dags/` folder used by the Amazon MWAA environment.

The SQL is already embedded in the DAG. The standalone `publish_showback.sql`
and `validate_showback.sql` files are provided for reference and don't need to
be uploaded to Amazon MWAA.

Don't put AWS access keys in the DAG or SQL. The AWS-aware DuckDB hook obtains
temporary credentials from the Amazon MWAA execution role.

### 4. Run and verify the DAG

In the Apache Airflow UI:

1. Locate and unpause the `duckdb_ai_inference_showback` DAG.
2. Manually trigger the DAG.
3. Confirm that `transform_and_publish_showback` succeeds.
4. Confirm that `validate_showback` runs afterward and succeeds.

Both tasks should appear as successful in the Airflow grid or graph view.

Trigger the unchanged DAG a second time. The validation task should succeed
again without creating duplicate detailed or summary rows.

## Clean up

To avoid ongoing charges, pause the DAG and remove its file from the Amazon
MWAA DAG folder. Delete the generated source data and tutorial Iceberg tables.
Delete the table bucket, source bucket, and Amazon MWAA environment only if
you created them exclusively for this sample.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.
