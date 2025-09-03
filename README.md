# Credix Data Pipeline

A fully local, cost-friendly analytics pipeline that ingests data from PostgreSQL, lands CDC deltas in GCS as Parquet, enforces schema on load into a BigQuery temp layer, incrementally merges into Bronze with dbt, transforms in Silver and Gold, validates with dbt tests + Elementary, and publishes a static Elementary report.

This README explains the whole process, how to run it locally, the resources provisioned via Terraform, and suggested next steps for production hardening.
Check the Data Archicteture overview in [Data Architecture.md](https://github.com/brendajanuario/credix-data-pipeline/blob/main/Data%20Architecture.md)

## High-level architecture

- Source (OLTP): PostgreSQL running in Docker
- Extraction & CDC: Dagster + Python (pandas/pyarrow) pull deltas using watermarks
- Landing: Parquet files stored in GCS in a partitioned path
- Staging: Parquet -> BigQuery Temp (unique hashed tables per run + stable table)
- Medallion model (dbt):
  - Bronze: incremental merge on unique keys, append-only metadata
  - Silver: cleaned/standardized business-ready base
  - Gold: curated analytics & risk scoring models
- Testing & Monitoring: dbt tests + Elementary report published to a public GCS website

<img width="4246" height="3224" alt="image" src="https://github.com/user-attachments/assets/51eda6d1-7189-48d0-addf-2804a86950ad" />

## Tech stack

- Orchestration: Dagster (Python assets, schedules)
- Transformations: dbt (BigQuery)
- Storage/Compute: GCS + BigQuery
- Observability: dbt tests + Elementary report
- Infra-as-code: Terraform (datasets, buckets, IAM)
- Local OLTP: PostgreSQL in Docker, optional one-shot data-loader container


## Repository map

- Docker & local OLTP:
  - `docker-compose.yml` (Postgres, optional data-loader)
  - `docker/postgres/init/` (DB bootstrap SQL)
  - `docker/data-loader/` (Parquet -> Postgres loader)
- Dagster project:
  - `credix_pipeline/credix_pipeline/credix_pipeline/`
    - `assets/` (extraction to Parquet/GCS/temp tables, dbt bronze/silver/gold hooks, Elementary)
    - `resources/` (Postgres & GCP clients)
    - `utils/` (CDC helpers, data & GCS utilities)
    - `jobs/` (asset jobs: cnpj, installments, full, monitoring)
    - `definitions.py` (Dagster Definitions: schedules, resources, assets)
- dbt project:
  - `dbt/business_case/` (models for bronze/silver/gold, packages, profiles)
- Infra via Terraform:
  - `terraform/` (BigQuery datasets, buckets, IAM)


## PostgreSQL ingestion (Docker) and optional data seeding

- Postgres service is defined in `docker-compose.yml`. It exposes port 5432 and runs an init script from `docker/postgres/init/01_init_schema.sql`.
- Optional: a one-shot `data_loader` container (`docker/data-loader/`) loads local Parquet files into Postgres.
  - It discovers `*.parquet` under a mounted `/app/data` and appends to a target table with the same base filename.
  - The script uses SQLAlchemy to write into Postgres (`parquet_to_postgres.py`). Default schema there is `oltp`.

Important note:
- Ensure the schema used by the source tables exists. Current init script creates `raw_data`; Python data-loader defaults to `oltp`. Either:
  - change the loader to use `raw_data`, or
  - create `oltp` schema and load there, or
  - point Dagster extraction queries to whichever schema standardize on.


## CDC implementation (change data capture)

- CDC state is maintained inside Dagster via materialization metadata on checkpoint assets.
  - Helper: `utils/cdc_helpers.py#get_cdc_last_processed_time` reads the last watermark (`max_updated_at`) from the latest checkpoint materialization.
- Each extractor asset queries only new/changed rows since the last watermark:
  - CNPJ: uses `created_at` / `updated_at` timestamp columns.
  - Installments: uses the max among `invoice_issue_date`, `paid_date` with `GREATEST(...)`.
- After Bronze succeeds, checkpoint assets advance the watermark by emitting `max_updated_at` in metadata (only after successful downstream ingestion), ensuring exactly-once progression.


## Parquet data process and schema enforcement

- DataFrames are normalized for BigQuery compatibility in `utils/data_processing.py`:
  - `prepare_dataframe_for_bigquery`: coerces date/timestamp columns appropriately
  - `dataframe_to_parquet_bytes`: writes Parquet with Arrow, preserving logical types
- Files are uploaded to `gs://data_lake_credix/business_case/landing/ingestion_dt=YYYY-MM-DD/<table>_<ts>.parquet`.
- On load into BigQuery temp:
  - Retrieve schema from a reference table
  - Load with explicit schema and `WRITE_TRUNCATE` for the stable table.


## Bucket movement (landing -> archive/failed)

- After a successful BigQuery load, the file is moved to the archive bucket `data_lake_credix_archive` under the mirrored path (`archive/` prefix).
- If the BigQuery load fails, the blob is moved to a `failed/` prefix in the original bucket to aid incident response.
- Implemented in `GCPResource.move_blob()` with copy + delete semantics.


## Unique temporary table per run (isolate each run)

- To guarantee isolation and deterministic Bronze merges, each run writes to a unique hashed temp table in `business_case_temp`:
  - Name generated by `utils/gcs_operations.py#generate_unique_table_name` (e.g., `cnpj_ws_ab12cd34ef56`).
  - Alongside, a stable table (e.g., `business_case_temp.cnpj_ws_dbt`) is also overwritten to simplify ad hoc exploration and dbt sources.
- The hashed table name is passed to dbt as a var (`temp_table_name`) by the Dagster dbt asset runner; Bronze models then refer to the stable source, and the checkpoint cleans up the hashed table after success.


## dbt medallion layers

- Bronze:
  - Incremental merge models with `unique_key` and `merge_update_columns`.
  - Keeps metadata like `_loaded_at`, `_source_table`.
  - Models: `models/bronze/*.sql` (e.g., `cnpj_ws.sql`, `installments.sql`).
- Silver:
  - Cleansing, normalization, and derived flags.
  - Examples: standardize state/UF/city, data quality flags.
  - Models: `models/silver/*.sql` (e.g., `cnpj_ws_clean.sql`, `installments_clean.sql`).
- Gold:
  - Business-ready analytics and risk scoring.
  - Models: `company_payment_summary.sql` and `payment_analytics_detailed.sql`.


## dbt tests

- Column-level tests are defined (e.g., `not_null`, `unique`, `accepted_values`) in `models/bronze/schema.yml`.
- Elementary augments testing with anomaly detection and reporting.


## Elementary (static report)

- Two Dagster assets handle Elementary:
  - `edr_monitor_asset`: runs `edr monitor`
  - `edr_send_report_asset`: runs `edr send-report` and uploads `index.html` to the `credix-elementary-report` bucket
- Terraform configures that bucket as a static website and makes objects public for read.
- After a run, the report is available at a public URL: `https://storage.googleapis.com/credix-elementary-report/index.html`.


## Terraform resources

Defined under `terraform/`:

- BigQuery datasets: `business_case_bronze`, `business_case_silver`, `business_case_gold`, `business_case_temp`, `business_case_elementary` (see `bigquery.tf`).
- GCS buckets: raw `data_lake_credix`, archive `data_lake_credix_archive`, Elementary `credix-elementary-report` with public viewer IAM and website main page (see `storage.tf`).
- Backend state in GCS (see `main.tf`). Configure your state bucket before `terraform init`.

## How it all runs (end-to-end)

1) Extract from Postgres with CDC
- Dagster assets `cnpj_raw_data` / `installments_raw_data` query rows updated since the last watermark.
- Results are normalized and written as Parquet.

2) Land files in GCS
- Files are written to the partitioned landing path.
- No-change runs short-circuit and emit a `no_changes` marker.

3) Load to BigQuery temp
- For each landed file, two loads occur:
  - Hashed table (append) for run isolation and traceability
  - Stable table (truncate) for consistent dbt sources
- On success: move blob to archive; on failure: move to failed.

4) dbt Bronze/Silver/Gold
- Bronze merges incremental deltas keyed by business identifiers.
- Silver applies cleaning and business rules.
- Gold produces curated analytics and risk scoring.

5) Advance CDC checkpoint and cleanup
- Only after Bronze success, watermark is advanced and hashed temp table is deleted.

6) Tests and monitoring
- dbt tests run inside the dbt builds.
- Elementary monitor and report assets generate & publish a report.


## Local setup and running

Prerequisites
- macOS with Docker and Docker Compose
- Python 3.11+
- A GCP project with a service account that has access to BigQuery and GCS; JSON keyfile locally
- dbt (BigQuery adapter) and Elementary installed in your environment

Environment variables (examples)
- `POSTGRES_HOST=localhost`
- `POSTGRES_PORT=5432`
- `POSTGRES_DB=credix_transactions`
- `POSTGRES_USER=postgres`
- `POSTGRES_PASSWORD=postgres123`
- `GCP_PROJECT=product-reliability-analyzer`
- `GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/your-sa.json`

1) Bring up Postgres (and optionally seed it)
- Start Postgres: `docker compose up -d postgres`
- Optionally load local Parquet into Postgres tables: `docker compose --profile data-loader up --build data_loader`
  - The loader expects Parquet files to be mounted at `./data` -> `/app/data` inside the container.

2) Prepare Python environment
- Create/activate a virtualenv
- Install the Dagster/dbt project in editable mode (from `credix_pipeline/credix_pipeline/`):
  - `pip install -e .`
- Ensure dbt deps are installed in `dbt/business_case`: `dbt deps`

3) Configure dbt profiles
- `dbt/business_case/profiles.yml` points to your keyfile and `product-reliability-analyzer`. Adjust as needed.
- Alternatively, set `DBT_PROFILES_DIR` to the `dbt/` folder.

4) Run Dagster
- From the repository root, run: `dagster dev -m credix_pipeline.credix_pipeline.definitions`
- Open the Dagster UI, materialize one of:
  - `cnpj_pipeline` group (or its job `cnpj_data_pipeline`)
  - `installments_pipeline` group (or `installments_data_pipeline`)
  - `full_data_pipeline` job to run both + gold group
  - `monitoring_job` to generate/publish the Elementary report

5) Verify results in BigQuery/GCS
- Check `business_case_temp` (hashed & stable tables)
- Check Bronze/Silver/Gold datasets
- Check GCS archive/failed paths for landed files

6) View the Elementary report
- Use the public URL from the asset metadata (or browse the bucket object path as described above).


## Operational considerations

- Watermarks & reprocessing: CDC state is tied to Dagster materializations. To replay a period, reset the checkpoint asset materialization (or override query filters temporarily).
- Idempotency: Bronze incremental merges ensure repeated loads with the same deltas won’t duplicate rows.
- Observability: Each asset emits metadata such as record counts, max timestamps, table references, and report URLs.


## Next steps / improvements

- Environment separation
  - Split Terraform state and resources into dev/stage/prod; parameterize bucket/dataset names and IAM per env.
  - Use separate Dagster deployments (or workspaces) and dbt targets per environment.
- CI/CD
  - GitHub Actions for: Terraform plan/apply (with approvals), dbt build + tests, Dagster code checks and packaging.
- Secrets management
  - Replace local env vars with Secret Manager / Vault; avoid embedding credentials in profiles files.
- Alerts & notifications
  - Add Dagster sensors to push Slack/Email/PubSub alerts on failure or data quality regressions.
- Backfill strategy
  - Implement backfill ingestion from the archive bucket by listing archived blobs for a date range and replaying loads into temp + Bronze.
- Automated testing
  - Add Dagster unit tests for assets/utils; include dbt unit tests or macros tests; introduce integration tests that spin up ephemeral Postgres and mock GCS.
- Scalability of ingestion
  - **This repo uses Dagster + Python extraction because it’s local and cost-efficient for relatively small datasets.**
  - For higher volumes or multiple sources, keep Dagster as the orchestrator and plug in a managed/elastic ingestion tier:
    - Airbyte (managed/source connectors) for CDC into GCS/BQ
    - Dataflow or Datastream for streaming or large-scale CDC pipelines
    - Choose per business requirement and expected volumetry
- Cost and performance
  - Consider clustering/partitioning strategies in BigQuery models; refine incremental filters; add table expiration where applicable.


## Infos!

- Why use both hashed and stable temp tables?
  - Hashed tables provide isolation and traceability for each run. The stable table gives a consistent reference for dbt sources and ad hoc validation.
- Where are dbt tests?
  - See `dbt/business_case/models/bronze/schema.yml`. Extend to silver/gold as you harden the models.

## Disclaimer

This project prioritizes local development speed and clarity. It is not productionized by default (e.g., secrets, retries, SLAs). Use the “Next steps” section to guide a production rollout.
