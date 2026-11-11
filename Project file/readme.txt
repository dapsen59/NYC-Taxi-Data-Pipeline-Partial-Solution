# NYC Taxi Data Pipeline — Partial Solution

This project uses Azure Data Factory, ADLS Gen2, and Azure Databricks to build a Bronze, Silver, and Gold data pipeline.

**Completed so far**

- Configured ADF to download monthly NYC taxi Parquet files into ADLS Gen2.
- Stored taxi trip files and the taxi zone lookup CSV in separate folders.
- Configured Databricks storage access using an Azure Access Connector and Unity Catalog.
- Verified source access, reviewed the schema, and prepared Bronze ingestion code.

**Assumptions**

- Monthly source files retain consistent names and schemas.
- The taxi zone lookup is reference data that changes infrequently.

**Trade-offs**

- Prioritized ingestion and storage setup within the available time; transformations and full validation remain pending.
- Prepared Auto Loader ingestion to support incremental processing, adding checkpoint management compared with a simple batch read.

**Production changes**

- Add automated orchestration, monitoring, alerts, and failure recovery.
- Enforce data quality checks, schema handling, and rerun validation.
- Apply least-privilege permissions, CI/CD, and compute cost controls.

**Remaining work**

Execute and validate Bronze ingestion, implement Silver cleaning and zone enrichment, and build Gold reporting tables.

**Cost incurred so far: $1.85.

**Status:** Partial implementation; end-to-end processing is not yet complete.