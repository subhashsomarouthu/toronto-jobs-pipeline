# Architecture Decision Records

## ADR-001: Azure ADLS Gen2 as object storage
**Decision:** Azure ADLS Gen2 (student account) as the data lake storage layer  
**Alternatives considered:** MinIO (Docker), local filesystem, AWS S3  
**Reason:** ADLS Gen2 is production Azure infrastructure — the same service 
used at TELUS and Moneris. Using it directly means ingestion code running 
locally is identical to what runs in production. Hierarchical namespace 
enables folder-level access control and efficient Spark reads.  
**Consequence:** Requires Azure student account. Cost risk if lifecycle 
policies are not configured. Mitigated by LRS replication and no 
redundant storage tiers.

## ADR-002: Four-layer storage architecture (raw/bronze/silver/gold)
**Decision:** Four zones — raw, bronze, silver, gold — not three  
**Alternatives considered:** Three-layer medallion (bronze/silver/gold only), 
single flat container with folder prefixes  
**Reason:** Raw is a separate disaster recovery layer — exact byte-for-byte 
copy of source data with zero transformation applied. Bronze adds ingestion 
metadata and converts to Parquet. Separating raw from bronze means if 
Bronze ingestion has a bug, the original source data is always recoverable 
by replaying from raw. Each layer has a distinct data contract.  
**Consequence:** Extra storage container, negligible cost on LRS pricing.

## ADR-003: Snowflake as the analytical warehouse
**Decision:** Snowflake over PostgreSQL or DuckDB for the warehouse layer  
**Alternatives considered:** PostgreSQL, DuckDB, Azure Synapse Analytics  
**Reason:** Snowflake separates storage and compute — warehouse costs scale 
to zero when idle via AUTO_SUSPEND=60. Supports semi-structured data natively 
via VARIANT columns. Integrates directly with Databricks, dbt, and Power BI 
without additional drivers. Most requested warehouse in Toronto job postings 
alongside Databricks.  
**Consequence:** Free trial credits are finite. All warehouses configured 
with AUTO_SUSPEND=60 to protect credits during development.

## ADR-004: Terraform for infrastructure as code
**Decision:** All Azure infrastructure defined in Terraform, not created manually  
**Alternatives considered:** Azure Portal (manual), Azure CLI scripts, Bicep  
**Reason:** Terraform is cloud-agnostic and the most requested IaC tool in 
Toronto job postings. The entire environment can be destroyed and recreated 
with one command — no tribal knowledge, no configuration drift. Every 
resource is version controlled and peer reviewable.  
**Consequence:** Terraform state must be managed carefully. For this project 
state is local. In production it would live in Azure Blob Storage backend 
with state locking via Azure Cosmos DB.

## ADR-005: terraform.tfstate and terraform.tfvars excluded from version control
**Decision:** Both files gitignored and stored locally only  
**Alternatives considered:** Committing state to repo, encrypting secrets in repo  
**Reason:** tfstate contains plaintext secrets including storage account keys. 
During this project GitHub secret scanning blocked a push because tfstate 
was accidentally committed. The correct fix was to rebase the commit out of 
history, rotate the compromised Azure storage key, and permanently add 
tfstate to gitignore. This is a real production incident and recovery — 
documented here as a learning record.  
**Consequence:** State not shared between developers. Acceptable for solo 
project. In a team environment state would be stored in Azure Blob Storage 
with shared backend and Cosmos DB state locking to prevent concurrent applies.

## ADR-006: Databricks Delta Live Tables for Bronze→Silver→Gold processing
**Decision:** Databricks DLT instead of plain PySpark scripts or dbt Core  
**Alternatives considered:** Plain PySpark scripts, dbt Core, Azure Data Factory  
**Reason:** DLT is declarative — transformations and data quality expectations 
are defined once, DLT manages execution, retries, and lineage automatically. 
Matches production stack used at TELUS and Moneris. Databricks Community 
Edition is free for development. DLT pipelines are observable by default — 
built-in lineage graph, quality metrics, and pipeline monitoring.  
**Consequence:** Locked into Databricks ecosystem for transformation layer. 
Acceptable since Databricks is the dominant processing platform in Toronto 
market and directly maps to resume experience.

## ADR-007: Three data sources — Kaggle, Adzuna, Indeed/Apify
**Decision:** Three sources with different ingestion patterns  
**Alternatives considered:** Single source (Kaggle only), web scraping  

| Source | Pattern | Frequency | Cost |
|---|---|---|---|
| Kaggle LinkedIn dataset | Full bulk load | Once — historical backfill | Free |
| Adzuna API | Incremental batch | Daily | Free |
| Indeed via Apify | Incremental batch | Daily | ~$0.10/1000 listings |

**Reason:** Three sources demonstrate three real ingestion patterns on one 
pipeline. Kaggle provides 124,000 historical records for bulk load testing. 
Adzuna provides daily Canadian job postings via free REST API. Apify scrapes 
Indeed for richer job descriptions and skills data unavailable in Adzuna.  
**Consequence:** Apify has per-listing cost. Circuit breaker monitors credit 
balance before each run — skips Indeed source with alert notification if 
balance drops below $1.00 threshold. Pipeline continues with remaining sources.

## ADR-008: Kafka for simulated real-time streaming layer
**Decision:** Apache Kafka added as streaming demonstration layer  
**Alternatives considered:** No streaming layer, Azure Event Hubs  
**Reason:** Kaggle, Adzuna, and Apify are all batch pull sources — none 
produce true real-time events. Kafka is added to simulate a real-time job 
posting feed for architectural completeness. A Python producer generates 
mock job events every 30 seconds. A consumer writes them to Bronze. This 
demonstrates producer/consumer patterns, topic design, offset management, 
and watermark-based late data handling without additional cost.  
**Consequence:** Simulated data, not real. Clearly documented as architectural 
demonstration. In production this would be replaced by a real event source 
such as a job board webhook or clickstream feed.

## ADR-009: Power BI as the dashboard layer
**Decision:** Power BI Desktop connected to Snowflake for final dashboard  
**Alternatives considered:** Evidence.dev, Metabase, Grafana  
**Reason:** Power BI is already on the resume from TELUS and Moneris 
experience. Connecting Power BI directly to Snowflake via the native 
connector is a production pattern used at most Canadian enterprises. 
Demonstrates DirectQuery vs Import mode trade-offs. Dashboard published 
to Power BI Service for shareable URL (free with student Microsoft account).  
**Consequence:** Power BI Desktop is Windows-only. .pbix file not 
easily version controlled. Dashboard screenshots committed to repo as 
documentation. Live link shared via Power BI Service.

## ADR-010: Parameterized pipeline framework via control tables
**Decision:** Single generic Airflow DAG driven by pipeline_config table  
**Alternatives considered:** One DAG per source, hardcoded pipeline logic  
**Reason:** A parameterized framework means adding a new data source 
requires inserting one row into pipeline_config — no code change. 
pipeline_control table tracks watermarks for incremental loading — 
each source stores its last_successful_run timestamp and last_record_loaded 
value. Circuit breaker logic reads credit_balance from pipeline_config 
before each Apify run.  
**Consequence:** More complex initial setup. Pays off immediately when 
a third or fourth source is added. This is the pattern used at scale 
in production data platforms.

## ADR-011: Databricks Unity Catalog for data governance
**Decision:** Use Unity Catalog instead of legacy Hive metastore  
**Alternatives considered:** Hive metastore, no catalog  
**Reason:** Unity Catalog provides centralized access control, data 
lineage, and auditing across all Databricks workspaces. External 
locations registered once — all notebooks and DLT pipelines access 
ADLS without any credentials in code.  
**Consequence:** Requires Premium tier. Trial provides 14 days free.

## ADR-012: Access Connector managed identity for ADLS authentication
**Decision:** Azure Access Connector managed identity over Service Principal  
**Alternatives considered:** Service Principal OAuth, account key  
**Reason:** Databricks Unity Catalog on Azure only supports managed 
identity for storage credentials — Service Principal option not 
available in the credential type dropdown. Access Connector is the 
recommended production approach by Microsoft and Databricks for 
Azure deployments. Zero credentials in code or notebooks.  
**Consequence:** Requires a separate Access Connector Azure resource. 
Added to jobs-pipeline-rg and managed identity granted 
Storage Blob Data Contributor on ADLS.

## ADR-013: Null handling strategy — keep with placeholder over drop
**Decision:** Replace nulls with standardized placeholders in silver layer
**Alternatives considered:** Drop null records, keep nulls as-is  
**Reason:** Dropping records loses data permanently — downstream models 
may need those records for different analyses. Keeping nulls as-is 
breaks aggregations and joins silently. Replacing with 'UNKNOWN' makes 
the data gap explicit and queryable.  
**Consequence:** 'UNKNOWN' values must be filtered in Gold layer 
aggregations where company name is a dimension.

## ADR-014: Bronze expectations set to ALLOW — enforcement in silver
**Decision:** DLT expectations in bronze layer use ALLOW action — 
records are never dropped in bronze  
**Alternatives considered:** DROP invalid records in bronze, FAIL pipeline  
**Reason:** Bronze is a disaster recovery layer — it must be a complete 
copy of source data. The Kaggle dataset has 57% null titles from malformed 
CSV rows where job descriptions containing newlines cause column shifting. 
Dropping these in bronze would make them unrecoverable. Silver enforces 
quality by filtering to valid records only.  
**Consequence:** Bronze contains 1.84M rows including malformed records. 
Silver will contain ~778K clean records from Kaggle source only.