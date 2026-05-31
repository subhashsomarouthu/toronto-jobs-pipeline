## ADR-001: Azure ADLS Gen2 as object storage
**Decision:** Use Azure ADLS Gen2 (student account) instead of MinIO  
**Alternatives considered:** MinIO (Docker), local filesystem  
**Reason:** ADLS Gen2 is production Azure infrastructure — the same 
service used at TELUS and Moneris. Using it directly means the ingestion 
code running locally is identical to what runs in production.  
**Consequence:** Requires Azure student account. Small cost risk if 
AUTO_SUSPEND and lifecycle policies are not configured correctly.



## ADR-002: Bronze/Silver/Gold medallion architecture
**Decision:** Three-layer medallion architecture over a flat structure  
**Alternatives considered:** Single container with folder prefixes  
**Reason:** Each layer has a different data contract. Bronze is 
append-only raw source data. Silver is validated and deduplicated. 
Gold is aggregated and optimized for BI queries. Separating them 
makes it impossible for downstream models to accidentally read 
unvalidated data.  
**Consequence:** Three containers to manage instead of one.

## ADR-003: Snowflake as the analytical warehouse
**Decision:** Snowflake over PostgreSQL for the warehouse layer  
**Alternatives considered:** PostgreSQL, DuckDB  
**Reason:** Snowflake's separation of storage and compute means warehouse 
costs scale to zero when idle (AUTO_SUSPEND). It also supports 
semi-structured data natively via VARIANT columns and integrates 
directly with dbt and Power BI without drivers.  
**Consequence:** Free trial credits are finite — all warehouses must 
have AUTO_SUSPEND=60 to protect credits during development.

## ADR-004: Terraform for infrastructure as code
**Decision:** All Azure infrastructure defined in Terraform, not created manually  
**Alternatives considered:** Azure Portal (manual clicking), Azure CLI scripts, Bicep  
**Reason:** Terraform is cloud-agnostic and the most requested IaC tool 
in Toronto job postings. The entire environment can be destroyed and 
recreated with one command — no tribal knowledge, no configuration drift.  
**Consequence:** Terraform state must be managed carefully. For this 
project state is local. In production it would live in Azure Blob 
Storage with state locking via Azure Cosmos DB.

## ADR-005: terraform.tfstate and terraform.tfvars excluded from version control
**Decision:** Both files are gitignored and stored locally only  
**Alternatives considered:** Committing state to repo, encrypting secrets in repo  
**Reason:** tfstate contains plaintext secrets including storage account 
keys. During this project GitHub secret scanning blocked a push because 
tfstate was accidentally committed — the correct fix was to rebase the 
commit out of history, rotate the Azure storage key, and add tfstate 
to gitignore permanently. tfvars contains environment-specific values 
that differ per developer.  
**Consequence:** State is not shared between developers. Acceptable for 
a solo project. In a team environment, state would be stored in Azure 
Blob Storage with a shared backend configuration and Cosmos DB state 
locking to prevent concurrent applies.