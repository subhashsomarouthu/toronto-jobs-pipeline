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