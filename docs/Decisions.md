## ADR-001: Azure ADLS Gen2 as object storage
**Decision:** Use Azure ADLS Gen2 (student account) instead of MinIO  
**Alternatives considered:** MinIO (Docker), local filesystem  
**Reason:** ADLS Gen2 is production Azure infrastructure — the same 
service used at TELUS and Moneris. Using it directly means the ingestion 
code running locally is identical to what runs in production.  
**Consequence:** Requires Azure student account. Small cost risk if 
AUTO_SUSPEND and lifecycle policies are not configured correctly.



## ADR-002: Three-zone data lake architecture
**Decision:** raw-zone → clean-zone → curated-zone  
**Alternatives considered:** Single bucket with folder prefixes  
**Reason:** Each zone has a different retention policy, access pattern, 
and data contract. Separating them makes it impossible to accidentally 
query unvalidated data from downstream models.  
**Consequence:** More buckets to manage, but enforces data quality by design.

## ADR-003: Snowflake as the analytical warehouse
**Decision:** Snowflake over PostgreSQL for the warehouse layer  
**Alternatives considered:** PostgreSQL, DuckDB  
**Reason:** Snowflake's separation of storage and compute means warehouse 
costs scale to zero when idle (AUTO_SUSPEND). It also supports 
semi-structured data natively via VARIANT columns and integrates 
directly with dbt and Power BI without drivers.  
**Consequence:** Free trial credits are finite — all warehouses must 
have AUTO_SUSPEND=60 to protect credits during development.