import dlt
from pyspark.sql import functions as F
from datetime import datetime

STORAGE_ACCOUNT = "jobspipelinesubhash"
RAW_BASE = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# ── KAGGLE POSTINGS ──────────────────────────────────────────

@dlt.table(
    name="bronze_kaggle_postings",
    comment="Raw LinkedIn job postings from Kaggle — 1.8M records — bronze layer",
    table_properties={"quality": "bronze", "source": "kaggle"}
)
@dlt.expect("valid_title", "title IS NOT NULL")
@dlt.expect("valid_company", "company_name IS NOT NULL")
def bronze_kaggle_postings():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "false")
        .option("escape", '"')
        .option("quote", '"')
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .option("mode", "PERMISSIVE") 
        .csv(f"{RAW_BASE}/kaggle/2026/05/31/postings.csv")
        .select(
            F.col("job_id"),
            F.col("company_name"),
            F.col("title"),
            F.col("description"),
            F.col("location"),
            F.col("min_salary").cast("double").alias("salary_min"),
            F.col("med_salary").cast("double").alias("salary_med"),
            F.col("max_salary").cast("double").alias("salary_max"),
            F.col("pay_period"),
            F.col("currency"),
            F.col("formatted_work_type").alias("work_type"),
            F.col("formatted_experience_level").alias("experience_level"),
            F.col("remote_allowed"),
            F.col("job_posting_url").alias("job_url"),
            F.col("listed_time"),
            F.col("original_listed_time"),
            F.col("skills_desc"),
            F.col("compensation_type"),
            F.col("normalized_salary")
        )
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_system", F.lit("kaggle"))
        .withColumn("_pipeline_run_id", F.lit(
            datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        ))
        .withColumn("_ingestion_date", F.current_date())
    )


# ── ADZUNA JOBS ───────────────────────────────────────────────

@dlt.table(
    name="bronze_adzuna_jobs",
    comment="Raw Adzuna API Canadian job postings — bronze layer",
    table_properties={"quality": "bronze", "source": "adzuna"}
)
@dlt.expect("valid_title", "title IS NOT NULL")
@dlt.expect("valid_company", "company_name IS NOT NULL")
def bronze_adzuna_jobs():
    return (
        spark.read
        .option("multiLine", "true")
        .json(f"{RAW_BASE}/adzuna/2026/05/31/")
        .select(F.explode("results").alias("job"))
        .select(
            F.col("job.id").alias("job_id"),
            F.col("job.title").alias("title"),
            F.col("job.company.display_name").alias("company_name"),
            F.col("job.location.display_name").alias("location"),
            F.col("job.description").alias("description"),
            F.col("job.salary_min").alias("salary_min"),
            F.col("job.salary_max").alias("salary_max"),
            F.col("job.contract_time").alias("contract_time"),
            F.col("job.contract_type").alias("contract_type"),
            F.col("job.created").alias("posted_date"),
            F.col("job.redirect_url").alias("job_url"),
            F.col("job.category.label").alias("category"),
            F.col("job.category.tag").alias("category_tag"),
            F.col("job.salary_is_predicted").alias("salary_is_predicted"),
            F.col("job.latitude").alias("latitude"),
            F.col("job.longitude").alias("longitude")
        )
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_system", F.lit("adzuna"))
        .withColumn("_pipeline_run_id", F.lit(
            datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        ))
        .withColumn("_ingestion_date", F.current_date())
    )


# ── LINKEDIN JOBS ─────────────────────────────────────────────

@dlt.table(
    name="bronze_linkedin_jobs",
    comment="Raw LinkedIn job postings via Apify scraper — bronze layer",
    table_properties={"quality": "bronze", "source": "linkedin"}
)
@dlt.expect("valid_title", "title IS NOT NULL")
@dlt.expect("valid_company", "company_name IS NOT NULL")
def bronze_linkedin_jobs():
    return (
        spark.read
        .option("multiLine", "true")
        .json(f"{RAW_BASE}/linkedin/2026/05/31/")
        .select(F.explode("results").alias("job"))
        .select(
            F.col("job.id").alias("job_id"),
            F.col("job.title").alias("title"),
            F.col("job.companyName").alias("company_name"),
            F.col("job.location").alias("location"),
            F.col("job.description").alias("description"),
            F.col("job.salary").alias("salary_raw"),
            F.col("job.contractType").alias("contract_type"),
            F.col("job.experienceLevel").alias("experience_level"),
            F.col("job.workType").alias("work_type"),
            F.col("job.sector").alias("sector"),
            F.col("job.postedDate").alias("posted_date"),
            F.col("job.postedTimeAgo").alias("posted_time_ago"),
            F.col("job.url").alias("job_url"),
            F.col("job.applyUrl").alias("apply_url"),
            F.col("job.applicationsCount").alias("applications_count"),
            F.col("job.recruiterName").alias("recruiter_name")
        )
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_system", F.lit("linkedin"))
        .withColumn("_pipeline_run_id", F.lit(
            datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        ))
        .withColumn("_ingestion_date", F.current_date())
    )