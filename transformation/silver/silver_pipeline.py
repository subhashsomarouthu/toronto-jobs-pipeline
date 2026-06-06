import dlt
from pyspark.sql import functions as F

# ── KAGGLE SILVER ─────────────────────────────────────────────

@dlt.table(
    name="silver_kaggle_jobs",
    comment="Cleaned Kaggle job postings — valid rows only — silver layer",
    table_properties={"quality": "silver", "source": "kaggle"}
)
@dlt.expect_or_drop("valid_job_id", "job_id RLIKE '^[0-9]+$'")
@dlt.expect_or_drop("valid_title_length", "length(title) < 100")
def silver_kaggle_jobs():
    return (
        spark.table("jobs_pipeline_databricks.bronze.bronze_kaggle_postings")
        .filter(F.col("title").isNotNull())
        .filter(F.col("company_name").isNotNull())
        .filter(F.col("job_id").rlike("^[0-9]+$"))
        .filter(F.length(F.col("title")) < 100)
        .filter(
            F.col("work_type").isNull() |
            ~F.col("work_type").rlike("^[0-9]+$")
        )
        .withColumn("title", F.trim(F.upper(F.col("title"))))
        .withColumn("company_name", F.trim(F.col("company_name")))
        .withColumn("location",
            F.when(F.col("location").isNull(), F.lit("UNKNOWN"))
            .otherwise(F.trim(F.col("location")))
        )
        .withColumn("remote_flag",
            F.when(F.col("remote_allowed") == "1.0", True)
            .otherwise(False)
        )
        .withColumn("work_type",
            F.when(F.col("work_type").isNull(), F.lit("UNKNOWN"))
            .otherwise(F.col("work_type"))
        )
        .withColumn("experience_level",
            F.when(F.col("experience_level").isNull(), F.lit("UNKNOWN"))
            .otherwise(F.col("experience_level"))
        )
        .select(
            F.col("job_id"),
            F.col("title"),
            F.col("company_name"),
            F.col("location"),
            F.lit(None).cast("double").alias("salary_min"),
            F.lit(None).cast("double").alias("salary_max"),
            F.col("work_type"),
            F.col("experience_level"),
            F.col("remote_flag"),
            F.col("job_url"),
            F.lit(None).cast("string").alias("posted_date"),
            F.lit("kaggle").alias("source_system"),
            F.col("_ingestion_date")
        )
    )


# ── ADZUNA SILVER ─────────────────────────────────────────────

@dlt.table(
    name="silver_adzuna_jobs",
    comment="Cleaned Adzuna Canadian job postings — silver layer",
    table_properties={"quality": "silver", "source": "adzuna"}
)
@dlt.expect_or_drop("valid_title", "title IS NOT NULL")
@dlt.expect_or_drop("valid_company", "company_name IS NOT NULL")
def silver_adzuna_jobs():
    return (
         spark.table("jobs_pipeline_databricks.bronze.bronze_adzuna_jobs")
        .filter(F.col("title").isNotNull())
        .filter(F.col("company_name").isNotNull())
        .withColumn("title", F.trim(F.upper(F.col("title"))))
        .withColumn("company_name", F.trim(F.col("company_name")))
        .withColumn("location",
            F.when(F.col("location").isNull(), F.lit("UNKNOWN"))
            .otherwise(F.trim(F.col("location")))
        )
        .withColumn("remote_flag",
            F.when(
                F.lower(F.col("location")).contains("remote"), True
            ).otherwise(False)
        )
        .withColumn("work_type",
            F.when(F.col("contract_type").isNull(), F.lit("UNKNOWN"))
            .otherwise(F.col("contract_type"))
        )
        .withColumn("posted_date",
            F.to_date(F.col("posted_date")).cast("string")
        )
        .select(
            F.col("job_id"),
            F.col("title"),
            F.col("company_name"),
            F.col("location"),
            F.col("salary_min").cast("double"),
            F.col("salary_max").cast("double"),
            F.col("work_type"),
            F.lit("UNKNOWN").alias("experience_level"),
            F.col("remote_flag"),
            F.col("job_url"),
            F.col("posted_date"),
            F.lit("adzuna").alias("source_system"),
            F.col("_ingestion_date")
        )
    )


# ── LINKEDIN SILVER ───────────────────────────────────────────

@dlt.table(
    name="silver_linkedin_jobs",
    comment="Cleaned LinkedIn job postings via Apify — silver layer",
    table_properties={"quality": "silver", "source": "linkedin"}
)
@dlt.expect_or_drop("valid_title", "title IS NOT NULL")
@dlt.expect_or_drop("valid_company", "company_name IS NOT NULL")
def silver_linkedin_jobs():
    return (
         spark.table("jobs_pipeline_databricks.bronze.bronze_linkedin_jobs")
        .filter(F.col("title").isNotNull())
        .filter(F.col("company_name").isNotNull())
        .withColumn("title", F.trim(F.upper(F.col("title"))))
        .withColumn("company_name", F.trim(F.col("company_name")))
        .withColumn("location",
            F.when(F.col("location").isNull(), F.lit("UNKNOWN"))
            .otherwise(F.trim(F.col("location")))
        )
        .withColumn("salary_raw",
            F.when(F.col("salary_raw") == "", None)
            .otherwise(F.col("salary_raw"))
        )
        .withColumn("remote_flag",
            F.when(
                F.lower(F.col("location")).contains("remote"), True
            ).otherwise(False)
        )
        .withColumn("work_type",
            F.when(F.col("work_type").isNull(), F.lit("UNKNOWN"))
            .otherwise(F.col("work_type"))
        )
        .withColumn("experience_level",
            F.when(F.col("experience_level").isNull(), F.lit("UNKNOWN"))
            .otherwise(F.col("experience_level"))
        )
        .withColumn("posted_date",
            F.to_date(F.col("posted_date")).cast("string")
        )
        .select(
            F.col("job_id").cast("string"),
            F.col("title"),
            F.col("company_name"),
            F.col("location"),
            F.lit(None).cast("double").alias("salary_min"),
            F.lit(None).cast("double").alias("salary_max"),
            F.col("work_type"),
            F.col("experience_level"),
            F.col("remote_flag"),
            F.col("job_url"),
            F.col("posted_date"),
            F.lit("linkedin").alias("source_system"),
            F.col("_ingestion_date")
        )
    )


# ── UNIFIED SILVER TABLE ──────────────────────────────────────

@dlt.table(
    name="silver_jobs_unified",
    comment="Unified job postings from all sources — 123K clean records — silver layer",
    table_properties={"quality": "silver"}
)
def silver_jobs_unified():
    kaggle  = dlt.read("silver_kaggle_jobs")
    adzuna  = dlt.read("silver_adzuna_jobs")
    linkedin = dlt.read("silver_linkedin_jobs")
    return kaggle.union(adzuna).union(linkedin)