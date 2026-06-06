import dlt
from pyspark.sql import functions as F

STORAGE_ACCOUNT = "jobspipelinesubhash"
RAW_BASE = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# ── TOP SKILLS ────────────────────────────────────────────────

@dlt.table(
    name="gold_top_skills",
    comment="Top in-demand skills by job count — feeds dashboard",
    table_properties={"quality": "gold"},
    path="abfss://gold@jobspipelinesubhash.dfs.core.windows.net/gold_top_skills"
)
def gold_top_skills():
    df_job_skills = (
        spark.read
        .option("header", "true")
        .csv(f"{RAW_BASE}/kaggle/2026/05/31/jobs/job_skills.csv")
    )

    df_skills_map = (
        spark.read
        .option("header", "true")
        .csv(f"{RAW_BASE}/kaggle/2026/05/31/mappings/skills.csv")
    )

    df_silver = spark.table(
        "jobs_pipeline_databricks.silver.silver_jobs_unified"
    )

    return (
        df_job_skills
        .join(df_skills_map, on="skill_abr", how="left")
        .join(df_silver, on="job_id", how="inner")
        .groupBy("skill_abr", "skill_name")
        .agg(
            F.count("*").alias("job_count"),
            F.countDistinct("company_name").alias("company_count")
        )
        .orderBy(F.col("job_count").desc())
        .withColumn("_ingestion_date", F.current_date())
    )


# ── TOP HIRING COMPANIES ──────────────────────────────────────

@dlt.table(
    name="gold_top_companies",
    comment="Top hiring companies by job count — feeds dashboard",
    table_properties={"quality": "gold"},
    path="abfss://gold@jobspipelinesubhash.dfs.core.windows.net/gold_top_companies"
)
def gold_top_companies():
    return (
        spark.table("jobs_pipeline_databricks.silver.silver_jobs_unified")
        .groupBy("company_name", "source_system")
        .agg(
            F.count("*").alias("job_count"),
            F.countDistinct("location").alias("location_count"),
            F.sum(F.when(F.col("remote_flag") == True, 1).otherwise(0))
             .alias("remote_jobs")
        )
        .filter(F.col("job_count") >= 2)
        .orderBy(F.col("job_count").desc())
        .withColumn("_ingestion_date", F.current_date())
    )


# ── SALARY BY ROLE ────────────────────────────────────────────

@dlt.table(
    name="gold_salary_by_role",
    comment="Salary ranges by job title — feeds dashboard",
    table_properties={"quality": "gold"},
     path="abfss://gold@jobspipelinesubhash.dfs.core.windows.net/gold_salary_by_role"
    
)
def gold_salary_by_role():
    return (
        spark.table("jobs_pipeline_databricks.silver.silver_jobs_unified")
        .filter(F.col("salary_min").isNotNull())
        .filter(F.col("salary_max").isNotNull())
        .groupBy("title")
        .agg(
            F.count("*").alias("job_count"),
            F.avg("salary_min").alias("avg_salary_min"),
            F.avg("salary_max").alias("avg_salary_max"),
            F.min("salary_min").alias("min_salary"),
            F.max("salary_max").alias("max_salary"),
            F.percentile_approx("salary_min", 0.5).alias("median_salary_min"),
            F.percentile_approx("salary_max", 0.5).alias("median_salary_max")
        )
        .filter(F.col("job_count") >= 3)
        .orderBy(F.col("avg_salary_max").desc())
        .withColumn("_ingestion_date", F.current_date())
    )


# ── REMOTE VS ONSITE RATIO ────────────────────────────────────

@dlt.table(
    name="gold_remote_ratio",
    comment="Remote vs onsite ratio by source and experience level",
    table_properties={"quality": "gold"},
    path="abfss://gold@jobspipelinesubhash.dfs.core.windows.net/gold_remote_ratio"
)
def gold_remote_ratio():
    return (
        spark.table("jobs_pipeline_databricks.silver.silver_jobs_unified")
        .groupBy("source_system", "experience_level")
        .agg(
            F.count("*").alias("total_jobs"),
            F.sum(F.when(F.col("remote_flag") == True, 1).otherwise(0))
             .alias("remote_jobs"),
            F.sum(F.when(F.col("remote_flag") == False, 1).otherwise(0))
             .alias("onsite_jobs")
        )
        .withColumn("remote_pct",
            F.round(
                F.col("remote_jobs") / F.col("total_jobs") * 100, 2
            )
        )
        .orderBy(F.col("remote_pct").desc())
        .withColumn("_ingestion_date", F.current_date())
    )


# ── DAILY JOB TREND ───────────────────────────────────────────

@dlt.table(
    name="gold_daily_trend",
    comment="Daily job posting trend by source",
    table_properties={"quality": "gold"},
    path="abfss://gold@jobspipelinesubhash.dfs.core.windows.net/gold_daily_trend"
)
def gold_daily_trend():
    return (
        spark.table("jobs_pipeline_databricks.silver.silver_jobs_unified")
        .filter(F.col("posted_date").isNotNull())
        .groupBy("posted_date", "source_system")
        .agg(
            F.count("*").alias("jobs_posted"),
            F.countDistinct("company_name").alias("companies_hiring")
        )
        .orderBy(F.col("posted_date").desc())
        .withColumn("_ingestion_date", F.current_date())
    )