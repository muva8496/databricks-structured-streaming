# Databricks Structured Streaming Pipeline (`databricks-streaming-pipeline`)

**Real-Time Data Ingestion, Landing Folder Monitoring, and Delta Lake Sinks in PySpark**

---

## 1. Executive Summary & Architecture

Modern transactional environments require near-instantaneous data ingestion to prevent latency bottlenecks and operational blind spots. Traditional batch processing loads static files on a scheduled interval (e.g., nightly), whereas **Spark Structured Streaming** models continuous incoming data streams as **unbounded tables**.

This repository details a production-grade **Databricks Structured Streaming** pipeline engineered in PySpark. The engine continuously monitors an active cloud landing directory (`/FileStore/vellamo_stream/landing_json`), ingests incoming JSON micro-batches, calculates calculated compensation metrics and tiering logic on the fly, and appends the transformed records into an ACID-compliant **Delta Lake Table Sink** with minimal latency.

```text
[ Incoming Data Drops ] 
         │ (JSON Records)
         ▼
┌─────────────────────────────────────────┐
│ Cloud Landing Folder                    │
│ (/FileStore/.../landing_json)           │
└────────────────────┬────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────┐
│ PySpark Structured Streaming Engine     │
│  - Continuous Directory Monitoring      │
│  - Explicit Schema Enforcement          │
│  - Micro-batch Processing               │
│  - Fault-Tolerant Checkpointing         │
└────────────────────┬────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────┐
│ Delta Lake Table Sink                   │
│ (/FileStore/.../delta_jobs_sink)        │
└─────────────────────────────────────────┘
2. Technical Concepts & GlossaryReal-Time Streaming Pipeline: An architectural flow that ingests, cleans, and delivers individual records or micro-batches the exact moment they land, minimizing processing delay.Latency & Minimal Latency: Latency is the time elapsed between event generation in the source system and its availability in the target analytical storage. Minimal Latency in Spark Structured Streaming refers to processing micro-batches within sub-second to low-second windows (e.g., 3-second triggers).Ingestion: The initial boundary phase where unorganized raw files (JSON/CSV/Avro) are ingested into the Spark engine with strict schema enforcement.Landing Folder: A designated directory in cloud object storage or Databricks DBFS (/FileStore/...) used as an un-processed buffer where upstream applications drop raw payload files.Stream Sink: The destination storage engine (such as a Delta Lake Table) that receives the output of a continuous write stream (writeStream).Checkpointing (checkpointLocation): A fault-tolerance mechanism where Spark maintains offset metadata tracking exactly which files have been processed, enabling exact-once processing semantics during cluster restarts or failures.3. Data Simulation & Pipeline ImplementationPhase A: Stream Simulation (data_jobs_20rows.xlsx)To simulate live ingestion without external webhooks, static rows from data_jobs_20rows.xlsx are iterated over, formatted as individual payload JSON objects, and written to the landing folder using dbutils.fs.put().Phase B: PySpark Streaming Core Engine (databricks_streaming_pipeline.py)Pythonfrom pyspark.sql.functions import col, current_timestamp, expr, when
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# 1. Define Explicit Ingestion Schema
job_schema = StructType([
    StructField("job_title_short", StringType(), True),
    StructField("job_posted_date", StringType(), True),
    StructField("job_location", StringType(), True),
    StructField("job_country", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("job_via", StringType(), True),
    StructField("job_schedule_type", StringType(), True),
    StructField("salary_year_avg", DoubleType(), True),
    StructField("job_work_from_home", StringType(), True),
    StructField("job_skills", StringType(), True),
])

landing_folder = "/FileStore/vellamo_stream/landing_json"
checkpoint_dir = "/FileStore/vellamo_stream/checkpoints_jobs"
delta_sink_path = "/FileStore/vellamo_stream/delta_jobs_sink"

# 2. INGESTION: Monitor Landing Folder
raw_job_stream = (
    spark.readStream.schema(job_schema)
    .option("maxFilesPerTrigger", 2)  # Control micro-batch velocity
    .json(landing_folder)
)

# 3. TRANSFORMATION: On-The-Fly Business Logic
transformed_job_stream = (
    raw_job_stream.withColumn(
        "monthly_est_salary", expr("ROUND(salary_year_avg / 12, 2)")
    )
    .withColumn(
        "compensation_tier",
        when(col("salary_year_avg") >= 120000, "High-Comp Senior")
        .when(col("salary_year_avg") > 0, "Standard")
        .otherwise("Unpaid/Not Listed"),
    )
    .withColumn("stream_processed_at", current_timestamp())
)

# 4. STREAM SINK: Continuous Append to Delta Lake Sink
job_streaming_query = (
    transformed_job_stream.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_dir)
    .trigger(processingTime="3 seconds")
    .start(delta_sink_path)
)
4. Execution Verification & ResultsUpon running the streaming engine against the simulated JSON drops, 20 total streaming records were picked up across micro-batches and materialized into the Delta Lake sink:High-Comp Senior Tier: Captures roles with annual compensation $\ge \$120,000$ (e.g., Stripe Data Scientist at $\$281,450.50$/yr $\rightarrow$ $\$23,454.21$/mo).Standard Tier: Captures valid roles below $\$120,000$ (e.g., Standard Bank Group Data Scientist at $\$70,000.00$/yr $\rightarrow$ $\$5,833.33$/mo).Unpaid / Not Listed Tier: Null compensation values dynamically map to "Unpaid/Not Listed".Telemetry Verification: Every record receives a microsecond-accurate ingestion timestamp (stream_processed_at).5. Repository LayoutPlaintextdatabricks-streaming-pipeline/
├── databricks_streaming_pipeline.py   # Databricks PySpark streaming notebook export
├── data_jobs_simulation.py            # JSON stream drop generator script
├── .gitignore                         # Excludes cache files and workspace artifacts
└── README.md                          # Technical documentation"# databricks-structured-streaming" 
