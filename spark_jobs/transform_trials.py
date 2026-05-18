"""
Clinical Trial Data Lake — PySpark Transformation Job
======================================================
Reads validated CSV → cleans & transforms → writes Parquet output

Transformations:
  1. Cast column types correctly
  2. Handle null outcome_score with median imputation
  3. Normalize dosage into low / medium / high buckets
  4. Aggregate stats per drug per trial
  5. Write final Parquet to output folder (S3 in production)
"""

import os
import sys

# ── Try to import PySpark; show helpful message if not installed ──────────────
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, FloatType, IntegerType, DateType
    )
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PATH = os.path.join(BASE_DIR, "data", "validated_trials.csv")
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "output_parquet")
AGGREGATED_PATH = os.path.join(BASE_DIR, "data", "aggregated_stats")


def run_spark_job():
    """Main PySpark transformation pipeline."""

    print("[SPARK] Starting Clinical Trial Transformation Job...")

    # ── 1. Create Spark session ───────────────────────────────────────────────
    spark = (
        SparkSession.builder
        .appName("ClinicalTrialDataLake")
        .master("local[*]")             # local mode for dev; use yarn/k8s in prod
        .config("spark.sql.shuffle.partitions", "4")  # small dataset in dev
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("[SPARK] Session created.")

    # ── 2. Define schema explicitly (better than inferSchema for prod) ────────
    schema = StructType([
        StructField("patient_id",       StringType(),  False),
        StructField("trial_id",         StringType(),  True),
        StructField("drug",             StringType(),  True),
        StructField("site",             StringType(),  True),
        StructField("dosage_mg",        FloatType(),   True),
        StructField("age",              IntegerType(), True),
        StructField("weight_kg",        FloatType(),   True),
        StructField("enrollment_date",  StringType(),  True),
        StructField("status",           StringType(),  True),
        StructField("outcome_score",    FloatType(),   True),
    ])

    # ── 3. Read validated CSV ─────────────────────────────────────────────────
    df = (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(INPUT_PATH)
    )
    print(f"[SPARK] Loaded {df.count()} records from {INPUT_PATH}")
    df.printSchema()

    # ── 4. Clean: drop rows where patient_id is null ──────────────────────────
    df = df.dropna(subset=["patient_id"])

    # ── 5. Impute null outcome_score with median ──────────────────────────────
    median_score = df.approxQuantile("outcome_score", [0.5], 0.01)[0]
    df = df.fillna({"outcome_score": round(median_score, 2)})
    print(f"[SPARK] Imputed null outcome_scores with median: {median_score:.2f}")

    # ── 6. Add dosage bucket column ───────────────────────────────────────────
    df = df.withColumn(
        "dosage_bucket",
        F.when(F.col("dosage_mg") < 100, "low")
         .when(F.col("dosage_mg") < 300, "medium")
         .otherwise("high")
    )

    # ── 7. Add BMI approximation column (weight / height proxy) ──────────────
    # Using age as a proxy grouping since height isn't in dataset
    df = df.withColumn(
        "weight_group",
        F.when(F.col("weight_kg") < 60, "underweight")
         .when(F.col("weight_kg") < 85, "normal")
         .otherwise("overweight")
    )

    # ── 8. Add ingestion timestamp ────────────────────────────────────────────
    df = df.withColumn("processed_at", F.current_timestamp())

    print("[SPARK] Transformation columns added.")
    df.show(5, truncate=False)

    # ── 9. Write transformed data as Parquet (partitioned by drug) ────────────
    (
        df.write
        .mode("overwrite")
        .partitionBy("drug")            # partitioning = faster queries in S3
        .parquet(OUTPUT_PATH)
    )
    print(f"[SPARK] ✅ Parquet output written to: {OUTPUT_PATH}")

    # ── 10. Aggregation: stats per drug per trial ─────────────────────────────
    agg_df = (
        df.groupBy("trial_id", "drug")
        .agg(
            F.count("patient_id").alias("patient_count"),
            F.avg("dosage_mg").alias("avg_dosage_mg"),
            F.avg("outcome_score").alias("avg_outcome_score"),
            F.avg("age").alias("avg_age"),
            F.countDistinct("site").alias("site_count"),
            F.sum(F.when(F.col("status") == "adverse_event", 1).otherwise(0))
             .alias("adverse_events"),
        )
        .orderBy("trial_id", "drug")
    )

    print("[SPARK] Aggregated stats per drug per trial:")
    agg_df.show(truncate=False)

    agg_df.write.mode("overwrite").parquet(AGGREGATED_PATH)
    print(f"[SPARK] ✅ Aggregated stats written to: {AGGREGATED_PATH}")

    spark.stop()
    print("[SPARK] 🎉 Job complete.")


# ── Fallback: run without Spark using pandas (for local dev without Java) ─────
def run_pandas_fallback():
    """
    Runs the same transformations using pandas.
    Use this if PySpark / Java is not installed yet.
    The logic is identical — just swap pandas for Spark DataFrames.
    """
    import pandas as pd
    import numpy as np

    print("[PANDAS FALLBACK] PySpark not found — running with pandas instead.")
    print("[PANDAS FALLBACK] Install PySpark: pip install pyspark")

    if not os.path.exists(INPUT_PATH):
        print(f"[PANDAS FALLBACK] Input not found: {INPUT_PATH}")
        print("Run data/generate_sample_data.py first.")
        return

    df = pd.read_csv(INPUT_PATH)
    print(f"[PANDAS FALLBACK] Loaded {len(df)} records.")

    # Impute nulls
    median_score = df["outcome_score"].median()
    df["outcome_score"] = df["outcome_score"].fillna(median_score)

    # Dosage bucket
    df["dosage_bucket"] = pd.cut(
        df["dosage_mg"],
        bins=[0, 100, 300, float("inf")],
        labels=["low", "medium", "high"]
    )

    # Weight group
    df["weight_group"] = pd.cut(
        df["weight_kg"],
        bins=[0, 60, 85, float("inf")],
        labels=["underweight", "normal", "overweight"]
    )

    df["processed_at"] = pd.Timestamp.now()

    os.makedirs(OUTPUT_PATH, exist_ok=True)
    out_file = os.path.join(OUTPUT_PATH, "transformed_trials.csv")
    df.to_csv(out_file, index=False)
    print(f"[PANDAS FALLBACK] ✅ Output written to: {out_file}")

    # Aggregation
    agg = (
        df.groupby(["trial_id", "drug"])
        .agg(
            patient_count=("patient_id", "count"),
            avg_dosage_mg=("dosage_mg", "mean"),
            avg_outcome_score=("outcome_score", "mean"),
            avg_age=("age", "mean"),
            site_count=("site", "nunique"),
            adverse_events=("status", lambda x: (x == "adverse_event").sum()),
        )
        .reset_index()
    )
    print("\n[PANDAS FALLBACK] Aggregated stats:")
    print(agg.to_string(index=False))

    os.makedirs(AGGREGATED_PATH, exist_ok=True)
    agg_file = os.path.join(AGGREGATED_PATH, "aggregated_stats.csv")
    agg.to_csv(agg_file, index=False)
    print(f"[PANDAS FALLBACK] ✅ Aggregated stats written to: {agg_file}")
    print("[PANDAS FALLBACK] 🎉 Job complete.")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if SPARK_AVAILABLE:
        run_spark_job()
    else:
        run_pandas_fallback()
