import os
import csv
import argparse
from typing import Optional
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count, rand


def build_spark(master_url: Optional[str]) -> SparkSession:
    """
    Build a SparkSession configured for local or cluster execution.
    """
    builder = (
        SparkSession.builder
        .appName("Problem1-LogLevelDistribution")
        .config("spark.sql.shuffle.partitions", "200")
        # Required to read from S3
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    )

    if master_url:
        builder = builder.master(master_url)
        builder = (
            builder.config(
                "spark.executorEnv.PYSPARK_PYTHON",
                "/home/ubuntu/spark-cluster/.venv/bin/python",
            )
            .config(
                "spark.executorEnv.PYSPARK_DRIVER_PYTHON",
                "/home/ubuntu/spark-cluster/.venv/bin/python",
            )
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "2g")
        )
    else:
        builder = builder.master("local[*]")

    return builder.getOrCreate()


def main():
    parser = argparse.ArgumentParser(description="Problem 1: Log Level Distribution")
    parser.add_argument(
        "master",
        nargs="?",
        default=None,
        help="Spark master URL (e.g., spark://10.0.0.5:7077). Leave empty for local mode.",
    )
    parser.add_argument("--net-id", required=False, help="Your NET ID (optional).")
    parser.add_argument(
        "--input",
        default=None,
        help="Input path (e.g., s3a://bucket/data or local folder).",
    )
    parser.add_argument(
        "--outdir",
        default="data/output",
        help="Output directory (default: data/output).",
    )
    parser.add_argument(
        "--sample-seed", type=int, default=42, help="Random seed for sampling."
    )
    args = parser.parse_args()

    #  Resolve input path 
    s3_default = "s3a://mt1584-assignment-spark-cluster-logs/data"
    if args.input:
        input_path = args.input.rstrip("/")
    else:
        input_path = s3_default

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    #  Build Spark Session 
    master_url = args.master or os.environ.get("SPARK_MASTER_URL")
    spark = build_spark(master_url)
    sc = spark.sparkContext
    print(f"[INFO] Using master: {sc.master}")
    print(f"[INFO] Reading from: {input_path}")
    print(f"[INFO] Writing outputs to: {outdir.resolve()}")

    #  Read text files recursively 
    df = spark.read.option("recursiveFileLookup", "true").text(input_path)

    #  Extract log levels and timestamps 
    parsed = df.select(
        regexp_extract(col("value"), r"(INFO|WARN|ERROR|DEBUG)", 1).alias("log_level"),
        regexp_extract(col("value"), r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1).alias("timestamp"),
        col("value").alias("log_entry"),
    )

    total_lines = df.count()
    parsed_non_empty = parsed.filter(col("log_level") != "")
    parsed_non_empty.cache()
    valid_lines = parsed_non_empty.count()
    unique_levels = parsed_non_empty.select("log_level").distinct().count()

    #  Counts 
    desired_order = ["INFO", "WARN", "ERROR", "DEBUG"]
    counts_df = parsed_non_empty.groupBy("log_level").agg(count("*").alias("count"))
    counts_map = {r["log_level"]: int(r["count"]) for r in counts_df.collect()}

    counts_csv = outdir / "problem1_counts.csv"
    with counts_csv.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["log_level", "count"])
        for lvl in desired_order:
            w.writerow([lvl, counts_map.get(lvl, 0)])

    #  Sample 
    ts_df = parsed_non_empty.filter(col("timestamp") != "")
    has_ts = ts_df.limit(1).count() > 0
    sample_source = ts_df if has_ts else parsed_non_empty
    sample_rows = (
        sample_source.orderBy(rand(args.sample_seed)).limit(10).select("log_entry", "log_level").collect()
    )
    sample_csv = outdir / "problem1_sample.csv"
    with sample_csv.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["log_entry", "log_level"])
        for r in sample_rows:
            w.writerow([r["log_entry"], r["log_level"]])

    #  Summary 
    summary_txt = outdir / "problem1_summary.txt"
    with summary_txt.open("w") as f:
        f.write(f"Total log lines processed: {total_lines}\n")
        f.write(f"Total lines with log levels: {valid_lines}\n")
        f.write(f"Unique log levels found: {unique_levels}\n\n")
        f.write("Log level distribution:\n")
        for lvl in desired_order:
            cnt = counts_map.get(lvl, 0)
            pct = (cnt / valid_lines * 100.0) if valid_lines else 0.0
            f.write(f"  {lvl:<5}: {cnt:>10,} ({pct:5.2f}%)\n")

    print("[SUCCESS] Wrote:")
    print(f"  {counts_csv}")
    print(f"  {sample_csv}")
    print(f"  {summary_txt}")

    parsed_non_empty.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
