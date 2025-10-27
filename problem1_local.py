import re
import os
import shutil
from glob import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, count, rand
import pandas as pd

def write_single_csv(df, output_dir, filename):
    """
    Write a Spark DataFrame as a single CSV file.
    """
    tmp_dir = os.path.join(output_dir, f"_tmp_{filename}")
    df.coalesce(1).write.csv(tmp_dir, header=True, mode="overwrite")

    # Find the part file Spark created
    part_file = glob(os.path.join(tmp_dir, "part-*.csv"))[0]
    final_path = os.path.join(output_dir, filename)

    # Move and rename it to the desired output file
    shutil.move(part_file, final_path)

    # Clean up the temporary directory
    shutil.rmtree(tmp_dir)

def main():
    # Spark Session Setup
    spark = SparkSession.builder \
        .appName("Problem1_LogLevelDistribution_Local") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Paths
    input_path = "data/sample/"
    output_dir = "data/output/"
    os.makedirs(output_dir, exist_ok=True)

    # Load Data
    # Read all text files recursively
    df = spark.read.text(f"{input_path}/**", recursiveFileLookup=True)
    total_lines = df.count()


    # Extract Log Level
    # Typical log format: "17/03/29 10:04:41 INFO ApplicationMaster: ..."
    # We'll capture INFO, WARN, ERROR, or DEBUG
    log_pattern = r"\b(INFO|WARN|ERROR|DEBUG)\b"
    df_with_levels = df.withColumn("log_level", regexp_extract(col("value"), log_pattern, 1))

    # Keep only lines with a recognized log level
    df_filtered = df_with_levels.filter(col("log_level") != "")
    total_with_levels = df_filtered.count()


    # Count Distribution
    counts_df = df_filtered.groupBy("log_level").agg(count("*").alias("count")).orderBy("count", ascending=False)
    write_single_csv(counts_df, output_dir, "local_problem1_counts.csv")

    # Sample Entries
    sample_df = df_filtered.orderBy(rand()).limit(10).select(
        col("value").alias("log_entry"), "log_level"
    )
    write_single_csv(sample_df, output_dir, "local_problem1_sample.csv")


    # Summary Statistics
    counts_pd = counts_df.toPandas()
    summary_lines = [
        f"Total log lines processed: {total_lines:,}",
        f"Total lines with log levels: {total_with_levels:,}",
        f"Unique log levels found: {counts_pd.shape[0]}",
        "",
        "Log level distribution:",
    ]

    for _, row in counts_pd.iterrows():
        pct = (row['count'] / total_with_levels) * 100 if total_with_levels else 0
        summary_lines.append(f"  {row['log_level']:<6}: {row['count']:>10,} ({pct:6.2f}%)")

    with open(os.path.join(output_dir, "local_problem1_summary.txt"), "w") as f:
        f.write("\n".join(summary_lines))

    print(f"\nCSV outputs written to {output_dir}")
    print(" - local_problem1_counts.csv")
    print(" - local_problem1_sample.csv")
    print(" - local_problem1_summary.txt")

    spark.stop()

if __name__ == "__main__":
    main()
