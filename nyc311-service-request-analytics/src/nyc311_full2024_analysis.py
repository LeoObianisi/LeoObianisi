from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("NYC311_Full2024_Final_Analysis")
    .getOrCreate()
)

RAW = "gs://sound-jigsaw-502710-a1-nyc311/data/full2024/raw_chunks/*.csv"
OUT = "gs://sound-jigsaw-502710-a1-nyc311/outputs/full2024"
PARQUET = "gs://sound-jigsaw-502710-a1-nyc311/data/full2024/processed/clean_parquet"

print("\n======================================")
print("NYC 311 FULL 2024 FINAL ANALYSIS")
print("======================================")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(RAW)
)

raw_count = df.count()

boroughs = [
    "BROOKLYN",
    "QUEENS",
    "BRONX",
    "MANHATTAN",
    "STATEN ISLAND"
]

df_clean = (
    df
    .filter(F.col("borough").isNotNull())
    .filter(F.col("complaint_type").isNotNull())
    .filter(F.col("borough").isin(boroughs))
    .withColumn("created_ts", F.to_timestamp("created_date"))
    .withColumn("closed_ts", F.to_timestamp("closed_date"))
    .withColumn(
        "resolution_hours",
        (
            F.unix_timestamp("closed_date")
            - F.unix_timestamp("created_date")
        ) / 3600.0
    )
    .withColumn("hour_of_day", F.hour("created_date"))
    .withColumn("day_of_week", F.date_format("created_date", "EEEE"))
    .withColumn("month_num", F.month("created_date"))
    .withColumn("month_name", F.date_format("created_date", "MMMM"))
)

clean_count = df_clean.count()

df_resolution = df_clean.filter(
    F.col("resolution_hours").isNotNull()
    & (F.col("resolution_hours") >= 0)
)

resolution_count = df_resolution.count()

print("\n--- DATA SUMMARY ---")
print("Raw rows:", raw_count)
print("Clean rows:", clean_count)
print("Rows removed:", raw_count - clean_count)
print("Valid resolution rows:", resolution_count)
print("Resolution rows excluded:", clean_count - resolution_count)

df_clean.write.mode("overwrite").parquet(PARQUET)
print("Full 2024 Parquet dataset saved.")

df_clean.createOrReplaceTempView("nyc311")
df_resolution.createOrReplaceTempView("nyc311_resolution")

queries = {}

queries["top_complaints"] = spark.sql("""
SELECT complaint_type, COUNT(*) AS complaint_count
FROM nyc311
GROUP BY complaint_type
ORDER BY complaint_count DESC
LIMIT 10
""")

queries["borough_counts"] = spark.sql("""
SELECT borough, COUNT(*) AS complaint_count
FROM nyc311
GROUP BY borough
ORDER BY complaint_count DESC
""")

queries["agency_counts"] = spark.sql("""
SELECT agency, COUNT(*) AS complaint_count
FROM nyc311
GROUP BY agency
ORDER BY complaint_count DESC
LIMIT 10
""")

queries["monthly_counts"] = spark.sql("""
SELECT month_num, month_name, COUNT(*) AS complaint_count
FROM nyc311
GROUP BY month_num, month_name
ORDER BY month_num
""")

queries["day_counts"] = spark.sql("""
SELECT day_of_week, COUNT(*) AS complaint_count
FROM nyc311
GROUP BY day_of_week
ORDER BY complaint_count DESC
""")

queries["hour_counts"] = spark.sql("""
SELECT hour_of_day, COUNT(*) AS complaint_count
FROM nyc311
GROUP BY hour_of_day
ORDER BY complaint_count DESC
""")

queries["borough_resolution"] = spark.sql("""
SELECT
    borough,
    COUNT(*) AS resolved_requests,
    percentile_approx(resolution_hours, 0.5) AS median_resolution_hours
FROM nyc311_resolution
GROUP BY borough
ORDER BY median_resolution_hours
""")

queries["agency_resolution"] = spark.sql("""
SELECT
    agency,
    COUNT(*) AS resolved_requests,
    percentile_approx(resolution_hours, 0.5) AS median_resolution_hours
FROM nyc311_resolution
GROUP BY agency
HAVING COUNT(*) >= 1000
ORDER BY median_resolution_hours DESC
""")

print("\n--- TOP 10 COMPLAINT TYPES ---")
queries["top_complaints"].show(10, False)

print("\n--- COMPLAINTS BY BOROUGH ---")
queries["borough_counts"].show(5, False)

print("\n--- TOP 10 AGENCIES ---")
queries["agency_counts"].show(10, False)

print("\n--- COMPLAINTS BY MONTH ---")
queries["monthly_counts"].show(12, False)

print("\n--- DAYS RANKED BY COMPLAINT VOLUME ---")
queries["day_counts"].show(7, False)

print("\n--- HOURS RANKED BY COMPLAINT VOLUME ---")
queries["hour_counts"].show(24, False)

print("\n--- MEDIAN RESOLUTION BY BOROUGH ---")
queries["borough_resolution"].show(5, False)

print("\n--- MEDIAN RESOLUTION BY AGENCY ---")
queries["agency_resolution"].show(20, False)

for name, result in queries.items():
    (
        result.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(f"{OUT}/{name}")
    )

print("\nAll full-year result tables saved.")
print("======================================")
print("FULL 2024 FINAL ANALYSIS COMPLETED")
print("======================================")

spark.stop()
