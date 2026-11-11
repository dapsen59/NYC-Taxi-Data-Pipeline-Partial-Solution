# Databricks notebook source
display(dbutils.fs.ls(
    "abfss://raw@ntctaxiadls001.dfs.core.windows.net/nyc-taxi/"
))

display(dbutils.fs.ls(
    "abfss://raw@ntctaxiadls001.dfs.core.windows.net/Taxi_zone_lookup_data/"
))

# COMMAND ----------

base_path = "abfss://raw@ntctaxiadls001.dfs.core.windows.net"

# Read only the Parquet files directly inside nyc-taxi.
trips_df = spark.read.parquet(
    f"{base_path}/nyc-taxi/*.parquet"
)

zones_df = (
    spark.read
    .option("header", "true")
    .csv(f"{base_path}/Taxi_zone_lookup_data/taxi_zone_lookup.csv")
)

display(trips_df.limit(10))
display(zones_df.limit(10))

trips_df.printSchema()
zones_df.printSchema()

# COMMAND ----------

display(dbutils.fs.ls(
    "/Volumes/nyc_taxi_v2/ops/pipeline_state/"
))