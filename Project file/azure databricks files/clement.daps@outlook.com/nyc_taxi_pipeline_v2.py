# Databricks notebook source
# DBTITLE 1,Pipeline configuration
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from functools import reduce
import uuid

CATALOG = "nyc_taxi_v2"

STORAGE_ACCOUNT = "ntctaxiadls001"
CONTAINER = "raw"
ZONE_FILENAME = "taxi_zone_lookup.csv"

ROOT = (
    f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}"
    ".dfs.core.windows.net"
)

TRIP_PATH = f"{ROOT}/nyc-taxi/"
ZONE_FILE = (
    f"{ROOT}/Taxi_zone_lookup_data/{ZONE_FILENAME}"
)

STATE = f"/Volumes/{CATALOG}/ops/pipeline_state"

BT = f"{CATALOG}.bronze.yellow_trips"
BZ = f"{CATALOG}.bronze.taxi_zones"

ST = f"{CATALOG}.silver.yellow_trips"
SZ = f"{CATALOG}.silver.taxi_zones"
QT = f"{CATALOG}.silver.trip_quarantine"
DQ = f"{CATALOG}.silver.dq_results"

GD = f"{CATALOG}.gold.daily_zone_revenue"
GH = f"{CATALOG}.gold.hourly_demand"
GQ = f"{CATALOG}.gold.data_quality_summary"

WM = f"{CATALOG}.ops.watermarks"

dbutils.widgets.text("pipeline_run_id", "")
RUN_ID = (
    dbutils.widgets.get("pipeline_run_id")
    or str(uuid.uuid4())
)

# Controls rendering of ingestion timestamps.
# Trip timestamps remain TIMESTAMP_NTZ.
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# DBTITLE 1,List landing folders
display(dbutils.fs.ls(TRIP_PATH))

display(
    dbutils.fs.ls(f"{ROOT}/Taxi_zone_lookup_data/")
)

# COMMAND ----------

CONTRACT = [
    ("VendorID", "INT", "vendor_id", "INT"),
    (
        "tpep_pickup_datetime", "TIMESTAMP_NTZ",
        "pickup_ts", "TIMESTAMP_NTZ"
    ),
    (
        "tpep_dropoff_datetime", "TIMESTAMP_NTZ",
        "dropoff_ts", "TIMESTAMP_NTZ"
    ),
    (
        "passenger_count", "BIGINT",
        "passenger_count", "BIGINT"
    ),
    ("trip_distance", "DOUBLE", "trip_distance", "DOUBLE"),
    ("RatecodeID", "BIGINT", "ratecode_id", "BIGINT"),
    (
        "store_and_fwd_flag", "STRING",
        "store_and_fwd_flag", "STRING"
    ),
    ("PULocationID", "INT", "pu_location_id", "INT"),
    ("DOLocationID", "INT", "do_location_id", "INT"),
    ("payment_type", "BIGINT", "payment_type", "BIGINT"),
    ("fare_amount", "DOUBLE", "fare_amount", "DECIMAL(18,2)"),
    ("extra", "DOUBLE", "extra", "DECIMAL(18,2)"),
    ("mta_tax", "DOUBLE", "mta_tax", "DECIMAL(18,2)"),
    ("tip_amount", "DOUBLE", "tip_amount", "DECIMAL(18,2)"),
    ("tolls_amount", "DOUBLE", "tolls_amount", "DECIMAL(18,2)"),
    (
        "improvement_surcharge", "DOUBLE",
        "improvement_surcharge", "DECIMAL(18,2)"
    ),
    (
        "total_amount", "DOUBLE",
        "total_amount", "DECIMAL(18,2)"
    ),
    (
        "congestion_surcharge", "DOUBLE",
        "congestion_surcharge", "DECIMAL(18,2)"
    ),
    ("Airport_fee", "DOUBLE", "airport_fee", "DECIMAL(18,2)"),
    (
        "cbd_congestion_fee", "DOUBLE",
        "cbd_congestion_fee", "DECIMAL(18,2)"
    )
]

SOURCE_SCHEMA = ", ".join(
    f"`{source}` {bronze_type}"
    for source, bronze_type, _, _ in CONTRACT
) + ", `_rescued_data` STRING"

BUSINESS_COLUMNS = [
    target for _, _, target, _ in CONTRACT
]

MONEY_COLUMNS = [
    target
    for _, _, target, dtype in CONTRACT
    if dtype.startswith("DECIMAL")
]

KEY_COLUMNS = [
    "vendor_id",
    "pickup_ts",
    "dropoff_ts",
    "pu_location_id",
    "do_location_id"
]

META_COLUMNS = [
    "source_month",
    "_source_file",
    "_source_modified_at",
    "_ingested_at",
    "_pipeline_run_id"
]

# COMMAND ----------

print(SOURCE_SCHEMA)
print("Business columns:", BUSINESS_COLUMNS)
print("Money columns:", MONEY_COLUMNS)

# COMMAND ----------

# DBTITLE 1,Helper functions
#defines helper functions

from datetime import datetime


def exists(table):
    return spark.catalog.tableExists(table)


def latest_version(table):
    return int(
        DeltaTable.forName(spark, table)
        .history(1)
        .select("version")
        .first()[0]
    )


def snapshot(table, version):
    return (
        spark.read.format("delta")
        .option("versionAsOf", version)
        .table(table)
    )


def changes(table, start, end):
    return (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", start)
        .option("endingVersion", end)
        .table(table)
    )


def watermark(name):
    rows = (
        spark.table(WM)
        .where(F.col("pipeline_name") == name)
        .select("last_version")
        .collect()
    )
    return int(rows[0][0]) if rows else -1


def advance(name, version):
    df = (
        spark.createDataFrame(
            [(name, int(version))],
            "pipeline_name string, last_version long"
        )
        .withColumn("updated_at", F.current_timestamp())
    )

    (
        DeltaTable.forName(spark, WM).alias("t")
        .merge(
            df.alias("s"),
            "t.pipeline_name = s.pipeline_name"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def ensure_table(table, df, cdf=False, append_only=False):
    # Existing tables retain their current configuration.
    if exists(table):
        return

    writer = (
        spark.createDataFrame([], df.schema)
        .write.format("delta")
        .mode("errorifexists")
    )

    if cdf:
        writer = (
            writer
            .option("delta.enableChangeDataFeed", "true")
            .option("delta.logRetentionDuration", "interval 90 days")
            .option(
                "delta.deletedFileRetentionDuration",
                "interval 90 days"
            )
        )

    if append_only:
        writer = writer.option("delta.appendOnly", "true")

    writer.saveAsTable(table)


def month_condition(months, alias=""):
    # Accept a collection of YYYY-MM strings, not a single string.
    if isinstance(months, str):
        raise ValueError(
            "Pass months as a list, for example ['2024-10']"
        )

    months = list(months)

    if not months:
        raise ValueError(
            "months must contain at least one YYYY-MM value"
        )

    for month in months:
        if not isinstance(month, str) or len(month) != 7:
            raise ValueError(f"Invalid source month: {month!r}")

        try:
            parsed_month = datetime.strptime(month, "%Y-%m")
        except ValueError as exc:
            raise ValueError(
                f"Invalid source month: {month!r}"
            ) from exc

        if parsed_month.strftime("%Y-%m") != month:
            raise ValueError(f"Invalid source month: {month!r}")

    # Both "t" and "t." produce the prefix "t.".
    prefix = f"{alias.rstrip('.')}." if alias else ""
    values = ", ".join(f"'{month}'" for month in months)

    return f"{prefix}source_month IN ({values})"


def replace_months(table, df, months, cdf=False):
    # Validate before creating or writing the table.
    predicate = month_condition(months)

    ensure_table(table, df, cdf=cdf)

    # df must contain the complete replacement data
    # for the specified months.
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .saveAsTable(table)
    )


def json_of(columns):
    return F.to_json(
        F.struct(*[F.col(c) for c in columns]),
        options={"ignoreNullFields": "false"}
    )


def all_true(expressions):
    return reduce(
        lambda a, b: a & b,
        expressions,
        F.lit(True)
    )


def any_true(expressions):
    return reduce(
        lambda a, b: a | b,
        expressions,
        F.lit(False)
    )

# COMMAND ----------

# DBTITLE 1,Initialize zone lookup


from pyspark.sql import functions as F

# Correct source path: capital T in Taxi_zone_lookup_data.
ZONE_FILE = f"{ROOT}/Taxi_zone_lookup_data/{ZONE_FILENAME}"


def exists(table):
    return spark.catalog.tableExists(table)


# Uses "error", which worked in  serverless environment.
def ensure_table(table, df, cdf=False, append_only=False):
    if exists(table):
        return

    writer = (
        spark.createDataFrame([], df.schema)
        .write.format("delta")
        .mode("error")
    )

    if cdf:
        writer = (
            writer
            .option("delta.enableChangeDataFeed", "true")
            .option("delta.logRetentionDuration", "interval 90 days")
            .option(
                "delta.deletedFileRetentionDuration",
                "interval 90 days"
            )
        )

    if append_only:
        writer = writer.option("delta.appendOnly", "true")

    writer.saveAsTable(table)


def initialize_zones():
    required = {"LocationID", "Borough", "Zone", "service_zone"}

    # Create Bronze only when it does not already exist.
    if not exists(BZ):
        source = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("mode", "FAILFAST")
            .csv(ZONE_FILE)
        )

        missing = required - set(source.columns)

        if missing:
            raise ValueError(
                f"Missing CSV columns: {sorted(missing)}"
            )

        raw = (
            source.select(
                "*",
                F.col("_metadata.file_path").alias("_source_file")
            )
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_pipeline_run_id", F.lit(RUN_ID))
        )

        (
            raw.write
            .format("delta")
            .option("delta.appendOnly", "true")
            .mode("error")
            .saveAsTable(BZ)
        )

        print(f"Created Bronze lookup: {BZ}")
    else:
        print(f"Using existing Bronze lookup: {BZ}")

    # Validate and transform the existing or newly created Bronze data.
    raw = spark.table(BZ)

    missing = required - set(raw.columns)

    if missing:
        raise ValueError(
            f"Missing columns in {BZ}: {sorted(missing)}"
        )

    zones = raw.select(
        F.expr("try_cast(LocationID AS INT)").alias("location_id"),
        F.trim(F.col("Borough")).alias("borough"),
        F.trim(F.col("Zone")).alias("zone"),
        F.trim(F.col("service_zone")).alias("service_zone")
    )

    row_count = zones.count()

    if row_count == 0:
        raise ValueError(f"The lookup table {BZ} is empty.")

    invalid_condition = F.col("location_id").isNull()

    for column in ["borough", "zone", "service_zone"]:
        invalid_condition = (
            invalid_condition
            | F.col(column).isNull()
            | (F.col(column) == "")
        )

    invalid = zones.where(invalid_condition)

    if invalid.limit(1).count() > 0:
        display(invalid.limit(20))
        raise ValueError(
            f"Invalid lookup rows found in {BZ}. "
            "Silver was not created or changed."
        )

    duplicates = (
        zones.groupBy("location_id")
        .count()
        .where(F.col("count") > 1)
    )

    if duplicates.limit(1).count() > 0:
        display(duplicates.limit(20))
        raise ValueError(
            "Duplicate location IDs found. "
            "Silver was not created or changed."
        )

    print(f"Bronze validation passed: {row_count} rows.")

    # Create Silver only when it does not already exist.
    if not exists(SZ):
        (
            zones.write
            .format("delta")
            .mode("error")
            .saveAsTable(SZ)
        )

        print(f"Created Silver lookup: {SZ}")
    else:
        print(f"Using existing Silver lookup: {SZ}; not refreshed.")

    print(f"Silver row count: {spark.table(SZ).count()}")
    print("Lookup initialization completed successfully.")


# Run initialization and preview Silver.
initialize_zones()

display(spark.table(SZ).orderBy("location_id"))

# COMMAND ----------

