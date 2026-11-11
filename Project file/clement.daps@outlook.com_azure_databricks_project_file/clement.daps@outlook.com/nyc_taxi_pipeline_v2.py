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

def ingest_trips():
    source = (
        spark.readStream
        .format("cloudFiles")
        .schema(SOURCE_SCHEMA)
        .option("cloudFiles.format", "parquet")
        .option(
            "cloudFiles.schemaLocation",
            f"{STATE}/schemas/yellow_trips"
        )
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("readerCaseSensitive", "false")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.allowOverwrites", "false")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.parquet")
        .load(TRIP_PATH)
    )

    source = (
        source.select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time")
             .alias("_source_modified_at")
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_pipeline_run_id", F.lit(RUN_ID))
    )

    parsed_month = F.regexp_extract(
        F.col("_source_file"),
        r"yellow_tripdata_(\d{4}-(?:0[1-9]|1[0-2]))\.parquet$",
        1
    )

    source = source.withColumn(
        "source_month",
        F.when(parsed_month != "", parsed_month)
         .otherwise(F.lit("__invalid__"))
    )

    ensure_table(BT, source, cdf=True, append_only=True)

    query = (
        source.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            f"{STATE}/checkpoints/yellow_trips"
        )
        .trigger(availableNow=True)
        .toTable(BT)
    )

    query.awaitTermination()

# COMMAND ----------

#function copies taxi trip records from the Parquet files in ADLS Gen2 into the Bronze Delta table.
ingest_trips()

display(
    spark.table("nyc_taxi_v2.bronze.yellow_trips").limit(20)
)

# COMMAND ----------

#verify that the three monthly Parquet files were loaded into the Bronze table.
display(
    spark.table(BT)
    .groupBy("source_month")
    .count()
    .orderBy("source_month")
)

# COMMAND ----------

#view _rescued_data

display(
    spark.table(BT)
    .select(
        "source_month",
        "_source_file",
        "_rescued_data"
    )
    .limit(100)
)

# COMMAND ----------

#show only records with rescued data:

display(
    spark.table(BT)
    .where(F.col("_rescued_data").isNotNull())
    .select(
        "source_month",
        "_source_file",
        "_rescued_data"
    )
    .limit(20)
)


# COMMAND ----------

#Normalize Bronze records for Silver

def normalize(raw):
    projections = []
    conversion_failures = []

    for original, _, target, dtype in CONTRACT:
        converted = F.expr(
            f"try_cast(`{original}` AS {dtype})"
        )

        projections.append(converted.alias(target))

        conversion_failures.append(
            F.col(original).isNotNull() & converted.isNull()
        )

    original_columns = [
        original for original, _, _, _ in CONTRACT
    ] + ["_rescued_data"]

    df = raw.select(
        *projections,
        *[F.col(c) for c in META_COLUMNS],
        F.col("_rescued_data"),
        json_of(original_columns).alias("_raw_record"),
        any_true(conversion_failures).alias("_cast_failed")
    )

    # Normalize only the Silver representation.
    df = df.withColumn(
        "store_and_fwd_flag",
        F.upper(F.trim("store_and_fwd_flag"))
    )

    complete_key = all_true([
        F.col(c).isNotNull() for c in KEY_COLUMNS
    ])

    df = df.withColumn(
        "trip_key",
        F.when(
            complete_key,
            F.sha2(json_of(KEY_COLUMNS), 256)
        )
    )

    return (
        df.withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn("day_of_week", F.dayofweek("pickup_ts"))
    )

# COMMAND ----------

#preview transformation

normalized = normalize(spark.table(BT))

display(
    normalized.select(
        "trip_key",
        "pickup_ts",
        "dropoff_ts",
        "fare_amount",
        "_cast_failed",
        "_rescued_data",
        "pickup_date",
        "pickup_hour",
        "day_of_week"
    ).limit(20)
)

# COMMAND ----------

#code validates taxi trips, adds pickup and drop-off zone details, and flags errors and warnings. It displays #results without saving or removing records.

def names_where(conditions):
    """Return the names of conditions that evaluate to True."""
    if not conditions:
        return F.expr("CAST(array() AS ARRAY<STRING>)")

    return F.filter(
        F.array(*[
            F.when(
                F.coalesce(condition, F.lit(False)),
                F.lit(name)
            )
            for name, condition in conditions.items()
        ]),
        lambda item: item.isNotNull()
    )


def assess(df, key_counts):
    """
    Assess normalized trips and enrich them with taxi zone details.

    key_counts must contain one row per non-null trip_key,
    with its count in _key_count.
    """
    df = df.join(key_counts, "trip_key", "left")

    zones = spark.table(SZ)

    pu = zones.select(
        F.col("location_id").alias("_pu_match"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone").alias("pickup_zone")
    )

    do = zones.select(
        F.col("location_id").alias("_do_match"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone").alias("dropoff_zone")
    )

    df = (
        df.join(
            F.broadcast(pu),
            F.col("pu_location_id") == F.col("_pu_match"),
            "left"
        )
        .join(
            F.broadcast(do),
            F.col("do_location_id") == F.col("_do_match"),
            "left"
        )
    )

    required = KEY_COLUMNS + [
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount"
    ]

    # Failed rules identify records for subsequent quarantine handling.
    valid_rules = {
        "schema_valid": (
            F.col("_rescued_data").isNull()
            & ~F.col("_cast_failed")
        ),

        "required_values_present": all_true([
            F.col(c).isNotNull()
            for c in required
        ]),

        "source_filename_valid": F.coalesce(
            F.col("source_month").rlike(
                r"^\d{4}-(0[1-9]|1[0-2])$"
            ),
            F.lit(False)
        ),

        "pickup_before_dropoff": (
            F.col("pickup_ts") < F.col("dropoff_ts")
        ),

        "zones_exist": (
            F.col("_pu_match").isNotNull()
            & F.col("_do_match").isNotNull()
        ),

        "distance_valid": (
            (F.col("trip_distance") >= 0)
            & ~F.isnan(F.col("trip_distance"))
            & (F.col("trip_distance") < F.lit(float("inf")))
        )
    }

    failures = {
        name: ~F.coalesce(predicate, F.lit(False))
        for name, predicate in valid_rules.items()
    }

    # Configured category allowlists.
    unexpected_codes = (
        ~F.coalesce(
            F.col("vendor_id").isin(1, 2, 6, 7),
            F.lit(False)
        )
        | ~F.coalesce(
            F.col("ratecode_id").isin(1, 2, 3, 4, 5, 6, 99),
            F.lit(False)
        )
        | ~F.coalesce(
            F.col("payment_type").isin(0, 1, 2, 3, 4, 5, 6),
            F.lit(False)
        )
        | ~F.coalesce(
            F.col("store_and_fwd_flag").isin("Y", "N"),
            F.lit(False)
        )
    )

    # Warnings preserve records for later review.
    warnings = {
        # The current trip_key is not guaranteed to be unique.
        "shared_trip_key": (
            F.col("_key_count") > 1
        ),

        "zero_passengers": (
            F.col("passenger_count") == 0
        ),

        "passenger_count_missing_or_negative": (
            F.col("passenger_count").isNull()
            | (F.col("passenger_count") < 0)
        ),

        "zero_distance": (
            F.col("trip_distance") == 0
        ),

        # Keep negative values signed; do not replace or remove them.
        "negative_monetary_value": any_true([
            F.coalesce(F.col(c) < 0, F.lit(False))
            for c in MONEY_COLUMNS
        ]),

        "pickup_month_differs_from_source": (
            F.date_format(F.col("pickup_ts"), "yyyy-MM")
            != F.col("source_month")
        ),

        "unexpected_or_missing_category": unexpected_codes
    }

    df = (
        df.withColumn("failed_rules", names_where(failures))
        .withColumn("warning_rules", names_where(warnings))
    )

    return df, list(valid_rules), list(warnings)

# COMMAND ----------

#It uses the assessed records to calculate monthly data-quality counts.

def quality_metrics(df, error_names, warning_names):
    output = []

    definitions = (
        [("ALL", "SUMMARY")]
        + [(name, "ERROR") for name in error_names]
        + [(name, "WARNING") for name in warning_names]
    )

    for rule, severity in definitions:
        if severity == "SUMMARY":
            failed = F.size("failed_rules") > 0
            warned = F.size("warning_rules") > 0

        elif severity == "ERROR":
            failed = F.array_contains("failed_rules", rule)
            warned = F.lit(False)

        else:
            failed = F.lit(False)
            warned = F.array_contains("warning_rules", rule)

        result = (
            df.groupBy("source_month")
            .agg(
                F.count("*").alias("rows_received"),
                F.sum(
                    F.when(failed, 1).otherwise(0)
                ).cast("long").alias("rows_quarantined"),
                F.sum(
                    F.when(warned, 1).otherwise(0)
                ).cast("long").alias("rows_warned")
            )
            .withColumn(
                "rows_passed",
                F.col("rows_received") - F.col("rows_quarantined")
            )
            .withColumn("rule", F.lit(rule))
            .withColumn("severity", F.lit(severity))
            .select(
                "source_month",
                "rule",
                "severity",
                "rows_received",
                "rows_passed",
                "rows_quarantined",
                "rows_warned"
            )
        )

        output.append(result)

    return reduce(
        lambda a, b: a.unionByName(b),
        output
    )

# COMMAND ----------

#Merge accepted Silver trips

SILVER_COLUMNS = (
    ["trip_key"]
    + BUSINESS_COLUMNS
    + [
        "pickup_date",
        "pickup_hour",
        "day_of_week",
        "pickup_borough",
        "pickup_zone",
        "dropoff_borough",
        "dropoff_zone",
        "warning_rules"
    ]
    + META_COLUMNS
)


def write_silver(assessed, months):
    """
    Replace Silver data for the specified months.

    IMPORTANT:
    assessed must contain the complete assessment of ALL Bronze
    records for these months, not just newly arrived records.
    """
    if isinstance(months, str):
        raise ValueError(
            "Pass months as a list, for example ['2024-10']."
        )

    months = list(months)

    # Validate month values before performing any writes.
    month_condition(months)

    required_columns = set(SILVER_COLUMNS) | {"failed_rules"}
    missing = required_columns - set(assessed.columns)

    if missing:
        raise ValueError(
            f"Missing assessed columns: {sorted(missing)}"
        )

    monthly = assessed.where(
        F.col("source_month").isin(*months)
    )

    # Prevent accidental deletion when an entire requested month
    # is absent from the supplied assessment.
    present_months = {
        row["source_month"]
        for row in monthly.select("source_month").distinct().collect()
    }

    missing_months = set(months) - present_months

    if missing_months:
        raise ValueError(
            "No assessed records were supplied for these months: "
            f"{sorted(missing_months)}. Silver was not changed."
        )

    valid = monthly.where(
        F.size(F.col("failed_rules")) == 0
    )

    silver_data = valid.select(*SILVER_COLUMNS)

    # Replace all target rows for these months with valid records.
    # Shared trip keys are preserved; no deduplication is applied.
    replace_months(
        ST,
        silver_data,
        months,
        cdf=True
    )

    print(f"Silver write completed: {ST}")
    print(f"Replaced source months: {', '.join(sorted(set(months)))}")



# COMMAND ----------

SILVER_COLUMNS = (
    ["trip_key"]
    + BUSINESS_COLUMNS
    + [
        "pickup_date",
        "pickup_hour",
        "day_of_week",
        "pickup_borough",
        "pickup_zone",
        "dropoff_borough",
        "dropoff_zone",
        "warning_rules"
    ]
    + META_COLUMNS
)


def write_silver(assessed, months):
    """
    Replace Silver records for the selected source months.

    assessed must contain ALL assessed Bronze records for those
    months, not just newly arrived records.
    """
    if isinstance(months, str):
        raise ValueError(
            "Pass months as a list, for example ['2024-10']."
        )

    months = list(months)

    # Validate the month list using the existing helper.
    month_condition(months)

    required_columns = set(SILVER_COLUMNS) | {"failed_rules"}
    missing = required_columns - set(assessed.columns)

    if missing:
        raise ValueError(
            f"Missing assessed columns: {sorted(missing)}"
        )

    monthly = assessed.where(
        F.col("source_month").isin(*months)
    )

    # Prevent replacement when a requested month is entirely absent.
    present_months = {
        row["source_month"]
        for row in (
            monthly.select("source_month")
            .distinct()
            .collect()
        )
    }

    missing_months = set(months) - present_months

    if missing_months:
        raise ValueError(
            "No assessed records were supplied for these months: "
            f"{sorted(missing_months)}. Silver was not changed."
        )

    # Only records with no failed validation rules enter Silver.
    # Records with warnings are retained.
    silver_data = (
        monthly
        .where(F.size(F.col("failed_rules")) == 0)
        .select(*SILVER_COLUMNS)
    )

    # Replace complete months without deduplicating shared trip keys.
    replace_months(
        ST,
        silver_data,
        months,
        cdf=True
    )

    print(f"Silver write completed: {ST}")
    print(
        "Replaced source months: "
        + ", ".join(sorted(set(months)))
    )

# COMMAND ----------

#Silver writer


# Months to rebuild in Silver.
months = ["2024-10", "2024-11", "2024-12"]

# Read and normalize ALL current Bronze trip records.
normalized = normalize(spark.table(BT))

# Count shared trip keys across the current Bronze dataset.
key_counts = (
    normalized
    .where(F.col("trip_key").isNotNull())
    .groupBy("trip_key")
    .agg(F.count("*").alias("_key_count"))
)

# Validate trips and add pickup/drop-off zone information.
assessed, validation_rule_names, warning_rule_names = assess(
    normalized,
    key_counts
)

# Write valid records, replacing Silver data for the selected months.
write_silver(assessed, months)

# Display the saved Silver row counts.
display(
    spark.table(ST)
    .where(F.col("source_month").isin(*months))
    .groupBy("source_month")
    .count()
    .orderBy("source_month")
)

# Preview the saved Silver records.
display(
    spark.table(ST)
    .where(F.col("source_month").isin(*months))
    .limit(20)
)

# COMMAND ----------

#Process new Bronze changes and affected historical months
def process_silver():
    name = "bronze_to_silver"
    previous = watermark(name)
    end = latest_version(BT)

    if end <= previous:
        print("Silver: no new Bronze versions.")
        return

    inserted = (
        changes(BT, previous + 1, end)
        .where(F.col("_change_type") == "insert")
    )

    if inserted.limit(1).count() == 0:
        advance(name, end)
        print("Silver: no new inserted Bronze records.")
        return

    new_normalized = normalize(inserted)
    all_normalized = normalize(snapshot(BT, end))

    new_keys = (
        new_normalized
        .where(F.col("trip_key").isNotNull())
        .select("trip_key")
        .distinct()
    )

    new_months = (
        new_normalized
        .select("source_month")
        .distinct()
    )

    # Include historical months sharing a newly received trip key,
    # so shared-key warnings are refreshed for those records too.
    linked_months = (
        all_normalized
        .join(new_keys, "trip_key", "left_semi")
        .select("source_month")
        .distinct()
    )

    months = [
        row["source_month"]
        for row in (
            new_months
            .unionByName(linked_months)
            .distinct()
            .collect()
        )
    ]

    # Validate before writing any outputs.
    # Invalid filename months require separate handling.
    try:
        month_condition(months)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"Cannot process source months: {months}. "
            "Check Bronze source filenames and source_month values. "
            "No outputs or watermark were changed by this run."
        ) from exc

    # Global counts identify shared keys across source months.
    key_counts = (
        all_normalized
        .where(F.col("trip_key").isNotNull())
        .groupBy("trip_key")
        .agg(F.count("*").alias("_key_count"))
    )

    # Rebuild complete affected months from the Bronze snapshot.
    affected = all_normalized.where(
        F.col("source_month").isin(*months)
    )

    assessed, errors, warnings = assess(affected, key_counts)

    rejected = (
        assessed
        .where(F.size("failed_rules") > 0)
        .select(
            "source_month",
            "trip_key",
            "_source_file",
            "_source_modified_at",
            "_ingested_at",
            "_pipeline_run_id",
            "_raw_record",
            "_rescued_data",
            "failed_rules",
            "warning_rules"
        )
    )

    metrics = quality_metrics(assessed, errors, warnings)

    # Each write replaces the complete affected months.
    write_silver(assessed, months)
    replace_months(QT, rejected, months)
    replace_months(DQ, metrics, months, cdf=True)

    # Advance only after all three output writes succeed.
    advance(name, end)

    print(f"Silver processed: {sorted(months)}")
    print(f"Bronze watermark advanced to version: {end}")



# COMMAND ----------

#runs the complete Bronze-to-Silver processing workflow.

process_silver()

# COMMAND ----------

#Refresh Gold from Silver Change Data Feed, Silver-to-Gold processing

def refresh_groups(
    target,
    current,
    changed,
    keys,
    aggregates,
    count_column
):
    affected_keys = changed.select(*keys).distinct()

    # No affected groups means there is nothing to refresh.
    if affected_keys.limit(1).count() == 0:
        return

    # Null-safe matching of current records to affected groups.
    current_alias = current.alias("c")
    affected_alias = affected_keys.alias("a")

    relevant_condition = all_true([
        F.col(f"c.`{key}`").eqNullSafe(F.col(f"a.`{key}`"))
        for key in keys
    ])

    relevant = current_alias.join(
        affected_alias,
        relevant_condition,
        "left_semi"
    )

    totals = relevant.groupBy(*keys).agg(*aggregates)

    ensure_table(target, totals)

    # Match affected groups to their recalculated totals.
    affected_alias = affected_keys.alias("a")
    totals_alias = totals.alias("g")

    totals_condition = all_true([
        F.col(f"a.`{key}`").eqNullSafe(F.col(f"g.`{key}`"))
        for key in keys
    ])

    aggregate_columns = [
        column for column in totals.columns
        if column not in keys
    ]

    source = (
        affected_alias
        .join(totals_alias, totals_condition, "left")
        .select(
            *[
                F.col(f"a.`{key}`").alias(key)
                for key in keys
            ],
            *[
                F.col(f"g.`{column}`").alias(column)
                for column in aggregate_columns
            ]
        )
    )

    merge_condition = " AND ".join(
        f"t.`{key}` <=> s.`{key}`"
        for key in keys
    )

    (
        DeltaTable.forName(spark, target).alias("t")
        .merge(source.alias("s"), merge_condition)
        # No current trips remain for this group.
        .whenMatchedDelete(
            condition=f"s.`{count_column}` IS NULL"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll(
            condition=f"s.`{count_column}` IS NOT NULL"
        )
        .execute()
    )


def process_trip_gold():
    if not exists(ST):
        print(f"Trip Gold skipped: {ST} does not exist.")
        return

    name = "silver_trips_to_gold"
    previous = watermark(name)
    end = latest_version(ST)

    if end <= previous:
        print("Trip Gold: no new Silver versions.")
        return

    # Keep all change types so old and new groups are refreshed.
    changed = changes(ST, previous + 1, end)
    current = snapshot(ST, end)

    refresh_groups(
        GD,
        current,
        changed,
        keys=["pickup_date", "pu_location_id"],
        aggregates=[
            F.count("*").alias("trips"),
            F.sum("fare_amount").alias("total_fare"),
            F.sum("tip_amount").alias("total_tips"),
            F.sum("total_amount").alias("total_amount"),
            F.avg("trip_distance").alias("average_distance")
        ],
        count_column="trips"
    )

    # Aggregates across all dates by hour, weekday, and borough.
    refresh_groups(
        GH,
        current,
        changed,
        keys=[
            "pickup_hour",
            "day_of_week",
            "pickup_borough"
        ],
        aggregates=[
            F.count("*").alias("trip_count")
        ],
        count_column="trip_count"
    )

    # Advance only after both Gold writes succeed.
    advance(name, end)

    print("Trip Gold processing completed.")
    print(f"Silver trip watermark advanced to version: {end}")


def process_quality_gold():
    if not exists(DQ):
        print(f"Quality Gold skipped: {DQ} does not exist.")
        return

    name = "silver_quality_to_gold"
    previous = watermark(name)
    end = latest_version(DQ)

    if end <= previous:
        print("Quality Gold: no new quality-result versions.")
        return

    changed = changes(DQ, previous + 1, end)

    months = [
        row["source_month"]
        for row in (
            changed.select("source_month")
            .distinct()
            .collect()
        )
    ]

    if months:
        current = (
            snapshot(DQ, end)
            .where(F.col("source_month").isin(*months))
        )

        replace_months(GQ, current, months)

        print(f"Quality Gold refreshed: {sorted(months)}")
    else:
        print("Quality Gold: no changed months.")

    advance(name, end)
    print(f"Quality watermark advanced to version: {end}")





# COMMAND ----------

#These functions create or refresh your Gold reporting tables from Silver data.

process_trip_gold()
process_quality_gold()

# COMMAND ----------

#this is the execution cell for your complete pipeline


# 1. Initialize or validate the taxi-zone lookup.
initialize_zones()

# 2. Load new Parquet files into Bronze.
ingest_trips()

# 3. Validate trips and write Silver, quarantine, and quality results.
process_silver()

# 4. Refresh Gold trip metrics.
process_trip_gold()

# 5. Refresh Gold quality metrics.
process_quality_gold()

print(f"Pipeline completed. Run ID: {RUN_ID}")


# COMMAND ----------

#skip lakflow job setup for now

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC