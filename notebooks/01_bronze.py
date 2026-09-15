# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: raw data in, unchanged
# MAGIC
# MAGIC Bronze is a faithful copy of what the sources delivered. No type
# MAGIC conversion, no normalisation, no rows removed. Everything is stored
# MAGIC as text. If something goes wrong later, silver can be rebuilt
# MAGIC without going back to the source.
# MAGIC
# MAGIC Three things are added, nothing is ever taken away:
# MAGIC `_source`, `_source_file` and `_ingested_at`.
# MAGIC
# MAGIC Two decisions worth explaining:
# MAGIC
# MAGIC 1. **Mimer's column names are renamed.** The originals are `Period`,
# MAGIC    `Avräknad (kWh)` and `Publiceringstidpunkt`. Parentheses and
# MAGIC    spaces do not work in Delta without column mapping, so they
# MAGIC    become `period_raw`, `settled_kwh_raw` and `published_at_raw`.
# MAGIC    The original names are recorded here. The content is untouched.
# MAGIC 2. **SCB is stored as text lines.** The file has a title row and a
# MAGIC    blank row before the header, it is wide, and it is ISO-8859-1.
# MAGIC    Interpreting that structure is a decision, and decisions belong
# MAGIC    in silver. Bronze only fixes the encoding and stores the lines
# MAGIC    as they are.
# MAGIC
# MAGIC    The file is read through the CSV reader with a delimiter that
# MAGIC    does not occur in the data, not through `spark.read.text()`. The
# MAGIC    text reader takes no `encoding` option and always assumes UTF-8,
# MAGIC    and the SCB file is ISO-8859-1.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
LANDING = f"/Volumes/{CATALOG}/bronze/landing"

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mimer: hourly values per bidding zone and production type
# MAGIC
# MAGIC The bidding zone, production type and period exist only in the file
# MAGIC name, not in the content. They are extracted here so that every row
# MAGIC is self-describing.
# MAGIC
# MAGIC The Summa (total) row at the end of each file is loaded as ordinary
# MAGIC data. It is part of what the source delivered, and it is filtered
# MAGIC out in silver, not here.

# COMMAND ----------

mimer_files = spark.read.format("binaryFile").load(f"{LANDING}/mimer/*.csv")
print(f"{mimer_files.count()} files in landing/mimer")

raw = (
    spark.read
    .option("header", True)
    .option("sep", ";")
    .option("encoding", "UTF-8")
    .option("inferSchema", False)
    .csv(f"{LANDING}/mimer/*.csv")
)

# Three columns plus an empty one from the trailing semicolon in the header.
assert len(raw.columns) == 4, f"Unexpected column count: {raw.columns}"

file_path = F.col("_metadata.file_path")
pattern = r"(SE\d)_([a-z_]+)_(\d{8})_(\d{8})\.csv$"

mimer_bronze = (
    raw.toDF("period_raw", "settled_kwh_raw", "published_at_raw", "trailing_empty")
    .withColumn("_source_file", F.element_at(F.split(file_path, "/"), -1))
    .withColumn("area", F.regexp_extract(file_path, pattern, 1))
    .withColumn("production_type", F.regexp_extract(file_path, pattern, 2))
    .withColumn("_source", F.lit("mimer"))
    .withColumn("_ingested_at", F.current_timestamp())
    .drop("trailing_empty")
)

assert mimer_bronze.filter(F.col("area") == "").count() == 0, \
    "A file name does not follow the pattern SEn_type_date_date.csv"

(mimer_bronze.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("bronze.mimer_production"))

print(f"{spark.table('bronze.mimer_production').count():,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The manifest
# MAGIC
# MAGIC The download script's log: URL, timestamp, bytes, SHA-256 and row
# MAGIC count per file. It makes it possible to verify that bronze holds
# MAGIC exactly what was fetched, and it records the combinations that have
# MAGIC no data at all.

# COMMAND ----------

manifest = (
    spark.read.json(f"{LANDING}/mimer/manifest.jsonl")
    .withColumn("_ingested_at", F.current_timestamp())
)

(manifest.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("bronze.mimer_manifest"))

display(
    manifest.groupBy("status")
    .agg(F.count("*").alias("files"), F.sum("data_rows").alias("rows"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check: does bronze match what was downloaded?
# MAGIC
# MAGIC The row count per file in bronze should be the manifest's
# MAGIC `data_rows` plus one, since the Summa row was excluded at download
# MAGIC time but is loaded here. Any deviation means something was lost on
# MAGIC the way in.

# COMMAND ----------

per_file = (
    spark.table("bronze.mimer_production")
    .groupBy("_source_file").agg(F.count("*").alias("rows_in_bronze"))
)

check = (
    manifest.filter(F.col("status") == "ok")
    .select(F.col("file").alias("_source_file"), "data_rows")
    .join(per_file, "_source_file", "full_outer")
    .withColumn("diff", F.col("rows_in_bronze") - F.col("data_rows") - F.lit(1))
)

mismatches = check.filter((F.col("diff") != 0) | F.col("diff").isNull())
if mismatches.count() > 0:
    display(mismatches)
    raise AssertionError("Bronze does not match the manifest, see the table above")
print(f"OK: {check.count()} files match the manifest")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCB: monthly values per bidding zone
# MAGIC
# MAGIC The file is ISO-8859-1. Read as UTF-8, "elområde" (bidding zone)
# MAGIC turns into "elomr?de", and the damage follows all the way to gold.
# MAGIC
# MAGIC Lines are stored in the order they appear. `line_no` lets silver
# MAGIC locate the header row and know which rows are data.

# COMMAND ----------

SENTINEL = "\u0001"  # does not occur in the file, gives one column per line

scb_lines = (
    spark.read
    .option("encoding", "ISO-8859-1")
    .option("sep", SENTINEL)
    .option("quote", "\u0000")   # disable quote handling, keep the line raw
    .option("header", False)
    .csv(f"{LANDING}/scb/*.csv")
    .select(F.coalesce(F.col("_c0"), F.lit("")).alias("value"), "_metadata")
    .withColumn("_source_file", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1))
    .drop("_metadata")
    .withColumn("line_no", F.monotonically_increasing_id())
    .withColumnRenamed("value", "line")
    .withColumn("_source", F.lit("scb"))
    .withColumn("_ingested_at", F.current_timestamp())
)

(scb_lines.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("bronze.scb_raw_lines"))

print(f"{spark.table('bronze.scb_raw_lines').count():,} rows written")

# Check: the encoding must be correct.
# Replacement characters are not a sufficient test, because ISO-8859-1 can
# decode any byte sequence without complaining. Check instead that a word
# which must exist in the file can actually be found.
scb = spark.table("bronze.scb_raw_lines")
assert scb.filter(F.col("line").contains("\ufffd")).count() == 0, \
    "Replacement characters in the SCB data, the encoding is wrong"
assert scb.filter(F.col("line").contains("elområde")).count() > 0, \
    "Cannot find 'elområde' in the SCB data, the encoding is wrong"
print("OK: the SCB data is readable and Swedish characters are intact")

display(spark.table("bronze.scb_raw_lines").orderBy("line_no").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What now exists
# MAGIC
# MAGIC - `bronze.mimer_production` — hourly values, all as text
# MAGIC - `bronze.mimer_manifest` — what was fetched and when
# MAGIC - `bronze.scb_raw_lines` — the SCB file's lines, correctly encoded
# MAGIC
# MAGIC Nothing is normalised, nothing is removed. Silver takes over.

# COMMAND ----------

display(spark.sql("SHOW TABLES IN bronze"))
