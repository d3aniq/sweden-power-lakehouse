# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: one schema, validated
# MAGIC
# MAGIC This is where the differences listed in `docs/source-differences.md`
# MAGIC are resolved, one at a time: encoding (done in bronze), wide format,
# MAGIC different zone codes, different units, decimal comma, categories
# MAGIC that do not correspond, and zero meaning two different things.
# MAGIC
# MAGIC The result is two tables in the same vocabulary:
# MAGIC `silver.production_hourly` from Mimer and `silver.production_monthly`
# MAGIC from SCB.
# MAGIC
# MAGIC The quality gates at the end stop the run if anything fails. A job
# MAGIC that fails loudly beats a gold table that is quietly wrong.

# COMMAND ----------

import csv
import io

from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = "workspace"
spark.sql(f"USE CATALOG {CATALOG}")

# Common period: Mimer ends 2025-03-17, SCB starts 2021-01.
PERIOD_START = "2021-01-01"
PERIOD_END = "2025-02-28"

# COMMAND ----------

# MAGIC %md
# MAGIC ## The mapping table
# MAGIC
# MAGIC The sources name the same things differently, and they do not cover
# MAGIC quite the same things. The mapping lives in a table rather than in
# MAGIC if-statements, so it can be read, changed and reviewed without
# MAGIC anyone touching the code.
# MAGIC
# MAGIC Two details worth noting:
# MAGIC
# MAGIC - SCB's category names carry trailing spaces and double spaces
# MAGIC   inside them. Both trimming and whitespace collapsing are applied
# MAGIC   on both sides of the join, or nothing matches.
# MAGIC - Mimer's *uppmätt ospecificerad produktion* (measured unspecified)
# MAGIC   has no SCB counterpart, and SCB's conventional thermal power
# MAGIC   includes diesel plants. The categories are not identical, only
# MAGIC   closest comparable. That is what the `comparable` column records.

# COMMAND ----------

mapping_rows = [
    # (source, category in source, shared code, measure, comparable across sources)
    ("mimer", "hydro",        "hydro",       "production", True),
    ("mimer", "wind",         "wind",        "production", True),
    ("mimer", "solar",        "solar",       "production", True),
    ("mimer", "nuclear",      "nuclear",     "production", True),
    ("mimer", "thermal",      "thermal",     "production", True),
    ("mimer", "unspecified",  "unspecified", "production", False),

    ("scb", "summa produktion",                     "total_production",  "total",       False),
    ("scb", "vattenkraft (inkl. pumpkraft), netto", "hydro",             "production",  True),
    ("scb", "vindkraft",                            "wind",              "production",  True),
    ("scb", "solkraft",                             "solar",             "production",  True),
    ("scb", "kärnkraft (kondens), netto",           "nuclear",           "production",  True),
    ("scb", "konventionell värmekraft, netto",      "thermal",           "production",  True),
    ("scb", "summa användning",                     "total_consumption", "consumption", False),
    ("scb", "mineralutvinning och tillverkning",    "industry",          "consumption", False),
    ("scb", "el-, gas-, värme- och vattenverk",     "utilities",         "consumption", False),
    ("scb", "järn- och spårvägar, busstrafik",      "transport",         "consumption", False),
    ("scb", "övrigt (bostäder, service m.m.)",      "other",             "consumption", False),
    ("scb", "förluster",                            "losses",            "consumption", False),
    ("scb", "överföring till eller från annat elområde eller land",
                                                    "net_transfer",      "transfer",    False),
]

mapping = (
    spark.createDataFrame(
        mapping_rows,
        "source STRING, source_category STRING, category STRING, measure STRING, comparable BOOLEAN",
    )
    # Same normalisation as on the source side, so both are treated alike.
    .withColumn("source_category", F.regexp_replace(F.trim("source_category"), r"\s+", " "))
)

(mapping.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("silver.category_mapping"))

display(mapping)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mimer: hourly values
# MAGIC
# MAGIC Four things happen: the Summa row goes, decimal comma becomes
# MAGIC point, kWh becomes MWh, and duplicates are removed.
# MAGIC
# MAGIC Deduplication keeps the most recently published version of each
# MAGIC hour. Mimer can republish a value, and the publication timestamp
# MAGIC stays as evidence of which version was used.

# COMMAND ----------

raw = spark.table("bronze.mimer_production")

parsed = (
    raw
    # The Summa row is the source's own aggregate, not an observation.
    .filter(F.col("period_raw") != "Summa")
    .withColumn("ts", F.to_timestamp("period_raw", "yyyy-MM-dd HH:mm"))
    .withColumn("published_at", F.to_timestamp("published_at_raw", "yyyy-MM-dd HH:mm"))
    # Decimal comma. Read as a thousands separator, the value ends up a
    # thousand times too large. See docs/source-differences.md, point 7.
    .withColumn("value_kwh", F.regexp_replace("settled_kwh_raw", ",", ".").cast("decimal(20,3)"))
    .withColumn("value_mwh", (F.col("value_kwh") / F.lit(1000)).cast("decimal(20,6)"))
)

# Catch rows that failed to parse before they disappear silently.
unparsed = parsed.filter(F.col("ts").isNull() | F.col("value_kwh").isNull())
if unparsed.count() > 0:
    display(unparsed.select("_source_file", "period_raw", "settled_kwh_raw").limit(20))
    raise AssertionError(f"{unparsed.count()} rows could not be parsed, see above")

latest = Window.partitionBy("area", "production_type", "ts").orderBy(F.col("published_at").desc())

hourly = (
    parsed
    .join(mapping.filter(F.col("source") == "mimer"),
          parsed.production_type == F.col("source_category"), "left")
    .withColumn("_rn", F.row_number().over(latest))
    .filter(F.col("_rn") == 1).drop("_rn")
    .select(
        F.lit("mimer").alias("source"),
        "area",
        "category",
        "measure",
        "ts",
        F.to_date("ts").alias("date"),
        F.date_trunc("month", F.col("ts")).cast("date").alias("month"),
        "value_mwh",
        "published_at",
        F.col("_source_file").alias("source_file"),
        F.current_timestamp().alias("_processed_at"),
    )
)

assert hourly.filter(F.col("category").isNull()).count() == 0, \
    "A production type is missing from the mapping table"

(hourly.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .partitionBy("area")
    .saveAsTable("silver.production_hourly"))

print(f"{spark.table('silver.production_hourly').count():,} hourly values")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCB: from wide to long
# MAGIC
# MAGIC The header row is read first, because the number of month columns
# MAGIC grows every time the table is updated. Hardcoding it would make
# MAGIC this notebook wrong at the next download.
# MAGIC
# MAGIC Data rows are recognised by their second field being SE1-SE4. The
# MAGIC title row and the header row fall away on their own, without the
# MAGIC code having to trust line numbers.

# COMMAND ----------

lines = spark.table("bronze.scb_raw_lines")

# The title row also contains the word "elområde", so the word alone is
# not a sufficient marker. The header row is the one with at least three
# fields where the second field is exactly "elområde".
candidates = [
    row["line"]
    for row in lines.filter(F.col("line").contains("elområde")).orderBy("line_no").collect()
]

header_line, columns = None, None
for candidate in candidates:
    fields = next(csv.reader(io.StringIO(candidate)), [])
    if len(fields) >= 3 and fields[1].strip().lower() == "elområde":
        header_line, columns = candidate, fields
        break

if header_line is None:
    for candidate in candidates:
        print("candidate:", candidate[:120])
    raise AssertionError(
        "No header row found in bronze.scb_raw_lines. Candidates are listed above. "
        "If the list is empty or the characters look wrong, rerun 01_bronze."
    )

months = columns[2:]
assert months, "The header row has no month columns"
print(f"{len(months)} month columns: {months[0]} to {months[-1]}")

schema = ", ".join(f"c{i} STRING" for i in range(len(columns)))

wide = (
    lines.filter(F.trim(F.col("line")) != "")
    .select(F.from_csv(F.col("line"), schema).alias("r"), "_source_file")
    .filter(F.col("r.c1").rlike("^SE[1-4]$"))
)

pairs = F.array(*[
    F.struct(F.lit(m).alias("month_code"), F.col(f"r.c{i + 2}").alias("value_raw"))
    for i, m in enumerate(months)
])

long = (
    wide.select(
        # The category names carry both trailing spaces and double spaces
        # inside them ("el-, gas-,  värme- och vattenverk "). Trimming
        # alone is not enough, inner whitespace must be collapsed too.
        F.regexp_replace(F.trim(F.col("r.c0")), r"\s+", " ").alias("source_category"),
        F.col("r.c1").alias("area"),
        F.col("_source_file").alias("source_file"),
        F.explode(pairs).alias("p"),
    )
    .select("source_category", "area", "source_file", "p.month_code", "p.value_raw")
    # 2021M01 -> 2021-01-01. F.concat, not +, which means arithmetic in PySpark.
    .withColumn(
        "month",
        F.to_date(F.concat(F.regexp_replace("month_code", "M", "-"), F.lit("-01"))),
    )
    # GWh to MWh.
    .withColumn("value_mwh", (F.col("value_raw").cast("decimal(20,3)") * F.lit(1000)).cast("decimal(20,6)"))
)

bad_months = long.filter(F.col("month").isNull()).select("month_code").distinct()
if bad_months.count() > 0:
    display(bad_months)
    raise AssertionError("Month codes that could not be parsed, see above")

unmapped = (
    long.join(mapping.filter(F.col("source") == "scb"), "source_category", "left_anti")
    .select("source_category").distinct()
)
if unmapped.count() > 0:
    display(unmapped)
    raise AssertionError("SCB categories missing from the mapping table, see above")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Zero meaning two different things
# MAGIC
# MAGIC SCB writes 0 for nuclear in SE1, SE2 and SE4. That is not a measured
# MAGIC zero but "this category does not apply here". Mimer responded with
# MAGIC an empty file for the same combinations, recorded in the manifest
# MAGIC with status `no_data`.
# MAGIC
# MAGIC The manifest is therefore used as the authority: combinations Mimer
# MAGIC has no data for are marked `not_applicable` rather than `reported`.
# MAGIC Writing both as 0 makes every average across bidding zones wrong.
# MAGIC
# MAGIC Combinations not yet fetched from Mimer are marked `unknown`. More
# MAGIC honest than guessing.

# COMMAND ----------

not_applicable = (
    spark.table("bronze.mimer_manifest")
    .filter(F.col("status") == "no_data")
    .join(mapping.filter(F.col("source") == "mimer"),
          F.col("production_type") == F.col("source_category"))
    .select("area", "category").distinct()
    .withColumn("availability", F.lit("not_applicable"))
)

fetched = (
    spark.table("bronze.mimer_manifest")
    .join(mapping.filter(F.col("source") == "mimer"),
          F.col("production_type") == F.col("source_category"))
    .select("area", "category").distinct()
)

monthly = (
    long.join(mapping.filter(F.col("source") == "scb"), "source_category")
    .join(not_applicable, ["area", "category"], "left")
    .join(fetched.withColumn("_fetched", F.lit(True)), ["area", "category"], "left")
    .withColumn(
        "availability",
        F.when(F.col("availability").isNotNull(), F.col("availability"))
        .when(F.col("measure") != "production", F.lit("reported"))
        .when(F.col("_fetched").isNotNull(), F.lit("reported"))
        .otherwise(F.lit("unknown")),
    )
    .select(
        F.lit("scb").alias("source"),
        "area", "category", "measure", "month", "value_mwh", "availability",
        "source_category", "source_file",
        F.current_timestamp().alias("_processed_at"),
    )
)

(monthly.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("silver.production_monthly"))

print(f"{spark.table('silver.production_monthly').count():,} monthly values")
display(
    spark.table("silver.production_monthly")
    .groupBy("measure", "availability").count().orderBy("measure", "availability")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality gates
# MAGIC
# MAGIC Six checks. If one fails the job stops here, and gold is never built
# MAGIC on data that does not hold.

# COMMAND ----------

failures = []

h = spark.table("silver.production_hourly")
m = spark.table("silver.production_monthly")

# 1. No nulls in the keys.
for table, name, keys in [
    (h, "production_hourly", ["area", "category", "ts", "value_mwh"]),
    (m, "production_monthly", ["area", "category", "month"]),
]:
    for key in keys:
        n = table.filter(F.col(key).isNull()).count()
        if n:
            failures.append(f"{name}: {n} rows with null in {key}")

# 2. Uniqueness. One observation per source, zone, category and timestamp.
dupes = h.groupBy("area", "category", "ts").count().filter(F.col("count") > 1)
if dupes.count():
    failures.append(f"production_hourly: {dupes.count()} duplicates")

# 3. Days with the wrong hour count. Daylight saving would give 23 or 25
#    if the timestamps were local time; 24 everywhere points to UTC.
odd_days = (
    h.groupBy("area", "category", "date").agg(F.count("*").alias("hours"))
    .filter(F.col("hours") != 24)
)
odd_count = odd_days.count()
if odd_count:
    print(f"Days with other than 24 hours: {odd_count}")
    display(odd_days.orderBy("date").limit(20))
else:
    print("Every day has 24 hours. The timestamps are not local time with DST.")

# 4. Negative values. Hydro including pumped storage can be negative net,
#    other production types cannot.
neg = h.filter((F.col("value_mwh") < 0) & (F.col("category") != "hydro"))
if neg.count():
    display(neg.limit(20))
    failures.append(f"production_hourly: {neg.count()} negative values outside hydro")

# 5. Dates within the expected range.
outside = h.filter((F.col("date") < PERIOD_START) | (F.col("date") > PERIOD_END))
if outside.count():
    failures.append(f"production_hourly: {outside.count()} rows outside {PERIOD_START}-{PERIOD_END}")

# 6. Solar at night. The production type exists only in Mimer's URL
#    parameter, never in the file, so a wrong code gives the right name on
#    the wrong data — which passes every formal check.
#
#    The test is relative, not absolute. Settled data carries small
#    corrections that show up as one or two MWh at night, which is real
#    but negligible; an absolute threshold would flag those. A wrong
#    mapping, on the other hand, would put night output on the same order
#    as noon. Night should be a rounding error next to midday.
solar = h.filter(F.col("category") == "solar").withColumn("hour_of_day", F.hour("ts"))
night_share = (
    solar.groupBy("area")
    .agg(
        F.avg(F.when(F.col("hour_of_day").isin(0, 1, 2, 23), F.col("value_mwh"))).alias("night_avg"),
        F.avg(F.when(F.col("hour_of_day") == 12, F.col("value_mwh"))).alias("noon_avg"),
    )
    .withColumn("night_vs_noon", F.round(F.col("night_avg") / F.col("noon_avg"), 4))
)
display(night_share)
suspect = night_share.filter(F.col("night_vs_noon") > 0.02)
if suspect.count():
    failures.append(
        f"production_hourly: solar output at night exceeds 2% of midday in "
        f"{suspect.count()} zones — check that the Mimer code really maps to solar"
    )

if failures:
    for f_ in failures:
        print("FAILED:", f_)
    raise AssertionError(f"{len(failures)} quality checks failed")
print("All quality checks passed")

# COMMAND ----------

display(spark.sql("SHOW TABLES IN silver"))
