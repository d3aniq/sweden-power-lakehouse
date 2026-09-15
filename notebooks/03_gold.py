# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: three tables someone actually wants
# MAGIC
# MAGIC 1. `gold.production_monthly` — monthly production per bidding zone
# MAGIC    and production type, from both sources in the same shape.
# MAGIC 2. `gold.source_reconciliation` — the difference between Svenska
# MAGIC    kraftnät's measured values and SCB's statistics, month by month.
# MAGIC 3. `gold.hourly_profile` — daily profile per production type, what
# MAGIC    hourly resolution gives that monthly statistics never can.
# MAGIC
# MAGIC Table 2 is the point. Two agencies describe the same physical
# MAGIC reality and the numbers differ. The question is not who is right
# MAGIC but by how much, where, and why.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# SCB reports GWh as integers. A value of 2,690 GWh could be anywhere
# between 2,689.5 and 2,690.5, which is 500 MWh either way. Differences
# smaller than that are rounding, not disagreement.
SCB_ROUNDING_MWH = 500

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Monthly production
# MAGIC
# MAGIC Hourly values are summed to months. A month is only included if all
# MAGIC its hours are present, otherwise the sum is too low and the
# MAGIC comparison is misleading without showing it. Incomplete months land
# MAGIC in `gold.excluded_months` rather than disappearing silently.

# COMMAND ----------

hourly = spark.table("silver.production_hourly")

per_month = (
    hourly.groupBy("area", "category", "month")
    .agg(
        F.sum("value_mwh").alias("value_mwh"),
        F.count("*").alias("hours_present"),
        F.min("ts").alias("first_hour"),
        F.max("ts").alias("last_hour"),
    )
    .withColumn("hours_expected", F.dayofmonth(F.last_day("month")) * F.lit(24))
    .withColumn("is_complete", F.col("hours_present") == F.col("hours_expected"))
)

excluded = per_month.filter(~F.col("is_complete"))
(excluded.select("area", "category", "month", "hours_present", "hours_expected")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("gold.excluded_months"))
print(f"{excluded.count()} incomplete months excluded (expected: February 2025 is the last)")

mimer_monthly = (
    per_month.filter("is_complete")
    .select(F.lit("mimer").alias("source"), "area", "category", "month", "value_mwh")
)

scb_monthly = (
    spark.table("silver.production_monthly")
    .filter((F.col("measure") == "production") & (F.col("availability") == "reported"))
    .select(F.lit("scb").alias("source"), "area", "category", "month", "value_mwh")
)

production_monthly = mimer_monthly.unionByName(scb_monthly)

(production_monthly.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("gold.production_monthly"))

display(
    production_monthly.groupBy("source", "category")
    .agg(F.count("*").alias("rows"), F.min("month").alias("from"), F.max("month").alias("to"))
    .orderBy("category", "source")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Do the sources agree?
# MAGIC
# MAGIC Only categories the mapping table marks as comparable are included.
# MAGIC Mimer's measured unspecified production has no SCB counterpart and
# MAGIC would only create false deviations.
# MAGIC
# MAGIC `within_rounding` says whether the difference fits inside SCB's own
# MAGIC rounding. If it does, there is no disagreement to explain.

# COMMAND ----------

comparable = (
    spark.table("silver.category_mapping")
    .filter(F.col("comparable")).select("category").distinct()
)

wide = (
    spark.table("gold.production_monthly")
    .join(comparable, "category")
    .groupBy("area", "category", "month")
    .pivot("source", ["mimer", "scb"])
    .agg(F.first("value_mwh"))
    .withColumnRenamed("mimer", "mimer_mwh")
    .withColumnRenamed("scb", "scb_mwh")
)

reconciliation = (
    wide.filter(F.col("mimer_mwh").isNotNull() & F.col("scb_mwh").isNotNull())
    .withColumn("diff_mwh", F.col("mimer_mwh") - F.col("scb_mwh"))
    .withColumn("abs_diff_mwh", F.abs("diff_mwh"))
    .withColumn(
        "diff_pct",
        F.when(F.col("scb_mwh") != 0,
               F.round(F.col("diff_mwh") / F.col("scb_mwh") * 100, 2)),
    )
    .withColumn("within_rounding", F.col("abs_diff_mwh") <= F.lit(SCB_ROUNDING_MWH))
    .withColumn("year", F.year("month"))
    .select("area", "category", "month", "year", "mimer_mwh", "scb_mwh",
            "diff_mwh", "abs_diff_mwh", "diff_pct", "within_rounding")
)

(reconciliation.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("gold.source_reconciliation"))

print(f"{reconciliation.count()} months compared")

# COMMAND ----------

# MAGIC %md
# MAGIC ### How large are the differences?
# MAGIC
# MAGIC `median_abs_pct` is more useful than the mean, since a few months
# MAGIC with small denominators would otherwise distort the whole picture.
# MAGIC Solar in December is one such case: a few GWh make a small absolute
# MAGIC difference look like a large relative one.

# COMMAND ----------

summary = (
    spark.table("gold.source_reconciliation")
    .groupBy("category", "area")
    .agg(
        F.count("*").alias("months"),
        F.sum(F.col("within_rounding").cast("int")).alias("within_rounding"),
        F.round(F.expr("percentile_approx(abs(diff_pct), 0.5)"), 2).alias("median_abs_pct"),
        F.round(F.max("abs_diff_mwh"), 0).alias("max_abs_diff_mwh"),
        F.round(F.avg("diff_mwh"), 0).alias("mean_diff_mwh"),
    )
    .orderBy("category", "area")
)
display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC A positive `mean_diff_mwh` means Svenska kraftnät sits consistently
# MAGIC above SCB, a negative one the opposite. A small number that flips
# MAGIC sign is noise; a number that points the same way in every zone is a
# MAGIC difference in definition.
# MAGIC
# MAGIC The largest deviations, worth a closer look:

# COMMAND ----------

display(
    spark.table("gold.source_reconciliation")
    .orderBy(F.col("abs_diff_mwh").desc()).limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Daily profile
# MAGIC
# MAGIC This cannot be derived from SCB. Wind varies across the day in a
# MAGIC different way than nuclear, and a monthly figure hides it entirely.
# MAGIC This table is the justification for fetching hourly data at all.

# COMMAND ----------

hourly_profile = (
    hourly.withColumn("hour_of_day", F.hour("ts"))
    .withColumn("year", F.year("ts"))
    .groupBy("area", "category", "year", "hour_of_day")
    .agg(
        F.round(F.avg("value_mwh"), 1).alias("avg_mwh"),
        F.round(F.expr("percentile_approx(value_mwh, 0.5)"), 1).alias("median_mwh"),
        F.round(F.min("value_mwh"), 1).alias("min_mwh"),
        F.round(F.max("value_mwh"), 1).alias("max_mwh"),
        F.count("*").alias("observations"),
    )
)

(hourly_profile.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("gold.hourly_profile"))

display(
    spark.table("gold.hourly_profile")
    .filter((F.col("area") == "SE3") & (F.col("year") == 2024))
    .orderBy("category", "hour_of_day")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check before gold may be used
# MAGIC
# MAGIC Aggregation must not lose energy. The sum in gold has to equal the
# MAGIC sum in silver for the months that were included.

# COMMAND ----------

silver_sum = (
    per_month.filter("is_complete")
    .agg(F.sum("value_mwh").alias("s")).first()["s"]
)
gold_sum = (
    spark.table("gold.production_monthly").filter(F.col("source") == "mimer")
    .agg(F.sum("value_mwh").alias("s")).first()["s"]
)
assert abs(float(silver_sum) - float(gold_sum)) < 1, \
    f"Energy lost in aggregation: {silver_sum} vs {gold_sum}"
print(f"OK: {float(gold_sum):,.0f} MWh in both silver and gold")

display(spark.sql("SHOW TABLES IN gold"))
