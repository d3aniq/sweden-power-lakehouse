# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: tre tabeller någon faktiskt vill se
# MAGIC
# MAGIC 1. `gold.production_monthly` — månadsproduktion per elområde och
# MAGIC    kraftslag, från båda källorna i samma form.
# MAGIC 2. `gold.source_reconciliation` — skillnaden mellan Svenska
# MAGIC    kraftnäts uppmätta värden och SCB:s statistik, månad för månad.
# MAGIC 3. `gold.hourly_profile` — dygnsprofil per kraftslag, det
# MAGIC    timupplösningen ger som månadsstatistik aldrig kan visa.
# MAGIC
# MAGIC Tabell 2 är poängen. Två myndigheter beskriver samma fysiska
# MAGIC verklighet, och siffrorna skiljer sig. Frågan är inte vem som har
# MAGIC rätt utan hur mycket, var och varför.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# SCB anger GWh i heltal. Ett värde på 2 690 GWh kan alltså ligga var
# som helst mellan 2 689,5 och 2 690,5, vilket är 500 MWh åt vardera
# hållet. Mindre skillnader än så är avrundning, inte oenighet.
SCB_ROUNDING_MWH = 500

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Månadsproduktion
# MAGIC
# MAGIC Timvärdena summeras till månad. En månad tas bara med om alla dess
# MAGIC timmar finns, annars blir summan för låg och jämförelsen missvisande
# MAGIC utan att det syns. Ofullständiga månader hamnar i `gold.excluded_months`
# MAGIC i stället för att tyst försvinna.

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
print(f"{excluded.count()} ofullständiga månader undantagna (väntat: februari 2025 är sista)")

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
    .agg(F.count("*").alias("rader"), F.min("month").alias("från"), F.max("month").alias("till"))
    .orderBy("category", "source")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Stämmer källorna överens?
# MAGIC
# MAGIC Bara kategorier som mappningstabellen markerat som jämförbara tas
# MAGIC med. Mimers *uppmätt ospecificerad produktion* har ingen motsvarighet
# MAGIC hos SCB och skulle bara skapa falska avvikelser.
# MAGIC
# MAGIC `within_rounding` säger om skillnaden ryms i SCB:s egen avrundning.
# MAGIC Gör den det finns ingen oenighet att förklara.

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

print(f"{reconciliation.count()} månader jämförda")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hur stora är skillnaderna?
# MAGIC
# MAGIC `median_abs_pct` är mer användbar än medelvärdet, eftersom enstaka
# MAGIC månader med små nämnare annars drar iväg hela bilden. Solkraft i
# MAGIC december är ett sådant fall: några enstaka GWh gör att en liten
# MAGIC absolut skillnad ser ut som en stor procentuell.

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
# MAGIC Ett positivt `mean_diff_mwh` betyder att Svenska kraftnät
# MAGIC genomgående ligger högre än SCB, ett negativt tvärtom. Ett litet
# MAGIC tal som växlar tecken är brus, ett tal som konsekvent pekar åt
# MAGIC samma håll är en definitionsskillnad.
# MAGIC
# MAGIC De största avvikelserna, att titta närmare på:

# COMMAND ----------

display(
    spark.table("gold.source_reconciliation")
    .orderBy(F.col("abs_diff_mwh").desc()).limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dygnsprofil
# MAGIC
# MAGIC Det här går inte att få ur SCB. Vindkraft varierar över dygnet på
# MAGIC ett annat sätt än kärnkraft, och en månadssiffra döljer det helt.
# MAGIC Tabellen är motiveringen till att timdata alls hämtades.

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
# MAGIC ## Kontroll innan gold får användas
# MAGIC
# MAGIC Aggregaten ska inte tappa energi. Summan i gold måste vara samma
# MAGIC som summan i silver för de månader som togs med.

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
    f"Energi försvann i aggregeringen: {silver_sum} mot {gold_sum}"
print(f"OK: {float(gold_sum):,.0f} MWh i både silver och gold")

display(spark.sql("SHOW TABLES IN gold"))
