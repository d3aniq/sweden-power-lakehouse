# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: rådata in, oförändrad
# MAGIC
# MAGIC Bronze är en trogen kopia av det källorna levererade. Ingen
# MAGIC typkonvertering, ingen normalisering, inga borttagna rader. Allt
# MAGIC lagras som text. Går något fel längre fram kan silver köras om utan
# MAGIC att någon källa behöver kontaktas igen.
# MAGIC
# MAGIC Tre saker läggs till, aldrig något tas bort:
# MAGIC `_source`, `_source_file` och `_ingested_at`.
# MAGIC
# MAGIC Två beslut värda att förklara:
# MAGIC
# MAGIC 1. **Mimers kolumnnamn döps om.** Originalen är `Period`,
# MAGIC    `Avräknad (kWh)` och `Publiceringstidpunkt`. Parenteser och
# MAGIC    blanksteg fungerar inte i Delta utan column mapping, så namnen
# MAGIC    blir `period_raw`, `settled_kwh_raw` och `published_at_raw`.
# MAGIC    Originalnamnen står i den här cellen. Innehållet är orört.
# MAGIC 2. **SCB lagras som textrader.** Filen har en titelrad och en tom
# MAGIC    rad före rubriken, är bred och ligger i ISO-8859-1. Att tolka
# MAGIC    strukturen är ett beslut, och beslut hör hemma i silver. Bronze
# MAGIC    rättar bara teckenkodningen och sparar raderna som de står.
# MAGIC
# MAGIC    Filen läses via CSV-läsaren med en avgränsare som inte finns i
# MAGIC    datan, inte via `spark.read.text()`. Textläsaren tar ingen
# MAGIC    `encoding`-option utan antar alltid UTF-8, och SCB-filen är
# MAGIC    ISO-8859-1.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
LANDING = f"/Volumes/{CATALOG}/bronze/landing"

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mimer: timvärden per elområde och kraftslag
# MAGIC
# MAGIC Elområde, kraftslag och period står bara i filnamnet, inte i
# MAGIC innehållet. De plockas ut här så att varje rad blir självbeskrivande.
# MAGIC
# MAGIC Summa-raden sist i varje fil läses in som vanlig data. Den är en del
# MAGIC av det källan levererade och filtreras bort i silver, inte här.

# COMMAND ----------

mimer_files = spark.read.format("binaryFile").load(f"{LANDING}/mimer/*.csv")
print(f"{mimer_files.count()} filer i landing/mimer")

raw = (
    spark.read
    .option("header", True)
    .option("sep", ";")
    .option("encoding", "UTF-8")
    .option("inferSchema", False)
    .csv(f"{LANDING}/mimer/*.csv")
)

# Tre kolumner plus en tom från det avslutande semikolonet i rubriken.
assert len(raw.columns) == 4, f"Oväntat antal kolumner: {raw.columns}"

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
    "Något filnamn följer inte mönstret SEn_kraftslag_datum_datum.csv"

(mimer_bronze.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("bronze.mimer_production"))

print(f"{spark.table('bronze.mimer_production').count():,} rader skrivna")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manifestet
# MAGIC
# MAGIC Nedladdningsskriptets logg: URL, tidpunkt, byte, SHA-256 och antal
# MAGIC rader per fil. Den gör det möjligt att kontrollera att bronze
# MAGIC innehåller exakt det som hämtades, och den registrerar de
# MAGIC kombinationer som saknar data helt.

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
    .agg(F.count("*").alias("filer"), F.sum("data_rows").alias("rader"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Kontroll: stämmer bronze med det som hämtades?
# MAGIC
# MAGIC Radantalet per fil i bronze ska vara manifestets `data_rows` plus
# MAGIC ett, eftersom Summa-raden räknades bort vid nedladdningen men läses
# MAGIC in här. Avviker någon fil har något gått förlorat på vägen.

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
    raise AssertionError("Bronze stämmer inte med manifestet, se tabellen ovan")
print(f"OK: {check.count()} filer stämmer mot manifestet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCB: månadsvärden per elområde
# MAGIC
# MAGIC Filen är i ISO-8859-1. Läses den som UTF-8 blir "elområde" till
# MAGIC "elomr?de", och felet följer med hela vägen till gold.
# MAGIC
# MAGIC Raderna sparas i den ordning de står i filen. `line_no` behövs för
# MAGIC att silver ska kunna hitta rubrikraden och veta vilka rader som är
# MAGIC data.

# COMMAND ----------

SENTINEL = "\u0001"  # förekommer inte i filen, ger en kolumn per rad

scb_lines = (
    spark.read
    .option("encoding", "ISO-8859-1")
    .option("sep", SENTINEL)
    .option("quote", "\u0000")   # stäng av citattolkning, raden ska in rå
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

print(f"{spark.table('bronze.scb_raw_lines').count():,} rader skrivna")

# Kontroll: teckenkodningen ska vara rättad.
# Ersättningstecken räcker inte som test, eftersom ISO-8859-1 kan avkoda
# vilken bytesekvens som helst utan att klaga. Kontrollera i stället att
# ett ord som måste finnas i filen faktiskt går att hitta.
scb = spark.table("bronze.scb_raw_lines")
assert scb.filter(F.col("line").contains("\ufffd")).count() == 0, \
    "Ersättningstecken i SCB-datan, teckenkodningen är fel"
assert scb.filter(F.col("line").contains("elområde")).count() > 0, \
    "Hittar inte 'elområde' i SCB-datan, teckenkodningen är fel"
print("OK: SCB-datan är läsbar och svenska tecken är intakta")

display(spark.table("bronze.scb_raw_lines").orderBy("line_no").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vad som nu finns
# MAGIC
# MAGIC - `bronze.mimer_production` — timvärden, allt som text
# MAGIC - `bronze.mimer_manifest` — vad som hämtades och när
# MAGIC - `bronze.scb_raw_lines` — SCB-filens rader, rätt teckenkodning
# MAGIC
# MAGIC Inget är normaliserat, inget är borttaget. Silver tar vid.

# COMMAND ----------

display(spark.sql("SHOW TABLES IN bronze"))
