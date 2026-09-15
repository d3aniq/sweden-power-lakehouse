# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: gemensamt schema, validerat
# MAGIC
# MAGIC Här löses skillnaderna i `docs/kallskillnader.md`, en i taget:
# MAGIC teckenkodning (gjord i bronze), bred form, olika områdeskoder, olika
# MAGIC enheter, decimalkomma, kategorier som inte motsvarar varandra, och
# MAGIC noll som betyder två olika saker.
# MAGIC
# MAGIC Resultatet är två tabeller i samma vokabulär: `silver.production_hourly`
# MAGIC från Mimer och `silver.production_monthly` från SCB.
# MAGIC
# MAGIC Kvalitetsgrindarna sist i notebooken stoppar körningen om något inte
# MAGIC håller. Ett jobb som failar högljutt är bättre än en gold-tabell som
# MAGIC tyst är fel.

# COMMAND ----------

import csv
import io

from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = "workspace"
spark.sql(f"USE CATALOG {CATALOG}")

# Gemensam period: Mimer slutar 2025-03-17, SCB börjar 2021-01.
PERIOD_START = "2021-01-01"
PERIOD_END = "2025-02-28"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mappningstabellen
# MAGIC
# MAGIC Källorna kallar samma sak olika, och de täcker inte riktigt samma
# MAGIC saker. Mappningen ligger i en tabell i stället för i if-satser, så
# MAGIC att den går att läsa, ändra och granska utan att någon rör koden.
# MAGIC
# MAGIC Två detaljer att lägga märke till:
# MAGIC
# MAGIC - SCB:s kategorinamn har blanksteg på slutet och dubbla mellanslag
# MAGIC   inuti. Både trimning och hopslagning av inre blanksteg görs på
# MAGIC   båda sidor av joinen, annars matchar ingenting.
# MAGIC - Mimers *uppmätt ospecificerad produktion* har ingen motsvarighet
# MAGIC   hos SCB, och SCB:s *konventionell värmekraft* inkluderar
# MAGIC   dieselkraftverk. Kategorierna är alltså inte identiska, bara
# MAGIC   närmast jämförbara. Det står i `comparable`-kolumnen.

# COMMAND ----------

mapping_rows = [
    # (källa, kategori i källan, gemensam kod, mått, jämförbar mellan källor)
    ("mimer", "vattenkraft",            "hydro",       "production", True),
    ("mimer", "vindkraft",              "wind",        "production", True),
    ("mimer", "solkraft",               "solar",       "production", True),
    ("mimer", "karnkraft",              "nuclear",     "production", True),
    ("mimer", "ovrig_varmekraft",       "thermal",     "production", True),
    ("mimer", "uppmatt_ospecificerad",  "unspecified", "production", False),

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
    # Samma normalisering som på källsidan, så att båda behandlas lika.
    .withColumn("source_category", F.regexp_replace(F.trim("source_category"), r"\s+", " "))
)

(mapping.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("silver.category_mapping"))

display(mapping)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mimer: timvärden
# MAGIC
# MAGIC Fyra saker händer: Summa-raden bort, decimalkomma till punkt, kWh
# MAGIC till MWh, och dubbletter bort.
# MAGIC
# MAGIC Dedupliceringen behåller den senast publicerade versionen av varje
# MAGIC timme. Mimer kan publicera om ett värde, och då finns publicerings-
# MAGIC tidpunkten kvar som bevis på vilken version som användes.

# COMMAND ----------

raw = spark.table("bronze.mimer_production")

parsed = (
    raw
    # Summa-raden är källans egen aggregering, inte en observation.
    .filter(F.col("period_raw") != "Summa")
    .withColumn("ts", F.to_timestamp("period_raw", "yyyy-MM-dd HH:mm"))
    .withColumn("published_at", F.to_timestamp("published_at_raw", "yyyy-MM-dd HH:mm"))
    # Decimalkomma. Tolkas kommat som tusentalsavgränsare blir värdet
    # tusen gånger för stort, se docs/kallskillnader.md punkt 7.
    .withColumn("value_kwh", F.regexp_replace("settled_kwh_raw", ",", ".").cast("decimal(20,3)"))
    .withColumn("value_mwh", (F.col("value_kwh") / F.lit(1000)).cast("decimal(20,6)"))
)

# Fånga rader som inte gick att tolka innan de försvinner tyst.
unparsed = parsed.filter(F.col("ts").isNull() | F.col("value_kwh").isNull())
if unparsed.count() > 0:
    display(unparsed.select("_source_file", "period_raw", "settled_kwh_raw").limit(20))
    raise AssertionError(f"{unparsed.count()} rader gick inte att tolka, se ovan")

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
    "Kraftslag saknas i mappningstabellen"

(hourly.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .partitionBy("area")
    .saveAsTable("silver.production_hourly"))

print(f"{spark.table('silver.production_hourly').count():,} timvärden")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCB: från bred till lång
# MAGIC
# MAGIC Rubrikraden läses ut först, eftersom antalet månadskolumner växer
# MAGIC varje gång tabellen uppdateras. Att hårdkoda det hade gjort
# MAGIC notebooken felaktig vid nästa hämtning.
# MAGIC
# MAGIC Dataraderna känns igen på att andra fältet är SE1–SE4. Titelraden
# MAGIC och rubrikraden faller bort av sig själva, utan att koden behöver
# MAGIC lita på radnummer.

# COMMAND ----------

lines = spark.table("bronze.scb_raw_lines")

# Titelraden innehåller också ordet "elområde", så ordet ensamt duger
# inte som kännetecken. Rubrikraden är den som har minst tre fält och
# där andra fältet är precis "elområde".
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
        print("kandidat:", candidate[:120])
    raise AssertionError(
        "Hittar ingen rubrikrad i bronze.scb_raw_lines. Kandidaterna står ovan. "
        "Är listan tom eller ser tecknen fel ut, kör om 01_bronze."
    )

months = columns[2:]
assert months, "Rubrikraden saknar månadskolumner"
print(f"{len(months)} månadskolumner: {months[0]} till {months[-1]}")

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
        # Kategorinamnen har både blanksteg på slutet och dubbla
        # mellanslag inuti ("el-, gas-,  värme- och vattenverk ").
        # Trimning ensam räcker inte, inre blanksteg måste också slås ihop.
        F.regexp_replace(F.trim(F.col("r.c0")), r"\s+", " ").alias("source_category"),
        F.col("r.c1").alias("area"),
        F.col("_source_file").alias("source_file"),
        F.explode(pairs).alias("p"),
    )
    .select("source_category", "area", "source_file", "p.month_code", "p.value_raw")
    # 2021M01 -> 2021-01-01. F.concat, inte +, som betyder aritmetik i PySpark.
    .withColumn(
        "month",
        F.to_date(F.concat(F.regexp_replace("month_code", "M", "-"), F.lit("-01"))),
    )
    # GWh till MWh.
    .withColumn("value_mwh", (F.col("value_raw").cast("decimal(20,3)") * F.lit(1000)).cast("decimal(20,6)"))
)

bad_months = long.filter(F.col("month").isNull()).select("month_code").distinct()
if bad_months.count() > 0:
    display(bad_months)
    raise AssertionError("Månadskoder som inte gick att tolka, se ovan")

unmapped = (
    long.join(mapping.filter(F.col("source") == "scb"), "source_category", "left_anti")
    .select("source_category").distinct()
)
if unmapped.count() > 0:
    display(unmapped)
    raise AssertionError("SCB-kategorier som saknas i mappningstabellen, se ovan")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Noll som betyder två olika saker
# MAGIC
# MAGIC SCB skriver 0 för kärnkraft i SE1, SE2 och SE4. Det är inte ett
# MAGIC uppmätt nollvärde utan "kategorin finns inte i det elområdet".
# MAGIC Mimer svarade med en tom fil för samma kombinationer, och det står
# MAGIC registrerat i manifestet med status `no_data`.
# MAGIC
# MAGIC Manifestet används därför som facit: kombinationer som Mimer inte
# MAGIC har data för märks `not_applicable` i stället för `reported`. Skrivs
# MAGIC båda som 0 blir varje medelvärde över elområden fel.
# MAGIC
# MAGIC Kombinationer som ännu inte hämtats från Mimer märks `unknown`.
# MAGIC Ärligare än att gissa.

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

print(f"{spark.table('silver.production_monthly').count():,} månadsvärden")
display(
    spark.table("silver.production_monthly")
    .groupBy("measure", "availability").count().orderBy("measure", "availability")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kvalitetsgrindar
# MAGIC
# MAGIC Fem kontroller. Faller någon stannar jobbet här, och gold byggs
# MAGIC aldrig på data som inte håller.

# COMMAND ----------

failures = []

h = spark.table("silver.production_hourly")
m = spark.table("silver.production_monthly")

# 1. Inga null i nycklarna.
for table, name, keys in [
    (h, "production_hourly", ["area", "category", "ts", "value_mwh"]),
    (m, "production_monthly", ["area", "category", "month"]),
]:
    for key in keys:
        n = table.filter(F.col(key).isNull()).count()
        if n:
            failures.append(f"{name}: {n} rader med null i {key}")

# 2. Unikhet. En observation per källa, område, kategori och tidpunkt.
dupes = h.groupBy("area", "category", "ts").count().filter(F.col("count") > 1)
if dupes.count():
    failures.append(f"production_hourly: {dupes.count()} dubbletter")

# 3. Dygn med fel antal timmar. Sommartid ger 23 eller 25 om
#    tidsstämplarna är lokal tid; 24 överallt talar för UTC.
odd_days = (
    h.groupBy("area", "category", "date").agg(F.count("*").alias("hours"))
    .filter(F.col("hours") != 24)
)
odd_count = odd_days.count()
if odd_count:
    print(f"Dygn med annat än 24 timmar: {odd_count}")
    display(odd_days.orderBy("date").limit(20))
else:
    print("Alla dygn har 24 timmar. Tidsstämplarna är inte lokal tid med sommartid.")

# 4. Negativa värden. Vattenkraft med pumpkraft kan vara negativ netto,
#    övriga kraftslag kan det inte.
neg = h.filter((F.col("value_mwh") < 0) & (F.col("category") != "hydro"))
if neg.count():
    display(neg.limit(20))
    failures.append(f"production_hourly: {neg.count()} negativa värden utanför vattenkraft")

# 5. Datum inom förväntat intervall.
outside = h.filter((F.col("date") < PERIOD_START) | (F.col("date") > PERIOD_END))
if outside.count():
    failures.append(f"production_hourly: {outside.count()} rader utanför {PERIOD_START}–{PERIOD_END}")

if failures:
    for f_ in failures:
        print("FEL:", f_)
    raise AssertionError(f"{len(failures)} kvalitetskontroller föll")
print("Alla kvalitetskontroller passerade")

# COMMAND ----------

display(spark.sql("SHOW TABLES IN silver"))
