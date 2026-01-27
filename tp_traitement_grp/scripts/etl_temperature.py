from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    year,
    avg,
    desc,
    floor,
    stddev,
    min,
    max
)

# =========================================================
# 1. Spark Session
# =========================================================
spark = SparkSession.builder \
    .appName("ETL Global Land Temperatures") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("=== Spark démarré ===")

# =========================================================
# 2. Chargement CSV (BRONZE)
# =========================================================
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/raw/GlobalLandTemperaturesByCity.csv")

print("Lignes chargées :", df_raw.count())

# =========================================================
# 3. Nettoyage des données (SILVER)
# =========================================================
df_clean = (
    df_raw
    .filter(col("AverageTemperature").isNotNull())
    .withColumn("dt", to_date("dt"))
    .withColumn("year", year("dt"))
)

# Cache pour performance
df_clean.cache()
df_clean.count()
print("Nettoyage terminé")

# =========================================================
# 4. Enrichissement (année → décennie)
# =========================================================
df_enriched = (
    df_clean
    .withColumn("decade", floor(col("year") / 10) * 10)
)

# Repartition stratégique
df_repart = df_enriched.repartition("Country")

# =========================================================
# 5. AGRÉGATIONS EXISTANTES
# =========================================================

# 🔹 Température moyenne par pays et par année
avg_temp_country_year = (
    df_repart
    .groupBy("Country", "year")
    .agg(avg("AverageTemperature").alias("avg_temperature"))
)

avg_temp_country_year.cache()
avg_temp_country_year.count()

# 🔹 Température moyenne par ville (globale)
avg_temp_city = (
    df_repart
    .groupBy("Country", "City")
    .agg(avg("AverageTemperature").alias("avg_temperature"))
)

print("Agrégations existantes terminées")

# =========================================================
# 6. NOUVELLES AGRÉGATIONS CLIMATIQUES
# =========================================================

# 🔥 1. Top 10 des villes les plus chaudes (historique)
top_10_hottest_cities = (
    avg_temp_city
    .orderBy(desc("avg_temperature"))
    .limit(10)
)

# 📈 2. Évolution annuelle globale
global_yearly_trend = (
    df_repart
    .groupBy("year")
    .agg(avg("AverageTemperature").alias("global_avg_temperature"))
    .orderBy("year")
)

# 🌍 3. Température moyenne par pays (globale)
avg_temp_by_country = (
    df_repart
    .groupBy("Country")
    .agg(avg("AverageTemperature").alias("avg_temperature"))
    .orderBy(desc("avg_temperature"))
)

# 🏙️ 4. Évolution annuelle par ville
avg_temp_city_year = (
    df_repart
    .groupBy("Country", "City", "year")
    .agg(avg("AverageTemperature").alias("avg_temperature"))
)

# 📆 5. Température moyenne par décennie
avg_temp_by_decade = (
    df_repart
    .groupBy("decade")
    .agg(avg("AverageTemperature").alias("avg_temperature"))
    .orderBy("decade")
)

# 🌪️ 6. Variabilité climatique (instabilité) par ville
temp_variability_city = (
    df_repart
    .groupBy("Country", "City")
    .agg(stddev("AverageTemperature").alias("temp_variability"))
    .orderBy(desc("temp_variability"))
)

# 🚀 7. Tendance de réchauffement (delta température)
temp_trend_city = (
    df_repart
    .groupBy("Country", "City")
    .agg(
        (max("AverageTemperature") - min("AverageTemperature"))
        .alias("temp_delta")
    )
    .orderBy(desc("temp_delta"))
)

print("Nouvelles agrégations climatiques terminées")

# =========================================================
# 7. Sauvegarde des résultats (CURATED)
# =========================================================

avg_temp_country_year.write \
    .mode("overwrite") \
    .parquet("data/curated/avg_temp_country_year")

avg_temp_city.write \
    .mode("overwrite") \
    .json("data/curated/avg_temp_city")

top_10_hottest_cities.write \
    .mode("overwrite") \
    .json("data/curated/top_10_hottest_cities")

global_yearly_trend.write \
    .mode("overwrite") \
    .parquet("data/curated/global_yearly_trend")

avg_temp_by_country.write \
    .mode("overwrite") \
    .parquet("data/curated/avg_temp_by_country")

avg_temp_city_year.write \
    .mode("overwrite") \
    .parquet("data/curated/avg_temp_city_year")

avg_temp_by_decade.write \
    .mode("overwrite") \
    .parquet("data/curated/avg_temp_by_decade")

temp_variability_city.write \
    .mode("overwrite") \
    .parquet("data/curated/temp_variability_city")

temp_trend_city.write \
    .mode("overwrite") \
    .parquet("data/curated/temp_trend_city")

print("Résultats sauvegardés")

# =========================================================
# 8. Fin
# =========================================================
spark.stop()
print("=== Pipeline climatique terminé ===")