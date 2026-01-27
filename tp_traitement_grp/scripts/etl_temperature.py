from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    year,
    avg
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
# 2. Chargement CSV
# =========================================================
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/raw/GlobalLandTemperaturesByCity.csv")

print("Lignes chargées :", df_raw.count())

# =========================================================
# 3. Nettoyage des données
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
# 4. Repartition stratégique
# =========================================================
df_repart = df_clean.repartition("Country")

# =========================================================
# 5. Agrégations climatiques (EXISTANTES)
# =========================================================

# Température moyenne par pays et par année
avg_temp_country_year = (
    df_repart
    .groupBy("Country", "year")
    .agg(avg("AverageTemperature").alias("avg_temperature"))
)

avg_temp_country_year.cache()
avg_temp_country_year.count()

# Température moyenne par ville
avg_temp_city = (
    df_repart
    .groupBy("Country", "City")
    .agg(avg("AverageTemperature").alias("avg_temperature"))
)

print("Agrégations existantes terminées")

# =========================================================
# 6. NOUVELLES AGRÉGATIONS
# =========================================================

# 🔹 Top 10 des villes les plus chaudes (toute période)
top_10_hottest_cities = (
    avg_temp_city
    .orderBy(col("avg_temperature").desc())
    .limit(10)
)

# 🔹 Évolution annuelle globale (toutes villes confondues)
global_yearly_trend = (
    df_repart
    .groupBy("year")
    .agg(avg("AverageTemperature").alias("global_avg_temperature"))
    .orderBy("year")
)

print("Nouvelles agrégations climatiques terminées")

# =========================================================
# 7. Sauvegarde des résultats
# =========================================================

# Ancien résultat
avg_temp_country_year.write \
    .mode("overwrite") \
    .parquet("data/curated/avg_temp_country_year")

# Ancien résultat
avg_temp_city.write \
    .mode("overwrite") \
    .json("output/avg_temp_city_json")

# 🔹 NOUVEAUX résultats
top_10_hottest_cities.write \
    .mode("overwrite") \
    .json("data/curated/top_10_hottest_cities")

global_yearly_trend.write \
    .mode("overwrite") \
    .parquet("data/curated/global_yearly_trend")

print("Résultats sauvegardés")

# =========================================================
# 8. Fin
# =========================================================
spark.stop()
print("=== Pipeline climatique terminé ===")
