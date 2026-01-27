"""
ETL Spark - Température et stations météorologiques

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, upper, regexp_replace, trim
import time
import os

print("=" * 70)
print("ETL SPARK - Température et Stations")
print("=" * 70)

# ============================================================================
# 1. CONFIGURATION SPARK
# ============================================================================
print("\n📦 Création de la session Spark...")
spark = SparkSession.builder \
    .appName("ETL_Temperature_Stations") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark démarré\n")

# ============================================================================
# 2. EXTRACT - Chargement des données
# ============================================================================
print("=" * 70)
print("PHASE 1 : EXTRACT - Chargement des données")
print("=" * 70)

start = time.time()

# --- Fichier GlobalLandTemperaturesByCity.csv ---
print("\n📥 Chargement de GlobalLandTemperaturesByCity.csv...")
df = spark.read.option("header", "true").option("inferSchema", "true") \
               .csv("GlobalLandTemperaturesByCity.csv")
print(f"✅ {df.count():,} lignes chargées, {len(df.columns)} colonnes")
df.show(5, truncate=False)

# --- Fichier ghcnd-stations.csv ---
print("\n📥 Chargement de ghcnd-stations.csv...")
columns = ["Station", "Latitude", "Longitude", "Elevation", "Col5", "City", "Col7", "Col8", "Col9"]
dg = spark.read.csv("ghcnd-stations.csv", header=False, inferSchema=True).toDF(*columns)
print(f"✅ {dg.count():,} lignes chargées, {len(dg.columns)} colonnes")
dg.show(5, truncate=False)

extract_time = time.time() - start
print(f"\n📊 Données chargées en {extract_time:.1f}s")

# ============================================================================
# 3. TRANSFORM - Nettoyage et normalisation
# ============================================================================
print("\n" + "=" * 70)
print("PHASE 2 : TRANSFORM - Nettoyage et normalisation")
print("=" * 70)

transform_start = time.time()

# --- Nettoyage df ---
df_clean = df.filter(col("City").isNotNull() & col("AverageTemperature").isNotNull()) \
             .withColumn("City_clean", upper(trim(col("City"))))

# --- Nettoyage dg ---
# Supprimer les suffixes courants pour harmoniser les noms de villes
dg_clean = dg.filter(col("City").isNotNull() & col("Elevation").isNotNull()) \
             .withColumn("City_clean", upper(trim(regexp_replace(col("City"), r'\s+(INTL|AIRP|OBS|FLD|FIELD|STATION|OBSY).*$', ''))))

# --- Requêtes combinées ---
print("\n🔹 Villes communes aux deux fichiers :")
common_cities = df_clean.select("City_clean").distinct() \
                        .join(dg_clean.select("City_clean").distinct(), "City_clean", "inner")
common_cities.show(20, truncate=False)

print("\n🔹 Température moyenne pour les villes avec station connue :")
cities_with_station = dg_clean.select("City_clean").distinct()
df_clean.join(cities_with_station, "City_clean", "inner") \
        .groupBy("City_clean").agg(avg("AverageTemperature").alias("AvgTemp")) \
        .orderBy(col("AvgTemp").desc()).show(10, truncate=False)

print("\n🔹 Villes tropicales (Latitude entre -23.5 et 23.5) avec température moyenne :")
tropical_cities = dg_clean.filter(col("Latitude").between(-23.5, 23.5)).select("City_clean").distinct()
df_clean.join(tropical_cities, "City_clean", "inner") \
        .groupBy("City_clean").agg(avg("AverageTemperature").alias("AvgTemp")) \
        .orderBy(col("AvgTemp").desc()).show(10, truncate=False)

print("\n🔹 Élévation moyenne des stations par ville :")
dg_clean.groupBy("City_clean").agg(avg("Elevation").alias("AvgElevation")) \
        .orderBy(col("AvgElevation").desc()).show(10, truncate=False)

transform_time = time.time() - transform_start
print(f"\n✅ Transformation terminée en {transform_time:.1f}s")

# ============================================================================
# 4. LOAD - Sauvegarde des résultats
# ============================================================================
print("\n" + "=" * 70)
print("PHASE 3 : LOAD - Sauvegarde des résultats")
print("=" * 70)

os.makedirs("resultats", exist_ok=True)

common_cities.write.mode("overwrite").csv("resultats/common_cities_csv", header=True)
print("✅ Villes communes sauvegardées dans resultats/common_cities_csv/")

df_clean.join(cities_with_station, "City_clean", "inner") \
        .groupBy("City_clean").agg(avg("AverageTemperature").alias("AvgTemp")) \
        .write.mode("overwrite").csv("resultats/avg_temp_stations", header=True)
print("✅ Température moyenne par ville sauvegardée dans resultats/avg_temp_stations/")

dg_clean.groupBy("City_clean").agg(avg("Elevation").alias("AvgElevation")) \
        .write.mode("overwrite").csv("resultats/avg_elevation_stations", header=True)
print("✅ Élévation moyenne par ville sauvegardée dans resultats/avg_elevation_stations/")

total_time = extract_time + transform_time
print(f"\n📊 Temps total d'exécution : {total_time:.1f}s")

# Fermer Spark
spark.stop()
print("\n✅ Session Spark arrêtée")
