"""
ETL Spark - Stations météorologiques

Objectif : Charger le fichier GHCND stations, nettoyer et analyser l'altitude (Elevation) par ville
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, desc, asc
import time
import os

print("=" * 70)
print("ETL SPARK - ANALYSE DES STATIONS MÉTÉO")
print("=" * 70)

# ============================================================================
# 1. CONFIGURATION SPARK
# ============================================================================
print("\n Création de la session Spark...")
spark = SparkSession.builder \
    .appName("ETL_Stations_Meteo") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(" Spark démarré")
print(f"📊 Spark UI disponible sur : http://localhost:4040\n")

# ============================================================================
# 2. EXTRACT - Charger les données
# ============================================================================
print("=" * 70)
print("PHASE 1 : EXTRACT - Chargement des données")
print("=" * 70)
print("\n Chargement du fichier CSV sans header...")
start = time.time()

# Définition des noms de colonnes
columns = ["Station", "Latitude", "Longitude", "Elevation", "Col5", "City", "Col7", "Col8", "Col9"]

# Lecture du CSV sans header
df = spark.read.csv(
    "ghcnd-stations.csv",
    header=False,
    inferSchema=True
)

# Renommer les colonnes
df = df.toDF(*columns)

nb_lignes = df.count()
extract_time = time.time() - start

print(f" Données chargées en {extract_time:.1f}s")
print(f"📊 {nb_lignes:,} lignes chargées")
print(f"📊 {len(df.columns)} colonnes")
print(f"📊 {df.rdd.getNumPartitions()} partitions Spark")

print("\n Aperçu des données :")
df.show(5, truncate=False)
df.printSchema()

# ============================================================================
# 3. TRANSFORM - Nettoyer et analyser
# ============================================================================
print("\n" + "=" * 70)
print("PHASE 2 : TRANSFORM - Nettoyage et analyse")
print("=" * 70)
print("\n Filtrage des lignes avec City et Elevation non nulles...")

transform_start = time.time()

df_clean = df.filter(col("City").isNotNull() & col("Elevation").isNotNull())

# Exemple : afficher 20 lignes pour DUBAI
print("\n Exemple : Elevation des stations à DUBAI")
df_clean.filter(df_clean.City.contains("DUBAI")) \
        .select("City", "Elevation") \
        .show(20, truncate=False)

# Top 10 villes avec la plus grande moyenne d'altitude
print("\n Top 10 villes avec la plus grande altitude moyenne")
df_clean.groupBy("City") \
        .agg(avg("Elevation").alias("AvgElevation")) \
        .orderBy(desc("AvgElevation")) \
        .show(10, truncate=False)

# 10 villes distinctes pour contrôle
print("\n Exemple : 10 villes distinctes")
df_clean.select("City").distinct().show(10, truncate=False)

transform_time = time.time() - transform_start
print(f"\n Transformation terminée en {transform_time:.1f}s")
print(f"📊 Lignes après nettoyage : {df_clean.count():,}")

# ============================================================================
# 4. LOAD - Sauvegarde des résultats
# ============================================================================
print("\n" + "=" * 70)
print("PHASE 3 : LOAD - Sauvegarde des résultats")
print("=" * 70)

os.makedirs("resultats", exist_ok=True)
load_start = time.time()

# 1. Format Parquet
print("\n Sauvegarde en Parquet...")
df_clean.write.mode("overwrite").parquet("resultats/stations_clean.parquet")
print("    OK")

# 2. Format CSV
print("\n Sauvegarde en CSV...")
df_clean.coalesce(1).write.mode("overwrite").option("header", "true").csv("resultats/stations_clean_csv")
print("    OK")

# 3. Format JSON
print("\n Sauvegarde en JSON...")
df_clean.write.mode("overwrite").json("resultats/stations_clean_json")
print("    OK")

load_time = time.time() - load_start
total_time = extract_time + transform_time + load_time

# ============================================================================
# 5. RÉSUMÉ FINAL
# ============================================================================
print("\n" + "=" * 70)
print(" RÉSUMÉ FINAL DE L'ETL")
print("=" * 70)

print(f"\n  Temps d'exécution :")
print(f"   • Extract  : {extract_time:.1f}s")
print(f"   • Transform: {transform_time:.1f}s")
print(f"   • Load     : {load_time:.1f}s")
print(f"   • TOTAL    : {total_time:.1f}s")
print(f"\n Données :")
print(f"   • Lignes initiales : {nb_lignes:,}")
print(f"   • Lignes nettoyées : {df_clean.count():,}")
print("\n Résultats sauvegardés dans : resultats/")
print(f"   • Parquet : resultats/stations_clean.parquet/")
print(f"   • CSV     : resultats/stations_clean_csv/")
print(f"   • JSON    : resultats/stations_clean_json/")
print(f"\n Spark UI : http://localhost:4040")
print("=" * 70)

# Fermer Spark
spark.stop()
print("\n Session Spark arrêtée")

