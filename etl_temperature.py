#!/usr/bin/env python3
"""
ETL Spark - Température moyenne par ville et année

Objectif : Calculer la température moyenne annuelle pour chaque ville
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, year, to_date, round, desc, asc
import time

print("=" * 70)
print("ETL SPARK - TEMPÉRATURE MOYENNE PAR VILLE ET ANNÉE")
print("=" * 70)

# ============================================================================
# 1. CONFIGURATION SPARK
# ============================================================================
print("\n📦 Création de la session Spark...")
spark = SparkSession.builder \
    .appName("ETL_Temperature_Par_Ville") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark démarré")
print(f"📊 Spark UI disponible sur : http://localhost:4040\n")

# ============================================================================
# 2. EXTRACT - Charger les données
# ============================================================================
print("=" * 70)
print("PHASE 1 : EXTRACT - Chargement des données")
print("=" * 70)
print("\n📥 Chargement du fichier CSV...")
start = time.time()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("GlobalLandTemperaturesByCity.csv")

nb_lignes = df.count()
extract_time = time.time() - start

print(f"✅ Données chargées en {extract_time:.1f}s")
print(f"📊 {nb_lignes:,} lignes chargées")
print(f"📊 {len(df.columns)} colonnes")
print(f"📊 {df.rdd.getNumPartitions()} partitions Spark")

print("\n📋 Aperçu des données :")
df.show(5, truncate=False)

# ============================================================================
# 3. TRANSFORM - Nettoyer et calculer la moyenne
# ============================================================================
print("\n" + "=" * 70)
print("PHASE 2 : TRANSFORM - Nettoyage et calculs")
print("=" * 70)
print("\n🔧 Transformation des données...")

transform_start = time.time()

# Étape 1 : Convertir la date et extraire l'année
print("\n  Étape 1/3 : Conversion de la date et extraction de l'année...")
df_with_year = df \
    .withColumn("date", to_date(col("dt"), "yyyy-MM-dd")) \
    .withColumn("year", year(col("date")))

# Étape 2 : Supprimer les valeurs nulles
print("  Étape 2/3 : Nettoyage des valeurs nulles...")
avant_nettoyage = df_with_year.count()
df_clean = df_with_year.filter(
    col("AverageTemperature").isNotNull() &
    col("City").isNotNull() &
    col("Country").isNotNull() &
    col("year").isNotNull()
)
apres_nettoyage = df_clean.count()

print(f"    • Lignes avant : {avant_nettoyage:,}")
print(f"    • Lignes après : {apres_nettoyage:,}")
print(f"    • Supprimées : {avant_nettoyage - apres_nettoyage:,} ({(avant_nettoyage - apres_nettoyage)/avant_nettoyage*100:.1f}%)")

# Étape 3 : Calculer la température moyenne par ville et année
print("\n  Étape 3/3 : Calcul de la température moyenne par ville et année...")
temperature_par_ville_annee = df_clean.groupBy("City", "Country", "year") \
    .agg(
        round(avg("AverageTemperature"), 2).alias("temperature_moyenne")
    ) \
    .orderBy("Country", "City", "year")

nb_resultats = temperature_par_ville_annee.count()
transform_time = time.time() - transform_start

print(f"\n✅ Transformation terminée en {transform_time:.1f}s")
print(f"📊 {nb_resultats:,} lignes calculées")

print("\n📋 Aperçu du résultat (10 premières lignes) :")
temperature_par_ville_annee.show(10, truncate=False)

# ============================================================================
# 4. STATISTIQUES
# ============================================================================
print("\n" + "=" * 70)
print("STATISTIQUES DU RÉSULTAT")
print("=" * 70)

nb_villes = temperature_par_ville_annee.select("City", "Country").distinct().count()
nb_annees = temperature_par_ville_annee.select("year").distinct().count()

print(f"\n  • Total de lignes (ville × année) : {nb_resultats:,}")
print(f"  • Nombre de villes uniques : {nb_villes:,}")
print(f"  • Nombre d'années couvertes : {nb_annees}")

# Exemple : Paris
print("\n📍 Exemple : Évolution de la température à Paris")
print("-" * 70)
temperature_par_ville_annee \
    .filter((col("City") == "Paris") & (col("Country") == "France")) \
    .orderBy("year") \
    .show(20)

# Top villes les plus chaudes en 2013
print("\n🔥 Top 10 villes les plus chaudes en 2013")
print("-" * 70)
temperature_par_ville_annee \
    .filter(col("year") == 2013) \
    .orderBy(desc("temperature_moyenne")) \
    .limit(10) \
    .show(truncate=False)

# Top villes les plus froides en 2013
print("\n❄️ Top 10 villes les plus froides en 2013")
print("-" * 70)
temperature_par_ville_annee \
    .filter(col("year") == 2013) \
    .orderBy(asc("temperature_moyenne")) \
    .limit(10) \
    .show(truncate=False)

# ============================================================================
# 5. LOAD - Sauvegarde des résultats
# ============================================================================
print("\n" + "=" * 70)
print("PHASE 3 : LOAD - Sauvegarde des résultats")
print("=" * 70)

import os
os.makedirs("resultats", exist_ok=True)

load_start = time.time()

# 1. Format Parquet (recommandé pour Spark, partitionné par pays)
print("\n1️⃣ Sauvegarde en Parquet (partitionné par pays)...")
temperature_par_ville_annee.write \
    .mode("overwrite") \
    .partitionBy("Country") \
    .parquet("resultats/temperature_ville_annee.parquet")
print("   ✅ OK")

# 2. Format CSV (pour Excel)
print("\n2️⃣ Sauvegarde en CSV...")
temperature_par_ville_annee.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("resultats/temperature_ville_annee_csv")
print("   ✅ OK")

# 3. Format JSON (pour APIs)
print("\n3️⃣ Sauvegarde en JSON...")
temperature_par_ville_annee.write \
    .mode("overwrite") \
    .json("resultats/temperature_ville_annee_json")
print("   ✅ OK")

load_time = time.time() - load_start

print(f"\n✅ Sauvegarde terminée en {load_time:.1f}s")

# ============================================================================
# 6. RÉSUMÉ FINAL
# ============================================================================
total_time = extract_time + transform_time + load_time

print("\n" + "=" * 70)
print("📊 RÉSUMÉ FINAL DE L'ETL")
print("=" * 70)
print(f"\n✅ Pipeline ETL terminé avec succès !\n")
print(f"⏱️  Temps d'exécution :")
print(f"   • Extract  : {extract_time:.1f}s")
print(f"   • Transform: {transform_time:.1f}s")
print(f"   • Load     : {load_time:.1f}s")
print(f"   • TOTAL    : {total_time:.1f}s")
print(f"\n📊 Données :")
print(f"   • Lignes traitées : {apres_nettoyage:,}")
print(f"   • Résultats générés : {nb_resultats:,} (ville × année)")
print(f"   • Villes analysées : {nb_villes:,}")
print(f"   • Années couvertes : {nb_annees}")
print(f"\n💾 Résultats sauvegardés dans : resultats/")
print(f"   • Parquet : resultats/temperature_ville_annee.parquet/")
print(f"   • CSV     : resultats/temperature_ville_annee_csv/")
print(f"   • JSON    : resultats/temperature_ville_annee_json/")
print(f"\n📊 Spark UI : http://localhost:4040")
print("=" * 70)

# Fermer Spark
spark.stop()
print("\n✅ Session Spark arrêtée")
