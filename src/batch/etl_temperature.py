#!/usr/bin/env python3
"""
Module Batch - ETL Spark pour les données de température

Ce module gère le traitement batch des données historiques de température.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, year, to_date, round, lit
import time
import os
import sys

# Ajouter le chemin parent pour importer la config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.config import PATHS, SPARK_CONFIG


class TemperatureETL:
    """Classe pour le traitement ETL des données de température"""
    
    def __init__(self, input_file=None):
        self.input_file = input_file or PATHS['csv_source']
        self.spark = None
        self.df_raw = None
        self.df_result = None
        
    def _create_spark_session(self):
        """Crée et configure la session Spark"""
        print("📦 Création de la session Spark...")
        self.spark = SparkSession.builder \
            .appName(f"{SPARK_CONFIG['app_name']}_Batch") \
            .config("spark.driver.memory", SPARK_CONFIG['driver_memory']) \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel(SPARK_CONFIG['log_level'])
        print("✅ Spark démarré")
        print(f"📊 Spark UI disponible sur : http://localhost:4040\n")
        
    def extract(self):
        """EXTRACT - Charge les données depuis le fichier CSV"""
        print("=" * 70)
        print("PHASE 1 : EXTRACT - Chargement des données")
        print("=" * 70)
        
        start = time.time()
        self.df_raw = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(self.input_file)
        
        nb_lignes = self.df_raw.count()
        extract_time = time.time() - start
        
        print(f"✅ Données chargées en {extract_time:.1f}s")
        print(f"📊 {nb_lignes:,} lignes | {len(self.df_raw.columns)} colonnes | {self.df_raw.rdd.getNumPartitions()} partitions")
        print("\n📋 Aperçu des données :")
        self.df_raw.show(5, truncate=False)
        
        return self
        
    def transform(self):
        """TRANSFORM - Nettoie et calcule la température moyenne par ville et année"""
        print("\n" + "=" * 70)
        print("PHASE 2 : TRANSFORM - Nettoyage et calculs")
        print("=" * 70)
        
        start = time.time()
        
        # Étape 1: Convertir la date et extraire l'année
        print("\n  1️⃣ Conversion de la date et extraction de l'année...")
        df_with_year = self.df_raw \
            .withColumn("date", to_date(col("dt"), "yyyy-MM-dd")) \
            .withColumn("year", year(col("date")))
        
        # Étape 2: Supprimer les valeurs nulles
        print("  2️⃣ Nettoyage des valeurs nulles...")
        avant = df_with_year.count()
        df_clean = df_with_year.filter(
            col("AverageTemperature").isNotNull() &
            col("City").isNotNull() &
            col("Country").isNotNull() &
            col("year").isNotNull()
        )
        apres = df_clean.count()
        print(f"      • Lignes: {avant:,} → {apres:,} ({avant - apres:,} supprimées)")
        
        # Étape 3: Calculer la moyenne par ville et année
        print("  3️⃣ Calcul de la température moyenne par ville et année...")
        self.df_result = df_clean.groupBy("City", "Country", "year") \
            .agg(round(avg("AverageTemperature"), 2).alias("temperature_moyenne")) \
            .withColumn("source", lit("batch")) \
            .orderBy("Country", "City", "year")
        
        print(f"\n✅ Transformation terminée en {time.time() - start:.1f}s")
        print(f"📊 {self.df_result.count():,} lignes calculées")
        print("\n📋 Aperçu du résultat :")
        self.df_result.show(10, truncate=False)
        
        return self
        
    def load(self, output_path=None):
        """LOAD - Sauvegarde les résultats dans le parquet unifié"""
        output_path = output_path or PATHS['results']
        
        print("\n" + "=" * 70)
        print("PHASE 3 : LOAD - Sauvegarde des résultats")
        print("=" * 70)
        
        os.makedirs(output_path, exist_ok=True)
        start = time.time()
        
        parquet_path = f"{output_path}/temperatures.parquet"
        print(f"\n  💾 Sauvegarde en Parquet unifié...")
        
        # Écrire en mode append pour fusionner avec streaming
        self.df_result.write.mode("append").parquet(parquet_path)
        
        print(f"      ✅ {parquet_path}")
        print(f"\n✅ Sauvegarde terminée en {time.time() - start:.1f}s")
        return self
        
    def run(self, output_path=None):
        """Exécute le pipeline ETL complet"""
        print("=" * 70)
        print("ETL SPARK - TEMPÉRATURE MOYENNE PAR VILLE ET ANNÉE")
        print("=" * 70)
        
        self._create_spark_session()
        self.extract().transform().load(output_path)
        
        print("\n" + "=" * 70)
        print("✅ PIPELINE ETL TERMINÉ AVEC SUCCÈS")
        print("=" * 70)
        
        return self
        
    def stop(self):
        """Arrête la session Spark"""
        if self.spark:
            self.spark.stop()
            print("🛑 Session Spark arrêtée")


def main():
    """Point d'entrée principal"""
    etl = TemperatureETL()
    etl.run()
    
    input("\n⏸️  Appuyez sur Entrée pour arrêter Spark...")
    etl.stop()


if __name__ == "__main__":
    main()
