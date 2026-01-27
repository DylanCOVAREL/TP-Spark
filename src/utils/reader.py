#!/usr/bin/env python3
"""
Utilitaire pour lire et analyser les résultats Parquet
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, count, desc, asc
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.config import PATHS, SPARK_CONFIG


class ParquetReader:
    """Lecteur et analyseur de fichiers Parquet"""
    
    def __init__(self):
        self.spark = None
        self.df = None
        
    def _create_spark_session(self):
        """Crée la session Spark"""
        self.spark = SparkSession.builder \
            .appName(f"{SPARK_CONFIG['app_name']}_Reader") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
    def load(self, path):
        """Charge un fichier ou dossier Parquet"""
        if not self.spark:
            self._create_spark_session()
            
        print(f"📥 Chargement: {path}")
        self.df = self.spark.read.parquet(path)
        print(f"✅ {self.df.count():,} lignes chargées")
        return self
        
    def show_schema(self):
        """Affiche le schéma des données"""
        print("\n📋 Schéma:")
        self.df.printSchema()
        return self
        
    def show_stats(self):
        """Affiche les statistiques générales"""
        print("\n📈 Statistiques:")
        self.df.describe().show()
        return self
        
    def show_sample(self, n=10):
        """Affiche un échantillon des données"""
        print(f"\n📊 Aperçu ({n} lignes):")
        self.df.show(n, truncate=False)
        return self
        
    def query(self, sql):
        """Exécute une requête SQL"""
        self.df.createOrReplaceTempView("data")
        result = self.spark.sql(sql)
        result.show(truncate=False)
        return result
        
    def stop(self):
        """Arrête Spark"""
        if self.spark:
            self.spark.stop()


def read_batch_results():
    """Lit les résultats du traitement batch"""
    print("=" * 70)
    print("LECTURE DES RÉSULTATS BATCH")
    print("=" * 70)
    
    reader = ParquetReader()
    path = f"{PATHS['data_output']}/temperature_ville_annee.parquet"
    
    if not os.path.exists(path):
        # Fallback vers l'ancien chemin
        path = "resultats/temperature_ville_annee.parquet"
        
    reader.load(path).show_schema().show_sample()
    
    # Quelques analyses
    print("\n🔍 Top 5 villes les plus chaudes (2013):")
    reader.df.filter(col("year") == 2013) \
        .orderBy(desc("temperature_moyenne")) \
        .select("City", "Country", "temperature_moyenne") \
        .show(5)
        
    print("\n🔍 Évolution Paris (dernières années):")
    reader.df.filter((col("City") == "Paris") & (col("Country") == "France")) \
        .orderBy(desc("year")) \
        .show(10)
        
    reader.stop()


def read_streaming_results():
    """Lit les résultats du streaming"""
    print("=" * 70)
    print("LECTURE DES RÉSULTATS STREAMING")
    print("=" * 70)
    
    reader = ParquetReader()
    path = PATHS['streaming_output']
    
    if not os.path.exists(path):
        path = "resultats/streaming_meteo"
        
    if not os.path.exists(path):
        print("❌ Aucun résultat de streaming trouvé")
        return
        
    reader.load(path).show_schema().show_sample()
    
    # Stats par ville
    print("\n📈 Statistiques par ville:")
    reader.df.groupBy("city") \
        .agg(
            count("*").alias("nb_mesures"),
            avg("temperature_celsius").alias("temp_moy"),
            max("temperature_celsius").alias("temp_max"),
            min("temperature_celsius").alias("temp_min")
        ) \
        .orderBy("city") \
        .show()
        
    reader.stop()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Lecteur de résultats Parquet')
    parser.add_argument('type', choices=['batch', 'streaming', 'both'], 
                       default='both', nargs='?', help='Type de résultats à lire')
    args = parser.parse_args()
    
    if args.type in ['batch', 'both']:
        read_batch_results()
    if args.type in ['streaming', 'both']:
        read_streaming_results()


if __name__ == "__main__":
    main()
