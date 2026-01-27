#!/usr/bin/env python3
"""
Utilitaire pour lire et analyser le Parquet unifié (batch + streaming)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, count, desc, asc
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.config import PATHS, SPARK_CONFIG


class ParquetReader:
    """Lecteur et analyseur du fichier Parquet unifié"""
    
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
        
    def load(self):
        """Charge le parquet unifié"""
        if not self.spark:
            self._create_spark_session()
        
        path = f"{PATHS['results']}/temperatures.parquet"
        
        # Fallback vers ancien chemin si nécessaire
        if not os.path.exists(path):
            path = "resultats/temperature_ville_annee.parquet"
            
        if not os.path.exists(path):
            print("❌ Aucun résultat trouvé")
            print(f"   Exécutez d'abord: python run.py batch")
            return None
            
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
        """Affiche les statistiques par source"""
        print("\n📈 Répartition par source:")
        self.df.groupBy("source").agg(
            count("*").alias("nb_lignes"),
            min("year").alias("année_min"),
            max("year").alias("année_max")
        ).show()
        return self
        
    def show_sample(self, n=10):
        """Affiche un échantillon des données"""
        print(f"\n📊 Aperçu ({n} lignes):")
        self.df.show(n, truncate=False)
        return self
        
    def show_analysis(self):
        """Affiche des analyses"""
        # Stats générales
        print("\n📈 Statistiques générales:")
        nb_villes = self.df.select("City").distinct().count()
        nb_pays = self.df.select("Country").distinct().count()
        print(f"   • Villes: {nb_villes:,}")
        print(f"   • Pays: {nb_pays}")
        
        # Top 5 villes chaudes (données récentes)
        print("\n🌡️  Top 5 villes les plus chaudes (données récentes):")
        self.df.filter(col("year") >= 2010) \
            .groupBy("City", "Country") \
            .agg(avg("temperature_moyenne").alias("temp_moy")) \
            .orderBy(desc("temp_moy")) \
            .show(5)
            
        # Comparaison batch vs streaming si les deux existent
        sources = [row.source for row in self.df.select("source").distinct().collect()]
        if len(sources) > 1:
            print("\n🔄 Comparaison Batch vs Streaming:")
            self.df.groupBy("source").agg(
                count("*").alias("nb_enregistrements"),
                avg("temperature_moyenne").alias("temp_moyenne_globale")
            ).show()
        
        return self
        
    def stop(self):
        """Arrête Spark"""
        if self.spark:
            self.spark.stop()


def read_results():
    """Lit et affiche les résultats unifiés"""
    print("=" * 70)
    print("LECTURE DES RÉSULTATS UNIFIÉS (BATCH + STREAMING)")
    print("=" * 70)
    
    reader = ParquetReader()
    if reader.load():
        reader.show_schema()
        reader.show_stats()
        reader.show_sample()
        reader.show_analysis()
        reader.stop()


# Compatibilité avec l'ancien code
def read_batch_results():
    read_results()

def read_streaming_results():
    read_results()


def main():
    read_results()


if __name__ == "__main__":
    main()
