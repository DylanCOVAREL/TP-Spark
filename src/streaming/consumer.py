#!/usr/bin/env python3
"""
Consommateur Spark Structured Streaming

Deux modes de consommation:
1. Fichiers CSV (file streaming) - pour tests sans Kafka
2. Kafka - pour production
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, window, avg, max, min, count, 
                                   current_timestamp, from_json, to_timestamp,
                                   lit, year as spark_year, round as spark_round)
from pyspark.sql.types import (StructType, StructField, StringType, 
                               DoubleType, IntegerType, TimestampType)
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.config import PATHS, SPARK_CONFIG, KAFKA_CONFIG, STREAMING_CONFIG


class WeatherConsumer:
    """Consommateur Spark Structured Streaming pour données météo"""
    
    # Schéma des données météo
    SCHEMA = StructType([
        StructField("timestamp", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("temperature_celsius", DoubleType(), True),
        StructField("wind_speed_kmh", DoubleType(), True),
        StructField("wind_direction_deg", IntegerType(), True),
        StructField("humidity_percent", DoubleType(), True),
        StructField("pressure_hpa", DoubleType(), True),
        StructField("source", StringType(), True)
    ])
    
    def __init__(self, mode='file'):
        """
        Args:
            mode: 'file' pour lire des CSV, 'kafka' pour Kafka
        """
        self.mode = mode
        self.spark = None
        self.queries = []
        
    def _create_spark_session(self):
        """Crée la session Spark avec support streaming"""
        print("📦 Création de la session Spark...")
        
        builder = SparkSession.builder \
            .appName(f"{SPARK_CONFIG['app_name']}_Streaming") \
            .config("spark.driver.memory", SPARK_CONFIG['driver_memory']) \
            .config("spark.sql.shuffle.partitions", SPARK_CONFIG['shuffle_partitions'])
        
        if self.mode == 'kafka':
            builder = builder.config(
                "spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
            )
            
        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel(SPARK_CONFIG['log_level'])
        print("✅ Spark démarré")
        
    def _read_from_files(self):
        """Lit le flux depuis des fichiers CSV"""
        input_dir = PATHS['streaming_input']
        os.makedirs(input_dir, exist_ok=True)
        
        print(f"📥 Lecture du flux depuis: {input_dir}")
        
        return self.spark.readStream \
            .schema(self.SCHEMA) \
            .option("header", "true") \
            .option("maxFilesPerTrigger", 1) \
            .csv(input_dir)
            
    def _read_from_kafka(self):
        """Lit le flux depuis Kafka"""
        print(f"📥 Connexion à Kafka: {KAFKA_CONFIG['bootstrap_servers']}")
        print(f"   Topic: {KAFKA_CONFIG['topic']}")
        
        df_kafka = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap_servers']) \
            .option("subscribe", KAFKA_CONFIG['topic']) \
            .option("startingOffsets", KAFKA_CONFIG['starting_offsets']) \
            .load()
            
        # Parser le JSON depuis Kafka
        return df_kafka.select(
            from_json(col("value").cast("string"), self.SCHEMA).alias("data")
        ).select("data.*")
        
    def _create_aggregations(self, df_stream):
        """Crée les agrégations par fenêtre de temps"""
        # Convertir le timestamp string en timestamp Spark
        df_enriched = df_stream \
            .withColumn("event_time", to_timestamp(col("timestamp"))) \
            .withColumn("processing_time", current_timestamp())
        
        # Agrégations par fenêtre de 1 minute et par ville
        df_windowed = df_enriched \
            .withWatermark("event_time", STREAMING_CONFIG['watermark_delay']) \
            .groupBy(
                window("event_time", STREAMING_CONFIG['window_duration']),
                "city",
                "country"
            ) \
            .agg(
                count("*").alias("nb_mesures"),
                spark_round(avg("temperature_celsius"), 2).alias("temperature_moyenne"),
                max("temperature_celsius").alias("temp_max"),
                min("temperature_celsius").alias("temp_min"),
                avg("wind_speed_kmh").alias("vent_moyen_kmh"),
                avg("humidity_percent").alias("humidite_moyenne"),
                avg("pressure_hpa").alias("pression_moyenne")
            )
        
        # Format unifié pour le parquet (même structure que batch)
        df_unified = df_windowed \
            .withColumn("year", spark_year(col("window.start"))) \
            .withColumn("source", lit("streaming")) \
            .select(
                col("city").alias("City"),
                col("country").alias("Country"),
                "year",
                "temperature_moyenne",
                "source"
            )
            
        return df_enriched, df_windowed, df_unified
        
    def run(self, output_console=True, output_parquet=True):
        """Lance le streaming"""
        print("=" * 70)
        print(f"CONSOMMATEUR SPARK STREAMING - Mode: {self.mode.upper()}")
        print("=" * 70)
        
        self._create_spark_session()
        
        # Lire le flux selon le mode
        if self.mode == 'kafka':
            df_stream = self._read_from_kafka()
        else:
            df_stream = self._read_from_files()
            
        print("✅ Stream configuré")
        
        # Créer les agrégations
        df_enriched, df_windowed, df_unified = self._create_aggregations(df_stream)
        
        # Output vers la console
        if output_console:
            print("\n📺 Activation de la sortie console...")
            query_console = df_windowed \
                .writeStream \
                .outputMode("update") \
                .format("console") \
                .option("truncate", "false") \
                .trigger(processingTime=STREAMING_CONFIG['trigger_interval']) \
                .start()
            self.queries.append(query_console)
            
        # Output vers Parquet unifié (même structure que batch)
        if output_parquet:
            output_path = f"{PATHS['results']}/temperatures.parquet"
            checkpoint_path = f"{PATHS['checkpoints']}/weather_streaming"
            os.makedirs(PATHS['results'], exist_ok=True)
            os.makedirs(checkpoint_path, exist_ok=True)
            
            print(f"💾 Sauvegarde vers Parquet unifié: {output_path}")
            
            query_parquet = df_unified \
                .writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", output_path) \
                .option("checkpointLocation", checkpoint_path) \
                .trigger(processingTime=STREAMING_CONFIG['trigger_interval']) \
                .start()
            self.queries.append(query_parquet)
            
        print("\n" + "=" * 70)
        print("▶️  Streaming actif - Ctrl+C pour arrêter")
        print("=" * 70 + "\n")
        
        # Attendre la fin
        try:
            for query in self.queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            print("\n⚠️  Arrêt du streaming...")
            self.stop()
            
    def stop(self):
        """Arrête le streaming et Spark"""
        for query in self.queries:
            query.stop()
        if self.spark:
            self.spark.stop()
        print("🛑 Streaming arrêté")


def main():
    """Point d'entrée"""
    import argparse
    parser = argparse.ArgumentParser(description='Consommateur Spark Streaming')
    parser.add_argument('--mode', choices=['file', 'kafka'], default='file',
                       help='Source des données')
    parser.add_argument('--no-console', action='store_true', help='Désactiver sortie console')
    parser.add_argument('--no-parquet', action='store_true', help='Désactiver sauvegarde Parquet')
    args = parser.parse_args()
    
    consumer = WeatherConsumer(mode=args.mode)
    consumer.run(
        output_console=not args.no_console,
        output_parquet=not args.no_parquet
    )


if __name__ == "__main__":
    main()
