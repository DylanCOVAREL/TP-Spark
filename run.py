#!/usr/bin/env python3
"""
Point d'entrée principal du projet

Usage:
    python run.py batch          # Exécuter le traitement batch
    python run.py stream         # Lancer le streaming (producteur + consommateur)
    python run.py producer       # Lancer uniquement le producteur
    python run.py consumer       # Lancer uniquement le consommateur
    python run.py read           # Lire les résultats
"""

import argparse
import sys
import os

# Ajouter le chemin src
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def run_batch():
    """Exécute le pipeline ETL batch"""
    from src.batch import TemperatureETL
    etl = TemperatureETL()
    etl.run()
    input("\n⏸️  Appuyez sur Entrée pour arrêter Spark...")
    etl.stop()


def run_producer(mode='simulation', kafka=False, iterations=0):
    """Lance le producteur de données météo"""
    from src.streaming import WeatherProducer
    producer = WeatherProducer(mode=mode, kafka_enabled=kafka)
    producer.run(max_iterations=iterations)


def run_consumer(mode='file'):
    """Lance le consommateur Spark Streaming"""
    from src.streaming import WeatherConsumer
    consumer = WeatherConsumer(mode=mode)
    consumer.run()


def run_stream(mode='simulation'):
    """Lance producteur et consommateur en parallèle"""
    import threading
    
    print("=" * 70)
    print("DÉMARRAGE DU STREAMING COMPLET")
    print("=" * 70)
    print("\n⚠️  Conseil: Ouvrir 2 terminaux séparés:")
    print("    Terminal 1: python run.py producer")
    print("    Terminal 2: python run.py consumer")
    print("\nOu lancer dans ce terminal (Ctrl+C pour arrêter):\n")
    
    # Lancer le producteur dans un thread
    producer_thread = threading.Thread(
        target=WeatherProducer(mode=mode).run, 
        kwargs={'max_iterations': 0},
        daemon=True
    )
    producer_thread.start()
    
    # Attendre un peu puis lancer le consommateur
    import time
    time.sleep(3)
    run_consumer('file')


def read_results():
    """Lit les résultats unifiés (batch + streaming)"""
    from src.utils import read_results as _read
    _read()


def main():
    parser = argparse.ArgumentParser(
        description='Projet Traitement des Données - Spark Batch & Streaming',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python run.py batch                       # ETL batch sur données historiques
  python run.py producer                    # Générer des données météo simulées
  python run.py producer --mode openweather # Générer avec OpenWeatherMap
  python run.py consumer                    # Consommer les données en streaming
  python run.py stream --mode openweather   # Lancer tout le pipeline streaming
  python run.py read                        # Lire le Parquet unifié (batch + streaming)
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commande à exécuter')
    
    # Commande batch
    subparsers.add_parser('batch', help='Exécuter le pipeline ETL batch')
    
    # Commande producer
    producer_parser = subparsers.add_parser('producer', help='Lancer le producteur de données')
    producer_parser.add_argument('--mode', choices=['simulation', 'api', 'windy', 'openweather'], default='simulation',
                                help='Mode: simulation (données aléatoires), api (Open-Meteo gratuit), windy (API Windy), openweather (OpenWeatherMap)')
    producer_parser.add_argument('--kafka', action='store_true', help='Envoyer vers Kafka')
    producer_parser.add_argument('-n', '--iterations', type=int, default=0, help='Nombre d\'itérations (0=infini)')
    
    # Commande consumer
    consumer_parser = subparsers.add_parser('consumer', help='Lancer le consommateur Spark')
    consumer_parser.add_argument('--kafka', action='store_true', help='Lire depuis Kafka')
    
    # Commande stream
    stream_parser = subparsers.add_parser('stream', help='Lancer producteur + consommateur')
    stream_parser.add_argument('--mode', choices=['simulation', 'api', 'windy', 'openweather'], default='simulation',
                              help='Mode du producteur')
    
    # Commande read
    subparsers.add_parser('read', help='Lire les résultats unifiés (batch + streaming)')
    
    args = parser.parse_args()
    
    if args.command == 'batch':
        run_batch()
    elif args.command == 'producer':
        producer = WeatherProducer(mode=args.mode, kafka_enabled=args.kafka)
        producer.run(max_iterations=args.iterations)
    elif args.command == 'consumer':
        mode = 'kafka' if args.kafka else 'file'
        run_consumer(mode)
    elif args.command == 'stream':
        run_stream(args.mode)
    elif args.command == 'read':
        read_results()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
