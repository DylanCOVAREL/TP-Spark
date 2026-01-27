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


def run_stream(api_mode=False):
    """Lance producteur et consommateur en parallèle"""
    import subprocess
    import threading
    
    print("=" * 70)
    print("DÉMARRAGE DU STREAMING COMPLET")
    print("=" * 70)
    print("\n⚠️  Conseil: Ouvrir 2 terminaux séparés:")
    print("    Terminal 1: python run.py producer")
    print("    Terminal 2: python run.py consumer")
    print("\nOu lancer dans ce terminal (Ctrl+C pour arrêter):\n")
    
    # Lancer le producteur dans un thread
    mode = 'api' if api_mode else 'simulation'
    producer_thread = threading.Thread(
        target=run_producer, 
        args=(mode, False, 0),
        daemon=True
    )
    producer_thread.start()
    
    # Attendre un peu puis lancer le consommateur
    import time
    time.sleep(3)
    run_consumer('file')


def read_results(result_type='both'):
    """Lit les résultats"""
    from src.utils import read_batch_results, read_streaming_results
    
    if result_type in ['batch', 'both']:
        read_batch_results()
    if result_type in ['streaming', 'both']:
        read_streaming_results()


def main():
    parser = argparse.ArgumentParser(
        description='Projet Traitement des Données - Spark Batch & Streaming',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python run.py batch                    # ETL batch sur données historiques
  python run.py producer                 # Générer des données météo simulées
  python run.py producer --api           # Générer avec API Open-Meteo réelle
  python run.py consumer                 # Consommer les données en streaming
  python run.py consumer --kafka         # Consommer depuis Kafka
  python run.py read                     # Lire tous les résultats
  python run.py read batch               # Lire uniquement les résultats batch
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commande à exécuter')
    
    # Commande batch
    subparsers.add_parser('batch', help='Exécuter le pipeline ETL batch')
    
    # Commande producer
    producer_parser = subparsers.add_parser('producer', help='Lancer le producteur de données')
    producer_parser.add_argument('--api', action='store_true', help='Utiliser l\'API Open-Meteo')
    producer_parser.add_argument('--kafka', action='store_true', help='Envoyer vers Kafka')
    producer_parser.add_argument('-n', '--iterations', type=int, default=0, help='Nombre d\'itérations (0=infini)')
    
    # Commande consumer
    consumer_parser = subparsers.add_parser('consumer', help='Lancer le consommateur Spark')
    consumer_parser.add_argument('--kafka', action='store_true', help='Lire depuis Kafka')
    
    # Commande stream
    stream_parser = subparsers.add_parser('stream', help='Lancer producteur + consommateur')
    stream_parser.add_argument('--api', action='store_true', help='Utiliser l\'API Open-Meteo')
    
    # Commande read
    read_parser = subparsers.add_parser('read', help='Lire les résultats')
    read_parser.add_argument('type', nargs='?', choices=['batch', 'streaming', 'both'], 
                            default='both', help='Type de résultats')
    
    args = parser.parse_args()
    
    if args.command == 'batch':
        run_batch()
    elif args.command == 'producer':
        mode = 'api' if args.api else 'simulation'
        run_producer(mode, args.kafka, args.iterations)
    elif args.command == 'consumer':
        mode = 'kafka' if args.kafka else 'file'
        run_consumer(mode)
    elif args.command == 'stream':
        run_stream(args.api)
    elif args.command == 'read':
        read_results(args.type)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
