#!/usr/bin/env python3
"""
Producteur de données météo - Génère des données pour le streaming

Deux modes:
1. Simulation locale (fichiers CSV) - pour tests sans Kafka
2. API météo réelle + Kafka - pour production
"""

import json
import time
import random
import os
import requests
import math
from datetime import datetime
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.config import PATHS, CITIES, API_KEYS, STREAMING_CONFIG


class WeatherProducer:
    """Producteur de données météo"""
    
    def __init__(self, mode='simulation', kafka_enabled=False):
        """
        Args:
            mode: 'simulation' ou 'api' (OpenMeteo gratuit)
            kafka_enabled: True pour envoyer vers Kafka
        """
        self.mode = mode
        self.kafka_enabled = kafka_enabled
        self.producer = None
        self.output_dir = PATHS['streaming_input']
        self.iteration = 0
        
        os.makedirs(self.output_dir, exist_ok=True)
        
    def _init_kafka(self):
        """Initialise le producteur Kafka"""
        if not self.kafka_enabled:
            return
        try:
            from kafka import KafkaProducer
            from src.config import KAFKA_CONFIG
            
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all'
            )
            print("✅ Connexion Kafka établie")
        except Exception as e:
            print(f"⚠️  Kafka non disponible: {e}")
            self.kafka_enabled = False
            
    def _fetch_simulated(self, city_info):
        """Génère des données météo simulées"""
        base_temps = {'Paris': 15, 'London': 12, 'New York': 10, 'Tokyo': 18, 'Sydney': 22}
        base_temp = base_temps.get(city_info['city'], 15)
        
        return {
            'city': city_info['city'],
            'country': city_info['country'],
            'lat': city_info['lat'],
            'lon': city_info['lon'],
            'timestamp': datetime.now().isoformat(),
            'temperature_celsius': round(base_temp + random.uniform(-5, 5), 2),
            'wind_speed_kmh': round(random.uniform(0, 30), 2),
            'wind_direction_deg': random.randint(0, 360),
            'humidity_percent': round(random.uniform(30, 90), 1),
            'pressure_hpa': round(random.uniform(990, 1030), 1),
            'source': 'simulation'
        }
        
    def _fetch_openmeteo(self, city_info):
        """Récupère les données depuis Open-Meteo (gratuit, sans clé)"""
        try:
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": city_info['lat'],
                "longitude": city_info['lon'],
                "current": "temperature_2m,relative_humidity_2m,pressure_msl,wind_speed_10m,wind_direction_10m"
            }
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            current = data['current']
            
            return {
                'city': city_info['city'],
                'country': city_info['country'],
                'lat': city_info['lat'],
                'lon': city_info['lon'],
                'timestamp': datetime.now().isoformat(),
                'temperature_celsius': current['temperature_2m'],
                'wind_speed_kmh': current['wind_speed_10m'],
                'wind_direction_deg': current['wind_direction_10m'],
                'humidity_percent': current['relative_humidity_2m'],
                'pressure_hpa': current['pressure_msl'],
                'source': 'open-meteo'
            }
        except Exception as e:
            print(f"  ✗ Open-Meteo {city_info['city']}: {e}")
            return self._fetch_simulated(city_info)
            
    def fetch_weather(self, city_info):
        """Récupère les données météo selon le mode"""
        if self.mode == 'api':
            return self._fetch_openmeteo(city_info)
        return self._fetch_simulated(city_info)
        
    def _write_csv(self, data_list):
        """Écrit les données dans un fichier CSV"""
        self.iteration += 1
        filename = f"weather_{int(time.time())}_{self.iteration}.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w') as f:
            f.write("timestamp,city,country,lat,lon,temperature_celsius,wind_speed_kmh,wind_direction_deg,humidity_percent,pressure_hpa,source\n")
            for d in data_list:
                f.write(f"{d['timestamp']},{d['city']},{d['country']},{d['lat']},{d['lon']},"
                       f"{d['temperature_celsius']},{d['wind_speed_kmh']},{d['wind_direction_deg']},"
                       f"{d['humidity_percent']},{d['pressure_hpa']},{d['source']}\n")
        return filepath
        
    def _send_to_kafka(self, data_list):
        """Envoie les données vers Kafka"""
        if not self.producer:
            return
        from src.config import KAFKA_CONFIG
        for data in data_list:
            self.producer.send(KAFKA_CONFIG['topic'], value=data)
        self.producer.flush()
        
    def run(self, max_iterations=None):
        """Lance la production de données"""
        max_iterations = max_iterations or STREAMING_CONFIG['max_iterations']
        interval = STREAMING_CONFIG['interval_seconds']
        
        print("=" * 70)
        print(f"PRODUCTEUR MÉTÉO - Mode: {self.mode.upper()}")
        print("=" * 70)
        print(f"📁 Output: {self.output_dir}")
        print(f"⏱️  Intervalle: {interval}s | 🌍 Villes: {len(CITIES)}")
        print(f"📡 Kafka: {'Activé' if self.kafka_enabled else 'Désactivé'}")
        print()
        
        self._init_kafka()
        
        try:
            print("▶️  Démarrage... (Ctrl+C pour arrêter)\n")
            iteration = 0
            
            while max_iterations == 0 or iteration < max_iterations:
                iteration += 1
                
                # Récupérer les données pour toutes les villes
                data_list = [self.fetch_weather(city) for city in CITIES]
                
                # Écrire en CSV (pour Spark file streaming)
                filepath = self._write_csv(data_list)
                
                # Envoyer vers Kafka si activé
                if self.kafka_enabled:
                    self._send_to_kafka(data_list)
                
                # Afficher le résumé
                print(f"✓ [{datetime.now().strftime('%H:%M:%S')}] Batch #{iteration}")
                for d in data_list:
                    print(f"   {d['city']:12s}: {d['temperature_celsius']:5.1f}°C | "
                          f"Vent: {d['wind_speed_kmh']:5.1f} km/h | "
                          f"Humidité: {d['humidity_percent']:4.1f}%")
                print()
                
                if max_iterations == 0 or iteration < max_iterations:
                    time.sleep(interval)
                    
        except KeyboardInterrupt:
            print(f"\n⚠️  Arrêt - {iteration} batches générés")
        
        print("=" * 70)


def main():
    """Point d'entrée - Mode simulation par défaut"""
    import argparse
    parser = argparse.ArgumentParser(description='Producteur de données météo')
    parser.add_argument('--mode', choices=['simulation', 'api'], default='simulation',
                       help='Mode de génération des données')
    parser.add_argument('--kafka', action='store_true', help='Activer Kafka')
    parser.add_argument('--iterations', type=int, default=0, help='Nombre d\'itérations (0=infini)')
    args = parser.parse_args()
    
    producer = WeatherProducer(mode=args.mode, kafka_enabled=args.kafka)
    producer.run(max_iterations=args.iterations)


if __name__ == "__main__":
    main()
