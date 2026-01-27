"""
Configuration centralisée du projet
"""

# ============================================================================
# CHEMINS ET DOSSIERS
# ============================================================================

PATHS = {
    'data_input': 'data/input',
    'data_output': 'data/output',
    'streaming_input': 'data/streaming_input',
    'streaming_output': 'data/streaming_output',
    'checkpoints': 'data/checkpoints',
    'csv_source': 'GlobalLandTemperaturesByCity.csv'
}

# ============================================================================
# CONFIGURATION SPARK
# ============================================================================

SPARK_CONFIG = {
    'app_name': 'MeteoProject',
    'driver_memory': '4g',
    'shuffle_partitions': '2',
    'log_level': 'WARN'
}

# ============================================================================
# CONFIGURATION KAFKA
# ============================================================================

KAFKA_CONFIG = {
    'bootstrap_servers': 'localhost:9092',
    'topic': 'windy-weather',
    'starting_offsets': 'latest'
}

# ============================================================================
# CLÉS API
# ============================================================================

API_KEYS = {
    'windy': 'VOTRE_CLE_API_WINDY',  # https://api.windy.com/
    'openweather': '',                # https://openweathermap.org/api
    'weatherapi': '',                 # https://www.weatherapi.com/
    'openmeteo': 'gratuit',           # Pas besoin de clé
    'visualcrossing': ''              # https://www.visualcrossing.com/
}

# ============================================================================
# VILLES POUR LE STREAMING
# ============================================================================

CITIES = [
    {"city": "Paris", "country": "France", "lat": 48.8566, "lon": 2.3522},
    {"city": "London", "country": "UK", "lat": 51.5074, "lon": -0.1278},
    {"city": "New York", "country": "USA", "lat": 40.7128, "lon": -74.0060},
    {"city": "Tokyo", "country": "Japan", "lat": 35.6762, "lon": 139.6503},
    {"city": "Sydney", "country": "Australia", "lat": -33.8688, "lon": 151.2093},
]

# ============================================================================
# PARAMÈTRES STREAMING
# ============================================================================

STREAMING_CONFIG = {
    'interval_seconds': 30,
    'max_iterations': 10,
    'window_duration': '1 minute',
    'watermark_delay': '2 minutes',
    'trigger_interval': '10 seconds'
}
