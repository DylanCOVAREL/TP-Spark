# 🌡️ Projet Traitement des Données - Spark Batch & Streaming

Pipeline de traitement de données météorologiques avec Apache Spark.

## 📁 Structure du projet

```
📦 Traitement des données - TP/
├── 📄 run.py                    # Point d'entrée unique
├── 📄 requirements.txt          # Dépendances Python
├── 📄 docker-compose-kafka.yaml # Kafka (optionnel)
├── 📄 GlobalLandTemperaturesByCity.csv  # Données historiques
│
├── 📂 src/                      # Code source
│   ├── 📄 config.py             # Configuration centralisée
│   │
│   ├── 📂 batch/                # Traitement par lots
│   │   └── 📄 etl_temperature.py
│   │
│   ├── 📂 streaming/            # Traitement temps réel
│   │   ├── 📄 producer.py       # Génère les données météo
│   │   └── 📄 consumer.py       # Traite les données en streaming
│   │
│   └── 📂 utils/                # Utilitaires
│       └── 📄 reader.py         # Lecture des résultats
│
└── 📂 data/                     # Données (auto-généré)
    ├── 📂 output/               # Résultats batch
    ├── 📂 streaming_input/      # Fichiers pour streaming
    ├── 📂 streaming_output/     # Résultats streaming
    └── 📂 checkpoints/          # Checkpoints Spark
```

## 🚀 Démarrage rapide

### Prérequis
```bash
pip install -r requirements.txt
```

### 1️⃣ Traitement Batch (ETL)
Analyse des températures historiques par ville et année:
```bash
python run.py batch
```

### 2️⃣ Streaming en temps réel

**Terminal 1 - Producteur** (génère des données météo):
```bash
# Mode simulation (données aléatoires)
python run.py producer

# Mode API réelle (Open-Meteo gratuit)
python run.py producer --api
```

**Terminal 2 - Consommateur** (traite les données):
```bash
python run.py consumer
```

### 3️⃣ Lire les résultats
```bash
python run.py read          # Tous les résultats
python run.py read batch    # Résultats batch uniquement
python run.py read streaming # Résultats streaming uniquement
```

## 📊 Fonctionnalités

### Batch (ETL)
- **Extract**: Charge le CSV des températures mondiales (8M+ lignes)
- **Transform**: Nettoie, extrait l'année, calcule les moyennes
- **Load**: Sauvegarde en Parquet, CSV, JSON

### Streaming
- **Producteur**: Génère des données météo (simulation ou API Open-Meteo)
- **Consommateur**: Agrégations par fenêtre temporelle avec Spark Structured Streaming
- **Formats**: Fichiers CSV ou Kafka

## ⚙️ Configuration

Modifiez `src/config.py` pour personnaliser:
- Chemins des données
- Paramètres Spark (mémoire, partitions)
- Configuration Kafka
- Clés API météo
- Villes surveillées
- Paramètres de fenêtrage

## 🐳 Kafka (optionnel)

Pour utiliser Kafka:
```bash
# Démarrer Kafka
docker compose -f docker-compose-kafka.yaml up -d

# Producteur vers Kafka
python run.py producer --kafka

# Consommateur depuis Kafka
python run.py consumer --kafka
```

## 📈 Spark UI

Pendant l'exécution, accédez à l'interface Spark: **http://localhost:4040**

## 🔧 Commandes

| Commande | Description |
|----------|-------------|
| `python run.py batch` | Pipeline ETL batch |
| `python run.py producer` | Producteur (simulation) |
| `python run.py producer --api` | Producteur (API réelle) |
| `python run.py producer --kafka` | Producteur vers Kafka |
| `python run.py consumer` | Consommateur (fichiers) |
| `python run.py consumer --kafka` | Consommateur Kafka |
| `python run.py read` | Lire les résultats |

---

## 📚 Ancienne documentation

<details>
<summary>Cliquez pour voir l'ancienne documentation Docker</summary>

### Avec Docker

```bash
docker compose exec spark-jupyter python lire_parquet.py
```

**Avec PySpark** :
```bash
docker compose exec spark-jupyter python lire_parquet_spark.py
```

---

## 🌊 Partie 2 : Streaming Temps Réel (API Windy)

### Configuration de l'API Windy

1. Créer un compte sur https://api.windy.com/
2. Obtenir une clé API
3. Éditer `streaming_windy.py` :
```python
WINDY_API_KEY = "votre_cle_api_ici"
```

### Lancer le Streaming

```bash
docker compose exec spark-jupyter python streaming_windy.py
```

**Ce que ça fait** :
- Récupère les données météo de 5 villes toutes les 10 secondes
- Températures en temps réel (API Windy)
- Conversion automatique Kelvin → Celsius
- Sauvegarde en Parquet

### Lire les résultats Streaming

```bash
docker compose exec spark-jupyter python lire_streaming_results.py
```

**Analyses disponibles** :
- Dernières mesures par ville
- Statistiques (moyenne, max, min)
- Évolution temporelle
- Classement par température

---

## 🔥 Mode Production : Kafka (Optionnel)

Pour un vrai streaming production avec Kafka :

### 1. Démarrer Kafka
```bash
# Arrêter le conteneur simple
docker compose down

# Démarrer avec Kafka
docker compose -f docker-compose-kafka.yaml up -d
```

### 2. Interfaces
- **Kafka UI** : http://localhost:8080
- **Spark UI** : http://localhost:4040

### 3. Lancer le producteur et consommateur

**Terminal 1** :
```bash
docker compose -f docker-compose-kafka.yaml exec spark-streaming python kafka_producer_windy.py
```

**Terminal 2** :
```bash
docker compose -f docker-compose-kafka.yaml exec spark-streaming python kafka_consumer_spark.py
```

</details>
