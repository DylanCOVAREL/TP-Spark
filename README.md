# TP Spark - Analyse des Températures Globales 🌍

Pipeline ETL avec Apache Spark pour analyser les données de température par ville (Kaggle).

## 🚀 Démarrage rapide avec Docker

### Prérequis
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installé
- Le fichier CSV `GlobalLandTemperaturesByCity.csv` téléchargé depuis [Kaggle](https://www.kaggle.com/)

### Installation en 3 étapes

1. **Cloner le repo**
```bash
git clone https://github.com/DylanCOVAREL/TP-Spark.git
cd TP-Spark
```

2. **Placer le fichier CSV**
- Télécharger `GlobalLandTemperaturesByCity.csv` depuis Kaggle
- Le placer dans le dossier du projet

3. **Lancer Docker**
```bash
docker-compose up --build
```

### 📊 Accéder aux interfaces

Une fois les conteneurs démarrés :

- **Jupyter Notebook** : http://localhost:8888
  - Le token d'accès s'affiche dans le terminal
  - Ouvrir `etl_temperature_simple.ipynb`

- **Spark UI** : http://localhost:4040
  - Monitoring des jobs Spark en temps réel
  - Disponible après avoir lancé des cellules Spark

### 🛠️ Utilisation

1. Ouvrir le notebook `etl_temperature_simple.ipynb` dans Jupyter
2. Exécuter les cellules une par une (Shift + Enter)
3. Les résultats seront sauvegardés dans `etl_output/`

### 📁 Structure du projet

```
TP-Spark/
├── Dockerfile                          # Configuration Docker
├── docker-compose.yaml                 # Orchestration des services
├── requirements.txt                    # Dépendances Python
├── etl_temperature_simple.ipynb        # Notebook principal (simplifié)
├── GlobalLandTemperaturesByCity.csv    # Données (non versionnées)
├── etl_output/                         # Résultats générés
└── README.md                           # Ce fichier
```

### 🧹 Arrêter et nettoyer

```bash
# Arrêter les conteneurs
docker-compose down

# Supprimer les volumes (données)
docker-compose down -v

# Supprimer les images
docker-compose down --rmi all
```

## 📝 Que fait ce pipeline ?

### Extract (Extraction)
- Charge le CSV (8M+ lignes) avec Spark
- Inférence automatique du schéma

### Transform (Transformation)
- Nettoyage des valeurs manquantes
- Conversion des dates
- Extraction année/mois

### Load (Chargement)
- Sauvegarde en Parquet (partitionné par année)
- Export CSV et JSON pour analyses
- Génération de rapports

### Analyses incluses
- ✅ Top 10 pays les plus chauds/froids
- ✅ Évolution temporelle (par année et décennie)
- ✅ Détection des températures extrêmes
- ✅ Analyse du réchauffement climatique
- ✅ Saisonnalité (température par mois)

## 🤝 Travail en équipe

### Pour les membres de l'équipe :

1. Cloner le repo
2. Télécharger le CSV depuis Kaggle
3. Lancer `docker-compose up`
4. C'est tout ! Pas d'installation Python/Spark/Java nécessaire

### Pour partager vos modifications :

```bash
git add .
git commit -m "Description de vos changements"
git push
```

## 📚 Ressources

- [Documentation PySpark](https://spark.apache.org/docs/latest/api/python/)
- [Dataset Kaggle](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

## ⚠️ Notes importantes

- Le fichier CSV (499 MB) n'est **pas versionné** sur Git (trop volumineux)
- Chaque membre doit le télécharger séparément depuis Kaggle
- Les résultats dans `etl_output/` ne sont pas versionnés non plus

## 🐛 Problèmes courants

**Jupyter ne démarre pas ?**
- Vérifier que Docker Desktop est bien lancé
- Vérifier que les ports 8888 et 4040 ne sont pas déjà utilisés

**Spark UI ne s'affiche pas ?**
- Normal, il n'apparaît qu'après avoir exécuté des cellules Spark dans le notebook

**Out of Memory ?**
- Augmenter la RAM allouée à Docker (Settings > Resources > Memory)
- Recommandé : minimum 4 GB

## 👥 Équipe

- Dylan COVAREL
- [Membre 2]
- [Membre 3]
