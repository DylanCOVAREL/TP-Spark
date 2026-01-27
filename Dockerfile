# Image de base Python
FROM python:3.11-slim

# Installation de Java (requis pour Spark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Définir JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Créer le répertoire de travail
WORKDIR /app

# Installer les dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier les fichiers du projet
COPY . .

# Exposer le port pour Jupyter et Spark UI
EXPOSE 8888 4040

# Lancer Jupyter Notebook
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
