from api.spark import spark

BASE_PATH = "data/curated"

def load_parquet(name):
    return spark.read.parquet(f"{BASE_PATH}/{name}")

def load_json(name):
    return spark.read.json(f"{BASE_PATH}/{name}")
