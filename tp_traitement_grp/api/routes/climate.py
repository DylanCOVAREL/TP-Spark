from fastapi import APIRouter, Query
from api.loaders import load_parquet, load_json
from pyspark.sql.functions import col

router = APIRouter(prefix="/climate", tags=["Climate"])
