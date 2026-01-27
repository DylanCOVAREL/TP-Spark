from typing import Optional, List
import glob
import pandas as pd

from fastapi import FastAPI, HTTPException, Query

app = FastAPI(
    title="Climate Analytics API",
    version="1.0",
    description="API Gateway pour exposer les résultats Spark (Parquet/JSON) avec filtres avancés."
)

BASE_PATH = "data/curated"

# ======================================================
# Helpers
# ======================================================

def _read_parquet(rel_path: str) -> pd.DataFrame:
    path = f"{BASE_PATH}/{rel_path}"
    try:
        return pd.read_parquet(path)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lecture Parquet: {path} | {e}"
        )


def _read_json_dir(rel_path: str) -> pd.DataFrame:
    """
    Spark écrit le JSON dans un DOSSIER avec part-*.json
    """
    path = f"{BASE_PATH}/{rel_path}"
    files = glob.glob(f"{path}/part-*.json")

    if not files:
        raise HTTPException(
            status_code=500,
            detail=f"Aucun fichier JSON part-* trouvé dans: {path}"
        )

    try:
        return pd.concat(
            (pd.read_json(f, lines=True) for f in files),
            ignore_index=True
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erreur lecture JSON: {path} | {e}"
        )


def _filter_year_range(
    df: pd.DataFrame,
    year_min: Optional[int],
    year_max: Optional[int],
    year_col: str = "year"
) -> pd.DataFrame:
    if year_col not in df.columns:
        return df

    if year_min is not None:
        df = df[df[year_col] >= year_min]

    if year_max is not None:
        df = df[df[year_col] <= year_max]

    return df


def _ensure_cols(df: pd.DataFrame, cols: List[str], name: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise HTTPException(
            status_code=500,
            detail=f"Colonnes manquantes dans {name}: {missing}"
        )

# ======================================================
# Root
# ======================================================

@app.get("/")
def root():
    return {
        "status": "API Climate opérationnelle",
        "docs": "/docs"
    }

# ======================================================
# 1) Évolution annuelle globale
# ======================================================

@app.get("/temperature/global/yearly")
def global_yearly_trend(
    year_min: Optional[int] = Query(None, ge=0),
    year_max: Optional[int] = Query(None, ge=0),
):
    df = _read_parquet("global_yearly_trend")
    _ensure_cols(df, ["year"], "global_yearly_trend")

    df = _filter_year_range(df, year_min, year_max)
    df = df.sort_values("year")

    return df.to_dict(orient="records")

# ======================================================
# 2) Top N villes les plus chaudes
# ======================================================

@app.get("/temperature/top-cities")
def top_cities(
    top_n: int = Query(10, ge=1, le=200),
):
    if top_n <= 10:
        df = _read_json_dir("top_10_hottest_cities")
        _ensure_cols(df, ["Country", "City"], "top_10_hottest_cities")

        if "avg_temperature" in df.columns:
            df = df.sort_values("avg_temperature", ascending=False)

        return df.head(top_n).to_dict(orient="records")

    # fallback sur toutes les villes
    df = _read_json_dir("avg_temp_city")
    _ensure_cols(df, ["Country", "City", "avg_temperature"], "avg_temp_city")

    df = df.sort_values("avg_temperature", ascending=False).head(top_n)
    return df.to_dict(orient="records")

# ======================================================
# 3) Température moyenne par pays
# ======================================================

@app.get("/temperature/country/avg")
def avg_temp_by_country(
    top_n: int = Query(30, ge=1, le=300),
    contains: Optional[str] = Query(None),
):
    df = _read_parquet("avg_temp_by_country")
    _ensure_cols(df, ["Country"], "avg_temp_by_country")

    if contains:
        df = df[df["Country"].str.contains(contains, case=False, na=False)]

    if "avg_temperature" in df.columns:
        df = df.sort_values("avg_temperature", ascending=False)

    return df.head(top_n).to_dict(orient="records")

# ======================================================
# 4) Évolution annuelle par pays
# ======================================================

@app.get("/temperature/country/{country}/yearly")
def country_yearly(
    country: str,
    year_min: Optional[int] = Query(None, ge=0),
    year_max: Optional[int] = Query(None, ge=0),
):
    df = _read_parquet("avg_temp_country_year")
    _ensure_cols(df, ["Country", "year"], "avg_temp_country_year")

    df = df[df["Country"] == country]
    df = _filter_year_range(df, year_min, year_max)
    df = df.sort_values("year")

    return df.to_dict(orient="records")

# ======================================================
# 5) Évolution annuelle par ville
# ======================================================

@app.get("/temperature/city/yearly")
def city_yearly(
    country: str = Query(...),
    city: str = Query(...),
    year_min: Optional[int] = Query(None, ge=0),
    year_max: Optional[int] = Query(None, ge=0),
):
    df = _read_parquet("avg_temp_city_year")
    _ensure_cols(df, ["Country", "City", "year"], "avg_temp_city_year")

    df = df[(df["Country"] == country) & (df["City"] == city)]
    df = _filter_year_range(df, year_min, year_max)
    df = df.sort_values("year")

    return df.to_dict(orient="records")

# ======================================================
# 6) Température moyenne par décennie
# ======================================================

@app.get("/temperature/decade")
def avg_temp_by_decade(
    decade_min: Optional[int] = Query(None, ge=0),
    decade_max: Optional[int] = Query(None, ge=0),
):
    df = _read_parquet("avg_temp_by_decade")
    _ensure_cols(df, ["decade"], "avg_temp_by_decade")

    if decade_min is not None:
        df = df[df["decade"] >= decade_min]
    if decade_max is not None:
        df = df[df["decade"] <= decade_max]

    df = df.sort_values("decade")
    return df.to_dict(orient="records")

# ======================================================
# 7) Variabilité climatique
# ======================================================

@app.get("/temperature/variability")
def variability_top_cities(
    top_n: int = Query(20, ge=1, le=300),
    country: Optional[str] = Query(None),
):
    df = _read_parquet("temp_variability_city")
    _ensure_cols(df, ["Country", "City", "temp_variability"], "temp_variability_city")

    if country:
        df = df[df["Country"] == country]

    df = df.sort_values("temp_variability", ascending=False).head(top_n)
    return df.to_dict(orient="records")

# ======================================================
# 8) Tendance de réchauffement
# ======================================================

@app.get("/temperature/trend")
def warming_trend_top_cities(
    top_n: int = Query(20, ge=1, le=300),
    country: Optional[str] = Query(None),
):
    df = _read_parquet("temp_trend_city")
    _ensure_cols(df, ["Country", "City", "temp_delta"], "temp_trend_city")

    if country:
        df = df[df["Country"] == country]

    df = df.sort_values("temp_delta", ascending=False).head(top_n)
    return df.to_dict(orient="records")
