from typing import Optional, List, Dict, Tuple
import glob
import os
import time
import threading

import pandas as pd
import jwt

from fastapi import FastAPI, HTTPException, Query, Depends, status, Security, Request
from fastapi.security import (
    OAuth2PasswordBearer,
    OAuth2PasswordRequestForm,
    APIKeyHeader,
)
from fastapi.middleware.cors import CORSMiddleware

# ======================================================
# App
# ======================================================

app = FastAPI(
    title="Climate Analytics API",
    version="1.2",
    description="JWT + API Key + Quota protected Spark Analytics API",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # en prod: mets ton frontend (ex: http://localhost:4200)
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======================================================
# CONFIG
# ======================================================

BASE_PATH = "data/curated"

JWT_SECRET = os.environ.get("JWT_SECRET", "dev-secret-change-me")
JWT_ALGORITHM = os.environ.get("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_SECONDS = int(os.environ.get("ACCESS_TOKEN_EXPIRE_SECONDS", "3600"))

# Demo user credentials
API_USER = os.environ.get("API_USER", "admin")
API_PASS = os.environ.get("API_PASS", "API_PASS")

# API KEY (app-level auth)
API_KEY = os.environ.get("API_KEY", "boss123456789")
API_KEY_NAME = "X-API-Key"

# Quota / Rate limit (per API key)
RATE_LIMIT_MAX = int(os.environ.get("RATE_LIMIT_MAX", "100"))                
RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("RATE_LIMIT_WINDOW_SECONDS", "60"))  

# ======================================================
# SECURITY SCHEMES (Swagger)
# ======================================================

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
api_key_scheme = APIKeyHeader(name=API_KEY_NAME, auto_error=True)

# ======================================================
# QUOTA STORAGE (in-memory)
# key -> (window_start_epoch, count)
# ======================================================

_rate_lock = threading.Lock()
_rate_state: Dict[str, Tuple[int, int]] = {}

def _check_and_consume_quota(api_key: str) -> None:
    """
    Sliding window simplified: fixed windows.
    For each api_key:
      - if window expired -> reset count
      - else increment count, if over limit -> 429
    """
    now = int(time.time())
    with _rate_lock:
        window_start, count = _rate_state.get(api_key, (now, 0))

        # reset window if expired
        if now - window_start >= RATE_LIMIT_WINDOW_SECONDS:
            window_start = now
            count = 0

        # consume 1
        count += 1
        _rate_state[api_key] = (window_start, count)

        if count > RATE_LIMIT_MAX:
            retry_after = RATE_LIMIT_WINDOW_SECONDS - (now - window_start)
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Quota exceeded: {RATE_LIMIT_MAX} requests/{RATE_LIMIT_WINDOW_SECONDS}s",
                headers={"Retry-After": str(max(1, retry_after))},
            )

# ======================================================
# AUTH HELPERS
# ======================================================

def create_access_token(username: str) -> str:
    now = int(time.time())
    payload = {
        "sub": username,
        "iat": now,
        "exp": now + ACCESS_TOKEN_EXPIRE_SECONDS,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def get_current_user(token: str = Depends(oauth2_scheme)) -> str:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        username = payload.get("sub")
        if not username:
            raise HTTPException(status_code=401, detail="Invalid token payload")
        return username
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

def get_api_key(api_key: str = Security(api_key_scheme)) -> str:
    # Here you can later support multiple keys (DB/list/etc.)
    if api_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or missing API Key",
        )
    return api_key

def require_auth(
    request: Request,
    user: str = Depends(get_current_user),
    api_key: str = Depends(get_api_key),
) -> str:
    """
    Require BOTH JWT + API Key + enforce quota for protected endpoints.
    """
    # Apply quota only on protected endpoints (this dependency is only used there)
    _check_and_consume_quota(api_key)
    return user

# ======================================================
# Helpers (data)
# ======================================================

def _read_parquet(rel_path: str) -> pd.DataFrame:
    path = f"{BASE_PATH}/{rel_path}"
    try:
        return pd.read_parquet(path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lecture Parquet: {path} | {e}")

def _read_json_dir(rel_path: str) -> pd.DataFrame:
    """
    Spark écrit le JSON dans un DOSSIER avec part-*.json
    """
    path = f"{BASE_PATH}/{rel_path}"
    files = glob.glob(f"{path}/part-*.json")
    if not files:
        raise HTTPException(status_code=500, detail=f"Aucun fichier JSON part-* trouvé dans: {path}")

    try:
        return pd.concat((pd.read_json(f, lines=True) for f in files), ignore_index=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lecture JSON: {path} | {e}")

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
        raise HTTPException(status_code=500, detail=f"Colonnes manquantes dans {name}: {missing}")

# ======================================================
# Public endpoints
# ======================================================

@app.get("/")
def root():
    return {
        "status": "API Climate opérationnelle",
        "docs": "/docs",
        "auth": {
            "jwt": "POST /token (x-www-form-urlencoded username/password)",
            "api_key_header": API_KEY_NAME
        },
        "quota": {
            "max_requests": RATE_LIMIT_MAX,
            "window_seconds": RATE_LIMIT_WINDOW_SECONDS,
            "scope": "per API key, protected endpoints only"
        }
    }

@app.post("/token")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Login and return JWT.
    Swagger will send x-www-form-urlencoded automatically.
    """
    if form_data.username != API_USER or form_data.password != API_PASS:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    token = create_access_token(form_data.username)
    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_in": ACCESS_TOKEN_EXPIRE_SECONDS,
    }

# ✅ Check auth after clicking "Authorize" in Swagger
@app.get("/auth/check")
def auth_check(user: str = Depends(require_auth)):
    return {"ok": True, "user": user}

# ======================================================
# Protected endpoints (JWT + API KEY + QUOTA)
# ======================================================

@app.get("/temperature/global/yearly")
def global_yearly_trend(
    year_min: Optional[int] = Query(None, ge=0),
    year_max: Optional[int] = Query(None, ge=0),
    user: str = Depends(require_auth),
):
    df = _read_parquet("global_yearly_trend")
    _ensure_cols(df, ["year"], "global_yearly_trend")
    df = _filter_year_range(df, year_min, year_max)
    df = df.sort_values("year")
    return df.to_dict(orient="records")

@app.get("/temperature/top-cities")
def top_cities(
    top_n: int = Query(10, ge=1, le=200),
    user: str = Depends(require_auth),
):
    if top_n <= 10:
        df = _read_json_dir("top_10_hottest_cities")
        _ensure_cols(df, ["Country", "City"], "top_10_hottest_cities")
        if "avg_temperature" in df.columns:
            df = df.sort_values("avg_temperature", ascending=False)
        return df.head(top_n).to_dict(orient="records")

    df = _read_json_dir("avg_temp_city")
    _ensure_cols(df, ["Country", "City", "avg_temperature"], "avg_temp_city")
    df = df.sort_values("avg_temperature", ascending=False).head(top_n)
    return df.to_dict(orient="records")

@app.get("/temperature/country/avg")
def avg_temp_by_country(
    top_n: int = Query(30, ge=1, le=300),
    contains: Optional[str] = Query(None),
    user: str = Depends(require_auth),
):
    df = _read_parquet("avg_temp_by_country")
    _ensure_cols(df, ["Country"], "avg_temp_by_country")

    if contains:
        df = df[df["Country"].str.contains(contains, case=False, na=False)]
    if "avg_temperature" in df.columns:
        df = df.sort_values("avg_temperature", ascending=False)

    return df.head(top_n).to_dict(orient="records")

@app.get("/temperature/country/{country}/yearly")
def country_yearly(
    country: str,
    year_min: Optional[int] = Query(None, ge=0),
    year_max: Optional[int] = Query(None, ge=0),
    user: str = Depends(require_auth),
):
    df = _read_parquet("avg_temp_country_year")
    _ensure_cols(df, ["Country", "year"], "avg_temp_country_year")

    df = df[df["Country"] == country]
    df = _filter_year_range(df, year_min, year_max)
    df = df.sort_values("year")
    return df.to_dict(orient="records")

@app.get("/temperature/city/yearly")
def city_yearly(
    country: str = Query(...),
    city: str = Query(...),
    year_min: Optional[int] = Query(None, ge=0),
    year_max: Optional[int] = Query(None, ge=0),
    user: str = Depends(require_auth),
):
    df = _read_parquet("avg_temp_city_year")
    _ensure_cols(df, ["Country", "City", "year"], "avg_temp_city_year")

    df = df[(df["Country"] == country) & (df["City"] == city)]
    df = _filter_year_range(df, year_min, year_max)
    df = df.sort_values("year")
    return df.to_dict(orient="records")

@app.get("/temperature/decade")
def avg_temp_by_decade(
    decade_min: Optional[int] = Query(None, ge=0),
    decade_max: Optional[int] = Query(None, ge=0),
    user: str = Depends(require_auth),
):
    df = _read_parquet("avg_temp_by_decade")
    _ensure_cols(df, ["decade"], "avg_temp_by_decade")

    if decade_min is not None:
        df = df[df["decade"] >= decade_min]
    if decade_max is not None:
        df = df[df["decade"] <= decade_max]

    df = df.sort_values("decade")
    return df.to_dict(orient="records")

@app.get("/temperature/variability")
def variability_top_cities(
    top_n: int = Query(20, ge=1, le=300),
    country: Optional[str] = Query(None),
    user: str = Depends(require_auth),
):
    df = _read_parquet("temp_variability_city")
    _ensure_cols(df, ["Country", "City", "temp_variability"], "temp_variability_city")

    if country:
        df = df[df["Country"] == country]

    df = df.sort_values("temp_variability", ascending=False).head(top_n)
    return df.to_dict(orient="records")

@app.get("/temperature/trend")
def warming_trend_top_cities(
    top_n: int = Query(20, ge=1, le=300),
    country: Optional[str] = Query(None),
    user: str = Depends(require_auth),
):
    df = _read_parquet("temp_trend_city")
    _ensure_cols(df, ["Country", "City", "temp_delta"], "temp_trend_city")

    if country:
        df = df[df["Country"] == country]

    df = df.sort_values("temp_delta", ascending=False).head(top_n)
    return df.to_dict(orient="records")