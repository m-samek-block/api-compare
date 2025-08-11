from fastapi import FastAPI, HTTPException, Query
import numpy as np
import pandas as pd
import math
import os, sqlite3
from pathlib import Path


DB_PATH = os.getenv("ZEUS_DB_PATH", r"C:\Users\msamek\PycharmProjects\FastAPIProject\zeus.db")
TABLE = os.getenv("ZEUS_SQLITE_TABLE", "weather")
TIME_COL = os.getenv("ZEUS_SQLITE_TIMECOL", "time")
CORE_VARS = ["temperature_2m", "precipitation", "wind_speed_100m", "wind_direction_100m"]

app = FastAPI(title="Zeus DB API", version="1.0.0")

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _deg_box(lat0, lon0, radius_km: float):
    lat_deg = radius_km / 111.0
    lon_deg = radius_km / (111.0 * max(0.1, math.cos(math.radians(lat0))))
    return lat_deg, lon_deg

def _haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0088
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat/2.0)**2 + np.cos(np.radians(lat1))*np.cos(np.radians(lat2)) * np.sin(dlon/2.0)**2
    return 2*R*np.arcsin(np.sqrt(a))

def _detect_latlon_cols(conn):
    info = pd.read_sql_query(f'PRAGMA table_info("{TABLE}")', conn)
    mp = {c.lower(): c for c in info["name"]}
    lat_col = mp.get("latitude") or mp.get("lat")
    lon_col = mp.get("longitude") or mp.get("lon")
    if not lat_col or not lon_col:
        raise HTTPException(status_code=500, detail=f'Nie znaleziono kolumn latitude/longitude (ani lat/lon) w tabeli "{TABLE}"')
    return lat_col, lon_col

@app.get("/forecast")
async def forecast(
    start: str,
    end: str,
    lat: float = Query(..., alias="latitude"),
    lon: float = Query(..., alias="longitude"),
    mode: str = Query("nearest", pattern="^(nearest|mean)$"),
    radius_km: float = Query(10.0, ge=0.1, le=200.0)
):
    cols = ["temperature_2m", "precipitation", "wind_speed_100m", "wind_direction_100m"]

    if not os.path.exists(DB_PATH):
        raise HTTPException(status_code=404, detail=f"DB not found: {DB_PATH}")

    with get_conn() as conn:
        # weryfikacja tabeli
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (TABLE,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=404, detail=f"Table {TABLE} not found")

        lat_col, lon_col = _detect_latlon_cols(conn)
        lat_deg, lon_deg = _deg_box(lat, lon, radius_km)
        lat_min, lat_max = lat - lat_deg, lat + lat_deg
        lon_min, lon_max = lon - lon_deg, lon + lon_deg

        select_cols = [f'"{TIME_COL}" AS time', f'"{lat_col}" AS latitude', f'"{lon_col}" AS longitude'] + [f'"{c}"' for c in cols]
        q = f'''
            SELECT {", ".join(select_cols)}
            FROM "{TABLE}"
            WHERE "{TIME_COL}" >= ? AND "{TIME_COL}" < ?
              AND "{lat_col}" BETWEEN ? AND ?
              AND "{lon_col}" BETWEEN ? AND ?
            ORDER BY "{TIME_COL}" ASC
        '''
        df = pd.read_sql_query(q, conn, params=[start, end, lat_min, lat_max, lon_min, lon_max])

    # Pusta odpowiedź w stylu Open-Meteo
    if df.empty:
        return {
            "latitude": lat,
            "longitude": lon,
            "hourly": {
                "time": [],
                "temperature_2m": [],
                "precipitation": [],
                "wind_speed_100m": [],
                "wind_direction_100m": []
            }
        }

    df["dist_km"] = _haversine_km(lat, lon, df["latitude"].values, df["longitude"].values)

    if mode == "nearest":
        idx = df.groupby("time")["dist_km"].idxmin()
        dfg = df.loc[idx].sort_values("time")
    else:
        dfg = df[df["dist_km"] <= radius_km].copy()
        if dfg.empty:
            idx = df.groupby("time")["dist_km"].idxmin()
            dfg = df.loc[idx]
        dfg = dfg.groupby("time", as_index=False)[cols].mean(numeric_only=True).sort_values("time")

    times = dfg["time"].astype(str).tolist()

    hourly = {
        "time": times,
        "temperature_2m":      dfg["temperature_2m"].astype(float).where(pd.notna(dfg["temperature_2m"]), None).tolist(),
        "precipitation":       dfg["precipitation"].astype(float).where(pd.notna(dfg["precipitation"]), None).tolist(),
        "wind_speed_100m":     dfg["wind_speed_100m"].astype(float).where(pd.notna(dfg["wind_speed_100m"]), None).tolist(),
        "wind_direction_100m": dfg["wind_direction_100m"].astype(float).where(pd.notna(dfg["wind_direction_100m"]), None).tolist(),
    }

    # Minimalny, zgodny z Open-Meteo kształt odpowiedzi
    return {
        "latitude": lat,
        "longitude": lon,
        "hourly": hourly
    }
