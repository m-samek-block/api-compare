#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sqlite3, requests, sys
from typing import List
import pandas as pd
from datetime import datetime, timedelta

# ===== KONFIG =====
DB = "zeus.db"
TABLE = "weather"

# pełny zakres, jaki chcesz pobrać
START_DATE = "2025-01-01"
END_DATE   = "2025-08-11"

# długość pojedynczego zapytania w dniach
CHUNK_DAYS = 7

TIMEZONE = "Europe/Warsaw"
VARS = "temperature_2m,precipitation,wind_speed_100m,wind_direction_100m"
WINDSPEED_UNIT = "ms"
PRECIP_UNIT = "mm"
MODEL = "gfs_global"   # albo None

LOCATIONS = [
    {"name": "Warszawa", "lat": 52.2297, "lon": 21.0122},
    {"name": "Kraków",   "lat": 50.0647, "lon": 19.9450},
    {"name": "Gdańsk",   "lat": 54.3520, "lon": 18.6466},
    {"name": "Wrocław",  "lat": 51.1079, "lon": 17.0385},
    {"name": "Poznań",   "lat": 52.4064, "lon": 16.9252},
]

# ===== FUNKCJE =====
def ensure_columns(conn: sqlite3.Connection, df: pd.DataFrame):
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (TABLE,))
    if cur.fetchone() is None:
        return
    ex = {row[1] for row in conn.execute(f'PRAGMA table_info("{TABLE}");').fetchall()}
    missing = [c for c in df.columns if c not in ex]
    for col in missing:
        if pd.api.types.is_integer_dtype(df[col]):
            coltype = "INTEGER"
        elif pd.api.types.is_float_dtype(df[col]):
            coltype = "REAL"
        else:
            coltype = "TEXT"
        conn.execute(f'ALTER TABLE "{TABLE}" ADD COLUMN "{col}" {coltype};')
    if missing:
        conn.commit()

def chunk_ranges(start_date: str, end_date: str, chunk_days: int):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end   = datetime.strptime(end_date, "%Y-%m-%d")
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=chunk_days), end)
        yield cur.strftime("%Y-%m-%d"), nxt.strftime("%Y-%m-%d")
        cur = nxt

def fetch(lat: float, lon: float, start_date: str, end_date: str) -> pd.DataFrame:
    url = "https://previous-runs-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": VARS,
        "timeformat": "iso8601",
        "timezone": TIMEZONE,
        "windspeed_unit": WINDSPEED_UNIT,
        "precipitation_unit": PRECIP_UNIT,
    }
    if MODEL:
        params["models"] = MODEL
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    j = r.json()
    if "hourly" not in j:
        raise RuntimeError(f"Brak 'hourly' w odpowiedzi ({lat},{lon}) {start_date}..{end_date}")
    df = pd.DataFrame(j["hourly"])
    df["latitude"] = float(lat)
    df["longitude"] = float(lon)
    return df

# ===== GŁÓWNA LOGIKA =====
def main():
    conn = sqlite3.connect(DB)
    total = 0
    try:
        for loc in LOCATIONS:
            name, lat, lon = loc["name"], loc["lat"], loc["lon"]
            print(f"==> {name} ({lat},{lon}) {START_DATE}..{END_DATE}")
            all_chunks: List[pd.DataFrame] = []
            for s, e in chunk_ranges(START_DATE, END_DATE, CHUNK_DAYS):
                print(f"   - pobieram chunk {s}..{e}")
                df_chunk = fetch(lat, lon, s, e)
                all_chunks.append(df_chunk)
            if not all_chunks:
                print("   brak danych")
                continue
            df = pd.concat(all_chunks, ignore_index=True)
            ensure_columns(conn, df)
            df.to_sql(TABLE, conn, if_exists="append", index=False)
            print(f"   +{len(df)} wierszy")
            total += len(df)
        print(f"✅ Suma: {total} → {DB}:{TABLE}")
    finally:
        conn.close()

if __name__ == "__main__":
    sys.exit(main())
