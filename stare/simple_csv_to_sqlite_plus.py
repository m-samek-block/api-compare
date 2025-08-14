#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import glob
import sqlite3
import sys
from typing import Optional

import pandas as pd
import requests

DEFAULT_VARS = "temperature_2m,precipitation,wind_speed_100m,wind_direction_100m"

# -------------------------- Schema helpers --------------------------

def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table,))
    return cur.fetchone() is not None

def existing_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info('{table}');")
    return {row[1] for row in cur.fetchall()}

def sql_type_for_series(s: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(s): return "INTEGER"
    if pd.api.types.is_float_dtype(s):   return "REAL"
    return "TEXT"

def ensure_sqlite_columns(conn: sqlite3.Connection, table: str, df: pd.DataFrame, verbose: bool = True) -> None:
    if not table_exists(conn, table):
        return
    ex = existing_columns(conn, table)
    missing = [c for c in df.columns if c not in ex]
    for col in missing:
        coltype = sql_type_for_series(df[col])
        conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" {coltype};')
        if verbose:
            print(f"# [auto-alter] added column {col} {coltype}", file=sys.stderr)
    if missing:
        conn.commit()

# -------------------------- Core IO --------------------------

def save_df(df: pd.DataFrame, conn: sqlite3.Connection, table: str, auto_alter: bool) -> int:
    n = len(df)
    if n == 0:
        return 0
    if auto_alter:
        ensure_sqlite_columns(conn, table, df, verbose=True)
    df.to_sql(table, conn, if_exists='append', index=False)
    return n

def import_csv(files, db, table, sep=",", encoding="utf-8", chunksize: Optional[int] = None, auto_alter: bool = True):
    conn = sqlite3.connect(db)
    try:
        total = 0
        for f in files:
            if chunksize:
                for chunk in pd.read_csv(f, sep=sep, encoding=encoding, chunksize=chunksize):
                    total += save_df(chunk, conn, table, auto_alter)
                print(f"✅ {f}: zapisano w chunkach (łącznie {total} wierszy narastająco)")
            else:
                df = pd.read_csv(f, sep=sep, encoding=encoding)
                total += save_df(df, conn, table, auto_alter)
                print(f"✅ {f}: zapisano {len(df)} wierszy")
        print(f"🎉 CSV → SQLite: suma {total} wierszy → {db}:{table}")
    finally:
        conn.close()

def fetch_previous_runs(start_date: str, end_date: str, lat: float, lon: float,
                        hourly_vars: str, timezone: str,
                        windspeed_unit: Optional[str], precipitation_unit: Optional[str],
                        model: Optional[str]) -> pd.DataFrame:
    url = "https://previous-runs-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": hourly_vars,
        "timeformat": "iso8601",
        "timezone": timezone,
    }
    if windspeed_unit:
        params["windspeed_unit"] = windspeed_unit   # kmh | ms | mph | kn
    if precipitation_unit:
        params["precipitation_unit"] = precipitation_unit  # mm | inch
    if model:
        params["models"] = model

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    if "hourly" not in data:
        raise SystemExit("⚠️ Brak sekcji 'hourly' w odpowiedzi API.")
    df = pd.DataFrame(data["hourly"])
    # dopisz współrzędne do DF (żeby trafiły do bazy)
    df["latitude"] = float(lat)
    df["longitude"] = float(lon)
    return df

def import_previous_runs(db, table, start, end, lat, lon, vars_csv, timezone,
                         windspeed_unit, precipitation_unit, model, auto_alter: bool):
    conn = sqlite3.connect(db)
    try:
        df = fetch_previous_runs(start, end, lat, lon, vars_csv, timezone, windspeed_unit, precipitation_unit, model)
        n = save_df(df, conn, table, auto_alter)
        print(f"🎉 Previous Runs → SQLite: zapisano {n} wierszy → {db}:{table}")
    finally:
        conn.close()

# -------------------------- CLI --------------------------

def main():
    ap = argparse.ArgumentParser(description="Importer CSV / Open-Meteo Previous Runs -> SQLite (append) z auto-ALTER.")
    ap.add_argument("--db", required=True, help="Ścieżka do pliku bazy SQLite, np. zeus.db")
    ap.add_argument("--table", required=True, help="Nazwa tabeli, np. weather")
    ap.add_argument("--no-auto-alter", action="store_true", help="Wyłącz automatyczne dodawanie brakujących kolumn.")

    # CSV mode
    ap.add_argument("--csv", nargs="+", help="Ścieżki do CSV lub wzorce glob (np. data/*.csv)")
    ap.add_argument("--sep", default=",", help="Separator w CSV")
    ap.add_argument("--encoding", default="utf-8", help="Kodowanie CSV")
    ap.add_argument("--chunksize", type=int, help="Czytaj CSV w kawałkach po N wierszy")

    # Previous Runs mode
    ap.add_argument("--start", help="Data początkowa YYYY-MM-DD (dla Previous Runs)")
    ap.add_argument("--end", help="Data końcowa YYYY-MM-DD (dla Previous Runs)")
    ap.add_argument("--lat", type=float, default=52.2297, help="Szerokość geogr.")
    ap.add_argument("--lon", type=float, default=21.0122, help="Długość geogr.")
    ap.add_argument("--vars", default=DEFAULT_VARS,
                    help=f"Lista zmiennych hourly (domyślnie: {DEFAULT_VARS})")
    ap.add_argument("--timezone", default="Europe/Warsaw", help="Strefa czasowa")
    ap.add_argument("--windspeed-unit", choices=["kmh","ms","mph","kn"], default="ms",
                    help="Jednostka wiatru (domyślnie ms)")
    ap.add_argument("--precipitation-unit", choices=["mm","inch"], default="mm",
                    help="Jednostka opadów (domyślnie mm)")
    ap.add_argument("--model", help="Model, np. gfs_global")

    args = ap.parse_args()
    csv_mode = args.csv is not None
    api_mode = (args.start is not None and args.end is not None)

    if csv_mode and api_mode:
        print("❌ Użyj ALBO --csv, ALBO (--start i --end) do Previous Runs.", file=sys.stderr); sys.exit(2)
    if not csv_mode and not api_mode:
        print("❌ Podaj --csv LUB zakres dat --start/--end.", file=sys.stderr); sys.exit(2)

    auto_alter = not args.no_auto_alter

    if csv_mode:
        files = []
        for pattern in args.csv:
            m = glob.glob(pattern)
            files.extend(m if m else [pattern])
        if not files:
            print("❌ Nie znaleziono żadnych plików CSV.", file=sys.stderr); sys.exit(2)
        import_csv(files, args.db, args.table, sep=args.sep, encoding=args.encoding,
                   chunksize=args.chunksize, auto_alter=auto_alter)
    else:
        import_previous_runs(args.db, args.table, args.start, args.end, args.lat, args.lon,
                             args.vars, args.timezone, args.windspeed_unit, args.precipitation_unit,
                             args.model, auto_alter=auto_alter)

if __name__ == "__main__":
    main()
