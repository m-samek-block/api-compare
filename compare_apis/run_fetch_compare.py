#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess, sys, pathlib, os
from datetime import datetime, timezone

# === USTAWIENIA ===
LAT, LON = 54.3520, 18.6466
START = "2025-08-12T00:00:00Z"
END   = "2025-08-19T00:00:00Z"
PROVIDERS = "openmeteo,metno,openweather,weatherapi,visualcrossing"
WIND_ALPHA = "0.143"

here = pathlib.Path(__file__).resolve().parent
dane = here / "dane"; dane.mkdir(exist_ok=True)
wyniki = here / "wyniki"; wyniki.mkdir(exist_ok=True)

# Klucze z ENV (opcjonalne)
OPENWEATHER = os.environ.get("OPENWEATHER_KEY", "")
WEATHERAPI  = os.environ.get("WEATHERAPI_KEY", "")
VISUALX     = os.environ.get("VISUALCROSSING_KEY", "")

def run(cmd):
    print(">>", " ".join(str(c) for c in cmd))
    r = subprocess.run(cmd)
    if r.returncode != 0:
        sys.exit(r.returncode)

# -------- 0) MAKE ERA5 (syntetyczny, dopasowany do LAT/LON/zakresu) --------
make_cmd = [
    sys.executable, "make_era5.py",
    "--lat", str(LAT), "--lon", str(LON),
    "--start", START, "--end", END,
    "--out", str(dane / "era5.csv"),
]
run(make_cmd)

# -------- 1) FETCH --------
fetch_cmd = [
    sys.executable, "fetch_forecasts.py",
    "--lat", str(LAT), "--lon", str(LON),
    "--start", START, "--end", END,
    "--providers", PROVIDERS,
    "--outdir", str(dane),
]
if OPENWEATHER: fetch_cmd += ["--openweather-key", OPENWEATHER]
if WEATHERAPI:  fetch_cmd += ["--weatherapi-key", WEATHERAPI]
if VISUALX:     fetch_cmd += ["--visualcrossing-key", VISUALX]
run(fetch_cmd)

# -------- 2) COMPARE --------
compare_cmd = [
    sys.executable, "run_compare.py",
    "--lat", str(LAT), "--lon", str(LON),
    "--start", START, "--end", END,
    "--providers", PROVIDERS,
    "--era5", str(dane / "era5.csv"),
    "--outdir", str(wyniki),
    "--wind-alpha", WIND_ALPHA,
]
if OPENWEATHER: compare_cmd += ["--openweather-key", OPENWEATHER]
if WEATHERAPI:  compare_cmd += ["--weatherapi-key", WEATHERAPI]
if VISUALX:     compare_cmd += ["--visualcrossing-key", VISUALX]
run(compare_cmd)

# -------- 3) STEMPEL CZASU W NAZWIE WYNIKU --------
summary = wyniki / "era5_comparison_summary.csv"
ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
stamped = wyniki / f"era5_comparison_summary_{ts}.csv"
if summary.exists():
    summary.replace(stamped)
    print(f"[OK] Zmieniono nazwę wyniku na: {stamped}")
else:
    print("[UWAGA] Nie znaleziono pliku era5_comparison_summary.csv do stemplowania.")

print("\nOK! Metryki:", stamped if stamped.exists() else summary, "\nWykresy:", wyniki / "plots", "\n")
