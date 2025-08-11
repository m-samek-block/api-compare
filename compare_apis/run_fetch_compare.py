#!/usr/bin/env python3
import subprocess, sys, pathlib

LAT, LON = 52.2297, 21.0122
START = "2025-08-11T00:00:00Z"
END   = "2025-08-13T00:00:00Z"
PROVIDERS = "openmeteo,metno,openweather,weatherapi,visualcrossing"

here = pathlib.Path(__file__).resolve().parent
dane = here / "dane"; dane.mkdir(exist_ok=True)
wyniki = here / "wyniki"; wyniki.mkdir(exist_ok=True)

def run(cmd):
    print(">>", " ".join(cmd))
    r = subprocess.run(cmd)
    if r.returncode != 0:
        sys.exit(r.returncode)

run([
    sys.executable, "fetch_forecasts.py",
    "--lat", str(LAT), "--lon", str(LON),
    "--start", START, "--end", END,
    "--providers", PROVIDERS,
    "--outdir", str(dane),
])

run([
    sys.executable, "run_compare.py",
    "--lat", str(LAT), "--lon", str(LON),
    "--start", START, "--end", END,
    "--providers", PROVIDERS,
    "--era5", str(dane / "era5.csv"),
    "--outdir", str(wyniki),
])

print("\nOK! Metryki: wyniki/era5_comparison_summary.csv\nWykresy: wyniki/plots/")
