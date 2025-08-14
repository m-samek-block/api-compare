#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import subprocess, sys, pathlib, os, datetime as dt

# === KONFIG ===
LAT, LON = 52.2297, 21.0122
START = "2025-08-01T00:00:00Z"  # podaj zakres dostępny w CDS (ERA5 ma ~5–7 dni opóźnienia)
END = "2025-08-08T00:00:00Z"
PROVIDERS = "openmeteo,metno,openweather,weatherapi,visualcrossing"

# przeliczanie wiatru 10m->100m (gdy trzeba)
WIND_ALPHA = "0.143"
# próg opadu (mm/h) dla metryk POD/FAR/CSI
PRECIP_THRESH = "0.1"

here = pathlib.Path(__file__).resolve().parent
parent_dir = here.parent  # folder nadrzędny (compare_apis)
dane = here / "dane";
dane.mkdir(exist_ok=True)
wyniki = here / "wyniki";
wyniki.mkdir(exist_ok=True)
# Utwórz folder summaries
summaries = wyniki / "summaries";
summaries.mkdir(exist_ok=True)

# Klucze API z ENV (opcjonalne)
OPENWEATHER = os.environ.get("OPENWEATHER_KEY", "")
WEATHERAPI = os.environ.get("WEATHERAPI_KEY", "")
VISUALX = os.environ.get("VISUALCROSSING_KEY", "")


def find_script(script_name):
    """Znajdź skrypt - najpierw w bieżącym folderze, potem w folderze nadrzędnym."""
    local_path = here / script_name
    parent_path = parent_dir / script_name

    if local_path.exists():
        return str(local_path)
    elif parent_path.exists():
        return str(parent_path)
    else:
        raise FileNotFoundError(f"Nie znaleziono skryptu {script_name} ani w {here} ani w {parent_dir}")


def run(cmd):
    print(">>", " ".join(str(c) for c in cmd))
    r = subprocess.run(cmd)
    if r.returncode != 0:
        sys.exit(r.returncode)


def cds_safety_check(start_iso: str, end_iso: str, margin_days: int = 5):
    # ostrzeż, jeśli pytamy o przyszłość lub zbyt świeże dane
    now = dt.datetime.now(dt.UTC).replace(tzinfo=dt.timezone.utc)
    s = dt.datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    e = dt.datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
    if e > now:
        print("[WARN] --end jest w przyszłości. ERA5 nie istnieje dla przyszłości.")
    if e > (now - dt.timedelta(days=margin_days)):
        print(f"[WARN] Koniec okna jest nowszy niż {margin_days} dni. ERA5 może jeszcze nie być dostępne.")


# 0) Bezpieczeństwo dat
cds_safety_check(START, END, margin_days=5)

# 1) ERA5 z Copernicusa (prawdziwe) - skrypt lokalny
run([
    sys.executable, "make_era5_cds.py",
    "--lat", str(LAT), "--lon", str(LON),
    "--start", START, "--end", END,
    "--out", str(dane / "era5.csv"),
])

# 2) PROGNOZY z darmowych API - skrypt może być w folderze nadrzędnym
try:
    fetch_script = find_script("fetch_forecasts.py")
    fetch_cmd = [
        sys.executable, fetch_script,
        "--lat", str(LAT), "--lon", str(LON),
        "--start", START, "--end", END,
        "--providers", PROVIDERS,
        "--outdir", str(dane),
    ]
    if OPENWEATHER:
        fetch_cmd += ["--openweather-key", OPENWEATHER]
    if WEATHERAPI:
        fetch_cmd += ["--weatherapi-key", WEATHERAPI]
    if VISUALX:
        fetch_cmd += ["--visualcrossing-key", VISUALX]
    run(fetch_cmd)
except FileNotFoundError as e:
    print(f"[ERROR] {e}")
    print("[INFO] Pomijam pobieranie prognoz - brak skryptu fetch_forecasts.py")

# 3) PORÓWNANIE z ERA5 + wykresy + analizy - skrypt może być w folderze nadrzędnym
try:
    compare_script = find_script("run_compare.py")
    compare_cmd = [
        sys.executable, compare_script,
        "--lat", str(LAT), "--lon", str(LON),
        "--start", START, "--end", END,
        "--providers", PROVIDERS,
        "--era5", str(dane / "era5.csv"),
        "--outdir", str(wyniki),
        "--wind-alpha", WIND_ALPHA,
        "--precip-thresh", PRECIP_THRESH,
    ]
    # (opcjonalnie) podaj tu też klucze
    if OPENWEATHER:
        compare_cmd += ["--openweather-key", OPENWEATHER]
    if WEATHERAPI:
        compare_cmd += ["--weatherapi-key", WEATHERAPI]
    if VISUALX:
        compare_cmd += ["--visualcrossing-key", VISUALX]
    run(compare_cmd)
except FileNotFoundError as e:
    print(f"[ERROR] {e}")
    print("[INFO] Pomijam porównanie - brak skryptu run_compare.py")

# 4) Kopia summary z datami (łatwiej wersjonować) - TERAZ W FOLDERZE SUMMARIES
stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d_%H%M%SZ")
dest = summaries / f"era5_comparison_summary_CDS_{START[:10]}_{END[:10]}_{stamp}.csv"  # ZMIANA: summaries/
src = summaries / "era5_comparison_summary.csv"  # ZMIANA: szukamy w summaries/

if src.exists():
    src.replace(dest)
    print(f"[OK] Summary: {dest}")
else:
    print("[WARN] Brak 'era5_comparison_summary.csv' do skopiowania.")

print("\nOK!\n- ERA5: dane/era5.csv")
if (summaries / "era5_comparison_summary.csv").exists() or any(summaries.glob("era5_comparison_summary_*.csv")):
    print("- Metryki: wyniki/summaries/era5_comparison_summary_*.csv")  # ZMIANA: wskazuje na summaries/
if (wyniki / "plots").exists():
    print("- Wykresy: wyniki/plots/")
if (wyniki / "analysis").exists() or (wyniki / "analysis_history").exists():
    print("- Analizy: wyniki/analysis/ oraz wyniki/analysis_history/")