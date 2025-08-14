#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_weather_analysis.py - Weather analysis with REAL DATA (Historical + Current/Forecast)

Funkcje:
- Dla danych historycznych: prawdziwe ERA5 z CDS + historical APIs
- Dla danych aktualnych/prognoz: porównanie realnych API między sobą
- BRAK sztucznych/syntetycznych danych
- Inteligentne wybieranie trybu analizy

Użycie:
  # Dane historyczne (z ERA5 reference)
  python run_weather_analysis.py --location warszawa --date-preset yesterday --use-cds
  
  # Dane aktualne/prognozy (porównanie API)
  python run_weather_analysis.py --location warszawa --date-preset forecast-3days
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Predefiniowane lokalizacje
LOCATIONS = {
    "warszawa": (52.2297, 21.0122),
    "krakow": (50.0647, 19.9450),
    "gdansk": (54.3520, 18.6466),
    "wroclaw": (51.1079, 17.0385),
    "poznan": (52.4064, 16.9252),
    "szczecin": (53.4285, 14.5528),
    "bydgoszcz": (53.1235, 18.0084),
    "lublin": (51.2465, 22.5684),
    "zakopane": (49.2992, 19.9496),
    "hel": (54.6086, 18.8067),
}

def get_date_range(preset: str):
    """Zwraca zakres dat dla różnych presetów"""
    now = datetime.now(timezone.utc)
    
    # Dane historyczne
    if preset == "yesterday": 
        end = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - timedelta(days=1)
    elif preset == "last-3days":
        end = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - timedelta(days=3)
    elif preset == "last-week":
        end = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - timedelta(days=7)
    elif preset == "last-month":
        end = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - timedelta(days=30)
    
    # Dane aktualne/prognozy
    elif preset == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
    elif preset == "forecast-3days":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=3)
    elif preset == "forecast-week":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=7)
    elif preset == "current-week":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=3)
        end = start + timedelta(days=7)  # 3 dni wstecz + 4 naprzód
    else:
        raise ValueError(f"Unknown date preset: {preset}")
    
    return start, end

def is_historical_data(start: datetime, end: datetime) -> bool:
    """Sprawdza czy dane są w pełni historyczne (starsze niż 6h)"""
    now = datetime.now(timezone.utc)
    return end < (now - timedelta(hours=6))

def check_cds_availability():
    """Sprawdza czy CDS API jest dostępne"""
    try:
        import cdsapi
        client = cdsapi.Client()
        print("[CDS] ✅ CDS API client configured")
        return True
    except ImportError:
        print("[CDS] ❌ cdsapi not installed")
        print("💡 Install with: pip install cdsapi")
        return False
    except Exception as e:
        print(f"[CDS] ❌ CDS API configuration error: {e}")
        print("💡 Check your ~/.cdsapirc file")
        print("💡 Get API key from: https://cds.climate.copernicus.eu/profile")
        return False

def download_cds_era5(lat: float, lon: float, start: datetime, end: datetime) -> Path:
    """Pobiera prawdziwe dane ERA5 z Copernicus CDS"""
    era5_file = Path("cache/era5") / f"era5_cds_{lat:.4f}_{lon:.4f}_{start.date()}_{end.date()}.csv"
    era5_file.parent.mkdir(parents=True, exist_ok=True)
    
    if era5_file.exists():
        # Sprawdź czy plik ma dane
        try:
            import pandas as pd
            df = pd.read_csv(era5_file)
            if len(df) > 0:
                print(f"[ERA5] ✅ Using existing CDS data: {era5_file}")
                print(f"[ERA5] 📊 Contains {len(df)} records")
                return era5_file
            else:
                print(f"[ERA5] ⚠️ Existing file is empty, re-downloading...")
        except Exception as e:
            print(f"[ERA5] ⚠️ Existing file corrupted ({e}), re-downloading...")
    
    print(f"[ERA5] 🌍 Downloading from Copernicus CDS...")
    
    # Sprawdź czy make_era5_cds.py istnieje
    make_cds_script = Path("make_era5_cds.py")
    if not make_cds_script.exists():
        print("[ERROR] ❌ make_era5_cds.py not found")
        print("💡 This file is required for CDS data download")
        sys.exit(1)
    
    # Uruchom pobieranie CDS
    cmd = [
        sys.executable, str(make_cds_script),
        "--lat", str(lat),
        "--lon", str(lon),
        "--start", start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "--end", end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "--out", str(era5_file)
    ]
    
    print(f"[ERA5] 🔄 Running CDS download...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        # Sprawdź czy plik rzeczywiście powstał i ma dane
        if era5_file.exists() and era5_file.stat().st_size > 0:
            try:
                import pandas as pd
                df = pd.read_csv(era5_file)
                if len(df) > 0:
                    print(f"[ERA5] ✅ CDS download successful - {len(df)} records")
                    return era5_file
            except Exception as e:
                print(f"[ERA5] ❌ Downloaded file is corrupted: {e}")
        
    print(f"[ERA5] ❌ CDS download failed:")
    if result.stdout:
        print(f"  stdout: {result.stdout}")
    if result.stderr:
        print(f"  stderr: {result.stderr}")
    
    # Dla danych historycznych, CDS jest wymagane
    print("\n💡 CDS API troubleshooting:")
    print("1. Check if you have ~/.cdsapirc configured correctly")
    print("2. Verify your CDS API key at: https://cds.climate.copernicus.eu/profile")
    print("3. Make sure cdsapi is installed: pip install cdsapi")
    print("4. Check CDS API status: https://cds.climate.copernicus.eu/live/queue")
    print("5. ERA5 data has ~5-7 day delay - use older dates")
    print("6. Check error message above for specific issues")
    sys.exit(1)

def create_baseline_reference(provider_data: dict, lat: float, lon: float, start: datetime, end: datetime) -> Path:
    """Tworzy baseline reference z najlepszego providera (dla prognoz)"""
    baseline_file = Path("cache/era5") / f"baseline_{lat:.4f}_{lon:.4f}_{start.date()}_{end.date()}.csv"
    baseline_file.parent.mkdir(parents=True, exist_ok=True)
    
    if baseline_file.exists():
        print(f"[BASELINE] ✅ Using existing baseline: {baseline_file}")
        return baseline_file
    
    # Wybierz najlepszy provider jako baseline (OpenMeteo ma najlepsze modele)
    if 'openmeteo' in provider_data and len(provider_data['openmeteo']) > 0:
        baseline_provider = 'openmeteo'
    elif provider_data:
        # Użyj pierwszego dostępnego
        baseline_provider = list(provider_data.keys())[0]
    else:
        print("[ERROR] ❌ No provider data available for baseline")
        sys.exit(1)
    
    print(f"[BASELINE] 📊 Creating baseline reference from {baseline_provider}")
    baseline_df = provider_data[baseline_provider].copy()
    
    # Zapisz jako baseline w formacie ERA5
    baseline_df.to_csv(baseline_file, index=False)
    print(f"[BASELINE] ✅ Created baseline: {len(baseline_df)} records")
    return baseline_file

def run_analysis(lat: float, lon: float, start: datetime, end: datetime, 
                providers: str, reference_file: Path, args, analysis_mode: str):
    """Uruchamia skonsolidowaną analizę"""
    
    # Sprawdź czy mamy skonsolidowany skrypt
    consolidated_script = Path("consolidated_analysis.py")
    if not consolidated_script.exists():
        print("[ERROR] ❌ consolidated_analysis.py not found")
        print("💡 This file is required for weather analysis")
        return False
    
    cmd = [
        sys.executable, str(consolidated_script),
        "--lat", str(lat),
        "--lon", str(lon),
        "--start", start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "--end", end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "--providers", providers,
        "--era5", str(reference_file)
    ]
    
    # Dodaj klucze API jeśli dostępne
    if args.openweather_key:
        cmd.extend(["--openweather-key", args.openweather_key])
    if args.weatherapi_key:
        cmd.extend(["--weatherapi-key", args.weatherapi_key])
    if args.visualcrossing_key:
        cmd.extend(["--visualcrossing-key", args.visualcrossing_key])
    
    # Dodaj informację o trybie analizy
    print(f"[ANALYSIS] 🚀 Starting {analysis_mode} analysis...")
    result = subprocess.run(cmd)
    return result.returncode == 0

def main():
    ap = argparse.ArgumentParser(description="Weather analysis with REAL DATA (historical + current/forecast)")
    
    # Lokalizacja
    location_group = ap.add_mutually_exclusive_group(required=True)
    location_group.add_argument("--location", choices=list(LOCATIONS.keys()),
                               help=f"Predefined location: {', '.join(LOCATIONS.keys())}")
    location_group.add_argument("--coords", nargs=2, type=float, metavar=("LAT", "LON"),
                               help="Custom coordinates (lat lon)")
    
    # Czas
    time_group = ap.add_mutually_exclusive_group(required=True)
    time_group.add_argument("--date-preset", choices=[
        # Dane historyczne
        "yesterday", "last-3days", "last-week", "last-month",
        # Dane aktualne/prognozy  
        "today", "forecast-3days", "forecast-week", "current-week"
    ], help="Predefined date range")
    time_group.add_argument("--custom-dates", nargs=2, metavar=("START", "END"),
                           help="Custom date range (ISO format: YYYY-MM-DDTHH:MM:SSZ)")
    
    # CDS (opcjonalne - tylko dla danych historycznych)
    ap.add_argument("--use-cds", action="store_true",
                   help="🌍 Use Copernicus CDS for real ERA5 data (for historical analysis)")
    
    # Opcje analizy
    ap.add_argument("--providers", type=str, 
                   default="openmeteo,metno,weatherapi,visualcrossing,openweather",
                   help="Comma-separated list of weather providers")
    
    # Klucze API
    ap.add_argument("--openweather-key", type=str, 
                   default=os.environ.get("OPENWEATHER_KEY", ""),
                   help="OpenWeather API key")
    ap.add_argument("--weatherapi-key", type=str,
                   default=os.environ.get("WEATHERAPI_KEY", ""),
                   help="WeatherAPI key")
    ap.add_argument("--visualcrossing-key", type=str,
                   default=os.environ.get("VISUALCROSSING_KEY", ""),
                   help="Visual Crossing API key")
    
    args = ap.parse_args()
    
    # Określ współrzędne
    if args.location:
        lat, lon = LOCATIONS[args.location]
        location_name = args.location.capitalize()
    else:
        lat, lon = args.coords
        location_name = f"{lat:.2f}°N, {lon:.2f}°E"
    
    # Określ zakres dat
    if args.date_preset:
        start, end = get_date_range(args.date_preset)
        time_desc = args.date_preset.replace("-", " ").title()
    else:
        start = datetime.fromisoformat(args.custom_dates[0].replace("Z", "+00:00")).astimezone(timezone.utc)
        end = datetime.fromisoformat(args.custom_dates[1].replace("Z", "+00:00")).astimezone(timezone.utc)
        time_desc = f"{start.date()} to {end.date()}"
    
    # Określ tryb analizy
    is_historical = is_historical_data(start, end)
    
    print("=" * 70)
    print("🌤️  WEATHER ANALYSIS - REAL DATA")
    print("=" * 70)
    print(f"📍 Location: {location_name}")
    print(f"📅 Time range: {time_desc}")
    print(f"🌐 Providers: {args.providers}")
    print(f"⏰ Analysis started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if is_historical:
        print(f"📊 Data type: HISTORICAL (Real ERA5 + Historical APIs)")
        analysis_mode = "HISTORICAL"
        
        # Dla danych historycznych sprawdź/wymagaj CDS
        if not args.use_cds:
            print("⚠️ WARNING: Historical data analysis without ERA5 reference")
            print("💡 Add --use-cds for accurate historical analysis with ERA5 baseline")
            
            response = input("Continue with provider-only comparison? (y/N): ").strip().lower()
            if response not in ('y', 'yes'):
                print("💡 Use --use-cds for historical analysis with ERA5 reference")
                sys.exit(1)
            analysis_mode = "PROVIDER_COMPARISON"
        
        if args.use_cds:
            # Sprawdź CDS dostępność
            if not check_cds_availability():
                print("\n❌ CDS API is not available")
                print("💡 Configure CDS API or continue without --use-cds")
                sys.exit(1)
            
            # Pobierz ERA5
            reference_file = download_cds_era5(lat, lon, start, end)
        else:
            print("[MODE] 📊 Provider comparison mode (no ERA5 reference)")
            # Później utworzymy baseline z providerów
            reference_file = None
            
    else:
        print(f"📊 Data type: CURRENT/FORECAST (Real API comparison)")
        analysis_mode = "FORECAST_COMPARISON"
        print("[MODE] 📈 Forecast/current data - provider comparison")
        reference_file = None
    
    print("=" * 70)
    
    # Jeśli nie mamy reference file, będziemy potrzebować stworzyć baseline z providerów
    if reference_file is None:
        print(f"[MODE] 🔄 Will create baseline reference from best provider")
    
    # Uruchom analizę z odpowiednim trybem
    if reference_file:
        # Mamy ERA5 reference
        success = run_analysis(lat, lon, start, end, args.providers, reference_file, args, analysis_mode)
    else:
        # Potrzebujemy najpierw pobrać dane providerów, potem stworzyć baseline
        print(f"[FETCH] 🌐 Pre-fetching provider data for baseline creation...")
        
        # Pobierz dane providerów
        fetch_script = Path("fetch_forecasts.py")
        if not fetch_script.exists():
            print("[ERROR] ❌ fetch_forecasts.py not found")
            sys.exit(1)
        
        temp_dir = Path("temp_fetch")
        temp_dir.mkdir(exist_ok=True)
        
        cmd = [
            sys.executable, str(fetch_script),
            "--lat", str(lat),
            "--lon", str(lon),
            "--start", start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "--end", end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "--providers", args.providers,
            "--outdir", str(temp_dir)
        ]
        
        # Dodaj klucze API
        if args.openweather_key:
            cmd.extend(["--openweather-key", args.openweather_key])
        if args.weatherapi_key:
            cmd.extend(["--weatherapi-key", args.weatherapi_key])
        if args.visualcrossing_key:
            cmd.extend(["--visualcrossing-key", args.visualcrossing_key])
        
        result = subprocess.run(cmd)
        if result.returncode != 0:
            print("[ERROR] ❌ Failed to fetch provider data")
            sys.exit(1)
        
        # Wczytaj dane providerów
        import pandas as pd
        provider_data = {}
        for provider_file in temp_dir.glob("provider_*.csv"):
            provider_name = provider_file.stem.replace("provider_", "")
            try:
                df = pd.read_csv(provider_file)
                if len(df) > 0:
                    provider_data[provider_name] = df
                    print(f"[FETCH] ✅ Loaded {provider_name}: {len(df)} records")
            except Exception as e:
                print(f"[FETCH] ⚠️ Error loading {provider_name}: {e}")
        
        if not provider_data:
            print("[ERROR] ❌ No provider data available")
            sys.exit(1)
        
        # Stwórz baseline reference
        reference_file = create_baseline_reference(provider_data, lat, lon, start, end)
        
        # Teraz uruchom analizę
        success = run_analysis(lat, lon, start, end, args.providers, reference_file, args, analysis_mode)
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    if success:
        print("\n" + "=" * 70)
        print("✅ ANALYSIS COMPLETE")
        print("=" * 70)
        print("📊 Results: central_weather_results.csv")
        print("🎨 Dashboard: dashboard/weather_analysis_dashboard.png")
        print("📈 Trends: dashboard/provider_trends.png")
        print("📋 Summary: dashboard/ANALYSIS_SUMMARY.md")
        print("💾 Cache: cache/")
        print(f"🔬 Analysis type: {analysis_mode}")
        print("\n🔍 View results with:")
        print("  python analysis_viewer.py overview")
        print("  python analysis_viewer.py compare")
        print("  python analysis_viewer.py trends")
        print("\n🌐 Open dashboard files:")
        print("  dashboard/weather_analysis_dashboard.png")
        print("  dashboard/ANALYSIS_SUMMARY.md")
    else:
        print("\n❌ ANALYSIS FAILED")
        print("Check logs above for error details")
        sys.exit(1)

if __name__ == "__main__":
    main()
