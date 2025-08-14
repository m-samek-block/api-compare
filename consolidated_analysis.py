#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
consolidated_analysis.py - Skonsolidowana analiza pogodowa (Z PRAWDZIWYMI ERA5)

Funkcje:
- Pobiera prawdziwe dane ERA5 z CDS używając make_era5_cds.py
- Porównuje wszystkich providerów z rzeczywistymi danymi ERA5
- Generuje dashboard i raporty

Użycie:
  python consolidated_analysis.py --lat 52.2297 --lon 21.0122 \
    --start 2025-08-14T00:00:00Z --end 2025-08-17T00:00:00Z \
    --providers openmeteo,metno,weatherapi,visualcrossing --location-name "Warszawa"
"""

import argparse
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# Fix matplotlib for Windows
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from dateutil import parser as dtparser

# Import funkcji z oryginalnych skryptów
import importlib.util
import sys

# Centralne lokalizacje
CACHE_DIR = Path("cache")
RESULTS_FILE = Path("central_weather_results.csv")
DASHBOARD_DIR = Path("dashboard")


def setup_directories():
    """Tworzy strukturę katalogów"""
    CACHE_DIR.mkdir(exist_ok=True)
    (CACHE_DIR / "era5").mkdir(exist_ok=True)
    (CACHE_DIR / "providers").mkdir(exist_ok=True)
    DASHBOARD_DIR.mkdir(exist_ok=True)


def load_fetch_functions():
    """Dynamiczne ładowanie funkcji z fetch_forecasts.py"""
    here = Path(__file__).resolve().parent
    fetch_path = here / "fetch_forecasts.py"

    if not fetch_path.exists():
        raise SystemExit("Missing fetch_forecasts.py in current directory")

    spec = importlib.util.spec_from_file_location("fetch_forecasts", fetch_path)
    fetch_forecasts = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fetch_forecasts)

    return fetch_forecasts


def fetch_real_era5_data(lat: float, lon: float, start: str, end: str, params: Dict) -> pd.DataFrame:
    """Pobiera prawdziwe dane ERA5 używając make_era5_cds.py"""
    
    # Sprawdź czy mamy cache
    era5_cache_file = CACHE_DIR / "era5" / f"era5_{params['hash']}.csv"
    
    if era5_cache_file.exists():
        print(f"[ERA5] ✅ Loading cached ERA5: {era5_cache_file}")
        try:
            df = pd.read_csv(era5_cache_file)
            df['time'] = pd.to_datetime(df['time'])
            return df
        except Exception as e:
            print(f"[ERA5] ⚠️ Cache read error: {e}")
    
    # Pobierz nowe dane
    print(f"[ERA5] 🌐 Fetching real ERA5 data from CDS...")
    
    # Sprawdź czy plik make_era5_cds.py istnieje
    era5_script = Path("make_era5_cds.py")
    if not era5_script.exists():
        raise SystemExit("❌ Missing make_era5_cds.py script")
    
    try:
        # Uruchom skrypt pobierania ERA5
        cmd = [
            "python", str(era5_script),
            "--lat", str(lat),
            "--lon", str(lon), 
            "--start", start,
            "--end", end,
            "--out", str(era5_cache_file)
        ]
        
        print(f"[ERA5] Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)  # 30 min timeout
        
        if result.returncode != 0:
            print(f"[ERA5] ❌ ERA5 script failed:")
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
            raise SystemExit("Failed to fetch ERA5 data")
        
        print(f"[ERA5] ✅ ERA5 script completed successfully")
        
        # Load the generated data
        if era5_cache_file.exists():
            df = pd.read_csv(era5_cache_file)
            df['time'] = pd.to_datetime(df['time'])
            print(f"[ERA5] ✅ Loaded {len(df)} ERA5 records")
            return df
        else:
            raise SystemExit("ERA5 script completed but no output file found")
            
    except subprocess.TimeoutExpired:
        raise SystemExit("❌ ERA5 data fetch timed out (>30 minutes)")
    except Exception as e:
        raise SystemExit(f"❌ Error fetching ERA5 data: {e}")


def convert_long_to_wide_era5(era5_long: pd.DataFrame) -> pd.DataFrame:
    """Konwertuje long format ERA5 do wide format dla analizy"""
    
    # Pivot table to convert from long to wide format
    wide_df = era5_long.pivot_table(
        index=['time', 'latitude', 'longitude'],
        columns='variable', 
        values='value',
        aggfunc='first'
    ).reset_index()
    
    # Flatten column names
    wide_df.columns.name = None
    
    # Rename columns to match provider format
    column_mapping = {
        'temperature_2m': 'temperature_2m',
        'precipitation': 'precipitation', 
        'wind_speed_100m': 'wind_speed_100m',
        'wind_direction_100m': 'wind_direction_100m'
    }
    
    # Apply mapping
    for old_name, new_name in column_mapping.items():
        if old_name in wide_df.columns:
            wide_df = wide_df.rename(columns={old_name: new_name})
    
    print(f"[ERA5] ✅ Converted to wide format: {len(wide_df)} records")
    print(f"[ERA5] Variables: {[col for col in wide_df.columns if col not in ['time', 'latitude', 'longitude']]}")
    
    return wide_df


def generate_data_hash(params: Dict) -> str:
    """Generate unique hash for this data configuration"""
    import hashlib
    hash_str = f"{params['lat']:.4f}_{params['lon']:.4f}_{params['start']}_{params['end']}_" + \
               "_".join(sorted(params['providers']))
    return hashlib.md5(hash_str.encode()).hexdigest()[:12]


def load_or_create_central_results() -> pd.DataFrame:
    """Load central results file or create empty one"""
    if RESULTS_FILE.exists():
        try:
            # Fix potential date parsing issues
            df = pd.read_csv(RESULTS_FILE)
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')
            return df
        except Exception as e:
            print(f"⚠️ Error loading {RESULTS_FILE}: {e}")
            print("Creating new results file...")

    # Create empty results structure
    columns = [
        'hash', 'timestamp', 'location_name', 'lat', 'lon',
        'start_time', 'end_time', 'provider', 'analysis_type',
        'temperature_2m_rmse', 'temperature_2m_bias', 'temperature_2m_correlation', 'temperature_2m_n',
        'precipitation_rmse', 'precipitation_bias', 'precipitation_correlation', 'precipitation_n',
        'wind_speed_100m_rmse', 'wind_speed_100m_bias', 'wind_speed_100m_correlation', 'wind_speed_100m_n',
        'wind_direction_100m_rmse', 'wind_direction_100m_bias', 'wind_direction_100m_correlation', 'wind_direction_100m_n',
        'overall_score', 'data_points_total', 'notes'
    ]
    return pd.DataFrame(columns=columns)


def append_results_to_central(new_results: pd.DataFrame):
    """Append new results to central file, avoiding duplicates"""
    if new_results.empty:
        print("⚠️ No new results to append")
        return

    # Load existing
    existing = load_or_create_central_results()

    # Remove any existing entries with same hash to prevent duplicates
    if not existing.empty and 'hash' in new_results.columns:
        for hash_val in new_results['hash'].unique():
            existing = existing[existing['hash'] != hash_val]
            print(f"[RESULTS] Removed existing data for hash: {hash_val}")

    # Append new results
    combined = pd.concat([existing, new_results], ignore_index=True)

    # Save
    combined.to_csv(RESULTS_FILE, index=False)
    print(f"[RESULTS] ✅ Updated central results: {RESULTS_FILE} ({len(combined)} total records)")


def fetch_and_cache_provider_data(provider: str, params: Dict, fetch_module) -> Optional[pd.DataFrame]:
    """Fetch provider data and cache it"""
    cache_file = CACHE_DIR / "providers" / f"{provider}_{params['hash']}.csv"

    if cache_file.exists():
        print(f"[CACHE] ✅ Loading cached {provider}: {cache_file}")
        try:
            return pd.read_csv(cache_file, parse_dates=['time'])
        except Exception as e:
            print(f"[CACHE] ⚠️ Cache read error for {provider}: {e}")

    # Fetch new data
    print(f"[FETCH] 🌐 Fetching REAL data from {provider}...")
    try:
        if provider == "openmeteo":
            data = fetch_module.fetch_openmeteo_data(
                params['lat'], params['lon'], params['start'], params['end'], use_forecast=True
            )
        elif provider == "metno":
            data = fetch_module.fetch_metno_data(
                params['lat'], params['lon'], params['start'], params['end'], use_forecast=True
            )
        elif provider == "weatherapi":
            data = fetch_module.fetch_weatherapi_data(
                params['lat'], params['lon'], params['start'], params['end'], use_forecast=True
            )
        elif provider == "visualcrossing":
            data = fetch_module.fetch_visualcrossing_data(
                params['lat'], params['lon'], params['start'], params['end'], use_forecast=True
            )
        elif provider == "openweather":
            data = fetch_module.fetch_openweather_data(
                params['lat'], params['lon'], params['start'], params['end'], use_forecast=True
            )
        else:
            print(f"[FETCH] ❌ Unknown provider: {provider}")
            return None

        if data is not None and not data.empty:
            # Cache the data
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            data.to_csv(cache_file, index=False)
            print(f"[CACHE] ✅ Cached {provider}: {len(data)} records")
            print(f"  ✅ {len(data)} real data points fetched")
            return data
        else:
            print(f"  ❌ No real data returned")
            return None

    except Exception as e:
        print(f"[FETCH] ❌ Error fetching {provider}: {e}")
        return None


def calculate_weather_metrics(era5_data: pd.DataFrame, provider_data: pd.DataFrame,
                              variables: List[str]) -> Dict:
    """Calculate RMSE, bias, correlation for weather variables vs ERA5"""
    results = {}

    for var in variables:
        if var not in era5_data.columns or var not in provider_data.columns:
            print(f"  ⚠️ No {var} data for comparison")
            results[var] = {
                'rmse': np.nan, 'bias': np.nan, 'correlation': np.nan, 'n': 0
            }
            continue

        # Align data by time
        era5_sorted = era5_data.sort_values('time').set_index('time')
        provider_sorted = provider_data.sort_values('time').set_index('time')

        # Find overlapping times
        common_times = era5_sorted.index.intersection(provider_sorted.index)

        if len(common_times) == 0:
            print(f"  ⚠️ No overlapping times for {var}")
            results[var] = {
                'rmse': np.nan, 'bias': np.nan, 'correlation': np.nan, 'n': 0
            }
            continue

        era5_vals = era5_sorted.loc[common_times, var].dropna()
        provider_vals = provider_sorted.loc[common_times, var].dropna()

        # Align again after dropna
        common_indices = era5_vals.index.intersection(provider_vals.index)
        if len(common_indices) == 0:
            results[var] = {
                'rmse': np.nan, 'bias': np.nan, 'correlation': np.nan, 'n': 0
            }
            continue

        era5_final = era5_vals.loc[common_indices]
        provider_final = provider_vals.loc[common_indices]

        # Calculate metrics
        rmse = np.sqrt(np.mean((era5_final - provider_final) ** 2))
        bias = np.mean(provider_final - era5_final)
        correlation = np.corrcoef(era5_final, provider_final)[0, 1] if len(era5_final) > 1 else np.nan

        results[var] = {
            'rmse': rmse,
            'bias': bias,
            'correlation': correlation,
            'n': len(era5_final)
        }

        print(f"  ✅ {var}: RMSE={rmse:.3f}, correlation={correlation:.3f}, n={len(era5_final)}")

    return results


def safe_polyfit_trend(x_data, y_data):
    """Safely calculate polynomial fit trend, handling length mismatches."""
    try:
        x = np.array(x_data)
        y = np.array(y_data)
        
        # Remove NaN values from both arrays simultaneously
        mask = ~(np.isnan(x) | np.isnan(y))
        x_clean = x[mask]
        y_clean = y[mask]
        
        if len(x_clean) < 2 or len(y_clean) < 2:
            return 0.0
            
        # Ensure arrays have the same length
        min_length = min(len(x_clean), len(y_clean))
        x_final = x_clean[:min_length]
        y_final = y_clean[:min_length]
        
        if len(x_final) != len(y_final):
            return 0.0
            
        slope = np.polyfit(x_final, y_final, 1)[0]
        return float(slope)
        
    except Exception as e:
        print(f"⚠️ Error calculating trend: {str(e)}")
        return 0.0


def create_dashboard_plots(all_results: pd.DataFrame, current_hash: str):
    """Create enhanced dashboard with multiple visualizations"""
    if all_results.empty:
        print("⚠️ No results to visualize")
        return

    # Create figure with subplots
    fig = plt.figure(figsize=(20, 16))
    gs = fig.add_gridspec(4, 3, hspace=0.3, wspace=0.3)

    # Color scheme
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    
    # Current run data
    current_data = all_results[all_results['hash'] == current_hash]

    # 1. Temperature RMSE by Provider (Current Run)
    ax1 = fig.add_subplot(gs[0, 0])
    if not current_data.empty:
        temp_data = current_data.dropna(subset=['temperature_2m_rmse'])
        if not temp_data.empty:
            bars = ax1.bar(temp_data['provider'], temp_data['temperature_2m_rmse'], 
                          color=colors[:len(temp_data)])
            ax1.set_title('🌡️ Temperature RMSE vs ERA5', fontsize=12, fontweight='bold')
            ax1.set_ylabel('RMSE (°C)')
            ax1.tick_params(axis='x', rotation=45)
            
            # Add value labels on bars
            for bar, value in zip(bars, temp_data['temperature_2m_rmse']):
                ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                        f'{value:.1f}', ha='center', va='bottom')

    # 2. Overall Score Comparison
    ax2 = fig.add_subplot(gs[0, 1])
    if not current_data.empty:
        score_data = current_data.dropna(subset=['overall_score'])
        if not score_data.empty:
            bars = ax2.bar(score_data['provider'], score_data['overall_score'], 
                          color=colors[:len(score_data)])
            ax2.set_title('🏆 Overall Score vs ERA5', fontsize=12, fontweight='bold')
            ax2.set_ylabel('Score')
            ax2.tick_params(axis='x', rotation=45)

    # Add main title and metadata
    fig.suptitle('🌤️ Weather Provider Analysis vs Real ERA5 Data 📊', 
                fontsize=20, fontweight='bold', y=0.98)
    
    # Add metadata
    metadata_text = f"📍 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | " \
                   f"🌍 Current Hash: {current_hash[:8]} | " \
                   f"🔢 Total Records: {len(all_results)} | " \
                   f"✅ Real ERA5 Data"
    
    fig.text(0.5, 0.02, metadata_text, ha='center', va='bottom', 
            fontsize=10, style='italic')

    # Save dashboard
    dashboard_file = DASHBOARD_DIR / "weather_analysis_dashboard.png"
    plt.tight_layout()
    plt.savefig(dashboard_file, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()

    print(f"[DASHBOARD] ✅ Enhanced dashboard: {dashboard_file}")


def create_summary_report(all_results: pd.DataFrame):
    """Create comprehensive summary report"""
    print("\n[SUMMARY] 📋 Creating comprehensive analysis report...")
    
    if all_results.empty:
        print("⚠️ No results to analyze")
        return
    
    # Generate report
    report_lines = [
        "🌟 WEATHER PROVIDER ANALYSIS vs REAL ERA5 DATA",
        "=" * 60,
        f"📊 Total analyses: {len(all_results)}",
        f"🌐 Providers analyzed: {', '.join(all_results['provider'].unique())}",
        f"📅 Date range: {all_results['timestamp'].min()} to {all_results['timestamp'].max()}",
        f"✅ Reference: Real ERA5 data from Copernicus CDS",
        "",
        "🏆 BEST PERFORMERS vs ERA5:",
        "=" * 35,
    ]
    
    # Best performers
    metrics_to_check = ['temperature_2m_rmse', 'precipitation_rmse', 'wind_speed_100m_rmse', 'overall_score']
    for metric in metrics_to_check:
        if metric in all_results.columns and not all_results[metric].isna().all():
            if 'score' in metric:
                best_idx = all_results[metric].idxmax()  # Higher score is better
                best_val = all_results.loc[best_idx, metric]
                symbol = "📈"
            else:
                best_idx = all_results[metric].idxmin()  # Lower RMSE is better
                best_val = all_results.loc[best_idx, metric]
                symbol = "🎯"
                
            best_provider = all_results.loc[best_idx, 'provider']
            report_lines.append(f"{symbol} {metric.replace('_', ' ').title()}: {best_provider} ({best_val:.3f})")
    
    # Save report
    report_file = DASHBOARD_DIR / "weather_analysis_summary.txt"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    print(f"✅ Summary report saved: {report_file}")


def main():
    parser = argparse.ArgumentParser(description='Consolidated Weather Analysis with Real ERA5')
    parser.add_argument('--lat', type=float, required=True, help='Latitude')
    parser.add_argument('--lon', type=float, required=True, help='Longitude')
    parser.add_argument('--start', required=True, help='Start time (ISO format)')
    parser.add_argument('--end', required=True, help='End time (ISO format)')
    parser.add_argument('--providers', required=True, help='Comma-separated list of providers')
    parser.add_argument('--location-name', default='Unknown', help='Location name for reports')
    parser.add_argument('--analysis-type', default='forecast_vs_era5', help='Type of analysis')

    args = parser.parse_args()

    # Setup
    setup_directories()

    # Load modules
    fetch_module = load_fetch_functions()

    # Parse parameters
    providers = [p.strip() for p in args.providers.split(',')]
    params = {
        'lat': args.lat,
        'lon': args.lon,
        'start': args.start,
        'end': args.end,
        'providers': providers,
        'location_name': args.location_name,
        'analysis_type': args.analysis_type
    }

    # Generate hash for this configuration
    data_hash = generate_data_hash(params)
    params['hash'] = data_hash

    print("🌤️ CONSOLIDATED WEATHER ANALYSIS vs REAL ERA5")
    print("=" * 60)
    print(f"📍 Location: {args.location_name} ({args.lat:.4f}, {args.lon:.4f})")
    print(f"📅 Period: {args.start} to {args.end}")
    print(f"🌐 Providers: {', '.join(providers)}")
    print(f"📊 Analysis: {args.analysis_type}")
    print(f"🔑 Hash: {data_hash}")
    print(f"✅ Reference: Real ERA5 data from CDS")

    # 1. Get real ERA5 data
    print(f"\n[ERA5] 📊 Fetching real ERA5 reference data...")
    try:
        era5_long = fetch_real_era5_data(args.lat, args.lon, args.start, args.end, params)
        era5_data = convert_long_to_wide_era5(era5_long)
        
        if era5_data is None or era5_data.empty:
            raise SystemExit("❌ No ERA5 data available")
        print(f"[ERA5] ✅ ERA5 data: {len(era5_data)} records")
    except Exception as e:
        raise SystemExit(f"❌ Error loading ERA5 data: {e}")

    # 2. Fetch provider data
    provider_datasets = {}
    print(f"\n[PROVIDERS] 🌐 Fetching data from {len(providers)} providers...")

    for provider in providers:
        data = fetch_and_cache_provider_data(provider, params, fetch_module)
        if data is not None and not data.empty:
            provider_datasets[provider] = data
            print(f"[PROVIDERS] ✅ {provider}: {len(data)} records")
        else:
            print(f"[PROVIDERS] ❌ {provider}: No data")

    if not provider_datasets:
        raise SystemExit("❌ No provider data available")

    # 3. Run analysis for each provider vs ERA5
    print(f"\n[ANALYSIS] 🔬 Analyzing {len(provider_datasets)} providers vs ERA5...")
    results_rows = []

    weather_variables = ['temperature_2m', 'precipitation', 'wind_speed_100m', 'wind_direction_100m']

    for provider, provider_data in provider_datasets.items():
        print(f"[ANALYSIS] Analyzing {provider} vs ERA5...")

        # Calculate metrics
        metrics = calculate_weather_metrics(era5_data, provider_data, weather_variables)

        # Calculate overall score (lower RMSE = better score)
        temp_rmse = metrics.get('temperature_2m', {}).get('rmse', np.inf)
        overall_score = 1.0 / (1.0 + temp_rmse) if not np.isnan(temp_rmse) else 0.0

        # Create result row
        result = {
            'hash': data_hash,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'location_name': args.location_name,
            'lat': args.lat,
            'lon': args.lon,
            'start_time': args.start,
            'end_time': args.end,
            'provider': provider,
            'analysis_type': args.analysis_type,
            'overall_score': overall_score,
            'data_points_total': len(provider_data),
            'notes': f"Real data analysis vs Real ERA5 from CDS"
        }

        # Add metrics for each variable
        for var in weather_variables:
            if var in metrics:
                result[f'{var}_rmse'] = metrics[var]['rmse']
                result[f'{var}_bias'] = metrics[var]['bias']
                result[f'{var}_correlation'] = metrics[var]['correlation']
                result[f'{var}_n'] = metrics[var]['n']

        results_rows.append(result)

    # 4. Save results
    new_results = pd.DataFrame(results_rows)
    append_results_to_central(new_results)

    # 5. Generate enhanced dashboard
    print(f"\n[DASHBOARD] 🎨 Generating visualizations...")
    all_results = load_or_create_central_results()
    create_dashboard_plots(all_results, data_hash)
    create_summary_report(all_results)

    print(f"\n🎉 [SUCCESS] Analysis complete!")
    print(f"📊 Central results: {RESULTS_FILE}")
    print(f"🎨 Enhanced dashboard: {DASHBOARD_DIR}/")
    print(f"💾 Cache: {CACHE_DIR}/")
    print(f"✅ Used REAL ERA5 data as reference")


if __name__ == "__main__":
    main()