#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
batch_weather_analysis.py - Batch processing wielu lokalizacji i okresów

Funkcje:
- Analiza wielu miast jednocześnie
- Porównanie regionalne (Polska vs Europa vs Świat)
- Analiza sezonowa i długoterminowa
- Automatyczne generowanie raportów porównawczych
- Parallel processing dla szybszych analiz
"""

import argparse
import concurrent.futures
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

# Predefiniowane zestawy lokalizacji
LOCATION_SETS = {
    "poland_major": {
        "warszawa": (52.2297, 21.0122),
        "krakow": (50.0647, 19.9450),
        "gdansk": (54.3520, 18.6466),
        "wroclaw": (51.1079, 17.0385),
        "poznan": (52.4064, 16.9252),
        "szczecin": (53.4285, 14.5528),
        "lublin": (51.2465, 22.5684),
        "bydgoszcz": (53.1235, 18.0084),
        "katowice": (50.2649, 19.0238),
        "rzeszow": (50.0412, 21.9991)
    },
    
    "europe_capitals": {
        "warsaw": (52.2297, 21.0122),
        "berlin": (52.5200, 13.4050),
        "paris": (48.8566, 2.3522),
        "london": (51.5074, -0.1278),
        "rome": (41.9028, 12.4964),
        "madrid": (40.4168, -3.7038),
        "amsterdam": (52.3676, 4.9041),
        "vienna": (48.2082, 16.3738),
        "prague": (50.0755, 14.4378),
        "stockholm": (59.3293, 18.0686)
    },
    
    "world_major": {
        "warsaw": (52.2297, 21.0122),
        "london": (51.5074, -0.1278),
        "new_york": (40.7128, -74.0060),
        "tokyo": (35.6762, 139.6503),
        "sydney": (-33.8688, 151.2093),
        "cape_town": (-33.9249, 18.4241),
        "sao_paulo": (-23.5505, -46.6333),
        "dubai": (25.2048, 55.2708),
        "mumbai": (19.0760, 72.8777),
        "toronto": (43.6532, -79.3832)
    },
    
    "coastal_vs_inland": {
        "gdansk_coast": (54.3520, 18.6466),
        "hamburg_coast": (53.5511, 9.9937),
        "valencia_coast": (39.4699, -0.3763),
        "nice_coast": (43.7102, 7.2620),
        "warszawa_inland": (52.2297, 21.0122),
        "munich_inland": (48.1351, 11.5820),
        "madrid_inland": (40.4168, -3.7038),
        "prague_inland": (50.0755, 14.4378)
    }
}

# Predefiniowane okresy czasowe
TIME_PERIODS = {
    "yesterday": lambda: get_day_range(-1),
    "last_week": lambda: get_range_days(-7, -1),
    "last_month": lambda: get_range_days(-30, -1),
    "last_season": lambda: get_range_days(-90, -1),
    "forecast_3days": lambda: get_range_days(0, 3),
    "forecast_week": lambda: get_range_days(0, 7),
    "winter_2024": lambda: (datetime(2024, 12, 21, tzinfo=timezone.utc), datetime(2025, 3, 20, tzinfo=timezone.utc)),
    "summer_2024": lambda: (datetime(2024, 6, 21, tzinfo=timezone.utc), datetime(2024, 9, 22, tzinfo=timezone.utc))
}

def get_day_range(days_offset: int) -> Tuple[datetime, datetime]:
    """Zwraca zakres dla konkretnego dnia (offset od dzisiaj)"""
    base = datetime.now(timezone.utc) + timedelta(days=days_offset)
    start = base.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end

def get_range_days(start_offset: int, end_offset: int) -> Tuple[datetime, datetime]:
    """Zwraca zakres dni (offsety od dzisiaj)"""
    now = datetime.now(timezone.utc)
    start = (now + timedelta(days=start_offset)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = (now + timedelta(days=end_offset)).replace(hour=0, minute=0, second=0, microsecond=0)
    return start, end

def run_single_analysis(location_name: str, lat: float, lon: float, 
                       start: datetime, end: datetime, providers: str,
                       use_cds: bool = False, **kwargs) -> Dict:
    """Uruchamia analizę dla jednej lokalizacji"""
    
    print(f"🔄 Starting analysis: {location_name}")
    start_time = time.time()
    
    # Przygotuj komendę
    cmd = [
        sys.executable, "consolidated_analysis.py",
        "--lat", str(lat),
        "--lon", str(lon),
        "--start", start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "--end", end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "--providers", providers
    ]
    
    # Dodaj klucze API jeśli dostępne
    for key_name in ["openweather_key", "weatherapi_key", "visualcrossing_key"]:
        if key_name in kwargs and kwargs[key_name]:
            cmd.extend([f"--{key_name.replace('_', '-')}", kwargs[key_name]])
    
    # ERA5 handling
    if use_cds:
        # Najpierw spróbuj wygenerować ERA5 z CDS
        era5_cmd = [
            sys.executable, "make_era5_cds.py",
            "--lat", str(lat), "--lon", str(lon),
            "--start", start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "--end", end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "--out", f"cache/era5/era5_cds_{lat:.4f}_{lon:.4f}_{start.date()}_{end.date()}.csv"
        ]
        
        era5_result = subprocess.run(era5_cmd, capture_output=True, text=True)
        if era5_result.returncode == 0:
            cmd.extend(["--era5", f"cache/era5/era5_cds_{lat:.4f}_{lon:.4f}_{start.date()}_{end.date()}.csv"])
    
    # Uruchom analizę
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)  # 10 min timeout
        
        duration = time.time() - start_time
        
        if result.returncode == 0:
            print(f"✅ Completed: {location_name} ({duration:.1f}s)")
            return {
                "location": location_name,
                "lat": lat,
                "lon": lon,
                "status": "success",
                "duration": duration,
                "stdout": result.stdout,
                "stderr": result.stderr
            }
        else:
            print(f"❌ Failed: {location_name} ({duration:.1f}s)")
            return {
                "location": location_name,
                "lat": lat,
                "lon": lon,
                "status": "error",
                "duration": duration,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "returncode": result.returncode
            }
            
    except subprocess.TimeoutExpired:
        print(f"⏰ Timeout: {location_name}")
        return {
            "location": location_name,
            "lat": lat,
            "lon": lon,
            "status": "timeout",
            "duration": 600,
            "error": "Analysis timed out after 10 minutes"
        }
    except Exception as e:
        print(f"💥 Exception: {location_name} - {e}")
        return {
            "location": location_name,
            "lat": lat,
            "lon": lon,
            "status": "exception",
            "duration": time.time() - start_time,
            "error": str(e)
        }

def run_batch_analysis(locations: Dict[str, Tuple[float, float]], 
                      start: datetime, end: datetime, providers: str,
                      max_workers: int = 3, use_cds: bool = False, **kwargs) -> List[Dict]:
    """Uruchamia analizy dla wielu lokalizacji równolegle"""
    
    results = []
    
    if max_workers == 1:
        # Sequential processing
        for name, (lat, lon) in locations.items():
            result = run_single_analysis(name, lat, lon, start, end, providers, use_cds, **kwargs)
            results.append(result)
    else:
        # Parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            
            # Submit all tasks
            future_to_location = {}
            for name, (lat, lon) in locations.items():
                future = executor.submit(run_single_analysis, name, lat, lon, start, end, providers, use_cds, **kwargs)
                future_to_location[future] = name
            
            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_location):
                location = future_to_location[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as exc:
                    print(f"💥 {location} generated an exception: {exc}")
                    results.append({
                        "location": location,
                        "status": "exception",
                        "error": str(exc),
                        "duration": 0
                    })
    
    return results

def create_batch_report(results: List[Dict], locations_name: str, period_name: str, 
                       output_dir: Path):
    """Tworzy raport z batch analysis"""
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Summary statistics
    total = len(results)
    successful = sum(1 for r in results if r["status"] == "success")
    failed = total - successful
    total_duration = sum(r.get("duration", 0) for r in results)
    avg_duration = total_duration / total if total > 0 else 0
    
    # Detailed report
    report_lines = [
        f"# Batch Weather Analysis Report",
        f"",
        f"**Location Set**: {locations_name}",
        f"**Time Period**: {period_name}",
        f"**Analysis Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"",
        f"## Summary",
        f"",
        f"- **Total Locations**: {total}",
        f"- **Successful**: {successful} ({successful/total*100:.1f}%)",
        f"- **Failed**: {failed} ({failed/total*100:.1f}%)",
        f"- **Total Duration**: {total_duration:.1f}s ({total_duration/60:.1f} min)",
        f"- **Average Duration**: {avg_duration:.1f}s per location",
        f"",
        f"## Results by Location",
        f""
    ]
    
    # Table header
    report_lines.extend([
        f"| Location | Status | Duration (s) | Coordinates | Notes |",
        f"|----------|--------|-------------|-------------|-------|"
    ])
    
    # Results table
    for result in sorted(results, key=lambda x: x.get("location", "")):
        status_icon = {"success": "✅", "error": "❌", "timeout": "⏰", "exception": "💥"}.get(result["status"], "❓")
        location = result.get("location", "Unknown")
        status = f"{status_icon} {result['status']}"
        duration = f"{result.get('duration', 0):.1f}"
        lat = result.get('lat', 0)
        lon = result.get('lon', 0)
        coords = f"{lat:.2f}°N, {lon:.2f}°E"
        
        notes = ""
        if result["status"] == "error":
            notes = f"Exit code: {result.get('returncode', 'N/A')}"
        elif result["status"] == "timeout":
            notes = "Analysis timed out"
        elif result["status"] == "exception":
            notes = result.get("error", "Unknown exception")[:50]
        
        report_lines.append(f"| {location} | {status} | {duration} | {coords} | {notes} |")
    
    # Error details
    error_results = [r for r in results if r["status"] != "success"]
    if error_results:
        report_lines.extend([
            f"",
            f"## Error Details",
            f""
        ])
        
        for result in error_results:
            report_lines.extend([
                f"### {result['location']} ({result['status']})",
                f"```",
                result.get('stderr', result.get('error', 'No details available'))[:1000],
                f"```",
                f""
            ])
    
    # Save report
    report_file = output_dir / f"batch_report_{locations_name}_{period_name}.md"
    report_file.write_text("\n".join(report_lines), encoding="utf-8")
    
    # Save JSON results
    json_file = output_dir / f"batch_results_{locations_name}_{period_name}.json"
    with open(json_file, 'w', encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"📊 Batch report saved: {report_file}")
    print(f"📄 Detailed results: {json_file}")
    
    return report_file

def create_comparative_analysis(locations_name: str, period_name: str, output_dir: Path):
    """Tworzy analizę porównawczą na podstawie wyników w central_weather_results.csv"""
    
    central_file = Path("central_weather_results.csv")
    if not central_file.exists():
        print("❌ No central results file found")
        return
    
    try:
        df = pd.read_csv(central_file)
        df['run_timestamp'] = pd.to_datetime(df['run_timestamp'])
        
        # Filtruj do najnowszych wyników (ostatnie 24h)
        recent_cutoff = df['run_timestamp'].max() - timedelta(hours=24)
        recent_df = df[df['run_timestamp'] >= recent_cutoff]
        
        if recent_df.empty:
            print("❌ No recent results found for comparative analysis")
            return
        
        # Analiza per lokalizacja
        location_analysis = recent_df.groupby(['lat', 'lon']).agg({
            'provider': 'nunique',
            'rmse': ['mean', 'median', 'std'],
            'bias': 'mean',
            'correlation': 'median',
            'coverage_pct': 'mean'
        }).round(3)
        
        location_analysis.columns = ['_'.join(col).strip() for col in location_analysis.columns]
        
        # Ranking lokalizacji (najlepsze/najgorsze)
        location_analysis = location_analysis.sort_values('rmse_median')
        
        # Create comparison report
        comp_lines = [
            f"# Comparative Analysis: {locations_name} - {period_name}",
            f"",
            f"Based on data from: {recent_cutoff.strftime('%Y-%m-%d %H:%M')} to {df['run_timestamp'].max().strftime('%Y-%m-%d %H:%M')}",
            f"",
            f"## Location Performance Ranking",
            f"",
            f"| Rank | Location | Providers | RMSE (Med/Avg/Std) | Bias | Correlation | Coverage % |",
            f"|------|----------|-----------|-------------------|------|-------------|-----------|"
        ]
        
        for rank, ((lat, lon), row) in enumerate(location_analysis.iterrows(), 1):
            rmse_str = f"{row['rmse_median']:.3f}/{row['rmse_mean']:.3f}/{row['rmse_std']:.3f}"
            comp_lines.append(
                f"| {rank} | {lat:.2f}°N, {lon:.2f}°E | {row['provider_nunique']:.0f} | "
                f"{rmse_str} | {row['bias_mean']:+.3f} | {row['correlation_median']:.3f} | {row['coverage_pct_mean']:.1f}% |"
            )
        
        # Provider performance across all locations  
        provider_analysis = recent_df.groupby('provider').agg({
            'rmse': ['count', 'mean', 'median', 'std'],
            'bias': 'mean',
            'correlation': 'median'
        }).round(3)
        
        provider_analysis.columns = ['_'.join(col).strip() for col in provider_analysis.columns]
        provider_analysis = provider_analysis.sort_values('rmse_median')
        
        comp_lines.extend([
            f"",
            f"## Provider Performance Summary",
            f"",
            f"| Provider | Locations | RMSE (Med/Avg/Std) | Bias | Correlation |",
            f"|----------|-----------|-------------------|------|-------------|"
        ])
        
        for provider, row in provider_analysis.iterrows():
            rmse_str = f"{row['rmse_median']:.3f}/{row['rmse_mean']:.3f}/{row['rmse_std']:.3f}"
            comp_lines.append(
                f"| {provider} | {row['rmse_count']:.0f} | {rmse_str} | "
                f"{row['bias_mean']:+.3f} | {row['correlation_median']:.3f} |"
            )
        
        # Variable analysis
        var_analysis = recent_df.groupby('variable')['rmse'].agg(['count', 'median', 'std']).round(3)
        var_analysis = var_analysis.sort_values('median')
        
        comp_lines.extend([
            f"",
            f"## Variable Performance",
            f"",
            f"| Variable | Data Points | Median RMSE | Std RMSE |",
            f"|----------|-------------|-------------|----------|"
        ])
        
        for variable, row in var_analysis.iterrows():
            comp_lines.append(
                f"| {variable} | {row['count']:.0f} | {row['median']:.3f} | {row['std']:.3f} |"
            )
        
        # Save comparative analysis
        comp_file = output_dir / f"comparative_analysis_{locations_name}_{period_name}.md"
        comp_file.write_text("\n".join(comp_lines), encoding="utf-8")
        
        print(f"📈 Comparative analysis: {comp_file}")
        
    except Exception as e:
        print(f"❌ Error creating comparative analysis: {e}")

def main():
    ap = argparse.ArgumentParser(description="Batch weather analysis for multiple locations")
    
    # Location sets
    location_group = ap.add_mutually_exclusive_group(required=True)
    location_group.add_argument("--location-set", choices=list(LOCATION_SETS.keys()),
                               help="Predefined set of locations")
    location_group.add_argument("--custom-locations", type=str,
                               help="JSON file with custom locations")
    
    # Time periods  
    time_group = ap.add_mutually_exclusive_group(required=True)
    time_group.add_argument("--time-period", choices=list(TIME_PERIODS.keys()),
                           help="Predefined time period")
    time_group.add_argument("--custom-time", nargs=2, metavar=("START", "END"),
                           help="Custom time range (ISO format)")
    
    # Analysis options
    ap.add_argument("--providers", type=str, 
                   default="openmeteo,metno,weatherapi,visualcrossing",
                   help="Comma-separated providers")
    ap.add_argument("--max-workers", type=int, default=3,
                   help="Maximum parallel workers")
    ap.add_argument("--use-cds", action="store_true",
                   help="Use Copernicus CDS for ERA5 data")
    ap.add_argument("--output-dir", type=str, default="batch_results",
                   help="Output directory for reports")
    
    # API keys
    ap.add_argument("--openweather-key", type=str, default="")
    ap.add_argument("--weatherapi-key", type=str, default="")
    ap.add_argument("--visualcrossing-key", type=str, default="")
    
    args = ap.parse_args()
    
    # Determine locations
    if args.location_set:
        locations = LOCATION_SETS[args.location_set]
        locations_name = args.location_set
    else:
        with open(args.custom_locations, 'r') as f:
            locations = json.load(f)
        locations_name = Path(args.custom_locations).stem
    
    # Determine time period
    if args.time_period:
        start, end = TIME_PERIODS[args.time_period]()
        period_name = args.time_period
    else:
        start = datetime.fromisoformat(args.custom_time[0].replace("Z", "+00:00"))
        end = datetime.fromisoformat(args.custom_time[1].replace("Z", "+00:00"))
        period_name = f"{start.date()}_to_{end.date()}"
    
    print("=" * 70)
    print("🚀 BATCH WEATHER ANALYSIS")
    print("=" * 70)
    print(f"📍 Locations: {locations_name} ({len(locations)} cities)")
    print(f"📅 Period: {period_name} ({start.date()} to {end.date()})")
    print(f"🌐 Providers: {args.providers}")
    print(f"⚙️  Workers: {args.max_workers}")
    print(f"📊 CDS ERA5: {'Yes' if args.use_cds else 'No'}")
    print("=" * 70)
    
    # Start batch analysis
    batch_start = time.time()
    
    results = run_batch_analysis(
        locations=locations,
        start=start,
        end=end,
        providers=args.providers,
        max_workers=args.max_workers,
        use_cds=args.use_cds,
        openweather_key=args.openweather_key,
        weatherapi_key=args.weatherapi_key,
        visualcrossing_key=args.visualcrossing_key
    )
    
    batch_duration = time.time() - batch_start
    
    # Generate reports
    output_dir = Path(args.output_dir)
    report_file = create_batch_report(results, locations_name, period_name, output_dir)
    
    # Create comparative analysis
    create_comparative_analysis(locations_name, period_name, output_dir)
    
    # Summary
    successful = sum(1 for r in results if r["status"] == "success")
    print("\n" + "=" * 70)
    print("✅ BATCH ANALYSIS COMPLETE")
    print("=" * 70)
    print(f"📈 Success Rate: {successful}/{len(results)} ({successful/len(results)*100:.1f}%)")
    print(f"⏱️  Total Duration: {batch_duration:.1f}s ({batch_duration/60:.1f} min)")
    print(f"📊 Results: {report_file}")
    print(f"📁 Output Dir: {output_dir}")
    
    if successful < len(results):
        failed = len(results) - successful
        print(f"⚠️  {failed} locations failed - check the report for details")

if __name__ == "__main__":
    main()
