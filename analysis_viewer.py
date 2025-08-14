#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analysis_viewer.py - Przeglądarka i analiza wyników z central_weather_results.csv

Funkcje:
- Wyświetlanie podsumowań i statystyk
- Filtrowanie i wyszukiwanie w danych
- Porównania między runami/lokalizacjami/providerami
- Eksport raportów i wykresów na żądanie
- Zarządzanie danymi (usuwanie, archiwizacja)
- Ulepszone wykresy i dashboard
"""

import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns

CENTRAL_FILE = Path("central_weather_results.csv")

# Konfiguracja kolorów
PROVIDER_COLORS = {
    'openmeteo': '#2E8B57',      # SeaGreen
    'metno': '#4169E1',          # RoyalBlue  
    'weatherapi': '#FF6347',     # Tomato
    'visualcrossing': '#9370DB', # MediumPurple
    'openweather': '#FF8C00',    # DarkOrange
}

VARIABLE_COLORS = {
    'temperature_2m': '#FF4444',
    'precipitation': '#4444FF', 
    'wind_speed_100m': '#44FF44',
    'wind_direction_100m': '#FFAA44'
}

def load_central_data():
    """Ładuje centralne dane z walidacją"""
    if not CENTRAL_FILE.exists():
        print(f"❌ Central results file not found: {CENTRAL_FILE}")
        print("💡 Run an analysis first or migrate old data with migrate_cleanup.py")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(CENTRAL_FILE)
        # Poprawka formatów dat - użyj format='mixed' dla kompatybilności z pandas
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')
        if 'start_time' in df.columns:
            df['start_time'] = pd.to_datetime(df['start_time'], format='mixed', errors='coerce')
        if 'end_time' in df.columns:
            df['end_time'] = pd.to_datetime(df['end_time'], format='mixed', errors='coerce')
        print(f"✅ Loaded {len(df)} records from {df['timestamp'].nunique() if 'timestamp' in df.columns else 'unknown'} analysis runs")
        return df
    except Exception as e:
        print(f"❌ Error loading central data: {e}")
        return pd.DataFrame()

def show_overview(df: pd.DataFrame):
    """Wyświetla ogólny przegląd danych z lepszym formatowaniem"""
    if df.empty:
        print("❌ No data available")
        return
    
    print("=" * 70)
    print("🌤️ WEATHER ANALYSIS OVERVIEW")
    print("=" * 70)
    
    # Podstawowe statystyki
    total_runs = df['timestamp'].nunique() if 'timestamp' in df.columns else 0
    total_locations = df[['lat', 'lon']].drop_duplicates().shape[0] if 'lat' in df.columns and 'lon' in df.columns else 0
    total_providers = df['provider'].nunique() if 'provider' in df.columns else 0
    total_records = len(df)
    
    if 'timestamp' in df.columns and not df['timestamp'].isna().all():
        date_range = f"{df['timestamp'].min().strftime('%Y-%m-%d')} to {df['timestamp'].max().strftime('%Y-%m-%d')}"
    else:
        date_range = "Unknown"
    
    print(f"📈 Total analysis runs: {total_runs:,}")
    print(f"📍 Unique locations: {total_locations}")
    print(f"🌐 Weather providers: {total_providers}")
    print(f"📊 Total records: {total_records:,}")
    print(f"📅 Date range: {date_range}")
    print()
    
    # Lokalizacje z lepszym formatowaniem
    if 'lat' in df.columns and 'lon' in df.columns:
        print("🗺️ ANALYZED LOCATIONS:")
        locations = df.groupby(['lat', 'lon']).size().sort_values(ascending=False)
        for rank, ((lat, lon), count) in enumerate(locations.head(10).items(), 1):
            if rank == 1:
                emoji = "🏆"
            elif rank == 2:
                emoji = "🥈"
            elif rank == 3:
                emoji = "🥉"
            else:
                emoji = f"{rank}."
            
            # Spróbuj znaleźć nazwę lokalizacji
            location_name = "Unknown"
            if 'location_name' in df.columns:
                location_subset = df[(df['lat'] == lat) & (df['lon'] == lon)]
                if not location_subset.empty and not location_subset['location_name'].isna().all():
                    location_name = location_subset['location_name'].iloc[0]
            
            print(f"  {emoji} {location_name} ({lat:.2f}°N, {lon:.2f}°E) - {count} analyses")
        print()
    
    # Providery
    if 'provider' in df.columns:
        print("🌐 PROVIDER PERFORMANCE:")
        providers = df['provider'].value_counts()
        for provider, count in providers.items():
            color_indicator = "🟢" if provider == "openmeteo" else "🔵" if provider == "metno" else "🟡"
            print(f"  {color_indicator} {provider}: {count} analyses")
        print()
    
    # Najlepsze wyniki
    if 'temperature_2m_rmse' in df.columns:
        print("🎯 BEST TEMPERATURE ACCURACY:")
        best_temp = df.loc[df['temperature_2m_rmse'].idxmin()]
        print(f"  🌡️ {best_temp['provider']} in {best_temp.get('location_name', 'Unknown')}: {best_temp['temperature_2m_rmse']:.2f}°C RMSE")
        print()
    
    # Ostatnie analizy
    if 'timestamp' in df.columns and not df['timestamp'].isna().all():
        print("🕒 RECENT ANALYSES:")
        recent = df.nlargest(5, 'timestamp')
        for _, row in recent.iterrows():
            timestamp = row['timestamp'].strftime('%Y-%m-%d %H:%M') if pd.notna(row['timestamp']) else 'Unknown'
            location = row.get('location_name', 'Unknown')
            provider = row.get('provider', 'Unknown')
            print(f"  📅 {timestamp}: {provider} in {location}")

def show_comparison(df: pd.DataFrame):
    """Porównanie między providerami"""
    if df.empty:
        print("❌ No data available")
        return
    
    print("=" * 70)
    print("🆚 PROVIDER COMPARISON")
    print("=" * 70)
    
    if 'provider' not in df.columns:
        print("❌ No provider data available")
        return
    
    # Statystyki per provider
    metrics = ['temperature_2m_rmse', 'precipitation_rmse', 'wind_speed_100m_rmse']
    available_metrics = [m for m in metrics if m in df.columns]
    
    if not available_metrics:
        print("❌ No metric columns found")
        return
    
    print("📊 AVERAGE PERFORMANCE BY PROVIDER:")
    print()
    
    provider_stats = df.groupby('provider')[available_metrics].agg(['mean', 'count']).round(3)
    
    for provider in df['provider'].unique():
        provider_data = df[df['provider'] == provider]
        print(f"🔹 {provider.upper()}:")
        print(f"   Analyses: {len(provider_data)}")
        
        for metric in available_metrics:
            if not provider_data[metric].isna().all():
                avg = provider_data[metric].mean()
                std = provider_data[metric].std()
                count = provider_data[metric].count()
                print(f"   {metric.replace('_', ' ').title()}: {avg:.3f} ± {std:.3f} ({count} samples)")
        print()
    
    # Ranking
    print("🏆 PROVIDER RANKINGS:")
    for metric in available_metrics:
        print(f"\n📈 {metric.replace('_', ' ').title()} (lower is better):")
        ranking = df.groupby('provider')[metric].mean().sort_values()
        for rank, (provider, score) in enumerate(ranking.items(), 1):
            if pd.notna(score):
                medal = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else f"{rank}."
                print(f"   {medal} {provider}: {score:.3f}")

def show_locations(df: pd.DataFrame):
    """Analiza per lokalizacja"""
    if df.empty:
        print("❌ No data available")
        return
    
    print("=" * 70)
    print("🗺️ LOCATION ANALYSIS")
    print("=" * 70)
    
    if 'lat' not in df.columns or 'lon' not in df.columns:
        print("❌ No location data available")
        return
    
    # Grupuj per lokalizacja
    location_groups = df.groupby(['lat', 'lon'])
    
    print("📍 LOCATION PERFORMANCE SUMMARY:")
    print()
    
    for (lat, lon), group in location_groups:
        location_name = group['location_name'].iloc[0] if 'location_name' in group.columns else "Unknown"
        provider_count = group['provider'].nunique() if 'provider' in group.columns else 0
        analysis_count = len(group)
        
        print(f"🌍 {location_name} ({lat:.2f}°N, {lon:.2f}°E)")
        print(f"   📊 Analyses: {analysis_count}, Providers: {provider_count}")
        
        # Najlepszy provider dla tej lokalizacji
        if 'temperature_2m_rmse' in group.columns and not group['temperature_2m_rmse'].isna().all():
            best_provider = group.loc[group['temperature_2m_rmse'].idxmin()]
            print(f"   🎯 Best: {best_provider['provider']} (RMSE: {best_provider['temperature_2m_rmse']:.2f}°C)")
        
        # Średnie wyniki
        if 'temperature_2m_rmse' in group.columns:
            avg_rmse = group['temperature_2m_rmse'].mean()
            if pd.notna(avg_rmse):
                print(f"   📈 Avg RMSE: {avg_rmse:.2f}°C")
        print()

def show_trends(df: pd.DataFrame, days: int = 30):
    """Pokazuje trendy w czasie"""
    if df.empty:
        print("❌ No data available")
        return
    
    if 'timestamp' not in df.columns:
        print("❌ No timestamp data available")
        return
    
    print("=" * 70)
    print(f"📈 TRENDS (last {days} days)")
    print("=" * 70)
    
    # Filtruj ostatnie dni
    cutoff = datetime.now() - timedelta(days=days)
    recent_df = df[df['timestamp'] >= cutoff] if not df['timestamp'].isna().all() else df
    
    if recent_df.empty:
        print(f"❌ No data from last {days} days")
        return
    
    print(f"📊 Analyzing {len(recent_df)} records from last {days} days")
    print()
    
    # Trend per provider
    if 'provider' in recent_df.columns and 'temperature_2m_rmse' in recent_df.columns:
        print("📈 PROVIDER PERFORMANCE TRENDS:")
        for provider in recent_df['provider'].unique():
            provider_data = recent_df[recent_df['provider'] == provider].sort_values('timestamp')
            if len(provider_data) >= 2 and not provider_data['temperature_2m_rmse'].isna().all():
                first_rmse = provider_data['temperature_2m_rmse'].iloc[0]
                last_rmse = provider_data['temperature_2m_rmse'].iloc[-1]
                change = last_rmse - first_rmse
                trend = "📈 improving" if change < 0 else "📉 declining" if change > 0 else "➡️ stable"
                print(f"   {provider}: {trend} ({change:+.3f})")

def export_summary(df: pd.DataFrame, filename: str):
    """Eksportuje podsumowanie do pliku"""
    if df.empty:
        print("❌ No data to export")
        return
    
    output_file = Path(filename)
    
    # Przygotuj dane do eksportu
    summary_data = []
    
    if 'provider' in df.columns:
        for provider in df['provider'].unique():
            provider_data = df[df['provider'] == provider]
            
            row = {
                'provider': provider,
                'total_analyses': len(provider_data),
                'locations': provider_data[['lat', 'lon']].drop_duplicates().shape[0] if 'lat' in provider_data.columns else 0
            }
            
            # Dodaj metryki
            metrics = ['temperature_2m_rmse', 'precipitation_rmse', 'wind_speed_100m_rmse']
            for metric in metrics:
                if metric in provider_data.columns:
                    row[f'{metric}_mean'] = provider_data[metric].mean()
                    row[f'{metric}_std'] = provider_data[metric].std()
            
            summary_data.append(row)
    
    if summary_data:
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_csv(output_file, index=False)
        print(f"✅ Summary exported to: {output_file}")
    else:
        print("❌ No summary data to export")

def main():
    parser = argparse.ArgumentParser(description="Weather Analysis Viewer")
    parser.add_argument("command", choices=["overview", "compare", "locations", "trends", "export"],
                       help="Command to execute")
    parser.add_argument("--days", type=int, default=30, help="Days for trends analysis")
    parser.add_argument("--export-file", type=str, default="weather_summary.csv", 
                       help="Output file for export")
    
    args = parser.parse_args()
    
    # Załaduj dane
    df = load_central_data()
    
    if df.empty and args.command != "overview":
        print("❌ No data available. Run some analyses first.")
        return
    
    # Wykonaj komendę
    if args.command == "overview":
        show_overview(df)
    elif args.command == "compare":
        show_comparison(df)
    elif args.command == "locations":
        show_locations(df)
    elif args.command == "trends":
        show_trends(df, args.days)
    elif args.command == "export":
        export_summary(df, args.export_file)

if __name__ == "__main__":
    main()