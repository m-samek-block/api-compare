#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
migrate_and_cleanup.py - Migracja starych danych do nowego systemu

Funkcje:
- Skanuje stare pliki summary (wyniki/summaries/, wyniki/)
- Migruje dane do central_weather_results.csv
- Przenosi ERA5 i dane providerów do cache/
- Usuwa duplikaty i stare pliki
- Tworzy backup przed migracją
"""

import argparse
import shutil
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import re
import hashlib

def create_backup(source_dirs):
    """Tworzy backup przed migracją - skupia się na compare_apis/"""
    backup_dir = Path("backup_before_migration")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"backup_{timestamp}"
    backup_path.mkdir(parents=True, exist_ok=True)
    
    print(f"[BACKUP] Creating backup at: {backup_path}")
    
    # Priorytet dla compare_apis/
    compare_apis = Path("compare_apis")
    if compare_apis.exists():
        dest = backup_path / "compare_apis"
        try:
            shutil.copytree(compare_apis, dest, dirs_exist_ok=True)
            print(f"[BACKUP] compare_apis/ -> {dest}")
        except TypeError:
            # Fallback dla starszych wersji Pythona
            try:
                shutil.copytree(compare_apis, dest)
                print(f"[BACKUP] compare_apis/ -> {dest}")
            except FileExistsError:
                # Jeśli folder istnieje, usuń i skopiuj ponownie
                shutil.rmtree(dest, ignore_errors=True)
                shutil.copytree(compare_apis, dest)
                print(f"[BACKUP] compare_apis/ -> {dest} (overwritten)")
            except Exception as e:
                print(f"[BACKUP] Warning: Could not backup compare_apis/: {e}")
    
    # Dodatkowe foldery jeśli istnieją
    for source_dir in source_dirs:
        source_path = Path(source_dir)
        if source_path.exists() and source_path.name != "compare_apis":  # Nie duplikuj
            dest = backup_path / source_path.name
            try:
                if source_path.is_dir():
                    try:
                        shutil.copytree(source_path, dest, dirs_exist_ok=True)
                    except TypeError:
                        # Fallback dla starszych wersji
                        if dest.exists():
                            shutil.rmtree(dest, ignore_errors=True)
                        shutil.copytree(source_path, dest)
                else:
                    shutil.copy2(source_path, dest)
                print(f"[BACKUP] {source_dir} -> {dest}")
            except Exception as e:
                print(f"[BACKUP] Warning: Could not backup {source_dir}: {e}")
    
    return backup_path

def find_summary_files():
    """Znajduje wszystkie pliki summary - szuka rekurencyjnie w compare_apis/"""
    summary_files = []
    
    print("[SCAN] Searching for summary files...")
    
    # Główny folder compare_apis - przeszukaj rekurencyjnie
    compare_apis_dir = Path("compare_apis")
    if compare_apis_dir.exists():
        print(f"[SCAN] Searching recursively in: {compare_apis_dir}")
        
        # Rekurencyjne szukanie wszystkich plików CSV summary
        for pattern in ["**/era5_comparison_summary*.csv", "**/*summary*Z.csv", "**/*comparison_summary*.csv"]:
            found_files = list(compare_apis_dir.glob(pattern))
            summary_files.extend(found_files)
            if found_files:
                print(f"[SCAN] Found {len(found_files)} files matching {pattern}")
                for f in found_files:
                    print(f"  -> {f}")
    
    # Dodatkowe lokalizacje bezpośrednio w project/
    additional_paths = [
        Path("wyniki/summaries/"),
        Path("wyniki/"),
        Path("./")
    ]
    
    for search_path in additional_paths:
        if search_path.exists():
            before_count = len(summary_files)
            summary_files.extend(search_path.glob("era5_comparison_summary*.csv"))
            summary_files.extend(search_path.glob("*comparison_summary*Z.csv"))
            after_count = len(summary_files)
            if after_count > before_count:
                print(f"[SCAN] Found {after_count - before_count} additional files in: {search_path}")
    
    # Usuń duplikaty ale zachowaj informację o źródle
    unique_files = []
    seen = set()
    for f in summary_files:
        if f.resolve() not in seen:
            unique_files.append(f)
            seen.add(f.resolve())
    
    print(f"[SCAN] Total unique summary files found: {len(unique_files)}")
    return unique_files

def extract_metadata_from_filename(filename: str):
    """Wyciąga metadane z nazwy pliku"""
    print(f"    [DEBUG] Parsing filename: {filename}")
    
    # Wzorce dla różnych formatów nazw
    patterns = [
        # Format: era5_comparison_summary_YYYYMMDD_HHMMSSZ.csv
        r"era5_comparison_summary_(\d{8}_\d{6}Z)\.csv$",
        # Format: era5_comparison_summary_CDS_YYYY-MM-DD_YYYY-MM-DD_YYYYMMDD_HHMMSSZ.csv
        r"era5_comparison_summary_CDS_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_(\d{8}_\d{6}Z)\.csv$",
        # Format: era5_comparison_summary_CITY_YYYY-MM-DD_YYYY-MM-DD_YYYYMMDD_HHMMSSZ.csv
        r"era5_comparison_summary_([^_]+)_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_(\d{8}_\d{6}Z)\.csv$",
        # Format: summary_YYYYMMDD_HHMMSSZ.csv
        r"summary_(\d{8}_\d{6}Z)\.csv$"
    ]
    
    for i, pattern in enumerate(patterns):
        print(f"    [DEBUG] Trying pattern {i+1}: {pattern}")
        match = re.search(pattern, filename)
        if match:
            groups = match.groups()
            print(f"    [DEBUG] Pattern {i+1} matched with {len(groups)} groups: {groups}")
            
            try:
                if len(groups) == 1:
                    # Format: era5_comparison_summary_YYYYMMDD_HHMMSSZ.csv
                    timestamp = groups[0]
                    result = {
                        'timestamp': datetime.strptime(timestamp, "%Y%m%d_%H%M%SZ").replace(tzinfo=timezone.utc),
                        'start_date': None,
                        'end_date': None,
                        'location': None
                    }
                    print(f"    [DEBUG] Extracted: {result}")
                    return result
                        
                elif len(groups) == 3 and groups[0].startswith('20'):
                    # Format: era5_comparison_summary_CDS_YYYY-MM-DD_YYYY-MM-DD_YYYYMMDD_HHMMSSZ.csv
                    start_date, end_date, timestamp = groups
                    result = {
                        'timestamp': datetime.strptime(timestamp, "%Y%m%d_%H%M%SZ").replace(tzinfo=timezone.utc),
                        'start_date': start_date,
                        'end_date': end_date,
                        'location': 'CDS'
                    }
                    print(f"    [DEBUG] Extracted: {result}")
                    return result
                        
                elif len(groups) == 4:
                    # Format: era5_comparison_summary_CITY_YYYY-MM-DD_YYYY-MM-DD_YYYYMMDD_HHMMSSZ.csv
                    location, start_date, end_date, timestamp = groups
                    result = {
                        'timestamp': datetime.strptime(timestamp, "%Y%m%d_%H%M%SZ").replace(tzinfo=timezone.utc),
                        'start_date': start_date,
                        'end_date': end_date,
                        'location': location
                    }
                    print(f"    [DEBUG] Extracted: {result}")
                    return result
                    
            except ValueError as e:
                print(f"    [DEBUG] ValueError parsing timestamp in pattern {i+1}: {e}")
                continue
        else:
            print(f"    [DEBUG] Pattern {i+1} did not match")
    
    # Jeśli nie można wyciągnąć z nazwy, użyj czasu modyfikacji pliku
    print(f"    [DEBUG] No patterns matched, using file modification time")
    file_path = Path(filename)
    if file_path.exists():
        mtime = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)
        result = {
            'timestamp': mtime,
            'start_date': None,
            'end_date': None,
            'location': None
        }
        print(f"    [DEBUG] Using file mtime: {result}")
        return result
    
    print(f"    [DEBUG] File does not exist, returning None")
    return None

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalizuje nazwy kolumn z różnych formatów"""
    column_mapping = {
        # Polskie nazwy
        "provider (API)": "provider",
        "zmienna": "variable", 
        "n_api": "n_points",
        "n_era5": "n_points",
        "pokrycie%": "coverage_pct",
        "dorobione%": "derived_pct",
        "bias": "bias",
        "MAE": "mae",
        "RMSE": "rmse",
        "corr": "correlation",
        "%zawyż": "over_pct",
        "%zaniż": "under_pct",
        
        # Angielskie nazwy
        "provider (dostawca)": "provider",
        "variable (zmienna)": "variable",
        "n_provider (liczba punktów API)": "n_points",
        "n (liczba dopasowań z ERA5)": "n_points",
        "coverage_pct (% pokrycia z ERA5)": "coverage_pct",
        "derived_pct (% dorobionych wartości)": "derived_pct",
        "bias_mean (błąd średni: pred − ERA5)": "bias",
        "mae (średni błąd bezwzględny)": "mae",
        "rmse (pierwiastek średniego błędu kwadratowego)": "rmse",
        "corr (korelacja Pearsona)": "correlation",
        "over_pct (% przypadków z zawyżeniem)": "over_pct",
        "under_pct (% przypadków z zaniżeniem)": "under_pct"
    }
    
    # Zastosuj mapowanie
    df = df.rename(columns=column_mapping)
    
    # Usuń nieznane kolumny i zachowaj tylko potrzebne
    required_cols = ['provider', 'variable', 'n_points', 'coverage_pct', 'bias', 'mae', 'rmse', 'correlation', 'over_pct', 'under_pct']
    available_cols = [col for col in required_cols if col in df.columns]
    
    return df[available_cols]

def generate_data_hash(lat: float, lon: float, start_time: str, end_time: str, providers: list) -> str:
    """Generuje hash dla identyfikacji zestawu danych"""
    key = f"{lat:.4f}_{lon:.4f}_{start_time}_{end_time}_{','.join(sorted(providers))}"
    return hashlib.md5(key.encode()).hexdigest()[:12]

def migrate_summary_files(summary_files: list, central_file: Path):
    """Migruje pliki summary do centralnego pliku"""
    
    all_migrated = []
    processed_hashes = set()
    
    for summary_file in summary_files:
        try:
            print(f"[MIGRATE] Processing {summary_file}")
            
            # Wyciągnij metadane
            metadata = extract_metadata_from_filename(str(summary_file))
            if not metadata:
                print(f"  -> Cannot extract metadata, using file modification time")
                # Fallback - użyj czasu modyfikacji pliku
                file_time = datetime.fromtimestamp(summary_file.stat().st_mtime, tz=timezone.utc)
                metadata = {
                    'timestamp': file_time,
                    'start_date': '2025-08-01',
                    'end_date': '2025-08-31',
                    'location': 'unknown'
                }
            
            # Wczytaj dane
            df = pd.read_csv(summary_file)
            if df.empty:
                print(f"  -> Empty file, skipping")
                continue
            
            # Normalizuj kolumny
            df_orig_cols = len(df.columns)
            df = normalize_column_names(df)
            df_norm_cols = len(df.columns)
            
            if df.empty or df_norm_cols == 0:
                print(f"  -> No recognizable columns after normalization (had {df_orig_cols}), skipping")
                continue
            
            # Usuń duplikaty kolumn jeśli istnieją
            df = df.loc[:, ~df.columns.duplicated()]
            
            # Dodaj metadane
            df['run_timestamp'] = metadata['timestamp'].isoformat()
            
            # Spróbuj wyodrębnić lokalizację z nazwy pliku lub metadanych
            location_info = metadata.get('location')
            if location_info and location_info != 'unknown':
                # Mapowanie lokalizacji na współrzędne (przybliżone)
                location_coords = get_location_coordinates(location_info)
                df['lat'] = location_coords['lat']
                df['lon'] = location_coords['lon']
            else:
                # Domyślne wartości (Warszawa)
                df['lat'] = 52.2297  
                df['lon'] = 21.0122
            
            df['start_time'] = metadata.get('start_date') or '2025-08-01T00:00:00Z'
            df['end_time'] = metadata.get('end_date') or '2025-08-31T00:00:00Z'
            
            # Generuj hash - użyj więcej informacji dla unikatowości
            providers = df['provider'].unique().tolist() if 'provider' in df.columns else ['unknown']
            hash_input = f"{df['lat'].iloc[0]:.4f}_{df['lon'].iloc[0]:.4f}_{df['start_time'].iloc[0]}_{df['end_time'].iloc[0]}_{','.join(sorted(providers))}_{metadata['timestamp'].strftime('%Y%m%d_%H%M%S')}"
            data_hash = hashlib.md5(hash_input.encode()).hexdigest()[:12]
            
            # Sprawdź duplikaty
            if data_hash in processed_hashes:
                print(f"  -> Duplicate hash {data_hash}, skipping")
                continue
            
            processed_hashes.add(data_hash)
            df['data_hash'] = data_hash
            
            # Dodaj brakujące kolumny z domyślnymi wartościami
            required_columns = ['run_timestamp', 'data_hash', 'lat', 'lon', 'start_time', 'end_time', 
                              'provider', 'variable', 'n_points', 'coverage_pct', 'bias', 'mae', 
                              'rmse', 'correlation', 'over_pct', 'under_pct']
            
            for col in required_columns:
                if col not in df.columns:
                    if col in ['coverage_pct', 'bias', 'mae', 'rmse', 'correlation', 'over_pct', 'under_pct']:
                        df[col] = 0.0
                    elif col == 'n_points':
                        df[col] = 0
                    elif col in ['provider', 'variable']:
                        df[col] = 'unknown'
            
            # Zachowaj tylko wymagane kolumny w określonej kolejności
            df = df[required_columns]
            
            all_migrated.append(df)
            print(f"  -> Migrated {len(df)} records with hash {data_hash}")
            
        except Exception as e:
            print(f"  -> Error processing {summary_file}: {e}")
            continue
    
    if all_migrated:
        # Połącz wszystkie dane
        try:
            combined = pd.concat(all_migrated, ignore_index=True, sort=False)
        except Exception as e:
            print(f"[ERROR] Failed to combine DataFrames: {e}")
            print("Trying alternative combination method...")
            
            # Alternatywna metoda - upewnij się że wszystkie mają te same kolumny
            required_columns = ['run_timestamp', 'data_hash', 'lat', 'lon', 'start_time', 'end_time', 
                              'provider', 'variable', 'n_points', 'coverage_pct', 'bias', 'mae', 
                              'rmse', 'correlation', 'over_pct', 'under_pct']
            
            aligned_frames = []
            for df in all_migrated:
                # Upewnij się że ma wszystkie kolumny w odpowiedniej kolejności
                aligned_df = pd.DataFrame(columns=required_columns)
                for col in required_columns:
                    if col in df.columns:
                        aligned_df[col] = df[col]
                    else:
                        if col in ['coverage_pct', 'bias', 'mae', 'rmse', 'correlation', 'over_pct', 'under_pct']:
                            aligned_df[col] = 0.0
                        elif col == 'n_points':
                            aligned_df[col] = 0
                        else:
                            aligned_df[col] = 'unknown'
                aligned_frames.append(aligned_df)
            
            combined = pd.concat(aligned_frames, ignore_index=True, sort=False)
        
        # Sortuj chronologicznie
        combined = combined.sort_values('run_timestamp', ascending=False)
        
        # Zapisz do centralnego pliku
        central_file.parent.mkdir(exist_ok=True)
        combined.to_csv(central_file, index=False)
        print(f"[OK] Migrated {len(combined)} records to {central_file}")
        
        return len(summary_files), len(combined)
    
    return len(summary_files), 0

def get_location_coordinates(location_name: str) -> dict:
    """Mapowanie nazw lokalizacji na współrzędne"""
    locations = {
        'warszawa': {'lat': 52.2297, 'lon': 21.0122},
        'krakow': {'lat': 50.0647, 'lon': 19.9450},
        'gdansk': {'lat': 54.3520, 'lon': 18.6466},
        'poznan': {'lat': 52.4064, 'lon': 16.9252},
        'hel': {'lat': 54.6086, 'lon': 18.8067},
        'zakopane': {'lat': 49.2992, 'lon': 19.9496},
        'london': {'lat': 51.5074, 'lon': -0.1278},
        'athens': {'lat': 37.9838, 'lon': 23.7275},
        'barcelona': {'lat': 41.3851, 'lon': 2.1734},
        'bergen': {'lat': 60.3913, 'lon': 5.3221},
        'cairo': {'lat': 30.0444, 'lon': 31.2357},
        'denver': {'lat': 39.7392, 'lon': -104.9903},
        'dubai': {'lat': 25.2048, 'lon': 55.2708},
        'istanbul': {'lat': 41.0082, 'lon': 28.9784},
        'lisbon': {'lat': 38.7223, 'lon': -9.1393},
        'miami': {'lat': 25.7617, 'lon': -80.1918},
        'mumbai': {'lat': 19.0760, 'lon': 72.8777},
        'newyork': {'lat': 40.7128, 'lon': -74.0060},
        'reykjavik': {'lat': 64.1466, 'lon': -21.9426},
        'singapore': {'lat': 1.3521, 'lon': 103.8198},
        'sydney': {'lat': -33.8688, 'lon': 151.2093},
        'cds': {'lat': 52.2297, 'lon': 21.0122},  # Domyślnie Warszawa dla CDS
    }
    
    location_key = location_name.lower()
    return locations.get(location_key, {'lat': 52.2297, 'lon': 21.0122})

def migrate_cache_data():
    """Migruje dane ERA5 i providerów do cache - szuka rekurencyjnie w compare_apis/"""
    cache_dir = Path("cache")
    cache_dir.mkdir(exist_ok=True)
    (cache_dir / "era5").mkdir(exist_ok=True)
    (cache_dir / "providers").mkdir(exist_ok=True)
    
    migrated_files = 0
    
    print("[CACHE] Migrating data files...")
    
    # === MIGRUJ PLIKI ERA5 ===
    print("[CACHE] Looking for ERA5 files...")
    
    # Rekurencyjnie szukaj w compare_apis/
    compare_apis_dir = Path("compare_apis")
    if compare_apis_dir.exists():
        era5_files = list(compare_apis_dir.glob("**/era5*.csv"))
        for era5_file in era5_files:
            if era5_file.is_file():
                # Stwórz unikalną nazwę bazując na ścieżce
                rel_path = era5_file.relative_to(Path("."))
                safe_name = str(rel_path).replace("/", "_").replace("\\", "_")
                dest_path = cache_dir / "era5" / f"migrated_{safe_name}"
                
                shutil.copy2(era5_file, dest_path)
                print(f"[CACHE] ERA5: {era5_file} -> {dest_path}")
                migrated_files += 1
    
    # Dodatkowe lokalizacje ERA5
    additional_era5_sources = ["dane/era5.csv", "era5.csv", "data/era5.csv"]
    for source in additional_era5_sources:
        source_path = Path(source)
        if source_path.exists():
            dest_path = cache_dir / "era5" / f"migrated_{source.replace('/', '_')}"
            shutil.copy2(source_path, dest_path)
            print(f"[CACHE] ERA5: {source_path} -> {dest_path}")
            migrated_files += 1
    
    # === MIGRUJ PLIKI PROVIDERÓW ===
    print("[CACHE] Looking for provider files...")
    
    # Rekurencyjnie szukaj provider_*.csv w compare_apis/
    if compare_apis_dir.exists():
        provider_files = list(compare_apis_dir.glob("**/provider_*.csv"))
        for provider_file in provider_files:
            if provider_file.is_file():
                # Stwórz unikalną nazwę
                rel_path = provider_file.relative_to(Path("."))
                safe_name = str(rel_path).replace("/", "_").replace("\\", "_")
                dest_path = cache_dir / "providers" / f"migrated_{safe_name}"
                
                shutil.copy2(provider_file, dest_path)
                print(f"[CACHE] Provider: {provider_file} -> {dest_path}")
                migrated_files += 1
    
    # Dodatkowe lokalizacje providerów
    additional_provider_patterns = [
        "dane/provider_*.csv",
        "wyniki/provider_*.csv", 
        "provider_*.csv"
    ]
    
    for pattern in additional_provider_patterns:
        for source_path in Path(".").glob(pattern):
            if source_path.is_file() and source_path.exists():
                safe_name = str(source_path).replace("/", "_").replace("\\", "_")
                dest_path = cache_dir / "providers" / f"migrated_{safe_name}"
                shutil.copy2(source_path, dest_path)
                print(f"[CACHE] Provider: {source_path} -> {dest_path}")
                migrated_files += 1
    
    return migrated_files

def cleanup_old_files(dry_run=False):
    """Usuwa stare pliki po migracji - bezpiecznie tylko w compare_apis/"""
    files_to_remove = []
    dirs_to_check = []
    
    print("[CLEANUP] Scanning for files to cleanup...")
    
    # Pliki do usunięcia - tylko w compare_apis/ (bezpieczeństwo)
    compare_apis_dir = Path("compare_apis")
    if compare_apis_dir.exists():
        # Summary files
        files_to_remove.extend(compare_apis_dir.glob("**/era5_comparison_summary*.csv"))
        files_to_remove.extend(compare_apis_dir.glob("**/*summary*Z.csv"))
        
        # Provider files  
        files_to_remove.extend(compare_apis_dir.glob("**/provider_*.csv"))
        
        # Niektóre pliki era5 (ale ostrożnie)
        potential_era5 = list(compare_apis_dir.glob("**/era5.csv"))
        files_to_remove.extend(potential_era5)
        
        print(f"[CLEANUP] Found {len(files_to_remove)} files in compare_apis/ to potentially remove")
    
    # Potencjalne puste foldery do sprawdzenia (tylko w compare_apis/)
    if compare_apis_dir.exists():
        for subdir in ["wyniki", "dane", "plots", "analysis", "summaries"]:
            for path in compare_apis_dir.glob(f"**/{subdir}"):
                if path.is_dir():
                    dirs_to_check.append(path)
    
    if dry_run:
        print("\n[DRY RUN] Files that would be removed:")
        for file in files_to_remove:
            print(f"  - {file}")
        
        print("\n[DRY RUN] Directories that would be checked for removal:")
        for dir_path in dirs_to_check:
            print(f"  - {dir_path}")
            try:
                # Sprawdź czy pusty
                contents = list(dir_path.iterdir())
                if not contents:
                    print(f"    (EMPTY - would be removed)")
                else:
                    print(f"    (NOT EMPTY - {len(contents)} items)")
            except:
                print(f"    (Cannot check contents)")
        
        return len(files_to_remove), len(dirs_to_check)
    
    # Usuń pliki
    removed_files = 0
    for file_path in files_to_remove:
        try:
            if file_path.exists():
                file_path.unlink()
                print(f"[CLEANUP] Removed file: {file_path}")
                removed_files += 1
        except Exception as e:
            print(f"[CLEANUP] Could not remove {file_path}: {e}")
    
    # Usuń puste foldery (od najgłębszych do płytszych)
    removed_dirs = 0
    dirs_to_check_sorted = sorted(dirs_to_check, key=lambda x: len(x.parts), reverse=True)
    
    for dir_path in dirs_to_check_sorted:
        if dir_path.exists() and dir_path.is_dir():
            try:
                # Sprawdź czy folder jest pusty
                contents = list(dir_path.iterdir())
                if not contents:
                    dir_path.rmdir()
                    print(f"[CLEANUP] Removed empty directory: {dir_path}")
                    removed_dirs += 1
                else:
                    print(f"[CLEANUP] Directory not empty, keeping: {dir_path} ({len(contents)} items)")
            except Exception as e:
                print(f"[CLEANUP] Could not check/remove directory {dir_path}: {e}")
    
    return removed_files, removed_dirs

def main():
    ap = argparse.ArgumentParser(description="Migrate and cleanup old weather analysis files")
    ap.add_argument("--dry-run", action="store_true", help="Show what would be done without actually doing it")
    ap.add_argument("--no-backup", action="store_true", help="Skip creating backup")
    ap.add_argument("--no-cleanup", action="store_true", help="Skip cleanup after migration")
    ap.add_argument("--scan-only", action="store_true", help="Only scan and show what files are found")
    args = ap.parse_args()
    
    print("=" * 60)
    print("WEATHER DATA MIGRATION AND CLEANUP")
    print("=" * 60)
    print("Running from:", Path.cwd())
    print("Target folder: compare_apis/ (recursive search)")
    print("=" * 60)
    
    # 1. Znajdź pliki do migracji
    summary_files = find_summary_files()
    
    if not summary_files:
        print("\n[INFO] No summary files found to migrate")
        print("💡 Make sure you're running from project/ directory")
        print("💡 Expected structure: project/compare_apis/...")
        return
    
    # Jeśli tylko skanowanie
    if args.scan_only:
        print(f"\n[SCAN ONLY] Found {len(summary_files)} summary files:")
        for f in summary_files:
            rel_path = f.relative_to(Path("."))
            file_size = f.stat().st_size if f.exists() else 0
            print(f"  📄 {rel_path} ({file_size:,} bytes)")
        
        print(f"\n[SCAN] Looking for cache data...")
        # Pokaż co będzie w cache
        compare_apis_dir = Path("compare_apis")
        if compare_apis_dir.exists():
            era5_files = list(compare_apis_dir.glob("**/era5*.csv"))
            provider_files = list(compare_apis_dir.glob("**/provider_*.csv"))
            print(f"  🗄️  ERA5 files: {len(era5_files)}")
            print(f"  🌐 Provider files: {len(provider_files)}")
            
            if era5_files:
                print("  ERA5 files found:")
                for f in era5_files:
                    print(f"    -> {f.relative_to(Path('.'))}")
                    
            if provider_files:
                print("  Provider files found:")
                for f in provider_files[:5]:  # Limit output
                    print(f"    -> {f.relative_to(Path('.'))}")
                if len(provider_files) > 5:
                    print(f"    ... and {len(provider_files) - 5} more")
        
        print(f"\n✅ Scan complete. Run without --scan-only to proceed with migration.")
        return
    
    # 2. Backup (jeśli nie wyłączony)
    if not args.no_backup:
        backup_dirs = ["compare_apis", "wyniki", "dane"]  # Priorytet dla compare_apis
        backup_path = create_backup(backup_dirs)
        print(f"\n[BACKUP] Created backup at: {backup_path}")
    else:
        print(f"\n[WARNING] Backup disabled - changes will be irreversible!")
    
    # 3. Migracja w trybie dry-run
    if args.dry_run:
        print("\n" + "="*40)
        print("DRY RUN - NO CHANGES WILL BE MADE")
        print("="*40)
        
        print(f"\n[DRY RUN] Would migrate {len(summary_files)} summary files to central_weather_results.csv")
        
        # Pokaż pliki cache
        cache_files = migrate_cache_data() if not args.dry_run else 0
        print(f"[DRY RUN] Would migrate approximately {cache_files} cache files")
        
        # Pokaż cleanup
        if not args.no_cleanup:
            files_count, dirs_count = cleanup_old_files(dry_run=True)
            print(f"[DRY RUN] Would remove {files_count} files and {dirs_count} directories")
        
        print("\n[DRY RUN] Run without --dry-run to perform actual migration")
        return
    
    # 4. Rzeczywista migracja
    print("\n[MIGRATE] Starting migration...")
    
    # Migruj summary files
    central_file = Path("central_weather_results.csv")
    processed_files, migrated_records = migrate_summary_files(summary_files, central_file)
    
    # Migruj cache
    cached_files = migrate_cache_data()
    
    # 5. Cleanup (jeśli nie wyłączony)
    if not args.no_cleanup:
        print("\n[CLEANUP] Cleaning up old files...")
        removed_files, removed_dirs = cleanup_old_files()
        print(f"[CLEANUP] Removed {removed_files} files and {removed_dirs} directories")
    
    # 6. Podsumowanie
    print("\n" + "="*60)
    print("MIGRATION SUMMARY")
    print("="*60)
    print(f"Summary files processed: {processed_files}")
    print(f"Records migrated: {migrated_records}")
    print(f"Cache files migrated: {cached_files}")
    
    if not args.no_cleanup:
        print(f"Old files removed: {removed_files}")
        print(f"Empty directories removed: {removed_dirs}")
    
    if migrated_records > 0:
        print(f"\n✅ Migration complete!")
        print(f"📊 Central results: {central_file}")
        print(f"💾 Cache directory: cache/")
        print(f"🎯 Use: python consolidated_weather_analysis.py for new analyses")
    else:
        print("\n⚠️  No records were migrated")
        print("Check if summary files contain valid data")

if __name__ == "__main__":
    main()
