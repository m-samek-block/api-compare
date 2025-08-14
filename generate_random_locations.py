#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_random_locations.py - Generator losowych lokalizacji dla batch analysis

Generuje plik JSON z losowymi współrzędnymi do użycia z batch_analyzer.py
"""

import json
import random
import argparse
from pathlib import Path

# Obszary lądowe (przybliżone bbox dla kontynentów)
LAND_AREAS = [
    # Europa
    {"name": "Europe", "min_lat": 35, "max_lat": 71, "min_lon": -10, "max_lon": 40},
    # Azja
    {"name": "Asia", "min_lat": 10, "max_lat": 70, "min_lon": 60, "max_lon": 180},
    # Ameryka Północna
    {"name": "North_America", "min_lat": 15, "max_lat": 70, "min_lon": -170, "max_lon": -50},
    # Ameryka Południowa  
    {"name": "South_America", "min_lat": -55, "max_lat": 15, "min_lon": -85, "max_lon": -35},
    # Afryka
    {"name": "Africa", "min_lat": -35, "max_lat": 37, "min_lon": -20, "max_lon": 52},
    # Australia/Oceania
    {"name": "Australia", "min_lat": -45, "max_lat": -10, "min_lon": 110, "max_lon": 160},
]

def generate_random_coordinates(count: int, include_oceans: bool = False, min_distance: float = 1.0):
    """Generuje losowe współrzędne"""
    locations = {}
    attempts = 0
    max_attempts = count * 10  # 10x więcej prób
    
    print(f"🎲 Generating {count} random locations...")
    print(f"🌊 Include oceans: {include_oceans}")
    print(f"📏 Min distance: {min_distance}°")
    print("")
    
    while len(locations) < count and attempts < max_attempts:
        attempts += 1
        
        if include_oceans:
            # Całkowicie losowe (włącznie z oceanami)
            lat = random.uniform(-90, 90)
            lon = random.uniform(-180, 180)
            region = "Ocean_Random"
        else:
            # Wybierz losowy obszar lądowy
            area = random.choice(LAND_AREAS)
            lat = random.uniform(area["min_lat"], area["max_lat"])
            lon = random.uniform(area["min_lon"], area["max_lon"])
            region = area["name"]
            
            # Dodaj małą losowość dla precyzji
            lat += random.uniform(-0.5, 0.5)
            lon += random.uniform(-0.5, 0.5)
            
            # Ogranicz do prawidłowych wartości
            lat = max(-90, min(90, lat))
            lon = max(-180, min(180, lon))
        
        # Sprawdź odległość od istniejących lokalizacji
        too_close = False
        for existing_coords in locations.values():
            distance = ((lat - existing_coords[0])**2 + (lon - existing_coords[1])**2)**0.5
            if distance < min_distance:
                too_close = True
                break
        
        if not too_close:
            location_name = f"Random_{len(locations)+1:03d}_{region}"
            locations[location_name] = [round(lat, 4), round(lon, 4)]
            
            # Progress indicator
            if len(locations) % 10 == 0 or len(locations) <= 20:
                print(f"📍 Generated {len(locations):3d}: {location_name} ({lat:.4f}, {lon:.4f})")
    
    if len(locations) < count:
        print(f"\n⚠️ Generated only {len(locations)} unique locations (attempted {attempts} times)")
        print(f"💡 Try reducing --min-distance or increasing --count for better results")
    
    return locations

def main():
    parser = argparse.ArgumentParser(description="Generate random locations for batch analysis")
    parser.add_argument("--count", type=int, default=50, help="Number of random locations")
    parser.add_argument("--include-oceans", action="store_true", help="Include ocean locations")
    parser.add_argument("--min-distance", type=float, default=1.0, help="Minimum distance between locations (degrees)")
    parser.add_argument("--output", type=str, default="random_locations.json", help="Output JSON file")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("🎲 RANDOM LOCATION GENERATOR")
    print("=" * 60)
    
    # Generuj lokalizacje
    locations = generate_random_coordinates(
        count=args.count,
        include_oceans=args.include_oceans,
        min_distance=args.min_distance
    )
    
    # Zapisz do JSON
    output_file = Path(args.output)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(locations, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Generated {len(locations)} locations")
    print(f"💾 Saved to: {output_file}")
    
    # Statystyki per region
    if not args.include_oceans:
        region_counts = {}
        for location_name in locations.keys():
            parts = location_name.split('_')
            if len(parts) >= 3:
                region = parts[2]  # Extract region from name
                region_counts[region] = region_counts.get(region, 0) + 1
        
        print(f"\n📊 Distribution by region:")
        for region, count in sorted(region_counts.items()):
            percentage = (count / len(locations)) * 100
            print(f"  🌍 {region}: {count} locations ({percentage:.1f}%)")
    
    print(f"\n🚀 Next steps:")
    print(f"  1. Review generated locations in: {output_file}")
    print(f"  2. Run batch analysis:")
    print(f"     python batch_analyzer.py --custom-locations {output_file} --time-period yesterday --max-workers 5")
    print(f"  3. Analyze results:")
    print(f"     python analysis_viewer.py overview")

if __name__ == "__main__":
    main()