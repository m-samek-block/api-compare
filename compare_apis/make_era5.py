# make_era5.py
# Generuje syntetyczny plik era5.csv (time,latitude,longitude,variable,value)
# dla podanego zakresu czasu (UTC) i okolic Warszawy (2x2 punkty siatki).

from datetime import datetime, timedelta, timezone
import math, random, csv, sys
from pathlib import Path

# === USTAWIENIA ===
LAT0, LON0 = 52.2297, 21.0122   # Warszawa
DLAT, DLON = 0.05, 0.05         # odstęp siatki
START = "2025-08-11T00:00:00Z"  # dostosuj do swojego uruchomienia
END   = "2025-08-13T00:00:00Z"
OUT   = Path(r"C:\Users\msamek\PycharmProjects\FastAPIProject\compare_apis\dane\era5.csv")

# Zmienne do wygenerowania (możesz dodać kolejne)
VARS = ["temperature_2m", "precipitation", "wind_speed_100m", "wind_direction_100m"]

def iso_hours(start_iso, end_iso):
    t = datetime.fromisoformat(start_iso.replace("Z","+00:00")).astimezone(timezone.utc)
    end = datetime.fromisoformat(end_iso.replace("Z","+00:00")).astimezone(timezone.utc)
    while t < end:
        yield t
        t += timedelta(hours=1)

def main():
    pts = [
        (LAT0, LON0),
        (LAT0+DLAT, LON0),
        (LAT0, LON0+DLON),
        (LAT0+DLAT, LON0+DLON),
    ]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(42)

    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["time","latitude","longitude","variable","value"])
        for t in iso_hours(START, END):
            hodz = t.hour
            # proste, "wiarygodne" kształty do testów
            base_temp = 20 + 6*math.sin((hodz/24)*2*math.pi)  # dobowy cykl
            base_wspd = 5 + 2*math.sin((hodz/24)*2*math.pi + 1)
            base_wdir = (180 + 30*math.sin((hodz/24)*2*math.pi + 0.3)) % 360
            for (lat, lon) in pts:
                temp = base_temp + rng.uniform(-0.7, 0.7)
                wspd = base_wspd + rng.uniform(-0.4, 0.4)
                wdir = (base_wdir + rng.uniform(-10, 10)) % 360
                # rzadkie opady
                pr = 0.0
                if rng.random() < 0.2:
                    pr = max(0.0, rng.gammavariate(1.2, 0.6) - 0.4)

                iso = t.strftime("%Y-%m-%dT%H:%M:%SZ")
                w.writerow([iso, lat, lon, "temperature_2m", f"{temp:.3f}"])
                w.writerow([iso, lat, lon, "precipitation", f"{pr:.3f}"])
                w.writerow([iso, lat, lon, "wind_speed_100m", f"{wspd:.3f}"])
                w.writerow([iso, lat, lon, "wind_direction_100m", f"{wdir:.3f}"])

    print(f"OK: zapisano {OUT}")

if __name__ == "__main__":
    main()
