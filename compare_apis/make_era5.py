# make_era5.py
# Generuje era5.csv (time, latitude, longitude, variable, value)
from datetime import datetime, timedelta, timezone
import math, random, csv, argparse
from pathlib import Path

# DOMYŚLNE (gdy nie podasz argumentów)
DEF_LAT, DEF_LON = 52.2297, 21.0122
DEF_START = "2025-08-11T00:00:00Z"
DEF_END   = "2025-08-13T00:00:00Z"
DEF_OUT   = "dane/era5.csv"

VARS = ["temperature_2m", "precipitation", "wind_speed_100m", "wind_direction_100m"]

def iso_hours(s, e):
    t = datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(timezone.utc)
    e = datetime.fromisoformat(e.replace("Z","+00:00")).astimezone(timezone.utc)
    while t < e:
        yield t; t += timedelta(hours=1)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lat", type=float, default=DEF_LAT)
    ap.add_argument("--lon", type=float, default=DEF_LON)
    ap.add_argument("--start", type=str, default=DEF_START)
    ap.add_argument("--end", type=str, default=DEF_END)
    ap.add_argument("--out", type=str, default=DEF_OUT)
    args = ap.parse_args()

    out = Path(args.out); out.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(42)

    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["time","latitude","longitude","variable","value"])
        for t in iso_hours(args.start, args.end):
            hodz = t.hour
            base_temp = 20 + 6*math.sin((hodz/24)*2*math.pi)
            base_wspd = 5 + 2*math.sin((hodz/24)*2*math.pi + 1)
            base_wdir = (180 + 30*math.sin((hodz/24)*2*math.pi + 0.3)) % 360
            temp = base_temp + rng.uniform(-0.7, 0.7)
            wspd = base_wspd + rng.uniform(-0.4, 0.4)
            wdir = (base_wdir + rng.uniform(-10, 10)) % 360
            pr = 0.0
            if rng.random() < 0.2:
                pr = max(0.0, rng.gammavariate(1.2, 0.6) - 0.4)
            iso = t.strftime("%Y-%m-%dT%H:%M:%SZ")
            w.writerow([iso, args.lat, args.lon, "temperature_2m", f"{temp:.3f}"])
            w.writerow([iso, args.lat, args.lon, "precipitation", f"{pr:.3f}"])
            w.writerow([iso, args.lat, args.lon, "wind_speed_100m", f"{wspd:.3f}"])
            w.writerow([iso, args.lat, args.lon, "wind_direction_100m", f"{wdir:.3f}"])

    print(f"OK: zapisano {out}")

if __name__ == "__main__":
    main()
