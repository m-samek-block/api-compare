#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# make_era5.py — generuje syntetyczny ERA5 pod (lat,lon) i zakres czasu (UTC)
# Format: time,latitude,longitude,variable,value

import argparse, csv, math, random
from datetime import datetime, timedelta, timezone
from pathlib import Path

ISO = "%Y-%m-%dT%H:%M:%SZ"

def to_utc(dt_str: str) -> datetime:
    # akceptuje ISO z "Z" lub bez
    if dt_str.endswith("Z"):
        dt = datetime.fromisoformat(dt_str.replace("Z","+00:00"))
    else:
        dt = datetime.fromisoformat(dt_str)
    return dt.astimezone(timezone.utc)

def floor_hour(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)

def iso_hours(start: datetime, end: datetime):
    t = floor_hour(start)
    while t < end:
        yield t
        t += timedelta(hours=1)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lat", type=float, required=True)
    ap.add_argument("--lon", type=float, required=True)
    ap.add_argument("--start", type=str, required=True)
    ap.add_argument("--end", type=str, required=True)
    ap.add_argument("--out", type=str, required=True)
    args = ap.parse_args()

    lat, lon = float(args.lat), float(args.lon)
    start = to_utc(args.start)
    end   = to_utc(args.end)
    out   = Path(args.out)

    rng = random.Random(1234)

    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["time","latitude","longitude","variable","value"])

        for t in iso_hours(start, end):
            hodz = t.hour
            base_temp = 20 + 6*math.sin((hodz/24)*2*math.pi)      # cykl dobowy
            base_wspd = 5 + 2*math.sin((hodz/24)*2*math.pi + 1)
            base_wdir = (180 + 30*math.sin((hodz/24)*2*math.pi + .3)) % 360
            # losowy, rzadki opad
            pr = 0.0
            if rng.random() < 0.2:
                pr = max(0.0, rng.gammavariate(1.2, 0.6) - 0.4)

            iso = t.strftime(ISO)
            w.writerow([iso, lat, lon, "temperature_2m",      f"{(base_temp + rng.uniform(-0.7, 0.7)):.3f}"])
            w.writerow([iso, lat, lon, "precipitation",       f"{pr:.3f}"])
            w.writerow([iso, lat, lon, "wind_speed_100m",     f"{(base_wspd + rng.uniform(-0.4, 0.4)):.3f}"])
            w.writerow([iso, lat, lon, "wind_direction_100m", f"{((base_wdir + rng.uniform(-10, 10)) % 360):.3f}"])

    print(f"OK: zapisano {out}")

if __name__ == "__main__":
    main()
