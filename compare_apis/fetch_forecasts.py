#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# This script fetches hourly forecasts from several *free* weather APIs
# and saves them in the normalized long format required by our ERA5
# comparison pipeline:
#
#   time, latitude, longitude, variable, value
#
# Supported providers (free tiers; some require an API key):
#   - openmeteo         (no key)
#   - metno (MET Norway / Yr) (no key, but requires a proper User-Agent)
#   - visualcrossing    (API key)
#   - openweather       (API key)
#   - weatherapi        (API key)
#
# Notes & caveats:
# - We normalize variable names to: temperature_2m [°C], precipitation [mm],
#   wind_speed_100m [m/s] (only where available), wind_direction_100m [deg].
# - Most providers only expose 10m wind; we include those as wind_speed_10m
#   and wind_direction_10m. If your ERA5 ground truth is for 100m, either
#   (a) fetch 100m wind from Open-Meteo only, or (b) add ERA5 10m and compare
#   like-to-like.
# - Time range: providers differ in horizon and history availability.
#   We fetch the intersection whenever possible.
#
# Usage example (CLI):
#   pip install requests pandas python-dateutil
#   python fetch_forecasts.py --lat 52.2297 --lon 21.0122 \
#     --start 2025-08-10T00:00:00Z --end 2025-08-12T00:00:00Z \
#     --providers openmeteo,metno,visualcrossing,openweather,weatherapi \
#     --outdir ./      # where provider_<name>.csv will be written
#
# API keys (optional; for providers that need them):
#   export VISUALCROSSING_KEY=...
#   export OPENWEATHER_KEY=...
#   export WEATHERAPI_KEY=...
#
import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from dateutil import parser as dtparser
import os
from pathlib import Path
from typing import Iterable, List, Tuple
import requests
import pandas as pd

ISO = "%Y-%m-%dT%H:%M:%SZ"


def iso_floor_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)


def iso_hours(start: datetime, end: datetime) -> List[str]:
    t = iso_floor_hour(start)
    out = []
    while t < end:
        out.append(t.strftime(ISO))
        t += timedelta(hours=1)
    return out


def write_long_csv(
    path: Path,
    lat: float,
    lon: float,
    rows: Iterable[Tuple[str, str, float]],
):
    """
    rows: iterable of (time_iso, variable, value)
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["time", "latitude", "longitude", "variable", "value"])
        for t, var, val in rows:
            w.writerow([t, lat, lon, var, val])


# -------------------- Providers -------------------- #

def fetch_openmeteo(lat: float, lon: float, start: datetime, end: datetime) -> List[Tuple[str, str, float]]:
    # Open-Meteo supports many hourly vars including 100m wind
    base = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join([
            "temperature_2m",
            "precipitation",
            "wind_speed_100m",
            "wind_direction_100m",
            "wind_speed_10m",
            "wind_direction_10m",
        ]),
        "timezone": "UTC",
        # Past/future control: open-meteo has separate archive endpoints for history;
        # for a single call window we rely on forecast horizon.
        "start_hour": start.strftime(ISO),
        "end_hour": end.strftime(ISO),
    }
    # Some deployments use "start_date"/"end_date"; "start_hour"/"end_hour" is supported as of 2024+.
    # We'll fallback if the server rejects unknown params.
    r = requests.get(base, params=params, timeout=30)
    if r.status_code != 200:
        # Fallback to date-based slicing
        params.pop("start_hour", None)
        params.pop("end_hour", None)
        params["start_date"] = start.date().isoformat()
        params["end_date"] = (end - timedelta(seconds=1)).date().isoformat()
        r = requests.get(base, params=params, timeout=30)
        r.raise_for_status()
    data = r.json()
    H = data.get("hourly", {})
    times = H.get("time", [])
    rows = []
    for i, t in enumerate(times):
        for src, tgt in [
            ("temperature_2m","temperature_2m"),
            ("precipitation","precipitation"),
            ("wind_speed_100m","wind_speed_100m"),
            ("wind_direction_100m","wind_direction_100m"),
            ("wind_speed_10m","wind_speed_10m"),
            ("wind_direction_10m","wind_direction_10m"),
        ]:
            arr = H.get(src)
            if arr is not None and i < len(arr) and arr[i] is not None:
                rows.append((t, tgt, float(arr[i])))
    return rows


def fetch_metno(lat: float, lon: float, start: datetime, end: datetime, user_agent: str) -> List[Tuple[str, str, float]]:
    # MET Norway (locationforecast/2.0/compact) — hourly, no key required.
    # Requires a descriptive User-Agent per Terms.
    url = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
    headers = {"User-Agent": user_agent}
    params = {"lat": lat, "lon": lon}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    rows = []
    for item in data.get("properties", {}).get("timeseries", []):
        t_iso = item.get("time")
        if not t_iso:
            continue
        t = dtparser.isoparse(t_iso)
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        else:
            t = t.astimezone(timezone.utc)
        if not (start <= t < end):
            continue
        details = (item.get("data", {}) or {}).get("instant", {}).get("details", {})
        # Temperature at 2m:
        if "air_temperature" in details:
            rows.append((t.strftime(ISO), "temperature_2m", float(details["air_temperature"])))
        # Wind 10m:
        if "wind_speed" in details:
            rows.append((t.strftime(ISO), "wind_speed_10m", float(details["wind_speed"])))
        if "wind_from_direction" in details:
            rows.append((t.strftime(ISO), "wind_direction_10m", float(details["wind_from_direction"])))
        # Precipitation from next hour summary (if available)
        next_hour = (item.get("data", {}) or {}).get("next_1_hours", {})
        if "details" in next_hour and "precipitation_amount" in next_hour["details"]:
            rows.append((t.strftime(ISO), "precipitation", float(next_hour["details"]["precipitation_amount"])))
    return rows


def fetch_visualcrossing(lat: float, lon: float, start: datetime, end: datetime, api_key: str) -> List[Tuple[str, str, float]]:
    """
    Visual Crossing 'timeline' (history+forecast) with robust time parsing via datetimeEpoch.
    Zwraca: (time_iso, variable, value) w godzinnej rozdzielczości, jednostki metryczne.
    """
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{lat},{lon}/{start.date()}/{(end - timedelta(seconds=1)).date()}"
    params = {
        "unitGroup": "metric",
        "include": "hours",
        "key": api_key,
        "contentType": "json",
    }
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    rows = []
    for day in data.get("days", []):
        for h in day.get("hours", []):
            # <= KLUCZOWE: preferujemy epoch
            if "datetimeEpoch" in h:
                t = datetime.fromtimestamp(h["datetimeEpoch"], tz=timezone.utc)
            else:
                t = dtparser.isoparse(h.get("datetime") or h.get("datetimeStr"))
                t = t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t.astimezone(timezone.utc)

            if not (start <= t < end):
                continue
            iso = t.strftime(ISO)

            # temp (°C)
            if "temp" in h:
                rows.append((iso, "temperature_2m", float(h["temp"])))
            # precip (mm/h)
            if h.get("precip") is not None:
                rows.append((iso, "precipitation", float(h["precip"])))
            # wind 10m (m/s) z km/h
            if h.get("windspeed") is not None:
                rows.append((iso, "wind_speed_10m", float(h["windspeed"]) / 3.6))
            # wind direction 10m (deg)
            if h.get("winddir") is not None:
                rows.append((iso, "wind_direction_10m", float(h["winddir"])))
    return rows




def fetch_openweather(lat: float, lon: float, start: datetime, end: datetime, api_key: str):
    """
    OpenWeather:
      - preferuje One Call 3.0 /hourly (do ~48h); ma 'rain': {'1h': mm}, 'snow': {'1h': mm}
      - fallback: 5 day / 3 hour forecast (dzielimy 'rain.3h'/'snow.3h' przez 3 => mm/h)
    Zwraca listę (time_iso, variable, value).
    """
    rows = []
    try:
        # 1) Spróbuj One Call (godzinowa na ~48h)
        if (end - start) <= timedelta(hours=48):
            url = "https://api.openweathermap.org/data/3.0/onecall"
            params = {
                "lat": lat, "lon": lon, "appid": api_key, "units": "metric",
                "exclude": "minutely,daily,current,alerts",
            }
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            for h in data.get("hourly", []):
                t = datetime.fromtimestamp(h["dt"], tz=timezone.utc)
                if not (start <= t < end):
                    continue
                iso = t.strftime(ISO)
                # temperatura
                if h.get("temp") is not None:
                    rows.append((iso, "temperature_2m", float(h["temp"])))
                # wiatr 10 m
                if h.get("wind_speed") is not None:
                    rows.append((iso, "wind_speed_10m", float(h["wind_speed"])))
                if h.get("wind_deg") is not None:
                    rows.append((iso, "wind_direction_10m", float(h["wind_deg"])))
                # opady (mm/h): rain.1h + snow.1h
                pr = 0.0
                if isinstance(h.get("rain"), dict) and "1h" in h["rain"]:
                    pr += float(h["rain"]["1h"])
                if isinstance(h.get("snow"), dict) and "1h" in h["snow"]:
                    pr += float(h["snow"]["1h"])
                # dopisz nawet 0.0, żeby mieć pełną serię
                rows.append((iso, "precipitation", float(pr)))
            return rows
    except Exception as e:
        # jeśli One Call nieosiągalny (np. plan), przejdź do fallbacku
        print(f"  [openweather] One Call fallback: {e}")

    # 2) Fallback: 5-dniowa prognoza co 3h
    url = "https://api.openweathermap.org/data/2.5/forecast"
    params = {"lat": lat, "lon": lon, "appid": api_key, "units": "metric"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    for e3 in data.get("list", []):
        t = datetime.fromtimestamp(e3["dt"], tz=timezone.utc)
        if not (start <= t < end):
            continue
        iso = t.strftime(ISO)
        main = e3.get("main", {})
        if main.get("temp") is not None:
            rows.append((iso, "temperature_2m", float(main["temp"])))
        wind = e3.get("wind", {})
        if wind.get("speed") is not None:
            rows.append((iso, "wind_speed_10m", float(wind["speed"])))
        if wind.get("deg") is not None:
            rows.append((iso, "wind_direction_10m", float(wind["deg"])))
        # opad 3h -> mm/h (dzielimy przez 3)
        pr_3h = 0.0
        rain = e3.get("rain", {})
        snow = e3.get("snow", {})
        if isinstance(rain, dict):
            # '3h' lub e.g. '1h' w niektórych odpowiedziach
            pr_3h += float(rain.get("3h", rain.get("1h", 0.0)))
        if isinstance(snow, dict):
            pr_3h += float(snow.get("3h", snow.get("1h", 0.0)))
        rows.append((iso, "precipitation", float(pr_3h) / 3.0))
    return rows


def fetch_weatherapi(lat: float, lon: float, start: datetime, end: datetime, api_key: str) -> List[Tuple[str, str, float]]:
    # WeatherAPI: forecast.json supports up to 14 days depending on plan.
    rows = []
    cur = start
    while cur < end:
        days = min(10, (end.date() - cur.date()).days + 1)  # chunk
        url = "http://api.weatherapi.com/v1/forecast.json"
        params = {"key": api_key, "q": f"{lat},{lon}", "days": days, "aqi": "no", "alerts": "no"}
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        for d in data.get("forecast", {}).get("forecastday", []):
            for h in d.get("hour", []):
                t = dtparser.isoparse(h["time"])
                if t.tzinfo is None:
                    t = t.replace(tzinfo=timezone.utc)
                else:
                    t = t.astimezone(timezone.utc)
                if not (start <= t < end):
                    continue
                iso = t.strftime(ISO)
                if "temp_c" in h:
                    rows.append((iso, "temperature_2m", float(h["temp_c"])))
                if "precip_mm" in h and h["precip_mm"] is not None:
                    rows.append((iso, "precipitation", float(h["precip_mm"])))
                if "wind_kph" in h and h["wind_kph"] is not None:
                    rows.append((iso, "wind_speed_10m", float(h["wind_kph"]) / 3.6))
                if "wind_degree" in h and h["wind_degree"] is not None:
                    rows.append((iso, "wind_direction_10m", float(h["wind_degree"])))
        cur += timedelta(days=days)
    return rows


# -------------------- Orchestrator -------------------- #

PROVIDERS = {
    "openmeteo": fetch_openmeteo,
    "metno": fetch_metno,
    "visualcrossing": fetch_visualcrossing,
    "openweather": fetch_openweather,
    "weatherapi": fetch_weatherapi,
}


def main():
    ap = argparse.ArgumentParser(description="Fetch weather forecasts from multiple free APIs and save normalized CSVs.")
    ap.add_argument("--lat", type=float, required=True)
    ap.add_argument("--lon", type=float, required=True)
    ap.add_argument("--start", type=str, required=True, help="ISO8601, e.g., 2025-08-10T00:00:00Z")
    ap.add_argument("--end", type=str, required=True, help="ISO8601, exclusive")
    ap.add_argument("--providers", type=str, default="openmeteo,metno,visualcrossing,openweather,weatherapi")
    ap.add_argument("--outdir", type=str, default=".")
    ap.add_argument("--metno-user-agent", type=str, default="BlockWise-Weather-Compare/1.0 (contact: you@example.com)")
    # API keys via env or flags
    ap.add_argument("--visualcrossing-key", type=str, default=os.environ.get("VISUALCROSSING_KEY", "X5TXF44RU47MDP2HHYS52P7KB"))
    ap.add_argument("--openweather-key", type=str, default=os.environ.get("OPENWEATHER_KEY", "54f216d976bdf228f5a89444ca5d1502"))
    ap.add_argument("--weatherapi-key", type=str, default=os.environ.get("WEATHERAPI_KEY", "564b50d8beb04a26986131157251108"))

    args = ap.parse_args()

    lat, lon = args.lat, args.lon
    start = dtparser.isoparse(args.start).astimezone(timezone.utc)
    end = dtparser.isoparse(args.end).astimezone(timezone.utc)
    outdir = Path(args.outdir)

    reqs = [p.strip().lower() for p in args.providers.split(",") if p.strip()]
    for p in reqs:
        if p not in PROVIDERS:
            raise SystemExit(f"Unknown provider: {p}. Supported: {', '.join(PROVIDERS.keys())}")

    for prov in reqs:
        print(f"Fetching {prov} ...")
        try:
            if prov == "metno":
                rows = fetch_metno(lat, lon, start, end, user_agent=args.metno_user_agent)
            elif prov == "visualcrossing":
                if not args.visualcrossing_key:
                    print("  Skipping visualcrossing (no API key).")
                    continue
                rows = fetch_visualcrossing(lat, lon, start, end, api_key=args.visualcrossing_key)
            elif prov == "openweather":
                if not args.openweather_key:
                    print("  Skipping openweather (no API key).")
                    continue
                rows = fetch_openweather(lat, lon, start, end, api_key=args.openweather_key)
            elif prov == "weatherapi":
                if not args.weatherapi_key:
                    print("  Skipping weatherapi (no API key).")
                    continue
                rows = fetch_weatherapi(lat, lon, start, end, api_key=args.weatherapi_key)
            else:
                rows = fetch_openmeteo(lat, lon, start, end)  # openmeteo
        except Exception as e:
            print(f"  Error fetching {prov}: {e}")
            continue

        if not rows:
            print(f"  No rows fetched for {prov}.")
            continue
        path = outdir / f"provider_{prov}.csv"
        write_long_csv(path, lat, lon, rows)
        print(f"  Wrote {path} ({len(rows)} rows).")


if __name__ == "__main__":
    main()
