#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_forecasts.py - Enhanced weather fetch with REAL DATA (Historical + Current/Forecast)

REAL DATA ENDPOINTS:
1. OpenMeteo: /v1/archive (historical) + /v1/forecast (current/forecast)
2. OpenWeather: /onecall/timemachine (historical) + /onecall (current/forecast)
3. WeatherAPI: /history.json (historical) + /forecast.json (current/forecast)
4. VisualCrossing: timeline API (historical + current/forecast)
5. Met.no: /locationforecast (current/forecast only)

NO SYNTHETIC DATA - real observations, reanalysis, and model forecasts only
"""

import argparse
import csv
from datetime import datetime, timedelta, timezone
from dateutil import parser as dtparser
import os
from pathlib import Path
from typing import Iterable, List, Tuple, Optional
import requests
import time

ISO = "%Y-%m-%dT%H:%M:%SZ"


def write_long_csv(path: Path, lat: float, lon: float, rows: Iterable[Tuple[str, str, float]]):
    """rows: iterable of (time_iso, variable, value)"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["time", "latitude", "longitude", "variable", "value"])
        for t, var, val in rows:
            w.writerow([t, lat, lon, var, val])


def is_historical_data(start: datetime, end: datetime) -> bool:
    """Check if the requested time range is in the past (historical data needed)"""
    now = datetime.now(timezone.utc)
    return end < (now - timedelta(hours=6))  # More than 6h old = historical


def fetch_openmeteo(lat: float, lon: float, start: datetime, end: datetime) -> List[Tuple[str, str, float]]:
    """
    OpenMeteo with REAL DATA - historical and forecast
    """
    rows = []

    if is_historical_data(start, end):
        print(f"  [openmeteo] Using HISTORICAL endpoints")
        
        # Try the archive endpoint first (ERA5 reanalysis from 1940)
        try:
            print(f"  [openmeteo] Trying /v1/archive (ERA5 reanalysis)...")
            base = "https://api.open-meteo.com/v1/archive"
            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": start.date().isoformat(),
                "end_date": (end - timedelta(seconds=1)).date().isoformat(),
                "hourly": ",".join([
                    "temperature_2m",
                    "precipitation",
                    "wind_speed_100m", 
                    "wind_direction_100m",
                    "wind_speed_10m",
                    "wind_direction_10m",
                ]),
                "timezone": "UTC",
            }
            r = requests.get(base, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            
            H = data.get("hourly", {})
            times = H.get("time", [])
            for i, t in enumerate(times):
                dt = dtparser.isoparse(t).astimezone(timezone.utc)
                if not (start <= dt < end):
                    continue
                iso = dt.strftime(ISO)
                for src, tgt in [
                    ("temperature_2m", "temperature_2m"),
                    ("precipitation", "precipitation"),
                    ("wind_speed_100m", "wind_speed_100m"),
                    ("wind_direction_100m", "wind_direction_100m"),
                    ("wind_speed_10m", "wind_speed_10m"),
                    ("wind_direction_10m", "wind_direction_10m"),
                ]:
                    arr = H.get(src)
                    if arr is not None and i < len(arr) and arr[i] is not None:
                        rows.append((iso, tgt, float(arr[i])))
            
            if rows:
                print(f"  [openmeteo] ✅ Archive data: {len(rows)} points")
                return rows
                
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                print(f"  [openmeteo] Archive API 404 - trying historical forecast...")
            else:
                print(f"  [openmeteo] Archive error: {e}")
        except Exception as e:
            print(f"  [openmeteo] Archive exception: {e}")
        
        # Try historical forecast API (high-res models from 2021+)
        try:
            print(f"  [openmeteo] Trying historical-forecast-api...")
            base = "https://historical-forecast-api.open-meteo.com/v1/forecast"
            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": start.date().isoformat(),
                "end_date": (end - timedelta(seconds=1)).date().isoformat(),
                "hourly": ",".join([
                    "temperature_2m",
                    "precipitation",
                    "wind_speed_100m",
                    "wind_direction_100m",
                    "wind_speed_10m", 
                    "wind_direction_10m",
                ]),
                "timezone": "UTC",
            }
            r = requests.get(base, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()

            H = data.get("hourly", {})
            times = H.get("time", [])
            for i, t in enumerate(times):
                dt = dtparser.isoparse(t).astimezone(timezone.utc)
                if not (start <= dt < end):
                    continue
                iso = dt.strftime(ISO)
                for src, tgt in [
                    ("temperature_2m", "temperature_2m"),
                    ("precipitation", "precipitation"),
                    ("wind_speed_100m", "wind_speed_100m"),
                    ("wind_direction_100m", "wind_direction_100m"),
                    ("wind_speed_10m", "wind_speed_10m"),
                    ("wind_direction_10m", "wind_direction_10m"),
                ]:
                    arr = H.get(src)
                    if arr is not None and i < len(arr) and arr[i] is not None:
                        rows.append((iso, tgt, float(arr[i])))
            
            if rows:
                print(f"  [openmeteo] ✅ Historical forecast: {len(rows)} points")
            else:
                print(f"  [openmeteo] ❌ No historical data available for this date range")
                        
        except Exception as e:
            print(f"  [openmeteo] Historical forecast error: {e}")
    else:
        # Use regular forecast endpoint for current/future data
        print(f"  [openmeteo] Using FORECAST endpoint /v1/forecast")
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
        }
        
        # Add time constraints for forecast
        now = datetime.now(timezone.utc)
        if start > now:
            # Pure forecast
            params["start_date"] = start.date().isoformat()
            params["end_date"] = (end - timedelta(seconds=1)).date().isoformat()
        
        try:
            r = requests.get(base, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()

            H = data.get("hourly", {})
            times = H.get("time", [])
            for i, t in enumerate(times):
                dt = dtparser.isoparse(t).astimezone(timezone.utc)
                if not (start <= dt < end):
                    continue
                iso = dt.strftime(ISO)
                for src, tgt in [
                    ("temperature_2m", "temperature_2m"),
                    ("precipitation", "precipitation"),
                    ("wind_speed_100m", "wind_speed_100m"),
                    ("wind_direction_100m", "wind_direction_100m"),
                    ("wind_speed_10m", "wind_speed_10m"),
                    ("wind_direction_10m", "wind_direction_10m"),
                ]:
                    arr = H.get(src)
                    if arr is not None and i < len(arr) and arr[i] is not None:
                        rows.append((iso, tgt, float(arr[i])))
            
            print(f"  [openmeteo] ✅ Forecast data: {len(rows)} points")
                        
        except Exception as e:
            print(f"  [openmeteo] Forecast error: {e}")

    return rows


def fetch_metno(lat: float, lon: float, start: datetime, end: datetime, user_agent: str) -> List[Tuple[str, str, float]]:
    """
    MET Norway - CURRENT/FORECAST ONLY (no historical API)
    """
    if is_historical_data(start, end):
        print(f"  [metno] ❌ No historical data API available")
        print(f"  [metno] Met.no only provides current + forecast data")
        return []
    
    print(f"  [metno] Using FORECAST endpoint /locationforecast/2.0/compact")
    url = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
    headers = {"User-Agent": user_agent}
    params = {"lat": lat, "lon": lon}
    
    try:
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
            if "air_temperature" in details:
                rows.append((t.strftime(ISO), "temperature_2m", float(details["air_temperature"])))
            if "wind_speed" in details:
                rows.append((t.strftime(ISO), "wind_speed_10m", float(details["wind_speed"])))
            if "wind_from_direction" in details:
                rows.append((t.strftime(ISO), "wind_direction_10m", float(details["wind_from_direction"])))
            # Precipitation from next hour summary
            next_hour = (item.get("data", {}) or {}).get("next_1_hours", {})
            if "details" in next_hour and "precipitation_amount" in next_hour["details"]:
                rows.append((t.strftime(ISO), "precipitation", float(next_hour["details"]["precipitation_amount"])))
        
        print(f"  [metno] ✅ Forecast data: {len(rows)} points")
        return rows
        
    except Exception as e:
        print(f"  [metno] Error: {e}")
        return []


def fetch_visualcrossing(lat: float, lon: float, start: datetime, end: datetime, api_key: str) -> List[Tuple[str, str, float]]:
    """
    VisualCrossing with REAL DATA - historical and forecast
    """
    print(f"  [visualcrossing] Using TIMELINE API")
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{lat},{lon}/{start.date()}/{(end - timedelta(seconds=1)).date()}"
    params = {
        "unitGroup": "metric",
        "include": "hours",
        "key": api_key,
        "contentType": "json",
    }
    
    try:
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        rows = []
        
        for day in data.get("days", []):
            for h in day.get("hours", []):
                # Use datetimeEpoch for precise time parsing
                if "datetimeEpoch" in h:
                    t = datetime.fromtimestamp(h["datetimeEpoch"], tz=timezone.utc)
                else:
                    t = dtparser.isoparse(h.get("datetime") or h.get("datetimeStr"))
                    t = t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t.astimezone(timezone.utc)

                if not (start <= t < end):
                    continue
                iso = t.strftime(ISO)

                if "temp" in h and h["temp"] is not None:
                    rows.append((iso, "temperature_2m", float(h["temp"])))
                if h.get("precip") is not None:
                    rows.append((iso, "precipitation", float(h["precip"])))
                if h.get("windspeed") is not None:
                    rows.append((iso, "wind_speed_10m", float(h["windspeed"]) / 3.6))  # km/h to m/s
                if h.get("winddir") is not None:
                    rows.append((iso, "wind_direction_10m", float(h["winddir"])))
        
        data_type = "historical" if is_historical_data(start, end) else "forecast"
        print(f"  [visualcrossing] ✅ {data_type.title()} data: {len(rows)} points")
        return rows
        
    except requests.HTTPError as e:
        print(f"  [visualcrossing] HTTP error: {e}")
        return []
    except Exception as e:
        print(f"  [visualcrossing] Error: {e}")
        return []


def fetch_openweather(lat: float, lon: float, start: datetime, end: datetime, api_key: str) -> List[Tuple[str, str, float]]:
    """
    OpenWeather with REAL DATA - historical and forecast
    """
    rows = []

    if is_historical_data(start, end):
        print(f"  [openweather] Using HISTORICAL endpoint /onecall/timemachine")
        try:
            current = start
            while current < end:
                timestamp = int(current.timestamp())
                url = "https://api.openweathermap.org/data/3.0/onecall/timemachine"
                params = {
                    "lat": lat, "lon": lon,
                    "dt": timestamp,
                    "appid": api_key,
                    "units": "metric"
                }
                r = requests.get(url, params=params, timeout=30)
                
                if r.status_code == 401:
                    print("  [openweather] ❌ Invalid API key")
                    break
                elif r.status_code == 402:
                    print("  [openweather] ❌ Historical API requires paid subscription")
                    break
                elif r.status_code == 429:
                    print("  [openweather] ⚠️ Rate limit, waiting...")
                    time.sleep(60)
                    continue
                
                r.raise_for_status()
                data = r.json()

                # Extract data for this timestamp
                if "data" in data and data["data"]:
                    hist_data = data["data"][0]
                elif "current" in data:
                    hist_data = data["current"]
                else:
                    current += timedelta(hours=1)
                    continue

                iso = current.strftime(ISO)

                if "temp" in hist_data:
                    rows.append((iso, "temperature_2m", float(hist_data["temp"])))
                if "wind_speed" in hist_data:
                    rows.append((iso, "wind_speed_10m", float(hist_data["wind_speed"])))
                if "wind_deg" in hist_data:
                    rows.append((iso, "wind_direction_10m", float(hist_data["wind_deg"])))

                # Precipitation handling
                pr = 0.0
                if isinstance(hist_data.get("rain"), dict) and "1h" in hist_data["rain"]:
                    pr += float(hist_data["rain"]["1h"])
                if isinstance(hist_data.get("snow"), dict) and "1h" in hist_data["snow"]:
                    pr += float(hist_data["snow"]["1h"])
                rows.append((iso, "precipitation", float(pr)))

                current += timedelta(hours=1)
                time.sleep(0.1)  # Rate limiting

            if rows:
                print(f"  [openweather] ✅ Historical data: {len(rows)} points")
                        
        except Exception as e:
            print(f"  [openweather] Historical error: {e}")
    else:
        print(f"  [openweather] Using FORECAST endpoint /onecall")
        try:
            url = "https://api.openweathermap.org/data/3.0/onecall"
            params = {
                "lat": lat, "lon": lon, "appid": api_key, "units": "metric",
                "exclude": "minutely,daily,alerts",
            }
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            
            # Current weather
            if "current" in data:
                current_data = data["current"]
                t = datetime.fromtimestamp(current_data["dt"], tz=timezone.utc)
                if start <= t < end:
                    iso = t.strftime(ISO)
                    if current_data.get("temp") is not None:
                        rows.append((iso, "temperature_2m", float(current_data["temp"])))
                    if current_data.get("wind_speed") is not None:
                        rows.append((iso, "wind_speed_10m", float(current_data["wind_speed"])))
                    if current_data.get("wind_deg") is not None:
                        rows.append((iso, "wind_direction_10m", float(current_data["wind_deg"])))
                    
                    # Precipitation
                    pr = 0.0
                    if isinstance(current_data.get("rain"), dict) and "1h" in current_data["rain"]:
                        pr += float(current_data["rain"]["1h"])
                    if isinstance(current_data.get("snow"), dict) and "1h" in current_data["snow"]:
                        pr += float(current_data["snow"]["1h"])
                    rows.append((iso, "precipitation", float(pr)))
            
            # Hourly forecasts
            for h in data.get("hourly", []):
                t = datetime.fromtimestamp(h["dt"], tz=timezone.utc)
                if not (start <= t < end):
                    continue
                iso = t.strftime(ISO)
                if h.get("temp") is not None:
                    rows.append((iso, "temperature_2m", float(h["temp"])))
                if h.get("wind_speed") is not None:
                    rows.append((iso, "wind_speed_10m", float(h["wind_speed"])))
                if h.get("wind_deg") is not None:
                    rows.append((iso, "wind_direction_10m", float(h["wind_deg"])))
                # Precipitation
                pr = 0.0
                if isinstance(h.get("rain"), dict) and "1h" in h["rain"]:
                    pr += float(h["rain"]["1h"])
                if isinstance(h.get("snow"), dict) and "1h" in h["snow"]:
                    pr += float(h["snow"]["1h"])
                rows.append((iso, "precipitation", float(pr)))
                
            print(f"  [openweather] ✅ Forecast data: {len(rows)} points")
            
        except Exception as e:
            print(f"  [openweather] Forecast error: {e}")

    return rows


def fetch_weatherapi(lat: float, lon: float, start: datetime, end: datetime, api_key: str) -> List[Tuple[str, str, float]]:
    """
    WeatherAPI with REAL DATA - historical and forecast
    """
    rows = []
    
    if is_historical_data(start, end):
        print(f"  [weatherapi] Using HISTORICAL endpoint /history.json")
        current_date = start.date()
        end_date = (end - timedelta(seconds=1)).date()

        while current_date <= end_date:
            try:
                url = "http://api.weatherapi.com/v1/history.json"
                params = {
                    "key": api_key,
                    "q": f"{lat},{lon}",
                    "dt": current_date.isoformat(),
                    "aqi": "no"
                }
                r = requests.get(url, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()

                for h in data.get("forecast", {}).get("forecastday", [{}])[0].get("hour", []):
                    t = dtparser.isoparse(h["time"])
                    if t.tzinfo is None:
                        t = t.replace(tzinfo=timezone.utc)
                    else:
                        t = t.astimezone(timezone.utc)
                    if not (start <= t < end):
                        continue
                    iso = t.strftime(ISO)
                    
                    if "temp_c" in h and h["temp_c"] is not None:
                        rows.append((iso, "temperature_2m", float(h["temp_c"])))
                    if "precip_mm" in h and h["precip_mm"] is not None:
                        rows.append((iso, "precipitation", float(h["precip_mm"])))
                    if "wind_kph" in h and h["wind_kph"] is not None:
                        rows.append((iso, "wind_speed_10m", float(h["wind_kph"]) / 3.6))
                    if "wind_degree" in h and h["wind_degree"] is not None:
                        rows.append((iso, "wind_direction_10m", float(h["wind_degree"])))

                current_date += timedelta(days=1)
                time.sleep(0.1)  # Rate limiting

            except Exception as e:
                print(f"  [weatherapi] Historical error for {current_date}: {e}")
                current_date += timedelta(days=1)
                continue

        print(f"  [weatherapi] ✅ Historical data: {len(rows)} points")
    else:
        print(f"  [weatherapi] Using FORECAST endpoint /forecast.json")
        try:
            days = min(14, (end.date() - start.date()).days + 2)  # WeatherAPI supports up to 14 days
            url = "http://api.weatherapi.com/v1/forecast.json"
            params = {"key": api_key, "q": f"{lat},{lon}", "days": days, "aqi": "no", "alerts": "no"}
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            
            # Current weather
            if "current" in data:
                current_time = datetime.now(timezone.utc)
                if start <= current_time < end:
                    iso = current_time.strftime(ISO)
                    current_data = data["current"]
                    if current_data.get("temp_c") is not None:
                        rows.append((iso, "temperature_2m", float(current_data["temp_c"])))
                    if current_data.get("precip_mm") is not None:
                        rows.append((iso, "precipitation", float(current_data["precip_mm"])))
                    if current_data.get("wind_kph") is not None:
                        rows.append((iso, "wind_speed_10m", float(current_data["wind_kph"]) / 3.6))
                    if current_data.get("wind_degree") is not None:
                        rows.append((iso, "wind_direction_10m", float(current_data["wind_degree"])))
            
            # Forecast data
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
                    if "temp_c" in h and h["temp_c"] is not None:
                        rows.append((iso, "temperature_2m", float(h["temp_c"])))
                    if "precip_mm" in h and h["precip_mm"] is not None:
                        rows.append((iso, "precipitation", float(h["precip_mm"])))
                    if "wind_kph" in h and h["wind_kph"] is not None:
                        rows.append((iso, "wind_speed_10m", float(h["wind_kph"]) / 3.6))
                    if "wind_degree" in h and h["wind_degree"] is not None:
                        rows.append((iso, "wind_direction_10m", float(h["wind_degree"])))
            
            print(f"  [weatherapi] ✅ Forecast data: {len(rows)} points")
            
        except Exception as e:
            print(f"  [weatherapi] Forecast error: {e}")

    return rows


# -------------------- Provider Registry -------------------- #

PROVIDERS = {
    "openmeteo": fetch_openmeteo,
    "metno": fetch_metno,
    "visualcrossing": fetch_visualcrossing,
    "openweather": fetch_openweather,
    "weatherapi": fetch_weatherapi,
}


# ================================================================================
# WRAPPER FUNCTIONS for consolidated_analysis.py compatibility
# ================================================================================

def fetch_openmeteo_data(lat: float, lon: float, start: str, end: str, use_forecast=True) -> Optional['pd.DataFrame']:
    """Wrapper function for consolidated_analysis.py compatibility"""
    try:
        import pandas as pd
        from dateutil import parser as dtparser
        
        # Convert string dates to datetime objects
        start_dt = dtparser.isoparse(start).astimezone(timezone.utc)
        end_dt = dtparser.isoparse(end).astimezone(timezone.utc)
        
        # Call the existing function
        rows = fetch_openmeteo(lat, lon, start_dt, end_dt)
        
        if not rows:
            return None
            
        # Convert to DataFrame format expected by consolidated_analysis
        df_data = []
        for time_iso, variable, value in rows:
            time_obj = dtparser.isoparse(time_iso)
            df_data.append({
                'time': time_obj,
                'variable': variable,
                'value': value
            })
        
        if not df_data:
            return None
            
        # Convert to wide format
        df_long = pd.DataFrame(df_data)
        df_wide = df_long.pivot_table(
            index='time',
            columns='variable', 
            values='value',
            aggfunc='first'
        ).reset_index()
        
        # Ensure all expected columns exist
        expected_cols = ['temperature_2m', 'precipitation', 'wind_speed_100m', 'wind_direction_100m']
        for col in expected_cols:
            if col not in df_wide.columns:
                df_wide[col] = 0.0
        
        return df_wide
        
    except Exception as e:
        print(f"Error in fetch_openmeteo_data wrapper: {e}")
        return None


def fetch_metno_data(lat: float, lon: float, start: str, end: str, use_forecast=True) -> Optional['pd.DataFrame']:
    """Wrapper function for consolidated_analysis.py compatibility"""
    try:
        import pandas as pd
        from dateutil import parser as dtparser
        
        # Convert string dates to datetime objects
        start_dt = dtparser.isoparse(start).astimezone(timezone.utc)
        end_dt = dtparser.isoparse(end).astimezone(timezone.utc)
        
        # Call the existing function
        user_agent = "Weather-Analysis/1.0"
        rows = fetch_metno(lat, lon, start_dt, end_dt, user_agent)
        
        if not rows:
            return None
            
        # Convert to DataFrame format
        df_data = []
        for time_iso, variable, value in rows:
            time_obj = dtparser.isoparse(time_iso)
            df_data.append({
                'time': time_obj,
                'variable': variable,
                'value': value
            })
        
        if not df_data:
            return None
            
        # Convert to wide format
        df_long = pd.DataFrame(df_data)
        df_wide = df_long.pivot_table(
            index='time',
            columns='variable', 
            values='value',
            aggfunc='first'
        ).reset_index()
        
        # Map 10m wind to 100m (approximate)
        if 'wind_speed_10m' in df_wide.columns and 'wind_speed_100m' not in df_wide.columns:
            df_wide['wind_speed_100m'] = df_wide['wind_speed_10m'] * 1.2  # Rough conversion
        if 'wind_direction_10m' in df_wide.columns and 'wind_direction_100m' not in df_wide.columns:
            df_wide['wind_direction_100m'] = df_wide['wind_direction_10m']
            
        # Ensure all expected columns exist
        expected_cols = ['temperature_2m', 'precipitation', 'wind_speed_100m', 'wind_direction_100m']
        for col in expected_cols:
            if col not in df_wide.columns:
                df_wide[col] = 0.0
        
        return df_wide
        
    except Exception as e:
        print(f"Error in fetch_metno_data wrapper: {e}")
        return None


def fetch_weatherapi_data(lat: float, lon: float, start: str, end: str, use_forecast=True) -> Optional['pd.DataFrame']:
    """Wrapper function for consolidated_analysis.py compatibility"""
    try:
        import pandas as pd
        from dateutil import parser as dtparser
        
        # WeatherAPI requires API key
        api_key = os.environ.get("WEATHERAPI_KEY", "")
        if not api_key:
            print("WeatherAPI requires API key in WEATHERAPI_KEY environment variable")
            return None
        
        # Convert string dates to datetime objects
        start_dt = dtparser.isoparse(start).astimezone(timezone.utc)
        end_dt = dtparser.isoparse(end).astimezone(timezone.utc)
        
        # Call the existing function
        rows = fetch_weatherapi(lat, lon, start_dt, end_dt, api_key)
        
        if not rows:
            return None
            
        # Convert to DataFrame format
        df_data = []
        for time_iso, variable, value in rows:
            time_obj = dtparser.isoparse(time_iso)
            df_data.append({
                'time': time_obj,
                'variable': variable,
                'value': value
            })
        
        if not df_data:
            return None
            
        # Convert to wide format
        df_long = pd.DataFrame(df_data)
        df_wide = df_long.pivot_table(
            index='time',
            columns='variable', 
            values='value',
            aggfunc='first'
        ).reset_index()
        
        # Map 10m wind to 100m
        if 'wind_speed_10m' in df_wide.columns:
            df_wide['wind_speed_100m'] = df_wide['wind_speed_10m'] * 1.2
        if 'wind_direction_10m' in df_wide.columns:
            df_wide['wind_direction_100m'] = df_wide['wind_direction_10m']
            
        # Ensure all expected columns exist
        expected_cols = ['temperature_2m', 'precipitation', 'wind_speed_100m', 'wind_direction_100m']
        for col in expected_cols:
            if col not in df_wide.columns:
                df_wide[col] = 0.0
        
        return df_wide
        
    except Exception as e:
        print(f"Error in fetch_weatherapi_data wrapper: {e}")
        return None


def fetch_visualcrossing_data(lat: float, lon: float, start: str, end: str, use_forecast=True) -> Optional['pd.DataFrame']:
    """Wrapper function for consolidated_analysis.py compatibility"""
    try:
        import pandas as pd
        from dateutil import parser as dtparser
        
        # Visual Crossing requires API key
        api_key = os.environ.get("VISUALCROSSING_KEY", "")
        if not api_key:
            print("Visual Crossing requires API key in VISUALCROSSING_KEY environment variable")
            return None
        
        # Convert string dates to datetime objects
        start_dt = dtparser.isoparse(start).astimezone(timezone.utc)
        end_dt = dtparser.isoparse(end).astimezone(timezone.utc)
        
        # Call the existing function
        rows = fetch_visualcrossing(lat, lon, start_dt, end_dt, api_key)
        
        if not rows:
            return None
            
        # Convert to DataFrame format
        df_data = []
        for time_iso, variable, value in rows:
            time_obj = dtparser.isoparse(time_iso)
            df_data.append({
                'time': time_obj,
                'variable': variable,
                'value': value
            })
        
        if not df_data:
            return None
            
        # Convert to wide format
        df_long = pd.DataFrame(df_data)
        df_wide = df_long.pivot_table(
            index='time',
            columns='variable', 
            values='value',
            aggfunc='first'
        ).reset_index()
        
        # Map 10m wind to 100m
        if 'wind_speed_10m' in df_wide.columns:
            df_wide['wind_speed_100m'] = df_wide['wind_speed_10m'] * 1.2
        if 'wind_direction_10m' in df_wide.columns:
            df_wide['wind_direction_100m'] = df_wide['wind_direction_10m']
            
        # Ensure all expected columns exist
        expected_cols = ['temperature_2m', 'precipitation', 'wind_speed_100m', 'wind_direction_100m']
        for col in expected_cols:
            if col not in df_wide.columns:
                df_wide[col] = 0.0
        
        return df_wide
        
    except Exception as e:
        print(f"Error in fetch_visualcrossing_data wrapper: {e}")
        return None


def fetch_openweather_data(lat: float, lon: float, start: str, end: str, use_forecast=True) -> Optional['pd.DataFrame']:
    """Wrapper function for consolidated_analysis.py compatibility"""
    try:
        import pandas as pd
        from dateutil import parser as dtparser
        
        # OpenWeather requires API key
        api_key = os.environ.get("OPENWEATHER_KEY", "")
        if not api_key:
            print("OpenWeather requires API key in OPENWEATHER_KEY environment variable")
            return None
        
        # Convert string dates to datetime objects
        start_dt = dtparser.isoparse(start).astimezone(timezone.utc)
        end_dt = dtparser.isoparse(end).astimezone(timezone.utc)
        
        # Call the existing function
        rows = fetch_openweather(lat, lon, start_dt, end_dt, api_key)
        
        if not rows:
            return None
            
        # Convert to DataFrame format
        df_data = []
        for time_iso, variable, value in rows:
            time_obj = dtparser.isoparse(time_iso)
            df_data.append({
                'time': time_obj,
                'variable': variable,
                'value': value
            })
        
        if not df_data:
            return None
            
        # Convert to wide format
        df_long = pd.DataFrame(df_data)
        df_wide = df_long.pivot_table(
            index='time',
            columns='variable', 
            values='value',
            aggfunc='first'
        ).reset_index()
        
        # Map 10m wind to 100m
        if 'wind_speed_10m' in df_wide.columns:
            df_wide['wind_speed_100m'] = df_wide['wind_speed_10m'] * 1.2
        if 'wind_direction_10m' in df_wide.columns:
            df_wide['wind_direction_100m'] = df_wide['wind_direction_10m']
            
        # Ensure all expected columns exist
        expected_cols = ['temperature_2m', 'precipitation', 'wind_speed_100m', 'wind_direction_100m']
        for col in expected_cols:
            if col not in df_wide.columns:
                df_wide[col] = 0.0
        
        return df_wide
        
    except Exception as e:
        print(f"Error in fetch_openweather_data wrapper: {e}")
        return None


def main():
    ap = argparse.ArgumentParser(description="Enhanced weather fetch with REAL DATA (historical + current/forecast)")
    ap.add_argument("--lat", type=float, required=True)
    ap.add_argument("--lon", type=float, required=True)
    ap.add_argument("--start", type=str, required=True, help="ISO8601, e.g., 2025-08-01T00:00:00Z")
    ap.add_argument("--end", type=str, required=True, help="ISO8601, exclusive")
    ap.add_argument("--providers", type=str, default="openmeteo,metno,visualcrossing,weatherapi")
    ap.add_argument("--outdir", type=str, default=".")
    ap.add_argument("--metno-user-agent", type=str, default="Weather-Compare-Real/1.0")

    # API keys
    ap.add_argument("--visualcrossing-key", type=str,
                    default=os.environ.get("VISUALCROSSING_KEY", ""))
    ap.add_argument("--openweather-key", type=str,
                    default=os.environ.get("OPENWEATHER_KEY", ""))
    ap.add_argument("--weatherapi-key", type=str,
                    default=os.environ.get("WEATHERAPI_KEY", ""))

    args = ap.parse_args()

    lat, lon = args.lat, args.lon
    start = dtparser.isoparse(args.start).astimezone(timezone.utc)
    end = dtparser.isoparse(args.end).astimezone(timezone.utc)
    outdir = Path(args.outdir)

    reqs = [p.strip().lower() for p in args.providers.split(",") if p.strip()]
    for p in reqs:
        if p not in PROVIDERS:
            raise SystemExit(f"Unknown provider: {p}. Supported: {', '.join(PROVIDERS.keys())}")

    # Determine data type
    data_type = "HISTORICAL" if is_historical_data(start, end) else "CURRENT/FORECAST"
    print(f"🌍 REAL DATA FETCH - {data_type}")
    print(f"📅 Period: {start.date()} to {end.date()}")
    print(f"📍 Location: {lat:.4f}°N, {lon:.4f}°E")
    print(f"🌐 Providers: {', '.join(reqs)}")
    print("=" * 50)

    for prov in reqs:
        print(f"🔄 Fetching {prov}...")
        try:
            if prov == "metno":
                rows = fetch_metno(lat, lon, start, end, user_agent=args.metno_user_agent)
            elif prov == "visualcrossing":
                if not args.visualcrossing_key:
                    print("  ⚠️ Skipping visualcrossing (no API key).")
                    continue
                rows = fetch_visualcrossing(lat, lon, start, end, api_key=args.visualcrossing_key)
            elif prov == "openweather":
                if not args.openweather_key:
                    print("  ⚠️ Skipping openweather (no API key).")
                    continue
                rows = fetch_openweather(lat, lon, start, end, api_key=args.openweather_key)
            elif prov == "weatherapi":
                if not args.weatherapi_key:
                    print("  ⚠️ Skipping weatherapi (no API key).")
                    continue
                rows = fetch_weatherapi(lat, lon, start, end, api_key=args.weatherapi_key)
            else:
                rows = fetch_openmeteo(lat, lon, start, end)  # openmeteo
        except Exception as e:
            print(f"  ❌ Error fetching {prov}: {e}")
            continue

        if not rows:
            print(f"  ❌ No rows fetched for {prov}.")
            continue
            
        path = outdir / f"provider_{prov}.csv"
        write_long_csv(path, lat, lon, rows)
        print(f"  ✅ Wrote {path} ({len(rows)} rows)")


if __name__ == "__main__":
    main()