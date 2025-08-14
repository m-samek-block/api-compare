#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# make_era5_cds.py — pobiera prawdziwe ERA5 (single levels) z CDS i zapisuje long CSV dla 1 punktu
# Wyjście: time,latitude,longitude,variable,value
# Jednostki (spójne z providerami): temperature_2m [°C], precipitation [mm/h], wind_speed_100m [m/s], wind_direction_100m [°]

import argparse
import tempfile
import gzip
import zipfile
import gc
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import cdsapi
import numpy as np
import pandas as pd
import xarray as xr

ISO = "%Y-%m-%dT%H:%M:%SZ"


def to_utc_floor_hour(s: str) -> datetime:
    dt = datetime.fromisoformat(s.replace("Z", "+00:00")) if s.endswith("Z") else datetime.fromisoformat(s)
    dt = dt.astimezone(timezone.utc)
    return dt.replace(minute=0, second=0, microsecond=0)


def detect_and_prepare_nc(path: Path) -> Path:
    if not path.exists() or path.stat().st_size == 0:
        raise SystemExit("Pobrany plik ERA5 jest pusty lub nie istnieje.")
    head = path.read_bytes()[:16]
    is_zip = head.startswith(b"PK\x03\x04")
    is_gz = head.startswith(b"\x1f\x8b")
    is_grib = head.startswith(b"GRIB")
    is_cdf = head.startswith(b"CDF") or head[1:4] == b"CDF"
    is_hdf5 = head.startswith(b"\x89HDF")

    if is_zip:
        with zipfile.ZipFile(path, "r") as z:
            names = [n for n in z.namelist() if not n.endswith("/")]
            if not names:
                raise SystemExit("ZIP z CDS jest pusty.")
            data = z.read(names[0])
        out_nc = path.with_suffix(".unzipped.nc")
        out_nc.write_bytes(data)
        return out_nc

    if is_gz:
        with gzip.open(path, "rb") as gz:
            data = gz.read()
        out_nc = path.with_suffix(".ungz.nc")
        out_nc.write_bytes(data)
        return out_nc

    if is_grib:
        # ten skrypt obsłuży GRIB tylko dla fallbacku tp; główny plik oczekujemy jako netcdf
        return path  # pozwólmy polec na netcdf4 i wtedy damy czytelny komunikat

    if is_cdf or is_hdf5:
        return path

    return path


def open_nc_dataset(nc_path: Path):
    try:
        return xr.open_dataset(nc_path, engine="netcdf4")
    except Exception:
        return xr.open_dataset(nc_path, engine="h5netcdf")


def normalize_time_coord(ds: xr.Dataset) -> xr.Dataset:
    if ("time" in ds.coords) or ("time" in ds.dims):
        return ds
    for cand in ("valid_time", "verification_time", "forecast_time"):
        if (cand in ds.coords) or (cand in ds.dims):
            return ds.rename({cand: "time"})
    if "valid_time" in ds:
        ds = ds.assign_coords(time=ds["valid_time"]).swap_dims({"valid_time": "time"}).drop_vars("valid_time")
        return ds
    raise SystemExit(f"Brak osi czasu w Dataset: coords={list(ds.coords)} dims={list(ds.dims)}")


def safe_to_float(value):
    """Safely convert a value to float, handling Series objects."""
    if isinstance(value, pd.Series):
        if len(value) == 1:
            return float(value.iloc[0])
        else:
            # If multiple values, take the first one or handle as needed
            print(f"[WARN] Multiple values in Series: {len(value)} values, taking first one")
            return float(value.iloc[0])
    elif hasattr(value, 'item'):  # numpy scalar
        return float(value.item())
    else:
        return float(value)


def retrieve_tp_grib_series(c: cdsapi.Client, lat: float, lon: float, startN: pd.Timestamp,
                            endN: pd.Timestamp) -> pd.Series:
    """Fallback: pobiera tylko total_precipitation jako GRIB i zwraca godzinowy mm/h w postaci pandas.Series (index=time)."""
    with tempfile.TemporaryDirectory() as td2:
        grib_path = Path(td2) / "tp.grib"
        req_tp = {
            "product_type": "reanalysis",
            "format": "grib",
            "variable": ["total_precipitation"],
            "year": [str(y) for y in range(startN.year, endN.year + 1)],
            "month": [f"{m:02d}" for m in sorted(set(pd.date_range(startN, endN, freq="MS").month))],
            "day": [f"{d:02d}" for d in sorted(set(pd.date_range(startN, endN, freq="D").day))],
            "time": [f"{h:02d}:00" for h in range(24)],
            "area": [lat + 0.25, lon - 0.25, lat - 0.25, lon + 0.25],
        }
        c.retrieve("reanalysis-era5-single-levels", req_tp, str(grib_path))
        # wymaga: pip install cfgrib eccodes
        ds_tp = xr.open_dataset(grib_path, engine="cfgrib")
        try:
            ds_tp = normalize_time_coord(ds_tp)
            ds_tp = ds_tp.sel(time=slice(startN, endN))
            dsp_tp = ds_tp.sel(latitude=lat, longitude=lon, method="nearest").load()
            if "tp" not in dsp_tp:
                raise SystemExit(f"Fallback GRIB nie zawiera 'tp'. Vars: {list(dsp_tp.data_vars)}")
            tp_acc = dsp_tp["tp"].to_series()  # m (akumulacja)
            tp_mph = tp_acc.diff().fillna(0.0).clip(lower=0.0) * 1000.0  # -> mm/h
            return tp_mph
        finally:
            try:
                ds_tp.close()
            except Exception:
                pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lat", type=float, required=True)
    ap.add_argument("--lon", type=float, required=True)
    ap.add_argument("--start", type=str, required=True)  # ISO UTC (włącznie)
    ap.add_argument("--end", type=str, required=True)  # ISO UTC (WYŁĄCZNIE)
    ap.add_argument("--out", type=str, required=True)
    args = ap.parse_args()

    lat, lon = float(args.lat), float(args.lon)
    start = to_utc_floor_hour(args.start)
    end = to_utc_floor_hour(args.end)
    if end <= start:
        raise SystemExit("Błąd: --end musi być po --start")

    # xarray zwykle ma oś czasu tz-naive → przygotujmy takie znaczniki
    startN = pd.Timestamp(start.replace(tzinfo=None))
    endN = pd.Timestamp(end.replace(tzinfo=None))

    c = cdsapi.Client()

    # --- Pobranie zbiorcze (netCDF) ---
    with tempfile.TemporaryDirectory() as td:
        raw_path = Path(td) / "era5.download"
        req = {
            "product_type": "reanalysis",
            "format": "netcdf",
            "variable": [
                "2m_temperature",
                "total_precipitation",
                "100m_u_component_of_wind",
                "100m_v_component_of_wind",
            ],
            "year": [str(y) for y in range(startN.year, endN.year + 1)],
            "month": [f"{m:02d}" for m in sorted(set(pd.date_range(startN, endN, freq="MS").month))],
            "day": [f"{d:02d}" for d in sorted(set(pd.date_range(startN, endN, freq="D").day))],
            "time": [f"{h:02d}:00" for h in range(24)],
            "area": [lat + 0.25, lon - 0.25, lat - 0.25, lon + 0.25],
        }
        c.retrieve("reanalysis-era5-single-levels", req, str(raw_path))

        nc_path = detect_and_prepare_nc(raw_path)
        ds = open_nc_dataset(nc_path)

        try:
            ds = normalize_time_coord(ds)
            ds = ds.sel(time=slice(startN, endN))
            dsp = ds.sel(latitude=lat, longitude=lon, method="nearest").load()

            # Podstawowe zmienne
            try:
                t2m = dsp["t2m"].to_series()  # K
                u100 = dsp["u100"].to_series()  # m/s
                v100 = dsp["v100"].to_series()  # m/s
            except KeyError as e:
                raise SystemExit(f"Brak zmiennej w NetCDF: {e}. Dostępne: {list(dsp.data_vars)}")

            # Opad (netCDF) – może być brak
            tp_mm: Optional[pd.Series] = None
            if "tp" in dsp:
                tp_acc = dsp["tp"].to_series()  # m (akumulacja)
                tp_mm = tp_acc.diff().fillna(0.0).clip(lower=0.0) * 1000.0  # mm/h
            else:
                print("[WARN] netCDF nie zawiera 'tp' — pobieram fallback GRIB dla total_precipitation…")
                tp_mm = retrieve_tp_grib_series(c, lat, lon, startN, endN)

        finally:
            try:
                ds.close()
            except Exception:
                pass
            del ds, dsp
            gc.collect()

    # Konwersje do jednostek providerów:
    T_C = t2m - 273.15  # °C
    P_mm = tp_mm  # mm/h
    wspd = np.sqrt(u100 ** 2 + v100 ** 2)  # m/s
    wdir = (np.degrees(np.arctan2(-u100, -v100)) % 360.0)  # °

    # Zrzut do long CSV
    rows = []
    for t in T_C.index:
        iso = pd.Timestamp(t).tz_localize("UTC").strftime(ISO)

        # Use safe_to_float to handle potential Series conversion issues
        temp_val = safe_to_float(T_C.loc[t])
        precip_val = safe_to_float(P_mm.loc[t]) if t in P_mm.index else 0.0
        wspd_val = safe_to_float(wspd.loc[t])
        wdir_val = safe_to_float(wdir.loc[t])

        rows.append([iso, lat, lon, "temperature_2m", temp_val])
        rows.append([iso, lat, lon, "precipitation", precip_val])
        rows.append([iso, lat, lon, "wind_speed_100m", wspd_val])
        rows.append([iso, lat, lon, "wind_direction_100m", wdir_val])

    df = pd.DataFrame(rows, columns=["time", "latitude", "longitude", "variable", "value"])

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"OK: ERA5 zapisane do {out}")


if __name__ == "__main__":
    main()