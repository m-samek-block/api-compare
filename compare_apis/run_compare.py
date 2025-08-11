
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Runner: fetch forecasts from free APIs and compare to ERA5.

Usage examples:
  # Keyless only (Open-Meteo + MET Norway), Warsaw, last 48h window
  python run_compare.py --lat 52.2297 --lon 21.0122 \
    --start 2025-08-10T00:00:00Z --end 2025-08-12T00:00:00Z \
    --providers openmeteo,metno \
    --era5 /path/to/era5.csv \
    --outdir ./outputs

  # With API keys (export env vars or pass flags; see --help)
  export VISUALCROSSING_KEY=...
  export OPENWEATHER_KEY=...
  export WEATHERAPI_KEY=...
  python run_compare.py --lat 52.2297 --lon 21.0122 \
    --start 2025-08-10T00:00:00Z --end 2025-08-12T00:00:00Z \
    --providers openmeteo,metno,visualcrossing,openweather,weatherapi \
    --era5 /path/to/era5.csv --outdir ./outputs
"""
import argparse
import importlib.util
import os
from pathlib import Path
from datetime import datetime, timezone
from dateutil import parser as dtparser
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent

# --- Load fetch_forecasts.py dynamically ---
fetch_path = HERE / "fetch_forecasts.py"
if not fetch_path.exists():
    raise SystemExit("Missing fetch_forecasts.py next to this script.")

spec = importlib.util.spec_from_file_location("fetch_mod", str(fetch_path))
fetch_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch_mod)

def load_long_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    req = ["time","latitude","longitude","variable","value"]
    for c in req:
        if c not in df.columns:
            raise ValueError(f"{path} missing required column {c}")
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["time","latitude","longitude","variable","value"])
    return df

def mae(x): return np.mean(np.abs(x)) if len(x)>0 else np.nan
def rmse(x): return np.sqrt(np.mean(np.square(x))) if len(x)>0 else np.nan

def align_on_keys(df_gt: pd.DataFrame, df_pred: pd.DataFrame) -> pd.DataFrame:
    on = ["time","latitude","longitude","variable"]
    merged = df_pred.merge(df_gt.rename(columns={"value":"era5_value"}), on=on, how="inner")
    merged = merged.rename(columns={"value":"pred_value"})
    merged["error"] = merged["pred_value"] - merged["era5_value"]
    return merged

def summarize_errors(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["provider","variable","n","bias_mean","mae","rmse","corr","over_pct","under_pct"])
    g = df.groupby(["provider","variable"])
    out = g.agg(
        n=("error","size"),
        bias_mean=("error","mean"),
        mae=("error", mae),
        rmse=("error", rmse),
        corr=("pred_value", lambda s: np.corrcoef(s, df.loc[s.index, "era5_value"])[0,1] if len(s)>2 else np.nan),
        over_pct=("error", lambda s: 100*np.mean(s>0)),
        under_pct=("error", lambda s: 100*np.mean(s<0)),
    ).reset_index()
    return out

def safe_savefig(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(path, dpi=140, bbox_inches="tight")
    plt.close()

def run():
    ap = argparse.ArgumentParser(description="Fetch forecasts (free APIs) and compare to ERA5.")
    ap.add_argument("--lat", type=float, required=True)
    ap.add_argument("--lon", type=float, required=True)
    ap.add_argument("--start", type=str, required=True)
    ap.add_argument("--end", type=str, required=True)
    ap.add_argument("--providers", type=str, default="openmeteo,metno")
    ap.add_argument("--era5", type=str, required=True, help="Path to ERA5 CSV (long format)")
    ap.add_argument("--outdir", type=str, default=str(HERE / "outputs"))
    ap.add_argument("--metno-user-agent", type=str, default="Weather-Compare/1.0 (contact: you@example.com)")
    ap.add_argument("--visualcrossing-key", type=str,
                    default=os.environ.get("VISUALCROSSING_KEY", "X5TXF44RU47MDP2HHYS52P7KB"))
    ap.add_argument("--openweather-key", type=str,
                    default=os.environ.get("OPENWEATHER_KEY", "54f216d976bdf228f5a89444ca5d1502"))
    ap.add_argument("--weatherapi-key", type=str,
                    default=os.environ.get("WEATHERAPI_KEY", "564b50d8beb04a26986131157251108"))

    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    lat, lon = args.lat, args.lon
    start = dtparser.isoparse(args.start).astimezone(timezone.utc)
    end = dtparser.isoparse(args.end).astimezone(timezone.utc)

    reqs = [p.strip().lower() for p in args.providers.split(",") if p.strip()]
    available = set(fetch_mod.PROVIDERS.keys())
    for p in reqs:
        if p not in available:
            raise SystemExit(f"Unknown provider {p}. Supported: {sorted(list(available))}")

    # --- Fetch ---
    for prov in reqs:
        print(f"[Fetch] {prov}")
        try:
            if prov == "metno":
                rows = fetch_mod.fetch_metno(lat, lon, start, end, user_agent=args.metno_user_agent)
            elif prov == "visualcrossing":
                if not args.visualcrossing_key:
                    print("  Skipping visualcrossing (no API key)."); continue
                rows = fetch_mod.fetch_visualcrossing(lat, lon, start, end, api_key=args.visualcrossing_key)
            elif prov == "openweather":
                if not args.openweather_key:
                    print("  Skipping openweather (no API key)."); continue
                rows = fetch_mod.fetch_openweather(lat, lon, start, end, api_key=args.openweather_key)
            elif prov == "weatherapi":
                if not args.weatherapi_key:
                    print("  Skipping weatherapi (no API key)."); continue
                rows = fetch_mod.fetch_weatherapi(lat, lon, start, end, api_key=args.weatherapi_key)
            else:
                rows = fetch_mod.fetch_openmeteo(lat, lon, start, end)
        except Exception as e:
            print(f"  Error fetching {prov}: {e}")
            continue

        if not rows:
            print(f"  No rows for {prov}.")
            continue
        fetch_mod.write_long_csv(outdir / f"provider_{prov}.csv", lat, lon, rows)
        print(f"  -> wrote {outdir / f'provider_{prov}.csv'} ({len(rows)} rows)")

    # --- Compare ---
    era5 = load_long_csv(Path(args.era5))

    # collect provider files written in this run
    provider_files = list(outdir.glob("provider_*.csv"))
    if not provider_files:
        print("No provider CSVs found. Exiting.")
        return

    def load_provider(path: Path) -> pd.DataFrame:
        df = load_long_csv(path)
        df["provider"] = path.stem.replace("provider_","")
        return df

    providers = pd.concat([load_provider(p) for p in provider_files], ignore_index=True)

    # Only compare variables available in both ERA5 and provider files
    common_vars = sorted(set(era5["variable"].unique()) & set(providers["variable"].unique()))
    if not common_vars:
        print("No common variables between ERA5 and providers.")
        return

    # Filter to common vars only
    era5c = era5[era5["variable"].isin(common_vars)].copy()
    provc = providers[providers["variable"].isin(common_vars)].copy()

    # Align on (time,lat,lon,variable)
    merged = provc.merge(
        era5c.rename(columns={"value":"era5_value"}),
        on=["time","latitude","longitude","variable"],
        how="inner"
    ).rename(columns={"value":"pred_value"})
    if merged.empty:
        print("No overlapping (time,lat,lon,variable) between ERA5 and providers.")
        return
    merged["error"] = merged["pred_value"] - merged["era5_value"]

    # Summary
    def _corr(group):
        if len(group) < 3: return np.nan
        return np.corrcoef(group["pred_value"], group["era5_value"])[0,1]

    summary = merged.groupby(["provider","variable"]).agg(
        n=("error","size"),
        bias_mean=("error","mean"),
        mae=("error", lambda s: np.mean(np.abs(s))),
        rmse=("error", lambda s: np.sqrt(np.mean(np.square(s)))),
        corr=("error", lambda s: _corr(merged.loc[s.index])),
        over_pct=("error", lambda s: 100*np.mean(s>0)),
        under_pct=("error", lambda s: 100*np.mean(s<0)),
    ).reset_index()

    summary_path = outdir / "era5_comparison_summary.csv"
    summary.to_csv(summary_path, index=False)
    print(f"[OK] Wrote summary: {summary_path}")

    # Plots
    plots_dir = outdir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    # Error histograms
    for (prov, var), df in merged.groupby(["provider","variable"]):
        if df.empty: continue
        import matplotlib.pyplot as plt
        plt.figure()
        plt.hist(df["error"].dropna(), bins=30)
        plt.xlabel("Błąd (pred - ERA5)")
        plt.ylabel("Liczba obserwacji")
        plt.title(f"Histogram błędu: {prov} vs ERA5 — {var}")
        plt.tight_layout(); plt.savefig(plots_dir / f"error_hist_{prov}_{var}.png", dpi=140); plt.close()

    # Scatter pred vs ERA5
    for (prov, var), df in merged.groupby(["provider","variable"]):
        if df.empty: continue
        plt.figure()
        plt.scatter(df["era5_value"], df["pred_value"], s=6, alpha=0.6)
        mn = float(np.nanmin([df["era5_value"].min(), df["pred_value"].min()]))
        mx = float(np.nanmax([df["era5_value"].max(), df["pred_value"].max()]))
        plt.plot([mn, mx], [mn, mx], linewidth=1)
        plt.xlabel("ERA5"); plt.ylabel(f"{prov}")
        plt.title(f"Predykcja vs ERA5 — {var}")
        plt.tight_layout(); plt.savefig(plots_dir / f"scatter_{prov}_{var}.png", dpi=140); plt.close()

    # Bias by hour
    merged["hour"] = pd.to_datetime(merged["time"]).dt.hour
    for (prov, var), df in merged.groupby(["provider","variable"]):
        if df.empty: continue
        by_hour = df.groupby("hour")["error"].mean()
        plt.figure()
        plt.plot(by_hour.index, by_hour.values, marker="o")
        plt.axhline(0, linewidth=1)
        plt.xlabel("Godzina (UTC)"); plt.ylabel("Średni błąd")
        plt.title(f"Bias dobowy: {prov} — {var}")
        plt.tight_layout(); plt.savefig(plots_dir / f"bias_by_hour_{prov}_{var}.png", dpi=140); plt.close()

    # Boxplot per variable
    for var, dfv in merged.groupby("variable"):
        plt.figure()
        labels = sorted(dfv["provider"].unique())
        data = [dfv.loc[dfv["provider"]==p, "error"].dropna().values for p in labels]
        plt.boxplot(data, labels=labels, showmeans=True)
        plt.axhline(0, linewidth=1)
        plt.ylabel("Błąd (pred - ERA5)")
        plt.title(f"Rozkład błędów — {var}")
        plt.tight_layout(); plt.savefig(plots_dir / f"boxplot_errors_{var}.png", dpi=140); plt.close()

    print(f"[OK] Plots in: {plots_dir}")

if __name__ == "__main__":
    run()
