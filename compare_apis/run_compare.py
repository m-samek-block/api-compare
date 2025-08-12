#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_compare.py — pobierz prognozy z wybranych darmowych API i porównaj do ERA5.

Przykład (PowerShell):
  python .\run_compare.py `
    --lat 52.2297 --lon 21.0122 `
    --start 2025-08-11T00:00:00Z --end 2025-08-13T00:00:00Z `
    --providers openmeteo,metno,openweather,weatherapi,visualcrossing `
    --openweather-key "$env:OPENWEATHER_KEY" `
    --weatherapi-key "$env:WEATHERAPI_KEY" `
    --visualcrossing-key "$env:VISUALCROSSING_KEY" `
    --era5 .\dane\era5.csv `
    --outdir .\wyniki `
    --wind-alpha 0.143
"""

import argparse
import importlib.util
import os
from pathlib import Path
from datetime import timezone
from dateutil import parser as dtparser
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# ================== KONFIG ================== #

TARGET_VARS = [
    "precipitation",
    "temperature_2m",
    "wind_speed_100m",
    "wind_direction_100m",
]

HERE = Path(__file__).resolve().parent

# ================ HELPERS =================== #

def load_long_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    req = ["time","latitude","longitude","variable","value"]
    for c in req:
        if c not in df.columns:
            raise ValueError(f"{path} missing required column {c}")
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df["latitude"]  = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["value"]     = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["time","latitude","longitude","variable","value"])
    return df

def safe_savefig(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(path, dpi=140, bbox_inches="tight")
    plt.close()

def ensure_target_vars(prov_df: pd.DataFrame, alpha: float = 0.143) -> pd.DataFrame:
    """
    Uzupełnij brakujące 100 m z 10 m w ramach TEGO SAMEGO providera:
      - U100 = U10 * (100/10)^alpha
      - dir100 = dir10
    Nic nie pożyczamy z innych providerów (żeby nie rozmywać biasu).
    """
    df = prov_df.copy()
    keys = ["time","latitude","longitude","provider","variable"]

    # --- prędkość 100m z 10m ---
    if "wind_speed_10m" in df["variable"].unique():
        base = df.loc[df["variable"]=="wind_speed_10m", ["time","latitude","longitude","provider","value"]]
        # wylicz 100m
        add = base.copy()
        add["value"] = add["value"] * ((100.0/10.0) ** alpha)
        add["variable"] = "wind_speed_100m"
        # dodaj tylko tam, gdzie oryginalnego 100m brak
        have = df[df["variable"]=="wind_speed_100m"][keys]
        add = add.merge(have.assign(_has=1), on=keys, how="left")
        add = add[add["_has"].isna()].drop(columns=["_has"])
        if not add.empty:
            add["__derived__"] = "wind_speed_100m"
            df = pd.concat([df, add], ignore_index=True)

    # --- kierunek 100m z 10m ---
    if "wind_direction_10m" in df["variable"].unique():
        base = df.loc[df["variable"]=="wind_direction_10m", ["time","latitude","longitude","provider","value"]]
        add  = base.copy()
        add["variable"] = "wind_direction_100m"
        have = df[df["variable"]=="wind_direction_100m"][keys]
        add = add.merge(have.assign(_has=1), on=keys, how="left")
        add = add[add["_has"].isna()].drop(columns=["_has"])
        if not add.empty:
            add["__derived__"] = "wind_direction_100m"
            df = pd.concat([df, add], ignore_index=True)

    return df

def count_derived(df: pd.DataFrame) -> pd.DataFrame:
    """Zlicz ile wartości dorobiliśmy (np. 10m→100m) per provider × variable."""
    if "__derived__" not in df.columns:
        return pd.DataFrame(columns=["provider","variable","derived_n"])
    tmp = df.loc[df["__derived__"].notna(), ["provider","variable"]]
    if tmp.empty:
        return pd.DataFrame(columns=["provider","variable","derived_n"])
    return (tmp.assign(one=1)
              .groupby(["provider","variable"])["one"].sum()
              .rename("derived_n").reset_index())

def rmse_np(a, b) -> float:
    a = np.asarray(a, dtype=float); b = np.asarray(b, dtype=float)
    return float(np.sqrt(np.mean((a - b) ** 2))) if len(a) else np.nan

# ================== MAIN ==================== #

def run():
    ap = argparse.ArgumentParser(description="Fetch forecasts (free APIs) and compare to ERA5.")
    ap.add_argument("--lat", type=float, required=True)
    ap.add_argument("--lon", type=float, required=True)
    ap.add_argument("--start", type=str, required=True)
    ap.add_argument("--end", type=str, required=True)
    ap.add_argument("--providers", type=str, default="openmeteo,metno")
    ap.add_argument("--era5", type=str, required=True, help="Path to ERA5 CSV (long format)")
    ap.add_argument("--outdir", type=str, default=str(HERE / "outputs"))

    ap.add_argument("--metno-user-agent", type=str,
                    default="Weather-Compare/1.0 (contact: you@example.com)")
    # Klucze API — z env lub parametru (nie trzymamy w kodzie)
    ap.add_argument("--visualcrossing-key", type=str,
                    default=os.environ.get("VISUALCROSSING_KEY", ""))
    ap.add_argument("--openweather-key", type=str,
                    default=os.environ.get("OPENWEATHER_KEY", ""))
    ap.add_argument("--weatherapi-key", type=str,
                    default=os.environ.get("WEATHERAPI_KEY", ""))

    # Parametr wzoru potęgowego do 10m→100m
    ap.add_argument("--wind-alpha", type=float, default=0.143,
                    help="Parametr α w U100 = U10*(100/10)^α (domyślnie 0.143).")
    ap.add_argument("--precip-thresh", type=float, default=0.1,
                    help="Próg zdarzenia opad (mm/h) do metryk detekcji (POD/FAR/CSI).")

    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    lat, lon = args.lat, args.lon
    start = dtparser.isoparse(args.start).astimezone(timezone.utc)
    end   = dtparser.isoparse(args.end).astimezone(timezone.utc)

    # ---- dynamiczne wczytanie fetch_forecasts.py ---- #
    fetch_path = Path(__file__).with_name("fetch_forecasts.py")
    if not fetch_path.exists():
        raise SystemExit("Missing fetch_forecasts.py next to this script.")
    spec = importlib.util.spec_from_file_location("fetch_mod", str(fetch_path))
    fetch_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fetch_mod)

    reqs = [p.strip().lower() for p in args.providers.split(",") if p.strip()]
    available = set(fetch_mod.PROVIDERS.keys())
    for p in reqs:
        if p not in available:
            raise SystemExit(f"Unknown provider {p}. Supported: {sorted(list(available))}")

    # ---------------- FETCH ---------------- #
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

    # --------------- COMPARE ---------------- #
    era5 = load_long_csv(Path(args.era5))

    # wczytaj WSZYSTKIE provider_*.csv zapisane w tym przebiegu (w outdir)
    provider_files = list(outdir.glob("provider_*.csv"))
    if not provider_files:
        print("No provider CSVs found. Exiting.")
        return

    def load_provider(path: Path) -> pd.DataFrame:
        df = load_long_csv(path)
        df["provider"] = path.stem.replace("provider_","")
        return df

    providers_df = pd.concat([load_provider(p) for p in provider_files], ignore_index=True)

    # Uzupełnij brakujące 100m z 10m (w obrębie tego samego providera)
    providers_df = ensure_target_vars(providers_df, alpha=args.wind_alpha)

    # Zatrzymujemy tylko interesujące nas 4 zmienne
    providers_df = providers_df[providers_df["variable"].isin(TARGET_VARS)].copy()

    # policz ile rekordów dorobiliśmy
    derived_counts = count_derived(providers_df)

    # 1) ile wierszy dostarcza samo API per zmienna (n_provider)
    prov_counts = (
        providers_df
        .groupby(["provider","variable"])
        .size()
        .rename("n_provider")
        .reset_index()
    )

    # 2) dopasowanie z ERA5
    era5_4 = era5[era5["variable"].isin(TARGET_VARS)].copy()
    merged = providers_df.merge(
        era5_4.rename(columns={"value":"era5_value"}),
        on=["time","latitude","longitude","variable"],
        how="inner"
    ).rename(columns={"value":"pred_value"})

    if merged.empty:
        print("No overlapping (time,lat,lon,variable) between ERA5 and providers.")
        full = prov_counts.copy()
        full["n_overlap"] = 0
        for c in ["bias_mean","mae","rmse","corr","over_pct","under_pct"]:
            full[c] = np.nan
    else:
        merged["error"] = merged["pred_value"] - merged["era5_value"]

        def _corr(idx):
            g = merged.loc[idx]
            if len(g) < 3: return np.nan
            return float(np.corrcoef(g["pred_value"], g["era5_value"])[0,1])

        metrics = (
            merged
            .groupby(["provider","variable"])
            .agg(
                n_overlap=("error","size"),
                bias_mean=("error","mean"),
                mae=("error", lambda s: float(np.mean(np.abs(s)))),
                rmse=("error", lambda s: float(np.sqrt(np.mean(np.square(s))))),
                corr=("error", lambda s: _corr(s.index)),
                over_pct=("error", lambda s: 100.0*float(np.mean(s>0))),
                under_pct=("error", lambda s: 100.0*float(np.mean(s<0))),
            )
            .reset_index()
        )

        full = prov_counts.merge(metrics, on=["provider","variable"], how="left")

    # 3) coverage i derived info
    full["coverage_pct"] = (
        100.0 * full["n_overlap"].fillna(0) / full["n_provider"].replace(0, np.nan)
    )
    full["coverage_pct"] = full["coverage_pct"].fillna(0).round(2)

    full = full.merge(derived_counts, on=["provider","variable"], how="left")
    full["derived_n"] = full["derived_n"].fillna(0).astype(int)
    full["derived_pct"] = (
        100.0 * full["derived_n"] / full["n_provider"].replace(0, np.nan)
    ).fillna(0).round(2)

    # 4) polskie objaśnienia kolumn
    cols = [
        "provider","variable","n_provider","n_overlap","coverage_pct",
        "derived_n","derived_pct",
        "bias_mean","mae","rmse","corr","over_pct","under_pct"
    ]
    full = full.reindex(columns=cols)

    rename_cols = {
        "provider":     "provider",
        "variable":     "variable",
        "n_provider":   "n_provider (l pkt API)",
        "n_overlap":    "n (l dopasowań z ERA5)",
        "coverage_pct": "coverage_pct (% pokrycia ERA5)",
        "derived_n":    "derived_n (ile wartości dorobiono z 10 m)",
        "derived_pct":  "derived_pct (% dorobionych wartości)",
        "bias_mean":    "bias_mean (błąd średni: pred − ERA5)",
        "mae":          "mae (średni błąd bezwzględny)",
        "rmse":         "rmse (pierwiastek średniego błędu kwadratowego)",
        "corr":         "corr (korelacja P)",
        "over_pct":     "over_pct (% zawyżenia)",
        "under_pct":    "under_pct (% zanizenia)",
    }
    full = full.rename(columns=rename_cols)

    summary_path = outdir / "era5_comparison_summary.csv"
    full.to_csv(summary_path, index=False)
    print(f"[OK] Wrote summary: {summary_path}")

    # ========== ANALIZA PER API (wzorce/tendencje) ==========
    analysis_dir = outdir / "analysis"
    analysis_dir.mkdir(parents=True, exist_ok=True)

    if 'merged' in locals() and not merged.empty:
        m2 = merged.copy()

        # (1) błąd kątowy dla kierunku wiatru (poprawne na okręgu):
        is_wdir = m2["variable"].str.contains("wind_direction")
        if is_wdir.any():
            ang = ((m2.loc[is_wdir, "pred_value"] - m2.loc[is_wdir, "era5_value"] + 180.0) % 360.0) - 180.0
            m2.loc[is_wdir, "error_angle"] = ang
        m2["error_angle"] = m2["error_angle"].fillna(m2["error"])

        # (2) regresja liniowa: pred = slope*ERA5 + intercept (skala/offset)
        def linfit(x, y):
            x = np.asarray(x, float);
            y = np.asarray(y, float)
            if x.size < 3: return np.nan, np.nan, np.nan
            A = np.vstack([x, np.ones_like(x)]).T
            slope, intercept = np.linalg.lstsq(A, y, rcond=None)[0]
            yhat = slope * x + intercept
            ss_res = np.sum((y - yhat) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else np.nan
            return float(slope), float(intercept), float(r2)

        # (3) dobowy wzorzec biasu
        m2["hour"] = pd.to_datetime(m2["time"]).dt.hour

        rows = []
        for (prov, var), df in m2.groupby(["provider", "variable"]):
            if df.empty:
                continue
            # metryki na error_angle
            bias = float(np.mean(df["error_angle"]))
            mae = float(np.mean(np.abs(df["error_angle"])))
            rmse = float(np.sqrt(np.mean(np.square(df["error_angle"]))))
            sl, itc, r2 = linfit(df["era5_value"], df["pred_value"])

            # dobowy wzorzec
            byh = df.groupby("hour")["error_angle"].mean()
            if len(byh) > 0:
                diurnal_amp = float(byh.max() - byh.min())
                peak_hour = int(byh.abs().idxmax())
            else:
                diurnal_amp = np.nan
                peak_hour = np.nan

            rows.append({
                "provider": prov, "variable": var,
                "bias": bias, "MAE": mae, "RMSE": rmse,
                "slope": sl, "intercept": itc, "R2": r2,
                "diurnal_amp": diurnal_amp, "diurnal_peak_hour": peak_hour,
            })

        patterns = pd.DataFrame(rows)

        # (4) metryki detekcji opadu (POD/FAR/CSI) per provider
        thr = args.precip_thresh
        ev_rows = []
        for prov, dfp in m2[m2["variable"] == "precipitation"].groupby("provider"):
            if dfp.empty:
                ev_rows.append({"provider": prov, "POD": np.nan, "FAR": np.nan, "CSI": np.nan})
                continue
            pred_ev = (dfp["pred_value"] > thr).to_numpy()
            era_ev = (dfp["era5_value"] > thr).to_numpy()
            TP = int(np.sum(pred_ev & era_ev))
            FP = int(np.sum(pred_ev & ~era_ev))
            FN = int(np.sum(~pred_ev & era_ev))
            denom_pod = (TP + FN)
            denom_far = (TP + FP)
            denom_csi = (TP + FP + FN)
            POD = (TP / denom_pod) if denom_pod > 0 else np.nan  # Probability of Detection
            FAR = (FP / denom_far) if denom_far > 0 else np.nan  # False Alarm Ratio
            CSI = (TP / denom_csi) if denom_csi > 0 else np.nan  # Critical Success Index
            ev_rows.append({"provider": prov, "POD": POD, "FAR": FAR, "CSI": CSI})

        ev_df = pd.DataFrame(ev_rows)

        # (5) dołącz coverage/dorobione z tabeli "full"
        #    kolumny: provider (API), zmienna, pokrycie%, dorobione%
        slim = full.rename(columns={
            full.columns[0]: "provider",
            full.columns[1]: "variable",
            full.columns[4]: "pokrycie%",
            full.columns[6]: "dorobione%",
        })[["provider", "variable", "pokrycie%", "dorobione%"]]

        patterns = patterns.merge(slim, on=["provider", "variable"], how="left")

        # zapisz zbiorczy CSV
        patterns_path = analysis_dir / "api_patterns.csv"
        patterns.to_csv(patterns_path, index=False)

        # zrób CSV z metrykami opadowymi
        ev_path = analysis_dir / "precip_detection_metrics.csv"
        ev_df.to_csv(ev_path, index=False)

        print(f"[OK] API patterns: {patterns_path}")
        print(f"[OK] Precip detection: {ev_path}")

        # (6) proste wykresy pomocnicze – regresja i dobowy bias per zmienna/prov
        #    (rysujemy tylko gdy są dane)
        for (prov, var), df in m2.groupby(["provider", "variable"]):
            if df.empty:
                continue
            # scatter z linią regresji
            plt.figure()
            plt.scatter(df["era5_value"], df["pred_value"], s=6, alpha=0.6)
            # linia 1:1
            mn = float(np.nanmin([df["era5_value"].min(), df["pred_value"].min()]))
            mx = float(np.nanmax([df["era5_value"].max(), df["pred_value"].max()]))
            plt.plot([mn, mx], [mn, mx], linewidth=1)
            # linia regresji
            sl, itc, _ = linfit(df["era5_value"], df["pred_value"])
            if not np.isnan(sl):
                xx = np.linspace(mn, mx, 50)
                yy = sl * xx + itc
                plt.plot(xx, yy, linewidth=1)
            plt.xlabel("ERA5");
            plt.ylabel(f"{prov}")
            plt.title(f"Regresja: {prov} — {var} (y = {sl:.2f}x + {itc:.2f})")
            safe_savefig(analysis_dir / f"regression_{prov}_{var}.png")

            # dobowy bias (na error_angle)
            byh = df.groupby("hour")["error_angle"].mean()
            if len(byh) > 0:
                plt.figure()
                plt.plot(byh.index, byh.values, marker="o")
                plt.axhline(0, linewidth=1)
                plt.xlabel("Godzina (UTC)");
                plt.ylabel("Średni błąd")
                plt.title(f"Bias dobowy: {prov} — {var}")
                safe_savefig(analysis_dir / f"diurnal_bias_{prov}_{var}.png")

    # --------------- PLOTS ------------------ #
    plots_dir = outdir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    if not merged.empty:
        # Histogram błędów
        for (prov, var), df in merged.groupby(["provider","variable"]):
            if df.empty: continue
            plt.figure()
            plt.hist(df["error"].dropna(), bins=30)
            plt.xlabel("Błąd (pred − ERA5)")
            plt.ylabel("Liczba obserwacji")
            plt.title(f"Histogram błędu: {prov} vs ERA5 — {var}")
            safe_savefig(plots_dir / f"error_hist_{prov}_{var}.png")

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
            safe_savefig(plots_dir / f"scatter_{prov}_{var}.png")

        # Bias dobowy
        merged["hour"] = pd.to_datetime(merged["time"]).dt.hour
        for (prov, var), df in merged.groupby(["provider","variable"]):
            if df.empty: continue
            by_hour = df.groupby("hour")["error"].mean()
            plt.figure()
            plt.plot(by_hour.index, by_hour.values, marker="o")
            plt.axhline(0, linewidth=1)
            plt.xlabel("Godzina (UTC)"); plt.ylabel("Średni błąd")
            plt.title(f"Bias dobowy: {prov} — {var}")
            safe_savefig(plots_dir / f"bias_by_hour_{prov}_{var}.png")

        # Boxplot błędów per zmienna
        for var, dfv in merged.groupby("variable"):
            plt.figure()
            labels = sorted(dfv["provider"].unique())
            data = [dfv.loc[dfv["provider"]==p, "error"].dropna().values for p in labels]
            plt.boxplot(data, labels=labels, showmeans=True)
            plt.axhline(0, linewidth=1)
            plt.ylabel("Błąd (pred − ERA5)")
            plt.title(f"Rozkład błędów — {var}")
            safe_savefig(plots_dir / f"boxplot_errors_{var}.png")

    print(f"[OK] Plots in: {plots_dir}")

if __name__ == "__main__":
    run()
