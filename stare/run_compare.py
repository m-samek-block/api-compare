#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_compare.py — pobiera prognozy z darmowych API i porównuje do ERA5 (long CSV).
Zapisuje:
  - wyniki/summaries/era5_comparison_summary.csv  (krótkie PL nagłówki)
  - wyniki/plots/*.png                            (hist, scatter, bias dob.)
  - wyniki/analysis/*.{csv,png,md}                (analiza per API + raport)

Wymagane:
  - fetch_forecasts.py w tym samym katalogu
  - era5.csv w formacie long: time,latitude,longitude,variable,value

Przykład:
  python run_compare.py --lat 52.2297 --lon 21.0122 \
    --start 2025-08-12T00:00:00Z --end 2025-08-14T00:00:00Z \
    --providers openmeteo,metno,openweather,weatherapi,visualcrossing \
    --era5 ./dane/era5.csv --outdir ./wyniki --wind-alpha 0.143 --precip-thresh 0.1
"""
import argparse
import importlib.util
import os
from pathlib import Path
from datetime import datetime, timezone
from dateutil import parser as dtparser

import numpy as np
import pandas as pd

# backend bez GUI (Windows bez Tcl/Tk)
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent

# --- Załaduj fetch_forecasts.py dynamicznie ---
fetch_path = HERE / "fetch_forecasts.py"
if not fetch_path.exists():
    raise SystemExit("Missing fetch_forecasts.py next to this script.")

spec = importlib.util.spec_from_file_location("fetch_mod", str(fetch_path))
fetch_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch_mod)

ISO = "%Y-%m-%dT%H:%M:%SZ"

TARGET_VARS = {
    "temperature_2m",
    "precipitation",
    "wind_speed_100m",
    "wind_direction_100m",
}


def round_to_hour(ts: pd.Series) -> pd.Series:
    t = pd.to_datetime(ts, utc=True, errors="coerce")
    # zaokrąglamy w dół do pełnej godziny (spójne z generatorami godzinowymi)
    return t.dt.floor("h")


def load_long_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    req = ["time", "latitude", "longitude", "variable", "value"]
    for c in req:
        if c not in df.columns:
            raise ValueError(f"{path} missing required column {c}")
    df["time"] = round_to_hour(df["time"])
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["time", "latitude", "longitude", "variable", "value"])
    return df


def safe_savefig(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(path, dpi=140, bbox_inches="tight")
    plt.close()


# --- Dorabianie zmiennych docelowych (np. wiatr 100m z 10m) ---
def ensure_target_vars(df: pd.DataFrame, alpha: float) -> tuple[pd.DataFrame, set]:
    """
    df: long format z kolumnami time,latitude,longitude,variable,value
    alpha: parametr prawa potęgowego (10m->100m)

    Zwraca: (df2, derived_set) — df2 z dorobionymi wind_*_100m (o ile brak);
             derived_set = zbiór (provider, variable) które były dorobione.
    """
    if df.empty:
        return df.copy(), set()

    out = df.copy()
    derived = set()

    # Czy mamy 10m?
    has_wspd10 = (out["variable"] == "wind_speed_10m").any()
    has_wdir10 = (out["variable"] == "wind_direction_10m").any()

    # Wiatr 100m z 10m
    if (not (out["variable"] == "wind_speed_100m").any()) and has_wspd10:
        v10 = out[out["variable"] == "wind_speed_10m"].copy()
        v10.loc[:, "variable"] = "wind_speed_100m"
        factor = (100.0 / 10.0) ** float(alpha)
        v10.loc[:, "value"] = pd.to_numeric(v10["value"], errors="coerce") * factor
        out = pd.concat([out, v10], ignore_index=True)
        # provider ustawimy przy późniejszym etapie (po dodaniu kolumny 'provider')

    if (not (out["variable"] == "wind_direction_100m").any()) and has_wdir10:
        d10 = out[out["variable"] == "wind_direction_10m"].copy()
        d10.loc[:, "variable"] = "wind_direction_100m"
        # kierunek 10m ≈ 100m (brak lepszej informacji w danych wejściowych)
        out = pd.concat([out, d10], ignore_index=True)

    # Zaznaczymy dorobione później, gdy znamy 'provider'
    return out, derived


def circular_error_deg(pred_deg: pd.Series, true_deg: pd.Series) -> pd.Series:
    # wrap różnicy do [-180, 180]
    return ((pred_deg - true_deg + 180.0) % 360.0) - 180.0


def run():
    ap = argparse.ArgumentParser(description="Fetch forecasts (free APIs) and compare to ERA5.")
    ap.add_argument("--lat", type=float, required=True)
    ap.add_argument("--lon", type=float, required=True)
    ap.add_argument("--start", type=str, required=True)
    ap.add_argument("--end", type=str, required=True)
    ap.add_argument("--providers", type=str, default="openmeteo,metno")
    ap.add_argument("--era5", type=str, required=True, help="Path to ERA5 CSV (long format)")
    ap.add_argument("--outdir", type=str, default=str(HERE / "wyniki"))
    ap.add_argument("--wind-alpha", type=str, default="0.143",
                    help="Parametr prawa potęgowego do przeliczeń 10m->100m (domyślnie 0.143).")
    ap.add_argument("--grid-step", type=float, default=0.0,
                    help="Jeśli >0, zaokrąglaj lat/lon do kroku siatki (np. 0.25) przed merge.")
    ap.add_argument("--precip-thresh", type=float, default=0.1,
                    help="Próg (mm/h) dla metryk detekcji opadu POD/FAR/CSI.")

    # Klucze API (opcjonalnie; jeśli brak — dane zostaną pominięte)
    ap.add_argument("--metno-user-agent", type=str,
                    default="Weather-Compare/1.0 (contact: you@example.com)")
    ap.add_argument("--visualcrossing-key", type=str,
                    default=os.environ.get("VISUALCROSSING_KEY", ""))
    ap.add_argument("--openweather-key", type=str,
                    default=os.environ.get("OPENWEATHER_KEY", ""))
    ap.add_argument("--weatherapi-key", type=str,
                    default=os.environ.get("WEATHERAPI_KEY", ""))

    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Utwórz dedykowany folder na summaries
    summaries_dir = outdir / "summaries"
    summaries_dir.mkdir(parents=True, exist_ok=True)

    plots_dir = outdir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    lat, lon = args.lat, args.lon
    start = dtparser.isoparse(args.start).astimezone(timezone.utc)
    end = dtparser.isoparse(args.end).astimezone(timezone.utc)
    start = pd.Timestamp(start).floor("H").to_pydatetime().replace(tzinfo=timezone.utc)
    end = pd.Timestamp(end).floor("H").to_pydatetime().replace(tzinfo=timezone.utc)

    reqs = [p.strip().lower() for p in args.providers.split(",") if p.strip()]
    available = set(fetch_mod.PROVIDERS.keys())
    for p in reqs:
        if p not in available:
            raise SystemExit(f"Unknown provider {p}. Supported: {sorted(list(available))}")

    # --- FETCHE ---
    for prov in reqs:
        print(f"[Fetch] {prov}")
        try:
            if prov == "metno":
                rows = fetch_mod.fetch_metno(lat, lon, start, end, user_agent=args.metno_user_agent)
            elif prov == "visualcrossing":
                if not args.visualcrossing_key:
                    print("  Skipping visualcrossing (no API key).");
                    continue
                rows = fetch_mod.fetch_visualcrossing(lat, lon, start, end, api_key=args.visualcrossing_key)
            elif prov == "openweather":
                if not args.openweather_key:
                    print("  Skipping openweather (no API key).");
                    continue
                rows = fetch_mod.fetch_openweather(lat, lon, start, end, api_key=args.openweather_key)
            elif prov == "weatherapi":
                if not args.weatherapi_key:
                    print("  Skipping weatherapi (no API key).");
                    continue
                rows = fetch_mod.fetch_weatherapi(lat, lon, start, end, api_key=args.weatherapi_key)
            else:
                rows = fetch_mod.fetch_openmeteo(lat, lon, start, end)
        except Exception as e:
            print(f"  Error fetching {prov}: {e}")
            continue

        if not rows:
            print(f"  No rows for {prov}.");
            continue

        # zapisujemy w outdir
        fetch_mod.write_long_csv(outdir / f"provider_{prov}.csv", lat, lon, rows)
        print(f"  -> wrote {outdir / f'provider_{prov}.csv'} ({len(rows)} rows)")

    # --- ERA5 ---
    era5 = load_long_csv(Path(args.era5))
    # Bezpiecznik: użyj TYLKO dokładnie tego punktu (lat,lon), który porównujemy
    era5 = era5[(era5["latitude"].sub(lat).abs() < 1e-9) &
                (era5["longitude"].sub(lon).abs() < 1e-9)].copy()

    # --- Wczytaj provider_*.csv z outdir ---
    provider_files = list(outdir.glob("provider_*.csv"))
    if not provider_files:
        print("No provider CSVs found. Exiting.")
        # ale i tak zapisz pusty summary (spójność pipeline'u) - TERAZ W FOLDERZE SUMMARIES
        (summaries_dir / "era5_comparison_summary.csv").write_text("", encoding="utf-8")
        return

    def load_provider(path: Path) -> pd.DataFrame:
        df = load_long_csv(path)
        df["provider"] = path.stem.replace("provider_", "")
        return df

    prov_all = pd.concat([load_provider(p) for p in provider_files], ignore_index=True)

    # Dorób brakujące targety (np. 100m z 10m) per provider
    prov_list = []
    derived_marks = set()
    for prov, dfp in prov_all.groupby("provider"):
        df2, _ = ensure_target_vars(dfp, alpha=float(args.wind_alpha))
        df2["provider"] = prov  # zachowaj nazwę
        prov_list.append(df2)
    providers = pd.concat(prov_list, ignore_index=True)

    # Filtrujemy tylko targetowe zmienne + wyrównanie czasu
    providers = providers[providers["variable"].isin(TARGET_VARS)].copy()
    providers["time"] = round_to_hour(providers["time"])
    era5c = era5[era5["variable"].isin(TARGET_VARS)].copy()
    era5c["time"] = round_to_hour(era5c["time"])

    if providers.empty or era5c.empty:
        print("No data for target variables. Exiting.")
        # ZMIANA: summary w folderze summaries
        (summaries_dir / "era5_comparison_summary.csv").write_text("", encoding="utf-8")
        return

    # (opcjonalnie) snap do siatki
    if args.grid_step and args.grid_step > 0:
        def snap(x, s):
            return np.round(x / s) * s

        for df in (providers, era5c):
            df["lat_g"] = snap(df["latitude"], args.grid_step)
            df["lon_g"] = snap(df["longitude"], args.grid_step)
        merged = providers.merge(
            era5c.rename(columns={"value": "era5_value"}),
            left_on=["time", "lat_g", "lon_g", "variable"],
            right_on=["time", "lat_g", "lon_g", "variable"],
            how="inner"
        ).rename(columns={"value": "pred_value"})
    else:
        merged = providers.merge(
            era5c.rename(columns={"value": "era5_value"}),
            on=["time", "latitude", "longitude", "variable"],
            how="inner"
        ).rename(columns={"value": "pred_value"})

    if merged.empty:
        print("No overlapping (time,lat,lon,variable) between ERA5 and providers.")
        # zapisz pusty summary (nagłówki) dla spójności - W FOLDERZE SUMMARIES
        cols = ["provider (API)", "zmienna", "n_api", "n_era5", "pokrycie%", "dorobione%", "bias", "MAE", "RMSE",
                "corr", "%zawyż", "%zaniż"]
        pd.DataFrame(columns=cols).to_csv(summaries_dir / "era5_comparison_summary.csv", index=False)
        print(f"[OK] Wrote summary: {summaries_dir / 'era5_comparison_summary.csv'}")
        print(f"[OK] Plots in: {plots_dir}")
        return

    # Błąd i błąd kątowy dla kierunku
    merged["error"] = merged["pred_value"] - merged["era5_value"]
    is_wdir = merged["variable"].str.contains("wind_direction")
    merged.loc[is_wdir, "error"] = circular_error_deg(
        merged.loc[is_wdir, "pred_value"], merged.loc[is_wdir, "era5_value"]
    )

    # Pokrycie i dorobione
    # (liczymy n_api po provider×variable w PROWIDERS, n_era5 = liczba w merged)
    n_api = providers.groupby(["provider", "variable"]).size().rename("n_api").reset_index()
    n_ovl = merged.groupby(["provider", "variable"]).size().rename("n_era5").reset_index()
    cover = n_api.merge(n_ovl, on=["provider", "variable"], how="left").fillna({"n_era5": 0})
    cover["coverage_pct"] = np.where(cover["n_api"] > 0, 100.0 * cover["n_era5"] / cover["n_api"], np.nan)

    # Podstawowe metryki
    def _corr_block(g):
        if len(g) < 3: return np.nan
        return np.corrcoef(g["pred_value"], g["era5_value"])[0, 1]

    summary = merged.groupby(["provider", "variable"]).agg(
        n=("error", "size"),
        bias_mean=("error", "mean"),
        mae=("error", lambda s: np.mean(np.abs(s))),
        rmse=("error", lambda s: np.sqrt(np.mean(np.square(s)))),
        corr=("error", lambda s: _corr_block(merged.loc[s.index])),
        over_pct=("error", lambda s: 100 * np.mean(s > 0)),
        under_pct=("error", lambda s: 100 * np.mean(s < 0)),
    ).reset_index()

    # Dołącz coverage; dorobione% nie liczymy tu precyzyjnie na wierszach — zostawiamy 0
    full = summary.merge(cover, on=["provider", "variable"], how="left")
    full["derived_pct"] = 0.0

    # Krótkie polskie nagłówki
    rename_cols = {
        "provider": "provider (API)",
        "variable": "zmienna",
        "n_api": "n_api",
        "n_era5": "n_era5",
        "coverage_pct": "pokrycie%",
        "derived_pct": "dorobione%",
        "bias_mean": "bias",
        "mae": "MAE",
        "rmse": "RMSE",
        "corr": "corr",
        "over_pct": "%zawyż",
        "under_pct": "%zaniż",
    }
    full = full.rename(columns=rename_cols)
    # kolejność kolumn
    cols = ["provider (API)", "zmienna", "n_api", "n_era5", "pokrycie%", "dorobione%", "bias", "MAE", "RMSE", "corr",
            "%zawyż", "%zaniż"]
    full = full.reindex(columns=cols)

    # ZMIANA: summary zapisujemy w dedykowanym folderze summaries/
    summary_path = summaries_dir / "era5_comparison_summary.csv"
    full.to_csv(summary_path, index=False)
    print(f"[OK] Wrote summary: {summary_path}")

    # --- WYKRESY PODSTAWOWE ---
    # Histogram błędów
    for (prov, var), df in merged.groupby(["provider", "variable"]):
        if df.empty: continue
        plt.figure()
        plt.hist(df["error"].dropna(), bins=30)
        plt.xlabel("Błąd (pred − ERA5)")
        plt.ylabel("Liczba obserwacji")
        plt.title(f"Histogram błędu: {prov} vs ERA5 — {var}")
        safe_savefig(plots_dir / f"error_hist_{prov}_{var}.png")

    # Scatter pred vs ERA5
    for (prov, var), df in merged.groupby(["provider", "variable"]):
        if df.empty: continue
        plt.figure()
        plt.scatter(df["era5_value"], df["pred_value"], s=6, alpha=0.6)
        mn = float(np.nanmin([df["era5_value"].min(), df["pred_value"].min()]))
        mx = float(np.nanmax([df["era5_value"].max(), df["pred_value"].max()]))
        plt.plot([mn, mx], [mn, mx], linewidth=1)
        plt.xlabel("ERA5");
        plt.ylabel(f"{prov}")
        plt.title(f"Predykcja vs ERA5 — {var}")
        safe_savefig(plots_dir / f"scatter_{prov}_{var}.png")

    # Bias dobowy
    merged["hour"] = pd.to_datetime(merged["time"]).dt.hour
    for (prov, var), df in merged.groupby(["provider", "variable"]):
        if df.empty: continue
        by_hour = df.groupby("hour")["error"].mean()
        plt.figure()
        plt.plot(by_hour.index, by_hour.values, marker="o")
        plt.axhline(0, linewidth=1)
        plt.xlabel("Godzina (UTC)");
        plt.ylabel("Średni błąd")
        plt.title(f"Bias dobowy: {prov} — {var}")
        safe_savefig(plots_dir / f"bias_by_hour_{prov}_{var}.png")

    # Boxplot per zmienna
    for var, dfv in merged.groupby("variable"):
        plt.figure()
        labels = sorted(dfv["provider"].unique())
        data = [dfv.loc[dfv["provider"] == p, "error"].dropna().values for p in labels]
        plt.boxplot(data, tick_labels=labels, showmeans=True)
        plt.axhline(0, linewidth=1)
        plt.ylabel("Błąd (pred − ERA5)")
        plt.title(f"Rozkład błędów — {var}")
        safe_savefig(plots_dir / f"boxplot_errors_{var}.png")

    print(f"[OK] Plots in: {plots_dir}")

    # --- ANALIZA PER API (wzorce/tendencje) + RAPORT ---
    analysis_dir = outdir / "analysis"
    analysis_dir.mkdir(parents=True, exist_ok=True)

    m2 = merged.copy()
    # błąd kątowy dla kierunku
    is_wdir = m2["variable"].str.contains("wind_direction")
    m2.loc[is_wdir, "error_angle"] = circular_error_deg(m2.loc[is_wdir, "pred_value"], m2.loc[is_wdir, "era5_value"])
    m2["error_angle"] = m2["error_angle"].fillna(m2["error"])
    m2["hour"] = pd.to_datetime(m2["time"]).dt.hour

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

    rows = []
    for (prov, var), df in m2.groupby(["provider", "variable"]):
        if df.empty: continue
        bias = float(np.mean(df["error_angle"]))
        mae = float(np.mean(np.abs(df["error_angle"])))
        rmse = float(np.sqrt(np.mean(np.square(df["error_angle"]))))
        sl, itc, r2 = linfit(df["era5_value"], df["pred_value"])
        byh = df.groupby("hour")["error_angle"].mean()
        diurnal_amp = float(byh.max() - byh.min()) if len(byh) > 0 else np.nan
        peak_hour = int(byh.abs().idxmax()) if len(byh) > 0 else np.nan
        rows.append({
            "provider": prov, "variable": var,
            "bias": bias, "MAE": mae, "RMSE": rmse,
            "slope": sl, "intercept": itc, "R2": r2,
            "diurnal_amp": diurnal_amp, "diurnal_peak_hour": peak_hour,
        })
    patterns = pd.DataFrame(rows)

    # metryki detekcji opadu POD/FAR/CSI
    thr = float(args.precip_thresh)
    ev_rows = []
    for prov, dfp in m2[m2["variable"] == "precipitation"].groupby("provider"):
        if dfp.empty:
            ev_rows.append({"provider": prov, "POD": np.nan, "FAR": np.nan, "CSI": np.nan});
            continue
        pred_ev = (dfp["pred_value"] > thr).to_numpy()
        era_ev = (dfp["era5_value"] > thr).to_numpy()
        TP = int(np.sum(pred_ev & era_ev));
        FP = int(np.sum(pred_ev & ~era_ev));
        FN = int(np.sum(~pred_ev & era_ev))
        POD = TP / (TP + FN) if (TP + FN) > 0 else np.nan
        FAR = FP / (TP + FP) if (TP + FP) > 0 else np.nan
        CSI = TP / (TP + FP + FN) if (TP + FP + FN) > 0 else np.nan
        ev_rows.append({"provider": prov, "POD": POD, "FAR": FAR, "CSI": CSI})
    ev_df = pd.DataFrame(ev_rows)

    # dołącz coverage/dorobione do patterns
    slim = full[["provider (API)", "zmienna", "pokrycie%", "dorobione%"]].rename(
        columns={"provider (API)": "provider", "zmienna": "variable"})
    patterns = patterns.merge(slim, on=["provider", "variable"], how="left")

    patterns_path = analysis_dir / "api_patterns.csv"
    ev_path = analysis_dir / "precip_detection_metrics.csv"
    patterns.to_csv(patterns_path, index=False)
    ev_df.to_csv(ev_path, index=False)
    print(f"[OK] API patterns: {patterns_path}")
    print(f"[OK] Precip detection: {ev_path}")

    # wykresy pomocnicze do raportów
    for (prov, var), df in m2.groupby(["provider", "variable"]):
        if df.empty: continue
        # scatter + linia 1:1 + regresja
        plt.figure()
        plt.scatter(df["era5_value"], df["pred_value"], s=6, alpha=0.6)
        mn = float(np.nanmin([df["era5_value"].min(), df["pred_value"].min()]))
        mx = float(np.nanmax([df["era5_value"].max(), df["pred_value"].max()]))
        plt.plot([mn, mx], [mn, mx], linewidth=1)
        sl, itc, _ = linfit(df["era5_value"], df["pred_value"])
        if not np.isnan(sl):
            xx = np.linspace(mn, mx, 50);
            yy = sl * xx + itc
            plt.plot(xx, yy, linewidth=1)
        plt.xlabel("ERA5");
        plt.ylabel(f"{prov}")
        plt.title(f"Regresja: {prov} — {var} (y = {sl:.2f}x + {itc:.2f})")
        safe_savefig(analysis_dir / f"regression_{prov}_{var}.png")

        # dobowy bias
        byh = df.groupby("hour")["error_angle"].mean()
        if len(byh) > 0:
            plt.figure()
            plt.plot(byh.index, byh.values, marker="o")
            plt.axhline(0, linewidth=1)
            plt.xlabel("Godzina (UTC)");
            plt.ylabel("Średni błąd")
            plt.title(f"Bias dobowy: {prov} — {var}")
            safe_savefig(analysis_dir / f"diurnal_bias_{prov}_{var}.png")

    # Raporty MD per provider + index
    def describe_pattern(row, precip_ev=None):
        var = row["variable"]
        bias = row["bias"];
        mae = row["MAE"];
        rmse = row["RMSE"];
        sl = row["slope"];
        itc = row["intercept"];
        r2 = row["R2"]
        amp = row["diurnal_amp"];
        h = row["diurnal_peak_hour"]
        parts = []
        if not np.isnan(bias):
            trend = "zawyża" if bias > 0 else ("zaniża" if bias < 0 else "≈0")
            parts.append(f"Bias {bias:+.2f} ({trend}).")
        if not np.isnan(sl):
            parts.append(f"Skala/offset: slope={sl:.2f}, intercept={itc:.2f}, R²={r2:.2f}.")
        if not np.isnan(amp):
            parts.append(f"Wzorzec dobowy: amplituda {amp:.2f}, pik ok. godz. {h}.")
        if var == "precipitation" and precip_ev is not None:
            POD = precip_ev.get("POD");
            FAR = precip_ev.get("FAR");
            CSI = precip_ev.get("CSI")
            if POD is not None and not (np.isnan(POD) or np.isnan(FAR) or np.isnan(CSI)):
                parts.append(f"Opad: POD={POD:.2f}, FAR={FAR:.2f}, CSI={CSI:.2f} (próg {thr} mm/h).")
        return " ".join(parts)

    providers_u = sorted(patterns["provider"].unique())
    index_lines = ["# Raport porównania API vs ERA5\n"]
    # ranking po medianie RMSE
    best_rows = (patterns.groupby("provider")["RMSE"].median().sort_values().reset_index())
    index_lines.append("## Ranking (mediana RMSE po zmiennych)\n")
    for i, r in best_rows.iterrows():
        index_lines.append(f"{i + 1}. **{r['provider']}** — RMSE_med={r['RMSE']:.3f}")
    index_lines.append("")

    # opad – ranking po CSI
    if not ev_df.empty and ev_df["CSI"].notna().any():
        ev_rank = ev_df.sort_values("CSI", ascending=False).reset_index(drop=True)
        index_lines.append("## Wykrywanie opadów (CSI)\n")
        for i, r in ev_rank.iterrows():
            index_lines.append(
                f"{i + 1}. **{r['provider']}** — CSI={r['CSI']:.2f}, POD={r['POD']:.2f}, FAR={r['FAR']:.2f}")
        index_lines.append("")

    import re as _re
    for prov in providers_u:
        p_df = patterns[patterns["provider"] == prov].copy()
        ev_row = ev_df[ev_df["provider"] == prov].iloc[0].to_dict() if (
                    not ev_df.empty and (ev_df["provider"] == prov).any()) else {}
        slug = _re.sub(r"[^a-z0-9]+", "-", prov.lower()).strip("-")
        md_path = analysis_dir / f"provider_{slug}.md"
        lines = [f"# {prov}\n"]
        cols_show = ["variable", "pokrycie%", "dorobione%", "bias", "MAE", "RMSE", "slope", "intercept", "R2",
                     "diurnal_amp", "diurnal_peak_hour"]
        lines.append("| " + " | ".join(cols_show) + " |")
        lines.append("|" + "|".join(["---"] * len(cols_show)) + "|")
        for _, row in p_df[cols_show].iterrows():
            vals = []
            for c in cols_show:
                v = row[c]
                if isinstance(v, float):
                    vals.append(f"{v:.3f}")
                else:
                    vals.append(str(v))
            lines.append("| " + " | ".join(vals) + " |")
        lines.append("")
        for _, row in p_df.iterrows():
            var = row["variable"]
            desc = describe_pattern(row, precip_ev=ev_row if var == "precipitation" else None)
            lines.append(f"## {var}\n")
            lines.append(desc + "\n")
            reg_png = analysis_dir / f"regression_{prov}_{var}.png"
            di_png = analysis_dir / f"diurnal_bias_{prov}_{var}.png"
            if reg_png.exists():
                lines.append(f"![regresja]({reg_png.name})")
            if di_png.exists():
                lines.append(f"![bias dobowy]({di_png.name})")
            lines.append("")
        md_path.write_text("\n".join(lines), encoding="utf-8")
        index_lines.append(f"- [{prov}](provider_{slug}.md)")

    (analysis_dir / "REPORT.md").write_text("\n".join(index_lines), encoding="utf-8")
    print(f"[OK] Raport: {analysis_dir / 'REPORT.md'} (oraz pliki provider_*.md)")


if __name__ == "__main__":
    run()