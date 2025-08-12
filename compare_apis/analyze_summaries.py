#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analiza historyczna wielu plików era5_comparison_summary*.csv

Co robi:
- skanuje katalog z wynikami (domyślnie ./wyniki)
- wczytuje wszystkie pliki era5_comparison_summary*.csv (z i bez stempla)
- UJEDNOLICA kolumny (obsługuje "stare" długie i "nowe" krótkie nazwy)
- skleja do jednego dataframe z metadanymi RUN_TS (czas uruchomienia z nazwy pliku
  lub z mtime, jeśli w nazwie nie ma stempla)
- liczy statystyki i TRENDY (OLS slope po czasie) per provider × zmienna
- tworzy wykresy linii RMSE/bias po czasie
- generuje raport Markdown: HISTORY_REPORT.md + CSV z agregatami

Wymagania: pandas, numpy, matplotlib, python-dateutil
"""

import re
import os
import argparse
from pathlib import Path
from datetime import datetime, timezone
from dateutil import parser as dtparser

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


# --- możliwe warianty nazw kolumn -> nazwa kanoniczna ---
COLMAP = {
    "provider": ["provider (API)", "provider (dostawca)", "provider"],
    "variable": ["zmienna", "variable (zmienna)", "variable"],
    "n_provider": ["n_api", "n_provider (liczba punktów API)", "n_provider"],
    "n_era5": ["n_era5", "n (liczba dopasowań z ERA5)", "n"],
    "coverage_pct": ["pokrycie%", "coverage_pct (% pokrycia z ERA5)", "coverage_pct"],
    "derived_pct": ["dorobione%", "derived_pct (% dorobionych wartości)", "derived_pct"],
    "bias": ["bias", "bias_mean (błąd średni: pred − ERA5)", "bias_mean"],
    "mae": ["MAE", "mae", "mae (średni błąd bezwzględny)"],
    "rmse": ["RMSE", "rmse", "rmse (pierwiastek średniego błędu kwadratowego)"],
    "corr": ["corr", "corr (korelacja Pearsona)"],
    "over_pct": ["%zawyż", "over_pct", "over_pct (% przypadków z zawyżeniem)"],
    "under_pct": ["%zaniż", "under_pct", "under_pct (% przypadków z zaniżeniem)"],
}

RE_TS = re.compile(r"era5_comparison_summary_(\d{8}_\d{6}Z)\.csv$", re.IGNORECASE)

def find_column(df: pd.DataFrame, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    for canon, candidates in COLMAP.items():
        c = find_column(df, candidates)
        if c is not None:
            out[canon] = df[c]
        else:
            # kolumny opcjonalne mogą nie istnieć
            out[canon] = np.nan
    # typy
    for col in ["bias","mae","rmse","corr","coverage_pct","derived_pct","over_pct","under_pct"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ["n_provider","n_era5"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
    out["provider"] = out["provider"].astype(str)
    out["variable"] = out["variable"].astype(str)
    return out

def parse_run_ts_from_name(path: Path) -> datetime | None:
    m = RE_TS.search(path.name)
    if not m:
        return None
    ts = m.group(1)  # YYYYMMDD_HHMMSSZ
    return datetime.strptime(ts, "%Y%m%d_%H%M%SZ").replace(tzinfo=timezone.utc)

def load_summaries(root: Path) -> pd.DataFrame:
    files = sorted(list(root.glob("era5_comparison_summary*.csv")))
    if not files:
        raise SystemExit(f"Brak plików era5_comparison_summary*.csv w {root}")

    frames = []
    for p in files:
        try:
            df = pd.read_csv(p)
        except Exception as e:
            print(f"[WARN] Nie mogę wczytać {p}: {e}")
            continue
        df = normalize_columns(df)
        # znacznik czasu RUN_TS
        ts = parse_run_ts_from_name(p) or datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
        df["run_ts"] = ts
        df["run_file"] = p.name
        frames.append(df)

    if not frames:
        raise SystemExit("Nie udało się wczytać żadnego poprawnego pliku.")
    all_df = pd.concat(frames, ignore_index=True)

    # filtruj do 4 interesujących zmiennych, jeśli chcesz
    wanted = {"precipitation","temperature_2m","wind_speed_100m","wind_direction_100m"}
    all_df = all_df[all_df["variable"].isin(wanted)].copy()
    return all_df

def ols_slope(y, x):
    y = np.asarray(y, float)
    x = np.asarray(x, float)
    if len(y) < 2 or np.allclose(x, x[0]):
        return np.nan
    A = np.vstack([x, np.ones_like(x)]).T
    slope, intercept = np.linalg.lstsq(A, y, rcond=None)[0]
    return float(slope)

def make_plots(all_df: pd.DataFrame, outdir: Path, top_n_per_var: int = 6):
    outdir.mkdir(parents=True, exist_ok=True)
    # wykresy RMSE i bias po czasie per zmienna, top-N providerów wg mediany RMSE
    for var, dfv in all_df.groupby("variable"):
        rank = (dfv.groupby("provider")["rmse"].median()
                .sort_values().dropna())
        keep = rank.index[:top_n_per_var].tolist() if len(rank) > top_n_per_var else rank.index.tolist()

        plt.figure()
        for prov, d in dfv[dfv["provider"].isin(keep)].groupby("provider"):
            d = d.sort_values("run_ts")
            plt.plot(d["run_ts"], d["rmse"], marker="o", label=prov)
        plt.xlabel("czas uruchomienia")
        plt.ylabel("RMSE")
        plt.title(f"RMSE vs czas — {var}")
        plt.legend()
        plt.tight_layout()
        plt.savefig(outdir / f"rmse_time_{var}.png", dpi=140, bbox_inches="tight")
        plt.close()

        plt.figure()
        for prov, d in dfv[dfv["provider"].isin(keep)].groupby("provider"):
            d = d.sort_values("run_ts")
            plt.plot(d["run_ts"], d["bias"], marker="o", label=prov)
        plt.xlabel("czas uruchomienia")
        plt.ylabel("bias (pred − ERA5)")
        plt.title(f"Bias vs czas — {var}")
        plt.legend()
        plt.tight_layout()
        plt.savefig(outdir / f"bias_time_{var}.png", dpi=140, bbox_inches="tight")
        plt.close()

def summarize_history(all_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # bazowe agregaty per provider×zmienna
    grp = all_df.groupby(["provider","variable"])
    agg = grp.agg(
        runs=("run_ts","nunique"),
        rmse_med=("rmse","median"),
        rmse_mean=("rmse","mean"),
        rmse_std=("rmse","std"),
        bias_med=("bias","median"),
        bias_mean=("bias","mean"),
        bias_std=("bias","std"),
        mae_med=("mae","median"),
        corr_med=("corr","median"),
        over_med=("over_pct","median"),
        under_med=("under_pct","median"),
        cover_med=("coverage_pct","median"),
        deriv_med=("derived_pct","median"),
    ).reset_index()

    # trendy (slope po czasie; oś = dni od pierwszego runa)
    all_df = all_df.copy()
    t0 = all_df["run_ts"].min()
    all_df["t_days"] = (all_df["run_ts"] - t0).dt.total_seconds() / (24*3600)

    slopes = (all_df.groupby(["provider","variable"])
              .apply(lambda d: pd.Series({
                  "slope_rmse": ols_slope(d["rmse"], d["t_days"]),
                  "slope_bias": ols_slope(d["bias"], d["t_days"]),
              }))
              .reset_index())

    stats = agg.merge(slopes, on=["provider","variable"], how="left")
    return stats, all_df

def make_report(all_df: pd.DataFrame, stats: pd.DataFrame, plots_dir: Path, outpath: Path):
    lines = ["# Raport historyczny (wiele uruchomień)\n"]

    # 1) Top rankingi per zmienna (RMSE_med)
    lines.append("## Rankingi RMSE (mediana po runach)\n")
    for var, st in stats.groupby("variable"):
        st = st.sort_values("rmse_med")
        lines.append(f"### {var}")
        lines.append("| provider | runs | RMSE_med | bias_med | cover_med% | trend_rmse/dzień |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for _, r in st.iterrows():
            lines.append(
                f"| {r['provider']} | {int(r['runs'])} | {r['rmse_med']:.3f} | {r['bias_med']:.3f} "
                f"| {r['cover_med']:.1f} | {r['slope_rmse']:.4f} |"
            )
        # obrazki
        img_rmse = plots_dir / f"rmse_time_{var}.png"
        img_bias = plots_dir / f"bias_time_{var}.png"
        if img_rmse.exists():
            lines.append(f"\n![RMSE vs czas — {var}]({img_rmse.name})\n")
        if img_bias.exists():
            lines.append(f"\n![Bias vs czas — {var}]({img_bias.name})\n")

    # 2) Stabilność & tendencje (po providerze, agregując zmienne)
    prov_rank = (stats.groupby("provider")["rmse_med"]
                 .median().sort_values())
    lines.append("\n## Ranking ogólny (mediana RMSE_med po zmiennych)\n")
    for i, (prov, val) in enumerate(prov_rank.items(), start=1):
        lines.append(f"{i}. **{prov}** — {val:.3f}")

    # 3) Wzorce zawyżania/zaniżania
    lines.append("\n## Wzorce biasu (znak i wielkość)\n")
    bias_patterns = (stats.assign(trend=lambda r: np.where(r["bias_med"]>0, "zawyża", np.where(r["bias_med"]<0, "zaniża","≈0")))
                           .sort_values(["variable","bias_med"]))
    for var, st in bias_patterns.groupby("variable"):
        lines.append(f"### {var}")
        lines.append("| provider | bias_med | bias_mean | trend_bias | slope_bias/dzień |")
        lines.append("|---|---:|---:|---|---:|")
        for _, r in st.iterrows():
            lines.append(f"| {r['provider']} | {r['bias_med']:.3f} | {r['bias_mean']:.3f} | {r['trend']} | {r['slope_bias']:.4f} |")

    outpath.write_text("\n".join(lines), encoding="utf-8")
    print(f"[OK] Raport: {outpath}")

def main():
    ap = argparse.ArgumentParser(description="Analiza historyczna wielu summary CSV.")
    ap.add_argument("--summaries-dir", type=str, default="wyniki",
                    help="Katalog z plikami era5_comparison_summary*.csv")
    ap.add_argument("--outdir", type=str, default="wyniki/analysis_history",
                    help="Gdzie zapisać raport i wykresy")
    ap.add_argument("--top-n-per-var", type=int, default=6,
                    help="Ilu providerów pokazać na wykresach czasowych per zmienna")
    args = ap.parse_args()

    summaries_dir = Path(args.summaries_dir)
    outdir = Path(args.outdir)
    plots_dir = outdir / "plots"
    outdir.mkdir(parents=True, exist_ok=True)
    plots_dir.mkdir(parents=True, exist_ok=True)

    # 1) wczytaj wszystkie summary
    all_df = load_summaries(summaries_dir)

    # 2) zapisz surowy zlepek (dla audytu)
    all_df.to_csv(outdir / "history_all_runs_raw.csv", index=False)

    # 3) policz statystyki i trendy
    stats, all_df = summarize_history(all_df)
    stats.to_csv(outdir / "history_provider_stats.csv", index=False)

    # 4) wykresy
    make_plots(all_df, plots_dir, top_n_per_var=args.top_n_per_var)

    # 5) raport MD
    make_report(all_df, stats, plots_dir, outdir / "HISTORY_REPORT.md")

if __name__ == "__main__":
    main()
