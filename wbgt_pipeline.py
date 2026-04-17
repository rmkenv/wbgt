"""
Wet Bulb Globe Temperature (WBGT) Forecast Pipeline
=====================================================
Pulls hourly forecast data from Open-Meteo (free, no key required),
computes WBGT using the Bernard (1999) / Liljegren approximation,
and exports a GeoJSON grid for the React dashboard.

WBGT combines: dry-bulb temp + humidity + radiant heat + wind speed
It's the metric used by the US military, OSHA, and sports medicine
to determine safe outdoor work/activity thresholds.

Thresholds (military):
  < 80°F  — no restrictions
  80–84°F — flag yellow: limit strenuous activity
  85–87°F — flag red: limit heavy work to 10 min/hr
  88–89°F — flag black: no outdoor training
  ≥ 90°F  — flag black: heat casualty risk

Usage:
  python wbgt_pipeline.py                    # 72-hour forecast, CONUS grid
  python wbgt_pipeline.py --hours 48
  python wbgt_pipeline.py --output ./data
  python wbgt_pipeline.py --refresh          # ignore cache
"""

import os
import sys
import json
import time
import pickle
import argparse
import warnings
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice

import requests
import numpy as np
import pandas as pd
from tqdm import tqdm

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# GRID CONFIG — CONUS at ~1° resolution (~110km)
# For finer resolution increase GRID_STEP (uses more API calls)
# ─────────────────────────────────────────────────────────────────────────────

GRID_STEP   = 1.5          # degrees; 1.5° ≈ ~165km, gives ~500 grid points
LAT_RANGE   = (24.5, 49.5) # CONUS lat
LON_RANGE   = (-124.5, -66.5) # CONUS lon
MAX_WORKERS = 20           # concurrent Open-Meteo requests
CACHE_FILE  = "wbgt_cache.pkl"
CACHE_TTL   = 3600         # seconds — re-fetch if cache older than 1 hour

# ─────────────────────────────────────────────────────────────────────────────
# WBGT COMPUTATION
# ─────────────────────────────────────────────────────────────────────────────

def dewpoint(T_c, rh):
    """Magnus formula dewpoint in °C from temp (°C) and relative humidity (%)."""
    a, b = 17.625, 243.04
    rh = np.clip(rh, 0.01, 100)
    gamma = np.log(rh / 100) + a * T_c / (b + T_c)
    return b * gamma / (a - gamma)


def wet_bulb_temp(T_c, rh):
    """
    Stull (2011) empirical wet bulb temperature (°C).
    Valid for T in [-20, 50]°C and RH in [5, 99]%.
    """
    rh = np.clip(rh, 5, 99)
    Tw = (T_c * np.arctan(0.151977 * (rh + 8.313659) ** 0.5)
          + np.arctan(T_c + rh)
          - np.arctan(rh - 1.676331)
          + 0.00391838 * rh ** 1.5 * np.arctan(0.023101 * rh)
          - 4.686035)
    return Tw


def globe_temp(T_c, rh, wind_ms, shortwave_wm2):
    """
    Simplified globe temperature (°C) — approximates black globe thermometer.
    Uses solar radiation and wind correction per Liljegren et al. (2008).
    """
    # Natural wet bulb (accounts for evaporation in wind)
    Tw = wet_bulb_temp(T_c, rh)
    # Globe temp approximation
    solar_factor = 0.0014 * np.clip(shortwave_wm2, 0, 1200)
    wind_factor  = 1.1 * np.clip(wind_ms, 0.5, 20) ** (-0.25)
    Tg = T_c + solar_factor / wind_factor
    return Tg


def compute_wbgt(T_c, rh, wind_ms, shortwave_wm2):
    """
    Outdoor WBGT (°C) = 0.7 * Tnwb + 0.2 * Tg + 0.1 * Tdb
    Where:
      Tnwb = natural wet bulb temp
      Tg   = globe temp (radiant heat)
      Tdb  = dry bulb temp
    """
    T_c       = np.asarray(T_c, dtype=float)
    rh        = np.asarray(rh, dtype=float)
    wind_ms   = np.asarray(wind_ms, dtype=float)
    sw        = np.asarray(shortwave_wm2, dtype=float)

    Tnwb = wet_bulb_temp(T_c, rh)
    Tg   = globe_temp(T_c, rh, wind_ms, sw)
    wbgt = 0.7 * Tnwb + 0.2 * Tg + 0.1 * T_c
    return wbgt


def wbgt_c_to_f(wbgt_c):
    return wbgt_c * 9/5 + 32


def wbgt_flag(wbgt_f):
    """Return military/OSHA flag color and label."""
    if wbgt_f < 80:
        return "green",  "No restrictions"
    elif wbgt_f < 85:
        return "yellow", "Caution"
    elif wbgt_f < 88:
        return "red",    "High risk"
    elif wbgt_f < 90:
        return "black",  "Very high risk"
    else:
        return "black",  "Extreme / no outdoor activity"


FLAG_COLORS = {
    "green":  "#2ED573",
    "yellow": "#FFD32A",
    "red":    "#FF4757",
    "black":  "#C0392B",
}

# ─────────────────────────────────────────────────────────────────────────────
# OPEN-METEO FETCH
# ─────────────────────────────────────────────────────────────────────────────

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

def fetch_point(lat, lon, hours=72):
    """
    Pull hourly forecast for one grid point from Open-Meteo.
    Returns dict with arrays for each variable, or None on failure.
    """
    params = {
        "latitude":              lat,
        "longitude":             lon,
        "hourly":                "temperature_2m,relativehumidity_2m,windspeed_10m,shortwave_radiation",
        "wind_speed_unit":       "ms",
        "forecast_days":         max(1, hours // 24 + 1),
        "timezone":              "UTC",
    }
    try:
        r = requests.get(OPEN_METEO_URL, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        h = data["hourly"]
        n = min(hours, len(h["time"]))
        return {
            "lat":       lat,
            "lon":       lon,
            "time":      h["time"][:n],
            "temp_c":    np.array(h["temperature_2m"][:n], dtype=float),
            "rh":        np.array(h["relativehumidity_2m"][:n], dtype=float),
            "wind_ms":   np.array(h["windspeed_10m"][:n], dtype=float),
            "solar_wm2": np.array(h["shortwave_radiation"][:n], dtype=float),
        }
    except Exception:
        return None


def build_grid():
    """Generate (lat, lon) grid points covering CONUS."""
    lats = np.arange(LAT_RANGE[0], LAT_RANGE[1], GRID_STEP)
    lons = np.arange(LON_RANGE[0], LON_RANGE[1], GRID_STEP)
    return [(round(float(la), 2), round(float(lo), 2))
            for la in lats for lo in lons]


def fetch_all_points(grid, hours=72, cache_file=CACHE_FILE, refresh=False):
    """Fetch forecast data for all grid points, with disk cache."""
    if not refresh and os.path.exists(cache_file):
        age = time.time() - os.path.getmtime(cache_file)
        if age < CACHE_TTL:
            with open(cache_file, "rb") as f:
                cache = pickle.load(f)
            print(f"  ✓ Loaded {len(cache)} points from cache "
                  f"({age/60:.0f} min old — refresh after {CACHE_TTL//60} min)")
            return cache

    print(f"  Fetching {len(grid)} grid points from Open-Meteo ({hours}h forecast)...")
    results = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_point, lat, lon, hours): (lat, lon)
                   for lat, lon in grid}
        pbar = tqdm(as_completed(futures), total=len(futures),
                    desc="  Grid pts", unit="pt", ncols=72)
        for fut in pbar:
            data = fut.result()
            if data:
                key = (data["lat"], data["lon"])
                results[key] = data
            pbar.set_postfix(ok=len(results))

    with open(cache_file, "wb") as f:
        pickle.dump(results, f)
    print(f"  ✓ {len(results)}/{len(grid)} points fetched → cached to {cache_file}")
    return results


# ─────────────────────────────────────────────────────────────────────────────
# WBGT COMPUTATION PER GRID POINT
# ─────────────────────────────────────────────────────────────────────────────

def process_grid_point(data, hours=72):
    """
    Compute WBGT time series for one grid point.
    Returns dict of per-hour WBGT values plus summary stats.
    """
    T    = data["temp_c"]
    rh   = data["rh"]
    wind = data["wind_ms"]
    sw   = data["solar_wm2"]

    # Fill NaNs
    for arr in [T, rh, wind, sw]:
        mask = np.isnan(arr)
        if mask.any():
            arr[mask] = np.nanmedian(arr)

    wbgt_c = compute_wbgt(T, rh, wind, sw)
    wbgt_f = wbgt_c_to_f(wbgt_c)

    # Peak in next 72h
    peak_f   = float(np.nanmax(wbgt_f))
    peak_idx = int(np.nanargmax(wbgt_f))
    peak_hr  = data["time"][peak_idx] if peak_idx < len(data["time"]) else ""

    flag, label = wbgt_flag(peak_f)

    # Hours above each threshold
    hrs_caution   = int(np.sum(wbgt_f >= 80))
    hrs_high       = int(np.sum(wbgt_f >= 85))
    hrs_veryhigh   = int(np.sum(wbgt_f >= 88))
    hrs_extreme    = int(np.sum(wbgt_f >= 90))

    # Current (hour 0)
    current_f = float(wbgt_f[0]) if len(wbgt_f) > 0 else 0.0

    # 72-hour series for sparkline (every 3 hours)
    series = [round(float(v), 1) for v in wbgt_f[::3]]

    return {
        "lat":           data["lat"],
        "lon":           data["lon"],
        "current_wbgt_f": round(current_f, 1),
        "peak_wbgt_f":   round(peak_f, 1),
        "peak_wbgt_c":   round(float(np.nanmax(wbgt_c)), 1),
        "peak_time":     peak_hr,
        "flag":          flag,
        "flag_label":    label,
        "flag_color":    FLAG_COLORS[flag],
        "hrs_caution":   hrs_caution,
        "hrs_high":      hrs_high,
        "hrs_veryhigh":  hrs_veryhigh,
        "hrs_extreme":   hrs_extreme,
        "temp_c_now":    round(float(T[0]), 1),
        "rh_now":        round(float(rh[0]), 1),
        "wind_ms_now":   round(float(wind[0]), 1),
        "solar_now":     round(float(sw[0]), 1),
        "series_3h":     series,
        "series_times":  data["time"][::3],
    }


# ─────────────────────────────────────────────────────────────────────────────
# GEOJSON EXPORT
# ─────────────────────────────────────────────────────────────────────────────

def export_geojson(processed, output_dir="."):
    os.makedirs(output_dir, exist_ok=True)

    features = []
    for pt in processed:
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [pt["lon"], pt["lat"]]
            },
            "properties": {k: v for k, v in pt.items()
                           if k not in ("lat", "lon", "series_times")}
        })

    # Sort by peak WBGT descending so highest risk renders on top
    features.sort(key=lambda f: f["properties"]["peak_wbgt_f"], reverse=True)

    geojson = {
        "type": "FeatureCollection",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "features": features,
    }

    path = os.path.join(output_dir, "wbgt_forecast.geojson")
    with open(path, "w") as f:
        json.dump(geojson, f)

    # Also export compact summary JSON for dashboard metadata
    meta = {
        "generated_at":    datetime.now(timezone.utc).isoformat(),
        "n_points":        len(features),
        "grid_step_deg":   GRID_STEP,
        "forecast_hours":  72,
        "peak_wbgt_f":     round(max(f["properties"]["peak_wbgt_f"] for f in features), 1),
        "pct_caution":     round(100 * sum(1 for f in features if f["properties"]["flag"] != "green") / len(features), 1),
        "pct_high":        round(100 * sum(1 for f in features if f["properties"]["peak_wbgt_f"] >= 85) / len(features), 1),
        "pct_extreme":     round(100 * sum(1 for f in features if f["properties"]["peak_wbgt_f"] >= 90) / len(features), 1),
        "thresholds": {
            "caution":   80,
            "high":      85,
            "very_high": 88,
            "extreme":   90,
        },
        "flag_colors": FLAG_COLORS,
    }
    meta_path = os.path.join(output_dir, "wbgt_meta.json")
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)

    return path, meta_path, meta


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="WBGT Forecast Pipeline — Open-Meteo → GeoJSON",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python wbgt_pipeline.py
  python wbgt_pipeline.py --hours 48
  python wbgt_pipeline.py --output ./data
  python wbgt_pipeline.py --refresh
        """
    )
    parser.add_argument("--hours",   type=int, default=72,  help="Forecast hours (default: 72)")
    parser.add_argument("--output",  type=str, default=".", help="Output directory")
    parser.add_argument("--refresh", action="store_true",   help="Ignore cache, re-fetch")
    args = parser.parse_args()

    t0 = time.time()
    print("=" * 60)
    print("  WET BULB GLOBE TEMPERATURE FORECAST PIPELINE")
    print(f"  hours={args.hours}  grid={GRID_STEP}°  output={args.output}")
    print("  Source: Open-Meteo (free, no API key)")
    print("=" * 60)

    grid = build_grid()
    print(f"\n[1/3] Grid: {len(grid)} points at {GRID_STEP}° resolution")

    raw = fetch_all_points(grid, hours=args.hours,
                           refresh=args.refresh)

    print(f"\n[2/3] Computing WBGT for {len(raw)} grid points...")
    processed = []
    for data in tqdm(raw.values(), desc="  WBGT", unit="pt", ncols=72):
        pt = process_grid_point(data, hours=args.hours)
        processed.append(pt)

    print(f"\n[3/3] Exporting GeoJSON → {args.output}/")
    geojson_path, meta_path, meta = export_geojson(processed, output_dir=args.output)

    print(f"\n  ✓ wbgt_forecast.geojson  ({len(processed)} grid points)")
    print(f"  ✓ wbgt_meta.json")
    print(f"\n  Peak WBGT in CONUS: {meta['peak_wbgt_f']}°F")
    print(f"  Grid pts above caution (≥80°F): {meta['pct_caution']}%")
    print(f"  Grid pts high risk (≥85°F):     {meta['pct_high']}%")
    print(f"  Grid pts extreme (≥90°F):        {meta['pct_extreme']}%")
    print(f"\n  Total time: {time.time()-t0:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
