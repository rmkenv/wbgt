"""
wbgt_core.py
============
Shared computation and data-fetch logic.
Imported by both the CLI pipeline and the Streamlit app.
No Streamlit imports here — pure Python.
"""

import os
import time
import pickle
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# GRID
# ─────────────────────────────────────────────────────────────────────────────

GRID_STEP  = 1.5
LAT_RANGE  = (24.5, 49.5)
LON_RANGE  = (-124.5, -66.5)
CACHE_FILE = "wbgt_cache.pkl"
CACHE_TTL  = 3600   # 1 hour

FLAG_META = {
    "green":  {"label": "No Restriction", "color": "#2ED573", "threshold": 0},
    "yellow": {"label": "Caution",         "color": "#FFD32A", "threshold": 80},
    "red":    {"label": "High Risk",       "color": "#FF4757", "threshold": 85},
    "black":  {"label": "Extreme",         "color": "#C0392B", "threshold": 88},
}

THRESHOLDS = {
    "caution":   80,
    "high":      85,
    "very_high": 88,
    "extreme":   90,
}


def build_grid(step=GRID_STEP):
    lats = np.arange(LAT_RANGE[0], LAT_RANGE[1], step)
    lons = np.arange(LON_RANGE[0], LON_RANGE[1], step)
    return [(round(float(la), 2), round(float(lo), 2))
            for la in lats for lo in lons]


# ─────────────────────────────────────────────────────────────────────────────
# WBGT PHYSICS
# ─────────────────────────────────────────────────────────────────────────────

def wet_bulb_temp(T_c, rh):
    """Stull (2011) empirical wet bulb temperature (°C)."""
    rh = np.clip(rh, 5, 99)
    return (T_c * np.arctan(0.151977 * (rh + 8.313659) ** 0.5)
            + np.arctan(T_c + rh)
            - np.arctan(rh - 1.676331)
            + 0.00391838 * rh ** 1.5 * np.arctan(0.023101 * rh)
            - 4.686035)


def globe_temp(T_c, rh, wind_ms, shortwave_wm2):
    """Simplified outdoor globe temperature (°C) per Liljegren et al. (2008)."""
    solar_factor = 0.0014 * np.clip(shortwave_wm2, 0, 1200)
    wind_factor  = 1.1 * np.clip(wind_ms, 0.5, 20) ** (-0.25)
    return T_c + solar_factor / wind_factor


def compute_wbgt(T_c, rh, wind_ms, shortwave_wm2):
    """
    Outdoor WBGT (°C) = 0.7 * Tnwb + 0.2 * Tg + 0.1 * Tdb
    """
    T_c   = np.asarray(T_c,   dtype=float)
    rh    = np.asarray(rh,    dtype=float)
    wind  = np.asarray(wind_ms, dtype=float)
    sw    = np.asarray(shortwave_wm2, dtype=float)
    Tnwb  = wet_bulb_temp(T_c, rh)
    Tg    = globe_temp(T_c, rh, wind, sw)
    return 0.7 * Tnwb + 0.2 * Tg + 0.1 * T_c


def c_to_f(c): return c * 9 / 5 + 32


def wbgt_flag(wbgt_f):
    if wbgt_f >= 88: return "black"
    if wbgt_f >= 85: return "red"
    if wbgt_f >= 80: return "yellow"
    return "green"


# ─────────────────────────────────────────────────────────────────────────────
# OPEN-METEO FETCH
# ─────────────────────────────────────────────────────────────────────────────

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_point(lat, lon, hours=72):
    params = {
        "latitude":        lat,
        "longitude":       lon,
        "hourly":          "temperature_2m,relativehumidity_2m,windspeed_10m,shortwave_radiation",
        "wind_speed_unit": "ms",
        "forecast_days":   max(1, hours // 24 + 1),
        "timezone":        "UTC",
    }
    try:
        r = requests.get(OPEN_METEO_URL, params=params, timeout=15)
        r.raise_for_status()
        h = r.json()["hourly"]
        n = min(hours, len(h["time"]))
        return {
            "lat":       lat,
            "lon":       lon,
            "time":      h["time"][:n],
            "temp_c":    np.array(h["temperature_2m"][:n],       dtype=float),
            "rh":        np.array(h["relativehumidity_2m"][:n],  dtype=float),
            "wind_ms":   np.array(h["windspeed_10m"][:n],        dtype=float),
            "solar_wm2": np.array(h["shortwave_radiation"][:n],  dtype=float),
        }
    except Exception as e:
        logger.debug(f"fetch_point({lat},{lon}): {e}")
        return None


def fetch_all_points(grid, hours=72, max_workers=20,
                     cache_file=CACHE_FILE, refresh=False,
                     progress_cb=None):
    """
    Fetch all grid points from Open-Meteo with disk cache.
    progress_cb(done, total) called after each completed future (for Streamlit progress bars).
    """
    if not refresh and os.path.exists(cache_file):
        age = time.time() - os.path.getmtime(cache_file)
        if age < CACHE_TTL:
            with open(cache_file, "rb") as f:
                return pickle.load(f), True   # (cache, from_cache)

    results = {}
    total   = len(grid)
    done    = 0

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_point, lat, lon, hours): (lat, lon)
                   for lat, lon in grid}
        for fut in as_completed(futures):
            data = fut.result()
            if data:
                results[(data["lat"], data["lon"])] = data
            done += 1
            if progress_cb:
                progress_cb(done, total)

    with open(cache_file, "wb") as f:
        pickle.dump(results, f)

    return results, False


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE COMPUTATION
# ─────────────────────────────────────────────────────────────────────────────

def process_point(data, hours=72):
    T    = data["temp_c"].copy()
    rh   = data["rh"].copy()
    wind = data["wind_ms"].copy()
    sw   = data["solar_wm2"].copy()

    for arr in [T, rh, wind, sw]:
        mask = np.isnan(arr)
        if mask.any():
            med = np.nanmedian(arr)
            arr[mask] = med if not np.isnan(med) else 0.0

    wbgt_c = compute_wbgt(T, rh, wind, sw)
    wbgt_f = c_to_f(wbgt_c)

    peak_f   = float(np.nanmax(wbgt_f))
    peak_idx = int(np.nanargmax(wbgt_f))
    peak_hr  = data["time"][peak_idx] if peak_idx < len(data["time"]) else ""
    flag     = wbgt_flag(peak_f)

    series_f = [round(float(v), 1) for v in wbgt_f[::3]]
    times_3h = data["time"][::3]

    return {
        "lat":               data["lat"],
        "lon":               data["lon"],
        "current_wbgt_f":    round(float(wbgt_f[0]), 1),
        "current_wbgt_c":    round(float(wbgt_c[0]), 1),
        "peak_wbgt_f":       round(peak_f, 1),
        "peak_wbgt_c":       round(float(np.nanmax(wbgt_c)), 1),
        "peak_time":         peak_hr,
        "flag":              flag,
        "flag_label":        FLAG_META[flag]["label"],
        "flag_color":        FLAG_META[flag]["color"],
        "hrs_caution":       int(np.sum(wbgt_f >= THRESHOLDS["caution"])),
        "hrs_high":          int(np.sum(wbgt_f >= THRESHOLDS["high"])),
        "hrs_very_high":     int(np.sum(wbgt_f >= THRESHOLDS["very_high"])),
        "hrs_extreme":       int(np.sum(wbgt_f >= THRESHOLDS["extreme"])),
        "temp_c_now":        round(float(T[0]), 1),
        "rh_now":            round(float(rh[0]), 1),
        "wind_ms_now":       round(float(wind[0]), 1),
        "solar_now":         round(float(sw[0]), 1),
        "series_3h":         series_f,
        "series_times":      list(times_3h),
    }


def process_all(raw, hours=72):
    return [process_point(d, hours) for d in raw.values()]


def to_dataframe(processed):
    rows = []
    for p in processed:
        rows.append({k: v for k, v in p.items()
                     if k not in ("series_3h", "series_times")})
    return pd.DataFrame(rows)
