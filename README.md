# 🌡️ Wet Bulb Globe Temperature (WBGT) Forecast Dashboard

Live 72-hour WBGT forecast across CONUS, powered by Open-Meteo.

> **WBGT is the heat metric the US military uses to decide if soldiers can train outside.**  
> It combines dry-bulb temperature, humidity, radiant heat, and wind — the four variables that determine whether human thermoregulation can keep up.

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://your-app-url.streamlit.app)

---

## What is WBGT?

| Flag | WBGT | Restriction |
|---|---|---|
| 🟢 Green | < 80°F | No restrictions |
| 🟡 Yellow | 80–84°F | Limit strenuous activity |
| 🔴 Red | 85–87°F | Limit heavy work to 10 min/hr |
| ⬛ Black | 88–89°F | No outdoor training |
| ⬛ Black+ | ≥ 90°F | Heat casualty risk |

*Source: US Army TB MED 507 / OSHA Heat Illness Prevention*

---

## Method

```
WBGT = 0.7 × Tnwb + 0.2 × Tg + 0.1 × Tdb
```

- **Tnwb** — Natural wet bulb temperature (Stull 2011 empirical formula)
- **Tg** — Globe temperature (Liljegren et al. 2008 simplified approximation)
- **Tdb** — Dry bulb (air) temperature

Forecast variables pulled from [Open-Meteo](https://open-meteo.com):
- `temperature_2m` — dry bulb temp (°C)
- `relativehumidity_2m` — relative humidity (%)
- `windspeed_10m` — wind speed (m/s)
- `shortwave_radiation` — incoming solar radiation (W/m²)

---

## Run Locally

```bash
git clone https://github.com/YOUR_USERNAME/wbgt-dashboard.git
cd wbgt-dashboard
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

The first run fetches ~500 grid points from Open-Meteo (~15–30 seconds).  
Subsequent runs load from `wbgt_cache.pkl` instantly (1-hour TTL).

---

## Deploy on Streamlit Cloud

1. Push this repo to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect your GitHub account
4. Select this repo → `app.py` → Deploy

No secrets or API keys needed. Open-Meteo is free and public.

---

## Files

```
wbgt-dashboard/
├── app.py              # Streamlit dashboard
├── wbgt_core.py        # WBGT physics + Open-Meteo fetch (shared)
├── wbgt_pipeline.py    # CLI pipeline → exports GeoJSON
├── requirements.txt
├── README.md
└── .gitignore
```

---

## CLI Pipeline (optional)

Exports `wbgt_forecast.geojson` for use in other tools (Deck.gl, QGIS, Mapbox):

```bash
python wbgt_pipeline.py                    # 72h forecast, CONUS
python wbgt_pipeline.py --hours 48
python wbgt_pipeline.py --output ./data
python wbgt_pipeline.py --refresh          # ignore cache
```

---

## References

- Stull, R. (2011). Wet-Bulb Temperature from Relative Humidity and Air Temperature. *Journal of Applied Meteorology and Climatology*, 50(11), 2267–2269.
- Liljegren, J.C., et al. (2008). Modeling the Wet Bulb Globe Temperature Using Standard Meteorological Measurements. *Journal of Occupational and Environmental Hygiene*, 5(10), 645–655.
- US Army TB MED 507 / AFPAM 48-152: Heat Stress Control and Heat Casualty Management
