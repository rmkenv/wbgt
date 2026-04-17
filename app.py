"""
app.py — Streamlit WBGT Dashboard
"""

import os
import time
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime, timezone

from wbgt_core import (
    build_grid, fetch_all_points, process_all, to_dataframe,
    FLAG_META, THRESHOLDS, CACHE_TTL, GRID_STEP,
)

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="WBGT Forecast — Wet Bulb Globe Temperature",
    page_icon="🌡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# STYLES
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Bebas+Neue&display=swap');

html, body, [class*="css"] {
    background-color: #0d0d0d !important;
    color: #e0d8c8 !important;
}

section[data-testid="stSidebar"] {
    background-color: #0a0a0a !important;
    border-right: 1px solid rgba(255,255,255,0.06) !important;
}

h1 { font-family: 'Bebas Neue', sans-serif !important; letter-spacing: 0.06em !important; }
h2, h3 { font-family: 'DM Mono', monospace !important; font-size: 12px !important;
          letter-spacing: 0.12em !important; color: #556 !important; }

.metric-card {
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 4px;
    padding: 12px 16px;
    text-align: center;
}
.metric-val { font-family: 'Bebas Neue', sans-serif; font-size: 32px; line-height: 1; }
.metric-lbl { font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: 0.12em; color: #445; margin-top: 2px; }

.flag-pill {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 2px;
    font-family: 'DM Mono', monospace;
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.1em;
}

div[data-testid="stMetric"] {
    background: rgba(255,255,255,0.03) !important;
    border: 1px solid rgba(255,255,255,0.07) !important;
    border-radius: 4px !important;
    padding: 10px 14px !important;
}
div[data-testid="stMetricValue"] { font-family: 'Bebas Neue', sans-serif !important; font-size: 28px !important; }
div[data-testid="stMetricLabel"] { font-family: 'DM Mono', monospace !important; font-size: 9px !important;
                                    letter-spacing: 0.1em !important; color: #556 !important; }

.stButton button {
    background: transparent !important;
    border: 1px solid rgba(255,255,255,0.15) !important;
    color: #778 !important;
    font-family: 'DM Mono', monospace !important;
    font-size: 10px !important;
    letter-spacing: 0.1em !important;
    border-radius: 2px !important;
}
.stButton button:hover { border-color: rgba(255,255,255,0.35) !important; color: #ccc !important; }

.stSelectbox, .stSlider { font-family: 'DM Mono', monospace !important; }

hr { border-color: rgba(255,255,255,0.07) !important; }

.stAlert { font-family: 'DM Mono', monospace !important; font-size: 11px !important; }

#MainMenu, footer, header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def load_wbgt(hours, grid_step, _bust=0):
    grid = build_grid(step=grid_step)
    raw, from_cache = fetch_all_points(
        grid, hours=hours, max_workers=20, refresh=(_bust > 0)
    )
    processed = process_all(raw, hours=hours)
    df = to_dataframe(processed)
    return processed, df, from_cache

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## ⚙️ CONFIG")
    st.markdown("---")

    hours = st.select_slider(
        "Forecast window",
        options=[24, 48, 72],
        value=72,
        help="Hours of Open-Meteo forecast to pull"
    )

    grid_step = st.select_slider(
        "Grid resolution",
        options=[2.0, 1.5, 1.0],
        value=1.5,
        format_func=lambda x: f"{x}° (~{int(x*111)}km)",
        help="Smaller = more grid points = slower fetch"
    )

    flag_filter = st.multiselect(
        "Show flags",
        options=["green", "yellow", "red", "black"],
        default=["green", "yellow", "red", "black"],
        format_func=lambda x: FLAG_META[x]["label"],
    )

    st.markdown("---")
    refresh_bust = st.session_state.get("refresh_bust", 0)
    if st.button("🔄 Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.session_state["refresh_bust"] = refresh_bust + 1
        st.rerun()

    st.markdown("---")
    st.markdown("""
<div style='font-family: DM Mono, monospace; font-size: 9px; color: #334; line-height: 1.8;'>
SOURCE<br>Open-Meteo free API<br>No API key required<br><br>
METHOD<br>Stull (2011) wet bulb<br>Liljegren (2008) globe temp<br>Outdoor WBGT formula<br><br>
THRESHOLDS<br>US Army TB MED 507<br>OSHA Heat Illness Prevention
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────────────────────────────────────

with st.spinner("Fetching forecast from Open-Meteo..."):
    t0 = time.time()
    processed, df, from_cache = load_wbgt(
        hours, grid_step, _bust=st.session_state.get("refresh_bust", 0)
    )
    elapsed = time.time() - t0

if flag_filter:
    df_vis = df[df["flag"].isin(flag_filter)].copy()
else:
    df_vis = df.copy()

# ─────────────────────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────────────────────

now_str = datetime.now(timezone.utc).strftime("%b %d, %Y %H:%M UTC")
cache_str = (
    f"cached ({int((time.time() - os.path.getmtime('wbgt_cache.pkl')) / 60)}m old)"
    if from_cache and os.path.exists("wbgt_cache.pkl")
    else f"fetched in {elapsed:.1f}s"
)

st.markdown(f"""
<div style='font-family: DM Mono, monospace; font-size: 9px; color: #445; letter-spacing: 0.15em; margin-bottom: 6px;'>
  {now_str} · {len(df)} GRID POINTS · {grid_step}° RES · {cache_str}
</div>
<h1 style='color: #f0e8d8; font-size: clamp(28px, 4vw, 48px); margin-bottom: 2px;'>
  WET BULB GLOBE TEMPERATURE
</h1>
<div style='font-family: DM Mono, monospace; font-size: 11px; color: #445; margin-bottom: 20px;'>
  The heat metric the military uses to decide if soldiers can train outside · {hours}h forecast · CONUS
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# METRICS ROW
# ─────────────────────────────────────────────────────────────────────────────

m1, m2, m3, m4, m5 = st.columns(5)

peak       = df["peak_wbgt_f"].max()
n_extreme  = int((df["peak_wbgt_f"] >= 90).sum())
n_high     = int(((df["peak_wbgt_f"] >= 85) & (df["peak_wbgt_f"] < 90)).sum())
n_caution  = int(((df["peak_wbgt_f"] >= 80) & (df["peak_wbgt_f"] < 85)).sum())
n_safe     = int((df["peak_wbgt_f"] < 80).sum())
peak_color = "#C0392B" if peak >= 90 else "#FF4757" if peak >= 85 else "#FFD32A" if peak >= 80 else "#2ED573"

with m1:
    st.markdown(f"<div class='metric-card'><div class='metric-val' style='color:{peak_color}'>{peak:.1f}°F</div><div class='metric-lbl'>CONUS PEAK WBGT</div></div>", unsafe_allow_html=True)
with m2:
    st.markdown(f"<div class='metric-card'><div class='metric-val' style='color:#C0392B'>{n_extreme}</div><div class='metric-lbl'>EXTREME ≥90°F</div></div>", unsafe_allow_html=True)
with m3:
    st.markdown(f"<div class='metric-card'><div class='metric-val' style='color:#FF4757'>{n_high}</div><div class='metric-lbl'>HIGH RISK 85–89°F</div></div>", unsafe_allow_html=True)
with m4:
    st.markdown(f"<div class='metric-card'><div class='metric-val' style='color:#FFD32A'>{n_caution}</div><div class='metric-lbl'>CAUTION 80–84°F</div></div>", unsafe_allow_html=True)
with m5:
    st.markdown(f"<div class='metric-card'><div class='metric-val' style='color:#2ED573'>{n_safe}</div><div class='metric-lbl'>SAFE &lt;80°F</div></div>", unsafe_allow_html=True)

st.markdown("<div style='margin-bottom:16px'></div>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# MAP + DETAIL PANEL
# ─────────────────────────────────────────────────────────────────────────────

map_col, detail_col = st.columns([3, 1], gap="small")

with map_col:
    fig_map = go.Figure()

    for flag in ["green", "yellow", "red", "black"]:
        sub = df_vis[df_vis["flag"] == flag]
        if sub.empty:
            continue
        meta = FLAG_META[flag]
        size = sub["peak_wbgt_f"].apply(
            lambda v: 14 if v >= 90 else 11 if v >= 85 else 8 if v >= 80 else 6
        )
        hover = (
            "<b>" + sub["flag_label"] + "</b><br>" +
            "Peak WBGT: " + sub["peak_wbgt_f"].round(1).astype(str) + "°F<br>" +
            "Now: " + sub["current_wbgt_f"].round(1).astype(str) + "°F<br>" +
            "Temp: " + sub["temp_c_now"].round(1).astype(str) + "°C · " +
            "RH: " + sub["rh_now"].round(0).astype(str) + "%<br>" +
            "Wind: " + sub["wind_ms_now"].round(1).astype(str) + " m/s · " +
            "Solar: " + sub["solar_now"].round(0).astype(str) + " W/m²<br>" +
            sub["lat"].round(1).astype(str) + "°N, " +
            sub["lon"].abs().round(1).astype(str) + "°W"
        )
        fig_map.add_trace(go.Scattergeo(
            lat=sub["lat"],
            lon=sub["lon"],
            mode="markers",
            name=f"{meta['label']} ({len(sub)})",
            marker=dict(
                color=meta["color"],
                size=size,
                opacity=0.95,
                line=dict(width=1.2, color="#0d0d0d"),
                symbol="circle",
            ),
            text=hover,
            hovertemplate="%{text}<extra></extra>",
            customdata=sub.index,
        ))

    fig_map.update_layout(
        geo=dict(
            scope="usa",
            bgcolor="#0d0d0d",
            landcolor="#1c1f26",
            subunitcolor="rgba(255,255,255,0.18)",
            showlakes=True,
            lakecolor="#0d1520",
            showrivers=True,
            rivercolor="rgba(60,130,200,0.45)",
            coastlinecolor="rgba(255,255,255,0.35)",
            countrycolor="rgba(255,255,255,0.18)",
            showsubunits=True,
            showcoastlines=True,
            showcountries=True,
            projection_type="albers usa",
            showframe=False,
            showocean=True,
            oceancolor="#0a1520",
        ),
        paper_bgcolor="#0d0d0d",
        plot_bgcolor="#0d0d0d",
        font=dict(family="DM Mono, monospace", color="#e0d8c8", size=10),
        height=500,
        margin=dict(l=0, r=0, t=0, b=0),
        legend=dict(
            orientation="h", y=-0.04, x=0,
            bgcolor="rgba(13,13,13,0.85)",
            bordercolor="rgba(255,255,255,0.15)",
            borderwidth=1,
            font=dict(size=10, family="DM Mono"),
        ),
    )

    # ── Render map and persist clicked point to session_state ──
    clicked = st.plotly_chart(
        fig_map,
        use_container_width=True,
        on_select="rerun",
        selection_mode="points",
        key="wbgt_map",
    )
    if clicked and clicked.get("selection") and clicked["selection"].get("points"):
        pt = clicked["selection"]["points"][0]
        st.session_state["selected_lat"] = round(pt["lat"], 1)
        st.session_state["selected_lon"] = round(pt["lon"], 1)

# ─────────────────────────────────────────────────────────────────────────────
# DETAIL PANEL — reads from session_state so it survives reruns
# ─────────────────────────────────────────────────────────────────────────────

with detail_col:
    sel_lat = st.session_state.get("selected_lat")
    sel_lon = st.session_state.get("selected_lon")
    sel_pt  = None

    if sel_lat is not None and sel_lon is not None:
        matches = df[
            (df["lat"].round(1) == sel_lat) &
            (df["lon"].round(1) == sel_lon)
        ]
        if not matches.empty:
            sel_pt = next(
                (p for p in processed
                 if round(p["lat"], 1) == sel_lat and round(p["lon"], 1) == sel_lon),
                None
            )

    if sel_pt is None:
        st.markdown("""
<div style='font-family: DM Mono, monospace; font-size: 10px; color: #334;
            text-align: center; padding: 40px 10px; line-height: 2;'>
  ◎<br><br>
  CLICK A POINT<br>on the map to inspect<br>its 72h WBGT forecast<br>
  and component variables
</div>""", unsafe_allow_html=True)

        st.markdown("---")
        st.markdown("<div style='font-family: DM Mono; font-size: 9px; color: #334; letter-spacing: 0.12em; margin-bottom: 8px;'>MILITARY THRESHOLDS</div>", unsafe_allow_html=True)
        for flag, meta in FLAG_META.items():
            thr     = meta["threshold"]
            thr_str = f"≥ {thr}°F" if thr > 0 else "< 80°F"
            st.markdown(f"""
<div style='display: flex; align-items: center; gap: 8px; margin-bottom: 6px;'>
  <div style='width: 8px; height: 8px; border-radius: 50%; background: {meta["color"]}; flex-shrink: 0;'></div>
  <div style='font-family: DM Mono; font-size: 9px; color: #556;'>{thr_str} — {meta["label"]}</div>
</div>""", unsafe_allow_html=True)

    else:
        flag_color = FLAG_META[sel_pt["flag"]]["color"]

        st.markdown(f"""
<div style='border-left: 3px solid {flag_color}; padding-left: 10px; margin-bottom: 12px;'>
  <div style='font-family: DM Mono; font-size: 9px; color: {flag_color}; letter-spacing: 0.12em; font-weight: 600;'>
    {sel_pt["flag_label"].upper()}
  </div>
  <div style='font-family: Bebas Neue; font-size: 28px; color: #f0e8d8; line-height: 1;'>
    {sel_pt["peak_wbgt_f"]}°F
  </div>
  <div style='font-family: DM Mono; font-size: 9px; color: #445; margin-top: 3px;'>
    {sel_pt["lat"]}°N, {abs(sel_pt["lon"])}°W · 72h peak
  </div>
</div>""", unsafe_allow_html=True)

        # Sparkline
        vals = sel_pt["series_3h"]
        fig_spark = go.Figure()
        fig_spark.add_trace(go.Scatter(
            x=list(range(len(vals))), y=vals,
            mode="lines",
            line=dict(color=flag_color, width=2),
            fill="tozeroy",
            fillcolor=flag_color + "22",
        ))
        for thr_val in [80, 85, 90]:
            fig_spark.add_hline(
                y=thr_val,
                line_dash="dot", line_color="rgba(255,255,255,0.2)", line_width=1,
                annotation_text=f"{thr_val}°",
                annotation_font=dict(size=8, color="#556"),
                annotation_position="right",
            )
        fig_spark.update_layout(
            paper_bgcolor="#0d0d0d", plot_bgcolor="#0d0d0d",
            height=110, margin=dict(l=0, r=24, t=4, b=20),
            xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
            yaxis=dict(showgrid=False, tickfont=dict(size=8, color="#334"),
                       zeroline=False, ticksuffix="°"),
            showlegend=False,
        )
        st.markdown("<div style='font-family: DM Mono; font-size: 9px; color: #334; letter-spacing: 0.1em; margin-bottom: 4px;'>72H WBGT TREND</div>", unsafe_allow_html=True)
        st.plotly_chart(fig_spark, use_container_width=True, config={"displayModeBar": False})

        # Hours above threshold
        st.markdown("<div style='font-family: DM Mono; font-size: 9px; color: #334; letter-spacing: 0.1em; margin: 8px 0 6px;'>HOURS ABOVE THRESHOLD</div>", unsafe_allow_html=True)
        for label, key, color in [
            ("≥ 80°F Caution", "hrs_caution", "#FFD32A"),
            ("≥ 85°F High",    "hrs_high",    "#FF4757"),
            ("≥ 90°F Extreme", "hrs_extreme", "#C0392B"),
        ]:
            val = sel_pt[key]
            pct = min(100, val / 72 * 100)
            st.markdown(f"""
<div style='margin-bottom: 6px;'>
  <div style='display: flex; justify-content: space-between; font-family: DM Mono; font-size: 9px; color: #556; margin-bottom: 3px;'>
    <span>{label}</span><span style='color:{color}'>{val}h</span>
  </div>
  <div style='height: 4px; background: rgba(255,255,255,0.05); border-radius: 2px;'>
    <div style='width:{pct}%; height:100%; background:{color}; border-radius:2px;'></div>
  </div>
</div>""", unsafe_allow_html=True)

        # Component variables
        st.markdown("<div style='font-family: DM Mono; font-size: 9px; color: #334; letter-spacing: 0.1em; margin: 10px 0 6px;'>CURRENT INPUTS</div>", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            st.metric("Temp", f"{sel_pt['temp_c_now']}°C")
            st.metric("Wind", f"{sel_pt['wind_ms_now']} m/s")
        with c2:
            st.metric("RH", f"{sel_pt['rh_now']}%")
            st.metric("Solar", f"{sel_pt['solar_now']} W/m²")

        st.markdown("""
<div style='font-family: DM Mono; font-size: 9px; color: #334; margin-top: 10px;
            background: rgba(255,255,255,0.02); border: 1px solid rgba(255,255,255,0.05);
            border-radius: 3px; padding: 8px 10px; line-height: 1.9;'>
  WBGT = 0.7·Tnwb + 0.2·Tg + 0.1·Tdb
</div>""", unsafe_allow_html=True)

        if st.button("✕ Clear selection", use_container_width=True):
            del st.session_state["selected_lat"]
            del st.session_state["selected_lon"]
            st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# BOTTOM: DISTRIBUTION + TOP 10 TABLE
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("---")
chart_col, table_col = st.columns([2, 1], gap="medium")

with chart_col:
    st.markdown("### WBGT DISTRIBUTION ACROSS CONUS")
    fig_hist = go.Figure()
    fig_hist.add_trace(go.Histogram(
        x=df["peak_wbgt_f"],
        nbinsx=30,
        marker=dict(
            color=df["peak_wbgt_f"],
            colorscale=[
                [0.0,  "#2ED573"],
                [0.43, "#FFD32A"],
                [0.57, "#FF4757"],
                [1.0,  "#C0392B"],
            ],
            cmin=65, cmax=100,
            line=dict(width=0),
        ),
        opacity=0.85,
        hovertemplate="WBGT %{x}°F<br>Count: %{y}<extra></extra>",
    ))
    for thr, color, label in [(80, "#FFD32A", "Caution"), (85, "#FF4757", "High"), (90, "#C0392B", "Extreme")]:
        fig_hist.add_vline(x=thr, line_dash="dash", line_color=color, line_width=1.5,
                           annotation_text=label, annotation_position="top right",
                           annotation_font=dict(size=9, color=color))
    fig_hist.update_layout(
        paper_bgcolor="#0d0d0d", plot_bgcolor="#0d0d0d",
        height=260, margin=dict(l=0, r=0, t=10, b=0),
        font=dict(family="DM Mono", color="#556", size=10),
        xaxis=dict(title="Peak WBGT (°F)", gridcolor="rgba(255,255,255,0.05)", showgrid=True),
        yaxis=dict(title="Grid points",    gridcolor="rgba(255,255,255,0.05)", showgrid=True),
        bargap=0.05,
    )
    st.plotly_chart(fig_hist, use_container_width=True, config={"displayModeBar": False})

with table_col:
    st.markdown("### HOTTEST GRID POINTS")
    top10 = df.nlargest(10, "peak_wbgt_f")[
        ["lat", "lon", "peak_wbgt_f", "flag_label", "hrs_extreme"]
    ].copy()
    top10.columns = ["Lat", "Lon", "Peak °F", "Flag", "Hrs ≥90°F"]
    top10["Lat"]     = top10["Lat"].round(1)
    top10["Lon"]     = top10["Lon"].round(1)
    top10["Peak °F"] = top10["Peak °F"].round(1)
    st.dataframe(top10, use_container_width=True, height=260, hide_index=True)
