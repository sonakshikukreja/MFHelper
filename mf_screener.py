#!/usr/bin/env python3
"""
Mutual Fund Screening & Ranking Tool
=====================================
Screens and ranks mutual funds based on multi-metric evaluation:
  - Rolling Returns (Consistency)
  - Sharpe Ratio
  - Beta
  - Upside / Downside Capture Ratios
  - Benchmark Outperformance %
  - Total Expense Ratio (TER)
  - AUM (display only)

Usage:
  python mf_screener.py --category "Equity Scheme - Large Cap Fund"
  python mf_screener.py --category "Equity Scheme - Flexi Cap Fund" --top_n 15 --lookback 5
"""

import argparse
import json
import logging
import math
import os
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from jinja2 import Template
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

def load_config():
    config_path = "config.json"
    default = {
        "api": {"base_url": "https://api.mfapi.in"},
        "persistence": {"reports_dir": "Reports", "logs_dir": "Logs"},
    }
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                user_cfg = json.load(f)
                for k in user_cfg:
                    if k in default and isinstance(default[k], dict):
                        default[k].update(user_cfg[k])
                    else:
                        default[k] = user_cfg[k]
        except Exception as e:
            print(f"Warning: config load failed: {e}")
    return default

CONFIG = load_config()
MFAPI_BASE = CONFIG["api"]["base_url"]
REPORTS_DIR = CONFIG["persistence"]["reports_dir"]
LOGS_DIR = CONFIG["persistence"].get("logs_dir", "Logs")

# Logging
os.makedirs(LOGS_DIR, exist_ok=True)
log_file = os.path.join(LOGS_DIR, f"screener_{datetime.now():%d-%b-%Y_%H.%M}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_file)],
)

# Benchmark map
BENCHMARK_MAP = {}
bm_path = os.path.join(os.path.dirname(__file__) or ".", "benchmark_map.json")
if os.path.exists(bm_path):
    with open(bm_path, "r") as f:
        BENCHMARK_MAP = json.load(f)

# Default scoring weights (from BRD)
DEFAULT_WEIGHTS = {
    "rolling_consistency": 0.25,
    "sharpe": 0.20,
    "upside_capture": 0.15,
    "downside_capture": 0.15,
    "benchmark_outperf": 0.10,
    "beta_stability": 0.05,
    "ter": 0.10,
}

# ─────────────────────────────────────────────────────────────
# HTTP Session
# ─────────────────────────────────────────────────────────────

def create_session():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries, pool_maxsize=20))
    s.mount("http://", HTTPAdapter(max_retries=retries, pool_maxsize=20))
    return s

# ─────────────────────────────────────────────────────────────
# Data Fetching
# ─────────────────────────────────────────────────────────────

def fetch_all_schemes(session):
    """Fetch the full list of mutual fund schemes from MFAPI."""
    all_schemes = []
    offset = 0
    batch = 1000
    while True:
        try:
            resp = session.get(f"{MFAPI_BASE}/mf?limit={batch}&offset={offset}")
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logging.error(f"Error fetching schemes at offset {offset}: {e}")
            break
        if not data:
            break
        all_schemes.extend(data)
        if len(data) < batch:
            break
        offset += batch
    logging.info(f"Total schemes fetched: {len(all_schemes)}")
    return all_schemes


def fetch_fund_list(session, category, fund_limit=50):
    """
    Filter the MFAPI scheme list to Direct-Growth funds in the given category.
    Returns up to fund_limit scheme dicts with metadata.
    """
    all_schemes = fetch_all_schemes(session)
    candidates = []
    for s in all_schemes:
        name = s.get("schemeName", "")
        code = s.get("schemeCode")
        # Only Direct Growth plans
        name_lower = name.lower()
        if "direct" not in name_lower:
            continue
        if "growth" not in name_lower:
            continue
        # Skip dividend / IDCW
        if "idcw" in name_lower or "dividend" in name_lower:
            continue
        candidates.append({"schemeCode": code, "schemeName": name})

    # Now fetch metadata for each to filter by category
    # To keep it fast, we'll do a quick metadata check via the MFAPI
    # But MFAPI requires a full NAV fetch to get metadata, which is expensive.
    # Instead, we'll fetch metadata for a random sample first, or fetch all and cache.
    # OPTIMIZATION: Fetch metadata lazily during NAV fetch.
    logging.info(f"Found {len(candidates)} Direct-Growth schemes. Will filter by category during NAV fetch.")
    return candidates[:fund_limit * 10]  # Over-fetch to account for category filtering


def fetch_full_nav_history(session, scheme_code, lookback_years=5):
    """
    Fetch full NAV history for a scheme, return DataFrame with date + nav columns
    filtered to the lookback period.
    """
    url = f"{MFAPI_BASE}/mf/{scheme_code}"
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logging.debug(f"Error fetching NAV for {scheme_code}: {e}")
        return None, None

    if not data or "data" not in data or not data["data"]:
        return None, None

    meta = data.get("meta", {})
    navs = pd.DataFrame(data["data"])
    navs["date"] = pd.to_datetime(navs["date"], dayfirst=True)
    navs["nav"] = pd.to_numeric(navs["nav"], errors="coerce")
    navs = navs.dropna(subset=["nav"])
    navs = navs[navs["nav"] > 0]
    navs = navs.sort_values("date").reset_index(drop=True)

    # Filter to lookback period
    cutoff = datetime.now() - timedelta(days=lookback_years * 365)
    navs = navs[navs["date"] >= cutoff]

    if len(navs) < 60:  # Need at least ~60 data points
        return None, meta

    return navs, meta


def fetch_benchmark_nav(session, category, lookback_years=5):
    """Fetch benchmark NAV history using the benchmark map."""
    bm_code = BENCHMARK_MAP.get(category, BENCHMARK_MAP.get("default"))
    if not bm_code:
        logging.warning(f"No benchmark mapping for category: {category}")
        return None

    logging.info(f"Fetching benchmark NAV (scheme code: {bm_code}) for category: {category}")
    navs, _ = fetch_full_nav_history(session, bm_code, lookback_years)
    return navs


def fetch_kuvera_metadata(session, isin):
    """Fetch TER, AUM, and other metadata from Kuvera API."""
    if not isin:
        return {}
    try:
        resp = session.get(f"https://mf.captnemo.in/kuvera/{isin}", timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and data:
            data = data[0]
        return {
            "expense_ratio": float(data.get("expense_ratio", 0) or 0),
            "aum": float(data.get("aum", 0) or 0),
            "fund_manager": data.get("fund_manager", ""),
            "crisil_rating": data.get("crisil_rating", ""),
        }
    except Exception:
        return {}


# ─────────────────────────────────────────────────────────────
# Metrics Calculation
# ─────────────────────────────────────────────────────────────

def compute_monthly_returns(navs_df):
    """Convert daily NAV to monthly returns series."""
    df = navs_df.copy()
    df = df.set_index("date")
    # Resample to month-end, take last NAV
    monthly = df["nav"].resample("ME").last().dropna()
    returns = monthly.pct_change().dropna()
    return returns


def calculate_rolling_returns(navs_df, window_years=3):
    """
    Calculate rolling CAGR over a fixed window.
    Returns: mean_rolling, std_rolling, consistency_score
    """
    df = navs_df.copy().set_index("date").sort_index()
    nav_series = df["nav"]

    window_days = int(window_years * 365)
    rolling_cagrs = []

    dates = nav_series.index
    for i in range(len(dates)):
        start_date = dates[i]
        end_date = start_date + pd.DateOffset(years=window_years)
        # Find closest date
        future = nav_series[nav_series.index >= end_date]
        if future.empty:
            break
        end_nav = future.iloc[0]
        start_nav = nav_series.iloc[i]
        if start_nav <= 0:
            continue
        actual_days = (future.index[0] - start_date).days
        if actual_days < 300:  # At least ~10 months
            continue
        years = actual_days / 365.25
        cagr = (end_nav / start_nav) ** (1 / years) - 1
        rolling_cagrs.append(cagr)

    if not rolling_cagrs:
        return 0, 0, 0

    mean_r = np.mean(rolling_cagrs)
    std_r = np.std(rolling_cagrs)

    # Consistency Score: 1 - (StdDev / Mean), clamped to [0, 1]
    if mean_r > 0:
        consistency = max(0, min(1, 1 - (std_r / mean_r)))
    else:
        consistency = 0

    return mean_r, std_r, consistency


def calculate_sharpe(monthly_returns, risk_free_rate=0.065):
    """
    Sharpe Ratio = (Annualized Return - Risk Free Rate) / Annualized StdDev
    """
    if monthly_returns.empty or len(monthly_returns) < 12:
        return 0

    ann_return = (1 + monthly_returns.mean()) ** 12 - 1
    ann_std = monthly_returns.std() * np.sqrt(12)

    if ann_std == 0:
        return 0

    return (ann_return - risk_free_rate) / ann_std


def calculate_beta(fund_monthly, benchmark_monthly):
    """
    Beta = Covariance(Fund, Benchmark) / Variance(Benchmark)
    """
    # Align on common dates
    aligned = pd.DataFrame({"fund": fund_monthly, "bench": benchmark_monthly}).dropna()

    if len(aligned) < 12:
        return 1.0  # Default to market beta

    cov = np.cov(aligned["fund"], aligned["bench"])
    var_bench = cov[1][1]

    if var_bench == 0:
        return 1.0

    return cov[0][1] / var_bench


def calculate_capture_ratios(fund_monthly, benchmark_monthly):
    """
    Upside Capture = Avg fund return when benchmark > 0 / Avg benchmark return when benchmark > 0
    Downside Capture = Avg fund return when benchmark < 0 / Avg benchmark return when benchmark < 0
    """
    aligned = pd.DataFrame({"fund": fund_monthly, "bench": benchmark_monthly}).dropna()

    # Upside
    up_mask = aligned["bench"] > 0
    if up_mask.sum() > 0:
        upside = aligned.loc[up_mask, "fund"].mean() / aligned.loc[up_mask, "bench"].mean()
    else:
        upside = 1.0

    # Downside
    down_mask = aligned["bench"] < 0
    if down_mask.sum() > 0:
        downside = aligned.loc[down_mask, "fund"].mean() / aligned.loc[down_mask, "bench"].mean()
    else:
        downside = 1.0

    return upside, downside


def calculate_benchmark_outperformance(navs_df, benchmark_navs_df, window_years=3):
    """
    Calculate % of rolling windows where fund beat the benchmark.
    """
    fund_nav = navs_df.set_index("date")["nav"].sort_index()
    bench_nav = benchmark_navs_df.set_index("date")["nav"].sort_index()

    wins = 0
    total = 0

    fund_dates = fund_nav.index
    for i in range(len(fund_dates)):
        start = fund_dates[i]
        end = start + pd.DateOffset(years=window_years)

        fund_future = fund_nav[fund_nav.index >= end]
        bench_future = bench_nav[bench_nav.index >= end]
        bench_start = bench_nav[bench_nav.index >= start]

        if fund_future.empty or bench_future.empty or bench_start.empty:
            break

        fund_start_val = fund_nav.iloc[i]
        fund_end_val = fund_future.iloc[0]

        b_start = bench_start.iloc[0]
        b_end = bench_future.iloc[0]

        if fund_start_val <= 0 or b_start <= 0:
            continue

        actual_days = (fund_future.index[0] - start).days
        if actual_days < 300:
            continue

        years = actual_days / 365.25
        fund_cagr = (fund_end_val / fund_start_val) ** (1 / years) - 1
        bench_cagr = (b_end / b_start) ** (1 / years) - 1

        total += 1
        if fund_cagr > bench_cagr:
            wins += 1

    if total == 0:
        return 0.5  # Neutral

    return wins / total


# ─────────────────────────────────────────────────────────────
# Scoring & Ranking
# ─────────────────────────────────────────────────────────────

def normalize_column(series, invert=False):
    """Min-max normalize a series to [0, 1]. If invert, lower = better."""
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pd.Series(0.5, index=series.index)
    normed = (series - mn) / (mx - mn)
    if invert:
        normed = 1 - normed
    return normed


def calculate_final_score(df, weights=None):
    """Normalize metrics and compute weighted final score."""
    if weights is None:
        weights = DEFAULT_WEIGHTS

    df = df.copy()

    # Normalize each metric
    df["n_consistency"] = normalize_column(df["rolling_consistency"])
    df["n_sharpe"] = normalize_column(df["sharpe"])
    df["n_upside"] = normalize_column(df["upside_capture"])
    df["n_downside"] = normalize_column(df["downside_capture"], invert=True)  # Lower = better
    df["n_benchmark"] = normalize_column(df["benchmark_outperf"])
    df["n_beta"] = normalize_column(df["beta_stability"])
    df["n_ter"] = normalize_column(df["ter"], invert=True)  # Lower = better

    # Weighted sum
    df["final_score"] = (
        weights["rolling_consistency"] * df["n_consistency"]
        + weights["sharpe"] * df["n_sharpe"]
        + weights["upside_capture"] * df["n_upside"]
        + weights["downside_capture"] * df["n_downside"]
        + weights["benchmark_outperf"] * df["n_benchmark"]
        + weights["beta_stability"] * df["n_beta"]
        + weights["ter"] * df["n_ter"]
    )

    return df.sort_values("final_score", ascending=False).reset_index(drop=True)


# ─────────────────────────────────────────────────────────────
# HTML Report
# ─────────────────────────────────────────────────────────────

SCREENER_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MF Screener: {{ category }} — {{ report_date }}</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
            color: #e2e8f0;
            min-height: 100vh;
            padding: 24px;
        }
        .container { max-width: 1400px; margin: 0 auto; }

        .header {
            background: linear-gradient(135deg, #1e3a5f 0%, #0f2847 100%);
            border-radius: 16px;
            padding: 32px;
            margin-bottom: 24px;
            border: 1px solid rgba(59, 130, 246, 0.2);
            box-shadow: 0 4px 24px rgba(0,0,0,0.3);
        }
        .header h1 {
            font-size: 28px;
            font-weight: 800;
            background: linear-gradient(90deg, #60a5fa, #a78bfa);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 8px;
        }
        .header .subtitle { color: #94a3b8; font-size: 14px; }

        .params-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
            gap: 12px;
            margin-top: 20px;
        }
        .param-card {
            background: rgba(30, 41, 59, 0.6);
            border-radius: 10px;
            padding: 14px;
            border: 1px solid rgba(71, 85, 105, 0.3);
        }
        .param-card .label { font-size: 11px; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; font-weight: 600; }
        .param-card .value { font-size: 18px; font-weight: 700; color: #f1f5f9; margin-top: 4px; }

        .weight-bar {
            margin-top: 24px;
            background: rgba(30, 41, 59, 0.6);
            border-radius: 12px;
            padding: 16px 20px;
            border: 1px solid rgba(71, 85, 105, 0.3);
        }
        .weight-bar h3 { font-size: 13px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 10px; }
        .weights-flex { display: flex; gap: 6px; flex-wrap: wrap; }
        .weight-pill {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 6px 12px;
            border-radius: 8px;
            font-size: 12px;
            font-weight: 600;
            background: rgba(59, 130, 246, 0.15);
            color: #93c5fd;
            border: 1px solid rgba(59, 130, 246, 0.2);
        }
        .weight-pill .pct { color: #60a5fa; font-weight: 800; }

        .results-card {
            background: rgba(15, 23, 42, 0.7);
            border-radius: 16px;
            border: 1px solid rgba(71, 85, 105, 0.3);
            box-shadow: 0 4px 24px rgba(0,0,0,0.3);
            overflow: hidden;
            margin-bottom: 24px;
        }
        .results-card .card-header {
            padding: 20px 24px;
            background: rgba(30, 41, 59, 0.5);
            border-bottom: 1px solid rgba(71, 85, 105, 0.3);
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        .results-card .card-header h2 { font-size: 18px; font-weight: 700; }
        .results-card .card-header .badge {
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 700;
            background: rgba(34, 197, 94, 0.15);
            color: #4ade80;
            border: 1px solid rgba(34, 197, 94, 0.2);
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }
        thead th {
            position: sticky;
            top: 0;
            background: #1e293b;
            color: #94a3b8;
            font-size: 11px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            padding: 12px 10px;
            text-align: left;
            border-bottom: 2px solid rgba(71, 85, 105, 0.3);
            white-space: nowrap;
        }
        thead th.num { text-align: right; }
        tbody tr {
            border-bottom: 1px solid rgba(51, 65, 85, 0.3);
            transition: background 0.15s;
        }
        tbody tr:hover { background: rgba(30, 41, 59, 0.5); }
        tbody td {
            padding: 12px 10px;
            vertical-align: middle;
        }
        td.num { text-align: right; font-variant-numeric: tabular-nums; }

        .rank-badge {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 28px;
            height: 28px;
            border-radius: 8px;
            font-weight: 800;
            font-size: 13px;
        }
        .rank-1 { background: linear-gradient(135deg, #f59e0b, #d97706); color: #fff; }
        .rank-2 { background: linear-gradient(135deg, #94a3b8, #64748b); color: #fff; }
        .rank-3 { background: linear-gradient(135deg, #b45309, #92400e); color: #fff; }
        .rank-other { background: rgba(51, 65, 85, 0.5); color: #94a3b8; }

        .score-bar {
            width: 80px;
            height: 6px;
            background: rgba(51, 65, 85, 0.5);
            border-radius: 3px;
            overflow: hidden;
            display: inline-block;
            vertical-align: middle;
            margin-left: 8px;
        }
        .score-bar-fill {
            height: 100%;
            border-radius: 3px;
            transition: width 0.3s;
        }

        .fund-name {
            font-weight: 600;
            color: #f1f5f9;
            max-width: 300px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .metric-good { color: #4ade80; }
        .metric-neutral { color: #fbbf24; }
        .metric-bad { color: #f87171; }

        .footer {
            text-align: center;
            padding: 24px;
            color: #475569;
            font-size: 12px;
        }

        @media (max-width: 768px) {
            body { padding: 12px; }
            .header { padding: 20px; }
            .header h1 { font-size: 20px; }
            table { font-size: 11px; }
            .fund-name { max-width: 150px; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔬 MF Screener & Ranker</h1>
            <div class="subtitle">Multi-metric mutual fund evaluation — {{ report_date }}</div>

            <div class="params-grid">
                <div class="param-card">
                    <div class="label">Category</div>
                    <div class="value" style="font-size:14px;">{{ category }}</div>
                </div>
                <div class="param-card">
                    <div class="label">Lookback</div>
                    <div class="value">{{ lookback }}Y</div>
                </div>
                <div class="param-card">
                    <div class="label">Rolling Window</div>
                    <div class="value">{{ rolling_window }}Y</div>
                </div>
                <div class="param-card">
                    <div class="label">Risk-Free Rate</div>
                    <div class="value">{{ "%.1f"|format(risk_free_rate * 100) }}%</div>
                </div>
                <div class="param-card">
                    <div class="label">Funds Analyzed</div>
                    <div class="value">{{ total_funds }}</div>
                </div>
                <div class="param-card">
                    <div class="label">Showing Top</div>
                    <div class="value">{{ top_n }}</div>
                </div>
            </div>

            <div class="weight-bar">
                <h3>Scoring Weights</h3>
                <div class="weights-flex">
                    <span class="weight-pill">Consistency <span class="pct">25%</span></span>
                    <span class="weight-pill">Sharpe <span class="pct">20%</span></span>
                    <span class="weight-pill">Upside Capture <span class="pct">15%</span></span>
                    <span class="weight-pill">Downside Capture <span class="pct">15%</span></span>
                    <span class="weight-pill">Benchmark Beat <span class="pct">10%</span></span>
                    <span class="weight-pill">Beta Stability <span class="pct">5%</span></span>
                    <span class="weight-pill">TER <span class="pct">10%</span></span>
                </div>
            </div>
        </div>

        <div class="results-card">
            <div class="card-header">
                <h2>📊 Ranked Results</h2>
                <span class="badge">Top {{ top_n }} of {{ total_funds }}</span>
            </div>
            <div style="overflow-x: auto;">
                <table>
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>Fund Name</th>
                            <th class="num">Score</th>
                            <th class="num">Consistency</th>
                            <th class="num">Sharpe</th>
                            <th class="num">Beta</th>
                            <th class="num">Upside Cap</th>
                            <th class="num">Downside Cap</th>
                            <th class="num">BM Beat %</th>
                            <th class="num">TER %</th>
                            <th class="num">AUM (₹Cr)</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for row in funds %}
                        <tr>
                            <td>
                                {% if loop.index == 1 %}
                                    <span class="rank-badge rank-1">1</span>
                                {% elif loop.index == 2 %}
                                    <span class="rank-badge rank-2">2</span>
                                {% elif loop.index == 3 %}
                                    <span class="rank-badge rank-3">3</span>
                                {% else %}
                                    <span class="rank-badge rank-other">{{ loop.index }}</span>
                                {% endif %}
                            </td>
                            <td>
                                <div class="fund-name" title="{{ row.schemeName }}">{{ row.schemeName }}</div>
                            </td>
                            <td class="num">
                                <strong>{{ "%.2f"|format(row.final_score * 100) }}</strong>
                                <div class="score-bar">
                                    <div class="score-bar-fill" style="width: {{ (row.final_score * 100)|int }}%; background: linear-gradient(90deg,
                                        {% if row.final_score >= 0.7 %}#22c55e, #16a34a
                                        {% elif row.final_score >= 0.4 %}#eab308, #f59e0b
                                        {% else %}#ef4444, #dc2626{% endif %});"></div>
                                </div>
                            </td>
                            <td class="num {% if row.rolling_consistency >= 0.7 %}metric-good{% elif row.rolling_consistency >= 0.4 %}metric-neutral{% else %}metric-bad{% endif %}">
                                {{ "%.2f"|format(row.rolling_consistency) }}
                            </td>
                            <td class="num {% if row.sharpe >= 1.0 %}metric-good{% elif row.sharpe >= 0.5 %}metric-neutral{% else %}metric-bad{% endif %}">
                                {{ "%.2f"|format(row.sharpe) }}
                            </td>
                            <td class="num">{{ "%.2f"|format(row.beta) }}</td>
                            <td class="num {% if row.upside_capture >= 1.0 %}metric-good{% else %}metric-neutral{% endif %}">
                                {{ "%.2f"|format(row.upside_capture) }}
                            </td>
                            <td class="num {% if row.downside_capture <= 0.9 %}metric-good{% elif row.downside_capture <= 1.0 %}metric-neutral{% else %}metric-bad{% endif %}">
                                {{ "%.2f"|format(row.downside_capture) }}
                            </td>
                            <td class="num {% if row.benchmark_outperf >= 0.6 %}metric-good{% elif row.benchmark_outperf >= 0.4 %}metric-neutral{% else %}metric-bad{% endif %}">
                                {{ "%.0f"|format(row.benchmark_outperf * 100) }}%
                            </td>
                            <td class="num">{{ "%.2f"|format(row.ter) }}</td>
                            <td class="num" style="color: #94a3b8;">{{ "{:,.0f}".format(row.aum) if row.aum else 'N/A' }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>

        <div class="footer">
            Generated by MF Screener &amp; Ranker — {{ report_date }} |
            Data: MFAPI + Kuvera | AUM is display only — not included in scoring
        </div>
    </div>
</body>
</html>
"""


def generate_html_report(df, category, params, filepath):
    """Render the screener results to an HTML file."""
    template = Template(SCREENER_TEMPLATE)
    html = template.render(
        funds=df.to_dict("records"),
        category=category,
        report_date=datetime.now().strftime("%d %b %Y"),
        lookback=params["lookback"],
        rolling_window=params["rolling_window"],
        risk_free_rate=params["risk_free_rate"],
        total_funds=params["total_funds"],
        top_n=params["top_n"],
    )
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html)
    logging.info(f"Report saved: {filepath}")


# ─────────────────────────────────────────────────────────────
# Main Pipeline
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="MF Screener & Ranking Tool")
    parser.add_argument("--category", type=str, required=True,
                        help='Scheme category, e.g. "Equity Scheme - Large Cap Fund"')
    parser.add_argument("--lookback", type=int, default=5,
                        help="Lookback period in years (default: 5)")
    parser.add_argument("--rolling_window", type=int, default=3,
                        help="Rolling return window in years (default: 3)")
    parser.add_argument("--risk_free_rate", type=float, default=6.5,
                        help="Risk-free rate in %% (default: 6.5)")
    parser.add_argument("--top_n", type=int, default=10,
                        help="Number of top funds to display (default: 10)")
    parser.add_argument("--fund_limit", type=int, default=50,
                        help="Max funds to evaluate per category (default: 50)")
    parser.add_argument("--csv", action="store_true",
                        help="Also export results as CSV")
    args = parser.parse_args()

    rfr = args.risk_free_rate / 100.0  # Convert to decimal

    logging.info("=" * 60)
    logging.info(f"MF SCREENER & RANKER")
    logging.info(f"Category     : {args.category}")
    logging.info(f"Lookback     : {args.lookback} years")
    logging.info(f"Rolling Win  : {args.rolling_window} years")
    logging.info(f"Risk-Free    : {args.risk_free_rate}%")
    logging.info(f"Top N        : {args.top_n}")
    logging.info(f"Fund Limit   : {args.fund_limit}")
    logging.info("=" * 60)

    session = create_session()

    # Step 1: Fetch benchmark NAV
    logging.info("Step 1: Fetching benchmark NAV...")
    benchmark_navs = fetch_benchmark_nav(session, args.category, args.lookback)
    if benchmark_navs is None or benchmark_navs.empty:
        logging.error("Could not fetch benchmark data. Aborting.")
        return
    benchmark_monthly = compute_monthly_returns(benchmark_navs)
    logging.info(f"Benchmark: {len(benchmark_navs)} daily / {len(benchmark_monthly)} monthly data points")

    # Step 2: Fetch full scheme list and filter by category
    logging.info("Step 2: Fetching scheme list...")
    candidates = fetch_fund_list(session, args.category, args.fund_limit)
    logging.info(f"Candidate schemes: {len(candidates)}")

    # Step 3: Process each fund
    logging.info("Step 3: Processing funds...")
    results = []
    processed = 0

    for scheme in candidates:
        if processed >= args.fund_limit:
            break

        code = scheme["schemeCode"]
        name = scheme["schemeName"]

        # Fetch NAV
        navs, meta = fetch_full_nav_history(session, code, args.lookback)
        if navs is None or meta is None:
            continue

        # Check category match
        scheme_category = meta.get("scheme_category", "")
        if scheme_category != args.category:
            continue

        isin = meta.get("isin_growth", "")

        processed += 1
        logging.info(f"  [{processed}/{args.fund_limit}] {name}")

        # Monthly returns
        fund_monthly = compute_monthly_returns(navs)
        if len(fund_monthly) < 12:
            logging.debug(f"    Skipping {name}: insufficient monthly data ({len(fund_monthly)} months)")
            continue

        # Rolling returns
        mean_roll, std_roll, consistency = calculate_rolling_returns(navs, args.rolling_window)

        # Sharpe
        sharpe = calculate_sharpe(fund_monthly, rfr)

        # Beta
        beta = calculate_beta(fund_monthly, benchmark_monthly)

        # Beta stability: penalize deviation from 1
        beta_stability = max(0, 1 - abs(beta - 1))

        # Capture ratios
        upside_cap, downside_cap = calculate_capture_ratios(fund_monthly, benchmark_monthly)

        # Benchmark outperformance
        bm_outperf = calculate_benchmark_outperformance(navs, benchmark_navs, args.rolling_window)

        # Kuvera metadata (TER, AUM)
        kuvera = fetch_kuvera_metadata(session, isin)
        ter = kuvera.get("expense_ratio", 0)
        aum = kuvera.get("aum", 0)

        results.append({
            "schemeCode": code,
            "schemeName": name,
            "isin": isin,
            "mean_rolling_return": mean_roll,
            "rolling_std": std_roll,
            "rolling_consistency": consistency,
            "sharpe": sharpe,
            "beta": beta,
            "beta_stability": beta_stability,
            "upside_capture": upside_cap,
            "downside_capture": downside_cap,
            "benchmark_outperf": bm_outperf,
            "ter": ter,
            "aum": aum,
            "fund_manager": kuvera.get("fund_manager", ""),
            "crisil_rating": kuvera.get("crisil_rating", ""),
        })

    if not results:
        logging.error("No funds processed successfully. Check category name or data availability.")
        return

    logging.info(f"\nStep 4: Scoring & Ranking {len(results)} funds...")
    df = pd.DataFrame(results)
    df = calculate_final_score(df)

    # Show top N
    top = df.head(args.top_n)

    # Console output
    print("\n" + "=" * 100)
    print(f"  TOP {args.top_n} FUNDS — {args.category}")
    print("=" * 100)
    display_cols = [
        "schemeName", "final_score", "rolling_consistency", "sharpe",
        "beta", "upside_capture", "downside_capture", "benchmark_outperf", "ter", "aum"
    ]
    print(top[display_cols].to_string(index=True))
    print("=" * 100)

    # HTML Report
    os.makedirs(REPORTS_DIR, exist_ok=True)
    cat_slug = args.category.replace(" ", "_").replace("/", "_").replace("-", "")
    report_path = os.path.join(
        REPORTS_DIR,
        f"screener_{cat_slug}_{datetime.now():%Y-%m-%d}.html"
    )
    generate_html_report(
        top, args.category,
        {
            "lookback": args.lookback,
            "rolling_window": args.rolling_window,
            "risk_free_rate": rfr,
            "total_funds": len(results),
            "top_n": args.top_n,
        },
        report_path,
    )

    # Optional CSV
    if args.csv:
        csv_path = report_path.replace(".html", ".csv")
        df.to_csv(csv_path, index=False)
        logging.info(f"CSV saved: {csv_path}")

    logging.info("Done!")


if __name__ == "__main__":
    main()
