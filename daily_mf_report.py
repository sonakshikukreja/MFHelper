# daily_mf_report.py
import logging
import sys
import requests, os, math, json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
import smtplib
from jinja2 import Template
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# --- Configuration Loading ---
def load_config():
    config_path = "config.json"
    default_config = {
        "api": {"base_url": "https://api.mfapi.in"},
        "smtp": {
            "host": "smtp.gmail.com",
            "port": 587,
            "user": os.getenv("SMTP_USER"),
            "pass": os.getenv("SMTP_PASS"),
            "from": os.getenv("FROM_EMAIL")
        },
        "reporting": {
            "to_emails": [],
            "scheme_limit": os.getenv("SCHEME_LIMIT"),
            "staleness_days": 5
        },
        "persistence": {
            "chunk_size": 5000,
            "nav_data_dir": "NAVData",
            "reports_dir": "Reports",
            "logs_dir": "Logs"
        }
    }
    
    # 1. Load from entire JSON string if provided (GitHub Secrets pattern)
    env_config_json = os.getenv("APP_CONFIG_JSON")
    if env_config_json:
        try:
            user_config = json.loads(env_config_json)
            for key in user_config:
                if key in default_config and isinstance(default_config[key], dict):
                    default_config[key].update(user_config[key])
                else:
                    default_config[key] = user_config[key]
        except Exception as e:
            print(f"Warning: Failed to parse APP_CONFIG_JSON: {e}")

    # 2. Load from file if exists
    elif os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                user_config = json.load(f)
                for key in user_config:
                    if key in default_config and isinstance(default_config[key], dict):
                        default_config[key].update(user_config[key])
                    else:
                        default_config[key] = user_config[key]
        except Exception as e:
            print(f"Warning: Failed to load {config_path}: {e}")
            
    # 3. Final environment variable overrides (highest priority)
    if os.getenv("SMTP_PASS"): default_config["smtp"]["pass"] = os.getenv("SMTP_PASS")
    if os.getenv("SMTP_USER"): default_config["smtp"]["user"] = os.getenv("SMTP_USER")
    if os.getenv("REPORT_RECIPIENTS"): 
        default_config["reporting"]["to_emails"] = [e.strip() for e in os.getenv("REPORT_RECIPIENTS").split(",")]
    if os.getenv("SCHEME_LIMIT"): 
        default_config["reporting"]["scheme_limit"] = os.getenv("SCHEME_LIMIT")
    return default_config

CONFIG = load_config()

# Configure logging
# Create a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create Logs directory if not exists
log_dir = CONFIG["persistence"]["logs_dir"]
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Timestamp for log filename: app_DD-MMM-YYYY_HH.MM.log
log_filename = f"app_{datetime.now().strftime('%d-%b-%Y_%H.%M')}.log"
log_filepath = os.path.join(log_dir, log_filename)

# Create handlers
c_handler = logging.StreamHandler(sys.stdout)
f_handler = logging.FileHandler(log_filepath)
c_handler.setLevel(logging.INFO)
f_handler.setLevel(logging.INFO)

# Create formatters and add it to handlers
c_format = logging.Formatter('%(message)s') # Keep console output clean
f_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)

# Add handlers to the logger
if not logger.handlers:
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)

# Config 
MFAPI_BASE = CONFIG["api"]["base_url"]
SMTP_HOST = CONFIG["smtp"]["host"]
SMTP_PORT = CONFIG["smtp"]["port"]
SMTP_USER = CONFIG["smtp"]["user"]
SMTP_PASS = CONFIG["smtp"]["pass"]
FROM_EMAIL = CONFIG["smtp"]["from"] or SMTP_USER or "mf-report@gmail.com"
TO_EMAILS = CONFIG["reporting"]["to_emails"]

# Utility: fetch scheme list from MFAPI with pagination (batches of 1000)
def fetch_all_schemes(session):
    all_schemes = []
    offset = 0
    batch_size = 1000
    
    while True:
        logging.info(f"Fetching schemes: offset={offset}, limit={batch_size}")
        try:
            resp = session.get(f"{MFAPI_BASE}/mf?limit={batch_size}&offset={offset}")
            resp.raise_for_status()
            batch = resp.json()
        except Exception as e:
            logging.error(f"Error fetching schemes batch {offset}: {e}")
            break
        
        # If no schemes returned, we've fetched all
        if not batch or len(batch) == 0:
            break
            
        all_schemes.extend(batch)
        
        # If we got fewer schemes than requested, we've reached the end
        if len(batch) < batch_size:
            break
            
        offset += batch_size
    
# daily_mf_report.py
import logging
import sys
import requests, os, math, json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
import smtplib
from jinja2 import Template
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# --- Configuration Loading ---
def load_config():
    config_path = "config.json"
    default_config = {
        "api": {"base_url": "https://api.mfapi.in"},
        "smtp": {
            "host": "smtp.gmail.com",
            "port": 587,
            "user": os.getenv("SMTP_USER"),
            "pass": os.getenv("SMTP_PASS"),
            "from": os.getenv("FROM_EMAIL")
        },
        "reporting": {
            "to_emails": [],
            "scheme_limit": os.getenv("SCHEME_LIMIT"),
            "staleness_days": 5
        },
        "persistence": {
            "chunk_size": 5000,
            "nav_data_dir": "NAVData",
            "reports_dir": "Reports",
            "logs_dir": "Logs"
        }
    }
    
    # 1. Load from entire JSON string if provided (GitHub Secrets pattern)
    env_config_json = os.getenv("APP_CONFIG_JSON")
    if env_config_json:
        try:
            user_config = json.loads(env_config_json)
            for key in user_config:
                if key in default_config and isinstance(default_config[key], dict):
                    default_config[key].update(user_config[key])
                else:
                    default_config[key] = user_config[key]
        except Exception as e:
            print(f"Warning: Failed to parse APP_CONFIG_JSON: {e}")

    # 2. Load from file if exists
    elif os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                user_config = json.load(f)
                for key in user_config:
                    if key in default_config and isinstance(default_config[key], dict):
                        default_config[key].update(user_config[key])
                    else:
                        default_config[key] = user_config[key]
        except Exception as e:
            print(f"Warning: Failed to load {config_path}: {e}")
            
    # 3. Final environment variable overrides (highest priority)
    if os.getenv("SMTP_PASS"): default_config["smtp"]["pass"] = os.getenv("SMTP_PASS")
    if os.getenv("SMTP_USER"): default_config["smtp"]["user"] = os.getenv("SMTP_USER")
    if os.getenv("REPORT_RECIPIENTS"): 
        default_config["reporting"]["to_emails"] = [e.strip() for e in os.getenv("REPORT_RECIPIENTS").split(",")]
    if os.getenv("SCHEME_LIMIT"): 
        default_config["reporting"]["scheme_limit"] = os.getenv("SCHEME_LIMIT")
    return default_config

CONFIG = load_config()

# Configure logging
# Create a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create Logs directory if not exists
log_dir = CONFIG["persistence"]["logs_dir"]
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Timestamp for log filename: app_DD-MMM-YYYY_HH.MM.log
log_filename = f"app_{datetime.now().strftime('%d-%b-%Y_%H.%M')}.log"
log_filepath = os.path.join(log_dir, log_filename)

# Create handlers
c_handler = logging.StreamHandler(sys.stdout)
f_handler = logging.FileHandler(log_filepath)
c_handler.setLevel(logging.INFO)
f_handler.setLevel(logging.INFO)

# Create formatters and add it to handlers
c_format = logging.Formatter('%(message)s') # Keep console output clean
f_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)

# Add handlers to the logger
if not logger.handlers:
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)

# Config 
MFAPI_BASE = CONFIG["api"]["base_url"]
SMTP_HOST = CONFIG["smtp"]["host"]
SMTP_PORT = CONFIG["smtp"]["port"]
SMTP_USER = CONFIG["smtp"]["user"]
SMTP_PASS = CONFIG["smtp"]["pass"]
FROM_EMAIL = CONFIG["smtp"]["from"] or SMTP_USER or "mf-report@gmail.com"
TO_EMAILS = CONFIG["reporting"]["to_emails"]

# Utility: fetch scheme list from MFAPI with pagination (batches of 1000)
def fetch_all_schemes(session):
    all_schemes = []
    offset = 0
    batch_size = 1000
    
    while True:
        logging.info(f"Fetching schemes: offset={offset}, limit={batch_size}")
        try:
            resp = session.get(f"{MFAPI_BASE}/mf?limit={batch_size}&offset={offset}")
            resp.raise_for_status()
            batch = resp.json()
        except Exception as e:
            logging.error(f"Error fetching schemes batch {offset}: {e}")
            break
        
        # If no schemes returned, we've fetched all
        if not batch or len(batch) == 0:
            break
            
        all_schemes.extend(batch)
        
        # If we got fewer schemes than requested, we've reached the end
        if len(batch) < batch_size:
            break
            
        offset += batch_size
    
    logging.info(f"Total schemes fetched: {len(all_schemes)}")
    return all_schemes  # list of {"schemeName","schemeCode"}

# Fetch NAV history for a scheme code using date range (1 year)
# --- Kuvera Client for Scheme Details ---
class KuveraClient:
    def __init__(self, session, reports_dir, invested_isins=None):
        self.session = session
        self.base_url = "https://mf.captnemo.in/kuvera"
        self.details_dir = os.path.join(reports_dir, "Details")
        self.invested_isins = invested_isins or set()
        if not os.path.exists(self.details_dir):
            os.makedirs(self.details_dir)

    def fetch_details(self, isin):
        if not isin:
            return None
        try:
            url = f"{self.base_url}/{isin}"
            resp = self.session.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                # API returns list of funds directly
                if isinstance(data, list) and data:
                    return data[0]
                elif isinstance(data, dict):
                    # Handle case where it might be a dict with 'data' key or root object
                    funds = data.get("data")
                    if isinstance(funds, list) and funds:
                        return funds[0]
                    return data if data.get("isin") or data.get("ISIN") else None
            return None
        except Exception as e:
            logging.error(f"Error fetching Kuvera details for {isin}: {e}")
            return None

    def generate_detail_page(self, isin):
        if not isin:
            return None, None
        filepath = os.path.join(self.details_dir, f"{isin}.html")
        # Relative path for the link in the main report
        rel_path = f"Details/{isin}.html"
        
        details = self.fetch_details(isin)
        if not details:
            if os.path.exists(filepath):
                return rel_path, None
            return None, None
        
        # Determine can_invest status from Kuvera data
        lump_available = details.get('Lump Available') or details.get('lump_available') or details.get('lumpsum_available')
        sip_available = details.get('Sip Available') or details.get('sip_available')
        can_invest = str(lump_available).upper() == 'Y' or str(sip_available).upper() == 'Y'
        
        if os.path.exists(filepath):
            return rel_path, can_invest
            
        template_str = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{{ details.name }} - Details</title>
    <style>
        body { font-family: 'Outfit', sans-serif; line-height: 1.6; color: #333; max-width: 900px; margin: 40px auto; padding: 20px; background-color: #f8fafc; }
        .card { background: #fff; padding: 40px; border-radius: 16px; box-shadow: 0 10px 30px rgba(0,0,0,0.05); }
        h1 { color: #1e293b; margin-top: 0; font-size: 28px; border-bottom: 3px solid #3b82f6; padding-bottom: 12px; }
        h2 { color: #334155; font-size: 20px; margin-top: 30px; border-left: 5px solid #3b82f6; padding-left: 12px; }
        .meta-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 24px; margin: 25px 0; }
        .meta-item { background: #f1f5f9; padding: 18px; border-radius: 10px; border: 1px solid #e2e8f0; }
        .label { font-size: 11px; color: #64748b; text-transform: uppercase; font-weight: 800; display: block; letter-spacing: 0.05em; margin-bottom: 4px; }
        .value { font-size: 17px; color: #0f172a; font-weight: 700; }
        .objective { background: #fdfdfd; padding: 25px; border-radius: 10px; font-style: italic; border: 1px dashed #cbd5e1; color: #475569; }
        .returns-table { width: 100%; border-collapse: separate; border-spacing: 0; margin: 20px 0; border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; }
        .returns-table th, .returns-table td { padding: 14px 20px; text-align: left; border-bottom: 1px solid #e2e8f0; }
        .returns-table th { background-color: #f1f5f9; color: #475569; font-weight: 700; }
        .returns-table tr:last-child td { border-bottom: none; }
        .positive { color: #10b981; font-weight: 800; }
        .negative { color: #ef4444; font-weight: 800; }
        .btn-back { display: inline-block; margin-top: 35px; padding: 12px 24px; background: linear-gradient(135deg, #3b82f6, #2563eb); color: #fff; text-decoration: none; border-radius: 8px; font-weight: 600; box-shadow: 0 4px 12px rgba(37, 99, 235, 0.2); transition: all 0.3s; }
        .btn-back:hover { transform: translateY(-2px); box-shadow: 0 6px 15px rgba(37, 99, 235, 0.3); }
        .kv-section-title { margin-top: 36px; font-size: 20px; color: #1e293b; }
        .kv-subtitle { margin-top: 20px; font-size: 15px; color: #475569; }
        .kv-label { font-weight: 600; color: #64748b; width: 35%; }
        .kv-value { color: #0f172a; }
        .kv-inner-table { margin: 0; border: none; }
        .meta-item-top { display: flex; justify-content: space-between; align-items: center; gap: 12px; }
        .action-btn { display: inline-block; padding: 6px 14px; border-radius: 9999px; font-size: 11px; font-weight: 800; letter-spacing: 0.06em; text-transform: uppercase; }
        .action-btn-invest { background: #16a34a; color: #ffffff; }
        .action-btn-invested { background: #059669; color: #ffffff; }
        .action-btn-divest { background: #dc2626; color: #ffffff; }
    </style>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;600;700;800&display=swap" rel="stylesheet">
</head>
<body>
    <div class="card">
        {% set lump_available = details.get('Lump Available') or details.get('lump_available') or details.get('lumpsum_available') %}
        {% set sip_available = details.get('Sip Available') or details.get('sip_available') %}
        {% set can_invest = (lump_available|string).upper() == 'Y' or (sip_available|string).upper() == 'Y' %}
        {% set comparison = details.get('comparison') or details.get('Comparison') %}
        {% set nav_info = details.get('nav') or details.get('Nav') or details.get('NAV') %}
        <h1>{{ details.name }}</h1>
        
        <div class="meta-grid">
            <div class="meta-item">
                <div class="meta-item-top">
                    <div>
                        <span class="label">ISIN</span>
                        <span class="value">{{ details.isin or details.ISIN }}</span>
                    </div>
                    <div>
                        {% if (details.isin or details.ISIN) in invested_isins %}
                        <span class="action-btn action-btn-invested">INVESTED</span>
                        {% elif can_invest %}
                        <span class="action-btn action-btn-invest">I</span>
                        {% else %}
                        <span class="action-btn action-btn-divest">D</span>
                        {% endif %}
                    </div>
                </div>
            </div>
            <div class="meta-item">
                <span class="label">Fund Manager</span>
                <span class="value">{{ details.fund_manager or 'N/A' }}</span>
            </div>
            <div class="meta-item">
                <span class="label">Expense Ratio</span>
                <span class="value">{{ details.expense_ratio }}%</span>
            </div>
            <div class="meta-item">
                <span class="label">AUM</span>
                <span class="value">₹{{ "{:,.2f}".format(details.aum) if details.aum else 'N/A' }} Cr</span>
            </div>
            {% if nav_info %}
            <div class="meta-item">
                <span class="label">Last NAV</span>
                <span class="value">
                    ₹{% if nav_info.nav is not none %}{{ "%.4f"|format(nav_info.nav|float) }}{% else %}N/A{% endif %}
                    {% if nav_info.date %} on {{ nav_info.date }}{% endif %}
                </span>
            </div>
            {% endif %}
        </div>

        <h2>Investment Objective</h2>
        <div class="objective">
            {{ details.investment_objective }}
        </div>

        <h2>Performance Returns</h2>
        <table class="returns-table">
            <thead>
                <tr>
                    <th>Period</th>
                    <th>Return (%)</th>
                </tr>
            </thead>
            <tbody>
                {% set ret = details.returns %}
                <tr><td>1 Week</td><td class="{{ 'positive' if ret.week_1|float > 0 else 'negative' }}">{{ ret.week_1 }}%</td></tr>
                <tr><td>1 Year</td><td class="{{ 'positive' if ret.year_1|float > 0 else 'negative' }}">{{ ret.year_1 }}%</td></tr>
                <tr><td>3 Year</td><td class="{{ 'positive' if ret.year_3|float > 0 else 'negative' }}">{{ ret.year_3 }}%</td></tr>
                <tr><td>5 Year</td><td class="{{ 'positive' if ret.year_5|float > 0 else 'negative' }}">{{ ret.year_5 }}%</td></tr>
                <tr><td>Inception</td><td class="{{ 'positive' if ret.inception|float > 0 else 'negative' }}">{{ ret.inception }}%</td></tr>
            </tbody>
        </table>
        
        {% if comparison and comparison is sequence and not (comparison is string) and comparison|length > 0 %}
        <h2 class="kv-section-title">Comparison</h2>
        <table class="returns-table">
            <thead>
                <tr>
                    <th>Metric</th>
                    {% for mf in comparison %}
                    <th>MF {{ loop.index }}</th>
                    {% endfor %}
                </tr>
            </thead>
            <tbody>
                {% set first = comparison[0] %}
                {% if first is mapping %}
                    {% for metric, _ in first|dictsort %}
                    <tr>
                        <td class="kv-label">{{ metric.replace('_', ' ')|title }}</td>
                        {% for mf in comparison %}
                        <td class="kv-value">{{ mf.get(metric, '') }}</td>
                        {% endfor %}
                    </tr>
                    {% endfor %}
                {% else %}
                    <tr>
                        <td class="kv-label">Value</td>
                        {% for mf in comparison %}
                        <td class="kv-value">{{ mf }}</td>
                        {% endfor %}
                    </tr>
                {% endif %}
            </tbody>
        </table>
        {% endif %}
    </div>
</body>
</html>
"""
        try:
            template = Template(template_str)
            # Find the main report filename to link back properly
            # In main(), it's usually index.html or daily_report_...html
            # We'll pass it in later or assume a standard name. 
            # For now, let's use a placeholder or generic link.
            report_filename = "index.html" 
            html_content = template.render(details=details, report_filename=report_filename, invested_isins=self.invested_isins)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(html_content)
            return rel_path, can_invest
        except Exception as e:
            logging.error(f"Error generating detail page for {isin}: {e}")
            return None, None

def fetch_nav_history(session, scheme_code, as_of_date):
    # MFAPI does NOT support startDate/endDate filtering on the endpoint (returns 500)
    # We fetch the full history and filter locally.
    url = f"{MFAPI_BASE}/mf/{scheme_code}"
    resp = session.get(url)
    resp.raise_for_status()
    data = resp.json()
    
    # 'data' key: list of {date, nav}
    if not data or 'data' not in data or not data['data']:
        return None
    
    navs = pd.DataFrame(data["data"])
    navs['date'] = pd.to_datetime(navs['date'], dayfirst=True)
    navs['nav'] = pd.to_numeric(navs['nav'], errors='coerce')
    navs = navs.dropna(subset=['nav'])
    
    # Filter out zero or negative NAVs
    navs = navs[navs['nav'] > 0]
    
    # Filter for last 1 year locally
    start_boundary = as_of_date - pd.DateOffset(years=1)
    # Ensure both are naive for comparison
    navs = navs[(navs['date'] >= start_boundary) & (navs['date'] <= as_of_date)]
    
    if navs.empty:
        # logging.debug(f"No data for {scheme_code} in the last year (Window: {start_boundary} to {as_of_date})")
        return None
    
    navs = navs.sort_values('date').reset_index(drop=True)
    
    # Return only oldest and latest records along with metadata
    return {
        'meta': data.get('meta', {}),
        'start_date': navs.iloc[0]['date'].to_pydatetime(),
        'start_nav': navs.iloc[0]['nav'],
        'end_date': navs.iloc[-1]['date'].to_pydatetime(),
        'end_nav': navs.iloc[-1]['nav']
    }

# Compute XIRR for lump-sum using start and end NAV data
def compute_lumpsum_xirr(nav_data):
    if not nav_data:
        return None
    
    start_dt = nav_data['start_date']
    start_nav = nav_data['start_nav']
    end_dt = nav_data['end_date']
    end_nav = nav_data['end_nav']
    
    try:
        # Calculate annualized return
        days_diff = (end_dt - start_dt).days
        if days_diff <= 0:
            return None
        
        years = days_diff / 365.25
        annualized_return = ((end_nav / start_nav) ** (1/years)) - 1
        return annualized_return
    except Exception:
        return None

# Simple HTML email template
EMAIL_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --primary: #1e293b;
            --accent: #0ea5e9;
            --success: #10b981;
            --bg: #f8fafc;
            --text-main: #334155;
            --text-muted: #64748b;
        }
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background-color: var(--bg);
            color: var(--text-main);
            line-height: 1.5;
            margin: 0;
            padding: 40px 20px;
        }
        .container {
            max-width: 1000px;
            margin: 0 auto;
        }
        .header {
            background: white;
            padding: 32px;
            border-radius: 12px;
            box-shadow: 0 1px 3px 0 rgb(0 0 0 / 0.1);
            margin-bottom: 32px;
            border-left: 6px solid var(--accent);
            position: relative;
        }
        .insights-link {
            position: absolute;
            top: 20px;
            right: 20px;
            background: var(--accent);
            color: white;
            padding: 8px 16px;
            border-radius: 6px;
            text-decoration: none;
            font-size: 13px;
            font-weight: 600;
            transition: opacity 0.2s;
        }
        .insights-link:hover {
            opacity: 0.9;
        }
        h1 {
            margin: 0;
            font-size: 24px;
            font-weight: 700;
            color: var(--primary);
        }
        .meta {
            margin-top: 8px;
            color: var(--text-muted);
            font-size: 14px;
            display: flex;
            gap: 16px;
        }
        .status-badge {
            background: #e0f2fe;
            color: #0369a1;
            padding: 2px 10px;
            border-radius: 9999px;
            font-weight: 600;
            font-size: 12px;
        }
        .section-header {
            margin-top: 24px;
            margin-bottom: 8px;
        }
        details {
            background: white;
            border-radius: 12px;
            box-shadow: 0 1px 3px 0 rgb(0 0 0 / 0.1);
            margin-bottom: 16px;
            overflow: hidden;
            border: 1px solid #e2e8f0;
        }
        summary {
            padding: 20px 24px;
            font-size: 16px;
            font-weight: 700;
            color: var(--primary);
            cursor: pointer;
            list-style: none; /* Hide default arrow */
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: white;
            transition: background 0.2s;
        }
        summary::-webkit-details-marker {
            display: none; /* Hide default arrow in Safari */
        }
        summary:hover {
            background: #f8fafc;
        }
        summary::after {
            content: '+';
            font-size: 20px;
            color: var(--text-muted);
            font-weight: 400;
        }
        details[open] summary {
            border-bottom: 1px solid #f1f5f9;
        }
        details[open] summary::after {
            content: '−';
        }
        .table-container {
            padding: 0; /* Table sits inside details now */
            background: transparent;
            box-shadow: none;
            border-radius: 0;
            margin-bottom: 0;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            text-align: left;
        }
        th {
            background: #f1f5f9;
            padding: 16px;
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            font-weight: 600;
            color: var(--text-muted);
            border-bottom: 1px solid #e2e8f0;
        }
        td {
            padding: 16px;
            border-bottom: 1px solid #f1f5f9;
            font-size: 14px;
        }
        tr:last-child td {
            border-bottom: none;
        }
        tr:hover {
            background-color: #f8fafc;
        }
        .scheme-name {
            font-weight: 600;
            color: var(--primary);
            display: block;
        }
        .scheme-code {
            font-size: 12px;
            color: var(--text-muted);
        }
        .xirr-val {
            font-weight: 700;
            font-variant-numeric: tabular-nums;
        }
        .positive {
            color: var(--success);
        }
        .category-tag {
            font-size: 12px;
            background: #f1f5f9;
            padding: 2px 8px;
            border-radius: 4px;
        }
        .action-btn {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 9999px;
            font-size: 10px;
            font-weight: 800;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            margin-left: 4px;
        }
        .action-btn-invest {
            background: #16a34a;
            color: #ffffff;
        }
        .action-btn-invested {
            background: #059669;
            color: #ffffff;
        }
        .action-btn-divest {
            background: #dc2626;
            color: #ffffff;
        }
        .footer {
            margin-top: 32px;
            text-align: center;
            font-size: 12px;
            color: var(--text-muted);
        }
        .disclaimer {
            background: #fff7ed;
            border: 1px solid #ffedd5;
            color: #9a3412;
            padding: 16px;
            border-radius: 12px;
            font-size: 13px;
            margin-bottom: 32px;
            text-align: center;
            font-weight: 500;
        }
        tr:hover {
            background-color: #f8fafc;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <a href="portfolio_insights.html" class="insights-link">Portfolio Insights</a>
            <h1>Daily Mutual Fund Analysis</h1>
            <div class="meta">
                <span>Timestamp: <strong>{{ist_time}}</strong></span>
                <span class="status-badge">Multi-Category Performance Report</span>
            </div>
        </div>

        <div class="disclaimer">
            ⚠️ <strong>Disclaimer:</strong> This report is for educational purposes only. Past performance is not indicative of future results. Please consult a qualified financial advisor before making any investment decisions.
        </div>

        <div class="section-header" style="margin-top: 0;">
            <span style="font-size: 16px; font-weight: 600; color: var(--primary);">Ideal Parameter Guidelines</span>
        </div>
        <ul style="margin: 0 0 24px 20px; padding-left: 0; color: var(--text-main); font-size: 13px;">
            <li>✅ Rolling return consistency high</li>
            <li>✅ Beats benchmark &gt; 60% of the time</li>
            <li>✅ Sharpe &gt; 1</li>
            <li>✅ Beta around 1 (or slightly lower)</li>
            <li>✅ Upside capture &gt; 1</li>
            <li>✅ Downside capture &lt; 1</li>
            <li>✅ TER reasonable within category</li>
        </ul>

        {% if top_n %}
        <details>
            <summary>
                🏆 Overall Top 200 Performance
            </summary>
            <div class="table-container">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th style="width: 50px;">#</th>
                            <th>Scheme Name</th>
                            <th>Info</th>
                            <th style="text-align: right;">Months</th>
                            <th style="text-align: right;">12M XIRR</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for row in top_n %}
                        <tr>
                            <td style="color: var(--text-muted); font-weight: 600;">{{ loop.index }}</td>
                            <td>
                                <span class="scheme-name">{{ row.schemeName }}</span>
                            </td>
                            <td>
                                {% if row.isin_growth_link %}
                                    <a href="{{ row.isin_growth_link }}" target="_blank" style="text-decoration: none; font-size: 10px; padding: 2px 4px; background: #eff6ff; color: #2563eb; border-radius: 4px; border: 1px solid #dbeafe; font-weight: 700;">G</a>
                                {% endif %}
                                {% if row.isin_div_link %}
                                    <a href="{{ row.isin_div_link }}" target="_blank" style="text-decoration: none; font-size: 10px; padding: 2px 4px; background: #fef2f2; color: #dc2626; border-radius: 4px; border: 1px solid #fee2e2; font-weight: 700;">D</a>
                                {% endif %}
                                {% if row.isin_growth in invested_isins or row.isin_div_reinvestment in invested_isins %}
                                    <span class="action-btn action-btn-invested">INVESTED</span>
                                {% elif row.can_invest_growth == true %}
                                    <span class="action-btn action-btn-invest">I</span>
                                {% elif row.can_invest_growth == false %}
                                    <span class="action-btn action-btn-divest">D</span>
                                {% endif %}
                            </td>
                            <td style="text-align: right; font-weight: 500;">{{ row.months }}m</td>
                            <td style="text-align: right;">
                                <span class="xirr-val {{ 'positive' if row.xirr > 0 else '' }}">
                                    {{ "%.2f"|format(row.xirr * 100) if row.xirr is not none else 'n/a' }}%
                                </span>
                            </td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </details>
        {% endif %}

        <div class="section-header">
            <span style="font-size: 18px; font-weight: 700; color: var(--primary);">📁 Performance by Category</span>
        </div>

        {% if not groups %}
            <p>No valid scheme data found for analysis.</p>
        {% endif %}

        {% for group in groups %}
        <details>
            <summary>
                {{ group.title }}
            </summary>
            <div class="table-container">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th style="width: 50px;">#</th>
                            <th>Scheme Name</th>
                            <th>Info</th>
                            <th style="text-align: right;">Months</th>
                            <th style="text-align: right;">12M XIRR</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for row in group.rows %}
                        <tr>
                            <td style="color: var(--text-muted); font-weight: 600;">{{ loop.index }}</td>
                            <td>
                                <span class="scheme-name">{{ row.schemeName }}</span>
                            </td>
                            <td>
                                {% if row.isin_growth_link %}
                                    <a href="{{ row.isin_growth_link }}" target="_blank" style="text-decoration: none; font-size: 10px; padding: 2px 4px; background: #eff6ff; color: #2563eb; border-radius: 4px; border: 1px solid #dbeafe; font-weight: 700;">G</a>
                                {% endif %}
                                {% if row.isin_div_link %}
                                    <a href="{{ row.isin_div_link }}" target="_blank" style="text-decoration: none; font-size: 10px; padding: 2px 4px; background: #fef2f2; color: #dc2626; border-radius: 4px; border: 1px solid #fee2e2; font-weight: 700;">D</a>
                                {% endif %}
                                {% if row.isin_growth in invested_isins or row.isin_div_reinvestment in invested_isins %}
                                    <span class="action-btn action-btn-invested">INVESTED</span>
                                {% elif row.can_invest_growth == true %}
                                    <span class="action-btn action-btn-invest">I</span>
                                {% elif row.can_invest_growth == false %}
                                    <span class="action-btn action-btn-divest">D</span>
                                {% endif %}
                            </td>
                            <td style="text-align: right; font-weight: 500;">{{ row.months }}m</td>
                            <td style="text-align: right;">
                                <span class="xirr-val {{ 'positive' if row.xirr > 0 else '' }}">
                                    {{ "%.2f"|format(row.xirr * 100) if row.xirr is not none else 'n/a' }}%
                                </span>
                            </td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </details>
        {% endfor %}

        <div class="footer">
            Generated by Antigravity MF Helper &bull; Data source: api.mfapi.in
        </div>
    </div>

    <script>
        // No JavaScript required for static report
    </script>
</body>
</html>
"""

# Portfolio Insights Template
INSIGHTS_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --primary: #1e293b;
            --accent: #2563eb;
            --success: #10b981;
            --warning: #f59e0b;
            --bg: #f8fafc;
            --card-bg: #ffffff;
            --text-main: #334155;
            --text-muted: #64748b;
        }
        body {
            font-family: 'Outfit', sans-serif;
            background-color: var(--bg);
            color: var(--text-main);
            line-height: 1.6;
            margin: 0;
            padding: 40px 20px;
        }
        .container { max-width: 1000px; margin: 0 auto; }
        .header {
            background: var(--card-bg);
            padding: 32px;
            border-radius: 16px;
            box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1);
            margin-bottom: 32px;
            border-top: 4px solid var(--accent);
        }
        h1 { margin: 0; font-size: 28px; color: var(--primary); }
        .subtitle { color: var(--text-muted); margin-top: 8px; font-size: 16px; }
        .section { margin-bottom: 40px; }
        .section-title {
            font-size: 20px;
            font-weight: 700;
            margin-bottom: 16px;
            display: flex;
            align-items: center;
            gap: 12px;
            color: var(--primary);
        }
        .card {
            background: var(--card-bg);
            border-radius: 12px;
            border: 1px solid #e2e8f0;
            padding: 24px;
            margin-bottom: 16px;
            transition: transform 0.2s, box-shadow 0.2s;
        }
        .card:hover { transform: translateY(-2px); box-shadow: 0 10px 15px -3px rgb(0 0 0 / 0.1); }
        .top-performer { border-left: 4px solid var(--success); }
        .needs-review { border-left: 4px solid var(--warning); }
        .status-tag {
            font-size: 10px;
            font-weight: 800;
            padding: 4px 8px;
            border-radius: 9999px;
            text-transform: uppercase;
        }
        .tag-success { background: #dcfce7; color: #166534; }
        .tag-warning { background: #fef3c7; color: #92400e; }
        .fund-name { font-size: 16px; font-weight: 700; color: var(--primary); margin-bottom: 4px; }
        .isin { font-family: monospace; font-size: 12px; color: var(--text-muted); }
        .stats { display: flex; gap: 24px; margin-top: 12px; font-size: 14px; }
        .stat-item { display: flex; flex-direction: column; }
        .stat-label { font-size: 11px; color: var(--text-muted); text-transform: uppercase; font-weight: 600; }
        .btn-back { display: inline-block; margin-bottom: 20px; color: var(--accent); text-decoration: none; font-weight: 600; font-size: 14px; }
    </style>
</head>
<body>
    <div class="container">
        <a href="{{ main_report_name }}" class="btn-back">← Back to Main Report</a>
        <div class="header">
            <h1>Portfolio Insights</h1>
            <p class="subtitle">Comparing your current holdings with the Top 200 Performing Funds</p>
        </div>

        <div class="section">
            <div class="section-title">
                <span style="background: var(--success); color: white; width: 24px; height: 24px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 14px;">★</span>
                Top Performers in Portfolio ({{ top_performers|length }})
            </div>
            {% if not top_performers %}
                <p style="color: var(--text-muted);">None of your current portfolio funds were found in the overall Top 200 list.</p>
            {% endif %}
            {% for fund in top_performers %}
            <div class="card top-performer">
                <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                    <div>
                        <div class="fund-name">{{ fund.scheme_name }}</div>
                        <div class="isin">ISIN: {{ fund.isin }}</div>
                    </div>
                    <span class="status-tag tag-success">Top Performer</span>
                </div>
                <div class="stats">
                    <div class="stat-item">
                        <span class="stat-label">Rank</span>
                        <span style="font-weight: 600; color: var(--accent);">#{{ fund.rank }}</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">Cost Value</span>
                        <span style="font-weight: 600;">₹{{ fund.cost_value }}</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">Market Value</span>
                        <span style="font-weight: 600;">₹{{ fund.market_value }}</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">Current NAV</span>
                        <span style="font-weight: 600;">₹{{ fund.current_nav }}</span>
                    </div>
                </div>
            </div>
            {% endfor %}
        </div>

        <div class="section">
            <div class="section-title">
                <span style="background: var(--warning); color: white; width: 24px; height: 24px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 14px;">!</span>
                Portfolio Funds to Review ({{ review_list|length }})
            </div>
            <p style="font-size: 14px; color: var(--text-muted); margin-bottom: 16px;">These funds are in your portfolio but did not make it into the overall Top 200 best performing funds based on 12M XIRR.</p>
            {% for fund in review_list %}
            <div class="card needs-review">
                <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                    <div>
                        <div class="fund-name">{{ fund.scheme_name }}</div>
                        <div class="isin">ISIN: {{ fund.isin }}</div>
                    </div>
                    <span class="status-tag tag-warning">Outside Top 200</span>
                </div>
                <div class="stats">
                    <div class="stat-item">
                        <span class="stat-label">Cost Value</span>
                        <span style="font-weight: 600;">₹{{ fund.cost_value }}</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">Market Value</span>
                        <span style="font-weight: 600;">₹{{ fund.market_value }}</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">Folio</span>
                        <span style="font-weight: 600;">{{ fund.folio }}</span>
                    </div>
                </div>
            </div>
            {% endfor %}
        </div>
    </div>
</body>
</html>
"""


def send_email_smtp(subject, html_body):
    msg = MIMEText(html_body, "html")
    msg['Subject'] = subject
    msg['From'] = FROM_EMAIL
    msg['To'] = ", ".join(TO_EMAILS)
    s = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    s.starttls()
    s.login(SMTP_USER, SMTP_PASS)
    s.sendmail(FROM_EMAIL, TO_EMAILS, msg.as_string())
    s.quit()

# Helper function to process a single scheme
def process_scheme(session, scheme, as_of, kuvera_client=None):
    code = scheme['schemeCode']
    name = scheme['schemeName']
    
    # IDCW/Dividend Filter: remove if name contains "IDCW", "Income Distribution", or "Payout"
    # to focus strictly on Growth plans.
    lower_name = name.lower()
    if "idcw" in lower_name or "income distribution" in lower_name:
        return None
    try:
        nav_data = fetch_nav_history(session, code, as_of)
        if not nav_data:
            return None
            
        # Filter out records where latest_nav_date is older than currentdate - 5
        latest_date = nav_data.get('end_date')
        if latest_date and latest_date < (as_of - timedelta(days=5)):
            return None
            
        xirr = compute_lumpsum_xirr(nav_data)
        meta = nav_data.get('meta', {})
        
        # Strict ISIN filter: remove if both isin_growth and isin_div_reinvestment are null/empty
        isin_g = meta.get("isin_growth")
        isin_d = meta.get("isin_div_reinvestment")
        if not isin_g and not isin_d:
            return None

        # Calculate months between start and end date (round up)
        start_date = nav_data.get('start_date')
        end_date = nav_data.get('end_date')
        months = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
        if end_date.day > start_date.day:
            months += 1
            
        # Minimum duration filter: remove if months < 5
        if months < 5:
            return None

        # Generate local ISIN detail pages
        growth_link = None
        div_link = None
        can_invest_g = None
        can_invest_d = None
        if kuvera_client:
            growth_link, can_invest_g = kuvera_client.generate_detail_page(isin_g)
            div_link, can_invest_d = kuvera_client.generate_detail_page(isin_d)

        return {
            "schemeCode": code,
            "schemeName": name,
            "xirr": xirr,
            "months": months,
            "fund_house": meta.get("fund_house"),
            "scheme_type": meta.get("scheme_type"),
            "scheme_category": meta.get("scheme_category"),
            "isin_growth": isin_g,
            "isin_growth_link": growth_link,
            "can_invest_growth": can_invest_g,
            "isin_div_reinvestment": isin_d,
            "isin_div_link": div_link,
            "can_invest_div": can_invest_d,
            "latest_nav": nav_data.get('end_nav'),
            "latest_nav_date": end_date,
            "prev_year_nav": nav_data.get('start_nav'),
            "prev_year_nav_date": start_date
        }
    except Exception as e:
        # Log first few errors to diagnose issues in CI/CD without flooding
        error_msg = str(e)
        logging.error(f"Error processing {code} ({name}): {error_msg}")
        return None

def main():
    # MFAPI data is naive, so we use a naive UTC date for consistent comparison
    now_utc = datetime.now(timezone.utc)
    as_of = pd.to_datetime(now_utc.replace(tzinfo=None))
    ist_timestamp = (now_utc + timedelta(hours=5, minutes=30)).strftime('%d-%b-%Y %I:%M %p IST')
    
    # Initialize Session with Connection Pooling
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=25, pool_maxsize=25)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    reports_dir = CONFIG["persistence"]["reports_dir"]
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)
        
    # Load invested ISINs from portfolio.json
    invested_isins = set()
    portfolio_path = "portfolio.json"
    if os.path.exists(portfolio_path):
        try:
            with open(portfolio_path, "r", encoding="utf-8") as pf:
                portfolio_data = json.load(pf)
                for fund in portfolio_data.get("funds", []):
                    if fund.get("isin"):
                        invested_isins.add(fund["isin"])
            logging.info(f"Loaded {len(invested_isins)} unique invested ISINs from {portfolio_path}.")
        except Exception as e:
            logging.error(f"Failed to load portfolio.json: {e}")

    kuvera_client = KuveraClient(session, reports_dir, invested_isins=invested_isins)
    
    logging.info("Fetching scheme list...")
    schemes = fetch_all_schemes(session)
    
    # Filter out Regular funds (case-insensitive)
    schemes = [s for s in schemes if "regular" not in s['schemeName'].lower()]
    logging.info(f"Filtered to {len(schemes)} schemes (excluding regular funds).")
    
    results = []
    
    # Process all schemes by default, or use scheme_limit if set
    limit_val = CONFIG["reporting"]["scheme_limit"]
    limit = int(limit_val) if limit_val else len(schemes)
    schemes_to_process = schemes[:limit]
    
    logging.info(f"Processing {limit} schemes with 25 threads...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        # Submit all tasks
        future_to_scheme = {executor.submit(process_scheme, session, s, as_of, kuvera_client): s for s in schemes_to_process}
        
        completed = 0
        for future in concurrent.futures.as_completed(future_to_scheme):
            completed += 1
            if completed % 100 == 0:
                # Log to file every 1000 items to avoid flooding, print to console every 100
                msg = f"Processed {completed}/{limit} schemes..."
                print(msg, end='\r') # Keep running console update
                if completed % 1000 == 0:
                    logging.info(msg)
                
            try:
                data = future.result()
                if data:
                    results.append(data)
                    # Optional: Print found schemes with high XIRR to show progress
                    if data['xirr'] and data['xirr'] > 0.15: # > 15%
                         logging.info(f"Found: {data['schemeName'][:40]}... | XIRR: {data['xirr']*100:.2f}%")
            except Exception as e:
                pass
                
    logging.info(f"Finished processing. Total results: {len(results)}")
    
    # Persist nav_data to NAVData directory in chunks of 5000
    try:
        # Create directory if not exists
        output_dir = CONFIG["persistence"]["nav_data_dir"]
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # Sort results by schemeCode to ensure ordered chunks
        results.sort(key=lambda x: int(x['schemeCode']))
        
        chunk_size = CONFIG["persistence"]["chunk_size"] or 5000
        total_chunks = (len(results) + chunk_size - 1) // chunk_size
        
        for i in range(total_chunks):
            chunk = results[i*chunk_size : (i+1)*chunk_size]
            if not chunk:
                continue
                
            start_code = chunk[0]['schemeCode']
            end_code = chunk[-1]['schemeCode']
            filename = f"nav_data_{start_code}_{end_code}.txt"
            filepath = os.path.join(output_dir, filename)
            
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(chunk, f, indent=2, default=str)
            
            logging.info(f"Persisted chunk {i+1}/{total_chunks} to {filepath} ({len(chunk)} records)")
            
    except Exception as e:
        logging.error(f"Failed to persist NAV data chunks: {e}")

    df = pd.DataFrame(results)
    
    # Check if we have any results
    if df.empty:
        logging.warning("No scheme data fetched. Exiting.")
        return
    
    # --- Grouping Logic: Top 20 per Scheme Type & Category ---
    df = df.dropna(subset=['xirr'])
    
    # Exclude unwanted categories: Debt, ELSS, IDF, Income
    exclude_keywords = ['debt', 'elss', 'idf', 'income']
    df = df[~df['scheme_category'].str.lower().str.contains('|'.join(exclude_keywords), na=False)]
    
    # Sort globally by XIRR first
    df = df.sort_values(by='xirr', ascending=False)
    
    def get_category_priority(cat_name):
        c = cat_name.lower()
        if c.startswith('equity'): return (1, cat_name)
        if c.startswith('hybrid'): return (2, cat_name)
        if c.startswith('debt'): return (3, cat_name)
        return (4, cat_name)

    # Get Top 200 Overall as a flat list
    top_200_records = df.head(200).to_dict(orient='records')
    
    groups = []
    # Identify unique categories (ignoring scheme_type in title)
    unique_groups = df.groupby('scheme_category', dropna=False)
    
    for s_cat, group_df in unique_groups:
        title = str(s_cat) if s_cat else "General"
        top_20 = group_df.head(20).to_dict(orient='records')
        if top_20:
            groups.append({
                "title": title,
                "rows": top_20
            })
            
    # Sort groups by priority
    groups.sort(key=lambda x: get_category_priority(x['title']))

    # --- Portfolio Insights Generation ---
    top_performers = []
    review_list = []
    
    # Load raw portfolio data for insights
    portfolio_funds = []
    if os.path.exists(portfolio_path):
        try:
            with open(portfolio_path, "r", encoding="utf-8") as f:
                p_data = json.load(f)
                portfolio_funds = p_data.get("funds", [])
        except Exception:
            pass

    # Compare portfolio funds with Top 200 list
    # Use ISIN as unique identifier
    top_200_isins = {str(r.get('isin_growth')).strip().upper() for r in top_200_records if r.get('isin_growth')}
    top_200_isins.update({str(r.get('isin_div_reinvestment')).strip().upper() for r in top_200_records if r.get('isin_div_reinvestment')})

    for p_fund in portfolio_funds:
        isin = str(p_fund.get('isin', '')).strip().upper()
        # Find if this ISIN or its alternate exists in top results and get its rank
        matching_top_idx = next((idx for idx, r in enumerate(top_200_records, 1) if str(r.get('isin_growth')).strip().upper() == isin or str(r.get('isin_div_reinvestment')).strip().upper() == isin), None)
        
        insight_entry = {
            "scheme_name": p_fund.get('scheme_name'),
            "isin": isin,
            "cost_value": p_fund.get('cost_value'),
            "market_value": p_fund.get('market_value'),
            "folio": p_fund.get('folio'),
            "current_nav": p_fund.get('nav'),
            "rank": matching_top_idx
        }
        
        if matching_top_idx:
            top_performers.append(insight_entry)
        else:
            review_list.append(insight_entry)

    # Sort top performers by rank
    top_performers.sort(key=lambda x: x['rank'])

    today_date = as_of.strftime('%Y-%m-%d')
    output_filename = f"report_{today_date}.html"
    insights_filename = f"insights_{today_date}.html"

    # Create Reports directory if not exists
    report_dir = CONFIG["persistence"]["reports_dir"]
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)

    # Render Insights Report
    insights_html = Template(INSIGHTS_TEMPLATE).render(
        top_performers=top_performers,
        review_list=review_list,
        main_report_name=output_filename
    )
    
    insights_path = os.path.join(report_dir, insights_filename)
    with open(insights_path, "w", encoding="utf-8") as f:
        f.write(insights_html)
    logging.info(f"Insights report saved to {insights_path}")

    # Render Main Report and update header link
    main_template_with_insights = EMAIL_TEMPLATE.replace('portfolio_insights.html', insights_filename)
    
    html = Template(main_template_with_insights).render(
        date=as_of.date(), 
        ist_time=ist_timestamp,
        groups=groups, 
        top_n=top_200_records,
        invested_isins=invested_isins
    )
    
    # Create Reports directory if not exists (already checked but keeping structure consistent)
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)
        
    today_date = as_of.strftime('%Y-%m-%d')
    output_filename = f"report_{today_date}.html"
    output_filepath = os.path.join(report_dir, output_filename)
    
    with open(output_filepath, "w", encoding="utf-8") as f:
        f.write(html)
    logging.info(f"Report saved to {output_filepath}")

    # --- Generate latest_reports.json manifest for static hosting ---
    manifest = {
        "latest_report": output_filename,
        "latest_insights": insights_filename,
        "last_updated": ist_timestamp
    }
    manifest_path = os.path.join(report_dir, "latest_reports.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=4)
    logging.info(f"Manifest saved to {manifest_path}")

    try:
        subject = f"Mutual Fund Analysis: Top 75 Overall & Category Rankings - {as_of.date()}"
        send_email_smtp(subject, html)
        logging.info("Email sent.")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")

if __name__=='__main__':
    main()
