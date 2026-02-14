# daily_mf_report.py
import logging
import sys
import requests, os, math, json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from datetime import datetime, timedelta, UTC
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
        },
        "ai": {
            "gemini_api_key": os.getenv("GEMINI_API_KEY")
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
    if os.getenv("GEMINI_API_KEY"):
        if "ai" not in default_config: default_config["ai"] = {}
        default_config["ai"]["gemini_api_key"] = os.getenv("GEMINI_API_KEY")
    
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
def fetch_nav_history(session, scheme_code, as_of_date):
    # Calculate 1 year ago from as_of_date
    start_date = (as_of_date - pd.DateOffset(years=1)).strftime('%Y-%m-%d')
    end_date = as_of_date.strftime('%Y-%m-%d')
    
    # Use date range API to get only 1 year of data
    url = f"{MFAPI_BASE}/mf/{scheme_code}?startDate={start_date}&endDate={end_date}"
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
    
    # Filter out zero or negative NAVs to handle schemes with missing/zero historical data
    navs = navs[navs['nav'] > 0]
    
    if navs.empty:
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
        .ai-container {
            background: #f0f9ff;
            border: 1px solid #bae6fd;
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 32px;
        }
        .ai-title {
            display: flex;
            align-items: center;
            gap: 8px;
            font-weight: 700;
            color: #0369a1;
            margin-bottom: 12px;
        }
        .ai-input-group {
            display: flex;
            gap: 12px;
            margin-bottom: 16px;
        }
        #ai-prompt {
            flex: 1;
            padding: 12px 16px;
            border: 1px solid #cbd5e1;
            border-radius: 8px;
            font-family: inherit;
            font-size: 14px;
        }
        #ask-ai-btn {
            background: #0ea5e9;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 8px;
            font-weight: 600;
            cursor: pointer;
            transition: opacity 0.2s;
        }
        #ask-ai-btn:disabled { opacity: 0.5; cursor: not-allowed; }
        #ai-response {
            background: white;
            padding: 16px;
            border-radius: 8px;
            border: 1px solid #e2e8f0;
            font-size: 14px;
            color: var(--text-main);
            min-height: 60px;
            display: none;
            white-space: pre-wrap;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Daily Mutual Fund Analysis</h1>
            <div class="meta">
                <span>Date: <strong>{{date}}</strong></span>
                <span class="status-badge">Multi-Category Performance Report</span>
            </div>
        </div>

        <div class="disclaimer">
            ⚠️ <strong>Disclaimer:</strong> This report is for educational purposes only. Past performance is not indicative of future results. Please consult a qualified financial advisor before making any investment decisions.
        </div>

        <div class="ai-container">
            <div class="ai-title">✨ AI Insights (Gemini)</div>
            <div class="ai-input-group">
                <input type="text" id="ai-prompt" placeholder="Ask AI about this report (e.g., 'What are the top 3 small cap funds?')...">
                <button id="ask-ai-btn">Ask AI</button>
            </div>
            <div id="ai-response"></div>
        </div>

        {% if top_n %}
        <div class="section-header" style="margin-top: 0;">
            <span style="font-size: 18px; font-weight: 700; color: var(--primary);">🏆 Overall Top 200 Performance</span>
        </div>
        
        {% for group in top_n %}
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
                <table>
                    <thead>
                        <tr>
                            <th style="width: 50px;">#</th>
                            <th>Scheme Name</th>
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
        const API_KEY = "{{ gemini_key }}";
        const promptInput = document.getElementById('ai-prompt');
        const askBtn = document.getElementById('ask-ai-btn');
        const responseDiv = document.getElementById('ai-response');

        async function askGemini() {
            const prompt = promptInput.value.trim();
            if (!prompt) return;
            if (!API_KEY || API_KEY === "YOUR_GEMINI_API_KEY_HERE") {
                responseDiv.style.display = 'block';
                responseDiv.innerHTML = "❌ Error: Gemini API key not configured in config.json.";
                return;
            }

            askBtn.disabled = true;
            askBtn.innerText = "Thinking...";
            responseDiv.style.display = 'block';
            responseDiv.innerHTML = "⏳ Analyzing report data...";

            // Extract context from tables
            let context = "Mutual Fund Report Data Summary:\n";
            document.querySelectorAll('.data-table').forEach(table => {
                const category = table.closest('details')?.querySelector('summary')?.innerText.trim() || "Table";
                context += `\nCategory: ${category}\n`;
                const rows = Array.from(table.querySelectorAll('tbody tr')).slice(0, 50); // Limit context size
                rows.forEach(row => {
                    const cells = row.querySelectorAll('td');
                    if (cells.length >= 3) {
                        const name = cells[1].innerText.trim();
                        const xirr = cells[2].innerText.trim();
                        context += `- ${name}: ${xirr}\n`;
                    }
                });
            });

            try {
                const response = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=${API_KEY}`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        contents: [{
                            parts: [{
                                text: `Context: ${context}\n\nUser Question: ${prompt}\n\nStrictly use the provided mutual fund data to answer. If the answer isn't in the data, say you don't know.`
                            }]
                        }]
                    })
                });

                const data = await response.json();
                if (data.candidates && data.candidates[0].content.parts[0].text) {
                    responseDiv.innerHTML = data.candidates[0].content.parts[0].text;
                } else {
                    responseDiv.innerHTML = "Sorry, I couldn't generate an analysis. Please check your prompt or API key.";
                }
            } catch (error) {
                responseDiv.innerHTML = "❌ Error calling Gemini API: " + error.message;
            } finally {
                askBtn.disabled = false;
                askBtn.innerText = "Ask AI";
            }
        }

        askBtn.addEventListener('click', askGemini);
        promptInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') askGemini();
        });
    </script>
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
def process_scheme(session, scheme, as_of):
    code = scheme['schemeCode']
    name = scheme['schemeName']
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
        
        return {
            "schemeCode": code,
            "schemeName": name,
            "xirr": xirr,
            "fund_house": meta.get("fund_house"),
            "scheme_type": meta.get("scheme_type"),
            "scheme_category": meta.get("scheme_category"),
            "isin_growth": meta.get("isin_growth"),
            "isin_div_reinvestment": meta.get("isin_div_reinvestment"),
            "latest_nav": nav_data.get('end_nav'),
            "latest_nav_date": nav_data.get('end_date'),
            "prev_year_nav": nav_data.get('start_nav'),
            "prev_year_nav_date": nav_data.get('start_date')
        }
    except Exception as e:
        # logging.debug(f"Error processing {code}: {e}") 
        return None

def main():
    as_of = pd.to_datetime(datetime.utcnow())
    
    # Initialize Session with Connection Pooling
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=25, pool_maxsize=25)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    logging.info("Fetching scheme list...")
    schemes = fetch_all_schemes(session)
    results = []
    
    # Process all schemes by default, or use scheme_limit if set
    limit_val = CONFIG["reporting"]["scheme_limit"]
    limit = int(limit_val) if limit_val else len(schemes)
    schemes_to_process = schemes[:limit]
    
    logging.info(f"Processing {limit} schemes with 25 threads...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
        # Submit all tasks
        future_to_scheme = {executor.submit(process_scheme, session, s, as_of): s for s in schemes_to_process}
        
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
    import json
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
    
    # Sort globally by XIRR first
    df = df.sort_values(by='xirr', ascending=False)
    
    def get_category_priority(cat_name):
        c = cat_name.lower()
        if c.startswith('equity'): return (1, cat_name)
        if c.startswith('hybrid'): return (2, cat_name)
        if c.startswith('debt'): return (3, cat_name)
        return (4, cat_name)

    # Get Top 200 Overall and group by category
    top_200_df = df.head(200)
    top_200_groups = []
    for s_cat, group_df in top_200_df.groupby('scheme_category', dropna=False, sort=False):
        top_200_groups.append({
            "title": str(s_cat) if s_cat else "General",
            "rows": group_df.to_dict(orient='records')
        })
    # Sort Top 200 groups by priority
    top_200_groups.sort(key=lambda x: get_category_priority(x['title']))
    
    groups = []
    # Identify unique categories (ignoring scheme_type in title)
    unique_groups = df.groupby('scheme_category', dropna=False)
    
    for s_cat, group_df in unique_groups:
        title = str(s_cat) if s_cat else "General"
        top_200 = group_df.head(20).to_dict(orient='records')
        if top_200:
            groups.append({
                "title": title,
                "rows": top_200
            })
            
    # Sort groups by priority
    groups.sort(key=lambda x: get_category_priority(x['title']))

    html = Template(EMAIL_TEMPLATE).render(
        date=as_of.date(), 
        groups=groups, 
        top_n=top_200_groups,
        gemini_key=CONFIG.get("ai", {}).get("gemini_api_key", "")
    )
    
    # Create Reports directory if not exists
    report_dir = CONFIG["persistence"]["reports_dir"]
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)
        
    today_date = as_of.strftime('%Y-%m-%d')
    output_filename = f"report_{today_date}.html"
    output_filepath = os.path.join(report_dir, output_filename)
    
    with open(output_filepath, "w", encoding="utf-8") as f:
        f.write(html)
    logging.info(f"Report saved to {output_filepath}")

    try:
        subject = f"Mutual Fund Analysis: Top 75 Overall & Category Rankings - {as_of.date()}"
        send_email_smtp(subject, html)
        logging.info("Email sent.")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")

if __name__=='__main__':
    main()
