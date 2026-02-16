# MFHelper - Mutual Fund Daily Report & Analysis

MFHelper is a Python-based utility designed to automate the process of fetching, analyzing, and reporting on Indian Mutual Fund schemes. It calculates 12-month XIRR, generates detailed scheme reports, and sends automated email summaries with categorized insights.

## Features

- **Automated Data Fetching**: Retrieves daily NAV data and scheme metadata from [mfapi.in](https://mfapi.in).
- **Advanced Performance Analysis**: Calculates rolling 12-month XIRR for thousands of schemes.
- **ISIN Deep-dive**: Integrates with Kuvera API to generate standalone HTML detail pages (Investment Objective, Returns, Fund Manager, Expense Ratio) for specific ISIN tags.
- **Categorized Reporting**: Generates a consolidated HTML report with dedicated sections for:
  - Top 75 overall schemes by XIRR.
  - Top 20 schemes categorized by Equity (Small, Mid, Large Cap), Hybrid, Debt, etc.
- **Persistence Layer**: Efficiently stores downloaded NAV data in sharded text files to avoid redundant API calls.
- **Smart Filtering**: Automatically filters out "Regular" plans, "IDCW" (Income Distribution) schemes, and funds with less than 5 months of history.
- **Email Delivery**: Sends rich-text HTML reports via SMTP (Gmail supported).

## Prerequisites

- **Python 3.8+**
- **pip** (Python package installer)

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd MFHelper
   ```

2. **Set up a Virtual Environment** (Recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Project settings are managed via `config.json`. Populate it with your SMTP credentials and output paths.

```json
{
    "smtp": {
        "host": "smtp.gmail.com",
        "port": 587,
        "user": "your-email@gmail.com",
        "pass": "your-app-password",
        "from": "your-email@gmail.com"
    },
    "reporting": {
        "to_emails": ["recipient@example.com"],
        "scheme_limit": null,
        "staleness_days": 5
    },
    "persistence": {
        "nav_data_dir": "NAVData",
        "reports_dir": "Reports",
        "logs_dir": "Logs"
    }
}
```

### Environment Variables (Optional Overrides)

- `SMTP_USER`: SMTP username
- `SMTP_PASS`: SMTP password (or App Password)
- `FROM_EMAIL`: Sender email address
- `REPORT_RECIPIENTS`: Comma-separated list of recipients
- `SCHEME_LIMIT`: Limit the number of schemes to process (useful for testing)
- `APP_CONFIG_JSON`: Entire configuration as a JSON string (useful for CI/CD)

## Execution

### 1. Run the Full Pipeline
This fetches all schemes (excluding Regular funds), calculates XIRR, generates the report, and sends emails.
```bash
python daily_mf_report.py
```

### 2. Run without Sending Email
Generates the HTML report locally in the `Reports/` directory without dispatching it via SMTP.
```bash
python daily_mf_report.py --no-email
```

### 3. Limited Run for Testing
Process only a small number of schemes to verify logic quickly.
```bash
# On Windows PowerShell
$env:SCHEME_LIMIT=10; python daily_mf_report.py --no-email

# On Linux/macOS
SCHEME_LIMIT=10 python daily_mf_report.py --no-email
```

## Project Structure

- `daily_mf_report.py`: Main execution script.
- `config.json`: Project configuration.
- `NAVData/`: Sharded persistence for scheme NAV history.
- `Reports/`: Contains the generated `index.html` report.
- `Reports/Details/`: Standalone detail pages for ISINs.
- `Logs/`: Execution logs for debugging.

## License

MIT License - See [LICENSE](LICENSE) for details.
