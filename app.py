import subprocess
import sys
import os
import json
import glob
from flask import Flask, render_template, request, jsonify, redirect, url_for, send_from_directory
from pdf_parser import parse_portfolio_pdf
from tkinter import Tk, filedialog

app = Flask(__name__)

# Ensure the Reports directory exists
REPORTS_DIR = os.path.join(os.getcwd(), "Reports")
if not os.path.exists(REPORTS_DIR):
    os.makedirs(REPORTS_DIR)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/browse")
def browse():
    root = Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    file_path = filedialog.askopenfilename(
        title="Select Portfolio PDF",
        filetypes=[("PDF Files", "*.pdf")]
    )
    root.destroy()
    return jsonify({"path": file_path})

@app.route("/reports/<path:filename>")
def serve_report(filename):
    return send_from_directory(REPORTS_DIR, filename)

@app.route("/view-latest")
def view_latest():
    """Redirects to the latest existing report immediately."""
    reports = glob.glob(os.path.join(REPORTS_DIR, "report_*.html"))
    if not reports:
        return "No reports found in Reports/ directory.", 404
    
    latest = max(reports, key=os.path.getmtime)
    return redirect(url_for('serve_report', filename=os.path.basename(latest)))

@app.route("/latest-report")
def latest_report_page():
    """Serves the loading page which will then trigger /generate-report."""
    return render_template("loading.html")

@app.route("/generate-report")
def generate_report():
    """Background task triggered by loading.html to run the analysis."""
    try:
        script_path = os.path.join(os.getcwd(), "daily_mf_report.py")
        # Increase timeout or handle long running process if needed
        subprocess.run([sys.executable, script_path], check=True)
        
        # Find the latest report
        reports = glob.glob(os.path.join(REPORTS_DIR, "report_*.html"))
        if not reports:
            return jsonify({"success": False, "error": "No reports found after generation."}), 404
        
        latest = max(reports, key=os.path.getmtime)
        return jsonify({
            "success": True, 
            "redirect_url": url_for('serve_report', filename=os.path.basename(latest))
        })
    except subprocess.CalledProcessError as e:
        return jsonify({"success": False, "error": f"Error generating report: {e}"}), 500
    except Exception as e:
        return jsonify({"success": False, "error": f"An error occurred: {e}"}), 500

@app.route("/process", methods=["POST"])
def process_pdf():
    data = request.json
    pdf_path = data.get("pdf_path")
    password = data.get("password")

    if not pdf_path or not password:
        return jsonify({"success": False, "error": "PDF Path and Password are required."}), 400

    # Call the modularized parser
    result = parse_portfolio_pdf(pdf_path, password)

    if result and "error" in result:
        return jsonify({"success": False, "error": result["error"]})
    
    return jsonify({
        "success": True, 
        "message": "Portfolio JSON generated successfully!",
        "data": result
    })

if __name__ == "__main__":
    app.run(debug=True, port=5000)
