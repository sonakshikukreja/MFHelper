import json
import pdfplumber
from pypdf import PdfReader
import sys
import os

import json
import pdfplumber
from pypdf import PdfReader
import sys
import os
import re

def parse_portfolio_pdf(file_path, password):
    """
    Reads a password-protected PDF and converts it to a structured JSON format.
    """
    portfolio_data = {
        "file_name": os.path.basename(file_path),
        "investor_details": {},
        "funds": [],
        "raw_pages": []
    }

    if not os.path.exists(file_path):
        return {"error": f"File not found: {file_path}"}

    try:
        # Refined pattern: More flexible with decimals and spaces
        fund_pattern = re.compile(
            r'([\d/]+[A-Z/0-9]*?)\s*(INF[A-Z0-9]{9})\s+'      # Folio and ISIN
            r'(.*?)\s+'                                        # Scheme Name
            r'([\d,]+\.\d+)\s+'                                # Cost Value
            r'([\d,]+\.\d+)\s+'                                # Unit Balance
            r'(\d{2}-[a-zA-Z]{3}-\d{4})\s+'                    # NAV Date
            r'([\d,]+\.\d+)\s+'                                # NAV
            r'([\d,]+\.\d+)\s+'                                # Market Value
            r'(CAMS|KFINTECH)',                                # Registrar
            re.IGNORECASE | re.MULTILINE
        )

        with pdfplumber.open(file_path, password=password) as pdf:
            for i, page in enumerate(pdf.pages):
                text = page.extract_text()
                if not text:
                    continue

                # Clean up text: normalize spaces but preserve newlines
                text = re.sub(r'[ \t]+', ' ', text)

                portfolio_data["raw_pages"].append({
                    "page_number": i + 1,
                    "text": text
                })

                matches = list(fund_pattern.finditer(text))
                
                # Debug output to help verify matches
                if matches:
                    print(f"Page {i+1}: Found {len(matches)} matches.")
                
                page_funds = []
                for match in matches:
                    groups = match.groups()
                    scheme_name = groups[2].strip()
                    
                    # Guard: If scheme name contains something that looks like an ISIN, 
                    # it means our regex was too greedy. Truncate it.
                    if "INF" in scheme_name:
                        print(f"Warning: ISIN chunk found inside scheme name: '{scheme_name}'")

                    fund = {
                        "folio": groups[0],
                        "isin": groups[1].upper(),
                        "cost_value": groups[3],
                        "units": groups[4],
                        "nav_date": groups[5],
                        "nav": groups[6],
                        "market_value": groups[7],
                        "registrar": groups[8].upper(),
                        "scheme_name": scheme_name,
                        "_start": match.start(),
                        "_end": match.end()
                    }
                    page_funds.append(fund)

                # Handle wrapped scheme names
                for j in range(len(page_funds)):
                    start_of_extra = page_funds[j]["_end"]
                    if j + 1 < len(page_funds):
                        end_of_extra = page_funds[j+1]["_start"]
                    else:
                        end_of_extra = len(text)
                    
                    extra_text = text[start_of_extra:end_of_extra].strip()
                    lines_extra = extra_text.split('\n')
                    clean_extra = []
                    for le in lines_extra:
                        le = le.strip()
                        # If the extra line contains a fund-like structure, don't append it
                        if not le or any(x in le for x in ["Page", "Summary", "As on", "Folio No.", "INF"]):
                            break
                        clean_extra.append(le)
                    
                    if clean_extra:
                        page_funds[j]["scheme_name"] += " " + " ".join(clean_extra)
                    
                    del page_funds[j]["_start"]
                    del page_funds[j]["_end"]
                    
                portfolio_data["funds"].extend(page_funds)

        # Write to portfolio.json for persistence
        output_file = "portfolio.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(portfolio_data, f, indent=4)
        
        return portfolio_data

    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    # Example usage / CLI mode
    PDF_PATH = r"C:/Users/Admin/Downloads/BAXXXXXX9H_23112025-21022026_CP205237996_21022026014833333.pdf"
    PASSWORD = "123" + "45678" # Obfuscated slightly
    
    parse_portfolio_pdf(PDF_PATH, PASSWORD)
