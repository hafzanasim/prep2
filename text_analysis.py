#text_analysis.py
import google.generativeai as genai
import json
import re
import os
from dotenv import load_dotenv

load_dotenv()


def configure_gemini(api_key=None):
    genai.configure(api_key=api_key or os.getenv("GEMINI_API_KEY"))

def _remove_fences(text: str) -> str:
    """
    Remove leading/trailing ``` fences (with or without a language tag).
    """
    # strip outer whitespace first
    text = text.strip()
    # regex for ```anything\n at start and ``` at end
    fenced = re.match(r"^```[\w]*\s*(.*?)\s*```$", text, re.S)
    return fenced.group(1).strip() if fenced else text

def extract_findings(radiology_text, clinical_text):
    prompt = f"""
    Radiology Report:
    {radiology_text}

    Clinical Report (Patient History):
    {clinical_text}

    Based on the radiology report and the patient’s clinical report, extract and return the following in JSON format:

    * Critical Findings: Yes/No
    * Incidental Findings: Yes/No
    * Mammogram Score: [Numeric Score or Category]
    * Follow Up Required: Yes/No
    * Assign a patient risk level (based on findings and history): Low, Medium, or High
    * Provide a brief 2-3 sentence summary of the patient’s medical history based on patient’s clinical report.
    * Time Critical Findings Found: [Time in HH:MM AM/PM or HH:MM:SS format]
    * Time Critical Findings Reported: [Time in HH:MM AM/PM or HH:MM:SS format]
    * Scan Type: [Type of scan performed, e.g., MRI Brain, CT Chest]
    * Exam Date: [Date of examination from report text, e.g., YYYY-MM-DD]
    * Radiologist Name: [Name of the reporting radiologist]
    * Critical Findings Text: "[Actual text of critical findings from the report, or 'None' if no critical findings]"
    * Incidental Findings Text: "[Actual text of incidental findings from the report, or 'None' if no incidental findings]"

    Return ONLY the following keys in a JSON object:
    "Critical Findings", "Incidental Findings", "Mammogram Score", "Follow Up Required", "Risk Level", "Summary", "Time Critical Findings Found", "Time Critical Findings Reported", "Scan Type", "Exam Date", "Radiologist Name", "Critical Findings Text", and "Incidental Findings Text".

    Do not include commentary or code block formatting.
    """

    try:
        model = genai.GenerativeModel("gemini-1.5-flash")
        resp = model.generate_content(prompt)
        raw = resp.text
        print("Gemini raw output:", raw)

        clean = _remove_fences(raw)
        data = json.loads(clean)

        return {
            'critical_findings'  : data.get('Critical Findings', 'No'),
            'incidental_findings': data.get('Incidental Findings', 'No'),
            'mammogram_score'    : data.get('Mammogram Score', 'Not Available'),
            'follow_up'          : data.get('Follow Up Required', 'No'),
            'risk_level'         : data.get('Risk Level', 'Low'),
            'summary'            : data.get('Summary', ''),
            'time_found'         : data.get('Time Critical Findings Found', ''),
            'time_reported'      : data.get('Time Critical Findings Reported', ''),
            'scan_type'          : data.get('Scan Type', ''),
            'exam_date_ai'       : data.get('Exam Date', ''),
            'radiologist_name'   : data.get('Radiologist Name', ''),
            'critical_findings_text': data.get('Critical Findings Text', ''),
            'incidental_findings_text': data.get('Incidental Findings Text', '')
        }
    except Exception as e:
        print("Error extracting findings:", e)
        return {
            'critical_findings'  : 'None',
            'incidental_findings': 'None',
            'mammogram_score'    : 'None',
            'follow_up'          : 'None',
            'risk_level'         : 'None',
            'summary'            : '',
            'time_found'         : '',
            'time_reported'      : '',
            'scan_type'          : '',
            'exam_date_ai'       : '',
            'radiologist_name'   : '',
            'critical_findings_text': '',
            'incidental_findings_text': ''
        }

