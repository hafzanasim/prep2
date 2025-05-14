import google.generativeai as genai
import json
import re

def configure_gemini(api_key):
    genai.configure(api_key="AIzaSyAZ5BSEUTGEOrKeX2AIUdD-CIDuH5lTB1U")

def _remove_fences(text: str) -> str:
    """
    Remove leading/trailing ``` fences (with or without a language tag).
    """
    # strip outer whitespace first
    text = text.strip()
    # regex for ```anything\n at start and ``` at end
    fenced = re.match(r"^```[\w]*\s*(.*?)\s*```$", text, re.S)
    return fenced.group(1).strip() if fenced else text

def extract_findings(report_text):
    prompt = f"""
    Report Text: {report_text}
    Examine the following information from the given radiology report. Answer only the following in JSON format:

    * Critical Findings: Yes/No
    * Incidental Findings: Yes/No
    * Mammogram Score: [Numeric Score/Category Number]
    * Follow Up Required: Yes/No
    """
    try:
        model = genai.GenerativeModel("gemini-2.0-flash")
        resp = model.generate_content(prompt)
        raw = resp.text
        print("Gemini raw output:", raw)

        clean = _remove_fences(raw)
        data = json.loads(clean)

        return {
            'critical_findings'  : data.get('Critical Findings', 'No'),
            'incidental_findings': data.get('Incidental Findings', 'No'),
            'mammogram_score'    : data.get('Mammogram Score', 'Not Available'),
            'follow_up'          : data.get('Follow Up Required', 'No')
        }
    except Exception as e:
        print("Error extracting findings:", e)
        return {
            'critical_findings'  : 'None',
            'incidental_findings': 'None',
            'mammogram_score'    : 'None',
            'follow_up'          : 'None'
        }
