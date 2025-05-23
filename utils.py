# utils.py -- idk why i have this
import json

def process_extracted_findings(extracted_text):
    """
    Process the extracted text and return structured data for storage.
    """
    # Initialize findings with default values in case the JSON is malformed or missing
    findings = {
        'critical_findings': 'No',
        'incidental_findings': 'No',
        'mammogram_score': 'Not Available',
        'follow_up': 'No',
        'risk_level': 'Not Available'
    }

    if extracted_text is None:
        return findings

    try:
        # If extracted_text is a string, clean it
        if isinstance(extracted_text, str):
            clean_text = extracted_text.strip()
        elif isinstance(extracted_text, dict):
            # If it's already a dictionary, no need to clean
            return extracted_text
        
        # Parse the cleaned text as JSON if it's still a string
        extracted_data = json.loads(clean_text)
        
        # Ensure that we have all the necessary keys
        findings['critical_findings'] = extracted_data.get('Critical Findings', 'No')
        findings['incidental_findings'] = extracted_data.get('Incidental Findings', 'No')
        findings['mammogram_score'] = extracted_data.get('Mammogram Score', 'Not Available')
        findings['follow_up'] = extracted_data.get('Do you think a follow up is required', 'No')
        findings['risk_level'] = extracted_data.get('Risk Level', 'Not Available')

        return findings

    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        return findings  # Return default findings in case of an error
