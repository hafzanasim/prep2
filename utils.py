# utils.py -- idk why i have this
import json
import pandas as pd

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

# Helper to normalize timestamps
def canonical_ts(series: pd.Series) -> pd.Series:
    # Ensure it's a pandas Series
    if not isinstance(series, pd.Series):
        series = pd.Series(series)

    dt_series = pd.to_datetime(series, errors='coerce')

    # Check if any valid datetime objects exist after coercion
    if dt_series.notna().empty or dt_series.notna().sum() == 0:
        # Return an empty series of object dtype or original if all NaT
        # to avoid errors on .dt accessor with all NaT series.
        return pd.Series([], dtype='object') if series.empty else series.astype(object)


    # Convert to UTC if timezone aware
    if dt_series.dt.tz is not None:
        dt_series = dt_series.dt.tz_convert('UTC')

    # Floor to second, make naive, and format
    # Apply operations only to non-NaT values to avoid warnings/errors
    # then fill NaT back if any, though coerce should handle bad dates.
    
    # Create a mask for non-NaT values
    not_nat_mask = dt_series.notna()
    
    # Initialize result series with original values (especially NaTs)
    result_series = pd.Series(index=dt_series.index, dtype=object)

    if not_nat_mask.any():
      result_series[not_nat_mask] = dt_series[not_nat_mask].dt.floor("s").dt.tz_localize(None).dt.strftime("%Y-%m-%d %H:%M:%S")
    
    # Fill any NaNs/NaTs that might not have been covered - with None or pd.NaT as string
    result_series[~not_nat_mask] = None # Or pd.NaT depending on desired representation of invalid dates

    return result_series
