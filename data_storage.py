# data_storage.py
import sqlite3
import pandas as pd
import os
from datetime import datetime, date, time

# Helper function to parse time strings
def parse_time_string(time_str: str, report_date: date):
    if not time_str or not isinstance(time_str, str) or not report_date:
        return None
    
    formats_to_try = [
        "%I:%M %p",  # HH:MM AM/PM
        "%H:%M",     # HH:MM (24-hour)
        "%H:%M:%S",  # HH:MM:SS (24-hour)
        "%I:%M:%S %p", # HH:MM:SS AM/PM
    ]
    
    for fmt in formats_to_try:
        try:
            return datetime.combine(report_date, datetime.strptime(time_str, fmt).time())
        except ValueError:
            continue
    return None

# Helper function to parse AI-extracted date string to a specific timestamp string
def parse_ai_date_to_timestamp_str(date_str: str):
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        # Assuming date_str is YYYY-MM-DD
        dt_obj = datetime.strptime(date_str, "%Y-%m-%d")
        # Combine with a default time, e.g., midday
        full_dt_obj = datetime.combine(dt_obj.date(), time(12, 0, 0))
        return full_dt_obj.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        # Attempt to parse other common date formats if YYYY-MM-DD fails
        for fmt in ["%m/%d/%Y", "%d/%m/%Y", "%m-%d-%Y", "%d-%m-%Y"]:
            try:
                dt_obj = datetime.strptime(date_str, fmt)
                full_dt_obj = datetime.combine(dt_obj.date(), time(12, 0, 0))
                return full_dt_obj.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
        return None

# Initialize database with full schema (now includes summary)
def init_db(db_name="findings_db.sqlite"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS findings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        empi_id TEXT,
        critical_findings TEXT,
        incidental_findings TEXT,
        mammogram_score TEXT,
        follow_up TEXT,
        risk_level TEXT,
        summary TEXT,
        timestamp TEXT,
        critical_finding_response_time_minutes INTEGER,
        scan_type TEXT,
        radiologist_name TEXT,
        ai_report_timestamp TEXT,
        critical_findings_text TEXT,
        incidental_findings_text TEXT
    )
    """)
    conn.commit()
    conn.close()

# Determine risk level based on findings
def assess_risk(critical: str, incidental: str, follow_up: str) -> str:
    if (critical or "").strip().lower() == "yes":
        return "High"
    elif (incidental or "").strip().lower() == "yes" or (follow_up or "").strip().lower() == "yes":
        return "Medium"
    else:
        return "Low"

# Store findings into the SQLite database
def store_data_sql(extracted_data, db_name="findings_db.sqlite"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    init_db(db_name)

    for data in extracted_data:
        data['risk_level'] = assess_risk(
            data['critical_findings'],
            data['incidental_findings'],
            data['follow_up']
        )

    for data in extracted_data:
        # Timestamp handling for report date
        report_timestamp_obj = data['timestamp']
        if isinstance(report_timestamp_obj, str):
            try:
                report_timestamp_obj = pd.to_datetime(report_timestamp_obj)
            except ValueError:
                report_timestamp_obj = None # Invalid timestamp format
        
        report_date_obj = None
        if pd.notna(report_timestamp_obj) and hasattr(report_timestamp_obj, 'date'):
            report_date_obj = report_timestamp_obj.date()

        # Time parsing and difference calculation
        time_found_str = data.get('time_found')
        time_reported_str = data.get('time_reported')
        critical_finding_response_time_minutes = None

        if report_date_obj:
            found_datetime = parse_time_string(time_found_str, report_date_obj)
            reported_datetime = parse_time_string(time_reported_str, report_date_obj)

            if found_datetime and reported_datetime and reported_datetime > found_datetime:
                time_difference_seconds = (reported_datetime - found_datetime).total_seconds()
                critical_finding_response_time_minutes = int(time_difference_seconds / 60)
            elif found_datetime and reported_datetime and reported_datetime <= found_datetime: # Edge case: reported before or at the same time as found
                critical_finding_response_time_minutes = 0


        # Standardize timestamp for storage and duplicate check
        db_timestamp_str = None
        if pd.notna(report_timestamp_obj):
            if isinstance(report_timestamp_obj, pd.Timestamp):
                 db_timestamp_str = report_timestamp_obj.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(report_timestamp_obj, datetime):
                 db_timestamp_str = report_timestamp_obj.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(report_timestamp_obj, str): # Should have been converted, but as fallback
                 db_timestamp_str = report_timestamp_obj
        
        # Get new fields
        scan_type = data.get('scan_type', '')
        radiologist_name = data.get('radiologist_name', '')
        exam_date_ai_str = data.get('exam_date_ai', '')
        ai_report_timestamp_val = parse_ai_date_to_timestamp_str(exam_date_ai_str)
        
        # Get new text fields
        critical_findings_text = data.get('critical_findings_text', '')
        incidental_findings_text = data.get('incidental_findings_text', '')


        # Prevent duplicates
        cursor.execute("""
        SELECT 1 FROM findings WHERE empi_id = ? AND timestamp = ?
        """, (data['empi_id'], db_timestamp_str))

        if not cursor.fetchone():
            cursor.execute("""
            INSERT INTO findings (
                empi_id, critical_findings, incidental_findings,
                mammogram_score, follow_up, risk_level, summary, timestamp,
                critical_finding_response_time_minutes, scan_type, radiologist_name, ai_report_timestamp,
                critical_findings_text, incidental_findings_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data['empi_id'],
                data['critical_findings'],
                data['incidental_findings'],
                data['mammogram_score'],
                data['follow_up'],
                data['risk_level'],
                data.get('summary', ''),
                db_timestamp_str,
                critical_finding_response_time_minutes,
                scan_type,
                radiologist_name,
                ai_report_timestamp_val,
                critical_findings_text,
                incidental_findings_text
            ))

    conn.commit()
    conn.close()

# Load findings data from SQLite
def load_data_sql(db_name="findings_db.sqlite"):
    conn = sqlite3.connect(db_name)
    try:
        df = pd.read_sql_query("""
        SELECT empi_id, timestamp, critical_findings, incidental_findings,
               mammogram_score, follow_up, risk_level, summary,
               critical_finding_response_time_minutes, scan_type, radiologist_name, ai_report_timestamp,
               critical_findings_text, incidental_findings_text
        FROM findings
        """, conn)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # Ensure ai_report_timestamp is also datetime if needed for display, though it's stored as TEXT
        if 'ai_report_timestamp' in df.columns:
            df['ai_report_timestamp'] = pd.to_datetime(df['ai_report_timestamp'], errors='coerce')
        return df
    except Exception as e:
        print("Error loading data from DB:", e)
        return pd.DataFrame()
    finally:
        conn.close()

# Optional utility to reset the database during development
def reset_db(db_name="findings_db.sqlite"):
    if os.path.exists(db_name):
        os.remove(db_name)
    init_db(db_name)

# Optional utility to retry the database during development
def retry_failed_extractions(extract_fn, get_radio_fn, get_clinical_fn, db_name="findings_db.sqlite"):
    conn = sqlite3.connect(db_name)
    df_failed = pd.read_sql_query("""
        SELECT id, empi_id, timestamp
        FROM findings
        WHERE critical_findings IS NULL
           OR incidental_findings IS NULL
           OR follow_up IS NULL
           OR risk_level IS NULL
           OR summary IS NULL
           OR critical_finding_response_time_minutes IS NULL -- Retry if this is null too
    """, conn)

    if df_failed.empty:
        conn.close()
        return 0

    count_updated = 0
    cursor = conn.cursor()

    for _, row in df_failed.iterrows():
        empi_id = row["empi_id"]
        timestamp = row["timestamp"]

        # Get radiology report text
        radio_df = get_radio_fn(empi_id, timestamp)
        if radio_df.empty:
            continue
        radio_text = radio_df.iloc[0]["RADIO_REPORT_TEXT"]

        # Get nearest clinical report text
        clinical_df = get_clinical_fn(empi_id, timestamp)
        clinical_text = clinical_df.iloc[0]["CLINICAL_REPORT_TEXT"] if not clinical_df.empty else ""

        # Re-extract findings using Gemini
        findings = extract_fn(radio_text, clinical_text)

        # Time parsing and difference calculation for retry
        critical_finding_response_time_minutes = None
        report_timestamp_str = row["timestamp"] # This is a string from DB
        report_date_obj = None
        if report_timestamp_str:
            try:
                report_date_obj = datetime.strptime(report_timestamp_str.split(' ')[0], '%Y-%m-%d').date()
            except ValueError: # Handle cases where timestamp might be just date or other formats
                try:
                    report_date_obj = pd.to_datetime(report_timestamp_str).date()
                except:
                    report_date_obj = None


        if report_date_obj:
            time_found_str = findings.get('time_found')
            time_reported_str = findings.get('time_reported')
            
            found_datetime = parse_time_string(time_found_str, report_date_obj)
            reported_datetime = parse_time_string(time_reported_str, report_date_obj)

            if found_datetime and reported_datetime and reported_datetime > found_datetime:
                time_difference_seconds = (reported_datetime - found_datetime).total_seconds()
                critical_finding_response_time_minutes = int(time_difference_seconds / 60)
            elif found_datetime and reported_datetime and reported_datetime <= found_datetime:
                critical_finding_response_time_minutes = 0
        
        # Get new fields for retry
        scan_type = findings.get('scan_type', '')
        radiologist_name = findings.get('radiologist_name', '')
        exam_date_ai_str = findings.get('exam_date_ai', '')
        ai_report_timestamp_val = parse_ai_date_to_timestamp_str(exam_date_ai_str)
        
        # Get new text fields for retry
        critical_findings_text = findings.get('critical_findings_text', '')
        incidental_findings_text = findings.get('incidental_findings_text', '')


        cursor.execute("""
            UPDATE findings SET
                critical_findings = ?,
                incidental_findings = ?,
                mammogram_score = ?,
                follow_up = ?,
                risk_level = ?,
                summary = ?,
                critical_finding_response_time_minutes = ?,
                scan_type = ?,
                radiologist_name = ?,
                ai_report_timestamp = ?,
                critical_findings_text = ?,
                incidental_findings_text = ?
            WHERE id = ?
        """, (
            findings["critical_findings"],
            findings["incidental_findings"],
            findings["mammogram_score"],
            findings["follow_up"],
            findings["risk_level"],
            findings["summary"],
            critical_finding_response_time_minutes,
            scan_type,
            radiologist_name,
            ai_report_timestamp_val,
            critical_findings_text,
            incidental_findings_text,
            row["id"]
        ))

        count_updated += 1

    conn.commit()
    conn.close()
    return count_updated

