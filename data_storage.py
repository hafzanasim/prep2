# data_storage.py
import sqlite3
import pandas as pd
import os

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
        timestamp TEXT
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
        timestamp = data['timestamp']
        if isinstance(timestamp, pd.Timestamp):
            timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')

        # Prevent duplicates
        cursor.execute("""
        SELECT 1 FROM findings WHERE empi_id = ? AND timestamp = ?
        """, (data['empi_id'], timestamp))

        if not cursor.fetchone():
            cursor.execute("""
            INSERT INTO findings (
                empi_id, critical_findings, incidental_findings,
                mammogram_score, follow_up, risk_level, summary, timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data['empi_id'],
                data['critical_findings'],
                data['incidental_findings'],
                data['mammogram_score'],
                data['follow_up'],
                data['risk_level'],
                data.get('summary', ''),  # default to empty string if missing
                timestamp
            ))

    conn.commit()
    conn.close()

# Load findings data from SQLite
def load_data_sql(db_name="findings_db.sqlite"):
    conn = sqlite3.connect(db_name)
    try:
        df = pd.read_sql_query("""
        SELECT empi_id, timestamp, critical_findings, incidental_findings,
               mammogram_score, follow_up, risk_level, summary
        FROM findings
        """, conn)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
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

        cursor.execute("""
            UPDATE findings SET
                critical_findings = ?,
                incidental_findings = ?,
                mammogram_score = ?,
                follow_up = ?,
                risk_level = ?,
                summary = ?
            WHERE id = ?
        """, (
            findings["critical_findings"],
            findings["incidental_findings"],
            findings["mammogram_score"],
            findings["follow_up"],
            findings["risk_level"],
            findings["summary"],
            row["id"]
        ))

        count_updated += 1

    conn.commit()
    conn.close()
    return count_updated

