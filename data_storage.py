# data_storage.py
import sqlite3
import pandas as pd
import os

# Initialize database with full schema
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
        timestamp TEXT
    )
    """)
    conn.commit()
    conn.close()

def store_data_sql(extracted_data, db_name="findings_db.sqlite"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    init_db(db_name)

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
            INSERT INTO findings (empi_id, critical_findings, incidental_findings, mammogram_score, follow_up, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (
                data['empi_id'],
                data['critical_findings'],
                data['incidental_findings'],
                data['mammogram_score'],
                data['follow_up'],
                timestamp
            ))

    conn.commit()
    conn.close()

# Load findings data from SQLite
def load_data_sql(db_name="findings_db.sqlite"):
    conn = sqlite3.connect(db_name)
    try:
        df = pd.read_sql_query("""
        SELECT empi_id, timestamp, critical_findings, incidental_findings, mammogram_score, follow_up FROM findings
        """, conn)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception as e:
        print("Error loading data from DB:", e)
        return pd.DataFrame()
    finally:
        conn.close()

# Optional utility to clear DB during development
def reset_db(db_name="findings_db.sqlite"):
    if os.path.exists(db_name):
        os.remove(db_name)
    init_db(db_name)
