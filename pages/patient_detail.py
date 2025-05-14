# patient_detail.py
import streamlit as st
import pandas as pd
import sqlite3
from data_retrieval import get_snowflake_data
from data_storage import load_data_sql

st.set_page_config(page_title="Patient Report Detail", layout="wide")

st.title("📋 Radiology Report Detail")

# Get selected patient ID and timestamp from session
if 'selected_patient' not in st.session_state or 'selected_timestamp' not in st.session_state:
    st.warning("No patient selected. Please go back to the dashboard.")
    if st.button("Return to Dashboard"):
        st.switch_page("streamlit_app.py")
    st.stop()

patient_id = st.session_state.selected_patient
timestamp = st.session_state.selected_timestamp

# Debug fetch report by EMPI only
@st.cache_data(show_spinner=False)
def debug_fetch_rows(empi_id):
    query = f"""
        SELECT EMPI_ID, REPORT_TEXT, TIMESTAMP FROM radio_reports
        WHERE EMPI_ID = '{empi_id}'
        ORDER BY TIMESTAMP DESC
        LIMIT 5
    """
    return get_snowflake_data(
        user='HAFZANASIM', password='Goodluck1234567!', account='YYB34419',
        warehouse='COMPUTE_WH', database='RADIOLOGYPREP', schema='INFORMATION_SCHEMA',
        query=query
    )

@st.cache_data(show_spinner=False)
def fetch_report_text_from_debug(empi_id, ts):
    df = debug_fetch_rows(empi_id)
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
    match = df[df['TIMESTAMP'] == pd.to_datetime(ts)]
    return match(patient_id).iloc[0]['REPORT_TEXT'] if not match.empty else "No report text found."

# Load findings from SQLite
df = load_data_sql()
df['timestamp'] = pd.to_datetime(df['timestamp'])
data = df[(df['empi_id'] == patient_id) & (df['timestamp'] == timestamp)]

if data.empty:
    st.error("No findings found for this patient.")
    st.stop()

record = data.iloc[0]
report_text = debug_fetch_rows(patient_id).iloc[0]['REPORT_TEXT']


col1, col2 = st.columns(2)

with col1:
    st.subheader("Original Report")
    st.markdown(f"""
    <div class='report-text'>{report_text}</div>
    """, unsafe_allow_html=True)

with col2:
    st.subheader("Extracted Findings")
    st.markdown(f"""
    <div class='report-text'>
    <b>Critical Findings:</b> {record['critical_findings']}<br>
    <b>Incidental Findings:</b> {record['incidental_findings']}<br>
    <b>Mammogram Score:</b> {record['mammogram_score']}<br>
    <b>Follow-up Required:</b> {record['follow_up']}
    </div>
    """, unsafe_allow_html=True)

if st.button("🔙 Back to Dashboard"):
    st.switch_page("pages/patient_detail.py")
